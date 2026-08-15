package service_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/edgeflag/edgeflag-wave-publisher/internal/protocol"
	"github.com/edgeflag/edgeflag-wave-publisher/pkg/testkit"
	"github.com/edgeflag/edgeflag-wave-publisher/pkg/vnode"
)

// sendAckFrame writes an in-order ack frame with the given sequence number to
// the node-side wire so the service session can read it on the next PumpIn. The
// ack payload references a non-existent rollout on purpose: the session layer
// still advances the inbound sequence watermark for an in-order frame even when
// ProcessAck finds no matching rollout (its error is intentionally ignored by
// dispatchFrame), which is exactly the path whose watermark we are pinning down.
func sendAckFrame(t *testing.T, nodeWire interface{ Write([]byte) (int, error) }, seq uint64) {
	t.Helper()
	ap := protocol.AckPayload{
		RolloutID: "r-watermark", NodeID: "n-wm",
		ConfigVersion: 1, Attempt: 1, Level: 3, OK: true,
	}
	payload, err := json.Marshal(ap)
	if err != nil {
		t.Fatalf("marshal ack: %v", err)
	}
	enc, err := protocol.Encode(protocol.Frame{
		Sequence:    seq,
		MessageType: protocol.MsgAck,
		Payload:     payload,
	})
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}
	if _, err := nodeWire.Write(enc); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

// TestSessionInboundWatermarkSingleFrame verifies that after the session
// processes the in-order legal frame with sequence 1, the persisted session
// record's last_committed_seq is 1 (the sequence just processed) and
// expected_seq is 2 (the next expected). This is the regression guard for the
// bug where last_committed_seq ran ahead to 2.
func TestSessionInboundWatermarkSingleFrame(t *testing.T) {
	h := testkit.NewHarness(t)
	ctx := context.Background()

	nodeWire, svcWire := vnode.Connect()
	t.Cleanup(func() { _ = nodeWire.Close() })
	t.Cleanup(func() { _ = svcWire.Close() })

	sess, err := h.Service.AcceptSession(ctx, "n-wm", svcWire)
	if err != nil {
		t.Fatalf("accept session: %v", err)
	}

	// A fresh session expects sequence 1 and has committed nothing yet.
	before, err := h.Store.GetSession(ctx, sess.SessionID())
	if err != nil {
		t.Fatalf("get session before: %v", err)
	}
	if before.ExpectedSeq != 1 || before.LastCommittedSeq != 0 {
		t.Fatalf("fresh watermark: expected_seq=%d last_committed_seq=%d, want 1/0",
			before.ExpectedSeq, before.LastCommittedSeq)
	}

	sendAckFrame(t, nodeWire, 1)
	if _, err := sess.PumpIn(); err != nil {
		t.Fatalf("pump in: %v", err)
	}

	got, err := h.Store.GetSession(ctx, sess.SessionID())
	if err != nil {
		t.Fatalf("get session after: %v", err)
	}
	if got.LastCommittedSeq != 1 {
		t.Errorf("last_committed_seq = %d, want 1 (must reflect the just-processed frame, not run ahead)",
			got.LastCommittedSeq)
	}
	if got.ExpectedSeq != 2 {
		t.Errorf("expected_seq = %d, want 2 (next expected must stay correct)", got.ExpectedSeq)
	}
}

// TestSessionInboundWatermarkConsecutiveFrames verifies that processing a run of
// in-order frames advances last_committed_seq to the most recently completed
// frame while expected_seq tracks the next expected, frame by frame.
func TestSessionInboundWatermarkConsecutiveFrames(t *testing.T) {
	h := testkit.NewHarness(t)
	ctx := context.Background()

	nodeWire, svcWire := vnode.Connect()
	t.Cleanup(func() { _ = nodeWire.Close() })
	t.Cleanup(func() { _ = svcWire.Close() })

	sess, err := h.Service.AcceptSession(ctx, "n-wm", svcWire)
	if err != nil {
		t.Fatalf("accept session: %v", err)
	}

	// Send three in-order frames in one batch; the decoder drains them all and
	// handleFrame processes each against the freshly-loaded session row.
	for _, seq := range []uint64{1, 2, 3} {
		sendAckFrame(t, nodeWire, seq)
	}
	if _, err := sess.PumpIn(); err != nil {
		t.Fatalf("pump in: %v", err)
	}

	got, err := h.Store.GetSession(ctx, sess.SessionID())
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if got.LastCommittedSeq != 3 {
		t.Errorf("last_committed_seq = %d, want 3 (most recently completed frame)", got.LastCommittedSeq)
	}
	if got.ExpectedSeq != 4 {
		t.Errorf("expected_seq = %d, want 4 (next expected)", got.ExpectedSeq)
	}

	// A duplicate of the last-processed sequence must not move the watermark
	// (duplicate path performs no business mutation and no AdvanceSession).
	sendAckFrame(t, nodeWire, 3)
	if _, err := sess.PumpIn(); err != nil {
		t.Fatalf("pump duplicate: %v", err)
	}
	dup, err := h.Store.GetSession(ctx, sess.SessionID())
	if err != nil {
		t.Fatalf("get session after dup: %v", err)
	}
	if dup.LastCommittedSeq != 3 || dup.ExpectedSeq != 4 {
		t.Errorf("after duplicate: last_committed_seq=%d expected_seq=%d, want 3/4 (duplicate must not advance watermark)",
			dup.LastCommittedSeq, dup.ExpectedSeq)
	}
}

// TestSessionInboundWatermarkReconnectReadsPersisted verifies that on reconnect
// the new session's watermarks are read from the persisted prior session record
// rather than derived. After processing sequence 1 the persisted
// last_committed_seq is 1; on reconnect the new session must carry that exact
// persisted watermark forward (last_committed_seq=1, expected_seq=2). With the
// old advance bug the persisted last_committed_seq was 2, so a resume that read
// the persisted value would surface the corruption rather than mask it.
func TestSessionInboundWatermarkReconnectReadsPersisted(t *testing.T) {
	h := testkit.NewHarness(t)
	ctx := context.Background()

	nodeWire, svcWire := vnode.Connect()
	t.Cleanup(func() { _ = nodeWire.Close() })
	t.Cleanup(func() { _ = svcWire.Close() })

	sess1, err := h.Service.AcceptSession(ctx, "n-wm", svcWire)
	if err != nil {
		t.Fatalf("accept session 1: %v", err)
	}
	sendAckFrame(t, nodeWire, 1)
	if _, err := sess1.PumpIn(); err != nil {
		t.Fatalf("pump in: %v", err)
	}

	committed, err := h.Store.GetSession(ctx, sess1.SessionID())
	if err != nil {
		t.Fatalf("get session 1: %v", err)
	}
	if committed.LastCommittedSeq != 1 || committed.ExpectedSeq != 2 {
		t.Fatalf("pre-reconnect watermark: last_committed_seq=%d expected_seq=%d, want 1/2",
			committed.LastCommittedSeq, committed.ExpectedSeq)
	}

	// Simulate a reconnect: drop the live session and accept a new one for the
	// same node. AcceptSession resumes watermarks from the prior session row.
	h.Service.CloseSession(ctx, sess1)
	_ = svcWire.Close()

	nodeWire2, svcWire2 := vnode.Connect()
	t.Cleanup(func() { _ = nodeWire2.Close() })
	t.Cleanup(func() { _ = svcWire2.Close() })

	sess2, err := h.Service.AcceptSession(ctx, "n-wm", svcWire2)
	if err != nil {
		t.Fatalf("accept session 2: %v", err)
	}
	if sess2.SessionID() == sess1.SessionID() {
		t.Fatalf("reconnect allocated the same session id; expected a new row")
	}

	resumed, err := h.Store.GetSession(ctx, sess2.SessionID())
	if err != nil {
		t.Fatalf("get session 2: %v", err)
	}
	if resumed.LastCommittedSeq != 1 {
		t.Errorf("reconnect last_committed_seq = %d, want 1 (must read the persisted watermark, not run ahead)",
			resumed.LastCommittedSeq)
	}
	if resumed.ExpectedSeq != 2 {
		t.Errorf("reconnect expected_seq = %d, want 2 (next expected must be preserved)", resumed.ExpectedSeq)
	}

	// After reconnect, the next in-order frame is sequence 2 (resuming exactly
	// where the prior session left off), and committing it moves
	// last_committed_seq to 2.
	sendAckFrame(t, nodeWire2, 2)
	if _, err := sess2.PumpIn(); err != nil {
		t.Fatalf("pump in after reconnect: %v", err)
	}
	after, err := h.Store.GetSession(ctx, sess2.SessionID())
	if err != nil {
		t.Fatalf("get session 2 after: %v", err)
	}
	if after.LastCommittedSeq != 2 || after.ExpectedSeq != 3 {
		t.Errorf("post-reconnect advance: last_committed_seq=%d expected_seq=%d, want 2/3",
			after.LastCommittedSeq, after.ExpectedSeq)
	}
}
