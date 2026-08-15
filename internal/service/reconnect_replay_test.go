package service_test

import (
	"context"
	"testing"

	"github.com/edgeflag/edgeflag-wave-publisher/internal/domain"
	"github.com/edgeflag/edgeflag-wave-publisher/internal/protocol"
	"github.com/edgeflag/edgeflag-wave-publisher/pkg/testkit"
	"github.com/edgeflag/edgeflag-wave-publisher/pkg/vnode"
)

// TestSessionReconnectReplaysUnackedFrames is the regression test for a node
// streaming session reconnect: when the node disconnects after receiving an
// unacknowledged server frame and reconnects with the same node identity, the
// service must replay every still-pending frame to the new connection in its
// original sequence order so the node can resume processing and no publish
// instruction is lost.
//
// Before the fix, replayPendingFrames only re-enqueued the frames under the new
// session id in the persistent queue but never wrote them to the new wire, so
// the reconnecting node read zero frames.
func TestSessionReconnectReplaysUnackedFrames(t *testing.T) {
	h := testkit.NewHarness(t)
	ctx := context.Background()

	// Register the node.
	if err := h.Service.UpsertNode(ctx, domain.Node{
		ID: "n1", Labels: map[string]string{"zone": "a"}, Online: true,
	}); err != nil {
		t.Fatal(err)
	}

	// First connection: RefuseForward makes the node receive dispatches but never
	// acknowledge them, so the frames stay unacked in the pending queue.
	sess1, n1 := connectNode(t, h, "n1", vnode.Behavior{
		AckLevel:     domain.AckConfirmed,
		RefuseForward: true,
	})

	// Send two dispatch frames on the old connection. They are persisted as
	// pending frames for session 1 and written to the old wire.
	disp1 := protocol.DispatchPayload{
		RolloutID: "r1", ConfigVersion: 1, Attempt: 1,
		Direction: "forward", Content: `{"f":1}`,
	}
	disp2 := protocol.DispatchPayload{
		RolloutID: "r1", ConfigVersion: 2, Attempt: 1,
		Direction: "forward", Content: `{"f":2}`,
	}
	if _, err := sess1.Send(protocol.MsgDispatch, disp1); err != nil {
		t.Fatalf("send disp1: %v", err)
	}
	if _, err := sess1.Send(protocol.MsgDispatch, disp2); err != nil {
		t.Fatalf("send disp2: %v", err)
	}

	// Drive the old node to quiescence: it receives both frames (per the
	// scenario) but, because of RefuseForward, sends no acks.
	pumpQuiescent(t, sess1, n1)
	if got := len(n1.Dispatches()); got != 2 {
		t.Fatalf("old connection received %d dispatches, want 2", got)
	}
	if got := n1.AcksSent(); got != 0 {
		t.Fatalf("old node sent %d acks, want 0 (frames must stay unacked)", got)
	}

	// Disconnect the old connection. CloseSession drops the in-memory session and
	// marks the node offline; the session row and its pending frames remain in
	// the store for replay.
	h.Service.CloseSession(ctx, sess1)
	_ = n1.Close()

	// Reconnect with the same node identity on a fresh wire.
	sess2, n2 := connectNode(t, h, "n1", vnode.Behavior{
		AckLevel:     domain.AckConfirmed,
		RefuseForward: true,
	})

	// A new session id must have been allocated for the reconnect.
	if sess2.SessionID() == sess1.SessionID() {
		t.Fatalf("expected a new session id on reconnect, both were %d", sess1.SessionID())
	}

	// Pump the new connection: the pending frames must be replayed to the new
	// wire in original order.
	pumpQuiescent(t, sess2, n2)

	got := n2.Dispatches()
	if len(got) != 2 {
		t.Fatalf("new connection received %d dispatches, want 2 (replayed unacked frames)", len(got))
	}
	if got[0].ConfigVersion != disp1.ConfigVersion || got[1].ConfigVersion != disp2.ConfigVersion {
		t.Fatalf("replay order mismatch: got config versions [%d, %d], want [%d, %d]",
			got[0].ConfigVersion, got[1].ConfigVersion, disp1.ConfigVersion, disp2.ConfigVersion)
	}
	if got[0].Content != disp1.Content || got[1].Content != disp2.Content {
		t.Fatalf("replayed payload content mismatch: got %q, %q", got[0].Content, got[1].Content)
	}
}

// TestSessionReconnectReplaysAcrossTwoDrops verifies that replayed frames are
// re-persisted under the new session id, so a second disconnect (again before
// any ack) still replays them on the next reconnect. This guards the
// enqueue-then-write ordering of the replay boundary.
func TestSessionReconnectReplaysAcrossTwoDrops(t *testing.T) {
	h := testkit.NewHarness(t)
	ctx := context.Background()

	if err := h.Service.UpsertNode(ctx, domain.Node{
		ID: "n1", Labels: map[string]string{"zone": "a"}, Online: true,
	}); err != nil {
		t.Fatal(err)
	}

	disp := protocol.DispatchPayload{
		RolloutID: "r1", ConfigVersion: 7, Attempt: 1,
		Direction: "forward", Content: `{"f":7}`,
	}
	beh := vnode.Behavior{AckLevel: domain.AckConfirmed, RefuseForward: true}

	// First connection: send one unacked dispatch, then drop.
	sess1, n1 := connectNode(t, h, "n1", beh)
	if _, err := sess1.Send(protocol.MsgDispatch, disp); err != nil {
		t.Fatalf("send disp: %v", err)
	}
	pumpQuiescent(t, sess1, n1)
	h.Service.CloseSession(ctx, sess1)
	_ = n1.Close()

	// Second connection: the frame should replay, but the node still does not
	// ack, so it stays pending under the second session. Drop again.
	sess2, n2 := connectNode(t, h, "n1", beh)
	pumpQuiescent(t, sess2, n2)
	if got := len(n2.Dispatches()); got != 1 {
		t.Fatalf("second connection received %d dispatches, want 1 (first replay)", got)
	}
	h.Service.CloseSession(ctx, sess2)
	_ = n2.Close()

	// Third connection: the frame must replay again from the second session's
	// persisted queue.
	sess3, n3 := connectNode(t, h, "n1", beh)
	pumpQuiescent(t, sess3, n3)
	if got := len(n3.Dispatches()); got != 1 {
		t.Fatalf("third connection received %d dispatches, want 1 (replay after two drops)", got)
	}
	got := n3.Dispatches()
	if got[0].ConfigVersion != disp.ConfigVersion {
		t.Fatalf("replayed config version = %d, want %d", got[0].ConfigVersion, disp.ConfigVersion)
	}
}
