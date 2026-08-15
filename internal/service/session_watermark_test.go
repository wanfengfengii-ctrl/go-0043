package service_test

import (
	"context"
	"testing"

	"github.com/edgeflag/edgeflag-wave-publisher/internal/protocol"
	"github.com/edgeflag/edgeflag-wave-publisher/pkg/testkit"
	"github.com/edgeflag/edgeflag-wave-publisher/pkg/vnode"
)

func TestSessionPersistsCommittedInboundWatermark(t *testing.T) {
	h := testkit.NewHarness(t)
	ctx := context.Background()

	nodeWire, serviceWire := vnode.Connect()
	sess, err := h.Service.AcceptSession(ctx, "node-watermark", serviceWire)
	if err != nil {
		t.Fatalf("accept session: %v", err)
	}
	node := vnode.New("node-watermark", nodeWire, vnode.Behavior{})

	assertWatermarks := func(sessionID, committed, expected int64) {
		t.Helper()
		got, err := h.Store.GetSession(ctx, sessionID)
		if err != nil {
			t.Fatalf("get session %d: %v", sessionID, err)
		}
		if got.LastCommittedSeq != committed || got.ExpectedSeq != expected {
			t.Fatalf("session %d watermarks = (committed %d, expected %d), want (%d, %d)",
				sessionID, got.LastCommittedSeq, got.ExpectedSeq, committed, expected)
		}
	}

	for seq := uint64(1); seq <= 2; seq++ {
		if err := node.SendRaw(protocol.Frame{
			SessionID:   uint64(sess.SessionID()),
			Sequence:    seq,
			MessageType: protocol.MsgHeartbeat,
		}); err != nil {
			t.Fatalf("send heartbeat %d: %v", seq, err)
		}
		if _, err := sess.PumpIn(); err != nil {
			t.Fatalf("process heartbeat %d: %v", seq, err)
		}
		assertWatermarks(sess.SessionID(), int64(seq), int64(seq+1))
	}

	_, reconnectWire := vnode.Connect()
	resumed, err := h.Service.AcceptSession(ctx, "node-watermark", reconnectWire)
	if err != nil {
		t.Fatalf("resume session: %v", err)
	}
	assertWatermarks(resumed.SessionID(), 2, 3)
}
