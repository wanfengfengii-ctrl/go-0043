package service_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/edgeflag/edgeflag-wave-publisher/internal/protocol"
	"github.com/edgeflag/edgeflag-wave-publisher/pkg/testkit"
	"github.com/edgeflag/edgeflag-wave-publisher/pkg/transport"
	"github.com/edgeflag/edgeflag-wave-publisher/pkg/vnode"
)

func decodeWireFrames(t *testing.T, wire transport.Wire) []protocol.Frame {
	t.Helper()
	chunk, err := wire.ReadChunk()
	if err != nil {
		t.Fatalf("read wire: %v", err)
	}
	var frames []protocol.Frame
	decoder := protocol.NewDecoder(protocol.DefaultMaxFrame, func(frame protocol.Frame) error {
		frames = append(frames, frame)
		return nil
	})
	if err := decoder.Feed(chunk); err != nil {
		t.Fatalf("decode wire: %v", err)
	}
	return frames
}

func TestReplayPendingFramesOnReconnect(t *testing.T) {
	h := testkit.NewHarness(t)
	ctx := context.Background()

	oldNodeWire, oldServiceWire := vnode.Connect()
	oldSession, err := h.Service.AcceptSession(ctx, "n1", oldServiceWire)
	if err != nil {
		t.Fatalf("accept old session: %v", err)
	}

	payloads := []protocol.DispatchPayload{
		{RolloutID: "r1", ConfigVersion: 1, Attempt: 1, Direction: "forward", Content: `{"f":1}`},
		{RolloutID: "r2", ConfigVersion: 2, Attempt: 1, Direction: "forward", Content: `{"f":2}`},
		{RolloutID: "r3", ConfigVersion: 3, Attempt: 1, Direction: "forward", Content: `{"f":3}`},
	}
	for _, payload := range payloads {
		if _, err := oldSession.Send(protocol.MsgDispatch, payload); err != nil {
			t.Fatalf("send pending frame: %v", err)
		}
	}
	oldFrames := decodeWireFrames(t, oldNodeWire)
	if len(oldFrames) != len(payloads) {
		t.Fatalf("old connection received %d frames, want %d", len(oldFrames), len(payloads))
	}
	if err := oldSession.AckPendingFrame(ctx, int64(oldFrames[0].Sequence)); err != nil {
		t.Fatalf("ack first frame: %v", err)
	}
	h.Service.CloseSession(ctx, oldSession)

	newNodeWire, newServiceWire := vnode.Connect()
	if _, err := h.Service.AcceptSession(ctx, "n1", newServiceWire); err != nil {
		t.Fatalf("accept new session: %v", err)
	}
	newFrames := decodeWireFrames(t, newNodeWire)
	if len(newFrames) != len(oldFrames)-1 {
		t.Fatalf("new connection received %d frames, want %d", len(newFrames), len(oldFrames)-1)
	}
	for i, got := range newFrames {
		want := oldFrames[i+1]
		if got.Sequence != want.Sequence || got.MessageType != want.MessageType || !bytes.Equal(got.Payload, want.Payload) {
			t.Fatalf("replayed frame %d = seq %d type %d payload %s, want seq %d type %d payload %s", i, got.Sequence, got.MessageType, got.Payload, want.Sequence, want.MessageType, want.Payload)
		}
	}
}
