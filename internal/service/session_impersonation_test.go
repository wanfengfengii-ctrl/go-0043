package service_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/edgeflag/edgeflag-wave-publisher/internal/apperr"
	"github.com/edgeflag/edgeflag-wave-publisher/internal/domain"
	"github.com/edgeflag/edgeflag-wave-publisher/internal/protocol"
	"github.com/edgeflag/edgeflag-wave-publisher/internal/service"
	"github.com/edgeflag/edgeflag-wave-publisher/pkg/testkit"
	"github.com/edgeflag/edgeflag-wave-publisher/pkg/vnode"
)

// TestSessionAckRejectsImpersonatedNodeID covers the cross-node impersonation
// regression on the streaming session channel. A session bound to node A must
// not be able to advance publication progress by submitting an ack whose
// payload claims to be from node B. When B is a current-stage target and A is
// not, such a frame used to be recorded as B's valid confirmation and could
// drive the stage into observing (and onward to completion). The session layer
// now rejects the frame at the identity boundary; only an ack whose node_id
// matches the session's bound node may advance progress.
func TestSessionAckRejectsImpersonatedNodeID(t *testing.T) {
	h := testkit.NewHarness(t)
	ctx := context.Background()

	// Config version 1 from baseline 0.
	cfg, _, err := h.Service.CreateConfig(ctx, service.CreateConfigRequest{Baseline: 0, Content: `{"f":1}`, RequestKey: "c1"})
	if err != nil {
		t.Fatalf("create config: %v", err)
	}
	// Two nodes in group g1 (zone=a). Sorted membership is [n1, n2].
	for _, id := range []string{"n1", "n2"} {
		if err := h.Service.UpsertNode(ctx, domain.Node{ID: id, Labels: map[string]string{"zone": "a"}, Online: true}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := h.Service.UpsertGroup(ctx, domain.Group{ID: "g1", Selector: domain.LabelSelector{MatchLabels: map[string]string{"zone": "a"}}}); err != nil {
		t.Fatal(err)
	}

	// n1 is the current-stage target (it gets dispatched). It refuses to ack so
	// the stage stays dispatching with success count 0, leaving room for the
	// impersonation attempt to be the only thing that could advance it. n2 is
	// the impersonator: it is connected but never targeted by this stage.
	sess1, n1 := connectNode(t, h, "n1", vnode.Behavior{RefuseForward: true})
	sess2, n2 := connectNode(t, h, "n2", vnode.Behavior{})

	// One stage targeting count=1 -> node snapshot [n1]. n1 is a target; n2 is not.
	r, err := h.Service.CreateRollout(ctx, service.CreateRolloutRequest{
		ID: "r1", TargetConfigVersion: cfg.Version, GroupID: "g1",
		Stages: []service.StageSpec{{
			Name: "s0", Target: domain.StageTarget{Kind: domain.TargetCount, Count: 1},
			MinSuccessRate: 1.0, MaxFailures: 0,
			AckTimeout: time.Minute, ObservationWindow: 10 * time.Second,
		}},
	})
	if err != nil {
		t.Fatalf("create rollout: %v", err)
	}
	if _, err := h.Service.StartRollout(ctx, r.ID); err != nil {
		t.Fatalf("start rollout: %v", err)
	}
	// Pump so n1 receives its dispatch; n1 refuses to ack.
	pumpQuiescent(t, sess1, n1)
	if got := len(n1.Dispatches()); got != 1 {
		t.Fatalf("n1 got %d dispatches, want 1", got)
	}

	// --- Impersonation attempt ---
	// n2's session (bound to n2) sends an ack whose payload claims to be from
	// n1, the current-stage target. Sequence 1 is the next in-order sequence
	// for n2's fresh session.
	if err := n2.SendRaw(ackFrame("r1", "n1", cfg.Version, 1, domain.AckConfirmed, 1)); err != nil {
		t.Fatalf("send impersonated ack: %v", err)
	}
	_, perr := sess2.PumpIn()
	if !apperr.HasCode(perr, apperr.CodeNodeMismatch) {
		t.Fatalf("impersonated ack: expected node_mismatch error, got %v", perr)
	}

	// Business state must be untouched: the stage is still dispatching, no
	// success has been recorded, and no confirmed ack exists for n1.
	r2, err := h.Service.GetRollout(ctx, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if r2.Stages[0].State != domain.StageDispatching {
		t.Fatalf("after impersonation: stage state = %s, want dispatching", r2.Stages[0].State)
	}
	if r2.Stages[0].SuccessCount != 0 {
		t.Fatalf("after impersonation: success count = %d, want 0", r2.Stages[0].SuccessCount)
	}
	confirmed, err := h.Store.ConfirmedNodeIDs(ctx, r.ID, 1, cfg.Version)
	if err != nil {
		t.Fatal(err)
	}
	if len(confirmed) != 0 {
		t.Fatalf("after impersonation: confirmed nodes = %v, want none", confirmed)
	}
	// The rejected frame must not advance n2's session sequence watermark.
	sessRow, err := h.Store.GetSession(ctx, sess2.SessionID())
	if err != nil {
		t.Fatal(err)
	}
	if sessRow.ExpectedSeq != 1 {
		t.Fatalf("after impersonation: n2 expected_seq = %d, want 1 (watermark must not advance)", sessRow.ExpectedSeq)
	}

	// --- Legitimate ack still works ---
	// An ack whose node_id matches the session's bound node (n1 sent over n1's
	// own session) must still advance progress, proving the identity check does
	// not reject well-formed confirmations.
	if err := n1.SendRaw(ackFrame("r1", "n1", cfg.Version, 1, domain.AckConfirmed, 1)); err != nil {
		t.Fatalf("send legitimate ack: %v", err)
	}
	if _, err := sess1.PumpIn(); err != nil {
		t.Fatalf("legitimate ack: unexpected error: %v", err)
	}
	pumpQuiescent(t, sess1, n1)

	r3, err := h.Service.GetRollout(ctx, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if r3.Stages[0].State != domain.StageObserving {
		t.Fatalf("after legitimate ack: stage state = %s, want observing", r3.Stages[0].State)
	}
	if r3.Stages[0].SuccessCount != 1 {
		t.Fatalf("after legitimate ack: success count = %d, want 1", r3.Stages[0].SuccessCount)
	}
}

// ackFrame builds a MsgAck frame carrying the given acknowledgement fields at
// the given incoming sequence number. The frame is returned unencoded; the
// caller passes it to VNode.SendRaw, which encodes and writes it.
func ackFrame(rolloutID, nodeID string, cfgVer domain.ConfigVersion, attempt int, level domain.AckLevel, seq uint64) protocol.Frame {
	ap := protocol.AckPayload{
		RolloutID:     rolloutID,
		NodeID:        nodeID,
		ConfigVersion: int64(cfgVer),
		Attempt:       attempt,
		Level:         int(level),
		OK:            true,
	}
	payload, err := json.Marshal(ap)
	if err != nil {
		panic(err)
	}
	return protocol.Frame{
		Major:       protocol.SupportedMajor,
		Minor:       protocol.CurrentMinor,
		SessionID:   0,
		Sequence:    seq,
		MessageType: protocol.MsgAck,
		Payload:     payload,
	}
}
