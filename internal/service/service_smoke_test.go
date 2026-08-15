package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/edgeflag/edgeflag-wave-publisher/internal/domain"
	"github.com/edgeflag/edgeflag-wave-publisher/internal/service"
	"github.com/edgeflag/edgeflag-wave-publisher/pkg/testkit"
	"github.com/edgeflag/edgeflag-wave-publisher/pkg/vnode"
)

// pumpQuiescent pumps the node and session until both produce no new frames.
// This is the deterministic replacement for sleeping on a real network.
func pumpQuiescent(t *testing.T, sess *service.Session, node *vnode.VNode) {
	t.Helper()
	for i := 0; i < 50; i++ {
		n1, _ := node.Pump()
		n2, _ := sess.PumpIn()
		if n1 == 0 && n2 == 0 {
			return
		}
	}
	t.Fatal("did not reach quiescence within 50 pump rounds")
}

// connectNode registers a node, creates a wire pair, accepts a session and
// builds a virtual node on the other end.
func connectNode(t *testing.T, h *testkit.Harness, nodeID string, beh vnode.Behavior) (*service.Session, *vnode.VNode) {
	t.Helper()
	ctx := context.Background()
	nodeWire, svcWire := vnode.Connect()
	sess, err := h.Service.AcceptSession(ctx, nodeID, svcWire)
	if err != nil {
		t.Fatalf("accept session: %v", err)
	}
	node := vnode.New(nodeID, nodeWire, beh)
	return sess, node
}

func TestSmokeRolloutDispatchAckAdvance(t *testing.T) {
	h := testkit.NewHarness(t)
	ctx := context.Background()

	// Seed a config from baseline 0.
	cfg, _, err := h.Service.CreateConfig(ctx, service.CreateConfigRequest{Baseline: 0, Content: `{"f":1}`, RequestKey: "c1"})
	if err != nil {
		t.Fatalf("create config: %v", err)
	}
	// Register two nodes.
	for _, id := range []string{"n1", "n2"} {
		if err := h.Service.UpsertNode(ctx, domain.Node{ID: id, Labels: map[string]string{"zone": "a"}, Online: true}); err != nil {
			t.Fatal(err)
		}
	}
	// Group matching both nodes.
	_, err = h.Service.UpsertGroup(ctx, domain.Group{ID: "g1", Selector: domain.LabelSelector{MatchLabels: map[string]string{"zone": "a"}}})
	if err != nil {
		t.Fatal(err)
	}
	// Connect virtual nodes.
	sess1, n1 := connectNode(t, h, "n1", vnode.Behavior{AckLevel: domain.AckConfirmed})
	sess2, n2 := connectNode(t, h, "n2", vnode.Behavior{AckLevel: domain.AckConfirmed})
	_ = sess1
	_ = sess2

	// Create + start rollout with one stage targeting all nodes.
	r, err := h.Service.CreateRollout(ctx, service.CreateRolloutRequest{
		ID: "r1", TargetConfigVersion: cfg.Version, GroupID: "g1",
		Stages: []service.StageSpec{{
			Name: "s0", Target: domain.StageTarget{Kind: domain.TargetCount, Count: 2},
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
	// Start opens stage 0 and dispatches the first node. Dispatch the second.
	h.Service.DispatchTick(ctx, r.ID, 0)

	// Pump so the nodes receive dispatches and ack.
	pumpQuiescent(t, sess1, n1)
	pumpQuiescent(t, sess2, n2)

	// Both nodes should have received exactly one dispatch.
	if got := len(n1.Dispatches()); got != 1 {
		t.Fatalf("n1 got %d dispatches, want 1", got)
	}
	if got := len(n2.Dispatches()); got != 1 {
		t.Fatalf("n2 got %d dispatches, want 1", got)
	}
	// Pump the service session to process the acks.
	pumpQuiescent(t, sess1, n1)
	pumpQuiescent(t, sess2, n2)

	// After acks, the stage should be observing (success) and, after the
	// observation window, the rollout completes.
	r2, err := h.Service.GetRollout(ctx, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if r2.Stages[0].State != domain.StageObserving {
		t.Fatalf("stage state = %s, want observing", r2.Stages[0].State)
	}
	// Advance past the observation window.
	h.AdvanceTime(11 * time.Second)
	r3, _ := h.Service.GetRollout(ctx, r.ID)
	if r3.State != domain.StateCompleted {
		t.Fatalf("rollout state = %s, want completed", r3.State)
	}
}
