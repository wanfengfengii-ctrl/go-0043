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

// multiNodeFixture seeds a config, registers count online nodes (n1, n2, ...) in
// a single group, and connects virtual nodes that acknowledge every dispatch at
// AckConfirmed. It returns the config version and the sessions/virtual nodes in
// node-snapshot order so a test can assert on dispatch counts deterministically.
func multiNodeFixture(t *testing.T, h *testkit.Harness, count int) (domain.ConfigVersion, []*service.Session, []*vnode.VNode) {
	t.Helper()
	ctx := context.Background()
	cfg, _, err := h.Service.CreateConfig(ctx, service.CreateConfigRequest{Baseline: 0, Content: `{"f":1}`, RequestKey: "c1"})
	if err != nil {
		t.Fatalf("create config: %v", err)
	}
	ids := make([]string, count)
	for i := 0; i < count; i++ {
		ids[i] = "n" + itoa(i+1)
		if err := h.Service.UpsertNode(ctx, domain.Node{ID: ids[i], Labels: map[string]string{"zone": "a"}, Online: true}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := h.Service.UpsertGroup(ctx, domain.Group{ID: "g1", Selector: domain.LabelSelector{MatchLabels: map[string]string{"zone": "a"}}}); err != nil {
		t.Fatal(err)
	}
	sessions := make([]*service.Session, count)
	nodes := make([]*vnode.VNode, count)
	for i := 0; i < count; i++ {
		sessions[i], nodes[i] = connectNode(t, h, ids[i], vnode.Behavior{AckLevel: domain.AckConfirmed})
	}
	return cfg.Version, sessions, nodes
}

// itoa converts a small positive int to its decimal string.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// pumpAll pumps every session/node pair to quiescence, iterating until none of
// them produce new frames. This drives dispatch delivery and ack processing
// across all nodes in lockstep, which is what the incremental-dispatch handoff
// relies on (one node's ack dispatching the next node).
func pumpAll(t *testing.T, sessions []*service.Session, nodes []*vnode.VNode) {
	t.Helper()
	for i := 0; i < 100; i++ {
		var produced int
		for j := range sessions {
			n1, _ := nodes[j].Pump()
			n2, _ := sessions[j].PumpIn()
			produced += n1 + n2
		}
		if produced == 0 {
			return
		}
	}
	t.Fatal("did not reach quiescence within 100 pump rounds")
}

// TestMultiNodeDispatchNoManualTick verifies that a two-node stage started
// normally dispatches every target node in sequence without ever calling the
// test-only DispatchTick hook. After the first node confirms, the second must
// receive its dispatch automatically, the stage must leave "dispatching" and the
// rollout must reach a subsequent state (observing -> completed). This is the
// regression test for the stall where only the first node was dispatched and
// progress stuck at 1.
func TestMultiNodeDispatchNoManualTick(t *testing.T) {
	h := testkit.NewHarness(t)
	ctx := context.Background()
	cfg, sessions, nodes := multiNodeFixture(t, h, 2)

	r, err := h.Service.CreateRollout(ctx, service.CreateRolloutRequest{
		ID: "r1", TargetConfigVersion: cfg, GroupID: "g1",
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
	// Start opens the stage and dispatches only the first node. Pumping must
	// propagate that dispatch, receive n1's ack and — via the incremental
	// dispatch handoff — dispatch n2, all without a manual tick.
	pumpAll(t, sessions, nodes)

	// Both nodes must have received exactly one dispatch: n1 from openStage and
	// n2 from the ack-driven incremental handoff.
	for i, n := range nodes {
		if got := len(n.Dispatches()); got != 1 {
			t.Fatalf("node n%d got %d dispatches, want 1", i+1, got)
		}
	}
	// Both acks are now in; the stage must have advanced out of dispatching.
	r2, err := h.Service.GetRollout(ctx, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if r2.Stages[0].State != domain.StageObserving {
		t.Fatalf("stage state = %s, want observing (cursor=%d)", r2.Stages[0].State, r2.Stages[0].DispatchCursor)
	}
	if r2.Stages[0].DispatchCursor != 2 {
		t.Fatalf("dispatch cursor = %d, want 2", r2.Stages[0].DispatchCursor)
	}
	// Advancing past the observation window completes the rollout, proving the
	// stage progressed into a subsequent state rather than stalling at 1.
	h.AdvanceTime(11 * time.Second)
	r3, _ := h.Service.GetRollout(ctx, r.ID)
	if r3.State != domain.StateCompleted {
		t.Fatalf("rollout state = %s, want completed", r3.State)
	}
}

// TestMultiNodeDispatchRecovery verifies that after recovery the incremental
// dispatch handoff still covers every target node and the stage advances,
// without any manual dispatch tick. A three-node stage is used so that, after
// Recover re-arms the scheduler and dispatches the second node, the third node
// must still be reached via the ack-driven handoff — the exact behaviour that
// stalled before the fix.
func TestMultiNodeDispatchRecovery(t *testing.T) {
	h := testkit.NewHarness(t)
	ctx := context.Background()
	cfg, sessions, nodes := multiNodeFixture(t, h, 3)

	r, err := h.Service.CreateRollout(ctx, service.CreateRolloutRequest{
		ID: "r1", TargetConfigVersion: cfg, GroupID: "g1",
		Stages: []service.StageSpec{{
			Name: "s0", Target: domain.StageTarget{Kind: domain.TargetCount, Count: 3},
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
	// Only the first node has been dispatched (cursor=1). Simulate a restart:
	// Recover reloads persisted state and re-arms the scheduler, scheduling an
	// incremental-dispatch task for the undispatched nodes.
	progress, _ := h.Service.GetProgress(ctx, r.ID)
	if progress.Stages[0].Dispatched != 1 {
		t.Fatalf("pre-recover dispatched = %d, want 1", progress.Stages[0].Dispatched)
	}
	if err := h.Service.Recover(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}
	// Fire the re-armed dispatch task: this dispatches the second node
	// (cursor 1 -> 2). The third node remains undispatched and must be reached
	// by the ack-driven handoff when the first node acknowledges.
	h.AdvanceTime(0)
	recovered, _ := h.Service.GetProgress(ctx, r.ID)
	if recovered.Stages[0].Dispatched != 2 {
		t.Fatalf("post-recover dispatched = %d, want 2", recovered.Stages[0].Dispatched)
	}
	if recovered.Stages[0].State != domain.StageDispatching {
		t.Fatalf("post-recover stage state = %s, want dispatching", recovered.Stages[0].State)
	}

	// Pump all nodes. n1's ack must dispatch n3 (the remaining node); n2 and n3
	// then acknowledge and the stage must advance to observing. Without the fix
	// n3 would never be dispatched and the stage would stall at cursor 2.
	pumpAll(t, sessions, nodes)
	for i, n := range nodes {
		if got := len(n.Dispatches()); got != 1 {
			t.Fatalf("node n%d got %d dispatches, want 1", i+1, got)
		}
	}
	r2, _ := h.Service.GetRollout(ctx, r.ID)
	if r2.Stages[0].State != domain.StageObserving {
		t.Fatalf("post-recover stage state = %s, want observing (cursor=%d)", r2.Stages[0].State, r2.Stages[0].DispatchCursor)
	}
	if r2.Stages[0].DispatchCursor != 3 {
		t.Fatalf("post-recover dispatch cursor = %d, want 3", r2.Stages[0].DispatchCursor)
	}
	h.AdvanceTime(11 * time.Second)
	r3, _ := h.Service.GetRollout(ctx, r.ID)
	if r3.State != domain.StateCompleted {
		t.Fatalf("post-recover rollout state = %s, want completed", r3.State)
	}
}
