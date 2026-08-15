package service_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/edgeflag/edgeflag-wave-publisher/internal/domain"
	"github.com/edgeflag/edgeflag-wave-publisher/internal/service"
	"github.com/edgeflag/edgeflag-wave-publisher/pkg/testkit"
)

// runWithDeadline runs fn in a goroutine and fails the test (rather than hanging
// forever) if it does not return within d. The recover observe-end path used to
// re-enter the non-reentrant rollout mutex and deadlock; without this guard a
// reintroduction of that bug would hang the whole test binary until the Go test
// timeout. With it, the regression surfaces as a fast, readable failure.
func runWithDeadline(t *testing.T, name string, d time.Duration, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		fn()
		close(done)
	}()
	select {
	case <-done:
		return
	case <-time.After(d):
		t.Fatalf("%s did not return within %v (scheduler blocked; likely re-entrant rollout-lock deadlock in the recover observe-end path)", name, d)
	}
}

// ackConfirmed injects a confirmed-level ack for a node in the current attempt.
// It avoids the session/wire machinery entirely: the deterministic merge rule
// only needs the persisted ack row, so driving ProcessAck directly is both
// simpler and more reproducible than pumping virtual nodes.
func ackConfirmed(t *testing.T, svc *service.Service, rolloutID, nodeID string, cv domain.ConfigVersion, attempt int, seq int64) {
	t.Helper()
	out, err := svc.ProcessAck(context.Background(), domain.Ack{
		RolloutID:     rolloutID,
		NodeID:        nodeID,
		ConfigVersion: cv,
		Attempt:       attempt,
		Level:         domain.AckConfirmed,
		Seq:           seq,
	})
	if err != nil {
		t.Fatalf("ProcessAck(%s): %v", nodeID, err)
	}
	if !out.Applied {
		t.Fatalf("ProcessAck(%s) did not apply progress (outcome=%+v)", nodeID, out)
	}
}

// driveForwardToObserving advances a freshly-started single-stage rollout until
// every node in its snapshot has confirmed, leaving the stage in the observing
// state without advancing the clock past the observation window. The clock is
// left at its base time so a recovered instance can re-arm the observe-end timer
// deterministically.
func driveForwardToObserving(t *testing.T, svc *service.Service, rolloutID string, targetCV domain.ConfigVersion, attempt int, snapshot []string) {
	t.Helper()
	ctx := context.Background()
	// Start opened stage 0 and dispatched the first node; dispatch the rest.
	for range snapshot {
		svc.DispatchTick(ctx, rolloutID, 0)
	}
	for i, nodeID := range snapshot {
		ackConfirmed(t, svc, rolloutID, nodeID, targetCV, attempt, int64(i+1))
	}
	r, err := svc.GetRollout(ctx, rolloutID)
	if err != nil {
		t.Fatalf("GetRollout: %v", err)
	}
	if r.State != domain.StateRunning {
		t.Fatalf("rollout state = %s, want running before restart", r.State)
	}
	if r.Stages[0].State != domain.StageObserving {
		t.Fatalf("stage state = %s, want observing", r.Stages[0].State)
	}
}

// newRecoveryFile creates a file-backed harness over a fresh temp db path and
// returns the path so a second harness can reopen the same file to simulate a
// service restart.
func newRecoveryFile(t *testing.T) (*testkit.Harness, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "edgeflag.db")
	return testkit.NewFileHarness(t, path), path
}

// TestRecoverObservingStageCompletesForward is the core regression for the
// restart-blocks-at-observation-end bug. A single-stage forward rollout is
// driven into the observing state, the process "restarts" (store closed and
// reopened with a fresh scheduler and lock map), Recover re-arms the observe-end
// task, and advancing the scheduler to the end of the observation window must
// complete the rollout instead of deadlocking the recover task forever.
func TestRecoverObservingStageCompletesForward(t *testing.T) {
	ctx := context.Background()
	h1, path := newRecoveryFile(t)

	cfg, _, err := h1.Service.CreateConfig(ctx, service.CreateConfigRequest{Baseline: 0, Content: `{"f":1}`, RequestKey: "c1"})
	if err != nil {
		t.Fatalf("create config: %v", err)
	}
	for _, id := range []string{"n1", "n2"} {
		if err := h1.Service.UpsertNode(ctx, domain.Node{ID: id, Labels: map[string]string{"zone": "a"}, Online: true}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := h1.Service.UpsertGroup(ctx, domain.Group{ID: "g1", Selector: domain.LabelSelector{MatchLabels: map[string]string{"zone": "a"}}}); err != nil {
		t.Fatal(err)
	}

	const obsWindow = 10 * time.Second
	r, err := h1.Service.CreateRollout(ctx, service.CreateRolloutRequest{
		ID: "r1", TargetConfigVersion: cfg.Version, GroupID: "g1",
		Stages: []service.StageSpec{{
			Name: "s0", Target: domain.StageTarget{Kind: domain.TargetCount, Count: 2},
			MinSuccessRate: 1.0, MaxFailures: 0,
			AckTimeout: time.Minute, ObservationWindow: obsWindow,
		}},
	})
	if err != nil {
		t.Fatalf("create rollout: %v", err)
	}
	if _, err := h1.Service.StartRollout(ctx, r.ID); err != nil {
		t.Fatalf("start rollout: %v", err)
	}
	driveForwardToObserving(t, h1.Service, r.ID, cfg.Version, 1, []string{"n1", "n2"})

	// Confirm the persisted pre-crash state: rollout running, stage observing.
	before, _ := h1.Service.GetRollout(ctx, r.ID)
	if before.Stages[0].State != domain.StageObserving || before.State != domain.StateRunning {
		t.Fatalf("pre-restart state unexpected: rollout=%s stage=%s", before.State, before.Stages[0].State)
	}

	// Simulate a process restart: close the store, then reopen the same file with
	// a fresh service, scheduler and per-rollout lock map.
	if err := h1.Store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	h2 := testkit.NewFileHarness(t, path)

	// Recover must re-arm the observe-end task without error and without
	// advancing any stage (stages are restored in their persisted state).
	if err := h2.Service.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	restored, _ := h2.Service.GetRollout(ctx, r.ID)
	if restored.Stages[0].State != domain.StageObserving || restored.State != domain.StateRunning {
		t.Fatalf("post-Recover state changed: rollout=%s stage=%s", restored.State, restored.Stages[0].State)
	}
	if got := h2.Sched.Pending(); got != 1 {
		t.Fatalf("post-Recover pending tasks = %d, want 1 (the observe-end timer)", got)
	}

	// Advancing the scheduler to the end of the observation window fires the
	// recovered observe-end task. Previously this deadlocked because
	// recoverObservingStage held the rollout lock and then called
	// completeAttempt, which tried to re-acquire the non-reentrant mutex. The
	// deadline turns a hang into a fast failure.
	runWithDeadline(t, "advance past observation window", 5*time.Second, func() {
		h2.AdvanceTime(obsWindow + time.Second)
	})

	r2, err := h2.Service.GetRollout(ctx, r.ID)
	if err != nil {
		t.Fatalf("GetRollout after recover: %v", err)
	}
	if r2.State != domain.StateCompleted {
		t.Fatalf("rollout state = %s, want completed after recover", r2.State)
	}
	if r2.Stages[0].State != domain.StageObserving {
		t.Fatalf("stage state = %s, want observing (recovery completes the attempt, not the stage)", r2.Stages[0].State)
	}
	if got := h2.Sched.Pending(); got != 0 {
		t.Fatalf("pending tasks = %d, want 0 after completion (scheduler must not be blocked)", got)
	}
}

// seedRollbackObserving writes a rollout directly to the store whose current
// attempt is a rollback and whose single stage is already observing, then
// returns the rollout id and observation window. Constructing the state at the
// store level keeps this test focused on the Recover completion path for a
// rollback-direction attempt, which is the branch the forward test does not
// reach.
func seedRollbackObserving(t *testing.T, h *testkit.Harness) (string, time.Duration) {
	t.Helper()
	ctx := context.Background()
	// v1 is the rollback target config.
	cfg := testkit.SeedConfig(t, h.Store, 0, `{"f":1}`, "c1")
	const obsWindow = 5 * time.Second
	now := h.Clock.Now()
	r := domain.Rollout{
		ID:                    "r1",
		TargetConfigVersion:   cfg.Version,
		BaselineConfigVersion: cfg.Version,
		PlanRevision:          1,
		State:                 domain.StateRollingBack,
		CurrentAttempt:        1,
		GroupID:               "g1",
		CreatedAt:             now,
		UpdatedAt:             now,
		Attempts: []domain.Attempt{{
			Index:                1,
			Direction:            domain.AttemptRollback,
			TargetConfigVersion:  cfg.Version,
			BaselineConfigVersion: cfg.Version,
			CreatedAt:            now,
		}},
		Stages: []domain.Stage{{
			Index:             0,
			Name:              "rollback",
			Target:            domain.StageTarget{Kind: domain.TargetCount, Count: 2},
			MinSuccessRate:    1.0,
			MaxFailures:       0,
			AckTimeout:        time.Minute,
			ObservationWindow: obsWindow,
			State:             domain.StageObserving,
			NodeSnapshot:      []string{"n1", "n2"},
			OpenedAt:          now,
		}},
	}
	if err := h.Store.SaveRollout(ctx, r, 0); err != nil {
		t.Fatalf("seed rollback rollout: %v", err)
	}
	return r.ID, obsWindow
}

// TestRecoverObservingStageCompletesRollback covers the "correct terminal state
// per attempt direction" requirement: a recovered rollback attempt whose stage
// has finished observing must reach rolled_back, not completed, and must not
// deadlock the recover task. The persisted state is seeded at the store level so
// the test exercises the Recover completion path for a rollback-direction
// attempt regardless of how the rollback was originally driven.
func TestRecoverObservingStageCompletesRollback(t *testing.T) {
	ctx := context.Background()
	h1, path := newRecoveryFile(t)
	rolloutID, obsWindow := seedRollbackObserving(t, h1)

	// Simulate a process restart.
	if err := h1.Store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	h2 := testkit.NewFileHarness(t, path)

	if err := h2.Service.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	restored, _ := h2.Service.GetRollout(ctx, rolloutID)
	if restored.State != domain.StateRollingBack || restored.Stages[0].State != domain.StageObserving {
		t.Fatalf("post-Recover state changed: rollout=%s stage=%s", restored.State, restored.Stages[0].State)
	}
	if got := h2.Sched.Pending(); got != 1 {
		t.Fatalf("post-Recover pending tasks = %d, want 1 (the observe-end timer)", got)
	}

	runWithDeadline(t, "advance past rollback observation window", 5*time.Second, func() {
		h2.AdvanceTime(obsWindow + time.Second)
	})

	r2, err := h2.Service.GetRollout(ctx, rolloutID)
	if err != nil {
		t.Fatalf("GetRollout after recover: %v", err)
	}
	if r2.State != domain.StateRolledBack {
		t.Fatalf("rollout state = %s, want rolled_back after recover of a rollback attempt", r2.State)
	}
	if got := h2.Sched.Pending(); got != 0 {
		t.Fatalf("pending tasks = %d, want 0 after rollback completion (scheduler must not be blocked)", got)
	}
}
