package service_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/edgeflag/edgeflag-wave-publisher/internal/domain"
	"github.com/edgeflag/edgeflag-wave-publisher/internal/service"
	"github.com/edgeflag/edgeflag-wave-publisher/internal/store"
	"github.com/edgeflag/edgeflag-wave-publisher/pkg/clock"
	"github.com/edgeflag/edgeflag-wave-publisher/pkg/scheduler"
)

func TestRecoverCompletesFinalObservingStage(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "edgeflag.db")
	baseTime := time.Unix(1_700_000_000, 0)

	initialStore, err := store.Open(path)
	if err != nil {
		t.Fatalf("open initial store: %v", err)
	}
	t.Cleanup(func() { _ = initialStore.Close() })
	initialClock := clock.NewFixed(baseTime)
	initialService := service.New(initialStore, scheduler.NewManual(initialClock), initialClock, nil)

	cfg, _, err := initialService.CreateConfig(ctx, service.CreateConfigRequest{
		Baseline: 0, Content: `{"f":1}`, RequestKey: "recover-final-stage",
	})
	if err != nil {
		t.Fatalf("create config: %v", err)
	}
	if _, err := initialService.UpsertGroup(ctx, domain.Group{ID: "empty"}); err != nil {
		t.Fatalf("create group: %v", err)
	}
	r, err := initialService.CreateRollout(ctx, service.CreateRolloutRequest{
		ID: "recover-final", TargetConfigVersion: cfg.Version, GroupID: "empty",
		Stages: []service.StageSpec{{
			Name: "final", Target: domain.StageTarget{Kind: domain.TargetCount, Count: 0},
			MinSuccessRate: 1, MaxFailures: 0, ObservationWindow: time.Second,
		}},
	})
	if err != nil {
		t.Fatalf("create rollout: %v", err)
	}
	if _, err := initialService.StartRollout(ctx, r.ID); err != nil {
		t.Fatalf("start rollout: %v", err)
	}
	persisted, err := initialService.GetRollout(ctx, r.ID)
	if err != nil {
		t.Fatalf("load observing rollout: %v", err)
	}
	if persisted.Stages[0].State != domain.StageObserving {
		t.Fatalf("stage state = %s, want observing", persisted.Stages[0].State)
	}
	if err := initialStore.Close(); err != nil {
		t.Fatalf("close initial store: %v", err)
	}

	recoveredStore, err := store.Open(path)
	if err != nil {
		t.Fatalf("open recovered store: %v", err)
	}
	t.Cleanup(func() { _ = recoveredStore.Close() })
	recoveredClock := clock.NewFixed(baseTime)
	recoveredScheduler := scheduler.NewManual(recoveredClock)
	recoveredService := service.New(recoveredStore, recoveredScheduler, recoveredClock, nil)
	if err := recoveredService.Recover(ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}

	done := make(chan struct{})
	go func() {
		recoveredScheduler.Advance(recoveredClock.Now().Add(time.Second))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("recovery scheduler blocked completing final observing stage")
	}

	completed, err := recoveredService.GetRollout(ctx, r.ID)
	if err != nil {
		t.Fatalf("load completed rollout: %v", err)
	}
	if completed.State != domain.StateCompleted {
		t.Fatalf("rollout state = %s, want completed", completed.State)
	}
}
