package store

import (
	"context"
	"testing"
	"time"

	"github.com/edgeflag/edgeflag-wave-publisher/internal/domain"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestStoreCreateConfigIdempotent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Unix(1700000000, 0)

	r1, err := s.CreateConfig(ctx, 0, `{"f":"a"}`, "req-1", "tester", now)
	if err != nil {
		t.Fatalf("create first: %v", err)
	}
	if !r1.Created || r1.Config.Version != 1 {
		t.Fatalf("expected created version 1, got %+v", r1)
	}
	// Replay same request key: no new version.
	r2, err := s.CreateConfig(ctx, 0, `{"f":"a"}`, "req-1", "tester", now)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if r2.Created || r2.Config.Version != 1 {
		t.Fatalf("expected idempotent return of version 1, got %+v", r2)
	}
}

func TestStoreCreateConfigStaleBaseline(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Unix(1700000000, 0)
	if _, err := s.CreateConfig(ctx, 0, `{"f":"a"}`, "req-1", "t", now); err != nil {
		t.Fatal(err)
	}
	// Baseline 0 is now stale (head is 1).
	_, err := s.CreateConfig(ctx, 0, `{"f":"b"}`, "req-2", "t", now)
	if !IsStaleBaseline(err) {
		t.Fatalf("expected stale baseline, got %v", err)
	}
	// Correct baseline succeeds.
	r, err := s.CreateConfig(ctx, 1, `{"f":"b"}`, "req-3", "t", now)
	if err != nil || r.Config.Version != 2 {
		t.Fatalf("expected version 2, got %v %v", r, err)
	}
}

func TestStoreRolloutStageRoundTrip(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	now := time.Unix(1700000000, 0)
	if _, err := s.CreateConfig(ctx, 0, `{"f":"a"}`, "c1", "t", now); err != nil {
		t.Fatal(err)
	}
	r := domain.Rollout{
		ID:                   "r1",
		TargetConfigVersion:  1,
		BaselineConfigVersion: 0,
		PlanRevision:         1,
		State:                domain.StateDraft,
		CurrentAttempt:       1,
		GroupID:              "g1",
		Stages: []domain.Stage{
			{Index: 0, Name: "s0", Target: domain.StageTarget{Kind: domain.TargetCount, Count: 2},
				MinSuccessRate: 0.5, MaxFailures: 1, AckTimeout: time.Minute, ObservationWindow: 30 * time.Second,
				State: domain.StagePending, NodeSnapshot: []string{"n1", "n2"}},
		},
		Attempts: []domain.Attempt{
			{Index: 1, Direction: domain.AttemptForward, TargetConfigVersion: 1, BaselineConfigVersion: 0, CreatedAt: now},
		},
	}
	if err := s.SaveRollout(ctx, r, 0); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := s.LoadRollout(ctx, "r1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(got.Stages) != 1 || got.Stages[0].NodeSnapshot[0] != "n1" {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	// CAS with wrong revision fails.
	got.State = domain.StateRunning
	if err := s.SaveRollout(ctx, got, 99); !IsPlanConflict(err) {
		t.Fatalf("expected plan conflict, got %v", err)
	}
	// CAS with correct revision succeeds and bumps revision.
	got.PlanRevision = 2
	if err := s.SaveRollout(ctx, got, 1); err != nil {
		t.Fatalf("cas save: %v", err)
	}
}
