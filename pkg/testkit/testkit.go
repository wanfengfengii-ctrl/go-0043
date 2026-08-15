// Package testkit provides reusable test fixtures for the edge feature-flag
// publisher: a temporary SQLite database factory, a fixed-clock and manual
// scheduler builder, a deterministic id generator, and convenience wiring to
// construct a fully-in-memory Service. All acceptance tests build on these
// helpers so they never touch the real network, wall clock or external
// services.
package testkit

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/edgeflag/edgeflag-wave-publisher/internal/domain"
	"github.com/edgeflag/edgeflag-wave-publisher/internal/service"
	"github.com/edgeflag/edgeflag-wave-publisher/internal/store"
	"github.com/edgeflag/edgeflag-wave-publisher/pkg/clock"
	"github.com/edgeflag/edgeflag-wave-publisher/pkg/scheduler"
)

// TempStore returns a store backed by a fresh temporary database file in the
// test's temp directory. The store is closed automatically when the test ends.
func TempStore(t *testing.T) *store.Store {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "edgeflag.db")
	s, err := store.Open(path)
	if err != nil {
		t.Fatalf("open temp store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// MemoryStore returns a store backed by an in-memory database (no file
// persistence). Use TempStore when crash-recovery across a real file is needed.
func MemoryStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(":memory:")
	if err != nil {
		t.Fatalf("open memory store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// StoreAtPath opens a store at an explicit path (for cross-process / recovery
// tests that re-open the same file).
func StoreAtPath(t *testing.T, path string) *store.Store {
	t.Helper()
	s, err := store.Open(path)
	if err != nil {
		t.Fatalf("open store at %s: %v", path, err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// Harness bundles the deterministic components used by tests.
type Harness struct {
	Store    *store.Store
	Clock    *clock.Fixed
	Sched    *scheduler.Manual
	Service  *service.Service
	IDGen    *FixedIDs
	BaseTime time.Time
}

// NewHarness builds a fully-in-memory service with a fixed clock, manual
// scheduler and deterministic ids anchored at BaseTime.
func NewHarness(t *testing.T) *Harness {
	t.Helper()
	st := MemoryStore(t)
	clk := clock.NewFixed(time.Unix(1_700_000_000, 0))
	sch := scheduler.NewManual(clk)
	ids := &FixedIDs{}
	svc := service.New(st, sch, clk, ids)
	return &Harness{Store: st, Clock: clk, Sched: sch, Service: svc, IDGen: ids, BaseTime: clk.Now()}
}

// NewFileHarness is like NewHarness but uses a file-backed store at path so the
// database survives a Close/Reopen (for recovery tests).
func NewFileHarness(t *testing.T, path string) *Harness {
	t.Helper()
	st := StoreAtPath(t, path)
	clk := clock.NewFixed(time.Unix(1_700_000_000, 0))
	sch := scheduler.NewManual(clk)
	ids := &FixedIDs{}
	svc := service.New(st, sch, clk, ids)
	return &Harness{Store: st, Clock: clk, Sched: sch, Service: svc, IDGen: ids, BaseTime: clk.Now()}
}

// AdvanceTime advances the fixed clock and drains the manual scheduler.
func (h *Harness) AdvanceTime(d time.Duration) {
	h.Clock.Advance(d)
	_ = h.Sched.Advance(h.Clock.Now())
}

// SetTime sets the fixed clock and drains the scheduler.
func (h *Harness) SetTime(t time.Time) {
	h.Clock.Set(t)
	_ = h.Sched.Advance(h.Clock.Now())
}

// FixedIDs is a deterministic id generator. It assigns rollout ids from a
// counter with a prefix and session ids from a counter.
type FixedIDs struct {
	rollout atomic.Int64
	sess    atomic.Int64
	Prefix  string
}

// NewRolloutID returns the next rollout id.
func (f *FixedIDs) NewRolloutID() string {
	if f.Prefix == "" {
		f.Prefix = "r"
	}
	return f.Prefix + "-" + itoa(f.rollout.Add(1))
}

// NewSessionID returns the next session id.
func (f *FixedIDs) NewSessionID() int64 {
	return f.sess.Add(1)
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// SeedConfig is a test helper that creates a config version directly via the
// store, returning the resulting version. It assumes an empty baseline for the
// first config.
func SeedConfig(t *testing.T, st *store.Store, baseline domain.ConfigVersion, content, key string) domain.FeatureConfig {
	t.Helper()
	res, err := st.CreateConfig(context.Background(), baseline, content, key, "test", time.Unix(1_700_000_000, 0))
	if err != nil {
		t.Fatalf("seed config: %v", err)
	}
	return res.Config
}
