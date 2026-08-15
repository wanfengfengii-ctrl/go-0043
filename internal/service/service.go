// Package service implements the control-plane business logic of the edge
// feature-flag wave publisher: immutable config versioning with optimistic
// concurrency, dynamic node grouping, phased rollouts with a deterministic
// batch state machine, forward/rollback attempts with isolated progress,
// protocol session watermarks for resumption, and crash recovery.
//
// The Service is the single coordination point. It owns a *store.Store, a
// scheduler and a clock, and exposes transactional operations. Per-rollout
// mutexes serialise state-machine transitions so concurrent acks, plan edits
// and lifecycle commands cannot corrupt each other, while different rollouts
// proceed in parallel.
package service

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/edgeflag/edgeflag-wave-publisher/internal/apperr"
	"github.com/edgeflag/edgeflag-wave-publisher/internal/domain"
	"github.com/edgeflag/edgeflag-wave-publisher/internal/store"
	"github.com/edgeflag/edgeflag-wave-publisher/pkg/clock"
	"github.com/edgeflag/edgeflag-wave-publisher/pkg/scheduler"
)

// IDGen produces identifiers for new rollouts and sessions.
type IDGen interface {
	NewRolloutID() string
	NewSessionID() int64
}

// seqIDGen is a thread-safe monotonic id generator used by default and in
// tests where determinism is wanted.
type seqIDGen struct {
	mu     sync.Mutex
	rollout int64
	sess    int64
	rolloutPrefix string
}

// NewSeqIDGen returns a monotonic id generator with the given rollout prefix.
func NewSeqIDGen(prefix string) IDGen {
	return &seqIDGen{rolloutPrefix: prefix}
}

func (g *seqIDGen) NewRolloutID() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.rollout++
	return fmt.Sprintf("%s-%d", g.rolloutPrefix, g.rollout)
}

func (g *seqIDGen) NewSessionID() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.sess++
	return g.sess
}

// Service is the control-plane coordinator.
type Service struct {
	store            *store.Store
	sched            scheduler.Scheduler
	clk              clock.Clock
	idgen            IDGen

	mu         sync.Mutex
	rollouts   map[string]*sync.Mutex
	sessions   map[string]*Session // keyed by nodeID
	dispatchMu sync.Mutex
}

// New constructs a Service. The scheduler and clock may be the manual
// implementations for tests or the real ones for production.
func New(s *store.Store, sched scheduler.Scheduler, clk clock.Clock, idgen IDGen) *Service {
	if idgen == nil {
		idgen = NewSeqIDGen("r")
	}
	return &Service{
		store:    s,
		sched:    sched,
		clk:      clk,
		idgen:    idgen,
		rollouts: map[string]*sync.Mutex{},
		sessions: map[string]*Session{},
	}
}

// Store returns the underlying store (used by recovery helpers and tests).
func (s *Service) Store() *store.Store { return s.store }

// Clock returns the service clock (used by tests to read logical time).
func (s *Service) Clock() clock.Clock { return s.clk }

// Scheduler returns the scheduler (used by tests to drive manual time).
func (s *Service) Scheduler() scheduler.Scheduler { return s.sched }

// rolloutLock returns the mutex guarding state-machine transitions for a rollout.
func (s *Service) rolloutLock(id string) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.rollouts[id]
	if !ok {
		m = &sync.Mutex{}
		s.rollouts[id] = m
	}
	return m
}

// loadRollout is a convenience that wraps the store lookup with a structured
// not-found error.
func (s *Service) loadRollout(ctx context.Context, id string) (domain.Rollout, error) {
	r, err := s.store.LoadRollout(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return r, apperr.New(apperr.CodeRolloutNotFound, "rollout "+id+" not found")
	}
	return r, err
}
