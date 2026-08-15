package service

import (
	"context"

	"github.com/edgeflag/edgeflag-wave-publisher/internal/domain"
	"github.com/edgeflag/edgeflag-wave-publisher/pkg/scheduler"
)

// Recover reloads persisted state and re-registers scheduled work so the service
// can resume after a crash. It re-validates store invariants, re-schedules ack
// timeouts for dispatched-but-unconfirmed nodes, and re-arms observation-window
// timers for observing stages. It never re-advances a stage: stages are
// restored in their persisted state and only future acks/timeouts move them.
func (s *Service) Recover(ctx context.Context) error {
	if err := s.store.ValidateInvariants(ctx); err != nil {
		return err
	}
	ids, err := s.store.ListActiveRolloutIDs(ctx)
	if err != nil {
		return err
	}
	for _, id := range ids {
		r, err := s.store.LoadRollout(ctx, id)
		if err != nil {
			continue
		}
		s.rescheduleRollout(ctx, r)
	}
	return nil
}

// rescheduleRollout re-arms scheduler tasks for a recovered rollout. For each
// dispatched-but-unconfirmed node in the active stage it schedules an ack
// timeout; for an observing stage it schedules the observe-end / completion.
func (s *Service) rescheduleRollout(ctx context.Context, r domain.Rollout) {
	if r.State == domain.StateDraft || r.State.IsTerminal() {
		return
	}
	rec := r.CurrentAttemptRecord()
	if rec == nil {
		return
	}
	// Find the active stage.
	for i := range r.Stages {
		st := &r.Stages[i]
		switch st.State {
		case domain.StageDispatching:
			// For each dispatched node that has not yet confirmed, schedule an
			// ack timeout relative to "now" (the remaining budget). On the
			// manual scheduler the test advances time to fire them.
			confirmed, _ := s.store.ConfirmedNodeIDs(ctx, r.ID, r.CurrentAttempt, rec.TargetConfigVersion)
			confSet := map[string]bool{}
			for _, c := range confirmed {
				confSet[c] = true
			}
			for _, nodeID := range st.NodeSnapshot[:st.DispatchCursor] {
				if confSet[nodeID] {
					continue
				}
				nodeID := nodeID
				idx := i
				attempt := r.CurrentAttempt
				s.sched.Schedule(scheduler.Task{
					At: s.clk.Now().Add(st.AckTimeout), RolloutID: r.ID,
					StageIndex: idx, NodeID: nodeID, Attempt: attempt,
					Kind: scheduler.TaskAckTimeout,
					Fn: func() { s.handleAckTimeout(ctx, r.ID, idx, nodeID, attempt) },
				})
			}
			// If there are undispatched nodes and the rollout is running, resume
			// dispatch from the cursor.
			if r.State == domain.StateRunning && st.DispatchCursor < len(st.NodeSnapshot) {
				idx := i
				rr := r
				s.sched.Schedule(scheduler.Task{
					At: s.clk.Now(), RolloutID: r.ID, StageIndex: idx, Attempt: r.CurrentAttempt,
					Kind: scheduler.TaskOpenStage,
					Fn:   func() { s.dispatchNextNode(ctx, &rr, idx) },
				})
			}
		case domain.StageObserving:
			idx := i
			attempt := r.CurrentAttempt
			at := s.clk.Now().Add(st.ObservationWindow)
			if !st.OpenedAt.IsZero() && st.OpenedAt.Add(st.ObservationWindow).After(s.clk.Now()) {
				at = st.OpenedAt.Add(st.ObservationWindow)
			}
			// If the observation window already elapsed, re-evaluate now.
			if !at.After(s.clk.Now()) {
				s.sched.Schedule(scheduler.Task{
					At: s.clk.Now(), RolloutID: r.ID, StageIndex: idx, Attempt: attempt,
					Kind: scheduler.TaskObserveEnd,
					Fn: func() {
						s.recoverObservingStage(ctx, r.ID, idx, attempt)
					},
				})
			} else {
				s.sched.Schedule(scheduler.Task{
					At: at, RolloutID: r.ID, StageIndex: idx, Attempt: attempt,
					Kind: scheduler.TaskObserveEnd,
					Fn: func() {
						s.recoverObservingStage(ctx, r.ID, idx, attempt)
					},
				})
			}
		}
	}
}

// recoverObservingStage re-evaluates an observing stage after recovery: either
// advance to the next stage or complete the attempt.
func (s *Service) recoverObservingStage(ctx context.Context, rolloutID string, idx, attempt int) {
	lock := s.rolloutLock(rolloutID)
	lock.Lock()
	defer lock.Unlock()
	r, err := s.loadRollout(ctx, rolloutID)
	if err != nil {
		return
	}
	if r.CurrentAttempt != attempt {
		return
	}
	if idx >= len(r.Stages) {
		return
	}
	st := &r.Stages[idx]
	if st.State != domain.StageObserving {
		return
	}
	if idx+1 >= len(r.Stages) {
		s.completeAttempt(ctx, rolloutID, attempt)
		return
	}
	s.openStage(ctx, &r, idx+1)
}

// DispatchTick drives incremental dispatch for one node without advancing the
// scheduler clock. It is primarily a test hook; production dispatch is driven by
// the scheduler.
func (s *Service) DispatchTick(ctx context.Context, rolloutID string, stageIndex int) {
	s.dispatchTick(ctx, rolloutID, stageIndex)
}

// dispatchTick is the internal incremental-dispatch primitive.
func (s *Service) dispatchTick(ctx context.Context, rolloutID string, stageIndex int) {
	lock := s.rolloutLock(rolloutID)
	lock.Lock()
	defer lock.Unlock()
	r, err := s.loadRollout(ctx, rolloutID)
	if err != nil {
		return
	}
	s.dispatchNextNode(ctx, &r, stageIndex)
}
