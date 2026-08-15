package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/edgeflag/edgeflag-wave-publisher/internal/apperr"
	"github.com/edgeflag/edgeflag-wave-publisher/internal/domain"
	"github.com/edgeflag/edgeflag-wave-publisher/internal/store"
	"github.com/edgeflag/edgeflag-wave-publisher/pkg/scheduler"
)

// StageSpec is the user-facing definition of a stage, before a node snapshot is
// bound to it.
type StageSpec struct {
	Name              string
	Target            domain.StageTarget
	MinSuccessRate    float64
	MaxFailures       int
	AckTimeout        time.Duration
	ObservationWindow time.Duration
}

// CreateRolloutRequest defines a new rollout.
type CreateRolloutRequest struct {
	ID                  string // optional; generated if empty
	TargetConfigVersion domain.ConfigVersion
	GroupID             string
	Stages              []StageSpec
}

// CreateRollout binds a target config version, the baseline config version and a
// node selection snapshot into a new draft rollout. The config must exist and
// have a recorded baseline; the rollout records that baseline so rollback can
// target it. Stages are stored as pending; node snapshots are bound when each
// stage opens (so later group changes only affect not-yet-opened stages).
func (s *Service) CreateRollout(ctx context.Context, req CreateRolloutRequest) (domain.Rollout, error) {
	if req.ID == "" {
		req.ID = s.idgen.NewRolloutID()
	}
	cfg, err := s.store.GetConfig(ctx, req.TargetConfigVersion)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return domain.Rollout{}, apperr.New(apperr.CodeConfigNotFound,
				"target config version does not exist")
		}
		return domain.Rollout{}, err
	}
	group, err := s.store.GetGroup(ctx, req.GroupID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return domain.Rollout{}, apperr.New(apperr.CodeGroupNotFound, "group does not exist")
		}
		return domain.Rollout{}, err
	}
	if len(req.Stages) == 0 {
		return domain.Rollout{}, apperr.New(apperr.CodeBadRequest, "at least one stage is required")
	}
	now := s.clk.Now()
	stages := make([]domain.Stage, len(req.Stages))
	for i, sp := range req.Stages {
		stages[i] = domain.Stage{
			Index:             i,
			Name:              sp.Name,
			Target:            sp.Target,
			MinSuccessRate:    sp.MinSuccessRate,
			MaxFailures:       sp.MaxFailures,
			AckTimeout:        sp.AckTimeout,
			ObservationWindow: sp.ObservationWindow,
			State:             domain.StagePending,
		}
	}
	r := domain.Rollout{
		ID:                    req.ID,
		TargetConfigVersion:   req.TargetConfigVersion,
		BaselineConfigVersion: cfg.BaselineVersion,
		PlanRevision:          1,
		Stages:                stages,
		State:                 domain.StateDraft,
		CurrentAttempt:        0,
		GroupID:               group.ID,
		CreatedAt:             now,
		UpdatedAt:             now,
	}
	if err := s.store.SaveRollout(ctx, r, 0); err != nil {
		return domain.Rollout{}, err
	}
	return r, nil
}

// GetRollout loads a rollout.
func (s *Service) GetRollout(ctx context.Context, id string) (domain.Rollout, error) {
	return s.loadRollout(ctx, id)
}

// StartRollout transitions a draft rollout to running and opens the first stage.
func (s *Service) StartRollout(ctx context.Context, id string) (domain.Rollout, error) {
	lock := s.rolloutLock(id)
	lock.Lock()
	defer lock.Unlock()
	r, err := s.loadRollout(ctx, id)
	if err != nil {
		return r, err
	}
	if r.State != domain.StateDraft {
		return r, apperr.Newf(apperr.CodeInvalidState, "cannot start rollout in state %s", r.State)
	}
	// Open the first forward attempt.
	r.CurrentAttempt = 1
	if len(r.Attempts) == 0 {
		r.Attempts = append(r.Attempts, domain.Attempt{
			Index: 1, Direction: domain.AttemptForward,
			TargetConfigVersion:   r.TargetConfigVersion,
			BaselineConfigVersion: r.BaselineConfigVersion,
			CreatedAt:             s.clk.Now(),
		})
	}
	r.State = domain.StateRunning
	if err := s.store.SaveRollout(ctx, r, r.PlanRevision); err != nil {
		return r, err
	}
	s.openStage(ctx, &r, 0)
	return r, nil
}

// openStage binds the node snapshot for a stage (if not yet bound) from the
// current group membership, marks it dispatching and begins incremental
// dispatch. Already-opened stages keep their existing snapshot.
func (s *Service) openStage(ctx context.Context, r *domain.Rollout, idx int) {
	if idx < 0 || idx >= len(r.Stages) {
		return
	}
	st := &r.Stages[idx]
	if st.State != domain.StagePending {
		return
	}
	// Bind node snapshot from the current group membership, deterministically.
	if len(st.NodeSnapshot) == 0 {
		group, err := s.store.GetGroup(ctx, r.GroupID)
		if err == nil {
			target := st.Target.Resolve(len(group.MemberIDs))
			if target > len(group.MemberIDs) {
				target = len(group.MemberIDs)
			}
			st.NodeSnapshot = append([]string(nil), group.MemberIDs[:target]...)
		}
	}
	st.State = domain.StageDispatching
	st.OpenedAt = s.clk.Now()
	_ = s.store.SaveRollout(ctx, *r, r.PlanRevision)
	// Begin dispatching the first node.
	s.dispatchNextNode(ctx, r, idx)
}

// dispatchNextNode dispatches the config to the next undispatched node in the
// stage. It respects pause: if the rollout is paused, no new dispatch occurs.
// It records the dispatch and, if more nodes remain, schedules the next
// dispatch. For tests the next dispatch is driven by calling this method again
// (or by the scheduler in production).
func (s *Service) dispatchNextNode(ctx context.Context, r *domain.Rollout, idx int) {
	if r.State == domain.StatePaused || r.State.IsTerminal() {
		return
	}
	if idx < 0 || idx >= len(r.Stages) {
		return
	}
	st := &r.Stages[idx]
	if st.State != domain.StageDispatching {
		return
	}
	if st.DispatchCursor >= len(st.NodeSnapshot) {
		// All dispatched; evaluate whether the stage can complete.
		s.evaluateStage(ctx, r, idx)
		return
	}
	nodeID := st.NodeSnapshot[st.DispatchCursor]
	attempt := r.CurrentAttempt
	rec := r.CurrentAttemptRecord()
	if rec == nil {
		return
	}
	cfgVer := rec.TargetConfigVersion
	// Record dispatch (idempotent) and advance cursor.
	_ = s.store.RecordDispatch(ctx, r.ID, idx, attempt, nodeID, cfgVer, s.clk.Now())
	st.DispatchCursor++
	_ = s.store.SaveRollout(ctx, *r, r.PlanRevision)
	// Send the dispatch frame to the node's session if connected.
	s.sendDispatch(ctx, r.ID, nodeID, cfgVer, attempt, rec.Direction)
	// Schedule an ack-timeout for this dispatch.
	s.sched.Schedule(scheduler.Task{
		At:         s.clk.Now().Add(st.AckTimeout),
		RolloutID:  r.ID,
		StageIndex: idx,
		NodeID:     nodeID,
		Attempt:    attempt,
		Kind:       scheduler.TaskAckTimeout,
		Fn: func() {
			s.handleAckTimeout(ctx, r.ID, idx, nodeID, attempt)
		},
	})
}

// handleAckTimeout marks a node as failed if it has not reached the required ack
// level, then re-evaluates the stage.
func (s *Service) handleAckTimeout(ctx context.Context, rolloutID string, idx int, nodeID string, attempt int) {
	lock := s.rolloutLock(rolloutID)
	lock.Lock()
	defer lock.Unlock()
	r, err := s.loadRollout(ctx, rolloutID)
	if err != nil {
		return
	}
	if r.CurrentAttempt != attempt {
		return // stale timeout from an older attempt
	}
	if idx < 0 || idx >= len(r.Stages) {
		return
	}
	st := &r.Stages[idx]
	if st.State != domain.StageDispatching && st.State != domain.StageObserving {
		return
	}
	// If the node already confirmed, nothing to do.
	confirmed, _ := s.store.ConfirmedNodeIDs(ctx, rolloutID, attempt, r.CurrentAttemptRecord().TargetConfigVersion)
	for _, c := range confirmed {
		if c == nodeID {
			return
		}
	}
	// Check whether any ack at all was recorded for this node/attempt.
	hw, ok, _ := s.store.NodeAckHighWater(ctx, rolloutID, nodeID, attempt)
	if ok && hw.Level >= domain.AckReceived && hw.ConfigVersion == r.CurrentAttemptRecord().TargetConfigVersion {
		// Some ack received but not confirmed: treat as failure.
		st.FailureCount++
	} else {
		st.FailureCount++
	}
	_ = s.store.SaveRollout(ctx, r, r.PlanRevision)
	s.evaluateStage(ctx, &r, idx)
}

// evaluateStage recomputes success/failure counts from persisted acks and
// transitions the stage according to its thresholds. It is idempotent: once a
// stage is Succeeded/Failed it does nothing. This is the single decision point
// for stage advancement, ensuring the "exactly once" resume property.
func (s *Service) evaluateStage(ctx context.Context, r *domain.Rollout, idx int) {
	if idx < 0 || idx >= len(r.Stages) {
		return
	}
	st := &r.Stages[idx]
	if st.State == domain.StageSucceeded || st.State == domain.StageFailed {
		return
	}
	rec := r.CurrentAttemptRecord()
	if rec == nil {
		return
	}
	confirmed, _ := s.store.ConfirmedNodeIDs(ctx, r.ID, r.CurrentAttempt, rec.TargetConfigVersion)
	confirmedSet := map[string]bool{}
	for _, c := range confirmed {
		confirmedSet[c] = true
	}
	// Recompute success/failure from persisted state. Success = confirmed nodes
	// that are in this stage's snapshot. Failure = snapshot nodes not confirmed
	// and already timed-out (recorded as failures). We recompute success from
	// acks and keep the persisted failure count (incremented on timeout/nack).
	success := 0
	for _, n := range st.NodeSnapshot {
		if confirmedSet[n] {
			success++
		}
	}
	st.SuccessCount = success
	target := len(st.NodeSnapshot)
	// Fail fast if max failures exceeded.
	if st.FailureCount > st.MaxFailures {
		st.State = domain.StageFailed
		st.CompletedAt = s.clk.Now()
		_ = s.store.SaveRollout(ctx, *r, r.PlanRevision)
		s.onStageFailed(ctx, r, idx)
		return
	}
	resolved := success + st.FailureCount
	if resolved < target {
		// Still in progress; persist recomputed success count.
		_ = s.store.SaveRollout(ctx, *r, r.PlanRevision)
		return
	}
	// All resolved: decide.
	if target == 0 {
		st.State = domain.StageSucceeded
	} else {
		rate := float64(success) / float64(target)
		if rate >= st.MinSuccessRate && st.FailureCount <= st.MaxFailures {
			st.State = domain.StageSucceeded
		} else {
			st.State = domain.StageFailed
		}
	}
	st.CompletedAt = s.clk.Now()
	_ = s.store.SaveRollout(ctx, *r, r.PlanRevision)
	if st.State == domain.StageFailed {
		s.onStageFailed(ctx, r, idx)
		return
	}
	s.onStageSucceeded(ctx, r, idx)
}

// onStageSucceeded moves to observing and schedules the next stage open after
// the observation window, or completes the rollout if this was the last stage.
func (s *Service) onStageSucceeded(ctx context.Context, r *domain.Rollout, idx int) {
	st := &r.Stages[idx]
	st.State = domain.StageObserving
	_ = s.store.SaveRollout(ctx, *r, r.PlanRevision)
	if idx+1 >= len(r.Stages) {
		// Last stage: schedule completion after observation window.
		s.sched.Schedule(scheduler.Task{
			At: s.clk.Now().Add(st.ObservationWindow), RolloutID: r.ID, StageIndex: idx,
			Attempt: r.CurrentAttempt, Kind: scheduler.TaskObserveEnd,
			Fn: func() { s.completeAttempt(ctx, r.ID, r.CurrentAttempt) },
		})
		return
	}
	s.sched.Schedule(scheduler.Task{
		At: s.clk.Now().Add(st.ObservationWindow), RolloutID: r.ID, StageIndex: idx,
		Attempt: r.CurrentAttempt, Kind: scheduler.TaskObserveEnd,
		Fn: func() { s.openStageAfterObserve(ctx, r.ID, idx+1) },
	})
}

func (s *Service) openStageAfterObserve(ctx context.Context, rolloutID string, idx int) {
	lock := s.rolloutLock(rolloutID)
	lock.Lock()
	defer lock.Unlock()
	r, err := s.loadRollout(ctx, rolloutID)
	if err != nil {
		return
	}
	if r.State != domain.StateRunning {
		return
	}
	s.openStage(ctx, &r, idx)
}

// onStageFailed fails the rollout (or the rollback attempt). For a forward
// attempt, a stage failure transitions the rollout to Failed.
func (s *Service) onStageFailed(ctx context.Context, r *domain.Rollout, idx int) {
	rec := r.CurrentAttemptRecord()
	if rec != nil && rec.Direction == domain.AttemptRollback {
		// Rollback failed: the rollout is failed.
		r.State = domain.StateFailed
	} else {
		r.State = domain.StateFailed
	}
	_ = s.store.SaveRollout(ctx, *r, r.PlanRevision)
}

// completeAttempt finalises the current attempt. For a forward attempt where
// all stages succeeded, the rollout completes. For a rollback attempt, the
// rollout is rolled back.
func (s *Service) completeAttempt(ctx context.Context, rolloutID string, attempt int) {
	lock := s.rolloutLock(rolloutID)
	lock.Lock()
	defer lock.Unlock()
	s.completeAttemptLocked(ctx, rolloutID, attempt)
}

// completeAttemptLocked finalises an attempt while the caller owns its
// rollout lock. It avoids recursively acquiring the non-reentrant mutex during
// recovery.
func (s *Service) completeAttemptLocked(ctx context.Context, rolloutID string, attempt int) {
	r, err := s.loadRollout(ctx, rolloutID)
	if err != nil {
		return
	}
	if r.CurrentAttempt != attempt {
		return
	}
	rec := r.CurrentAttemptRecord()
	if rec == nil {
		return
	}
	if rec.Direction == domain.AttemptRollback {
		r.State = domain.StateRolledBack
	} else {
		r.State = domain.StateCompleted
	}
	_ = s.store.SaveRollout(ctx, r, r.PlanRevision)
}

// ---------- Plan revision ----------

// PlanChange describes an update to not-yet-opened stages.
type PlanChange struct {
	// StageUpdates replaces the spec of a pending stage by index. Only stages
	// whose state is still Pending may be changed.
	StageUpdates map[int]StageSpec
	// AddStages appends new pending stages at the end.
	AddStages []StageSpec
}

// UpdatePlan applies a plan change guarded by an expected plan revision. Only
// not-yet-opened (Pending) stages are modified; opened stages keep their node
// snapshot and thresholds. On success the plan revision increments by one.
// A concurrent update that bumped the revision first yields a
// plan_revision_conflict error.
func (s *Service) UpdatePlan(ctx context.Context, rolloutID string, expectedRevision int64, change PlanChange) (domain.Rollout, error) {
	lock := s.rolloutLock(rolloutID)
	lock.Lock()
	defer lock.Unlock()
	r, err := s.loadRollout(ctx, rolloutID)
	if err != nil {
		return r, err
	}
	if r.PlanRevision != expectedRevision {
		return r, apperr.Newf(apperr.CodePlanRevisionConflict,
			"expected plan revision %d but current is %d", expectedRevision, r.PlanRevision)
	}
	// Apply stage updates to pending stages only.
	for idx, sp := range change.StageUpdates {
		if idx < 0 || idx >= len(r.Stages) {
			return r, apperr.Newf(apperr.CodeStageNotFound, "stage %d out of range", idx)
		}
		if r.Stages[idx].State != domain.StagePending {
			return r, apperr.Newf(apperr.CodeInvalidState,
				"stage %d already started; cannot modify", idx)
		}
		r.Stages[idx].Name = sp.Name
		r.Stages[idx].Target = sp.Target
		r.Stages[idx].MinSuccessRate = sp.MinSuccessRate
		r.Stages[idx].MaxFailures = sp.MaxFailures
		r.Stages[idx].AckTimeout = sp.AckTimeout
		r.Stages[idx].ObservationWindow = sp.ObservationWindow
	}
	for _, sp := range change.AddStages {
		r.Stages = append(r.Stages, domain.Stage{
			Index:             len(r.Stages),
			Name:              sp.Name,
			Target:            sp.Target,
			MinSuccessRate:    sp.MinSuccessRate,
			MaxFailures:       sp.MaxFailures,
			AckTimeout:        sp.AckTimeout,
			ObservationWindow: sp.ObservationWindow,
			State:             domain.StagePending,
		})
	}
	r.PlanRevision = expectedRevision + 1
	// CAS save: only commits if the revision is still expectedRevision.
	if err := s.store.SaveRollout(ctx, r, expectedRevision); err != nil {
		if store.IsPlanConflict(err) {
			return r, apperr.New(apperr.CodePlanRevisionConflict,
				"plan revision changed concurrently")
		}
		return r, err
	}
	return r, nil
}

// Pause freezes new dispatches while continuing to record acks.
func (s *Service) Pause(ctx context.Context, rolloutID string) (domain.Rollout, error) {
	lock := s.rolloutLock(rolloutID)
	lock.Lock()
	defer lock.Unlock()
	r, err := s.loadRollout(ctx, rolloutID)
	if err != nil {
		return r, err
	}
	if r.State != domain.StateRunning && r.State != domain.StateRollingBack {
		return r, apperr.Newf(apperr.CodeInvalidState, "cannot pause rollout in state %s", r.State)
	}
	r.State = domain.StatePaused
	if err := s.store.SaveRollout(ctx, r, r.PlanRevision); err != nil {
		return r, err
	}
	return r, nil
}

// Resume restarts dispatch and, based on persisted acks, immediately and
// exactly once re-evaluates whether the current stage should advance.
func (s *Service) Resume(ctx context.Context, rolloutID string) (domain.Rollout, error) {
	lock := s.rolloutLock(rolloutID)
	lock.Lock()
	defer lock.Unlock()
	r, err := s.loadRollout(ctx, rolloutID)
	if err != nil {
		return r, err
	}
	if r.State != domain.StatePaused {
		return r, apperr.Newf(apperr.CodeInvalidState, "cannot resume rollout in state %s", r.State)
	}
	// Determine whether we were rolling back (current attempt is rollback).
	rec := r.CurrentAttemptRecord()
	if rec != nil && rec.Direction == domain.AttemptRollback {
		r.State = domain.StateRollingBack
	} else {
		r.State = domain.StateRunning
	}
	if err := s.store.SaveRollout(ctx, r, r.PlanRevision); err != nil {
		return r, err
	}
	// Re-evaluate the active stage exactly once. If the stage was dispatching
	// and all nodes are now resolved, this advances it; otherwise it resumes
	// dispatch for undispatched nodes.
	for i := range r.Stages {
		st := &r.Stages[i]
		if st.State == domain.StageDispatching {
			s.evaluateStage(ctx, &r, i)
			if st.State == domain.StageDispatching {
				s.dispatchNextNode(ctx, &r, i)
			}
			break
		}
		if st.State == domain.StageObserving {
			// Already observing; re-evaluate thresholds from persisted acks.
			s.evaluateStage(ctx, &r, i)
			break
		}
	}
	return r, nil
}

// Rollback starts a new rollback attempt targeting the recorded baseline
// config. Nodes that received the forward config must explicitly ack the
// baseline. Forward progress (attempt 1) is frozen; late forward acks are
// audit-only and cannot reactivate the rollout.
func (s *Service) Rollback(ctx context.Context, rolloutID string) (domain.Rollout, error) {
	lock := s.rolloutLock(rolloutID)
	lock.Lock()
	defer lock.Unlock()
	r, err := s.loadRollout(ctx, rolloutID)
	if err != nil {
		return r, err
	}
	if r.State.IsTerminal() {
		return r, apperr.Newf(apperr.CodeInvalidState, "cannot roll back terminal rollout in state %s", r.State)
	}
	if r.BaselineConfigVersion == 0 {
		return r, apperr.New(apperr.CodeInvalidState, "no baseline config to roll back to")
	}
	// Collect the union of nodes dispatched across forward attempts.
	dispatched := map[string]bool{}
	for _, a := range r.Attempts {
		if a.Direction != domain.AttemptForward {
			continue
		}
		for i := range r.Stages {
			ids, _ := s.store.DispatchedNodeIDs(ctx, r.ID, i, a.Index)
			for id := range ids {
				dispatched[id] = true
			}
		}
	}
	newAttempt := len(r.Attempts) + 1
	r.Attempts = append(r.Attempts, domain.Attempt{
		Index: newAttempt, Direction: domain.AttemptRollback,
		TargetConfigVersion:   r.BaselineConfigVersion,
		BaselineConfigVersion: r.BaselineConfigVersion,
		CreatedAt:             s.clk.Now(),
	})
	r.CurrentAttempt = newAttempt
	r.State = domain.StateRollingBack
	// Append a rollback stage targeting the dispatched node set.
	rollbackStage := domain.Stage{
		Index:             len(r.Stages),
		Name:              fmt.Sprintf("rollback-%d", newAttempt),
		Target:            domain.StageTarget{Kind: domain.TargetCount, Count: len(dispatched)},
		MinSuccessRate:    1.0,
		MaxFailures:       0,
		AckTimeout:        30 * time.Second,
		ObservationWindow: 5 * time.Second,
		State:             domain.StagePending,
		NodeSnapshot:      domain.SortMemberIDs(keysOf(dispatched)),
	}
	r.Stages = append(r.Stages, rollbackStage)
	if err := s.store.SaveRollout(ctx, r, r.PlanRevision); err != nil {
		return r, err
	}
	s.openStage(ctx, &r, len(r.Stages)-1)
	return r, nil
}

func keysOf(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// GetProgress returns the progress view for a rollout.
func (s *Service) GetProgress(ctx context.Context, rolloutID string) (domain.Progress, error) {
	r, err := s.loadRollout(ctx, rolloutID)
	if err != nil {
		return domain.Progress{}, err
	}
	p := domain.Progress{
		RolloutID:      r.ID,
		State:          r.State,
		PlanRevision:   r.PlanRevision,
		CurrentAttempt: r.CurrentAttempt,
	}
	for _, st := range r.Stages {
		sp := domain.StageProgress{
			Index:        st.Index,
			Name:         st.Name,
			State:        st.State,
			Target:       len(st.NodeSnapshot),
			Dispatched:   st.DispatchCursor,
			SuccessCount: st.SuccessCount,
			FailureCount: st.FailureCount,
		}
		if !st.OpenedAt.IsZero() {
			ot := st.OpenedAt
			sp.OpenedAt = &ot
		}
		if !st.CompletedAt.IsZero() {
			ct := st.CompletedAt
			sp.CompletedAt = &ct
		}
		p.Stages = append(p.Stages, sp)
	}
	return p, nil
}

// ListAuditAcks returns audit-only acknowledgements for a rollout.
func (s *Service) ListAuditAcks(ctx context.Context, rolloutID string) ([]domain.AuditAck, error) {
	return s.store.ListAuditAcks(ctx, rolloutID)
}
