package service

import (
	"context"
	"errors"

	"github.com/edgeflag/edgeflag-wave-publisher/internal/domain"
	"github.com/edgeflag/edgeflag-wave-publisher/internal/store"
)

// AckOutcome describes what happened to an acknowledgement.
type AckOutcome struct {
	Applied      bool             // true if the ack advanced the current attempt's progress
	Duplicate    bool             // true if the ack was an exact duplicate (no-op)
	AuditReason  domain.AuditReason // set when recorded as audit-only
	NewHighWater domain.Ack       // the node's new high-water ack (zero value if none)
}

// ProcessAck applies the deterministic merge rule to an acknowledgement. The
// full dedup identity is (rollout, node, configVersion, attempt, level, seq).
//
// Progress advancement is gated on three conditions, all of which must hold:
//  1. the ack's configVersion is a known config (else rejected, recorded as
//     audit "unknown_config_version");
//  2. the ack's attempt equals the rollout's current attempt;
//  3. the ack's configVersion equals the current attempt's target config and
//     the node is part of the current stage's target set.
//
// Acks failing any of 2/3 are recorded in the audit table but do not change
// node or rollout progress. Acks for an older attempt (e.g. late forward acks
// after a rollback) therefore cannot reactivate a rolled-back rollout, because
// the current attempt is the rollback attempt and the forward attempt no
// longer matches.
func (s *Service) ProcessAck(ctx context.Context, ack domain.Ack) (AckOutcome, error) {
	var out AckOutcome
	r, err := s.store.LoadRollout(ctx, ack.RolloutID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return out, errRolloutNotFound(ack.RolloutID)
		}
		return out, err
	}
	ack.ReceivedAt = s.clk.Now()

	// (1) Known config version?
	known, err := s.store.HasConfig(ctx, ack.ConfigVersion)
	if err != nil {
		return out, err
	}
	if !known {
		// Unknown config version: record as audit, do not change progress.
		out.AuditReason = domain.AuditUnknownConfig
		_ = s.store.RecordAuditAck(ctx, ack, domain.AuditUnknownConfig)
		// Trigger negotiation: send the node the current target version.
		s.negotiateUnknownConfig(ctx, &r, ack)
		return out, nil
	}

	rec := r.CurrentAttemptRecord()
	// (2) Attempt matches the current attempt?
	if rec == nil || ack.Attempt != r.CurrentAttempt {
		reason := domain.AuditStaleAttempt
		if ack.Attempt > r.CurrentAttempt {
			reason = domain.AuditStaleAttempt // future attempt: not processed
		}
		out.AuditReason = reason
		_ = s.store.RecordAuditAck(ctx, ack, reason)
		return out, nil
	}

	// (3) Config version matches the current attempt's target, and node is a
	// target of the current stage?
	if ack.ConfigVersion != rec.TargetConfigVersion {
		out.AuditReason = domain.AuditOldVersion
		_ = s.store.RecordAuditAck(ctx, ack, domain.AuditOldVersion)
		return out, nil
	}
	if !s.isCurrentTargetNode(&r, ack.NodeID) {
		out.AuditReason = domain.AuditNotTargetNode
		_ = s.store.RecordAuditAck(ctx, ack, domain.AuditNotTargetNode)
		return out, nil
	}

	// Monotonic merge: only accept the ack if it advances the node's high-water
	// for this attempt.
	hw, has, err := s.store.NodeAckHighWater(ctx, ack.RolloutID, ack.NodeID, r.CurrentAttempt)
	if err != nil {
		return out, err
	}
	if has {
		cmp := ack.Key().Compare(hw.Key())
		if cmp <= 0 {
			// Stale or exact duplicate: record as audit, no progress change.
			reason := domain.AuditDuplicate
			if cmp < 0 {
				reason = domain.AuditStaleAttempt
			}
			out.AuditReason = reason
			out.Duplicate = cmp == 0
			_ = s.store.RecordAuditAck(ctx, ack, reason)
			return out, nil
		}
	}

	// Applies to progress: persist the ack row.
	res, err := s.store.RecordAck(ctx, ack, true)
	if err != nil {
		return out, err
	}
	out.NewHighWater = ack
	if res.Inserted {
		out.Applied = true
	} else {
		out.Duplicate = true
		out.AuditReason = domain.AuditDuplicate
	}

	// Re-evaluate the active stage under the rollout lock.
	if out.Applied {
		lock := s.rolloutLock(ack.RolloutID)
		lock.Lock()
		defer lock.Unlock()
		// Reload to get the freshest stage state.
		r2, err := s.loadRollout(ctx, ack.RolloutID)
		if err == nil {
			for i := range r2.Stages {
				st := &r2.Stages[i]
				if st.State == domain.StageDispatching || st.State == domain.StageObserving {
					s.evaluateStage(ctx, &r2, i)
					break
				}
			}
		}
	}
	return out, nil
}

// isCurrentTargetNode reports whether nodeID is in the active (dispatching or
// observing) stage's node snapshot for the rollout's current attempt.
func (s *Service) isCurrentTargetNode(r *domain.Rollout, nodeID string) bool {
	for i := range r.Stages {
		st := &r.Stages[i]
		if st.State == domain.StageDispatching || st.State == domain.StageObserving {
			for _, n := range st.NodeSnapshot {
				if n == nodeID {
					return true
				}
			}
			return false
		}
	}
	return false
}

func errRolloutNotFound(id string) error {
	return errors.New("rollout not found: " + id)
}

// negotiateUnknownConfig sends a negotiate frame to the node telling it the
// current target config version. This is the "unknown config version
// negotiation" behaviour: the node is not penalised, it is simply informed.
func (s *Service) negotiateUnknownConfig(ctx context.Context, r *domain.Rollout, ack domain.Ack) {
	rec := r.CurrentAttemptRecord()
	current := int64(0)
	if rec != nil {
		current = int64(rec.TargetConfigVersion)
	}
	s.sendNegotiate(ctx, ack.NodeID, r.ID, current)
}
