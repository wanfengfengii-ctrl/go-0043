package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/edgeflag/edgeflag-wave-publisher/internal/domain"
)

// ---------- Rollouts ----------

// SaveRollout persists a rollout and all of its stages and attempts, replacing
// the prior state. It is used for create and for every state transition. The
// plan revision is written via a conditional update (CAS) when expectedRevision
// is non-zero; otherwise the rollout is assumed new or being mutated by the
// owner of the current revision.
func (s *Store) SaveRollout(ctx context.Context, r domain.Rollout, expectedRevision int64) error {
	now := nowNano()
	r.UpdatedAt = nanoToTime(now)
	return s.InTx(ctx, func(tx *sql.Tx) error {
		if expectedRevision > 0 {
			res, err := tx.ExecContext(ctx,
				`UPDATE rollouts SET state=?, current_attempt=?, plan_revision=?, updated_at=?
				 WHERE id=? AND plan_revision=?`,
				string(r.State), r.CurrentAttempt, r.PlanRevision, now, r.ID, expectedRevision)
			if err != nil {
				return fmt.Errorf("cas update rollout: %w", err)
			}
			n, _ := res.RowsAffected()
			if n == 0 {
				return errPlanConflict
			}
		} else {
			_, err := tx.ExecContext(ctx,
				`INSERT INTO rollouts (id, target_config_version, baseline_config_version, plan_revision, state, current_attempt, group_id, created_at, updated_at)
				 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				r.ID, int64(r.TargetConfigVersion), int64(r.BaselineConfigVersion), r.PlanRevision,
				string(r.State), r.CurrentAttempt, r.GroupID, now, now)
			if err != nil {
				return fmt.Errorf("insert rollout: %w", err)
			}
		}
		// Persist attempts.
		if _, err := tx.ExecContext(ctx, `DELETE FROM rollout_attempts WHERE rollout_id = ?`, r.ID); err != nil {
			return err
		}
		for _, a := range r.Attempts {
			_, err := tx.ExecContext(ctx,
				`INSERT INTO rollout_attempts (rollout_id, attempt_index, direction, target_config_version, baseline_config_version, created_at)
				 VALUES (?, ?, ?, ?, ?, ?)`,
				r.ID, a.Index, int(a.Direction), int64(a.TargetConfigVersion), int64(a.BaselineConfigVersion), a.CreatedAt.UnixNano())
			if err != nil {
				return err
			}
		}
		// Persist stages. Stages are fully replaced; their immutable node
		// snapshots are written verbatim so recovery restores them exactly.
		if _, err := tx.ExecContext(ctx, `DELETE FROM stages WHERE rollout_id = ?`, r.ID); err != nil {
			return err
		}
		for _, st := range r.Stages {
			if err := saveStage(ctx, tx, r.ID, st); err != nil {
				return err
			}
		}
		return nil
	})
}

func saveStage(ctx context.Context, tx *sql.Tx, rolloutID string, st domain.Stage) error {
	snap, err := idsJSON(st.NodeSnapshot)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO stages (rollout_id, stage_index, name, target_type, target_count, target_percent,
		    min_success_rate, max_failures, ack_timeout_ms, observation_window_ms, state,
		    node_snapshot, dispatch_cursor, success_count, failure_count, opened_at, completed_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		rolloutID, st.Index, st.Name, string(st.Target.Kind), st.Target.Count, st.Target.Percent,
		st.MinSuccessRate, st.MaxFailures, int64(st.AckTimeout), int64(st.ObservationWindow), string(st.State),
		snap, st.DispatchCursor, st.SuccessCount, st.FailureCount, st.OpenedAt.UnixNano(), st.CompletedAt.UnixNano())
	return err
}

// LoadRollout loads a rollout with its stages and attempts.
func (s *Store) LoadRollout(ctx context.Context, id string) (domain.Rollout, error) {
	var r domain.Rollout
	var state string
	var created, updated int64
	err := s.db.QueryRowContext(ctx,
		`SELECT id, target_config_version, baseline_config_version, plan_revision, state, current_attempt, group_id, created_at, updated_at
		 FROM rollouts WHERE id = ?`, id,
	).Scan(&r.ID, &r.TargetConfigVersion, &r.BaselineConfigVersion, &r.PlanRevision, &state, &r.CurrentAttempt, &r.GroupID, &created, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return r, ErrNotFound
	}
	if err != nil {
		return r, err
	}
	r.State = domain.RolloutState(state)
	r.CreatedAt = nanoToTime(created)
	r.UpdatedAt = nanoToTime(updated)

	rows, err := s.db.QueryContext(ctx,
		`SELECT attempt_index, direction, target_config_version, baseline_config_version, created_at
		 FROM rollout_attempts WHERE rollout_id = ? ORDER BY attempt_index ASC`, id)
	if err != nil {
		return r, err
	}
	for rows.Next() {
		var a domain.Attempt
		var dir, createdAt int64
		if err := rows.Scan(&a.Index, &dir, &a.TargetConfigVersion, &a.BaselineConfigVersion, &createdAt); err != nil {
			rows.Close()
			return r, err
		}
		a.Direction = domain.AttemptDirection(dir)
		a.CreatedAt = nanoToTime(createdAt)
		r.Attempts = append(r.Attempts, a)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return r, err
	}

	stages, err := s.loadStages(ctx, id)
	if err != nil {
		return r, err
	}
	r.Stages = stages
	return r, nil
}

func (s *Store) loadStages(ctx context.Context, rolloutID string) ([]domain.Stage, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT stage_index, name, target_type, target_count, target_percent,
		        min_success_rate, max_failures, ack_timeout_ms, observation_window_ms, state,
		        node_snapshot, dispatch_cursor, success_count, failure_count, opened_at, completed_at
		 FROM stages WHERE rollout_id = ? ORDER BY stage_index ASC`, rolloutID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Stage
	for rows.Next() {
		var st domain.Stage
		var targetType, snap, state string
		var ackTimeout, obsWindow, opened, completed int64
		if err := rows.Scan(&st.Index, &st.Name, &targetType, &st.Target.Count, &st.Target.Percent,
			&st.MinSuccessRate, &st.MaxFailures, &ackTimeout, &obsWindow, &state,
			&snap, &st.DispatchCursor, &st.SuccessCount, &st.FailureCount, &opened, &completed); err != nil {
			return nil, err
		}
		st.Target.Kind = domain.StageTargetKind(targetType)
		st.State = domain.StageState(state)
		st.AckTimeout = time.Duration(ackTimeout)
		st.ObservationWindow = time.Duration(obsWindow)
		st.NodeSnapshot, _ = parseIDs(snap)
		st.OpenedAt = nanoToTime(opened)
		st.CompletedAt = nanoToTime(completed)
		out = append(out, st)
	}
	return out, rows.Err()
}

// ListRollouts returns all rollout IDs.
func (s *Store) ListRollouts(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM rollouts ORDER BY created_at ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// ListActiveRolloutIDs returns IDs of rollouts whose state may still have
// scheduled work (not terminal).
func (s *Store) ListActiveRolloutIDs(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id FROM rollouts WHERE state IN ('running','paused','rolling_back') ORDER BY created_at ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// ---------- Acknowledgements ----------

// RecordAckResult reports what happened when an ack was recorded.
type RecordAckResult struct {
	Inserted      bool             // true if a new ack row was created
	AppliesToProgress bool         // true if the ack advances the current attempt
	Reason        domain.AuditReason // set when the ack was audit-only
}

// RecordAck persists an ack if it is not a duplicate. The full dedup key is
// (rollout, node, configVersion, attempt, level, seq). Returns whether it was
// newly inserted. The applies flag records whether this ack advances the
// current attempt's progress (the service decides this and passes it in).
func (s *Store) RecordAck(ctx context.Context, ack domain.Ack, applies bool) (RecordAckResult, error) {
	var res RecordAckResult
	res.AppliesToProgress = applies
	err := s.InTx(ctx, func(tx *sql.Tx) error {
		// Check for an exact duplicate of the full dedup key.
		var count int
		if err := tx.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM acks WHERE rollout_id=? AND node_id=? AND config_version=? AND attempt=? AND level=? AND seq=?`,
			ack.RolloutID, ack.NodeID, int64(ack.ConfigVersion), ack.Attempt, int(ack.Level), ack.Seq,
		).Scan(&count); err != nil {
			return err
		}
		if count > 0 {
			res.Inserted = false
			res.Reason = domain.AuditDuplicate
			return nil
		}
		var applied int
		if applies {
			applied = 1
		}
		_, err := tx.ExecContext(ctx,
			`INSERT INTO acks (rollout_id, node_id, config_version, attempt, level, seq, received_at, applies)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			ack.RolloutID, ack.NodeID, int64(ack.ConfigVersion), ack.Attempt, int(ack.Level), ack.Seq, ack.ReceivedAt.UnixNano(), applied)
		if err != nil {
			if isUniqueConstraint(err) {
				res.Inserted = false
				res.Reason = domain.AuditDuplicate
				return nil
			}
			return err
		}
		res.Inserted = true
		return nil
	})
	return res, err
}

// RecordAuditAck persists an audit-only acknowledgement.
func (s *Store) RecordAuditAck(ctx context.Context, ack domain.Ack, reason domain.AuditReason) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO audit_acks (rollout_id, node_id, config_version, attempt, level, seq, received_at, reason)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		ack.RolloutID, ack.NodeID, int64(ack.ConfigVersion), ack.Attempt, int(ack.Level), ack.Seq, ack.ReceivedAt.UnixNano(), string(reason))
	return err
}

// NodeAckHighWater returns the highest ack (by the deterministic merge key)
// recorded for a (rollout, node) pair, restricted to a given attempt if
// attempt >= 1. It is used to apply the monotonic merge rule.
func (s *Store) NodeAckHighWater(ctx context.Context, rolloutID, nodeID string, attempt int) (domain.Ack, bool, error) {
	q := `SELECT config_version, attempt, level, seq, received_at FROM acks
	      WHERE rollout_id=? AND node_id=?`
	args := []any{rolloutID, nodeID}
	if attempt >= 1 {
		q += ` AND attempt=?`
		args = append(args, attempt)
	}
	q += ` ORDER BY config_version DESC, attempt DESC, level DESC, seq DESC LIMIT 1`
	var ack domain.Ack
	var cv, att, lvl, seq, recv int64
	err := s.db.QueryRowContext(ctx, q, args...).Scan(&cv, &att, &lvl, &seq, &recv)
	if errors.Is(err, sql.ErrNoRows) {
		return ack, false, nil
	}
	if err != nil {
		return ack, false, err
	}
	ack.RolloutID = rolloutID
	ack.NodeID = nodeID
	ack.ConfigVersion = domain.ConfigVersion(cv)
	ack.Attempt = int(att)
	ack.Level = domain.AckLevel(lvl)
	ack.Seq = seq
	ack.ReceivedAt = nanoToTime(recv)
	return ack, true, nil
}

// CountAcksByLevel returns the number of distinct nodes that have reached at
// least the given ack level for a specific (rollout, attempt, configVersion).
// This is the basis for stage threshold computation.
func (s *Store) CountAcksByLevel(ctx context.Context, rolloutID string, attempt int, configVersion domain.ConfigVersion, level domain.AckLevel) (int, error) {
	// Count distinct nodes whose max level for this attempt/config is >= level.
	q := `SELECT COUNT(*) FROM (
			SELECT node_id, MAX(level) AS ml FROM acks
			WHERE rollout_id=? AND attempt=? AND config_version=? AND applies=1
			GROUP BY node_id HAVING ml >= ?)`
	var n int
	err := s.db.QueryRowContext(ctx, q, rolloutID, attempt, int64(configVersion), int(level)).Scan(&n)
	return n, err
}

// NodeAckLevels returns, for the given rollout/attempt/config, the set of node
// IDs that have reached at least AckConfirmed (success) and the set that have
// reported a failure (level AckReceived with ok=false is represented as a
// failure via the success/failure counts on the stage; here we return the
// confirmed node set for threshold checks).
func (s *Store) ConfirmedNodeIDs(ctx context.Context, rolloutID string, attempt int, configVersion domain.ConfigVersion) ([]string, error) {
	q := `SELECT node_id FROM (
			SELECT node_id, MAX(level) AS ml FROM acks
			WHERE rollout_id=? AND attempt=? AND config_version=? AND applies=1
			GROUP BY node_id HAVING ml >= ?)
		  ORDER BY node_id ASC`
	rows, err := s.db.QueryContext(ctx, q, rolloutID, attempt, int64(configVersion), int(domain.AckConfirmed))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// DispatchedNodeIDs returns the set of node IDs already dispatched for a given
// rollout/stage/attempt. Used by recovery to avoid re-dispatch.
func (s *Store) DispatchedNodeIDs(ctx context.Context, rolloutID string, stageIndex, attempt int) (map[string]bool, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT node_id FROM dispatches WHERE rollout_id=? AND stage_index=? AND attempt=?`,
		rolloutID, stageIndex, attempt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out[id] = true
	}
	return out, rows.Err()
}

// RecordDispatch persists that a node was dispatched for a stage/attempt.
func (s *Store) RecordDispatch(ctx context.Context, rolloutID string, stageIndex, attempt int, nodeID string, configVersion domain.ConfigVersion, at time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO dispatches (rollout_id, stage_index, node_id, attempt, config_version, dispatched_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		rolloutID, stageIndex, attempt, nodeID, int64(configVersion), at.UnixNano())
	return err
}

// ListAuditAcks returns audit acks for a rollout.
func (s *Store) ListAuditAcks(ctx context.Context, rolloutID string) ([]domain.AuditAck, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, rollout_id, node_id, config_version, attempt, level, seq, received_at, reason
		 FROM audit_acks WHERE rollout_id=? ORDER BY id ASC`, rolloutID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.AuditAck
	for rows.Next() {
		var a domain.AuditAck
		var cv, recv int64
		var lvl, seq int64
		var att int
		var reason string
		if err := rows.Scan(&a.ID, &a.RolloutID, &a.NodeID, &cv, &att, &lvl, &seq, &recv, &reason); err != nil {
			return nil, err
		}
		a.ConfigVersion = domain.ConfigVersion(cv)
		a.Attempt = att
		a.Level = domain.AckLevel(lvl)
		a.Seq = seq
		a.ReceivedAt = nanoToTime(recv)
		a.Reason = domain.AuditReason(reason)
		out = append(out, a)
	}
	return out, rows.Err()
}
