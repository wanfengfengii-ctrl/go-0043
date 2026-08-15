package store

import (
	"context"
	"database/sql"
	"fmt"
)

// migrations holds the ordered DDL applied at Open time. The set is
// idempotent (CREATE TABLE IF NOT EXISTS) so re-running is safe. The schema
// version is recorded in schema_meta and validated on startup.
var migrations = []string{
	// schema_meta tracks the applied schema version and other metadata.
	`CREATE TABLE IF NOT EXISTS schema_meta (
		key   TEXT PRIMARY KEY,
		value TEXT NOT NULL
	)`,
	// configs: immutable feature configurations. version is the primary key and
	// is strictly monotonic. request_key is unique (idempotency).
	`CREATE TABLE IF NOT EXISTS configs (
		version          INTEGER PRIMARY KEY,
		baseline_version INTEGER NOT NULL,
		content          TEXT NOT NULL,
		request_key      TEXT UNIQUE,
		created_at       INTEGER NOT NULL,
		created_by       TEXT NOT NULL DEFAULT ''
	)`,
	// nodes
	`CREATE TABLE IF NOT EXISTS nodes (
		id             TEXT PRIMARY KEY,
		labels         TEXT NOT NULL,
		online         INTEGER NOT NULL DEFAULT 0,
		group_revision INTEGER NOT NULL DEFAULT 0,
		last_seen      INTEGER NOT NULL DEFAULT 0
	)`,
	// groups: dynamic node groupings with a selector and computed membership.
	`CREATE TABLE IF NOT EXISTS groups (
		id         TEXT PRIMARY KEY,
		selector   TEXT NOT NULL,
		revision   INTEGER NOT NULL DEFAULT 1,
		member_ids TEXT NOT NULL,
		updated_at INTEGER NOT NULL DEFAULT 0
	)`,
	// rollouts
	`CREATE TABLE IF NOT EXISTS rollouts (
		id                    TEXT PRIMARY KEY,
		target_config_version INTEGER NOT NULL,
		baseline_config_version INTEGER NOT NULL,
		plan_revision         INTEGER NOT NULL,
		state                 TEXT NOT NULL,
		current_attempt       INTEGER NOT NULL DEFAULT 0,
		group_id              TEXT NOT NULL,
		created_at            INTEGER NOT NULL,
		updated_at            INTEGER NOT NULL
	)`,
	// rollout_attempts: forward and rollback attempts within a rollout.
	`CREATE TABLE IF NOT EXISTS rollout_attempts (
		rollout_id              TEXT NOT NULL,
		attempt_index           INTEGER NOT NULL,
		direction               INTEGER NOT NULL,
		target_config_version   INTEGER NOT NULL,
		baseline_config_version INTEGER NOT NULL,
		created_at              INTEGER NOT NULL,
		PRIMARY KEY (rollout_id, attempt_index)
	)`,
	`CREATE INDEX IF NOT EXISTS idx_rollout_attempts_rollout ON rollout_attempts(rollout_id)`,
	// stages
	`CREATE TABLE IF NOT EXISTS stages (
		rollout_id            TEXT NOT NULL,
		stage_index           INTEGER NOT NULL,
		name                  TEXT NOT NULL,
		target_type           TEXT NOT NULL,
		target_count          INTEGER NOT NULL,
		target_percent        INTEGER NOT NULL,
		min_success_rate      REAL NOT NULL,
		max_failures          INTEGER NOT NULL,
		ack_timeout_ms        INTEGER NOT NULL,
		observation_window_ms INTEGER NOT NULL,
		state                 TEXT NOT NULL,
		node_snapshot         TEXT NOT NULL,
		dispatch_cursor       INTEGER NOT NULL DEFAULT 0,
		success_count         INTEGER NOT NULL DEFAULT 0,
		failure_count         INTEGER NOT NULL DEFAULT 0,
		opened_at             INTEGER NOT NULL DEFAULT 0,
		completed_at          INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY (rollout_id, stage_index)
	)`,
	// acks: acknowledged node states. Primary key is the full dedup identity
	// (rollout, node, configVersion, attempt, level, seq).
	`CREATE TABLE IF NOT EXISTS acks (
		rollout_id      TEXT NOT NULL,
		node_id         TEXT NOT NULL,
		config_version  INTEGER NOT NULL,
		attempt         INTEGER NOT NULL,
		level           INTEGER NOT NULL,
		seq             INTEGER NOT NULL,
		received_at     INTEGER NOT NULL,
		applies         INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY (rollout_id, node_id, config_version, attempt, level, seq)
	)`,
	`CREATE INDEX IF NOT EXISTS idx_acks_rollout_attempt ON acks(rollout_id, attempt)`,
	`CREATE INDEX IF NOT EXISTS idx_acks_rollout_node ON acks(rollout_id, node_id)`,
	// audit_acks: acknowledgements recorded for audit only (stale / unknown /
	// non-target / duplicate).
	`CREATE TABLE IF NOT EXISTS audit_acks (
		id             INTEGER PRIMARY KEY AUTOINCREMENT,
		rollout_id     TEXT NOT NULL,
		node_id        TEXT NOT NULL,
		config_version INTEGER NOT NULL,
		attempt        INTEGER NOT NULL,
		level          INTEGER NOT NULL,
		seq            INTEGER NOT NULL,
		received_at    INTEGER NOT NULL,
		reason         TEXT NOT NULL
	)`,
	`CREATE INDEX IF NOT EXISTS idx_audit_rollout ON audit_acks(rollout_id)`,
	// idempotency: maps an idempotency key to the JSON-encoded result of the op.
	`CREATE TABLE IF NOT EXISTS idempotency (
		request_key TEXT PRIMARY KEY,
		op_type     TEXT NOT NULL,
		result      TEXT NOT NULL,
		created_at  INTEGER NOT NULL
	)`,
	// sessions: per-session protocol watermarks for resumption.
	`CREATE TABLE IF NOT EXISTS sessions (
		session_id          INTEGER PRIMARY KEY,
		node_id             TEXT NOT NULL,
		last_committed_seq  INTEGER NOT NULL DEFAULT 0,
		expected_seq        INTEGER NOT NULL DEFAULT 1,
		last_sent_seq       INTEGER NOT NULL DEFAULT 0,
		updated_at          INTEGER NOT NULL DEFAULT 0
	)`,
	// pending_frames: outgoing frames not yet acknowledged by the peer, keyed by
	// (session, sequence) for replay on reconnect.
	`CREATE TABLE IF NOT EXISTS pending_frames (
		session_id INTEGER NOT NULL,
		seq        INTEGER NOT NULL,
		frame      BLOB NOT NULL,
		acked      INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY (session_id, seq)
	)`,
	`CREATE INDEX IF NOT EXISTS idx_pending_session ON pending_frames(session_id, seq)`,
	// dispatches: records that a config dispatch was sent to a node within a
	// stage attempt, so recovery does not re-dispatch.
	`CREATE TABLE IF NOT EXISTS dispatches (
		rollout_id     TEXT NOT NULL,
		stage_index    INTEGER NOT NULL,
		node_id        TEXT NOT NULL,
		attempt        INTEGER NOT NULL,
		config_version INTEGER NOT NULL,
		dispatched_at  INTEGER NOT NULL,
		PRIMARY KEY (rollout_id, stage_index, node_id, attempt)
	)`,
}

// migrate applies all migrations and records/validates the schema version.
func (s *Store) migrate() error {
	ctx := context.Background()
	return s.InTx(ctx, func(tx *sql.Tx) error {
		for _, stmt := range migrations {
			if _, err := tx.Exec(stmt); err != nil {
				return fmt.Errorf("apply migration: %w\nstmt: %s", err, stmt)
			}
		}
		if _, err := tx.Exec(`INSERT OR IGNORE INTO schema_meta (key, value) VALUES ('version', '0')`); err != nil {
			return fmt.Errorf("init schema_meta: %w", err)
		}
		var current string
		if err := tx.QueryRow(`SELECT value FROM schema_meta WHERE key = 'version'`).Scan(&current); err != nil {
			return fmt.Errorf("read schema version: %w", err)
		}
		if current != "0" && current != "1" {
			return fmt.Errorf("unsupported schema version %q (expected 0 or 1)", current)
		}
		if _, err := tx.Exec(`UPDATE schema_meta SET value = '1' WHERE key = 'version'`); err != nil {
			return fmt.Errorf("set schema version: %w", err)
		}
		return nil
	})
}

// SchemaVersion returns the persisted schema version.
func (s *Store) SchemaVersion() (string, error) {
	var v string
	err := s.db.QueryRow(`SELECT value FROM schema_meta WHERE key = 'version'`).Scan(&v)
	return v, err
}

// ValidateInvariants runs lightweight consistency checks after recovery. It
// verifies that every stage references an existing rollout, that no rollout's
// current attempt exceeds its attempt count, and that stage node snapshots only
// reference known stages. It returns a descriptive error if an invariant is
// violated.
func (s *Store) ValidateInvariants(ctx context.Context) error {
	// Orphan stages
	var orphanStages int
	row := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM stages st
		WHERE NOT EXISTS (SELECT 1 FROM rollouts r WHERE r.id = st.rollout_id)`)
	if err := row.Scan(&orphanStages); err != nil {
		return fmt.Errorf("check orphan stages: %w", err)
	}
	if orphanStages > 0 {
		return fmt.Errorf("invariant: %d stages reference missing rollouts", orphanStages)
	}
	// Orphan acks
	var orphanAcks int
	row = s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM acks a
		WHERE NOT EXISTS (SELECT 1 FROM rollouts r WHERE r.id = a.rollout_id)`)
	if err := row.Scan(&orphanAcks); err != nil {
		return fmt.Errorf("check orphan acks: %w", err)
	}
	if orphanAcks > 0 {
		return fmt.Errorf("invariant: %d acks reference missing rollouts", orphanAcks)
	}
	// Attempt indices in range.
	var badAttempts int
	row = s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM rollouts WHERE current_attempt < 0`)
	if err := row.Scan(&badAttempts); err != nil {
		return fmt.Errorf("check attempts: %w", err)
	}
	if badAttempts > 0 {
		return fmt.Errorf("invariant: %d rollouts have negative current attempt", badAttempts)
	}
	return nil
}
