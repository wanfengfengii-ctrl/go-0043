package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Session holds the persisted protocol watermarks for a node stream session.
type Session struct {
	SessionID         int64
	NodeID            string
	LastCommittedSeq  int64 // last incoming sequence fully processed
	ExpectedSeq       int64 // next expected incoming sequence
	LastSentSeq       int64 // last outgoing sequence enqueued
	UpdatedAt         time.Time
}

// UpsertSession creates or updates a session's watermarks.
func (s *Store) UpsertSession(ctx context.Context, sess Session) error {
	now := nowNano()
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO sessions (session_id, node_id, last_committed_seq, expected_seq, last_sent_seq, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(session_id) DO UPDATE SET node_id=excluded.node_id,
		    last_committed_seq=excluded.last_committed_seq, expected_seq=excluded.expected_seq,
		    last_sent_seq=excluded.last_sent_seq, updated_at=excluded.updated_at`,
		sess.SessionID, sess.NodeID, sess.LastCommittedSeq, sess.ExpectedSeq, sess.LastSentSeq, now)
	return err
}

// GetSession loads a session by id.
func (s *Store) GetSession(ctx context.Context, sessionID int64) (Session, error) {
	var sess Session
	var updated int64
	err := s.db.QueryRowContext(ctx,
		`SELECT session_id, node_id, last_committed_seq, expected_seq, last_sent_seq, updated_at
		 FROM sessions WHERE session_id = ?`, sessionID,
	).Scan(&sess.SessionID, &sess.NodeID, &sess.LastCommittedSeq, &sess.ExpectedSeq, &sess.LastSentSeq, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return sess, ErrNotFound
	}
	if err != nil {
		return sess, err
	}
	sess.UpdatedAt = nanoToTime(updated)
	return sess, nil
}

// AdvanceSession commits an in-order incoming frame for a session. It is a
// CAS: it only advances if the current expected_seq equals fromSeq, preventing
// lost updates under concurrency. expected_seq becomes toSeq (the next expected
// incoming sequence) while last_committed_seq becomes fromSeq (the sequence
// just fully processed), so the persisted watermark never runs ahead of actual
// processing progress. Returns the new expected seq and whether the CAS
// succeeded.
func (s *Store) AdvanceSession(ctx context.Context, sessionID int64, fromSeq, toSeq int64) (int64, bool, error) {
	var newExpected int64 = toSeq
	var ok bool
	err := s.InTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx,
			`UPDATE sessions SET expected_seq=?, last_committed_seq=?, updated_at=? WHERE session_id=? AND expected_seq=?`,
			toSeq, fromSeq, nowNano(), sessionID, fromSeq)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		ok = n == 1
		return nil
	})
	return newExpected, ok, err
}

// PendingFrame is an outgoing frame awaiting peer acknowledgement.
type PendingFrame struct {
	SessionID int64
	Seq       int64
	Frame     []byte
	Acked     bool
}

// EnqueuePendingFrame stores an outgoing frame for later replay.
func (s *Store) EnqueuePendingFrame(ctx context.Context, pf PendingFrame) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO pending_frames (session_id, seq, frame, acked) VALUES (?, ?, ?, 0)`,
		pf.SessionID, pf.Seq, pf.Frame)
	if err != nil {
		return fmt.Errorf("enqueue pending frame: %w", err)
	}
	// Bump last_sent_seq.
	_, err = s.db.ExecContext(ctx,
		`UPDATE sessions SET last_sent_seq = MAX(last_sent_seq, ?), updated_at=? WHERE session_id=?`,
		pf.Seq, nowNano(), pf.SessionID)
	return err
}

// AckPendingFrame marks an outgoing frame as acknowledged (removes it).
func (s *Store) AckPendingFrame(ctx context.Context, sessionID, seq int64) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM pending_frames WHERE session_id=? AND seq=?`, sessionID, seq)
	return err
}

// PendingFramesAfter returns unacknowledged outgoing frames with seq > after,
// in ascending order, for replay on reconnect.
func (s *Store) PendingFramesAfter(ctx context.Context, sessionID, after int64) ([]PendingFrame, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT session_id, seq, frame, acked FROM pending_frames
		 WHERE session_id=? AND seq > ? ORDER BY seq ASC`, sessionID, after)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PendingFrame
	for rows.Next() {
		var pf PendingFrame
		var acked int
		if err := rows.Scan(&pf.SessionID, &pf.Seq, &pf.Frame, &acked); err != nil {
			return nil, err
		}
		pf.Acked = acked == 1
		out = append(out, pf)
	}
	return out, rows.Err()
}

// NextSentSeq allocates and returns the next outgoing sequence for a session,
// persisting the bumped last_sent_seq atomically.
func (s *Store) NextSentSeq(ctx context.Context, sessionID int64) (int64, error) {
	var seq int64
	err := s.InTx(ctx, func(tx *sql.Tx) error {
		var cur int64
		err := tx.QueryRowContext(ctx, `SELECT COALESCE(last_sent_seq, 0) FROM sessions WHERE session_id=?`, sessionID).Scan(&cur)
		if err != nil {
			return err
		}
		seq = cur + 1
		_, err = tx.ExecContext(ctx, `UPDATE sessions SET last_sent_seq=?, updated_at=? WHERE session_id=?`, seq, nowNano(), sessionID)
		return err
	})
	return seq, err
}

// ListSessions returns all sessions (for recovery).
func (s *Store) ListSessions(ctx context.Context) ([]Session, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT session_id, node_id, last_committed_seq, expected_seq, last_sent_seq, updated_at FROM sessions`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Session
	for rows.Next() {
		var sess Session
		var updated int64
		if err := rows.Scan(&sess.SessionID, &sess.NodeID, &sess.LastCommittedSeq, &sess.ExpectedSeq, &sess.LastSentSeq, &updated); err != nil {
			return nil, err
		}
		sess.UpdatedAt = nanoToTime(updated)
		out = append(out, sess)
	}
	return out, rows.Err()
}

// ---------- Idempotency ----------

// IdempotencyGet returns the stored result for an idempotency key.
func (s *Store) IdempotencyGet(ctx context.Context, key string) (string, bool, error) {
	var result string
	err := s.db.QueryRowContext(ctx, `SELECT result FROM idempotency WHERE request_key=?`, key).Scan(&result)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return result, true, nil
}

// IdempotencyPut stores a result for an idempotency key.
func (s *Store) IdempotencyPut(ctx context.Context, key, opType, result string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO idempotency (request_key, op_type, result, created_at) VALUES (?, ?, ?, ?)`,
		key, opType, result, nowNano())
	return err
}
