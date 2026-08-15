// Package store implements the SQLite persistence layer for the edge feature
// flag publisher. It owns schema migrations, exposes transactional operations
// and persists every piece of state required for crash recovery: configs,
// nodes, groups, rollouts, stages, attempts, acknowledgements, audit records,
// protocol session watermarks and pending (unacknowledged) frames.
//
// The driver is modernc.org/sqlite (pure Go, no cgo) so the service builds into
// a static binary with CGO_ENABLED=0. The store is safe for concurrent use:
// database/sql manages a connection pool and SQLite's busy timeout serialises
// write transactions. Optimistic concurrency is enforced with conditional
// UPDATE statements whose affected-row count distinguishes a successful CAS
// from a stale revision conflict.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// schemaVersion is bumped whenever migrations change. Migrate() applies the
// schema idempotently and records this version.
const schemaVersion = 1

// busyTimeoutMs makes concurrent writers wait rather than fail immediately with
// SQLITE_BUSY. Combined with database/sql serialising transactions this gives
// the deterministic version-conflict behaviour the service relies on.
const busyTimeoutMs = 5000

// Store wraps a *sql.DB plus its own mutex. The mutex guards the migrate-once
// path and is NOT held during normal query execution; transactional correctness
// comes from the database.
type Store struct {
	db   *sql.DB
	path string

	mu     sync.Mutex
	closed bool
}

// Open opens (or creates) a SQLite database at path and applies migrations.
// Pass ":memory:" for an ephemeral database (used by tests).
func Open(path string) (*Store, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(%d)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)&_pragma=synchronous(NORMAL)", path, busyTimeoutMs)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite %s: %w", path, err)
	}
	// A small pool is plenty; SQLite serialises writers anyway. Set max open
	// connections conservatively to avoid spurious "database is locked".
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping sqlite %s: %w", path, err)
	}
	s := &Store{db: db, path: path}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return s, nil
}

// Close closes the underlying database.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return s.db.Close()
}

// Path returns the database file path.
func (s *Store) Path() string { return s.path }

// DB returns the underlying *sql.DB. Callers should prefer the typed methods,
// but recovery and migrations may need raw access.
func (s *Store) DB() *sql.DB { return s.db }

// InTx runs fn inside a serialisable read/write transaction. If fn returns an
// error the transaction is rolled back. The error is returned as-is.
func (s *Store) InTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
	}()
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

func nowNano() int64 { return time.Now().UnixNano() }

func nanoToTime(ns int64) time.Time {
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns).UTC()
}

func labelsJSON(labels map[string]string) (string, error) {
	if labels == nil {
		labels = map[string]string{}
	}
	b, err := json.Marshal(labels)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func parseLabels(raw string) (map[string]string, error) {
	if raw == "" {
		return map[string]string{}, nil
	}
	m := map[string]string{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil, err
	}
	return m, nil
}

func idsJSON(ids []string) (string, error) {
	if ids == nil {
		ids = []string{}
	}
	b, err := json.Marshal(ids)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func parseIDs(raw string) ([]string, error) {
	if raw == "" {
		return nil, nil
	}
	var ids []string
	if err := json.Unmarshal([]byte(raw), &ids); err != nil {
		return nil, err
	}
	return ids, nil
}

// ErrNotFound is returned by getters when a row does not exist.
var ErrNotFound = errors.New("store: not found")

func isUniqueConstraint(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	// modernc.org/sqlite reports "constraint failed: UNIQUE constraint failed:"
	return contains(msg, "UNIQUE constraint failed")
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
