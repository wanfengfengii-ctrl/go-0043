package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/edgeflag/edgeflag-wave-publisher/internal/domain"
)

// ---------- Configs ----------

// CreateConfigResult is returned by CreateConfig.
type CreateConfigResult struct {
	Config  domain.FeatureConfig
	Created bool // false when an idempotent replay returned the existing config
}

// CreateConfig persists a new immutable config version. The operation is
// idempotent on RequestKey: a replay returns the previously-created config with
// Created=false. It enforces that baseline equals the current head version and
// that the new version (head+1) is unique; a concurrent creator that wins the
// race causes the loser to receive a unique-constraint error, which the caller
// translates into a version_conflict. All checks and the insert happen in one
// transaction.
func (s *Store) CreateConfig(ctx context.Context, baseline domain.ConfigVersion, content, requestKey, createdBy string, now time.Time) (CreateConfigResult, error) {
	var res CreateConfigResult
	err := s.InTx(ctx, func(tx *sql.Tx) error {
		// Idempotency: if a request key was supplied and a config already exists
		// for it, return that config unchanged.
		if requestKey != "" {
			var existing domain.FeatureConfig
			var createdAt int64
			err := tx.QueryRowContext(ctx,
				`SELECT version, baseline_version, content, request_key, created_at, created_by
				 FROM configs WHERE request_key = ?`, requestKey,
			).Scan(&existing.Version, &existing.BaselineVersion, &existing.Content, &existing.RequestKey, &createdAt, &existing.CreatedBy)
			if err == nil {
				existing.CreatedAt = nanoToTime(createdAt)
				res.Config = existing
				res.Created = false
				return nil
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("lookup request_key: %w", err)
			}
		}
		// Determine the current head version.
		var head int64
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM configs`).Scan(&head); err != nil {
			return fmt.Errorf("read head version: %w", err)
		}
		if baseline != domain.ConfigVersion(head) {
			return errStaleBaseline
		}
		newVersion := head + 1
		_, err := tx.ExecContext(ctx,
			`INSERT INTO configs (version, baseline_version, content, request_key, created_at, created_by)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			newVersion, int64(baseline), content, requestKey, now.UnixNano(), createdBy,
		)
		if err != nil {
			if isUniqueConstraint(err) {
				return errUniqueVersion
			}
			return fmt.Errorf("insert config: %w", err)
		}
		res.Config = domain.FeatureConfig{
			Version:         domain.ConfigVersion(newVersion),
			BaselineVersion: baseline,
			Content:         content,
			RequestKey:      requestKey,
			CreatedAt:       now,
			CreatedBy:       createdBy,
		}
		res.Created = true
		return nil
	})
	return res, err
}

// Sentinel errors returned inside transactions; the service translates these
// into structured apperr values.
var (
	errStaleBaseline  = errors.New("stale baseline")
	errUniqueVersion  = errors.New("unique version conflict")
	errPlanConflict   = errors.New("plan revision conflict")
	errRowNotFound    = errors.New("row not found")
	errDuplicateAck   = errors.New("duplicate ack")
)

// IsStaleBaseline reports whether err is the internal stale-baseline sentinel.
func IsStaleBaseline(err error) bool { return errors.Is(err, errStaleBaseline) }

// IsUniqueVersion reports whether err is the internal unique-version sentinel.
func IsUniqueVersion(err error) bool { return errors.Is(err, errUniqueVersion) }

// IsPlanConflict reports whether err is the internal plan-revision sentinel.
func IsPlanConflict(err error) bool { return errors.Is(err, errPlanConflict) }

// IsRowNotFound reports whether err is the internal not-found sentinel.
func IsRowNotFound(err error) bool { return errors.Is(err, errRowNotFound) }

// IsDuplicateAck reports whether err is the internal duplicate-ack sentinel.
func IsDuplicateAck(err error) bool { return errors.Is(err, errDuplicateAck) }

// GetConfig loads a config by version.
func (s *Store) GetConfig(ctx context.Context, version domain.ConfigVersion) (domain.FeatureConfig, error) {
	var c domain.FeatureConfig
	var createdAt int64
	err := s.db.QueryRowContext(ctx,
		`SELECT version, baseline_version, content, request_key, created_at, created_by
		 FROM configs WHERE version = ?`, int64(version),
	).Scan(&c.Version, &c.BaselineVersion, &c.Content, &c.RequestKey, &createdAt, &c.CreatedBy)
	if errors.Is(err, sql.ErrNoRows) {
		return c, ErrNotFound
	}
	if err != nil {
		return c, err
	}
	c.CreatedAt = nanoToTime(createdAt)
	return c, nil
}

// HasConfig reports whether a config version exists.
func (s *Store) HasConfig(ctx context.Context, version domain.ConfigVersion) (bool, error) {
	var v int64
	err := s.db.QueryRowContext(ctx, `SELECT version FROM configs WHERE version = ?`, int64(version)).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// HeadVersion returns the current maximum config version, or 0 if none.
func (s *Store) HeadVersion(ctx context.Context) (domain.ConfigVersion, error) {
	var head int64
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM configs`).Scan(&head)
	if err != nil {
		return 0, err
	}
	return domain.ConfigVersion(head), nil
}

// ListConfigs returns all configs ordered by version ascending.
func (s *Store) ListConfigs(ctx context.Context) ([]domain.FeatureConfig, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT version, baseline_version, content, request_key, created_at, created_by
		 FROM configs ORDER BY version ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.FeatureConfig
	for rows.Next() {
		var c domain.FeatureConfig
		var createdAt int64
		if err := rows.Scan(&c.Version, &c.BaselineVersion, &c.Content, &c.RequestKey, &createdAt, &c.CreatedBy); err != nil {
			return nil, err
		}
		c.CreatedAt = nanoToTime(createdAt)
		out = append(out, c)
	}
	return out, rows.Err()
}

// ---------- Nodes ----------

// UpsertNode inserts or replaces a node by ID.
func (s *Store) UpsertNode(ctx context.Context, n domain.Node) error {
	labels, err := labelsJSON(n.Labels)
	if err != nil {
		return err
	}
	var online int
	if n.Online {
		online = 1
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO nodes (id, labels, online, group_revision, last_seen)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET labels=excluded.labels, online=excluded.online, group_revision=excluded.group_revision, last_seen=excluded.last_seen`,
		n.ID, labels, online, n.GroupRevision, n.LastSeen.UnixNano())
	return err
}

// GetNode loads a node by ID.
func (s *Store) GetNode(ctx context.Context, id string) (domain.Node, error) {
	var n domain.Node
	var labels string
	var online, gr, lastSeen int64
	err := s.db.QueryRowContext(ctx,
		`SELECT id, labels, online, group_revision, last_seen FROM nodes WHERE id = ?`, id,
	).Scan(&n.ID, &labels, &online, &gr, &lastSeen)
	if errors.Is(err, sql.ErrNoRows) {
		return n, ErrNotFound
	}
	if err != nil {
		return n, err
	}
	n.Labels, err = parseLabels(labels)
	if err != nil {
		return n, err
	}
	n.Online = online == 1
	n.GroupRevision = gr
	n.LastSeen = nanoToTime(lastSeen)
	return n, nil
}

// ListNodes returns all nodes.
func (s *Store) ListNodes(ctx context.Context) ([]domain.Node, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, labels, online, group_revision, last_seen FROM nodes ORDER BY id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Node
	for rows.Next() {
		var n domain.Node
		var labels string
		var online, gr, lastSeen int64
		if err := rows.Scan(&n.ID, &labels, &online, &gr, &lastSeen); err != nil {
			return nil, err
		}
		n.Labels, _ = parseLabels(labels)
		n.Online = online == 1
		n.GroupRevision = gr
		n.LastSeen = nanoToTime(lastSeen)
		out = append(out, n)
	}
	return out, rows.Err()
}

// ---------- Groups ----------

// UpsertGroup inserts or replaces a group, bumping its revision on update.
func (s *Store) UpsertGroup(ctx context.Context, g domain.Group) (domain.Group, error) {
	sel, err := json.Marshal(g.Selector)
	if err != nil {
		return g, err
	}
	members, err := idsJSON(g.MemberIDs)
	if err != nil {
		return g, err
	}
	now := nowNano()
	err = s.InTx(ctx, func(tx *sql.Tx) error {
		var rev int64
		err := tx.QueryRowContext(ctx, `SELECT revision FROM groups WHERE id = ?`, g.ID).Scan(&rev)
		if errors.Is(err, sql.ErrNoRows) {
			rev = 0
		} else if err != nil {
			return err
		}
		g.Revision = rev + 1
		_, err = tx.ExecContext(ctx,
			`INSERT INTO groups (id, selector, revision, member_ids, updated_at)
			 VALUES (?, ?, ?, ?, ?)
			 ON CONFLICT(id) DO UPDATE SET selector=excluded.selector, revision=excluded.revision, member_ids=excluded.member_ids, updated_at=excluded.updated_at`,
			g.ID, string(sel), g.Revision, members, now)
		return err
	})
	if err != nil {
		return g, err
	}
	g.UpdatedAt = nanoToTime(now)
	return g, nil
}

// GetGroup loads a group by ID.
func (s *Store) GetGroup(ctx context.Context, id string) (domain.Group, error) {
	var g domain.Group
	var sel, members string
	var updatedAt int64
	err := s.db.QueryRowContext(ctx,
		`SELECT id, selector, revision, member_ids, updated_at FROM groups WHERE id = ?`, id,
	).Scan(&g.ID, &sel, &g.Revision, &members, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return g, ErrNotFound
	}
	if err != nil {
		return g, err
	}
	if err := json.Unmarshal([]byte(sel), &g.Selector); err != nil {
		return g, err
	}
	g.MemberIDs, err = parseIDs(members)
	if err != nil {
		return g, err
	}
	g.UpdatedAt = nanoToTime(updatedAt)
	return g, nil
}

// ListGroups returns all groups.
func (s *Store) ListGroups(ctx context.Context) ([]domain.Group, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, selector, revision, member_ids, updated_at FROM groups ORDER BY id ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Group
	for rows.Next() {
		var g domain.Group
		var sel, members string
		var updatedAt int64
		if err := rows.Scan(&g.ID, &sel, &g.Revision, &members, &updatedAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(sel), &g.Selector)
		g.MemberIDs, _ = parseIDs(members)
		g.UpdatedAt = nanoToTime(updatedAt)
		out = append(out, g)
	}
	return out, rows.Err()
}
