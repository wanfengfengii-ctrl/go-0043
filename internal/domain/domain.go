// Package domain defines the core entities and lifecycle states of the edge
// feature-flag wave publisher. Domain types are plain data with no behaviour
// beyond validation helpers; they are shared by the persistence, service and
// API layers.
package domain

import (
	"sort"
	"time"
)

// ConfigVersion is the monotonically increasing, immutable version number of a
// feature configuration. Version 0 denotes "no config" / the implicit baseline
// and is never persisted as a real config row.
type ConfigVersion int64

// FeatureConfig is an immutable feature configuration. Once created the content
// and version never change; every creation records the baseline version it was
// derived from and an optional idempotency request key.
type FeatureConfig struct {
	Version         ConfigVersion `json:"version"`
	BaselineVersion ConfigVersion `json:"baseline_version"`
	Content         string        `json:"content"`
	RequestKey      string        `json:"request_key,omitempty"`
	CreatedAt       time.Time     `json:"created_at"`
	CreatedBy       string        `json:"created_by,omitempty"`
}

// Node is an edge node. The ID is stable for the lifetime of the node. Labels
// drive group membership selection. GroupRevision is the highest group revision
// the node has been observed to participate in.
type Node struct {
	ID            string            `json:"id"`
	Labels        map[string]string `json:"labels,omitempty"`
	Online        bool              `json:"online"`
	GroupRevision int64             `json:"group_revision"`
	LastSeen      time.Time         `json:"last_seen"`
}

// LabelSelector matches nodes whose labels contain all the given key/value
// pairs. A selector with no match labels matches every node.
type LabelSelector struct {
	MatchLabels map[string]string `json:"match_labels,omitempty"`
}

// Matches reports whether the selector matches the given labels.
func (s LabelSelector) Matches(labels map[string]string) bool {
	for k, v := range s.MatchLabels {
		if labels[k] != v {
			return false
		}
	}
	return true
}

// Group is a dynamic node grouping. MemberIDs is the computed snapshot of node
// IDs matching the selector, sorted for determinism. Revision increments on
// every change.
type Group struct {
	ID        string        `json:"id"`
	Selector  LabelSelector `json:"selector"`
	Revision  int64         `json:"revision"`
	MemberIDs []string      `json:"member_ids"`
	UpdatedAt time.Time     `json:"updated_at"`
}

// AttemptDirection distinguishes forward rollouts from rollbacks.
type AttemptDirection int

const (
	AttemptForward  AttemptDirection = 1
	AttemptRollback AttemptDirection = 2
)

func (d AttemptDirection) String() string {
	switch d {
	case AttemptForward:
		return "forward"
	case AttemptRollback:
		return "rollback"
	default:
		return "unknown"
	}
}

// Attempt is a single forward or backward pass within a rollout. Each attempt
// binds a target config version; forward attempts target the rollout's new
// config and rollback attempts target the recorded baseline.
type Attempt struct {
	Index                int              `json:"index"`
	Direction            AttemptDirection `json:"direction"`
	TargetConfigVersion  ConfigVersion    `json:"target_config_version"`
	BaselineConfigVersion ConfigVersion   `json:"baseline_config_version"`
	CreatedAt            time.Time        `json:"created_at"`
}

// RolloutState is the lifecycle state of a rollout.
type RolloutState string

const (
	StateDraft       RolloutState = "draft"
	StateRunning     RolloutState = "running"
	StatePaused      RolloutState = "paused"
	StateRollingBack RolloutState = "rolling_back"
	StateCompleted   RolloutState = "completed"
	StateRolledBack  RolloutState = "rolled_back"
	StateFailed      RolloutState = "failed"
)

// IsTerminal reports whether the state is a terminal lifecycle state.
func (s RolloutState) IsTerminal() bool {
	switch s {
	case StateCompleted, StateRolledBack, StateFailed:
		return true
	}
	return false
}

// StageState is the lifecycle state of a single stage.
type StageState string

const (
	StagePending     StageState = "pending"
	StageDispatching StageState = "dispatching"
	StageObserving   StageState = "observing"
	StageSucceeded   StageState = "succeeded"
	StageFailed      StageState = "failed"
)

// StageTargetKind selects whether a stage targets a fixed node count or a
// percentage of the group membership.
type StageTargetKind string

const (
	TargetCount   StageTargetKind = "count"
	TargetPercent StageTargetKind = "percent"
)

// StageTarget describes how many nodes a stage should dispatch to.
type StageTarget struct {
	Kind    StageTargetKind `json:"kind"`
	Count   int             `json:"count,omitempty"`
	Percent int             `json:"percent,omitempty"`
}

// Resolve returns the concrete node count for a stage given the group size.
func (t StageTarget) Resolve(groupSize int) int {
	switch t.Kind {
	case TargetPercent:
		if t.Percent <= 0 {
			return 0
		}
		if t.Percent >= 100 {
			return groupSize
		}
		n := groupSize * t.Percent / 100
		return n
	default:
		if t.Count < 0 {
			return 0
		}
		return t.Count
	}
}

// Stage is one batch of a rollout. NodeSnapshot is the immutable set of node
// IDs selected when the stage opens; subsequent group changes never alter an
// opened stage's snapshot. DispatchCursor is the index into NodeSnapshot of
// the next node to dispatch, persisted so recovery can resume dispatch.
type Stage struct {
	Index             int           `json:"index"`
	Name              string        `json:"name"`
	Target            StageTarget   `json:"target"`
	MinSuccessRate    float64       `json:"min_success_rate"`
	MaxFailures       int           `json:"max_failures"`
	AckTimeout        time.Duration `json:"ack_timeout"`
	ObservationWindow time.Duration `json:"observation_window"`
	State             StageState    `json:"state"`
	NodeSnapshot      []string      `json:"node_snapshot"`
	DispatchCursor    int           `json:"dispatch_cursor"`
	SuccessCount      int           `json:"success_count"`
	FailureCount      int           `json:"failure_count"`
	OpenedAt          time.Time     `json:"opened_at"`
	CompletedAt       time.Time     `json:"completed_at"`
}

// Rollout binds a target config version, a baseline version, a node selection
// snapshot, a plan (set of stages) and a lifecycle. PlanRevision increments on
// every successful plan modification. CurrentAttempt is the index (1-based) of
// the active attempt within Attempts.
type Rollout struct {
	ID                   string       `json:"id"`
	TargetConfigVersion  ConfigVersion `json:"target_config_version"`
	BaselineConfigVersion ConfigVersion `json:"baseline_config_version"`
	PlanRevision         int64        `json:"plan_revision"`
	Stages               []Stage      `json:"stages"`
	State                RolloutState `json:"state"`
	Attempts             []Attempt    `json:"attempts"`
	CurrentAttempt       int          `json:"current_attempt"`
	GroupID              string       `json:"group_id"`
	CreatedAt            time.Time    `json:"created_at"`
	UpdatedAt            time.Time    `json:"updated_at"`
}

// CurrentAttemptRecord returns the active attempt, or nil if none.
func (r *Rollout) CurrentAttemptRecord() *Attempt {
	if r.CurrentAttempt < 1 || r.CurrentAttempt > len(r.Attempts) {
		return nil
	}
	return &r.Attempts[r.CurrentAttempt-1]
}

// AckLevel is the monotonically ordered acknowledgement level for a node within
// an attempt. Higher levels supersede lower ones for the same
// (configVersion, attempt) pair.
type AckLevel int

const (
	AckNone      AckLevel = 0
	AckReceived  AckLevel = 1
	AckApplied   AckLevel = 2
	AckConfirmed AckLevel = 3
)

func (l AckLevel) String() string {
	switch l {
	case AckReceived:
		return "received"
	case AckApplied:
		return "applied"
	case AckConfirmed:
		return "confirmed"
	default:
		return "none"
	}
}

// Ack is a node acknowledgement of a config version within an attempt. The
// deduplication identity is (RolloutID, NodeID, ConfigVersion, Attempt, Level,
// Seq); the high-water mark identity used by the deterministic merge rule is
// (ConfigVersion, Attempt, Level, Seq) ordered lexicographically.
type Ack struct {
	RolloutID     string        `json:"rollout_id"`
	NodeID        string        `json:"node_id"`
	ConfigVersion ConfigVersion `json:"config_version"`
	Attempt       int            `json:"attempt"`
	Level         AckLevel      `json:"level"`
	Seq           int64         `json:"seq"`
	ReceivedAt    time.Time     `json:"received_at"`
}

// AckKey is the (configVersion, attempt, level, seq) ordering key used by the
// deterministic monotonic merge. ConfigVersion is ordered first so that, within
// a single attempt's recorded history, newer config versions sort above older
// ones. Note that progress advancement is gated separately on
// attempt == current attempt, which is what prevents late forward acks from
// reactivating a rolled-back rollout.
type AckKey struct {
	ConfigVersion ConfigVersion
	Attempt       int
	Level         AckLevel
	Seq           int64
}

// Key returns the ordering key for the ack.
func (a Ack) Key() AckKey {
	return AckKey{ConfigVersion: a.ConfigVersion, Attempt: a.Attempt, Level: a.Level, Seq: a.Seq}
}

// Compare orders two ack keys lexicographically by
// (ConfigVersion, Attempt, Level, Seq).
func (k AckKey) Compare(o AckKey) int {
	switch {
	case k.ConfigVersion != o.ConfigVersion:
		if k.ConfigVersion < o.ConfigVersion {
			return -1
		}
		return 1
	case k.Attempt != o.Attempt:
		if k.Attempt < o.Attempt {
			return -1
		}
		return 1
	case k.Level != o.Level:
		if k.Level < o.Level {
			return -1
		}
		return 1
	default:
		if k.Seq < o.Seq {
			return -1
		}
		if k.Seq > o.Seq {
			return 1
		}
		return 0
	}
}

// AuditReason describes why an acknowledgement was recorded in the audit table
// instead of advancing progress.
type AuditReason string

const (
	AuditDuplicate     AuditReason = "duplicate"
	AuditStaleAttempt  AuditReason = "stale_attempt"
	AuditOldVersion    AuditReason = "old_version"
	AuditUnknownConfig AuditReason = "unknown_config_version"
	AuditNotTargetNode AuditReason = "not_target_node"
)

// AuditAck is an acknowledgement recorded for audit purposes only.
type AuditAck struct {
	ID            int64     `json:"id"`
	RolloutID     string    `json:"rollout_id"`
	NodeID        string    `json:"node_id"`
	ConfigVersion ConfigVersion `json:"config_version"`
	Attempt       int       `json:"attempt"`
	Level         AckLevel  `json:"level"`
	Seq           int64     `json:"seq"`
	ReceivedAt    time.Time `json:"received_at"`
	Reason        AuditReason `json:"reason"`
}

// StageProgress summarises a stage's progress for the API.
type StageProgress struct {
	Index        int        `json:"index"`
	Name         string     `json:"name"`
	State        StageState `json:"state"`
	Target       int        `json:"target"`
	Dispatched   int        `json:"dispatched"`
	SuccessCount int        `json:"success_count"`
	FailureCount int        `json:"failure_count"`
	OpenedAt     *time.Time `json:"opened_at,omitempty"`
	CompletedAt  *time.Time `json:"completed_at,omitempty"`
}

// Progress is the rollout progress view returned by the API.
type Progress struct {
	RolloutID     string          `json:"rollout_id"`
	State         RolloutState    `json:"state"`
	PlanRevision  int64           `json:"plan_revision"`
	CurrentAttempt int           `json:"current_attempt"`
	Stages        []StageProgress `json:"stages"`
}

// SortMemberIDs returns a sorted copy of the given member IDs. Group membership
// snapshots are always sorted so that stage node selection is deterministic
// regardless of map iteration order.
func SortMemberIDs(ids []string) []string {
	out := make([]string, len(ids))
	copy(out, ids)
	sort.Strings(out)
	return out
}
