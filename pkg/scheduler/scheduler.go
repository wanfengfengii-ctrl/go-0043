// Package scheduler provides a deterministic task scheduler abstraction used
// by the control service to open stages, fire ack timeouts and schedule
// retries.
//
// Two implementations are provided:
//
//   - Manual advances time only when a test calls Advance. Tasks scheduled for
//     the same instant run in a stable order determined by (rollout id, stage
//     index, node id, attempt, sequence). This makes batch behaviour
//     reproducible without depending on wall-clock races.
//
//   - Timer drives a real time.Timer per task for production use.
//
// The scheduler is intentionally minimal: a Task is just a time and a callback.
// The service owns the meaning of each task.
package scheduler

import (
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/edgeflag/edgeflag-wave-publisher/pkg/clock"
)

// TaskKind labels what a task does, purely for diagnostics and stable ordering.
type TaskKind int

const (
	TaskOpenStage TaskKind = iota
	TaskAckTimeout
	TaskRetry
	TaskObserveEnd
)

// String returns a human-readable kind name.
func (k TaskKind) String() string {
	switch k {
	case TaskOpenStage:
		return "open_stage"
	case TaskAckTimeout:
		return "ack_timeout"
	case TaskRetry:
		return "retry"
	case TaskObserveEnd:
		return "observe_end"
	default:
		return "unknown"
	}
}

// Task is a unit of scheduled work. The ordering fields (At, RolloutID,
// StageIndex, NodeID, Attempt, Seq) define a total order so that tasks at the
// same instant execute deterministically.
type Task struct {
	At         time.Time
	RolloutID  string
	StageIndex int
	NodeID     string
	Attempt    int
	Seq        int64 // monotonic insertion sequence, final tiebreaker
	Kind       TaskKind
	Fn         func()
}

// less reports whether task a should run before task b.
func (a Task) less(b Task) bool {
	if !a.At.Equal(b.At) {
		return a.At.Before(b.At)
	}
	if a.RolloutID != b.RolloutID {
		return a.RolloutID < b.RolloutID
	}
	if a.StageIndex != b.StageIndex {
		return a.StageIndex < b.StageIndex
	}
	if a.NodeID != b.NodeID {
		return a.NodeID < b.NodeID
	}
	if a.Attempt != b.Attempt {
		return a.Attempt < b.Attempt
	}
	if a.Kind != b.Kind {
		return a.Kind < b.Kind
	}
	return a.Seq < b.Seq
}

// taskHeap is a min-heap of Tasks by the less relation.
type taskHeap []Task

func (h taskHeap) Len() int { return len(h) }
func (h taskHeap) Less(i, j int) bool {
	return h[i].less(h[j])
}
func (h taskHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *taskHeap) Push(x any)   { *h = append(*h, x.(Task)) }
func (h *taskHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// Scheduler is the abstraction both implementations satisfy.
type Scheduler interface {
	// Schedule enqueues a task. It is safe for concurrent use.
	Schedule(t Task)
	// Run starts the scheduler loop. For the Manual scheduler this blocks
	// until the context is cancelled; tasks only fire via Advance. For the
	// Timer scheduler this processes tasks as wall time elapses.
	Run(ctx context.Context)
}

// Manual is a deterministic, clock-driven scheduler for tests. Tasks fire only
// when Advance moves the logical clock forward past their scheduled time. The
// scheduler reads its time from the provided clock, but Advance is the single
// way to advance it; this keeps ordering fully under test control.
type Manual struct {
	mu   sync.Mutex
	clk  *clock.Fixed
	heap taskHeap
	seq  int64
	next chan struct{}
	stop chan struct{}
}

// NewManual builds a manual scheduler anchored at the given fixed clock.
func NewManual(clk *clock.Fixed) *Manual {
	return &Manual{
		clk:  clk,
		next: make(chan struct{}, 1),
		stop: make(chan struct{}),
	}
}

// Schedule enqueues a task.
func (m *Manual) Schedule(t Task) {
	m.mu.Lock()
	m.seq++
	if t.Seq == 0 {
		t.Seq = m.seq
	}
	heap.Push(&m.heap, t)
	m.mu.Unlock()
	select {
	case m.next <- struct{}{}:
	default:
	}
}

// Advance moves time forward to (or by) the given target and runs every task
// whose scheduled At is <= the new time, in stable order. If a task schedules
// another task due now, that new task also runs in the same Advance pass (the
// loop drains until no due tasks remain). Returns the number of tasks run.
func (m *Manual) Advance(target time.Time) int {
	m.clk.Set(target)
	return m.drain()
}

// AdvanceBy moves time forward by d and runs due tasks.
func (m *Manual) AdvanceBy(d time.Duration) int {
	return m.Advance(m.clk.Now().Add(d))
}

func (m *Manual) drain() int {
	var ran int
	for {
		m.mu.Lock()
		if m.heap.Len() == 0 {
			m.mu.Unlock()
			return ran
		}
		top := m.heap[0]
		if top.At.After(m.clk.Now()) {
			m.mu.Unlock()
			return ran
		}
		heap.Pop(&m.heap)
		m.mu.Unlock()
		if top.Fn != nil {
			top.Fn()
		}
		ran++
	}
}

// Now returns the scheduler clock's current time.
func (m *Manual) Now() time.Time { return m.clk.Now() }

// Pending returns the number of tasks not yet fired.
func (m *Manual) Pending() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.heap.Len()
}

// Run satisfies the Scheduler interface. For Manual it blocks until the context
// is cancelled; it does nothing because time only advances via Advance.
func (m *Manual) Run(ctx context.Context) {
	<-ctx.Done()
}

// Timer is a real-time scheduler backed by time.Timer. It is safe for
// concurrent use.
type Timer struct {
	mu    sync.Mutex
	heap  taskHeap
	seq   int64
	wake  chan struct{}
	clock clock.Clock
}

// NewTimer builds a real-time scheduler using the wall clock.
func NewTimer() *Timer {
	return &Timer{wake: make(chan struct{}, 1), clock: clock.Real{}}
}

// Schedule enqueues a task.
func (t *Timer) Schedule(task Task) {
	t.mu.Lock()
	t.seq++
	if task.Seq == 0 {
		task.Seq = t.seq
	}
	heap.Push(&t.heap, task)
	t.mu.Unlock()
	select {
	case t.wake <- struct{}{}:
	default:
	}
}

// Run processes tasks as wall time elapses until the context is cancelled.
func (t *Timer) Run(ctx context.Context) {
	var timer *time.Timer
	for {
		t.mu.Lock()
		var next time.Time
		if t.heap.Len() > 0 {
			next = t.heap[0].At
		}
		t.mu.Unlock()

		if timer != nil {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer = nil
		}
		if !next.IsZero() {
			d := time.Until(next)
			if d < 0 {
				d = 0
			}
			timer = time.NewTimer(d)
		}

		select {
		case <-ctx.Done():
			return
		case <-t.wake:
			continue
		case <-func() <-chan time.Time {
			if timer == nil {
				return nil
			}
			return timer.C
		}():
			t.drain(ctx)
		}
	}
}

func (t *Timer) drain(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		t.mu.Lock()
		if t.heap.Len() == 0 {
			t.mu.Unlock()
			return
		}
		top := t.heap[0]
		if top.At.After(t.clock.Now()) {
			t.mu.Unlock()
			return
		}
		heap.Pop(&t.heap)
		t.mu.Unlock()
		if top.Fn != nil {
			top.Fn()
		}
	}
}

// Pending returns the number of tasks not yet fired.
func (t *Timer) Pending() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.heap.Len()
}
