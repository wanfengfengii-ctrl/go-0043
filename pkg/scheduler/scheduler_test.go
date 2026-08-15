package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/edgeflag/edgeflag-wave-publisher/pkg/clock"
)

func TestManualStableOrdering(t *testing.T) {
	clk := clock.NewFixed(time.Unix(1000, 0))
	m := NewManual(clk)

	at := clk.Now()
	var order []string
	// Deliberately enqueue out of key-order; the scheduler must sort them.
	m.Schedule(Task{At: at, RolloutID: "r2", StageIndex: 0, Kind: TaskOpenStage, Fn: func() { order = append(order, "r2") }})
	m.Schedule(Task{At: at, RolloutID: "r1", StageIndex: 1, Kind: TaskAckTimeout, Fn: func() { order = append(order, "r1-s1") }})
	m.Schedule(Task{At: at, RolloutID: "r1", StageIndex: 0, Kind: TaskAckTimeout, Fn: func() { order = append(order, "r1-s0") }})

	ran := m.Advance(at)
	if ran != 3 {
		t.Fatalf("expected 3 tasks run, got %d", ran)
	}
	want := []string{"r1-s0", "r1-s1", "r2"}
	for i, w := range want {
		if order[i] != w {
			t.Fatalf("order[%d] = %q, want %q (full: %v)", i, order[i], w, order)
		}
	}
}

func TestManualAdvanceByTime(t *testing.T) {
	clk := clock.NewFixed(time.Unix(1000, 0))
	m := NewManual(clk)
	now := clk.Now()

	var fired []int
	m.Schedule(Task{At: now.Add(5 * time.Second), RolloutID: "r1", Fn: func() { fired = append(fired, 5) }})
	m.Schedule(Task{At: now.Add(2 * time.Second), RolloutID: "r1", Fn: func() { fired = append(fired, 2) }})

	// Nothing fires before its time.
	m.Advance(now.Add(1 * time.Second))
	if len(fired) != 0 {
		t.Fatalf("expected no fires yet, got %v", fired)
	}
	m.Advance(now.Add(3 * time.Second))
	if len(fired) != 1 || fired[0] != 2 {
		t.Fatalf("expected [2], got %v", fired)
	}
	m.Advance(now.Add(10 * time.Second))
	if len(fired) != 2 || fired[1] != 5 {
		t.Fatalf("expected [2 5], got %v", fired)
	}
}

func TestTimerFires(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr := NewTimer()
	go tr.Run(ctx)

	var fired int
	var mu sync.Mutex
	done := make(chan struct{})
	tr.Schedule(Task{
		At:        time.Now().Add(20 * time.Millisecond),
		RolloutID: "r1",
		Fn: func() {
			mu.Lock()
			fired++
			mu.Unlock()
			close(done)
		},
	})
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timer task did not fire")
	}
}

func TestManualNewlyScheduledDueTaskRunsSamePass(t *testing.T) {
	clk := clock.NewFixed(time.Unix(1000, 0))
	m := NewManual(clk)
	now := clk.Now()

	var log []string
	m.Schedule(Task{
		At: now, RolloutID: "r1",
		Fn: func() {
			log = append(log, "first")
			// Schedule another task due now during the drain.
			m.Schedule(Task{At: now, RolloutID: "r1", StageIndex: 1, Fn: func() { log = append(log, "second") }})
		},
	})
	ran := m.Advance(now)
	if ran != 2 {
		t.Fatalf("expected 2 tasks run in one pass, got %d", ran)
	}
	if len(log) != 2 || log[0] != "first" || log[1] != "second" {
		t.Fatalf("unexpected log %v", log)
	}
}
