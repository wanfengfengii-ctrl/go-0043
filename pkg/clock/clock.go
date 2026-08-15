// Package clock provides a small Clock abstraction so that service code can be
// tested with a deterministic fixed clock while production uses the wall clock.
package clock

import (
	"sync"
	"time"
)

// Clock returns the current time.
type Clock interface {
	Now() time.Time
}

// Real returns the wall clock.
type Real struct{}

// Now returns the real wall time.
func (Real) Now() time.Time { return time.Now() }

// Fixed is a deterministic clock whose time only advances when a test calls
// Advance or Set. It is safe for concurrent use.
type Fixed struct {
	mu  sync.Mutex
	now time.Time
}

// NewFixed creates a fixed clock anchored at the given time.
func NewFixed(at time.Time) *Fixed {
	return &Fixed{now: at}
}

// Now returns the current fixed time.
func (f *Fixed) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Advance moves the fixed time forward by d.
func (f *Fixed) Advance(d time.Duration) time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
	return f.now
}

// Set replaces the fixed time.
func (f *Fixed) Set(at time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = at
}
