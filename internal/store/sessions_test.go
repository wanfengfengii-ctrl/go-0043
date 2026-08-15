package store

import (
	"context"
	"testing"
)

// upsertTestSession inserts a fresh session row with the given watermarks for
// store-level watermark tests.
func upsertTestSession(t *testing.T, s *Store, sessionID int64, lastCommitted, expected, lastSent int64) {
	t.Helper()
	if err := s.UpsertSession(context.Background(), Session{
		SessionID:        sessionID,
		NodeID:           "n-store",
		LastCommittedSeq: lastCommitted,
		ExpectedSeq:      expected,
		LastSentSeq:      lastSent,
	}); err != nil {
		t.Fatalf("upsert session: %v", err)
	}
}

// TestAdvanceSessionCommitsProcessedSeqNotNextExpected pins down the inbound
// sequence commit watermark: AdvanceSession(fromSeq, toSeq) must set
// expected_seq=toSeq (the next expected) but last_committed_seq=fromSeq (the
// sequence just fully processed). The bug set last_committed_seq=toSeq, making
// the "last committed" watermark run a full sequence ahead of real progress.
func TestAdvanceSessionCommitsProcessedSeqNotNextExpected(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Fresh session: nothing committed, expecting sequence 1.
	upsertTestSession(t, s, 100, 0, 1, 0)

	// Process sequence 1: advance from expected 1 -> 2.
	newExpected, ok, err := s.AdvanceSession(ctx, 100, 1, 2)
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if !ok {
		t.Fatalf("advance CAS did not succeed")
	}
	if newExpected != 2 {
		t.Errorf("newExpected = %d, want 2", newExpected)
	}

	got, err := s.GetSession(ctx, 100)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if got.LastCommittedSeq != 1 {
		t.Errorf("last_committed_seq = %d, want 1 (the just-processed sequence, not the next expected)",
			got.LastCommittedSeq)
	}
	if got.ExpectedSeq != 2 {
		t.Errorf("expected_seq = %d, want 2 (next expected)", got.ExpectedSeq)
	}
}

// TestAdvanceSessionConsecutiveCommits verifies the watermark advances one
// frame at a time across a run of in-order commits, with last_committed_seq
// always trailing expected_seq by exactly one.
func TestAdvanceSessionConsecutiveCommits(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	upsertTestSession(t, s, 200, 0, 1, 0)

	for from, to := 1, 2; from <= 3; from, to = from+1, to+1 {
		if _, ok, err := s.AdvanceSession(ctx, 200, int64(from), int64(to)); err != nil || !ok {
			t.Fatalf("advance %d->%d: ok=%v err=%v", from, to, ok, err)
		}
		got, err := s.GetSession(ctx, 200)
		if err != nil {
			t.Fatalf("get session after %d: %v", from, err)
		}
		if got.LastCommittedSeq != int64(from) {
			t.Errorf("after committing %d: last_committed_seq=%d, want %d", from, got.LastCommittedSeq, from)
		}
		if got.ExpectedSeq != int64(to) {
			t.Errorf("after committing %d: expected_seq=%d, want %d", from, got.ExpectedSeq, to)
		}
	}
}

// TestAdvanceSessionCASRejectsStaleFromSeq ensures the watermark fix did not
// weaken the CAS guard: advancing from a stale fromSeq must report ok=false and
// leave the watermarks untouched.
func TestAdvanceSessionCASRejectsStaleFromSeq(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	upsertTestSession(t, s, 300, 1, 2, 0)

	_, ok, err := s.AdvanceSession(ctx, 300, 99, 100) // wrong fromSeq
	if err != nil {
		t.Fatalf("advance: %v", err)
	}
	if ok {
		t.Fatalf("CAS succeeded with stale fromSeq, want failure")
	}
	got, err := s.GetSession(ctx, 300)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if got.LastCommittedSeq != 1 || got.ExpectedSeq != 2 {
		t.Errorf("watermark changed on failed CAS: last_committed_seq=%d expected_seq=%d, want 1/2",
			got.LastCommittedSeq, got.ExpectedSeq)
	}
}

// TestUpsertSessionRoundTripWatermarks is a small guard that the persisted
// watermarks survive a close/reopen of the store, which is what reconnect
// recovery relies on.
func TestUpsertSessionRoundTripWatermarks(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/wm.db"
	st, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := st.UpsertSession(context.Background(), Session{
		SessionID: 7, NodeID: "n-file",
		LastCommittedSeq: 5, ExpectedSeq: 6, LastSentSeq: 9,
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	st2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = st2.Close() }()
	got, err := st2.GetSession(context.Background(), 7)
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	if got.LastCommittedSeq != 5 || got.ExpectedSeq != 6 || got.LastSentSeq != 9 {
		t.Errorf("watermark round trip: committed=%d expected=%d sent=%d, want 5/6/9",
			got.LastCommittedSeq, got.ExpectedSeq, got.LastSentSeq)
	}
}
