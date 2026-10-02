package store

import (
	"context"
	"testing"
	"time"
)

func TestPutConflictPersistsImmutableEvent(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	created := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)

	first := makeRecord(t, "k1", "fp1", `{"v":1}`, created, created.Add(time.Hour))
	saved, outcome, err := st.PutRecord(ctx, first, created)
	if err != nil || outcome != PutCreated {
		t.Fatalf("first put: outcome=%d err=%v", outcome, err)
	}

	other := makeRecord(t, "k1", "fp2", `{"v":2}`, created.Add(time.Minute), created.Add(2*time.Hour))
	existing, outcome, err := st.PutRecord(ctx, other, created.Add(time.Minute))
	if err != nil || outcome != PutConflict {
		t.Fatalf("conflict put: outcome=%d err=%v", outcome, err)
	}
	if existing.ID != saved.ID {
		t.Fatalf("conflict must point at existing record: %s", existing.ID)
	}

	events, err := st.ListConflictEvents(ctx, ConflictFilter{Limit: 50})
	if err != nil {
		t.Fatalf("list conflicts: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	event := events[0]
	if !IsConflictID(event.ID) {
		t.Fatalf("event id shape: %s", event.ID)
	}
	if event.IdempotencyKey != "k1" ||
		event.ObservedRequestFingerprint != "fp2" ||
		event.ExistingRecordID != saved.ID ||
		event.ExistingRequestFingerprint != "fp1" {
		t.Fatalf("event fields mismatch: %+v", event)
	}
	if !event.CreatedAt.Equal(created.Add(time.Minute)) {
		t.Fatalf("event created_at = %v", event.CreatedAt)
	}
}

func TestPutCreateAndReplayWriteNoEvents(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	created := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)

	first := makeRecord(t, "k1", "fp1", `{"v":1}`, created, created.Add(time.Hour))
	if _, outcome, err := st.PutRecord(ctx, first, created); err != nil || outcome != PutCreated {
		t.Fatalf("create: outcome=%d err=%v", outcome, err)
	}
	replay := makeRecord(t, "k1", "fp1", `{"v":9}`, created.Add(time.Minute), created.Add(time.Hour))
	if _, outcome, err := st.PutRecord(ctx, replay, created.Add(time.Minute)); err != nil || outcome != PutReplayed {
		t.Fatalf("replay: outcome=%d err=%v", outcome, err)
	}

	events, err := st.ListConflictEvents(ctx, ConflictFilter{Limit: 50})
	if err != nil || len(events) != 0 {
		t.Fatalf("create/replay must not write events: events=%v err=%v", events, err)
	}
}

func TestPutConflictAfterExpiryWritesNoEvent(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	firstAt := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)

	old := makeRecord(t, "k1", "fp1", `{"v":1}`, firstAt, firstAt.Add(time.Hour))
	if _, _, err := st.PutRecord(ctx, old, firstAt); err != nil {
		t.Fatalf("put old: %v", err)
	}
	// After expiry a different fingerprint is a fresh create, not a conflict event.
	secondAt := firstAt.Add(2 * time.Hour)
	fresh := makeRecord(t, "k1", "fp2", `{"v":2}`, secondAt, secondAt.Add(time.Hour))
	if _, outcome, err := st.PutRecord(ctx, fresh, secondAt); err != nil || outcome != PutCreated {
		t.Fatalf("expired resubmit: outcome=%d err=%v", outcome, err)
	}

	events, err := st.ListConflictEvents(ctx, ConflictFilter{Limit: 50})
	if err != nil || len(events) != 0 {
		t.Fatalf("expired resubmit must not write an event: events=%v err=%v", events, err)
	}
}
