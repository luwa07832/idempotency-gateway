package store

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// seedConflictRows inserts immutable conflict events directly so ordering and filters can be
// exercised without driving the conflict write path.
func seedConflictRows(t *testing.T, st *Store, now time.Time) []ConflictEvent {
	t.Helper()
	ctx := context.Background()
	specs := []struct {
		id       string
		key      string
		observed string
		existing string
		fp       string
		created  time.Duration
	}{
		{"00000000000000000000000000000001", "k", "fpA", "rec_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "fp0", -3 * time.Hour},
		{"00000000000000000000000000000002", "k", "fpB", "rec_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "fp0", -2 * time.Hour},
		{"00000000000000000000000000000003", "other", "fpB", "rec_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "fpX", -90 * time.Minute},
		{"00000000000000000000000000000004", "k", "FPB", "rec_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "fp0", -60 * time.Minute},
	}
	events := make([]ConflictEvent, 0, len(specs))
	for _, spec := range specs {
		createdAt := now.Add(spec.created)
		event := ConflictEvent{
			ID:                         "con_" + spec.id,
			IdempotencyKey:             spec.key,
			ObservedRequestFingerprint: spec.observed,
			ExistingRecordID:           spec.existing,
			ExistingRequestFingerprint: spec.fp,
			CreatedAt:                  createdAt,
		}
		if _, err := st.db.ExecContext(ctx, `
INSERT INTO idempotency_conflicts
	(id, idempotency_key, observed_request_fingerprint, existing_record_id, existing_request_fingerprint, created_at_ns)
VALUES (?, ?, ?, ?, ?, ?)`,
			event.ID, event.IdempotencyKey, event.ObservedRequestFingerprint,
			event.ExistingRecordID, event.ExistingRequestFingerprint, createdAt.UnixNano()); err != nil {
			t.Fatalf("insert %s: %v", spec.id, err)
		}
		events = append(events, event)
	}
	return events
}

func TestListConflictsNewestFirstWithFixedShape(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	seeded := seedConflictRows(t, st, now)

	events, err := st.ListConflictEvents(ctx, ConflictFilter{Limit: 50})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(events) != 4 {
		t.Fatalf("events = %d, want 4", len(events))
	}
	wantIDs := []string{seeded[3].ID, seeded[2].ID, seeded[1].ID, seeded[0].ID}
	for i, want := range wantIDs {
		if events[i].ID != want {
			t.Fatalf("event %d = %s, want %s", i, events[i].ID, want)
		}
	}
	first := events[0]
	if first.ObservedRequestFingerprint != "FPB" || first.ExistingRequestFingerprint != "fp0" ||
		first.ExistingRecordID != "rec_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("newest event fields mismatch: %+v", first)
	}
}

func TestListConflictsFiltersAreExact(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	seedConflictRows(t, st, now)

	byKey, err := st.ListConflictEvents(ctx, ConflictFilter{IdempotencyKey: "k", Limit: 50})
	if err != nil || len(byKey) != 3 {
		t.Fatalf("key filter: events=%v err=%v", byKey, err)
	}

	byObserved, err := st.ListConflictEvents(ctx, ConflictFilter{ObservedRequestFingerprint: "fpB", Limit: 50})
	if err != nil || len(byObserved) != 2 {
		t.Fatalf("observed filter: events=%v err=%v", byObserved, err)
	}
	caseSensitive, err := st.ListConflictEvents(ctx, ConflictFilter{ObservedRequestFingerprint: "FPB", Limit: 50})
	if err != nil || len(caseSensitive) != 1 {
		t.Fatalf("observed must match character-for-character: events=%v err=%v", caseSensitive, err)
	}
	prefix, err := st.ListConflictEvents(ctx, ConflictFilter{ObservedRequestFingerprint: "fp", Limit: 50})
	if err != nil || len(prefix) != 0 {
		t.Fatalf("observed must not prefix-match: events=%v err=%v", prefix, err)
	}

	recordID := "rec_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	byRecord, err := st.ListConflictEvents(ctx, ConflictFilter{ExistingRecordID: recordID, Limit: 50})
	if err != nil || len(byRecord) != 3 {
		t.Fatalf("existing_record_id filter: events=%v err=%v", byRecord, err)
	}
	missingRecord, err := st.ListConflictEvents(ctx, ConflictFilter{
		ExistingRecordID: "rec_00000000000000000000000000000099", Limit: 50,
	})
	if err != nil || len(missingRecord) != 0 {
		t.Fatalf("unknown existing_record_id: events=%v err=%v", missingRecord, err)
	}

	combined, err := st.ListConflictEvents(ctx, ConflictFilter{
		IdempotencyKey: "k", ObservedRequestFingerprint: "fpB", ExistingRecordID: recordID, Limit: 50,
	})
	if err != nil || len(combined) != 1 || combined[0].ID != "con_00000000000000000000000000000002" {
		t.Fatalf("combined filter: events=%v err=%v", combined, err)
	}
}

func TestListConflictsPaginationAndCursorValidation(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	seeded := seedConflictRows(t, st, now)

	page1, err := st.ListConflictEvents(ctx, ConflictFilter{Limit: 2})
	if err != nil || len(page1) != 2 {
		t.Fatalf("page1: events=%v err=%v", page1, err)
	}
	if page1[0].ID != seeded[3].ID || page1[1].ID != seeded[2].ID {
		t.Fatalf("page1 order: %v", page1)
	}
	page2, err := st.ListConflictEvents(ctx, ConflictFilter{
		Limit:           2,
		CursorCreatedAt: page1[1].CreatedAt,
		CursorID:        page1[1].ID,
	})
	if err != nil || len(page2) != 2 {
		t.Fatalf("page2: events=%v err=%v", page2, err)
	}
	if page2[0].ID != seeded[1].ID || page2[1].ID != seeded[0].ID {
		t.Fatalf("page2 order: %v", page2)
	}
	page3, err := st.ListConflictEvents(ctx, ConflictFilter{
		Limit:           2,
		CursorCreatedAt: page2[1].CreatedAt,
		CursorID:        page2[1].ID,
	})
	if err != nil || len(page3) != 0 {
		t.Fatalf("page3 should be empty: events=%v err=%v", page3, err)
	}

	// A cursor aimed at a row excluded by the active filter is rejected.
	if _, err := st.ListConflictEvents(ctx, ConflictFilter{
		IdempotencyKey:  "k",
		Limit:           2,
		CursorCreatedAt: seeded[2].CreatedAt,
		CursorID:        seeded[2].ID,
	}); err != ErrCursorTarget {
		t.Fatalf("cross-filter cursor err = %v, want ErrCursorTarget", err)
	}
	if _, err := st.ListConflictEvents(ctx, ConflictFilter{
		Limit: 2, CursorCreatedAt: seeded[0].CreatedAt, CursorID: "con_not-a-valid-id",
	}); err != ErrInvalidID {
		t.Fatalf("malformed cursor id err = %v, want ErrInvalidID", err)
	}
	if _, err := st.ListConflictEvents(ctx, ConflictFilter{
		Limit:           2,
		CursorCreatedAt: seeded[0].CreatedAt,
		CursorID:        "con_" + fmt.Sprintf("%032d", 99),
	}); err != ErrCursorTarget {
		t.Fatalf("missing cursor event err = %v, want ErrCursorTarget", err)
	}
}

func TestListConflictsTieBreaksByIDDESC(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	created := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	lowerID := "con_" + "11111111111111111111111111111111"
	higherID := "con_" + "22222222222222222222222222222222"
	for _, id := range []string{lowerID, higherID} {
		if _, err := st.db.ExecContext(ctx, `
INSERT INTO idempotency_conflicts
	(id, idempotency_key, observed_request_fingerprint, existing_record_id, existing_request_fingerprint, created_at_ns)
VALUES (?, ?, ?, ?, ?, ?)`,
			id, "tie", "fp", "rec_00000000000000000000000000000001", "fp0", created.UnixNano()); err != nil {
			t.Fatalf("insert tie row: %v", err)
		}
	}
	page1, err := st.ListConflictEvents(ctx, ConflictFilter{IdempotencyKey: "tie", Limit: 1})
	if err != nil || len(page1) != 1 || page1[0].ID != higherID {
		t.Fatalf("tie page1: events=%v err=%v", page1, err)
	}
	page2, err := st.ListConflictEvents(ctx, ConflictFilter{
		IdempotencyKey: "tie", Limit: 1,
		CursorCreatedAt: page1[0].CreatedAt, CursorID: page1[0].ID,
	})
	if err != nil || len(page2) != 1 || page2[0].ID != lowerID {
		t.Fatalf("tie page2: events=%v err=%v", page2, err)
	}
}

func TestIsConflictIDShape(t *testing.T) {
	cases := map[string]bool{
		"con_0123456789abcdef0123456789abcdef":  true,
		"con_0000000000000000000000000000000a":  true,
		"con_0123456789abcdef0123456789abcde":   false,
		"con_0123456789abcdef0123456789abcdef0": false,
		"CON_0123456789abcdef0123456789abcdef":  false,
		"con_0123456789ABCDEF0123456789abcdef":  false,
		"rec_0123456789abcdef0123456789abcdef":  false,
		"":                                      false,
	}
	for id, want := range cases {
		if got := IsConflictID(id); got != want {
			t.Errorf("IsConflictID(%q) = %v, want %v", id, got, want)
		}
	}
}
