package store

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func seedConflictEvent(t *testing.T, st *Store, key, observed, existingID, existingFP string, createdAt time.Time) ConflictEvent {
	t.Helper()
	id, err := NewConflictID()
	if err != nil {
		t.Fatalf("conflict id: %v", err)
	}
	event := ConflictEvent{
		ID:                         id,
		IdempotencyKey:             key,
		ObservedRequestFingerprint: observed,
		ExistingRecordID:           existingID,
		ExistingRequestFingerprint: existingFP,
		CreatedAt:                  createdAt,
	}
	if _, err := st.db.ExecContext(context.Background(), `
INSERT INTO idempotency_conflict_events
	(id, idempotency_key, observed_request_fingerprint, existing_record_id, existing_request_fingerprint, created_at_ns)
VALUES (?, ?, ?, ?, ?, ?)`,
		event.ID, event.IdempotencyKey, event.ObservedRequestFingerprint,
		event.ExistingRecordID, event.ExistingRequestFingerprint, event.CreatedAt.UnixNano()); err != nil {
		t.Fatalf("insert conflict event: %v", err)
	}
	return event
}

func conflictShapeID(t *testing.T, id string) {
	t.Helper()
	if !strings.HasPrefix(id, "con_") || len(id) != len("con_")+32 {
		t.Fatalf("conflict id %q has wrong shape", id)
	}
	for _, ch := range id[len("con_"):] {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			t.Fatalf("conflict id %q has non-lowercase-hex character %q", id, ch)
		}
	}
}

func TestConflictIDShape(t *testing.T) {
	for i := 0; i < 16; i++ {
		id, err := NewConflictID()
		if err != nil {
			t.Fatalf("new conflict id: %v", err)
		}
		conflictShapeID(t, id)
	}
	valid := "con_0123456789abcdef0123456789abcdef"
	if !IsConflictID(valid) {
		t.Fatalf("valid id rejected: %s", valid)
	}
	for _, bad := range []string{
		"",
		"con_",
		"con_0123456789abcdef0123456789abcde",
		"con_0123456789abcdef0123456789abcdef0",
		"con_0123456789ABCDEF0123456789abcdef",
		"con_0123456789abcdef0123456789abcdeg",
		"rec_0123456789abcdef0123456789abcdef",
	} {
		if IsConflictID(bad) {
			t.Fatalf("invalid id accepted: %q", bad)
		}
	}
}

func TestPutConflictPersistsImmutableEventInSameCall(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)

	first := makeRecord(t, "k", "fp-a", `{"v":1}`, now, now.Add(time.Hour))
	saved, outcome, err := st.PutRecord(ctx, first, now)
	if err != nil || outcome != PutCreated {
		t.Fatalf("first put: outcome=%d err=%v", outcome, err)
	}

	loser := makeRecord(t, "k", "fp-b", `{"v":2}`, now, now.Add(time.Hour))
	existing, outcome, err := st.PutRecord(ctx, loser, now)
	if err != nil {
		t.Fatalf("conflict put: %v", err)
	}
	if outcome != PutConflict {
		t.Fatalf("outcome = %d, want conflict", outcome)
	}
	if existing.ID != saved.ID || existing.RequestFingerprint != "fp-a" {
		t.Fatalf("conflict returned %+v, want winner %s fp-a", existing, saved.ID)
	}

	events, err := st.ListConflictEvents(ctx, ConflictFilter{Limit: 50})
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	event := events[0]
	conflictShapeID(t, event.ID)
	if event.IdempotencyKey != "k" ||
		event.ObservedRequestFingerprint != "fp-b" ||
		event.ExistingRecordID != saved.ID ||
		event.ExistingRequestFingerprint != "fp-a" {
		t.Fatalf("event fields wrong: %+v", event)
	}
	if !event.CreatedAt.Equal(now) {
		t.Fatalf("event created_at = %v, want %v", event.CreatedAt, now)
	}

	// The winning record must remain byte-for-byte untouched.
	active, err := st.ActiveRecordByKey(ctx, "k", now)
	if err != nil {
		t.Fatalf("active lookup: %v", err)
	}
	if active.ID != saved.ID || string(active.ResponseSnapshot) != `{"v":1}` || active.RequestFingerprint != "fp-a" {
		t.Fatalf("winner mutated: %+v", active)
	}
}

func TestNonConflictPutsCreateNoEvents(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name        string
		key         string
		fingerprint string
		created     time.Time
		expires     time.Time
	}{
		{"first create", "create-key", "fp", now, now.Add(time.Hour)},
		{"replay", "create-key", "fp", now, now.Add(time.Hour)},
	}
	for _, tc := range cases {
		record := makeRecord(t, tc.key, tc.fingerprint, `{"v":1}`, tc.created, tc.expires)
		if _, _, err := st.PutRecord(ctx, record, tc.created); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
	}

	// Expired-then-resubmit inserts a fresh row and emits no event.
	later := now.Add(2 * time.Hour)
	renewed := makeRecord(t, "create-key", "fp", `{"v":2}`, later, later.Add(time.Hour))
	if _, outcome, err := st.PutRecord(ctx, renewed, later); err != nil || outcome != PutCreated {
		t.Fatalf("renew: outcome=%d err=%v", outcome, err)
	}

	events, err := st.ListConflictEvents(ctx, ConflictFilter{Limit: 50})
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("events = %v, want none for create/replay/renew", events)
	}
}

func TestPutConflictFailsWhenEventCannotBeStored(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)

	first := makeRecord(t, "k", "fp-a", `{"v":1}`, now, now.Add(time.Hour))
	if _, _, err := st.PutRecord(ctx, first, now); err != nil {
		t.Fatalf("first put: %v", err)
	}

	if _, err := st.db.ExecContext(ctx, `DROP TABLE idempotency_conflict_events`); err != nil {
		t.Fatalf("drop events table: %v", err)
	}

	loser := makeRecord(t, "k", "fp-b", `{"v":2}`, now, now.Add(time.Hour))
	existing, outcome, err := st.PutRecord(ctx, loser, now)
	if err == nil {
		t.Fatalf("conflict put unexpectedly succeeded: %+v outcome=%d", existing, outcome)
	}
	if existing != nil || outcome == PutConflict {
		t.Fatalf("want no conflict outcome when the event cannot be saved, got record=%+v outcome=%d", existing, outcome)
	}

	// The winner stays in place; no partial state leaks.
	active, lookupErr := st.ActiveRecordByKey(ctx, "k", now)
	if lookupErr != nil {
		t.Fatalf("active lookup: %v", lookupErr)
	}
	if active == nil || active.RequestFingerprint != "fp-a" {
		t.Fatalf("winner changed after failed conflict: %+v", active)
	}
}

func newConflictsSeed(t *testing.T, st *Store, now time.Time) []ConflictEvent {
	t.Helper()
	const recA = "rec_00000000000000000000000000000001"
	const recB = "rec_00000000000000000000000000000002"
	seeded := make([]ConflictEvent, 0, 6)
	specs := []struct {
		key, observed, existingID, existingFP string
		created                               time.Time
	}{
		{"k", "fp-x", recA, "fp-a", now.Add(-4 * time.Minute)},
		{"k", "fp-y", recA, "fp-a", now.Add(-3 * time.Minute)},
		{"other", "fp-x", recB, "fp-c", now.Add(-2 * time.Minute)},
		{"k", "fp-y", recB, "fp-c", now.Add(-1 * time.Minute)},
		{"k", "fp-z", recB, "fp-c", now},
		{"k", "FP-Y", recA, "fp-a", now.Add(-90 * time.Second)},
	}
	for _, spec := range specs {
		seeded = append(seeded, seedConflictEvent(t, st, spec.key, spec.observed, spec.existingID, spec.existingFP, spec.created))
	}
	return seeded
}

func TestListConflictEventsOrderingAndExactFilters(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	seeded := newConflictsSeed(t, st, now)

	all, err := st.ListConflictEvents(ctx, ConflictFilter{Limit: 50})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != len(seeded) {
		t.Fatalf("events = %d, want %d", len(all), len(seeded))
	}
	wantOrder := []string{seeded[4].ID, seeded[3].ID, seeded[5].ID, seeded[2].ID, seeded[1].ID, seeded[0].ID}
	for i, want := range wantOrder {
		if all[i].ID != want {
			t.Fatalf("position %d = %s, want %s", i, all[i].ID, want)
		}
	}

	byKey, err := st.ListConflictEvents(ctx, ConflictFilter{IdempotencyKey: "k", Limit: 50})
	if err != nil || len(byKey) != 5 {
		t.Fatalf("key filter rows=%d err=%v", len(byKey), err)
	}
	for _, event := range byKey {
		if event.IdempotencyKey != "k" {
			t.Fatalf("key filter leaked: %+v", event)
		}
	}

	byObserved, err := st.ListConflictEvents(ctx, ConflictFilter{ObservedRequestFingerprint: "fp-y", Limit: 50})
	if err != nil || len(byObserved) != 2 {
		t.Fatalf("observed filter rows=%d err=%v", len(byObserved), err)
	}
	for _, event := range byObserved {
		if event.ObservedRequestFingerprint != "fp-y" {
			t.Fatalf("observed filter leaked: %+v", event)
		}
	}

	// Case-sensitive: "FP-Y" is a different value from "fp-y".
	caseSensitive, err := st.ListConflictEvents(ctx, ConflictFilter{ObservedRequestFingerprint: "FP-Y", Limit: 50})
	if err != nil || len(caseSensitive) != 1 || caseSensitive[0].ID != seeded[5].ID {
		t.Fatalf("case folding applied: rows=%v err=%v", caseSensitive, err)
	}

	// Whitespace is matched literally as well.
	whitespace, err := st.ListConflictEvents(ctx, ConflictFilter{ObservedRequestFingerprint: "fp-y ", Limit: 50})
	if err != nil || len(whitespace) != 0 {
		t.Fatalf("whitespace normalized: rows=%v err=%v", whitespace, err)
	}

	byRecord, err := st.ListConflictEvents(ctx, ConflictFilter{ExistingRecordID: "rec_00000000000000000000000000000001", Limit: 50})
	if err != nil || len(byRecord) != 3 {
		t.Fatalf("existing record filter rows=%d err=%v", len(byRecord), err)
	}
	for _, event := range byRecord {
		if event.ExistingRecordID != "rec_00000000000000000000000000000001" {
			t.Fatalf("existing record filter leaked: %+v", event)
		}
	}

	combined, err := st.ListConflictEvents(ctx, ConflictFilter{
		IdempotencyKey:             "k",
		ObservedRequestFingerprint: "fp-y",
		ExistingRecordID:           "rec_00000000000000000000000000000002",
		Limit:                      50,
	})
	if err != nil || len(combined) != 1 || combined[0].ID != seeded[3].ID {
		t.Fatalf("combined filter rows=%v err=%v", combined, err)
	}

	none, err := st.ListConflictEvents(ctx, ConflictFilter{IdempotencyKey: "missing", Limit: 50})
	if err != nil || len(none) != 0 {
		t.Fatalf("missing key rows=%v err=%v", none, err)
	}
}

func TestListConflictEventsPaginatesWithStableCursor(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	seeded := newConflictsSeed(t, st, now)

	var collected []string
	cursorCreatedAt := time.Time{}
	cursorID := ""
	for page := 0; page < 10; page++ {
		rows, err := st.ListConflictEvents(ctx, ConflictFilter{
			IdempotencyKey:  "k",
			CursorCreatedAt: cursorCreatedAt,
			CursorID:        cursorID,
			Limit:           2,
		})
		if err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			collected = append(collected, row.ID)
		}
		last := rows[len(rows)-1]
		cursorCreatedAt = last.CreatedAt
		cursorID = last.ID
	}

	want := []string{seeded[4].ID, seeded[3].ID, seeded[5].ID, seeded[1].ID, seeded[0].ID}
	if len(collected) != len(want) {
		t.Fatalf("collected %v, want %v", collected, want)
	}
	for i := range want {
		if collected[i] != want[i] {
			t.Fatalf("position %d = %s, want %s; collected=%v", i, collected[i], want[i], collected)
		}
	}
}

func TestListConflictEventsCursorValidation(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	seeded := newConflictsSeed(t, st, now)

	badShape, err := st.ListConflictEvents(ctx, ConflictFilter{
		CursorCreatedAt: now,
		CursorID:        "con_bogus",
		Limit:           10,
	})
	if err != ErrInvalidID || badShape != nil {
		t.Fatalf("malformed cursor id: rows=%v err=%v", badShape, err)
	}

	unknown, err := st.ListConflictEvents(ctx, ConflictFilter{
		CursorCreatedAt: now,
		CursorID:        "con_00000000000000000000000000000099",
		Limit:           10,
	})
	if err != ErrCursorTarget || unknown != nil {
		t.Fatalf("unknown cursor id: rows=%v err=%v", unknown, err)
	}

	// A real event excluded by the key filter is an invalid cursor for that query.
	otherKey, err := st.ListConflictEvents(ctx, ConflictFilter{
		IdempotencyKey:  "missing",
		CursorCreatedAt: seeded[2].CreatedAt,
		CursorID:        seeded[2].ID,
		Limit:           10,
	})
	if err != ErrCursorTarget || otherKey != nil {
		t.Fatalf("cursor from non-matching row: rows=%v err=%v", otherKey, err)
	}

	// A timestamp that does not match the target row is rejected too.
	mismatchedTime, err := st.ListConflictEvents(ctx, ConflictFilter{
		CursorCreatedAt: now.Add(time.Hour),
		CursorID:        seeded[0].ID,
		Limit:           10,
	})
	if err != ErrCursorTarget || mismatchedTime != nil {
		t.Fatalf("mismatched cursor time: rows=%v err=%v", mismatchedTime, err)
	}
}

func TestConflictEventsTieBreakByIDDescending(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	seedConflictEvent(t, st, "k", "fp-x", "rec_00000000000000000000000000000001", "fp-a", now)
	seedConflictEvent(t, st, "k", "fp-y", "rec_00000000000000000000000000000001", "fp-a", now)

	events, err := st.ListConflictEvents(ctx, ConflictFilter{IdempotencyKey: "k", Limit: 50})
	if err != nil || len(events) != 2 {
		t.Fatalf("rows=%v err=%v", events, err)
	}
	// Random ids make the order unpredictable in general; assert it is deterministic and matches
	// lexicographic id descending at the same timestamp.
	if events[0].CreatedAt.Equal(events[1].CreatedAt) && events[0].ID < events[1].ID {
		t.Fatalf("tie break is not id descending: %s before %s", events[0].ID, events[1].ID)
	}
}

func TestCrossInstancesEachConflictLeavesOneEvent(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	created := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)

	winnerStore := openSharedStore(t, dir)
	winner := makeRecord(t, "race-key", "fp-winner", `{"v":"winner"}`, created, created.Add(time.Hour))
	if _, outcome, err := winnerStore.PutRecord(ctx, winner, created); err != nil || outcome != PutCreated {
		t.Fatalf("winner put: outcome=%d err=%v", outcome, err)
	}

	const racers = 8
	stores := make([]*Store, 0, racers)
	for i := 0; i < racers; i++ {
		stores = append(stores, openSharedStore(t, dir))
	}
	start := make(chan struct{})
	errCh := make(chan error, racers)
	var wg sync.WaitGroup
	for i, st := range stores {
		wg.Add(1)
		go func(instance int, st *Store) {
			defer wg.Done()
			<-start
			record := makeRecord(t, "race-key", "fp-loser-"+strconv.Itoa(instance), `{"v":"loser"}`, created, created.Add(time.Hour))
			existing, outcome, err := st.PutRecord(ctx, record, created)
			if err != nil {
				errCh <- err
				return
			}
			if outcome != PutConflict || existing.ID != winner.ID || existing.RequestFingerprint != "fp-winner" {
				errCh <- context.DeadlineExceeded
			}
		}(i, st)
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("racer error: %v", err)
	}

	events, err := stores[0].ListConflictEvents(ctx, ConflictFilter{
		IdempotencyKey: "race-key",
		Limit:          100,
	})
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) != racers {
		t.Fatalf("events = %d, want one per 409 request (%d)", len(events), racers)
	}
	seen := make(map[string]bool, len(events))
	for _, event := range events {
		if event.IdempotencyKey != "race-key" ||
			event.ExistingRecordID != winner.ID ||
			event.ExistingRequestFingerprint != "fp-winner" {
			t.Fatalf("event does not describe the race against the winner: %+v", event)
		}
		if !strings.HasPrefix(event.ObservedRequestFingerprint, "fp-loser-") {
			t.Fatalf("event lost the losing raw fingerprint: %+v", event)
		}
		if seen[event.ID] {
			t.Fatalf("duplicate event id %s", event.ID)
		}
		seen[event.ID] = true
	}
}
