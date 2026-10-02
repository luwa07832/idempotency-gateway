package store

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func makeRecord(t *testing.T, key, fingerprint, snapshot string, createdAt, expiresAt time.Time) Record {
	t.Helper()
	id, err := NewRecordID()
	if err != nil {
		t.Fatalf("new id: %v", err)
	}
	return Record{
		ID:                 id,
		IdempotencyKey:     key,
		RequestFingerprint: fingerprint,
		ResponseSnapshot:   []byte(snapshot),
		CreatedAt:          createdAt,
		ExpiresAt:          expiresAt,
	}
}

func TestPutCreatesThenReplaysFirstSnapshot(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	created := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	expires := created.Add(time.Hour)

	first := makeRecord(t, "k1", "fp1", `{"v":1}`, created, expires)
	saved, outcome, err := st.PutRecord(ctx, first, created)
	if err != nil || outcome != PutCreated {
		t.Fatalf("first put: outcome=%d err=%v", outcome, err)
	}
	if saved.ID != first.ID {
		t.Fatalf("saved id = %s, want %s", saved.ID, first.ID)
	}

	duplicate := makeRecord(t, "k1", "fp1", `{"v":999}`, created.Add(time.Minute), expires)
	replayed, outcome, err := st.PutRecord(ctx, duplicate, created.Add(time.Minute))
	if err != nil || outcome != PutReplayed {
		t.Fatalf("duplicate put: outcome=%d err=%v", outcome, err)
	}
	if replayed.ID != first.ID {
		t.Fatalf("replay id = %s, want first id %s", replayed.ID, first.ID)
	}
	if string(replayed.ResponseSnapshot) != `{"v":1}` {
		t.Fatalf("replay snapshot = %s, want the first snapshot", replayed.ResponseSnapshot)
	}
	if !replayed.CreatedAt.Equal(created) {
		t.Fatalf("replay changed created_at: %v", replayed.CreatedAt)
	}
}

func TestPutConflictsOnDifferentFingerprintAndKeepsOriginal(t *testing.T) {
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
	if existing.ID != saved.ID || existing.RequestFingerprint != "fp1" {
		t.Fatalf("conflict returned %+v, want the original record", existing)
	}

	lookup, err := st.ActiveRecordByKey(ctx, "k1", created.Add(time.Minute))
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if lookup.ID != saved.ID || string(lookup.ResponseSnapshot) != `{"v":1}` {
		t.Fatalf("original record was modified: %+v", lookup)
	}
}

func TestActiveLookupHidesUnknownAndExpiredRecords(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	created := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)

	expired := makeRecord(t, "gone", "fp", `{"v":1}`, created.Add(-2*time.Hour), created.Add(-time.Hour))
	if _, _, err := st.PutRecord(ctx, expired, created.Add(-2*time.Hour)); err != nil {
		t.Fatalf("put expired: %v", err)
	}
	active := makeRecord(t, "alive", "fp", `{"v":2}`, created, created.Add(time.Hour))
	if _, _, err := st.PutRecord(ctx, active, created); err != nil {
		t.Fatalf("put active: %v", err)
	}

	if found, err := st.ActiveRecordByKey(ctx, "missing", created); err != nil || found != nil {
		t.Fatalf("unknown key: record=%v err=%v", found, err)
	}
	if found, err := st.ActiveRecordByKey(ctx, "gone", created); err != nil || found != nil {
		t.Fatalf("expired key: record=%v err=%v", found, err)
	}
	found, err := st.ActiveRecordByKey(ctx, "alive", created)
	if err != nil || found == nil || found.ID != active.ID {
		t.Fatalf("active key lookup failed: record=%v err=%v", found, err)
	}
}

func TestReSubmitAfterExpiryCreatesNewRecordAndKeepsHistory(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	firstAt := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	first := makeRecord(t, "k1", "fp1", `{"v":"old"}`, firstAt, firstAt.Add(time.Hour))
	if _, _, err := st.PutRecord(ctx, first, firstAt); err != nil {
		t.Fatalf("put first: %v", err)
	}

	secondAt := firstAt.Add(2 * time.Hour)
	second := makeRecord(t, "k1", "fp1", `{"v":"new"}`, secondAt, secondAt.Add(time.Hour))
	saved, outcome, err := st.PutRecord(ctx, second, secondAt)
	if err != nil || outcome != PutCreated {
		t.Fatalf("post-expiry put: outcome=%d err=%v", outcome, err)
	}
	if saved.ID == first.ID {
		t.Fatalf("post-expiry put reused the historical record id")
	}
	lookup, err := st.ActiveRecordByKey(ctx, "k1", secondAt)
	if err != nil || string(lookup.ResponseSnapshot) != `{"v":"new"}` {
		t.Fatalf("lookup after expiry: record=%v err=%v", lookup, err)
	}

	rows, err := st.ListRecords(ctx, ListFilter{Limit: 100}, secondAt)
	if err != nil || len(rows) != 1 || rows[0].ID != second.ID {
		t.Fatalf("expired history leaked into listing: rows=%v err=%v", rows, err)
	}
}

func TestListFiltersOrderAndPagination(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)

	specs := []struct {
		key     string
		offset  time.Duration
		expires time.Duration
	}{
		{"k1", 0, time.Hour},
		{"k2", time.Minute, 30 * time.Minute},
		{"k3", 2 * time.Minute, 2 * time.Hour},
		{"gone", -2 * time.Hour, -time.Hour},
	}
	for _, spec := range specs {
		at := base.Add(spec.offset)
		record := makeRecord(t, spec.key, "fp", `"snap"`, at, at.Add(spec.expires))
		if _, _, err := st.PutRecord(ctx, record, at); err != nil {
			t.Fatalf("put %s: %v", spec.key, err)
		}
	}
	now := base.Add(3 * time.Minute)

	all, err := st.ListRecords(ctx, ListFilter{Limit: 10}, now)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("list len = %d, want 3 (expired excluded)", len(all))
	}
	if all[0].IdempotencyKey != "k3" || all[1].IdempotencyKey != "k2" || all[2].IdempotencyKey != "k1" {
		t.Fatalf("list not in created_at desc order: %v", []string{all[0].IdempotencyKey, all[1].IdempotencyKey, all[2].IdempotencyKey})
	}

	byKey, err := st.ListRecords(ctx, ListFilter{IdempotencyKey: "k2", Limit: 10}, now)
	if err != nil || len(byKey) != 1 || byKey[0].IdempotencyKey != "k2" {
		t.Fatalf("key filter: rows=%v err=%v", byKey, err)
	}

	boundary := base.Add(60 * time.Minute)
	before, err := st.ListRecords(ctx, ListFilter{ExpiresBefore: &boundary, Limit: 10}, now)
	if err != nil || len(before) != 1 || before[0].IdempotencyKey != "k2" {
		t.Fatalf("expires_before filter: rows=%v err=%v", before, err)
	}
	afterBoundary := base.Add(90 * time.Minute)
	after, err := st.ListRecords(ctx, ListFilter{ExpiresAfter: &afterBoundary, Limit: 10}, now)
	if err != nil || len(after) != 1 || after[0].IdempotencyKey != "k3" {
		t.Fatalf("expires_after filter: rows=%v err=%v", after, err)
	}

	page1, err := st.ListRecords(ctx, ListFilter{Limit: 2}, now)
	if err != nil || len(page1) != 2 {
		t.Fatalf("page1: rows=%v err=%v", page1, err)
	}
	page2, err := st.ListRecords(ctx, ListFilter{
		Limit:           2,
		CursorCreatedAt: page1[1].CreatedAt,
		CursorID:        page1[1].ID,
	}, now)
	if err != nil || len(page2) != 1 || page2[0].IdempotencyKey != "k1" {
		t.Fatalf("page2: rows=%v err=%v", page2, err)
	}
	if page1[0].ID == page2[0].ID {
		t.Fatalf("cursor pagination returned an overlapping row")
	}
}

func TestListFiltersByExactRequestFingerprint(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)

	specs := []struct {
		key         string
		fingerprint string
		offset      time.Duration
		expires     time.Duration
	}{
		{"k1", "alpha", 0, time.Hour},
		{"k2", "alpha", time.Minute, time.Hour},
		{"k3", "ALPHA", 2 * time.Minute, time.Hour},
		{"k4", "beta", 3 * time.Minute, time.Hour},
		{"expired", "alpha", -2 * time.Hour, -time.Hour},
	}
	for _, spec := range specs {
		at := base.Add(spec.offset)
		record := makeRecord(t, spec.key, spec.fingerprint, `"snap"`, at, at.Add(spec.expires))
		if _, _, err := st.PutRecord(ctx, record, at); err != nil {
			t.Fatalf("put %s: %v", spec.key, err)
		}
	}
	now := base.Add(4 * time.Minute)

	rows, err := st.ListRecords(ctx, ListFilter{RequestFingerprint: "alpha", Limit: 10}, now)
	if err != nil {
		t.Fatalf("list by fingerprint: %v", err)
	}
	if len(rows) != 2 || rows[0].IdempotencyKey != "k2" || rows[1].IdempotencyKey != "k1" {
		t.Fatalf("fingerprint filter rows = %v, want k2 then k1", rows)
	}

	upper, err := st.ListRecords(ctx, ListFilter{RequestFingerprint: "ALPHA", Limit: 10}, now)
	if err != nil || len(upper) != 1 || upper[0].IdempotencyKey != "k3" {
		t.Fatalf("fingerprint matching must be case-sensitive: rows=%v err=%v", upper, err)
	}

	missing, err := st.ListRecords(ctx, ListFilter{RequestFingerprint: "alp", Limit: 10}, now)
	if err != nil || len(missing) != 0 {
		t.Fatalf("fingerprint matching must not be prefix-based: rows=%v err=%v", missing, err)
	}

	whitespace, err := st.ListRecords(ctx, ListFilter{RequestFingerprint: "alpha ", Limit: 10}, now)
	if err != nil || len(whitespace) != 0 {
		t.Fatalf("fingerprint matching must not trim whitespace: rows=%v err=%v", whitespace, err)
	}

	combined, err := st.ListRecords(ctx, ListFilter{
		IdempotencyKey:     "k1",
		RequestFingerprint: "alpha",
		Limit:              10,
	}, now)
	if err != nil || len(combined) != 1 || combined[0].IdempotencyKey != "k1" {
		t.Fatalf("fingerprint+key filter: rows=%v err=%v", combined, err)
	}

	wrongKey, err := st.ListRecords(ctx, ListFilter{
		IdempotencyKey:     "k4",
		RequestFingerprint: "alpha",
		Limit:              10,
	}, now)
	if err != nil || len(wrongKey) != 0 {
		t.Fatalf("fingerprint+key filter leaked another key: rows=%v err=%v", wrongKey, err)
	}
}

func TestListFingerprintPaginationTieBreaker(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	created := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)

	tieRecords := []Record{
		makeRecord(t, "tie-a", "same", `"snap"`, created, created.Add(time.Hour)),
		makeRecord(t, "tie-b", "same", `"snap"`, created, created.Add(time.Hour)),
	}
	for _, record := range tieRecords {
		if _, _, err := st.PutRecord(ctx, record, created); err != nil {
			t.Fatalf("put tie record: %v", err)
		}
	}

	page1, err := st.ListRecords(ctx, ListFilter{RequestFingerprint: "same", Limit: 1}, created.Add(time.Minute))
	if err != nil || len(page1) != 1 {
		t.Fatalf("tie page1: rows=%v err=%v", page1, err)
	}
	page2, err := st.ListRecords(ctx, ListFilter{
		RequestFingerprint: "same",
		Limit:              1,
		CursorCreatedAt:    page1[0].CreatedAt,
		CursorID:           page1[0].ID,
	}, created.Add(time.Minute))
	if err != nil || len(page2) != 1 || page2[0].ID == page1[0].ID {
		t.Fatalf("tie page2 overlapped page1: rows=%v err=%v", page2, err)
	}
}

func TestConcurrentPutsElectSingleFirstRecord(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	created := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)

	const writers = 32
	results := make([]string, writers)
	outcomes := make([]PutOutcome, writers)
	var wg sync.WaitGroup
	wg.Add(writers)
	for i := 0; i < writers; i++ {
		go func(i int) {
			defer wg.Done()
			record := makeRecord(t, "race", "fp", `{"v":1}`, created, created.Add(time.Hour))
			saved, outcome, err := st.PutRecord(ctx, record, created)
			if err != nil {
				t.Errorf("put %d: %v", i, err)
				return
			}
			results[i] = saved.ID
			outcomes[i] = outcome
		}(i)
	}
	wg.Wait()

	winner := results[0]
	if winner == "" {
		t.Fatalf("no winning record")
	}
	createdCount := 0
	for i, id := range results {
		if id != winner {
			t.Fatalf("writer %d saw id %s, want winner %s", i, id, winner)
		}
		if outcomes[i] == PutCreated {
			createdCount++
		}
	}
	if createdCount != 1 {
		t.Fatalf("created count = %d, want exactly 1", createdCount)
	}
}

func TestRecordByIDResolvesAnyGeneration(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 2, 10, 0, 0, 0, time.UTC)

	old := makeRecord(t, "gen", "fp", `{"v":"old" }`, now.Add(-2*time.Hour), now.Add(-time.Hour))
	if _, _, err := st.PutRecord(ctx, old, now.Add(-2*time.Hour)); err != nil {
		t.Fatalf("seed expired: %v", err)
	}
	fresh := makeRecord(t, "gen", "fp", `{"v":"new"}`, now.Add(-time.Minute), now.Add(time.Hour))
	if _, _, err := st.PutRecord(ctx, fresh, now); err != nil {
		t.Fatalf("seed active: %v", err)
	}

	for _, want := range []Record{old, fresh} {
		got, err := st.RecordByID(ctx, want.ID)
		if err != nil {
			t.Fatalf("record by id %s: %v", want.ID, err)
		}
		if got == nil {
			t.Fatalf("record %s not found", want.ID)
		}
		if got.ID != want.ID || got.IdempotencyKey != want.IdempotencyKey {
			t.Fatalf("got %+v, want %+v", got, want)
		}
		if string(got.ResponseSnapshot) != string(want.ResponseSnapshot) {
			t.Fatalf("snapshot = %q, want %q", got.ResponseSnapshot, want.ResponseSnapshot)
		}
	}

	missing, err := st.RecordByID(ctx, "rec_00000000000000000000000000000009")
	if err != nil || missing != nil {
		t.Fatalf("missing id = %+v, %v", missing, err)
	}
}

func TestIsRecordIDShape(t *testing.T) {
	id, err := NewRecordID()
	if err != nil {
		t.Fatalf("new id: %v", err)
	}
	if !IsRecordID(id) {
		t.Fatalf("generated id %q rejected", id)
	}
	for _, bad := range []string{
		"", "rec_", "rec_" + "0000000000000000000000000000000",
		"rec_" + "000000000000000000000000000000000",
		"rec_" + "0000000000000000000000000000000g",
		"REC_" + "00000000000000000000000000000001",
	} {
		if IsRecordID(bad) {
			t.Fatalf("bad id %q accepted", bad)
		}
	}
}
