package store

import (
	"context"
	"encoding/json"
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

func sampleRecord(t *testing.T, key, fingerprint string, createdAt time.Time, ttl time.Duration) Record {
	t.Helper()
	id, err := NewRecordID()
	if err != nil {
		t.Fatalf("record id: %v", err)
	}
	return Record{
		ID:                 id,
		IdempotencyKey:     key,
		RequestFingerprint: fingerprint,
		ResponseSnapshot:   json.RawMessage(`{"order":"123","status":"paid"}`),
		CreatedAt:          createdAt,
		ExpiresAt:          createdAt.Add(ttl),
	}
}

func TestPutIfAbsentReplaysFirstRecordAndRejectsFingerprintMismatch(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	createdAt := time.Now().UTC()
	first := sampleRecord(t, "key-1", "fp-a", createdAt, time.Hour)

	stored, inserted, err := st.PutIfAbsent(ctx, first)
	if err != nil {
		t.Fatalf("put first: %v", err)
	}
	if !inserted || stored.ID != first.ID {
		t.Fatalf("first put inserted=%v id=%q", inserted, stored.ID)
	}

	repeat := sampleRecord(t, "key-1", "fp-a", createdAt.Add(time.Second), time.Hour)
	got, inserted, err := st.PutIfAbsent(ctx, repeat)
	if err != nil {
		t.Fatalf("put repeat: %v", err)
	}
	if inserted || got.ID != first.ID {
		t.Fatalf("repeat created a new visible record: inserted=%v id=%q", inserted, got.ID)
	}
	if string(got.ResponseSnapshot) != string(first.ResponseSnapshot) {
		t.Fatalf("snapshot changed: %s", got.ResponseSnapshot)
	}

	conflict := sampleRecord(t, "key-1", "fp-b", createdAt.Add(2*time.Second), time.Hour)
	kept, inserted, err := st.PutIfAbsent(ctx, conflict)
	if err != nil {
		t.Fatalf("put conflict: %v", err)
	}
	if inserted {
		t.Fatalf("conflicting put inserted a record")
	}
	if kept.RequestFingerprint != "fp-a" || kept.ID != first.ID {
		t.Fatalf("original record was not preserved: %+v", kept)
	}
}

func TestActiveRecordTreatsExpiredRowsAsMissing(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	createdAt := time.Now().UTC().Add(-2 * time.Hour)
	expired := sampleRecord(t, "expired-key", "fp", createdAt, time.Hour)
	if _, _, err := st.PutIfAbsent(ctx, expired); err != nil {
		t.Fatalf("put expired: %v", err)
	}

	rec, err := st.ActiveRecord(ctx, "expired-key")
	if err != nil {
		t.Fatalf("active record: %v", err)
	}
	if rec != nil {
		t.Fatalf("expired record was exposed: %+v", rec)
	}

	rec, err = st.ActiveRecord(ctx, "missing-key")
	if err != nil {
		t.Fatalf("missing record: %v", err)
	}
	if rec != nil {
		t.Fatalf("missing key returned a record: %+v", rec)
	}

	replacement := sampleRecord(t, "expired-key", "fp-2", time.Now().UTC(), time.Hour)
	stored, inserted, err := st.PutIfAbsent(ctx, replacement)
	if err != nil {
		t.Fatalf("reput after expiry: %v", err)
	}
	if !inserted || stored.ID != replacement.ID {
		t.Fatalf("reput after expiry did not store the new record")
	}
}

func TestListActiveOrdersDescendingFiltersExpiryAndPaginates(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	base := time.Now().UTC()
	records := []Record{
		sampleRecord(t, "k1", "fp1", base, 2*time.Hour),
		sampleRecord(t, "k2", "fp2", base.Add(time.Second), 3*time.Hour),
		sampleRecord(t, "k3", "fp3", base.Add(2*time.Second), 4*time.Hour),
		sampleRecord(t, "old", "fp4", base.Add(-3*time.Hour), time.Hour),
	}
	for _, rec := range records {
		if _, _, err := st.PutIfAbsent(ctx, rec); err != nil {
			t.Fatalf("put %s: %v", rec.IdempotencyKey, err)
		}
	}

	page1, hasMore, err := st.ListActive(ctx, ListFilter{Limit: 2})
	if err != nil {
		t.Fatalf("list page 1: %v", err)
	}
	if !hasMore || len(page1) != 2 {
		t.Fatalf("page 1 hasMore=%v len=%d", hasMore, len(page1))
	}
	if page1[0].IdempotencyKey != "k3" || page1[1].IdempotencyKey != "k2" {
		t.Fatalf("page 1 order = %s, %s", page1[0].IdempotencyKey, page1[1].IdempotencyKey)
	}

	page2, hasMore, err := st.ListActive(ctx, ListFilter{
		Limit:  2,
		Cursor: &Cursor{CreatedAt: page1[1].CreatedAt, RecordID: page1[1].ID},
	})
	if err != nil {
		t.Fatalf("list page 2: %v", err)
	}
	if hasMore || len(page2) != 1 || page2[0].IdempotencyKey != "k1" {
		t.Fatalf("page 2 hasMore=%v page=%v", hasMore, page2)
	}

	boundary := base.Add(2*time.Hour + 500*time.Millisecond)
	before, _, err := st.ListActive(ctx, ListFilter{Limit: 10, ExpiresBefore: &boundary})
	if err != nil {
		t.Fatalf("list before: %v", err)
	}
	if len(before) != 1 || before[0].IdempotencyKey != "k1" {
		t.Fatalf("expires_before result = %v", before)
	}

	after := boundary
	afterPage, _, err := st.ListActive(ctx, ListFilter{Limit: 10, ExpiresAfter: &after})
	if err != nil {
		t.Fatalf("list after: %v", err)
	}
	if len(afterPage) != 2 {
		t.Fatalf("expires_after len = %d, want 2", len(afterPage))
	}
}

func TestPutIfAbsentConcurrentFirstSnapshotWins(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	createdAt := time.Now().UTC()

	const contenders = 16
	var wg sync.WaitGroup
	results := make([]string, contenders)
	wg.Add(contenders)
	for i := range contenders {
		i := i
		go func() {
			defer wg.Done()
			rec := sampleRecord(t, "race-key", "fp-race", createdAt, time.Hour)
			stored, _, err := st.PutIfAbsent(ctx, rec)
			if err != nil {
				t.Errorf("put contender %d: %v", i, err)
				return
			}
			results[i] = stored.ID
		}()
	}
	wg.Wait()

	winner := results[0]
	for _, id := range results[1:] {
		if id != winner {
			t.Fatalf("contenders observed different records: %v", results)
		}
	}
}
