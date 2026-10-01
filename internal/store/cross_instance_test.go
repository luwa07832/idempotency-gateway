package store

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// openSharedStore opens an independent Store on the same database file, simulating another service
// instance in a different process: no in-memory locks are shared.
func openSharedStore(t *testing.T, path string) *Store {
	t.Helper()
	st, err := Open(filepath.Join(path, "shared.db"))
	if err != nil {
		t.Fatalf("open shared store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

// TestCrossInstancesSameFingerprintElectSingleRecord makes several independent stores submit the
// same key with the same fingerprint concurrently. Exactly one row is created and every caller
// replays the winner byte-for-byte.
func TestCrossInstancesSameFingerprintElectSingleRecord(t *testing.T) {
	dir := t.TempDir()
	const instances = 6
	stores := make([]*Store, instances)
	for i := range stores {
		stores[i] = openSharedStore(t, dir)
	}
	ctx := context.Background()
	created := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)

	type result struct {
		id      string
		outcome PutOutcome
		snap    string
	}
	resultCh := make(chan result, instances*2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, st := range stores {
		for j := 0; j < 2; j++ {
			wg.Add(1)
			go func(instance, call int, st *Store) {
				defer wg.Done()
				<-start
				snapshot := fmt.Sprintf(`{"v":%d}`, instance*10+call)
				record := makeRecord(t, "shared-key", "fp-same", snapshot, created, created.Add(time.Hour))
				saved, outcome, err := st.PutRecord(ctx, record, created)
				if err != nil {
					t.Errorf("instance %d call %d: %v", instance, call, err)
					return
				}
				resultCh <- result{id: saved.ID, outcome: outcome, snap: string(saved.ResponseSnapshot)}
			}(i, j, st)
		}
	}
	close(start)
	wg.Wait()
	close(resultCh)

	var winnerID, winnerSnap string
	createdCount := 0
	results := make([]result, 0, instances*2)
	for result := range resultCh {
		results = append(results, result)
		if winnerID == "" && result.outcome == PutCreated {
			winnerID = result.id
			winnerSnap = result.snap
		}
		if result.outcome == PutCreated {
			createdCount++
		}
		if result.outcome == PutConflict {
			t.Fatalf("same-fingerprint submission reported a conflict: %+v", result)
		}
	}
	if createdCount != 1 {
		t.Fatalf("created count = %d, want exactly 1", createdCount)
	}

	// Every caller must have observed the same winning identity and snapshot.
	for _, result := range results {
		if result.id != winnerID || result.snap != winnerSnap {
			t.Fatalf("caller saw id %s snap %s, want winner %s snap %s",
				result.id, result.snap, winnerID, winnerSnap)
		}
	}
	for _, st := range stores {
		active, err := st.ActiveRecordByKey(ctx, "shared-key", created)
		if err != nil {
			t.Fatalf("lookup: %v", err)
		}
		if active == nil || active.ID != winnerID || string(active.ResponseSnapshot) != winnerSnap {
			t.Fatalf("instance saw record %+v, want winner %s snap %s", active, winnerID, winnerSnap)
		}
	}

	rows, err := stores[0].ListRecords(ctx, ListFilter{Limit: 1000}, created)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	visible := 0
	for _, row := range rows {
		if row.IdempotencyKey == "shared-key" {
			visible++
		}
	}
	if visible != 1 {
		t.Fatalf("visible rows for shared-key = %d, want 1", visible)
	}
}

// TestCrossInstancesDifferentFingerprintOnlyWinnerSucceeds makes independent stores race with
// different fingerprints. Exactly one submission is created; every other submission gets the
// conflict outcome carrying the winner's fingerprint, and the winner's snapshot is never touched.
func TestCrossInstancesDifferentFingerprintOnlyWinnerSucceeds(t *testing.T) {
	dir := t.TempDir()
	const instances = 8
	stores := make([]*Store, instances)
	for i := range stores {
		stores[i] = openSharedStore(t, dir)
	}
	ctx := context.Background()
	created := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)

	type result struct {
		id          string
		fingerprint string
		snap        string
		outcome     PutOutcome
		err         error
	}
	resultCh := make(chan result, instances)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, st := range stores {
		wg.Add(1)
		go func(instance int, st *Store) {
			defer wg.Done()
			<-start
			fingerprint := fmt.Sprintf("fp-%d", instance)
			record := makeRecord(t, "conflict-key", fingerprint, fmt.Sprintf(`{"v":%d}`, instance), created, created.Add(time.Hour))
			saved, outcome, err := st.PutRecord(ctx, record, created)
			if err != nil {
				resultCh <- result{err: err}
				return
			}
			resultCh <- result{id: saved.ID, fingerprint: saved.RequestFingerprint, snap: string(saved.ResponseSnapshot), outcome: outcome}
		}(i, st)
	}
	close(start)
	wg.Wait()
	close(resultCh)

	var winner result
	createdCount, replayedCount, conflictCount := 0, 0, 0
	for result := range resultCh {
		if result.err != nil {
			t.Fatalf("put error: %v", result.err)
		}
		switch result.outcome {
		case PutCreated:
			createdCount++
			winner = result
		case PutReplayed:
			replayedCount++
		case PutConflict:
			conflictCount++
			if winner.id != "" && result.id != winner.id {
				t.Fatalf("conflict pointed at %s, want winner %s", result.id, winner.id)
			}
		}
	}
	if createdCount != 1 {
		t.Fatalf("created count = %d, want 1", createdCount)
	}
	if replayedCount != 0 {
		t.Fatalf("replayed count = %d, want 0 for distinct fingerprints", replayedCount)
	}
	if conflictCount != instances-1 {
		t.Fatalf("conflict count = %d, want %d", conflictCount, instances-1)
	}

	// Every loser's conflict result must carry the winner's identity; verify by listing active rows.
	active, err := stores[0].ActiveRecordByKey(ctx, "conflict-key", created)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if active.ID != winner.id || active.RequestFingerprint != winner.fingerprint || string(active.ResponseSnapshot) != winner.snap {
		t.Fatalf("winner record mutated: %+v, want %+v", active, winner)
	}
}

// TestCrossInstancesConcurrentResubmitAfterExpiryInsertsOneNewRow races independent stores right
// after the original row has expired. Exactly one new row becomes active and the old row stays in
// the underlying history untouched.
func TestCrossInstancesConcurrentResubmitAfterExpiryInsertsOneNewRow(t *testing.T) {
	dir := t.TempDir()
	st1 := openSharedStore(t, dir)
	st2 := openSharedStore(t, dir)
	ctx := context.Background()

	firstAt := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	first := makeRecord(t, "renew-key", "fp", `{"v":"old"}`, firstAt, firstAt.Add(time.Hour))
	if _, _, err := st1.PutRecord(ctx, first, firstAt); err != nil {
		t.Fatalf("put first: %v", err)
	}

	secondAt := firstAt.Add(2 * time.Hour)
	const racers = 6
	stores := []*Store{st1, st2, openSharedStore(t, dir), openSharedStore(t, dir), openSharedStore(t, dir), openSharedStore(t, dir)}
	resultCh := make(chan PutOutcome, racers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(st *Store) {
			defer wg.Done()
			<-start
			record := makeRecord(t, "renew-key", "fp", `{"v":"new"}`, secondAt, secondAt.Add(time.Hour))
			_, outcome, err := st.PutRecord(ctx, record, secondAt)
			if err != nil {
				t.Errorf("resubmit: %v", err)
				return
			}
			resultCh <- outcome
		}(stores[i])
	}
	close(start)
	wg.Wait()
	close(resultCh)

	createdCount := 0
	for outcome := range resultCh {
		switch outcome {
		case PutCreated:
			createdCount++
		case PutConflict:
			t.Fatalf("post-expiry resubmit with same fingerprint conflicted")
		}
	}
	if createdCount != 1 {
		t.Fatalf("created count = %d, want exactly 1 new row", createdCount)
	}

	active, err := st1.ActiveRecordByKey(ctx, "renew-key", secondAt)
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if string(active.ResponseSnapshot) != `{"v":"new"}` || active.ID == first.ID {
		t.Fatalf("active record after race = %+v, want the new row", active)
	}

	rows, err := st1.ListRecords(ctx, ListFilter{Limit: 1000}, secondAt)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	visible := 0
	for _, row := range rows {
		if row.IdempotencyKey == "renew-key" {
			visible++
		}
	}
	if visible != 1 {
		t.Fatalf("visible renewed rows = %d, want 1", visible)
	}

	// Historical rows remain physically present and readable past the active-record boundary.
	var count int
	if err := st1.db.QueryRowContext(ctx,
		`SELECT count(*) FROM idempotency_records WHERE idempotency_key = 'renew-key'`).Scan(&count); err != nil {
		t.Fatalf("count history: %v", err)
	}
	if count != 2 {
		t.Fatalf("physical rows for renew-key = %d, want 2 (history preserved)", count)
	}
}

// TestCrossInstancesStressMixingKeys fans out many independent stores and keys. Each key must end
// with exactly one visible record carrying one winning snapshot.
func TestCrossInstancesStressMixingKeys(t *testing.T) {
	dir := t.TempDir()
	const instances = 4
	const keys = 10
	const perKey = 8
	stores := make([]*Store, instances)
	for i := range stores {
		stores[i] = openSharedStore(t, dir)
	}
	ctx := context.Background()
	created := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)

	type outcome struct {
		key string
		id  string
	}
	outcomes := make(chan outcome, instances*keys*perKey)
	start := make(chan struct{})
	var wg sync.WaitGroup
	total := 0
	for keyIdx := 0; keyIdx < keys; keyIdx++ {
		for call := 0; call < perKey; call++ {
			wg.Add(1)
			total++
			go func(keyIdx int, st *Store) {
				defer wg.Done()
				<-start
				key := fmt.Sprintf("key-%d", keyIdx)
				record := makeRecord(t, key, "fp", `{"v":1}`, created, created.Add(time.Hour))
				saved, _, err := st.PutRecord(ctx, record, created)
				if err != nil {
					t.Errorf("put: %v", err)
					return
				}
				outcomes <- outcome{key: key, id: saved.ID}
			}(keyIdx, stores[(keyIdx+call)%instances])
		}
	}
	close(start)
	wg.Wait()
	close(outcomes)
	if total != keys*perKey {
		t.Fatalf("total = %d", total)
	}

	winners := map[string]string{}
	for result := range outcomes {
		if winner, ok := winners[result.key]; ok {
			if winner != result.id {
				t.Fatalf("key %s observed two winners: %s and %s", result.key, winner, result.id)
			}
		} else {
			winners[result.key] = result.id
		}
	}
	if len(winners) != keys {
		t.Fatalf("winner count = %d, want %d", len(winners), keys)
	}

	rows, err := stores[0].ListRecords(ctx, ListFilter{Limit: 1000}, created)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != keys {
		t.Fatalf("visible rows = %d, want %d", len(rows), keys)
	}
}
