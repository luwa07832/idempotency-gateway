package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func makeReservation(t *testing.T, key, fingerprint string, createdAt, expiresAt time.Time) Reservation {
	t.Helper()
	id, err := NewReservationID()
	if err != nil {
		t.Fatalf("new reservation id: %v", err)
	}
	return Reservation{
		ID:                 id,
		IdempotencyKey:     key,
		RequestFingerprint: fingerprint,
		CreatedAt:          createdAt,
		ExpiresAt:          expiresAt,
	}
}

func TestReservationIDShape(t *testing.T) {
	id, err := NewReservationID()
	if err != nil {
		t.Fatalf("new id: %v", err)
	}
	if !IsReservationID(id) {
		t.Fatalf("generated id %q failed its own shape check", id)
	}
	for _, bad := range []string{"rec_00000000000000000000000000000000", "res_ABC", "res_" +
		"0000000000000000000000000000000", "res_" + "000000000000000000000000000000000"} {
		if IsReservationID(bad) {
			t.Fatalf("bad id %q accepted", bad)
		}
	}
}

func TestReserveCreateReplayConflictAndExpiry(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	created := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	expires := created.Add(time.Hour)

	first := makeReservation(t, "k1", "fp1", created, expires)
	saved, outcome, err := st.ReserveReservation(ctx, first, created)
	if err != nil || outcome != ReservationCreated {
		t.Fatalf("first reserve: outcome=%d err=%v", outcome, err)
	}
	if saved.ID != first.ID || saved.CreatedAt != created || saved.ExpiresAt != expires {
		t.Fatalf("saved reservation mismatch: %+v", saved)
	}

	replay := makeReservation(t, "k1", "fp1", created.Add(time.Minute), expires)
	replayed, outcome, err := st.ReserveReservation(ctx, replay, created.Add(time.Minute))
	if err != nil || outcome != ReservationReplayed {
		t.Fatalf("same-fingerprint reserve: outcome=%d err=%v", outcome, err)
	}
	if replayed.ID != first.ID {
		t.Fatalf("replay id = %s, want %s", replayed.ID, first.ID)
	}

	conflict := makeReservation(t, "k1", "fp-other", created, expires)
	existing, outcome, err := st.ReserveReservation(ctx, conflict, created)
	if err != nil || outcome != ReservationConflict {
		t.Fatalf("different-fingerprint reserve: outcome=%d err=%v", outcome, err)
	}
	if existing.ID != first.ID || existing.RequestFingerprint != "fp1" {
		t.Fatalf("conflict returned wrong reservation: %+v", existing)
	}

	// After the placeholder expires the key is free again: a new placeholder wins and the old
	// row remains untouched.
	next := makeReservation(t, "k1", "fp1", expires.Add(time.Minute), expires.Add(2*time.Hour))
	savedNext, outcome, err := st.ReserveReservation(ctx, next, expires.Add(time.Minute))
	if err != nil || outcome != ReservationCreated {
		t.Fatalf("post-expiry reserve: outcome=%d err=%v", outcome, err)
	}
	if savedNext.ID == first.ID {
		t.Fatalf("expired reservation was reused: %s", savedNext.ID)
	}
}

func TestCompleteReservationLifecycle(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	created := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	expires := created.Add(time.Hour)
	reservation := makeReservation(t, "k1", "fp1", created, expires)
	if _, _, err := st.ReserveReservation(ctx, reservation, created); err != nil {
		t.Fatalf("reserve: %v", err)
	}

	record, _, outcome, err := st.CompleteReservation(ctx, reservation.ID, "fp1",
		[]byte(`{"v":1}`), created.Add(time.Minute))
	if err != nil || outcome != CompleteCreated {
		t.Fatalf("first result: outcome=%d err=%v", outcome, err)
	}
	if record.ID == "" || record.CreatedAt != created || record.ExpiresAt != expires {
		t.Fatalf("record did not inherit placeholder times: %+v", record)
	}
	if string(record.ResponseSnapshot) != `{"v":1}` {
		t.Fatalf("snapshot = %s", record.ResponseSnapshot)
	}

	// A repeated result replays the first value byte-for-byte and reports CompleteReplayed.
	replayed, _, outcome, err := st.CompleteReservation(ctx, reservation.ID, "fp1",
		[]byte(`{"v":999}`), created.Add(2*time.Minute))
	if err != nil || outcome != CompleteReplayed {
		t.Fatalf("repeat result: outcome=%d err=%v", outcome, err)
	}
	if replayed.ID != record.ID || string(replayed.ResponseSnapshot) != `{"v":1}` {
		t.Fatalf("replay mismatch: %+v", replayed)
	}

	// The promoted record is the active record for the key and visible through the existing
	// read paths until its inherited expires_at passes.
	active, err := st.ActiveRecordByKey(ctx, "k1", expires.Add(-time.Minute))
	if err != nil || active == nil || active.ID != record.ID {
		t.Fatalf("active record by key: record=%v err=%v", active, err)
	}
}

func TestCompleteReservationGuards(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	created := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	expires := created.Add(time.Hour)
	reservation := makeReservation(t, "k1", "fp1", created, expires)
	if _, _, err := st.ReserveReservation(ctx, reservation, created); err != nil {
		t.Fatalf("reserve: %v", err)
	}

	if _, bad, outcome, err := st.CompleteReservation(ctx, reservation.ID, "fp-other",
		[]byte(`null`), created.Add(time.Minute)); err != nil || outcome != CompleteConflict {
		t.Fatalf("fingerprint mismatch: reservation=%v outcome=%d err=%v", bad, outcome, err)
	}

	if _, bad, outcome, err := st.CompleteReservation(ctx, reservation.ID, "fp1",
		[]byte(`null`), expires.Add(time.Minute)); err != nil || outcome != CompleteExpired {
		t.Fatalf("expired placeholder: reservation=%v outcome=%d err=%v", bad, outcome, err)
	}

	if _, _, _, err := st.CompleteReservation(ctx, "res_00000000000000000000000000000000",
		"fp1", []byte(`null`), created.Add(time.Minute)); err != ErrReservationNotFound {
		t.Fatalf("missing placeholder err = %v, want ErrReservationNotFound", err)
	}
}

// TestCrossInstancesSingleReservationAndResult mirrors the baseline record election test: with
// independent stores sharing one DB_PATH, exactly one creator wins the placeholder and exactly
// one first result is promoted; every loser replays the winner.
func TestCrossInstancesSingleReservationAndResult(t *testing.T) {
	dir := t.TempDir()
	const instances = 6
	stores := make([]*Store, instances)
	for i := range stores {
		stores[i] = openSharedStore(t, dir)
	}
	ctx := context.Background()
	created := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	expires := created.Add(time.Hour)

	type reserveResult struct {
		id      string
		outcome ReservationOutcome
	}
	reserveCh := make(chan reserveResult, instances*2)
	start := make(chan struct{})
	var reserveWG sync.WaitGroup
	for _, st := range stores {
		for j := 0; j < 2; j++ {
			reserveWG.Add(1)
			go func(st *Store) {
				defer reserveWG.Done()
				<-start
				candidate := makeReservation(t, "shared-reservation", "fp-same", created, expires)
				saved, outcome, err := st.ReserveReservation(ctx, candidate, created)
				if err != nil {
					t.Errorf("reserve: %v", err)
					return
				}
				reserveCh <- reserveResult{id: saved.ID, outcome: outcome}
			}(st)
		}
	}
	close(start)
	reserveWG.Wait()
	close(reserveCh)

	createdCount := 0
	winnerID := ""
	for result := range reserveCh {
		if result.outcome == ReservationConflict {
			t.Fatalf("same-fingerprint reservation reported conflict: %+v", result)
		}
		if result.outcome == ReservationCreated {
			createdCount++
			winnerID = result.id
		}
	}
	if createdCount != 1 {
		t.Fatalf("reservation creators = %d, want 1", createdCount)
	}

	type completeResult struct {
		recordID string
		snap     string
		outcome  CompleteOutcome
	}
	completeCh := make(chan completeResult, instances)
	completeStart := make(chan struct{})
	var completeWG sync.WaitGroup
	for i, st := range stores {
		completeWG.Add(1)
		go func(instance int, st *Store) {
			defer completeWG.Done()
			<-completeStart
			record, _, outcome, err := st.CompleteReservation(ctx, winnerID, "fp-same",
				[]byte(fmt.Sprintf(`{"v":%d}`, instance)), created.Add(time.Minute))
			if err != nil {
				t.Errorf("complete: %v", err)
				return
			}
			completeCh <- completeResult{
				recordID: record.ID,
				snap:     string(record.ResponseSnapshot),
				outcome:  outcome,
			}
		}(i, st)
	}
	close(completeStart)
	completeWG.Wait()
	close(completeCh)

	firstResultCount := 0
	var firstSnap string
	completeResults := make([]completeResult, 0, instances)
	for result := range completeCh {
		if result.outcome == CompleteReplayed && result.recordID == "" {
			t.Fatalf("replay returned empty record")
		}
		if result.outcome == CompleteCreated {
			firstResultCount++
			firstSnap = result.snap
		}
		completeResults = append(completeResults, result)
	}
	if firstResultCount != 1 {
		t.Fatalf("first results = %d, want 1", firstResultCount)
	}
	winningRecordID := completeResults[0].recordID
	for _, result := range completeResults {
		if result.recordID != winningRecordID || result.snap != firstSnap {
			t.Fatalf("replay mismatch: %+v", result)
		}
	}
	for _, st := range stores {
		records, err := st.ListHistoryRecords(ctx, HistoryFilter{IdempotencyKey: "shared-reservation"},
			expires.Add(time.Minute))
		if err != nil {
			t.Fatalf("history: %v", err)
		}
		if len(records) != 1 {
			t.Fatalf("promoted records visible = %d, want 1", len(records))
		}
		if string(records[0].ResponseSnapshot) != firstSnap {
			t.Fatalf("visible snapshot = %s, want %s", records[0].ResponseSnapshot, firstSnap)
		}
	}
}
