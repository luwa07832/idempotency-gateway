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
		t.Fatalf("new reservation id: %v", err)
	}
	if !IsReservationID(id) {
		t.Fatalf("generated id %q fails its own shape check", id)
	}
	for _, bad := range []string{"", "res_", "rec_" + id[4:], "RES_" + id[4:], id + "0", id[:len(id)-1], "res_" + "0123456789abcdef0123456789abcdeg"} {
		if IsReservationID(bad) {
			t.Fatalf("IsReservationID(%q) = true, want false", bad)
		}
	}
}

func TestPutReservationCreateReplayConflict(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	created := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	expires := created.Add(time.Hour)

	first := makeReservation(t, "k1", "fp1", created, expires)
	saved, outcome, err := st.PutReservation(ctx, first, created)
	if err != nil || outcome != PutCreated {
		t.Fatalf("first put: outcome=%d err=%v", outcome, err)
	}
	if saved.ID != first.ID || saved.RecordID != "" {
		t.Fatalf("saved reservation = %+v, want id %s with no record", saved, first.ID)
	}

	duplicate := makeReservation(t, "k1", "fp1", created.Add(time.Minute), expires)
	replayed, outcome, err := st.PutReservation(ctx, duplicate, created.Add(time.Minute))
	if err != nil || outcome != PutReplayed {
		t.Fatalf("duplicate put: outcome=%d err=%v", outcome, err)
	}
	if replayed.ID != first.ID || !replayed.CreatedAt.Equal(created) {
		t.Fatalf("replay = %+v, want the first placeholder %s", replayed, first.ID)
	}

	other := makeReservation(t, "k1", "fp2", created.Add(time.Minute), expires)
	existing, outcome, err := st.PutReservation(ctx, other, created.Add(time.Minute))
	if err != nil || outcome != PutConflict {
		t.Fatalf("conflict put: outcome=%d err=%v", outcome, err)
	}
	if existing.ID != first.ID || existing.RequestFingerprint != "fp1" {
		t.Fatalf("conflict returned %+v, want the stored placeholder", existing)
	}
}

func TestPutReservationIgnoresExpiredPlaceholder(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	created := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)

	old := makeReservation(t, "k1", "fp1", created, created.Add(time.Hour))
	if _, outcome, err := st.PutReservation(ctx, old, created); err != nil || outcome != PutCreated {
		t.Fatalf("first put: outcome=%d err=%v", outcome, err)
	}

	later := created.Add(2 * time.Hour)
	fresh := makeReservation(t, "k1", "fp1", later, later.Add(time.Hour))
	saved, outcome, err := st.PutReservation(ctx, fresh, later)
	if err != nil || outcome != PutCreated {
		t.Fatalf("put after expiry: outcome=%d err=%v", outcome, err)
	}
	if saved.ID != fresh.ID {
		t.Fatalf("put after expiry reused %s, want new placeholder %s", saved.ID, fresh.ID)
	}
}

func TestSubmitResultLifecycle(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	created := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	expires := created.Add(time.Hour)

	reservation := makeReservation(t, "k1", "fp1", created, expires)
	if _, _, err := st.PutReservation(ctx, reservation, created); err != nil {
		t.Fatalf("put: %v", err)
	}

	recordID, err := NewRecordID()
	if err != nil {
		t.Fatalf("new record id: %v", err)
	}
	at := created.Add(time.Minute)
	storedReservation, record, outcome, err := st.SubmitResult(ctx, reservation.ID, "fp1", []byte(`{"v":1}`), recordID, at)
	if err != nil || outcome != ResultCreated {
		t.Fatalf("first result: outcome=%d err=%v", outcome, err)
	}
	if storedReservation.RecordID != recordID {
		t.Fatalf("reservation record id = %q, want %q", storedReservation.RecordID, recordID)
	}
	if record.ID != recordID || record.IdempotencyKey != "k1" || record.RequestFingerprint != "fp1" {
		t.Fatalf("record identity = %+v", record)
	}
	if !record.CreatedAt.Equal(created) || !record.ExpiresAt.Equal(expires) {
		t.Fatalf("record times = %v..%v, want the placeholder's %v..%v", record.CreatedAt, record.ExpiresAt, created, expires)
	}
	if string(record.ResponseSnapshot) != `{"v":1}` {
		t.Fatalf("snapshot = %s", record.ResponseSnapshot)
	}

	// The materialized record is a regular record: the active lookup sees it.
	active, err := st.ActiveRecordByKey(ctx, "k1", at)
	if err != nil || active == nil || active.ID != recordID {
		t.Fatalf("active lookup = %+v err=%v", active, err)
	}

	// A repeated result replays the first record without inserting anything.
	replayedReservation, replayedRecord, outcome, err := st.SubmitResult(ctx, reservation.ID, "fp1", []byte(`{"v":999}`), "rec_ffffffffffffffffffffffffffffffff", at.Add(time.Minute))
	if err != nil || outcome != ResultReplayed {
		t.Fatalf("repeated result: outcome=%d err=%v", outcome, err)
	}
	if replayedReservation.RecordID != recordID || replayedRecord.ID != recordID {
		t.Fatalf("replay = reservation %+v record %+v, want record %s", replayedReservation, replayedRecord, recordID)
	}
	if string(replayedRecord.ResponseSnapshot) != `{"v":1}` {
		t.Fatalf("replay snapshot = %s, want the first snapshot", replayedRecord.ResponseSnapshot)
	}
}

func TestSubmitResultNotFoundAndConflict(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	created := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)

	reservation := makeReservation(t, "k1", "fp1", created, created.Add(time.Hour))
	if _, _, err := st.PutReservation(ctx, reservation, created); err != nil {
		t.Fatalf("put: %v", err)
	}

	missingID, err := NewReservationID()
	if err != nil {
		t.Fatalf("new reservation id: %v", err)
	}
	if _, _, outcome, err := st.SubmitResult(ctx, missingID, "fp1", []byte(`{}`), "rec_00000000000000000000000000000000", created.Add(time.Minute)); err != nil || outcome != ResultNotFound {
		t.Fatalf("unknown id: outcome=%d err=%v", outcome, err)
	}

	conflicted, record, outcome, err := st.SubmitResult(ctx, reservation.ID, "fp-other", []byte(`{}`), "rec_00000000000000000000000000000000", created.Add(time.Minute))
	if err != nil || outcome != ResultConflict {
		t.Fatalf("fingerprint mismatch: outcome=%d err=%v", outcome, err)
	}
	if conflicted.ID != reservation.ID || conflicted.RequestFingerprint != "fp1" || record != nil {
		t.Fatalf("conflict = reservation %+v record %+v", conflicted, record)
	}

	// Neither failure wrote anything: the placeholder is still result-less and no record exists.
	if active, err := st.ActiveRecordByKey(ctx, "k1", created.Add(time.Minute)); err != nil || active != nil {
		t.Fatalf("active lookup after failures = %+v err=%v", active, err)
	}
}

func TestSubmitResultExpiredPlaceholder(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	created := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)

	reservation := makeReservation(t, "k1", "fp1", created, created.Add(time.Hour))
	if _, _, err := st.PutReservation(ctx, reservation, created); err != nil {
		t.Fatalf("put: %v", err)
	}

	afterExpiry := created.Add(2 * time.Hour)
	if _, _, outcome, err := st.SubmitResult(ctx, reservation.ID, "fp1", []byte(`{}`), "rec_00000000000000000000000000000000", afterExpiry); err != nil || outcome != ResultExpired {
		t.Fatalf("expired result: outcome=%d err=%v", outcome, err)
	}
	if active, err := st.ActiveRecordByKey(ctx, "k1", afterExpiry); err != nil || active != nil {
		t.Fatalf("active lookup after expired result = %+v err=%v", active, err)
	}
}

// TestCrossInstancesConcurrentReservations races independent stores on one database file: only
// one placeholder is created per key and only one first result is stored per placeholder.
func TestCrossInstancesConcurrentReservations(t *testing.T) {
	dir := t.TempDir()
	const instances = 6
	stores := make([]*Store, instances)
	for i := range stores {
		stores[i] = openSharedStore(t, dir)
	}
	ctx := context.Background()
	created := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	expires := created.Add(time.Hour)

	candidates := make([]Reservation, instances)
	for i := range candidates {
		candidates[i] = makeReservation(t, "race", "fp", created, expires)
	}
	type putResult struct {
		id      string
		outcome PutOutcome
	}
	putResults := make([]putResult, instances)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, st := range stores {
		wg.Add(1)
		go func(i int, st *Store) {
			defer wg.Done()
			<-start
			saved, outcome, err := st.PutReservation(ctx, candidates[i], created)
			if err != nil {
				t.Errorf("instance %d put: %v", i, err)
				return
			}
			putResults[i] = putResult{id: saved.ID, outcome: outcome}
		}(i, st)
	}
	close(start)
	wg.Wait()

	winnerID := ""
	createdCount := 0
	for i, result := range putResults {
		if result.outcome == PutCreated {
			createdCount++
			winnerID = result.id
		} else if result.outcome != PutReplayed {
			t.Fatalf("instance %d outcome = %d", i, result.outcome)
		}
	}
	if createdCount != 1 {
		t.Fatalf("created count = %d, want exactly 1", createdCount)
	}
	for i, result := range putResults {
		if result.id != winnerID {
			t.Fatalf("instance %d saw reservation %s, want winner %s", i, result.id, winnerID)
		}
	}

	recordIDs := make([]string, instances)
	for i := range recordIDs {
		id, err := NewRecordID()
		if err != nil {
			t.Fatalf("new record id: %v", err)
		}
		recordIDs[i] = id
	}
	type submitResult struct {
		recordID string
		outcome  ResultOutcome
	}
	submitResults := make([]submitResult, instances)
	start = make(chan struct{})
	for i, st := range stores {
		wg.Add(1)
		go func(i int, st *Store) {
			defer wg.Done()
			<-start
			snapshot := fmt.Sprintf(`{"attempt":%d}`, i)
			_, record, outcome, err := st.SubmitResult(ctx, winnerID, "fp", []byte(snapshot), recordIDs[i], created.Add(time.Minute))
			if err != nil {
				t.Errorf("instance %d submit: %v", i, err)
				return
			}
			submitResults[i] = submitResult{recordID: record.ID, outcome: outcome}
		}(i, st)
	}
	close(start)
	wg.Wait()

	winnerRecordID := ""
	resultCreatedCount := 0
	for i, result := range submitResults {
		switch result.outcome {
		case ResultCreated:
			resultCreatedCount++
			winnerRecordID = result.recordID
		case ResultReplayed:
		default:
			t.Fatalf("instance %d submit outcome = %d", i, result.outcome)
		}
	}
	if resultCreatedCount != 1 {
		t.Fatalf("result created count = %d, want exactly 1", resultCreatedCount)
	}
	for i, result := range submitResults {
		if result.recordID != winnerRecordID {
			t.Fatalf("instance %d saw record %s, want winner %s", i, result.recordID, winnerRecordID)
		}
	}

	records, err := stores[0].ListRecords(ctx, ListFilter{IdempotencyKey: "race", Limit: 10}, created.Add(time.Minute))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(records) != 1 || records[0].ID != winnerRecordID {
		t.Fatalf("visible records = %+v, want exactly the winning record %s", records, winnerRecordID)
	}
}
