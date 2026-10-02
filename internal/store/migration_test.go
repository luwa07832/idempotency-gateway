package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// TestOpenAddsReservationColumnToLegacyDatabase opens a database created with the baseline
// schema (no reservation_id), inserts a baseline row, then reopens it through Open: the
// migration must add the column and unique partial index without touching existing rows.
func TestOpenAddsReservationColumnToLegacyDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	if _, err := db.Exec(`
CREATE TABLE idempotency_records (
	id                  TEXT PRIMARY KEY,
	idempotency_key     TEXT NOT NULL,
	request_fingerprint TEXT NOT NULL,
	response_snapshot   TEXT NOT NULL,
	created_at_ns       INTEGER NOT NULL,
	expires_at_ns       INTEGER NOT NULL
)`); err != nil {
		t.Fatalf("create legacy table: %v", err)
	}
	if _, err := db.Exec(`
INSERT INTO idempotency_records
	(id, idempotency_key, request_fingerprint, response_snapshot, created_at_ns, expires_at_ns)
VALUES ('rec_legacy', 'k1', 'fp1', '{"v":1}', 1, 2)`); err != nil {
		t.Fatalf("insert legacy row: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close raw db: %v", err)
	}

	st, err := Open(path)
	if err != nil {
		t.Fatalf("migrate on open: %v", err)
	}
	defer st.Close()

	created := time.Unix(0, 1).UTC()
	record, err := st.RecordByID(context.Background(), "rec_legacy")
	if err != nil || record == nil {
		t.Fatalf("legacy row missing: record=%v err=%v", record, err)
	}
	if record.IdempotencyKey != "k1" {
		t.Fatalf("legacy row altered: %+v", record)
	}

	// Promoting a reservation on the migrated database must work and use the new column.
	id, err := NewReservationID()
	if err != nil {
		t.Fatalf("new reservation id: %v", err)
	}
	reservation := Reservation{
		ID:                 id,
		IdempotencyKey:     "k1",
		RequestFingerprint: "fp1",
		CreatedAt:          created,
		ExpiresAt:          created.Add(time.Hour),
	}
	if _, _, err := st.ReserveReservation(context.Background(), reservation, created); err != nil {
		t.Fatalf("reserve on migrated db: %v", err)
	}
	completed, _, outcome, err := st.CompleteReservation(context.Background(),
		reservation.ID, "fp1", []byte(`null`), created.Add(time.Minute))
	if err != nil || outcome != CompleteCreated {
		t.Fatalf("complete on migrated db: outcome=%d err=%v", outcome, err)
	}
	if completed.IdempotencyKey != "k1" {
		t.Fatalf("completed record mismatch: %+v", completed)
	}
}
