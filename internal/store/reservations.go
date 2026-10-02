// Reservations are pending execution placeholders for an idempotency key. A reservation row is
// promoted into the existing idempotency_records table exactly once, when the first result is
// committed; that record keeps the reservation's created_at and expires_at byte-for-byte in time
// semantics. An uncompleted reservation that passes its expires_at is expired and never blocks a
// new placeholder for the same key.
package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"time"
)

// Reservation is one pending execution placeholder. Rows are immutable: the first result commit
// promotes the row into idempotency_records and repeated result commits replay that record.
type Reservation struct {
	ID                 string
	IdempotencyKey     string
	RequestFingerprint string
	CreatedAt          time.Time
	ExpiresAt          time.Time
}

// ReservationOutcome tells the caller which branch ReserveReservation took.
type ReservationOutcome int

const (
	// ReservationCreated means no pending placeholder owned the key and a row was inserted.
	ReservationCreated ReservationOutcome = iota
	// ReservationReplayed means a pending placeholder with the same fingerprint was returned.
	ReservationReplayed
	// ReservationConflict means a pending placeholder with a different fingerprint was returned.
	ReservationConflict
)

// CompleteOutcome tells the caller which branch CompleteReservation took.
type CompleteOutcome int

const (
	// CompleteCreated means the first result for the placeholder was written as a record.
	CompleteCreated CompleteOutcome = iota
	// CompleteReplayed means the first result was returned; the submitted snapshot was ignored.
	CompleteReplayed
	// CompleteConflict means the placeholder is still pending but its fingerprint differs.
	CompleteConflict
	// CompleteExpired means the placeholder reached its expires_at without a committed result.
	CompleteExpired
)

// ErrReservationNotFound is returned when no reservation row carries the given id.
var ErrReservationNotFound = errors.New("reservation not found")

// NewReservationID generates the public identifier attached to every placeholder. The shape is
// fixed ("res_" followed by 32 lowercase hex characters) so callers can rely on it.
func NewReservationID() (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return "res_" + hex.EncodeToString(raw), nil
}

// IsReservationID reports whether id has the fixed shape NewReservationID emits.
func IsReservationID(id string) bool {
	const (
		prefix = "res_"
		hexLen = 32
	)
	if len(id) != len(prefix)+hexLen || id[:len(prefix)] != prefix {
		return false
	}
	for _, ch := range id[len(prefix):] {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	return true
}

// ReserveReservation inserts a pending placeholder unless a non-expired pending placeholder
// already owns the idempotency key at now. With an existing pending placeholder the outcome is
// ReservationReplayed when the fingerprints match and ReservationConflict otherwise; in both
// cases the returned reservation is the stored row and the candidate is not written.
//
// As with PutRecord the whole check-then-insert flow runs in one BEGIN IMMEDIATE transaction under
// writeMu, so instances sharing one DB_PATH elect exactly one creator: waiters only observe the
// winning row after it commits.
func (s *Store) ReserveReservation(ctx context.Context, candidate Reservation, now time.Time) (*Reservation, ReservationOutcome, error) {
	nowNS := now.UnixNano()

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, 0, err
	}
	defer tx.Rollback()

	existing, err := queryActiveReservation(ctx, tx, candidate.IdempotencyKey, nowNS)
	if err != nil {
		return nil, 0, err
	}
	if existing != nil {
		if existing.RequestFingerprint == candidate.RequestFingerprint {
			return existing, ReservationReplayed, nil
		}
		return existing, ReservationConflict, nil
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO idempotency_reservations
	(id, idempotency_key, request_fingerprint, created_at_ns, expires_at_ns)
VALUES (?, ?, ?, ?, ?)`,
		candidate.ID,
		candidate.IdempotencyKey,
		candidate.RequestFingerprint,
		candidate.CreatedAt.UnixNano(),
		candidate.ExpiresAt.UnixNano(),
	); err != nil {
		return nil, 0, err
	}
	if err := tx.Commit(); err != nil {
		return nil, 0, err
	}
	return &candidate, ReservationCreated, nil
}

// CompleteReservation promotes a pending placeholder into its first result record. The record
// reuses the placeholder's idempotency key, fingerprint, created_at and expires_at; only the
// record id and the response snapshot are new. A repeated result commit returns the stored record
// untouched (CompleteReplayed), so the first snapshot is replayed byte-for-byte.
//
// Outcomes:
//   - a fingerprint mismatch on a still-pending placeholder returns CompleteConflict;
//   - a placeholder already past its expires_at (with or without a result) returns
//     CompleteExpired, since the caller may create a new placeholder for the key;
//   - a placeholder promoted by an earlier commit returns the stored record as CompleteReplayed.
//
// The select-then-insert/select flow runs in one BEGIN IMMEDIATE transaction under writeMu. The
// unique partial index on idempotency_records(reservation_id) additionally guarantees that even
// two insert racing on the same placeholder can only commit one first result; the loser observes
// the winner row instead of failing with a constraint error.
func (s *Store) CompleteReservation(ctx context.Context, reservationID, fingerprint string, snapshot []byte, now time.Time) (*Record, *Reservation, CompleteOutcome, error) {
	nowNS := now.UnixNano()

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, 0, err
	}
	defer tx.Rollback()

	reservation, err := scanReservation(tx.QueryRowContext(ctx, `
SELECT id, idempotency_key, request_fingerprint, created_at_ns, expires_at_ns
FROM idempotency_reservations
WHERE id = ?`, reservationID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, 0, ErrReservationNotFound
	}
	if err != nil {
		return nil, nil, 0, err
	}

	existing, err := queryRecordByReservationID(ctx, tx, reservationID)
	if err != nil {
		return nil, nil, 0, err
	}
	if existing != nil {
		return existing, nil, CompleteReplayed, nil
	}

	if reservation.RequestFingerprint != fingerprint {
		return nil, reservation, CompleteConflict, nil
	}
	if reservation.ExpiresAt.UnixNano() <= nowNS {
		return nil, reservation, CompleteExpired, nil
	}

	id, err := NewRecordID()
	if err != nil {
		return nil, nil, 0, err
	}
	record := Record{
		ID:                 id,
		IdempotencyKey:     reservation.IdempotencyKey,
		RequestFingerprint: reservation.RequestFingerprint,
		ResponseSnapshot:   snapshot,
		CreatedAt:          reservation.CreatedAt,
		ExpiresAt:          reservation.ExpiresAt,
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO idempotency_records
	(id, idempotency_key, request_fingerprint, response_snapshot, created_at_ns, expires_at_ns, reservation_id)
VALUES (?, ?, ?, ?, ?, ?, ?)`,
		record.ID,
		record.IdempotencyKey,
		record.RequestFingerprint,
		string(record.ResponseSnapshot),
		record.CreatedAt.UnixNano(),
		record.ExpiresAt.UnixNano(),
		reservation.ID,
	); err != nil {
		return nil, nil, 0, err
	}
	if err := tx.Commit(); err != nil {
		return nil, nil, 0, err
	}
	return &record, nil, CompleteCreated, nil
}

const activeReservationQuery = `
SELECT id, idempotency_key, request_fingerprint, created_at_ns, expires_at_ns
FROM idempotency_reservations
WHERE idempotency_key = ? AND expires_at_ns > ?
ORDER BY created_at_ns DESC, id DESC
LIMIT 1`

func queryActiveReservation(ctx context.Context, querier rowQuerier, key string, nowNS int64) (*Reservation, error) {
	reservation, err := scanReservation(querier.QueryRowContext(ctx, activeReservationQuery, key, nowNS))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return reservation, nil
}

const recordByReservationIDQuery = `
SELECT id, idempotency_key, request_fingerprint, response_snapshot, created_at_ns, expires_at_ns
FROM idempotency_records
WHERE reservation_id = ?`

func queryRecordByReservationID(ctx context.Context, querier rowQuerier, reservationID string) (*Record, error) {
	record, err := scanRecord(querier.QueryRowContext(ctx, recordByReservationIDQuery, reservationID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return record, nil
}

func scanReservation(scanner rowScanner) (*Reservation, error) {
	var reservation Reservation
	var createdNS, expiresNS int64
	if err := scanner.Scan(
		&reservation.ID,
		&reservation.IdempotencyKey,
		&reservation.RequestFingerprint,
		&createdNS,
		&expiresNS,
	); err != nil {
		return nil, err
	}
	reservation.CreatedAt = time.Unix(0, createdNS).UTC()
	reservation.ExpiresAt = time.Unix(0, expiresNS).UTC()
	return &reservation, nil
}
