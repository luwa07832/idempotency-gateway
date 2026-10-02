// Reservations are execution placeholders: the first request for an idempotency key creates a
// pending placeholder so that concurrent same-key same-fingerprint requests reuse it instead of
// executing twice while no result exists yet. The first result submitted against a placeholder
// materializes as a regular record whose timestamps come from the placeholder; later submissions
// replay that record. A placeholder that reaches expires_at without a result is expired and no
// longer owns its key, so a fresh placeholder can be created for the same key.
package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// Reservation is one execution placeholder. RecordID is empty until the first result is
// submitted; afterwards it points at the record the result materialized into. Rows are never
// rewritten except for that one-time RecordID transition.
type Reservation struct {
	ID                 string
	IdempotencyKey     string
	RequestFingerprint string
	RecordID           string
	CreatedAt          time.Time
	ExpiresAt          time.Time
}

// ResultOutcome tells the caller which branch SubmitResult took.
type ResultOutcome int

const (
	// ResultCreated means the first result was stored as a fresh record linked to the placeholder.
	ResultCreated ResultOutcome = iota
	// ResultReplayed means the placeholder already had a result; the first record is returned.
	ResultReplayed
	// ResultConflict means the submitted fingerprint differs from the placeholder's.
	ResultConflict
	// ResultExpired means the placeholder reached expires_at without any result.
	ResultExpired
	// ResultNotFound means no placeholder carries the submitted reservation id.
	ResultNotFound
)

// NewReservationID generates the public identifier attached to every reservation. The shape is
// fixed ("res_" followed by 32 lowercase hex characters) so callers can rely on it.
func NewReservationID() (string, error) {
	return newPrefixedID("res_")
}

// IsReservationID reports whether id has the fixed shape NewReservationID emits: "res_" followed
// by exactly 32 lowercase hexadecimal characters.
func IsReservationID(id string) bool {
	return hasIDShape(id, "res_")
}

// PutReservation stores the candidate unless an unexpired reservation already owns the same
// idempotency key at now. With an existing unexpired reservation the outcome is PutReplayed when
// the fingerprints match and PutConflict otherwise; in both cases the returned reservation is the
// stored one and the candidate is not written. Expired placeholders never block a fresh
// reservation for the same key.
//
// The whole check-then-insert flow runs in one BEGIN IMMEDIATE transaction under writeMu, exactly
// like PutRecord, so concurrent creators across processes sharing one DB_PATH elect exactly one
// visible winner per key.
func (s *Store) PutReservation(ctx context.Context, candidate Reservation, now time.Time) (*Reservation, PutOutcome, error) {
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
			return existing, PutReplayed, nil
		}
		return existing, PutConflict, nil
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO idempotency_reservations
	(id, idempotency_key, request_fingerprint, record_id, created_at_ns, expires_at_ns)
VALUES (?, ?, ?, NULL, ?, ?)`,
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
	return &candidate, PutCreated, nil
}

// SubmitResult attaches the first result to a reservation. The submitted fingerprint must match
// the placeholder's; the first accepted result is inserted as a regular record whose timestamps
// are copied from the placeholder and whose snapshot bytes are stored verbatim, and the
// placeholder is linked to that record in the same transaction so a failure cannot leave a
// partial write behind. Later submissions with the matching fingerprint replay the first record
// without inserting anything. A placeholder that reached expires_at before any result arrived is
// expired and rejects results; an unknown id reports ResultNotFound.
//
// The check-then-write flow runs in one BEGIN IMMEDIATE transaction under writeMu, so concurrent
// first-result submissions against one reservation produce exactly one created record and every
// other caller replays it.
func (s *Store) SubmitResult(ctx context.Context, reservationID, fingerprint string, snapshot []byte, recordID string, now time.Time) (*Reservation, *Record, ResultOutcome, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, 0, err
	}
	defer tx.Rollback()

	reservation, err := queryReservationByID(ctx, tx, reservationID)
	if err != nil {
		return nil, nil, 0, err
	}
	if reservation == nil {
		return nil, nil, ResultNotFound, nil
	}
	if reservation.RequestFingerprint != fingerprint {
		return reservation, nil, ResultConflict, nil
	}
	if reservation.RecordID != "" {
		record, err := queryRecordByID(ctx, tx, reservation.RecordID)
		if err != nil {
			return nil, nil, 0, err
		}
		if record == nil {
			return nil, nil, 0, errors.New("reservation references a missing record")
		}
		return reservation, record, ResultReplayed, nil
	}
	if !reservation.ExpiresAt.After(now) {
		return reservation, nil, ResultExpired, nil
	}

	record := Record{
		ID:                 recordID,
		IdempotencyKey:     reservation.IdempotencyKey,
		RequestFingerprint: reservation.RequestFingerprint,
		ResponseSnapshot:   snapshot,
		CreatedAt:          reservation.CreatedAt,
		ExpiresAt:          reservation.ExpiresAt,
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO idempotency_records
	(id, idempotency_key, request_fingerprint, response_snapshot, created_at_ns, expires_at_ns)
VALUES (?, ?, ?, ?, ?, ?)`,
		record.ID,
		record.IdempotencyKey,
		record.RequestFingerprint,
		string(record.ResponseSnapshot),
		record.CreatedAt.UnixNano(),
		record.ExpiresAt.UnixNano(),
	); err != nil {
		return nil, nil, 0, err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE idempotency_reservations
SET record_id = ?
WHERE id = ?`, recordID, reservationID); err != nil {
		return nil, nil, 0, err
	}
	if err := tx.Commit(); err != nil {
		return nil, nil, 0, err
	}
	reservation.RecordID = recordID
	return reservation, &record, ResultCreated, nil
}

const activeReservationQuery = `
SELECT id, idempotency_key, request_fingerprint, record_id, created_at_ns, expires_at_ns
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

func queryReservationByID(ctx context.Context, querier rowQuerier, id string) (*Reservation, error) {
	reservation, err := scanReservation(querier.QueryRowContext(ctx, `
SELECT id, idempotency_key, request_fingerprint, record_id, created_at_ns, expires_at_ns
FROM idempotency_reservations
WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return reservation, nil
}

func scanReservation(scanner rowScanner) (*Reservation, error) {
	var reservation Reservation
	var recordID sql.NullString
	var createdNS, expiresNS int64
	if err := scanner.Scan(
		&reservation.ID,
		&reservation.IdempotencyKey,
		&reservation.RequestFingerprint,
		&recordID,
		&createdNS,
		&expiresNS,
	); err != nil {
		return nil, err
	}
	reservation.RecordID = recordID.String
	reservation.CreatedAt = time.Unix(0, createdNS).UTC()
	reservation.ExpiresAt = time.Unix(0, expiresNS).UTC()
	return &reservation, nil
}
