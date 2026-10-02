// Package store owns the SQLite file and every write the service performs.
package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"time"
)

// Record is one idempotency record. Rows are immutable: a replay never overwrites the first
// snapshot and a record re-created after expiry inserts a fresh row instead of updating history.
type Record struct {
	ID                 string
	IdempotencyKey     string
	RequestFingerprint string
	ResponseSnapshot   []byte
	CreatedAt          time.Time
	ExpiresAt          time.Time
}

// PutOutcome tells the caller which branch PutRecord took.
type PutOutcome int

const (
	// PutCreated means the candidate was inserted as the first record for its key.
	PutCreated PutOutcome = iota
	// PutReplayed means an active record with the same fingerprint was returned.
	PutReplayed
	// PutConflict means an active record with a different fingerprint was returned.
	PutConflict
)

// ErrInvalidID is returned when a pagination cursor references a malformed record id.
var ErrInvalidID = errors.New("invalid record id")

// NewRecordID generates the public identifier attached to every stored record. The shape is
// fixed ("rec_" followed by 32 lowercase hex characters) so callers can rely on it.
func NewRecordID() (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return "rec_" + hex.EncodeToString(raw), nil
}

// PutRecord stores the candidate unless an active (non-expired) record already owns the same
// idempotency key at now. With an existing active record the outcome is PutReplayed when the
// fingerprints match and PutConflict otherwise; in both cases the returned record is the stored
// one and the candidate is not written.
// On PutConflict one immutable idempotency_conflict_events row is committed in the same
// transaction, carrying the candidate's raw fingerprint and the stored record's id and
// fingerprint; a failure to persist that event makes the whole call fail instead of returning
// PutConflict.
//
// The whole check-then-insert flow runs in one BEGIN IMMEDIATE transaction, and writeMu also
// serializes writers within this instance. Across processes sharing the same file, SQLite grants
// the RESERVED lock to only one connection at a time (peers wait on busy_timeout), so concurrent
// submissions for one key elect exactly one visible winner: matching fingerprints all replay that
// winner, different fingerprints only see it after the winner commits (each losing request leaving
// its own conflict event), and a re-submission once every row has expired inserts one fresh row
// while leaving the historical rows untouched.
func (s *Store) PutRecord(ctx context.Context, candidate Record, now time.Time) (*Record, PutOutcome, error) {
	nowNS := now.UnixNano()

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, 0, err
	}
	defer tx.Rollback()

	existing, err := queryActiveRecord(ctx, tx, candidate.IdempotencyKey, nowNS)
	if err != nil {
		return nil, 0, err
	}
	if existing != nil {
		if existing.RequestFingerprint == candidate.RequestFingerprint {
			return existing, PutReplayed, nil
		}
		// A fingerprint conflict is acknowledged only once the immutable audit event is committed
		// in the same transaction: when the event cannot be stored reliably the caller must answer
		// 503 rather than returning 409 without a durable record of the clash. BEGIN IMMEDIATE
		// serializes conflicting requests across instances too, so every request that reaches this
		// branch persists its own event.
		if err := insertConflictEvent(ctx, tx, candidate, existing, now); err != nil {
			return nil, 0, err
		}
		if err := tx.Commit(); err != nil {
			return nil, 0, err
		}
		return existing, PutConflict, nil
	}

	if _, err := tx.ExecContext(ctx, `
INSERT INTO idempotency_records
	(id, idempotency_key, request_fingerprint, response_snapshot, created_at_ns, expires_at_ns)
VALUES (?, ?, ?, ?, ?, ?)`,
		candidate.ID,
		candidate.IdempotencyKey,
		candidate.RequestFingerprint,
		string(candidate.ResponseSnapshot),
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

// ActiveRecordByKey returns the non-expired record for key at now, or (nil, nil) when the key is
// unknown or every record for it has expired. Expired rows never leak through this lookup.
func (s *Store) ActiveRecordByKey(ctx context.Context, key string, now time.Time) (*Record, error) {
	return queryActiveRecord(ctx, s.db, key, now.UnixNano())
}

// RecordByID returns any record generation with the given id, including expired rows. It is the
// read-only backing for the records-by-id lookup: the caller decides active versus expired from
// the record's own expires_at. (nil, nil) means no row carries the id. The method issues no
// writes and never changes what the active or history endpoints can see.
func (s *Store) RecordByID(ctx context.Context, id string) (*Record, error) {
	record, err := scanRecord(s.db.QueryRowContext(ctx, `
SELECT id, idempotency_key, request_fingerprint, response_snapshot, created_at_ns, expires_at_ns
FROM idempotency_records
WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return record, nil
}

// ListFilter narrows ListRecords. Zero-valued fields are not applied.
type ListFilter struct {
	IdempotencyKey     string
	RequestFingerprint string
	ExpiresBefore      *time.Time
	ExpiresAfter       *time.Time
	CursorCreatedAt    time.Time
	CursorID           string
	Limit              int
}

// ListRecords returns active records matching filter, newest first with id as the deterministic
// tie-breaker. Expired records never participate. The cursor points at the last row of the
// previous page and only strictly earlier rows are returned, so inserts during paging never
// reshuffle earlier pages.
func (s *Store) ListRecords(ctx context.Context, filter ListFilter, now time.Time) ([]Record, error) {
	query := `
SELECT id, idempotency_key, request_fingerprint, response_snapshot, created_at_ns, expires_at_ns
FROM idempotency_records
WHERE expires_at_ns > ?`
	args := []any{now.UnixNano()}

	if filter.IdempotencyKey != "" {
		query += ` AND idempotency_key = ?`
		args = append(args, filter.IdempotencyKey)
	}
	if filter.RequestFingerprint != "" {
		query += ` AND request_fingerprint = ?`
		args = append(args, filter.RequestFingerprint)
	}
	if filter.ExpiresBefore != nil {
		query += ` AND expires_at_ns < ?`
		args = append(args, filter.ExpiresBefore.UnixNano())
	}
	if filter.ExpiresAfter != nil {
		query += ` AND expires_at_ns > ?`
		args = append(args, filter.ExpiresAfter.UnixNano())
	}
	if !filter.CursorCreatedAt.IsZero() && filter.CursorID != "" {
		query += ` AND (created_at_ns < ? OR (created_at_ns = ? AND id < ?))`
		args = append(args, filter.CursorCreatedAt.UnixNano(), filter.CursorCreatedAt.UnixNano(), filter.CursorID)
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = defaultListLimit
	}
	query += ` ORDER BY created_at_ns DESC, id DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	records := make([]Record, 0)
	for rows.Next() {
		record, err := scanRecord(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, *record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

const defaultListLimit = 50

type rowQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

const activeRecordQuery = `
SELECT id, idempotency_key, request_fingerprint, response_snapshot, created_at_ns, expires_at_ns
FROM idempotency_records
WHERE idempotency_key = ? AND expires_at_ns > ?
ORDER BY created_at_ns DESC, id DESC
LIMIT 1`

func queryActiveRecord(ctx context.Context, querier rowQuerier, key string, nowNS int64) (*Record, error) {
	record, err := scanRecord(querier.QueryRowContext(ctx, activeRecordQuery, key, nowNS))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return record, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanRecord(scanner rowScanner) (*Record, error) {
	var record Record
	var snapshot string
	var createdNS, expiresNS int64
	if err := scanner.Scan(
		&record.ID,
		&record.IdempotencyKey,
		&record.RequestFingerprint,
		&snapshot,
		&createdNS,
		&expiresNS,
	); err != nil {
		return nil, err
	}
	record.ResponseSnapshot = []byte(snapshot)
	record.CreatedAt = time.Unix(0, createdNS).UTC()
	record.ExpiresAt = time.Unix(0, expiresNS).UTC()
	return &record, nil
}
