// Conflict events are the immutable audit trail of fingerprint conflicts. An event is written in
// the same BEGIN IMMEDIATE transaction that identifies the conflict, so a request returns 409 only
// once its event is durably stored; events are never updated or deleted and are not reachable
// through any record-shaped endpoint.
package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"time"
)

// ConflictEvent is one stored fingerprint-conflict observation. Fields are fixed and rendered
// verbatim by the audit endpoint.
type ConflictEvent struct {
	ID                         string
	IdempotencyKey             string
	ObservedRequestFingerprint string
	ExistingRecordID           string
	ExistingRequestFingerprint string
	CreatedAt                  time.Time
}

// NewConflictID generates the public identifier attached to every conflict event: "con_"
// followed by 32 lowercase hex characters.
func NewConflictID() (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return "con_" + hex.EncodeToString(raw), nil
}

// IsConflictID reports whether id has the fixed shape NewConflictID emits: "con_" followed by
// exactly 32 lowercase hexadecimal characters.
func IsConflictID(id string) bool {
	const (
		prefix = "con_"
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

// conflictExecer is the transaction surface insertConflictEvent needs.
type conflictExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// SeedConflictEventForTest inserts an already-built conflict event verbatim. It exists for
// cross-package tests that need deterministic audit rows at arbitrary timestamps and is not used
// by the service's write path.
func (s *Store) SeedConflictEventForTest(ctx context.Context, event ConflictEvent) (ConflictEvent, error) {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO idempotency_conflict_events
	(id, idempotency_key, observed_request_fingerprint, existing_record_id, existing_request_fingerprint, created_at_ns)
VALUES (?, ?, ?, ?, ?, ?)`,
		event.ID, event.IdempotencyKey, event.ObservedRequestFingerprint,
		event.ExistingRecordID, event.ExistingRequestFingerprint, event.CreatedAt.UnixNano())
	if err != nil {
		return ConflictEvent{}, err
	}
	return event, nil
}

// insertConflictEvent persists one conflict observation inside the write transaction that detected
// it. The observed fingerprint is the losing request's raw value; the existing fingerprint and
// record id identify the active record that won. Any failure aborts the whole transaction so the
// caller can answer 503 instead of acknowledging a 409 without an audit row.
func insertConflictEvent(ctx context.Context, tx conflictExecer, candidate Record, existing *Record, now time.Time) error {
	id, err := NewConflictID()
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO idempotency_conflict_events
	(id, idempotency_key, observed_request_fingerprint, existing_record_id, existing_request_fingerprint, created_at_ns)
VALUES (?, ?, ?, ?, ?, ?)`,
		id,
		candidate.IdempotencyKey,
		candidate.RequestFingerprint,
		existing.ID,
		existing.RequestFingerprint,
		now.UnixNano(),
	)
	return err
}

// ConflictFilter narrows ListConflictEvents. Filters match their stored values exactly: no
// trimming, case folding or prefix matching. A zero-valued field is not applied.
type ConflictFilter struct {
	IdempotencyKey             string
	ObservedRequestFingerprint string
	ExistingRecordID           string
	CursorCreatedAt            time.Time
	CursorID                   string
	Limit                      int
}

// ListConflictEvents returns conflict events matching filter, newest first with id as the
// deterministic tie-breaker. It is read-only: the audit trail cannot be updated or deleted through
// this or any other method.
//
// When a cursor is present it must point at an event that satisfies every filter predicate,
// otherwise ErrInvalidID or ErrCursorTarget is returned so the API can distinguish a bad cursor
// from a storage failure. Events are append-only, so the cursor keeps referencing the same
// (created_at, id) while paging and later inserts never reshuffle earlier pages.
func (s *Store) ListConflictEvents(ctx context.Context, filter ConflictFilter) ([]ConflictEvent, error) {
	if filter.CursorID != "" {
		if !IsConflictID(filter.CursorID) {
			return nil, ErrInvalidID
		}
		if err := s.validateConflictCursor(ctx, filter); err != nil {
			return nil, err
		}
	}

	query := `
SELECT id, idempotency_key, observed_request_fingerprint, existing_record_id, existing_request_fingerprint, created_at_ns
FROM idempotency_conflict_events
WHERE 1 = 1`
	args := []any{}

	if filter.IdempotencyKey != "" {
		query += ` AND idempotency_key = ?`
		args = append(args, filter.IdempotencyKey)
	}
	if filter.ObservedRequestFingerprint != "" {
		query += ` AND observed_request_fingerprint = ?`
		args = append(args, filter.ObservedRequestFingerprint)
	}
	if filter.ExistingRecordID != "" {
		query += ` AND existing_record_id = ?`
		args = append(args, filter.ExistingRecordID)
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

	events := make([]ConflictEvent, 0)
	for rows.Next() {
		var event ConflictEvent
		var createdNS int64
		if err := rows.Scan(
			&event.ID,
			&event.IdempotencyKey,
			&event.ObservedRequestFingerprint,
			&event.ExistingRecordID,
			&event.ExistingRequestFingerprint,
			&createdNS,
		); err != nil {
			return nil, err
		}
		event.CreatedAt = time.Unix(0, createdNS).UTC()
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return events, nil
}

// validateConflictCursor confirms the cursor references a real event that still satisfies the
// query predicates. A cursor aimed at another event or one excluded by the filters is rejected
// rather than silently reshaping the page.
func (s *Store) validateConflictCursor(ctx context.Context, filter ConflictFilter) error {
	query := `
SELECT created_at_ns
FROM idempotency_conflict_events
WHERE id = ?`
	args := []any{filter.CursorID}

	if filter.IdempotencyKey != "" {
		query += ` AND idempotency_key = ?`
		args = append(args, filter.IdempotencyKey)
	}
	if filter.ObservedRequestFingerprint != "" {
		query += ` AND observed_request_fingerprint = ?`
		args = append(args, filter.ObservedRequestFingerprint)
	}
	if filter.ExistingRecordID != "" {
		query += ` AND existing_record_id = ?`
		args = append(args, filter.ExistingRecordID)
	}
	if !filter.CursorCreatedAt.IsZero() {
		query += ` AND created_at_ns = ?`
		args = append(args, filter.CursorCreatedAt.UnixNano())
	}

	var createdNS int64
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&createdNS)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrCursorTarget
	}
	return err
}
