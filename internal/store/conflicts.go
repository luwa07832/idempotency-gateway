// Conflict events are the immutable audit trail of fingerprint conflicts. A conflict is recorded
// exactly when a submission finds an active record for the same idempotency key carrying a
// different request fingerprint: the event is inserted in the same BEGIN IMMEDIATE transaction
// that identified the conflict, so a request never returns 409 unless its event was durably
// saved. Events are append-only: they are never updated, merged or deleted.
package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"time"
)

// ConflictEvent is one immutable fingerprint-conflict observation. Both fingerprints are stored
// verbatim: ObservedRequestFingerprint is this request's value and ExistingRequestFingerprint is
// the value of the active record it collided with.
type ConflictEvent struct {
	ID                         string
	IdempotencyKey             string
	ObservedRequestFingerprint string
	ExistingRecordID           string
	ExistingRequestFingerprint string
	CreatedAt                  time.Time
}

// NewConflictID generates the public identifier attached to every conflict event. The shape is
// fixed ("con_" followed by 32 lowercase hex characters).
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

// ConflictFilter narrows ListConflictEvents. Every field is matched character-for-character as
// supplied by the caller; a zero-valued field is not applied.
type ConflictFilter struct {
	IdempotencyKey             string
	ObservedRequestFingerprint string
	ExistingRecordID           string
	CursorCreatedAt            time.Time
	CursorID                   string
	Limit                      int
}

// ListConflictEvents returns conflict events matching filter, newest first with id as the
// deterministic tie-breaker. The query is read-only: events are append-only and never deleted, so
// a key-set cursor stays stable while paging even when new conflicts arrive concurrently.
//
// When a cursor is present its id must have the fixed event-id shape and it must point at a real
// event that still satisfies every filter predicate, otherwise ErrInvalidID or ErrCursorTarget is
// returned so the API can distinguish a bad cursor from a storage failure.
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
FROM idempotency_conflicts
WHERE 1 = 1`
	args := make([]any, 0, 8)

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
// query predicates. A cursor aimed at an event filtered out (or one that does not exist) is
// rejected rather than silently reshaping the page.
func (s *Store) validateConflictCursor(ctx context.Context, filter ConflictFilter) error {
	query := `
SELECT created_at_ns
FROM idempotency_conflicts
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
