// History queries are read-only views over the same immutable rows the write path keeps. A row
// becomes part of history exactly when expires_at_ns passes now; it is never rewritten, merged or
// physically deleted, so pagination over historical rows is stable across instances.
package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// ErrCursorTarget is returned when a decoded pagination cursor does not point at a record
// matching the query it accompanies.
var ErrCursorTarget = errors.New("pagination cursor does not reference a matching record")

// HistoryFilter narrows ListHistoryRecords. IdempotencyKey is mandatory: the audit endpoint is
// scoped to one idempotency key and must never mix generations of different keys. A zero-valued
// RequestFingerprint, ExpiresBefore or ExpiresAfter is not applied.
type HistoryFilter struct {
	IdempotencyKey     string
	RequestFingerprint string
	ExpiresBefore      *time.Time
	ExpiresAfter       *time.Time
	CursorCreatedAt    time.Time
	CursorID           string
	Limit              int
}

// ListHistoryRecords returns expired records matching filter at now (expires_at_ns <= now),
// newest created first with id as the deterministic tie-breaker. Active records never appear, and
// the rows are returned read-only: the method issues no writes.
//
// When a cursor is present it must point at a row of the same key that satisfies every filter
// predicate, otherwise ErrInvalidID or ErrCursorTarget is returned so the API can distinguish a
// bad cursor from a storage failure. Because rows are immutable and never deleted, the cursor
// keeps referencing the same (created_at, id) while paging: later inserts or expiries can only
// append to pages the client has not reached yet.
func (s *Store) ListHistoryRecords(ctx context.Context, filter HistoryFilter, now time.Time) ([]Record, error) {
	if filter.CursorID != "" {
		if !IsRecordID(filter.CursorID) {
			return nil, ErrInvalidID
		}
		if err := s.validateHistoryCursor(ctx, filter, now); err != nil {
			return nil, err
		}
	}

	query := `
SELECT id, idempotency_key, request_fingerprint, response_snapshot, created_at_ns, expires_at_ns
FROM idempotency_records
WHERE idempotency_key = ? AND expires_at_ns <= ?`
	args := []any{filter.IdempotencyKey, now.UnixNano()}

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

// validateHistoryCursor confirms the cursor references a real row of the same key that still
// satisfies the query predicates. A cursor aimed at another key, an active row, or a row excluded
// by the filters is rejected rather than silently reshaping the page.
func (s *Store) validateHistoryCursor(ctx context.Context, filter HistoryFilter, now time.Time) error {
	query := `
SELECT created_at_ns
FROM idempotency_records
WHERE id = ? AND idempotency_key = ? AND expires_at_ns <= ?`
	args := []any{filter.CursorID, filter.IdempotencyKey, now.UnixNano()}

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

// IsRecordID reports whether id has the fixed shape NewRecordID emits: "rec_" followed by
// exactly 32 lowercase hexadecimal characters.
func IsRecordID(id string) bool {
	return hasIDShape(id, "rec_")
}
