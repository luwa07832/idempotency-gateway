// Package store owns the SQLite file and every write the service performs.
package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Record is one immutable idempotency record. The response snapshot is never
// overwritten after insertion.
type Record struct {
	ID                 string
	IdempotencyKey     string
	RequestFingerprint string
	ResponseSnapshot   json.RawMessage
	CreatedAt          time.Time
	ExpiresAt          time.Time
}

// Cursor points at the last record returned by a previous list call.
type Cursor struct {
	CreatedAt time.Time
	RecordID  string
}

// ListFilter narrows an active-record listing. Zero-valued pointers disable a
// bound. A zero Cursor disables cursor pagination.
type ListFilter struct {
	ExpiresAfter  *time.Time
	ExpiresBefore *time.Time
	Cursor        *Cursor
	Limit         int
}

// NewRecordID returns an opaque, unique record identifier.
func NewRecordID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate record id: %w", err)
	}
	return "rec_" + hex.EncodeToString(raw[:]), nil
}

// PutIfAbsent stores rec only when no unexpired record shares its idempotency
// key. It returns the existing record (and inserted == false) when an active
// record is present, and the stored record (inserted == true) otherwise.
//
// The transaction starts as IMMEDIATE so concurrent callers serialize on the
// SQLite write lock and the first stored snapshot always wins.
func (s *Store) PutIfAbsent(ctx context.Context, rec Record) (*Record, bool, error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("acquire conn: %w", err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return nil, false, fmt.Errorf("begin tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		}
	}()

	now := time.Now().UTC().UnixNano()
	existing, err := queryActiveRecord(ctx, conn, rec.IdempotencyKey, now)
	if err != nil {
		return nil, false, err
	}
	if existing != nil {
		if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
			return nil, false, fmt.Errorf("commit: %w", err)
		}
		committed = true
		return existing, false, nil
	}

	if _, err := conn.ExecContext(ctx, `
INSERT INTO idempotency_records (
	record_id, idempotency_key, request_fingerprint, response_snapshot,
	created_at, created_unix_nano, expires_at, expires_unix_nano
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		rec.ID,
		rec.IdempotencyKey,
		rec.RequestFingerprint,
		string(rec.ResponseSnapshot),
		formatTimestamp(rec.CreatedAt),
		rec.CreatedAt.UnixNano(),
		formatTimestamp(rec.ExpiresAt),
		rec.ExpiresAt.UnixNano(),
	); err != nil {
		return nil, false, fmt.Errorf("insert record: %w", err)
	}

	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return nil, false, fmt.Errorf("commit: %w", err)
	}
	committed = true
	stored := rec
	return &stored, true, nil
}

// ActiveRecord returns the single unexpired record for key, or nil when none
// exists. Expired rows are never exposed.
func (s *Store) ActiveRecord(ctx context.Context, key string) (*Record, error) {
	now := time.Now().UTC().UnixNano()
	row := s.db.QueryRowContext(ctx, activeRecordSelect+`
ORDER BY created_unix_nano DESC, record_id DESC
LIMIT 1`, key, now)
	rec, err := scanRecord(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return rec, nil
}

// ListActive returns unexpired records in created-at-descending order,
// tie-broken by record id. It fetches one extra row to report whether more
// records follow; when hasMore is true the last returned record is the next
// cursor position.
func (s *Store) ListActive(ctx context.Context, filter ListFilter) (records []Record, hasMore bool, err error) {
	now := time.Now().UTC().UnixNano()
	query := `
SELECT record_id, idempotency_key, request_fingerprint, response_snapshot,
       created_unix_nano, expires_unix_nano
FROM idempotency_records
WHERE expires_unix_nano > ?`
	args := []any{now}
	if filter.ExpiresAfter != nil {
		query += ` AND expires_unix_nano >= ?`
		args = append(args, filter.ExpiresAfter.UnixNano())
	}
	if filter.ExpiresBefore != nil {
		query += ` AND expires_unix_nano <= ?`
		args = append(args, filter.ExpiresBefore.UnixNano())
	}
	if filter.Cursor != nil {
		query += ` AND (created_unix_nano < ? OR (created_unix_nano = ? AND record_id < ?))`
		args = append(args, filter.Cursor.CreatedAt.UnixNano(), filter.Cursor.CreatedAt.UnixNano(), filter.Cursor.RecordID)
	}
	query += ` ORDER BY created_unix_nano DESC, record_id DESC LIMIT ?`
	args = append(args, filter.Limit+1)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("list records: %w", err)
	}
	defer rows.Close()

	records = make([]Record, 0, filter.Limit)
	for rows.Next() {
		rec, scanErr := scanRecord(rows)
		if scanErr != nil {
			return nil, false, scanErr
		}
		records = append(records, *rec)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("iterate records: %w", err)
	}
	if len(records) > filter.Limit {
		hasMore = true
		records = records[:filter.Limit]
	}
	return records, hasMore, nil
}

const activeRecordSelect = `
SELECT record_id, idempotency_key, request_fingerprint, response_snapshot,
       created_unix_nano, expires_unix_nano
FROM idempotency_records
WHERE idempotency_key = ? AND expires_unix_nano > ?
`

type rowScanner interface {
	Scan(dest ...any) error
}

func queryActiveRecord(ctx context.Context, q queryRower, key string, nowNano int64) (*Record, error) {
	row := q.QueryRowContext(ctx, activeRecordSelect+`
ORDER BY created_unix_nano DESC, record_id DESC
LIMIT 1`, key, nowNano)
	rec, err := scanRecord(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return rec, nil
}

type queryRower interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func scanRecord(sc rowScanner) (*Record, error) {
	var rec Record
	var snapshot []byte
	var createdNano, expiresNano int64
	if err := sc.Scan(
		&rec.ID,
		&rec.IdempotencyKey,
		&rec.RequestFingerprint,
		&snapshot,
		&createdNano,
		&expiresNano,
	); err != nil {
		return nil, err
	}
	rec.ResponseSnapshot = json.RawMessage(snapshot)
	rec.CreatedAt = time.Unix(0, createdNano).UTC()
	rec.ExpiresAt = time.Unix(0, expiresNano).UTC()
	return &rec, nil
}

func formatTimestamp(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}
