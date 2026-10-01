// Package store owns the SQLite file and every write the service performs.
package store

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"

	_ "modernc.org/sqlite"
)

// Store wraps the SQLite handle so callers never touch database/sql directly.
type Store struct {
	db *sql.DB

	// writeMu serializes the check-then-insert flow in PutRecord within this process. Across
	// processes sharing the same database file, serialization comes from the write transaction
	// itself: every PutRecord runs inside BEGIN IMMEDIATE (see sqliteDSN), so SQLite holds the
	// database write lock from the existence check until commit and a concurrent instance
	// either waits out the winner within busy_timeout or fails with storage_unavailable.
	writeMu sync.Mutex
}

// Open prepares the database file and the schema this service needs.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", sqliteDSN(path))
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("enable wal: %w", err)
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return &Store{db: db}, nil
}

// sqliteDSN turns a plain file path into the driver DSN shared by every pooled connection.
// busy_timeout must be a DSN pragma because a one-off PRAGMA would only cover the single
// connection it ran on; _txlock=immediate makes every write transaction start with BEGIN
// IMMEDIATE, which is what makes the check-then-insert in PutRecord atomic across all
// service instances sharing this file: a concurrent BEGIN IMMEDIATE in another process
// waits up to busy_timeout for the current writer to commit, then observes its row.
func sqliteDSN(path string) string {
	const params = "_pragma=busy_timeout(5000)&_txlock=immediate"
	if strings.HasPrefix(path, "file:") {
		separator := "?"
		if strings.Contains(path, "?") {
			separator = "&"
		}
		return path + separator + params
	}
	// Percent-encode the characters that would otherwise be mistaken for the query
	// string delimiter or corrupt the URI.
	escaped := strings.NewReplacer("%", "%25", "?", "%3F", "#", "%23").Replace(path)
	return "file:" + escaped + "?" + params
}

// Ping reports whether the storage layer is usable.
func (s *Store) Ping() error { return s.db.Ping() }

// Close releases the database handle.
func (s *Store) Close() error { return s.db.Close() }

const schema = `
CREATE TABLE IF NOT EXISTS service_metadata (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS idempotency_records (
	id                  TEXT PRIMARY KEY,
	idempotency_key     TEXT NOT NULL,
	request_fingerprint TEXT NOT NULL,
	response_snapshot   TEXT NOT NULL,
	created_at_ns       INTEGER NOT NULL,
	expires_at_ns       INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_idempotency_records_key
	ON idempotency_records (idempotency_key, created_at_ns DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_idempotency_records_list
	ON idempotency_records (expires_at_ns, created_at_ns DESC, id DESC);
`
