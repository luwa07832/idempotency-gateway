// Package store owns the SQLite file and every write the service performs.
package store

import (
	"database/sql"
	"fmt"
	"sync"

	_ "modernc.org/sqlite"
)

// Store wraps the SQLite handle so callers never touch database/sql directly.
type Store struct {
	db *sql.DB

	// writeMu serializes the check-then-insert flow in PutRecord so concurrent first requests
	// for the same idempotency key cannot both observe "missing" and insert competing rows.
	writeMu sync.Mutex
}

// Open prepares the database file and the schema this service needs.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("enable wal: %w", err)
	}
	if _, err := db.Exec("PRAGMA busy_timeout = 5000"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set busy timeout: %w", err)
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return &Store{db: db}, nil
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
