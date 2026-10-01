// Package store owns the SQLite file and every write the service performs.
package store

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

// Store wraps the SQLite handle so callers never touch database/sql directly.
type Store struct {
	db *sql.DB
}

// Open prepares the database file and the schema this service needs.
func Open(path string) (*Store, error) {
	// DSN pragmas apply to every pooled connection.
	dsn := "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	for _, statement := range schema {
		if _, err := db.Exec(statement); err != nil {
			db.Close()
			return nil, fmt.Errorf("apply schema: %w", err)
		}
	}
	return &Store{db: db}, nil
}

// Ping reports whether the storage layer is usable.
func (s *Store) Ping() error { return s.db.Ping() }

// Close releases the database handle.
func (s *Store) Close() error { return s.db.Close() }

var schema = []string{
	`CREATE TABLE IF NOT EXISTS service_metadata (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);`,
	`CREATE TABLE IF NOT EXISTS idempotency_records (
	record_id          TEXT PRIMARY KEY,
	idempotency_key    TEXT NOT NULL,
	request_fingerprint TEXT NOT NULL,
	response_snapshot  TEXT NOT NULL,
	created_at         TEXT NOT NULL,
	created_unix_nano  INTEGER NOT NULL,
	expires_at         TEXT NOT NULL,
	expires_unix_nano  INTEGER NOT NULL
);`,
	`CREATE INDEX IF NOT EXISTS idx_idempotency_records_active_key
	ON idempotency_records (idempotency_key, expires_unix_nano);`,
	`CREATE INDEX IF NOT EXISTS idx_idempotency_records_list
	ON idempotency_records (created_unix_nano DESC, record_id DESC);`,
}
