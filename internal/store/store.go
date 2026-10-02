// Package store owns the SQLite file and every write the service performs.
package store

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	sqlite "modernc.org/sqlite"
	sqlite3lib "modernc.org/sqlite/lib"
)

// Store wraps the SQLite handle so callers never touch database/sql directly.
type Store struct {
	db *sql.DB

	// writeMu serializes the check-then-insert flow in PutRecord inside this process. Cross-process
	// safety comes from every write transaction starting as BEGIN IMMEDIATE: SQLite then grants the
	// single RESERVED lock to one connection at a time and makes other instances wait (bounded by
	// busy_timeout), so deployments of several instances sharing one DB_PATH cannot interleave the
	// check and the insert.
	writeMu sync.Mutex
}

// sqliteDSN returns the connection parameters every pooled connection must use:
//   - journal_mode=WAL lets readers and a single writer proceed concurrently;
//   - busy_timeout makes a lock held by another instance wait instead of failing immediately;
//   - _txlock=immediate upgrades every write transaction to BEGIN IMMEDIATE, which acquires the
//     RESERVED lock before the active-record lookup and is the real cross-process serialization
//     point for the check-then-insert in PutRecord.
func sqliteDSN(path string) string {
	return path + "?_txlock=immediate&_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"
}

const (
	schemaBusyRetries = 50
	schemaBusyWait    = 100 * time.Millisecond
)

// Open prepares the database file and the schema this service needs. Multiple Store instances
// (including instances in different processes) may safely share one path.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", sqliteDSN(path))
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// DDL runs as a deferred transaction, so several instances starting on a fresh file at the same
	// moment can observe SQLITE_BUSY/LOCKED despite busy_timeout; retry the idempotent CREATE
	// statements briefly instead of failing startup.
	if err := execWithBusyRetry(func() error {
		_, err := db.Exec(schema)
		return err
	}); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return &Store{db: db}, nil
}

// execWithBusyRetry retries op while SQLite reports the database is busy or locked.
func execWithBusyRetry(op func() error) error {
	var lastErr error
	for attempt := 0; attempt <= schemaBusyRetries; attempt++ {
		lastErr = op()
		if lastErr == nil {
			return nil
		}
		if !isBusyOrLocked(lastErr) {
			return lastErr
		}
		time.Sleep(schemaBusyWait)
	}
	return lastErr
}

func isBusyOrLocked(err error) bool {
	var sqliteErr *sqlite.Error
	if errors.As(err, &sqliteErr) {
		switch sqliteErr.Code() & 0xff {
		case sqlite3lib.SQLITE_BUSY, sqlite3lib.SQLITE_LOCKED:
			return true
		}
	}
	return false
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

CREATE TABLE IF NOT EXISTS idempotency_conflicts (
	id                            TEXT PRIMARY KEY,
	idempotency_key               TEXT NOT NULL,
	observed_request_fingerprint  TEXT NOT NULL,
	existing_record_id            TEXT NOT NULL,
	existing_request_fingerprint  TEXT NOT NULL,
	created_at_ns                 INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_idempotency_conflicts_list
	ON idempotency_conflicts (created_at_ns DESC, id DESC);
`
