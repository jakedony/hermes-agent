package main

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// Store is the hub SQLite database. State changes commit in one transaction
// with their audit events and command acknowledgement, or they do not happen.
type Store struct {
	db *sql.DB
	// failBegin, when set, makes the next withTx fail before any write.
	failBegin error
}

func openStore(path string) (*Store, error) {
	dsn := "file:" + path + "?_busy_timeout=5000&_foreign_keys=on&_journal_mode=WAL&_synchronous=FULL&_txlock=immediate"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetConnMaxLifetime(0)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.Exec(`PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;`); err != nil {
		_ = db.Close()
		return nil, err
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	if _, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (
		version INTEGER PRIMARY KEY,
		applied_at INTEGER NOT NULL
	)`); err != nil {
		return err
	}
	var v int
	if err := s.db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&v); err != nil {
		return err
	}
	if v >= 1 {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(schemaV1); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	if _, err := tx.Exec(`INSERT INTO schema_migrations(version, applied_at) VALUES (1, ?)`, time.Now().UnixMilli()); err != nil {
		return err
	}
	return tx.Commit()
}

const schemaV1 = `
CREATE TABLE meta (
	k TEXT PRIMARY KEY,
	v INTEGER NOT NULL
);
INSERT INTO meta(k, v) VALUES ('event_seq', 0), ('last_wall', 0);

CREATE TABLE tasks (
	id TEXT PRIMARY KEY,
	room_id TEXT NOT NULL,
	root_id TEXT NOT NULL,
	parent_id TEXT NOT NULL DEFAULT '',
	assigned_to TEXT NOT NULL,
	requested_by TEXT NOT NULL,
	coordinator_id TEXT NOT NULL,
	objective TEXT NOT NULL,
	context_json TEXT NOT NULL,
	profile TEXT NOT NULL,
	state TEXT NOT NULL,
	deadline_at INTEGER NOT NULL,
	not_before INTEGER NOT NULL DEFAULT 0,
	current_attempt_id TEXT NOT NULL DEFAULT '',
	attempt_count INTEGER NOT NULL DEFAULT 0,
	execution_ms INTEGER NOT NULL DEFAULT 0,
	result_json TEXT NOT NULL DEFAULT '',
	error_class TEXT NOT NULL DEFAULT '',
	error_message TEXT NOT NULL DEFAULT '',
	created_at INTEGER NOT NULL,
	updated_at INTEGER NOT NULL
);
CREATE INDEX idx_tasks_assignee ON tasks(assigned_to, state);
CREATE INDEX idx_tasks_root ON tasks(root_id);

CREATE TABLE attempts (
	id TEXT PRIMARY KEY,
	task_id TEXT NOT NULL REFERENCES tasks(id),
	n INTEGER NOT NULL,
	state TEXT NOT NULL,
	lease_expires_at INTEGER NOT NULL,
	started_at INTEGER NOT NULL,
	finished_at INTEGER NOT NULL DEFAULT 0,
	UNIQUE(task_id, n)
);

CREATE TABLE events (
	id TEXT PRIMARY KEY,
	seq INTEGER NOT NULL UNIQUE,
	room_id TEXT NOT NULL,
	task_id TEXT NOT NULL DEFAULT '',
	type TEXT NOT NULL,
	actor TEXT NOT NULL,
	payload_json TEXT NOT NULL,
	created_at INTEGER NOT NULL
);
CREATE INDEX idx_events_task ON events(task_id);

CREATE TABLE requests (
	principal TEXT NOT NULL,
	request_id TEXT NOT NULL,
	payload_hash TEXT NOT NULL,
	ok INTEGER NOT NULL,
	code TEXT NOT NULL,
	message TEXT NOT NULL,
	body_json TEXT NOT NULL,
	created_at INTEGER NOT NULL,
	PRIMARY KEY (principal, request_id)
);
CREATE INDEX idx_requests_created ON requests(created_at);
`

func (s *Store) withTx(fn func(*sql.Tx) error) error {
	if s.failBegin != nil {
		err := s.failBegin
		s.failBegin = nil
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
