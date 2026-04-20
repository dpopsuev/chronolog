package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/dpopsuev/chronolog/internal/domain"
	"github.com/dpopsuev/chronolog/internal/port"
	_ "modernc.org/sqlite" // SQLite driver registration
)

var _ port.Store = (*SQLiteStore)(nil)

const schema = `
CREATE TABLE IF NOT EXISTS domains (
	id TEXT PRIMARY KEY, name TEXT NOT NULL, alias TEXT DEFAULT '',
	description TEXT DEFAULT '', created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS environments (
	id TEXT PRIMARY KEY, domain_id TEXT NOT NULL, name TEXT NOT NULL,
	alias TEXT DEFAULT '', created_at TEXT NOT NULL,
	FOREIGN KEY (domain_id) REFERENCES domains(id)
);
CREATE TABLE IF NOT EXISTS sessions (
	id TEXT PRIMARY KEY, environment_id TEXT NOT NULL, name TEXT NOT NULL,
	alias TEXT DEFAULT '', started_at TEXT NOT NULL, ended_at TEXT DEFAULT '',
	FOREIGN KEY (environment_id) REFERENCES environments(id)
);
CREATE TABLE IF NOT EXISTS instances (
	id TEXT PRIMARY KEY, session_id TEXT NOT NULL, name TEXT NOT NULL,
	alias TEXT DEFAULT '', source_pattern TEXT DEFAULT '',
	started_at TEXT NOT NULL, ended_at TEXT DEFAULT '',
	FOREIGN KEY (session_id) REFERENCES sessions(id)
);
CREATE TABLE IF NOT EXISTS phases (
	id TEXT PRIMARY KEY, instance_id TEXT NOT NULL, name TEXT NOT NULL,
	label TEXT DEFAULT '', started_at TEXT NOT NULL, ended_at TEXT DEFAULT '',
	FOREIGN KEY (instance_id) REFERENCES instances(id)
);
CREATE TABLE IF NOT EXISTS events (
	id TEXT PRIMARY KEY, instance_id TEXT NOT NULL, timestamp TEXT NOT NULL,
	time_confidence TEXT NOT NULL DEFAULT 'unknown',
	message TEXT NOT NULL, source TEXT DEFAULT '',
	source_hash TEXT DEFAULT '', line_number INTEGER DEFAULT 0,
	raw_line TEXT NOT NULL, labels TEXT DEFAULT '{}', created_at TEXT NOT NULL,
	FOREIGN KEY (instance_id) REFERENCES instances(id)
);
CREATE INDEX IF NOT EXISTS idx_events_instance ON events(instance_id, timestamp);
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_dedup ON events(source_hash, line_number);

CREATE TABLE IF NOT EXISTS edges (
	from_id TEXT NOT NULL, relation TEXT NOT NULL, to_id TEXT NOT NULL,
	PRIMARY KEY (from_id, relation, to_id)
);
CREATE INDEX IF NOT EXISTS idx_edges_rev ON edges(to_id, relation, from_id);

CREATE TABLE IF NOT EXISTS services (
	id TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT DEFAULT ''
);
CREATE TABLE IF NOT EXISTS codebases (
	id TEXT PRIMARY KEY, name TEXT NOT NULL, repo_url TEXT DEFAULT '',
	root_path TEXT DEFAULT ''
);
CREATE TABLE IF NOT EXISTS bookmarks (
	id TEXT PRIMARY KEY, event_id TEXT NOT NULL, label TEXT NOT NULL,
	note TEXT DEFAULT '', created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS highlights (
	id TEXT PRIMARY KEY, event_id TEXT NOT NULL, substring TEXT NOT NULL,
	label TEXT DEFAULT '', note TEXT DEFAULT '', created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS buckets (
	id TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT DEFAULT '',
	query TEXT DEFAULT ''
);
CREATE TABLE IF NOT EXISTS aliases (
	alias TEXT PRIMARY KEY, id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS schema_version (
	version INTEGER NOT NULL
);

CREATE VIRTUAL TABLE IF NOT EXISTS events_fts USING fts5(
	event_id, message, source, raw_line
);
`

// SQLiteStore implements port.Store using SQLite.
type SQLiteStore struct {
	db *sql.DB
}

// OpenSQLite opens or creates a SQLite database and applies the schema.
func OpenSQLite(path string, busyTimeoutMs int) (*SQLiteStore, error) {
	if busyTimeoutMs <= 0 {
		busyTimeoutMs = 5000
	}
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create db directory: %w", err)
		}
	}
	dsn := fmt.Sprintf("%s?_pragma=journal_mode(wal)&_pragma=busy_timeout(%d)&_pragma=foreign_keys(on)&_pragma=cache_size(-64000)", path, busyTimeoutMs)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM schema_version").Scan(&count); err != nil {
		db.Close()
		return nil, fmt.Errorf("check schema version: %w", err)
	}
	if count == 0 {
		if _, err := db.Exec("INSERT INTO schema_version (version) VALUES (1)"); err != nil {
			db.Close()
			return nil, fmt.Errorf("set initial schema version: %w", err)
		}
	}
	return &SQLiteStore{db: db}, nil
}

func (s *SQLiteStore) Close() error { return s.db.Close() }

func fmtTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }
func fmtTimePtr(t *time.Time) string {
	if t == nil {
		return ""
	}
	return fmtTime(*t)
}
func parseTime(s string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, s)
	return t
}
func parseTimePtr(s string) *time.Time {
	if s == "" {
		return nil
	}
	t := parseTime(s)
	return &t
}

// --- EventStore ---

func (s *SQLiteStore) PutEvent(ctx context.Context, e *domain.Event) error {
	labelsJSON, _ := json.Marshal(e.Labels)
	_, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO events (id, instance_id, timestamp, time_confidence, message, source, source_hash, line_number, raw_line, labels, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		e.ID, e.InstanceID, fmtTime(e.Timestamp), e.TimeConfidence, e.Message, e.Source, e.SourceHash, e.LineNumber, e.RawLine, string(labelsJSON), fmtTime(e.CreatedAt))
	if err != nil {
		return fmt.Errorf("put event: %w", err)
	}
	_, _ = s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO events_fts (event_id, message, source, raw_line) VALUES (?, ?, ?, ?)`,
		e.ID, e.Message, e.Source, e.RawLine)
	return nil
}

func (s *SQLiteStore) GetEvent(ctx context.Context, id string) (*domain.Event, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, instance_id, timestamp, time_confidence, message, source, source_hash, line_number, raw_line, labels, created_at FROM events WHERE id = ?`, id)
	return scanEvent(row)
}

func (s *SQLiteStore) ListEvents(ctx context.Context, instanceID string, filter port.EventFilter) ([]*domain.Event, error) {
	q := `SELECT id, instance_id, timestamp, time_confidence, message, source, source_hash, line_number, raw_line, labels, created_at FROM events WHERE instance_id = ? ORDER BY timestamp`
	args := []any{instanceID}
	if filter.Limit > 0 {
		q += " LIMIT ?"
		args = append(args, filter.Limit)
	}
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	defer rows.Close()
	var result []*domain.Event
	for rows.Next() {
		e, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, e)
	}
	return result, rows.Err()
}

func (s *SQLiteStore) DeleteEvent(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM events WHERE id = ?`, id)
	return err
}

func (s *SQLiteStore) SearchEvents(ctx context.Context, query string, limit int) ([]*domain.Event, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT e.id, e.instance_id, e.timestamp, e.time_confidence, e.message, e.source, e.source_hash, e.line_number, e.raw_line, e.labels, e.created_at
		 FROM events e JOIN events_fts f ON e.id = f.event_id WHERE events_fts MATCH ? LIMIT ?`, query, limit)
	if err != nil {
		return nil, fmt.Errorf("search events: %w", err)
	}
	defer rows.Close()
	var result []*domain.Event
	for rows.Next() {
		e, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, e)
	}
	return result, rows.Err()
}

type scanner interface {
	Scan(dest ...any) error
}

func scanEvent(s scanner) (*domain.Event, error) {
	var e domain.Event
	var ts, createdAt, labelsJSON string
	if err := s.Scan(&e.ID, &e.InstanceID, &ts, &e.TimeConfidence, &e.Message, &e.Source, &e.SourceHash, &e.LineNumber, &e.RawLine, &labelsJSON, &createdAt); err != nil {
		return nil, err
	}
	e.Timestamp = parseTime(ts)
	e.CreatedAt = parseTime(createdAt)
	_ = json.Unmarshal([]byte(labelsJSON), &e.Labels)
	return &e, nil
}

// --- CascadeStore ---

func (s *SQLiteStore) PutDomain(ctx context.Context, d *domain.Domain) error {
	_, err := s.db.ExecContext(ctx, `INSERT OR REPLACE INTO domains (id, name, alias, description, created_at) VALUES (?, ?, ?, ?, ?)`,
		d.ID, d.Name, d.Alias, d.Description, fmtTime(d.CreatedAt))
	return err
}

func (s *SQLiteStore) GetDomain(ctx context.Context, id string) (*domain.Domain, error) {
	var d domain.Domain
	var createdAt string
	err := s.db.QueryRowContext(ctx, `SELECT id, name, alias, description, created_at FROM domains WHERE id = ?`, id).
		Scan(&d.ID, &d.Name, &d.Alias, &d.Description, &createdAt)
	if err != nil {
		return nil, err
	}
	d.CreatedAt = parseTime(createdAt)
	return &d, nil
}

func (s *SQLiteStore) ListDomains(ctx context.Context) ([]*domain.Domain, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, name, alias, description, created_at FROM domains`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*domain.Domain
	for rows.Next() {
		var d domain.Domain
		var createdAt string
		if err := rows.Scan(&d.ID, &d.Name, &d.Alias, &d.Description, &createdAt); err != nil {
			return nil, err
		}
		d.CreatedAt = parseTime(createdAt)
		result = append(result, &d)
	}
	return result, rows.Err()
}

func (s *SQLiteStore) PutEnvironment(ctx context.Context, e *domain.Environment) error {
	_, err := s.db.ExecContext(ctx, `INSERT OR REPLACE INTO environments (id, domain_id, name, alias, created_at) VALUES (?, ?, ?, ?, ?)`,
		e.ID, e.DomainID, e.Name, e.Alias, fmtTime(e.CreatedAt))
	return err
}

func (s *SQLiteStore) GetEnvironment(ctx context.Context, id string) (*domain.Environment, error) {
	var e domain.Environment
	var createdAt string
	err := s.db.QueryRowContext(ctx, `SELECT id, domain_id, name, alias, created_at FROM environments WHERE id = ?`, id).
		Scan(&e.ID, &e.DomainID, &e.Name, &e.Alias, &createdAt)
	if err != nil {
		return nil, err
	}
	e.CreatedAt = parseTime(createdAt)
	return &e, nil
}

//nolint:dupl // structural similarity with ListBookmarks; different types
func (s *SQLiteStore) ListEnvironments(ctx context.Context, domainID string) ([]*domain.Environment, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, domain_id, name, alias, created_at FROM environments WHERE domain_id = ?`, domainID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*domain.Environment
	for rows.Next() {
		var e domain.Environment
		var createdAt string
		if err := rows.Scan(&e.ID, &e.DomainID, &e.Name, &e.Alias, &createdAt); err != nil {
			return nil, err
		}
		e.CreatedAt = parseTime(createdAt)
		result = append(result, &e)
	}
	return result, rows.Err()
}

func (s *SQLiteStore) PutSession(ctx context.Context, sess *domain.Session) error {
	_, err := s.db.ExecContext(ctx, `INSERT OR REPLACE INTO sessions (id, environment_id, name, alias, started_at, ended_at) VALUES (?, ?, ?, ?, ?, ?)`,
		sess.ID, sess.EnvironmentID, sess.Name, sess.Alias, fmtTime(sess.StartedAt), fmtTimePtr(sess.EndedAt))
	return err
}

func (s *SQLiteStore) GetSession(ctx context.Context, id string) (*domain.Session, error) {
	var sess domain.Session
	var startedAt, endedAt string
	err := s.db.QueryRowContext(ctx, `SELECT id, environment_id, name, alias, started_at, ended_at FROM sessions WHERE id = ?`, id).
		Scan(&sess.ID, &sess.EnvironmentID, &sess.Name, &sess.Alias, &startedAt, &endedAt)
	if err != nil {
		return nil, err
	}
	sess.StartedAt = parseTime(startedAt)
	sess.EndedAt = parseTimePtr(endedAt)
	return &sess, nil
}

//nolint:dupl // structural similarity with ListPhases; different types
func (s *SQLiteStore) ListSessions(ctx context.Context, envID string) ([]*domain.Session, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, environment_id, name, alias, started_at, ended_at FROM sessions WHERE environment_id = ?`, envID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*domain.Session
	for rows.Next() {
		var sess domain.Session
		var startedAt, endedAt string
		if err := rows.Scan(&sess.ID, &sess.EnvironmentID, &sess.Name, &sess.Alias, &startedAt, &endedAt); err != nil {
			return nil, err
		}
		sess.StartedAt = parseTime(startedAt)
		sess.EndedAt = parseTimePtr(endedAt)
		result = append(result, &sess)
	}
	return result, rows.Err()
}

func (s *SQLiteStore) PutInstance(ctx context.Context, inst *domain.Instance) error {
	_, err := s.db.ExecContext(ctx, `INSERT OR REPLACE INTO instances (id, session_id, name, alias, source_pattern, started_at, ended_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		inst.ID, inst.SessionID, inst.Name, inst.Alias, inst.SourcePattern, fmtTime(inst.StartedAt), fmtTimePtr(inst.EndedAt))
	return err
}

func (s *SQLiteStore) GetInstance(ctx context.Context, id string) (*domain.Instance, error) {
	var inst domain.Instance
	var startedAt, endedAt string
	err := s.db.QueryRowContext(ctx, `SELECT id, session_id, name, alias, source_pattern, started_at, ended_at FROM instances WHERE id = ?`, id).
		Scan(&inst.ID, &inst.SessionID, &inst.Name, &inst.Alias, &inst.SourcePattern, &startedAt, &endedAt)
	if err != nil {
		return nil, err
	}
	inst.StartedAt = parseTime(startedAt)
	inst.EndedAt = parseTimePtr(endedAt)
	return &inst, nil
}

func (s *SQLiteStore) ListInstances(ctx context.Context, sessionID string) ([]*domain.Instance, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, session_id, name, alias, source_pattern, started_at, ended_at FROM instances WHERE session_id = ?`, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*domain.Instance
	for rows.Next() {
		var inst domain.Instance
		var startedAt, endedAt string
		if err := rows.Scan(&inst.ID, &inst.SessionID, &inst.Name, &inst.Alias, &inst.SourcePattern, &startedAt, &endedAt); err != nil {
			return nil, err
		}
		inst.StartedAt = parseTime(startedAt)
		inst.EndedAt = parseTimePtr(endedAt)
		result = append(result, &inst)
	}
	return result, rows.Err()
}

func (s *SQLiteStore) PutPhase(ctx context.Context, p *domain.Phase) error {
	_, err := s.db.ExecContext(ctx, `INSERT OR REPLACE INTO phases (id, instance_id, name, label, started_at, ended_at) VALUES (?, ?, ?, ?, ?, ?)`,
		p.ID, p.InstanceID, p.Name, p.Label, fmtTime(p.StartedAt), fmtTimePtr(p.EndedAt))
	return err
}

//nolint:dupl // structural similarity with ListSessions; different types
func (s *SQLiteStore) ListPhases(ctx context.Context, instanceID string) ([]*domain.Phase, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, instance_id, name, label, started_at, ended_at FROM phases WHERE instance_id = ?`, instanceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*domain.Phase
	for rows.Next() {
		var p domain.Phase
		var startedAt, endedAt string
		if err := rows.Scan(&p.ID, &p.InstanceID, &p.Name, &p.Label, &startedAt, &endedAt); err != nil {
			return nil, err
		}
		p.StartedAt = parseTime(startedAt)
		p.EndedAt = parseTimePtr(endedAt)
		result = append(result, &p)
	}
	return result, rows.Err()
}

// --- GraphStore ---

func (s *SQLiteStore) AddEdge(ctx context.Context, e domain.Edge) error {
	_, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO edges (from_id, relation, to_id) VALUES (?, ?, ?)`,
		e.FromID, e.Relation, e.ToID)
	return err
}

func (s *SQLiteStore) RemoveEdge(ctx context.Context, e domain.Edge) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM edges WHERE from_id = ? AND relation = ? AND to_id = ?`,
		e.FromID, e.Relation, e.ToID)
	return err
}

func (s *SQLiteStore) Neighbors(ctx context.Context, id, relation string, dir port.Direction) ([]domain.Edge, error) {
	var q string
	var args []any
	switch dir {
	case port.Outgoing:
		q = `SELECT from_id, relation, to_id FROM edges WHERE from_id = ?`
		args = []any{id}
	case port.Incoming:
		q = `SELECT from_id, relation, to_id FROM edges WHERE to_id = ?`
		args = []any{id}
	case port.Both:
		q = `SELECT from_id, relation, to_id FROM edges WHERE from_id = ? OR to_id = ?`
		args = []any{id, id}
	}
	if relation != "" {
		q += ` AND relation = ?`
		args = append(args, relation)
	}
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []domain.Edge
	for rows.Next() {
		var e domain.Edge
		if err := rows.Scan(&e.FromID, &e.Relation, &e.ToID); err != nil {
			return nil, err
		}
		result = append(result, e)
	}
	return result, rows.Err()
}

// --- AnnotationStore ---

func (s *SQLiteStore) PutBookmark(ctx context.Context, b *domain.Bookmark) error {
	_, err := s.db.ExecContext(ctx, `INSERT OR REPLACE INTO bookmarks (id, event_id, label, note, created_at) VALUES (?, ?, ?, ?, ?)`,
		b.ID, b.EventID, b.Label, b.Note, fmtTime(b.CreatedAt))
	return err
}

//nolint:dupl // structural similarity with ListEnvironments; different types
func (s *SQLiteStore) ListBookmarks(ctx context.Context, eventID string) ([]*domain.Bookmark, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, event_id, label, note, created_at FROM bookmarks WHERE event_id = ?`, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*domain.Bookmark
	for rows.Next() {
		var b domain.Bookmark
		var createdAt string
		if err := rows.Scan(&b.ID, &b.EventID, &b.Label, &b.Note, &createdAt); err != nil {
			return nil, err
		}
		b.CreatedAt = parseTime(createdAt)
		result = append(result, &b)
	}
	return result, rows.Err()
}

func (s *SQLiteStore) PutHighlight(ctx context.Context, h *domain.Highlight) error {
	_, err := s.db.ExecContext(ctx, `INSERT OR REPLACE INTO highlights (id, event_id, substring, label, note, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		h.ID, h.EventID, h.Substring, h.Label, h.Note, fmtTime(h.CreatedAt))
	return err
}

func (s *SQLiteStore) ListHighlights(ctx context.Context, eventID string) ([]*domain.Highlight, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, event_id, substring, label, note, created_at FROM highlights WHERE event_id = ?`, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*domain.Highlight
	for rows.Next() {
		var h domain.Highlight
		var createdAt string
		if err := rows.Scan(&h.ID, &h.EventID, &h.Substring, &h.Label, &h.Note, &createdAt); err != nil {
			return nil, err
		}
		h.CreatedAt = parseTime(createdAt)
		result = append(result, &h)
	}
	return result, rows.Err()
}

// --- RegistryStore ---

func (s *SQLiteStore) PutService(ctx context.Context, svc *domain.Service) error {
	_, err := s.db.ExecContext(ctx, `INSERT OR REPLACE INTO services (id, name, description) VALUES (?, ?, ?)`,
		svc.ID, svc.Name, svc.Description)
	return err
}

func (s *SQLiteStore) GetService(ctx context.Context, id string) (*domain.Service, error) {
	var svc domain.Service
	err := s.db.QueryRowContext(ctx, `SELECT id, name, description FROM services WHERE id = ?`, id).
		Scan(&svc.ID, &svc.Name, &svc.Description)
	return &svc, err
}

func (s *SQLiteStore) ListServices(ctx context.Context) ([]*domain.Service, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, name, description FROM services`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*domain.Service
	for rows.Next() {
		var svc domain.Service
		if err := rows.Scan(&svc.ID, &svc.Name, &svc.Description); err != nil {
			return nil, err
		}
		result = append(result, &svc)
	}
	return result, rows.Err()
}

func (s *SQLiteStore) PutCodebase(ctx context.Context, c *domain.Codebase) error {
	_, err := s.db.ExecContext(ctx, `INSERT OR REPLACE INTO codebases (id, name, repo_url, root_path) VALUES (?, ?, ?, ?)`,
		c.ID, c.Name, c.RepoURL, c.RootPath)
	return err
}

func (s *SQLiteStore) GetCodebase(ctx context.Context, id string) (*domain.Codebase, error) {
	var c domain.Codebase
	err := s.db.QueryRowContext(ctx, `SELECT id, name, repo_url, root_path FROM codebases WHERE id = ?`, id).
		Scan(&c.ID, &c.Name, &c.RepoURL, &c.RootPath)
	return &c, err
}

//nolint:dupl // structural similarity with ListBuckets; different types
func (s *SQLiteStore) ListCodebases(ctx context.Context) ([]*domain.Codebase, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, name, repo_url, root_path FROM codebases`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*domain.Codebase
	for rows.Next() {
		var c domain.Codebase
		if err := rows.Scan(&c.ID, &c.Name, &c.RepoURL, &c.RootPath); err != nil {
			return nil, err
		}
		result = append(result, &c)
	}
	return result, rows.Err()
}

// --- BucketStore ---

func (s *SQLiteStore) PutBucket(ctx context.Context, b *domain.Bucket) error {
	_, err := s.db.ExecContext(ctx, `INSERT OR REPLACE INTO buckets (id, name, description, query) VALUES (?, ?, ?, ?)`,
		b.ID, b.Name, b.Description, b.Query)
	return err
}

func (s *SQLiteStore) GetBucket(ctx context.Context, id string) (*domain.Bucket, error) {
	var b domain.Bucket
	err := s.db.QueryRowContext(ctx, `SELECT id, name, description, query FROM buckets WHERE id = ?`, id).
		Scan(&b.ID, &b.Name, &b.Description, &b.Query)
	return &b, err
}

//nolint:dupl // structural similarity with ListCodebases; different types
func (s *SQLiteStore) ListBuckets(ctx context.Context) ([]*domain.Bucket, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, name, description, query FROM buckets`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*domain.Bucket
	for rows.Next() {
		var b domain.Bucket
		if err := rows.Scan(&b.ID, &b.Name, &b.Description, &b.Query); err != nil {
			return nil, err
		}
		result = append(result, &b)
	}
	return result, rows.Err()
}

func (s *SQLiteStore) DeleteBucket(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM buckets WHERE id = ?`, id)
	return err
}

// --- MetaStore ---

func (s *SQLiteStore) SetAlias(ctx context.Context, id, alias string) error {
	_, err := s.db.ExecContext(ctx, `INSERT OR REPLACE INTO aliases (alias, id) VALUES (?, ?)`, alias, id)
	return err
}

func (s *SQLiteStore) ResolveAlias(ctx context.Context, alias string) (string, error) {
	var id string
	err := s.db.QueryRowContext(ctx, `SELECT id FROM aliases WHERE alias = ?`, alias).Scan(&id)
	if err != nil {
		return "", fmt.Errorf("alias %q not found: %w", alias, err)
	}
	return id, nil
}

func (s *SQLiteStore) SchemaVersion(ctx context.Context) (int, error) {
	var v int
	err := s.db.QueryRowContext(ctx, `SELECT version FROM schema_version LIMIT 1`).Scan(&v)
	return v, err
}

func (s *SQLiteStore) SetSchemaVersion(ctx context.Context, version int) error {
	_, err := s.db.ExecContext(ctx, `UPDATE schema_version SET version = ?`, version)
	return err
}
