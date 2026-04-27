package store

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/dpopsuev/chronolog/internal/domain"
	"github.com/dpopsuev/chronolog/internal/port"
)

func openTestDB(t *testing.T) *SQLiteStore {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := OpenSQLite(path, 5000)
	if err != nil {
		t.Fatalf("OpenSQLite: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestSQLite_SchemaVersion(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()

	v, err := s.SchemaVersion(ctx)
	if err != nil {
		t.Fatalf("SchemaVersion: %v", err)
	}
	if v != 1 {
		t.Errorf("initial version = %d, want 1", v)
	}
}

func TestSQLite_CascadeCRUD(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()

	d := &domain.Domain{ID: "d1", Name: "OpenShift PTP", CreatedAt: time.Now().UTC()}
	if err := s.PutDomain(ctx, d); err != nil {
		t.Fatalf("PutDomain: %v", err)
	}

	got, err := s.GetDomain(ctx, "d1")
	if err != nil {
		t.Fatalf("GetDomain: %v", err)
	}
	if got.Name != "OpenShift PTP" {
		t.Errorf("domain name = %q, want %q", got.Name, "OpenShift PTP")
	}

	env := &domain.Environment{ID: "e1", DomainID: "d1", Name: "4.16", CreatedAt: time.Now().UTC()}
	if err := s.PutEnvironment(ctx, env); err != nil {
		t.Fatalf("PutEnvironment: %v", err)
	}

	envs, err := s.ListEnvironments(ctx, "d1")
	if err != nil {
		t.Fatalf("ListEnvironments: %v", err)
	}
	if len(envs) != 1 {
		t.Fatalf("len = %d, want 1", len(envs))
	}

	sess := &domain.Session{ID: "s1", EnvironmentID: "e1", Name: "dec-20", StartedAt: time.Now().UTC()}
	if err := s.PutSession(ctx, sess); err != nil {
		t.Fatalf("PutSession: %v", err)
	}

	inst := &domain.Instance{ID: "i1", SessionID: "s1", Name: "ptp_bc_freerun", StartedAt: time.Now().UTC()}
	if err := s.PutInstance(ctx, inst); err != nil {
		t.Fatalf("PutInstance: %v", err)
	}

	instances, err := s.ListInstances(ctx, "s1")
	if err != nil {
		t.Fatalf("ListInstances: %v", err)
	}
	if len(instances) != 1 {
		t.Fatalf("len = %d, want 1", len(instances))
	}
}

func TestSQLite_EventCRUD(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()

	s.PutDomain(ctx, &domain.Domain{ID: "d1", Name: "test", CreatedAt: time.Now().UTC()})
	s.PutEnvironment(ctx, &domain.Environment{ID: "e1", DomainID: "d1", Name: "env", CreatedAt: time.Now().UTC()})
	s.PutSession(ctx, &domain.Session{ID: "s1", EnvironmentID: "e1", Name: "sess", StartedAt: time.Now().UTC()})
	s.PutInstance(ctx, &domain.Instance{ID: "i1", SessionID: "s1", Name: "inst", StartedAt: time.Now().UTC()})

	now := time.Now().UTC()
	e := &domain.Event{
		ID: "ev1", InstanceID: "i1", Timestamp: now,
		TimeConfidence: domain.ConfidenceRFC3339,
		Message:        "FREERUN published", Source: "cloud-events.log",
		SourceHash: "abc123", LineNumber: 42,
		RawLine:   "2025-12-20T14:59:21Z FREERUN published",
		CreatedAt: now,
	}
	if err := s.PutEvent(ctx, e); err != nil {
		t.Fatalf("PutEvent: %v", err)
	}

	got, err := s.GetEvent(ctx, "ev1")
	if err != nil {
		t.Fatalf("GetEvent: %v", err)
	}
	if got.Message != "FREERUN published" {
		t.Errorf("message = %q, want %q", got.Message, "FREERUN published")
	}

	events, err := s.ListEvents(ctx, "i1", port.EventFilter{})
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len = %d, want 1", len(events))
	}
}

func TestSQLite_IdempotentIntake(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()

	s.PutDomain(ctx, &domain.Domain{ID: "d1", Name: "test", CreatedAt: time.Now().UTC()})
	s.PutEnvironment(ctx, &domain.Environment{ID: "e1", DomainID: "d1", Name: "env", CreatedAt: time.Now().UTC()})
	s.PutSession(ctx, &domain.Session{ID: "s1", EnvironmentID: "e1", Name: "sess", StartedAt: time.Now().UTC()})
	s.PutInstance(ctx, &domain.Instance{ID: "i1", SessionID: "s1", Name: "inst", StartedAt: time.Now().UTC()})

	now := time.Now().UTC()
	e := &domain.Event{
		ID: "ev1", InstanceID: "i1", Timestamp: now,
		TimeConfidence: domain.ConfidenceRFC3339,
		Message:        "test", Source: "a.log", SourceHash: "hash1", LineNumber: 1,
		RawLine: "test", CreatedAt: now,
	}
	if err := s.PutEvent(ctx, e); err != nil {
		t.Fatalf("PutEvent first: %v", err)
	}

	e2 := &domain.Event{
		ID: "ev2", InstanceID: "i1", Timestamp: now,
		TimeConfidence: domain.ConfidenceRFC3339,
		Message:        "test", Source: "a.log", SourceHash: "hash1", LineNumber: 1,
		RawLine: "test", CreatedAt: now,
	}
	if err := s.PutEvent(ctx, e2); err != nil {
		t.Fatalf("PutEvent second: %v", err)
	}

	events, err := s.ListEvents(ctx, "i1", port.EventFilter{})
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(events) != 1 {
		t.Errorf("idempotent intake: len = %d, want 1 (INSERT OR IGNORE)", len(events))
	}
}

func TestSQLite_FTS5Search(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()

	s.PutDomain(ctx, &domain.Domain{ID: "d1", Name: "test", CreatedAt: time.Now().UTC()})
	s.PutEnvironment(ctx, &domain.Environment{ID: "e1", DomainID: "d1", Name: "env", CreatedAt: time.Now().UTC()})
	s.PutSession(ctx, &domain.Session{ID: "s1", EnvironmentID: "e1", Name: "sess", StartedAt: time.Now().UTC()})
	s.PutInstance(ctx, &domain.Instance{ID: "i1", SessionID: "s1", Name: "inst", StartedAt: time.Now().UTC()})

	now := time.Now().UTC()
	s.PutEvent(ctx, &domain.Event{ID: "ev1", InstanceID: "i1", Timestamp: now, TimeConfidence: "rfc3339", Message: "master offset 3ns", Source: "a.log", SourceHash: "h1", LineNumber: 1, RawLine: "master offset 3ns", CreatedAt: now})
	s.PutEvent(ctx, &domain.Event{ID: "ev2", InstanceID: "i1", Timestamp: now.Add(time.Second), TimeConfidence: "rfc3339", Message: "FREERUN published", Source: "b.log", SourceHash: "h2", LineNumber: 1, RawLine: "FREERUN published", CreatedAt: now})
	s.PutEvent(ctx, &domain.Event{ID: "ev3", InstanceID: "i1", Timestamp: now.Add(2 * time.Second), TimeConfidence: "rfc3339", Message: "master offset 5ns", Source: "a.log", SourceHash: "h3", LineNumber: 1, RawLine: "master offset 5ns", CreatedAt: now})

	results, err := s.SearchEvents(ctx, "FREERUN", 10)
	if err != nil {
		t.Fatalf("SearchEvents FREERUN: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("SearchEvents FREERUN len = %d, want 1", len(results))
	}
	if results[0].ID != "ev2" {
		t.Errorf("result ID = %q, want ev2", results[0].ID)
	}

	results, err = s.SearchEvents(ctx, "offset", 10)
	if err != nil {
		t.Fatalf("SearchEvents offset: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("SearchEvents offset len = %d, want 2", len(results))
	}
}

func TestSQLite_Edges(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()

	e := domain.Edge{FromID: "a", Relation: domain.RelContains, ToID: "b"}
	if err := s.AddEdge(ctx, e); err != nil {
		t.Fatalf("AddEdge: %v", err)
	}

	if err := s.AddEdge(ctx, e); err != nil {
		t.Fatalf("AddEdge duplicate: %v", err)
	}

	out, err := s.Neighbors(ctx, "a", domain.RelContains, port.Outgoing)
	if err != nil {
		t.Fatalf("Neighbors: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("Neighbors len = %d, want 1", len(out))
	}

	if err := s.RemoveEdge(ctx, e); err != nil {
		t.Fatalf("RemoveEdge: %v", err)
	}

	out, err = s.Neighbors(ctx, "a", "", port.Both)
	if err != nil {
		t.Fatalf("Neighbors after remove: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("Neighbors after remove len = %d, want 0", len(out))
	}
}

func TestSQLite_ListEvents_AfterBefore(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()

	s.PutDomain(ctx, &domain.Domain{ID: "d1", Name: "test", CreatedAt: time.Now().UTC()})
	s.PutEnvironment(ctx, &domain.Environment{ID: "e1", DomainID: "d1", Name: "env", CreatedAt: time.Now().UTC()})
	s.PutSession(ctx, &domain.Session{ID: "s1", EnvironmentID: "e1", Name: "sess", StartedAt: time.Now().UTC()})
	s.PutInstance(ctx, &domain.Instance{ID: "i1", SessionID: "s1", Name: "inst", StartedAt: time.Now().UTC()})

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range 5 {
		ts := base.Add(time.Duration(i) * time.Second)
		e := &domain.Event{
			ID: fmt.Sprintf("ev%d", i), InstanceID: "i1", Timestamp: ts,
			TimeConfidence: domain.ConfidenceRFC3339,
			Message:        fmt.Sprintf("event %d", i), Source: "a.log",
			SourceHash: fmt.Sprintf("h%d", i), LineNumber: i + 1,
			RawLine: fmt.Sprintf("event %d", i), CreatedAt: ts,
		}
		if err := s.PutEvent(ctx, e); err != nil {
			t.Fatalf("PutEvent %d: %v", i, err)
		}
	}

	after := base.Add(time.Second)      // after ev0, ev1
	before := base.Add(4 * time.Second) // before ev4
	events, err := s.ListEvents(ctx, "i1", port.EventFilter{After: &after, Before: &before})
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("ListEvents len = %d, want 2 (ev2, ev3)", len(events))
	}
	if events[0].ID != "ev2" || events[1].ID != "ev3" {
		t.Errorf("events = [%s, %s], want [ev2, ev3]", events[0].ID, events[1].ID)
	}
}

func TestSQLite_InstanceMaquette(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()
	s.PutDomain(ctx, &domain.Domain{ID: "d1", Name: "test", CreatedAt: time.Now().UTC()})
	s.PutEnvironment(ctx, &domain.Environment{ID: "e1", DomainID: "d1", Name: "env", CreatedAt: time.Now().UTC()})
	s.PutSession(ctx, &domain.Session{ID: "s1", EnvironmentID: "e1", Name: "sess", StartedAt: time.Now().UTC()})

	inst := &domain.Instance{
		ID: "i1", SessionID: "s1", Name: "with-maquette",
		StartedAt: time.Now().UTC(),
		Maquette: &domain.Maquette{
			Timestamp: &domain.MaquetteTimestamp{
				Regex:  `^(\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})`,
				Format: "Jan 2 15:04:05",
			},
			Source: &domain.MaquetteSource{
				Regex: `(?P<source>\S+)\[\d+\]:`,
			},
			Severity: &domain.MaquetteSeverity{
				Keywords: map[string]string{"ERROR": "error"},
			},
		},
	}
	if err := s.PutInstance(ctx, inst); err != nil {
		t.Fatalf("PutInstance: %v", err)
	}

	got, err := s.GetInstance(ctx, "i1")
	if err != nil {
		t.Fatalf("GetInstance: %v", err)
	}
	if got.Maquette == nil {
		t.Fatal("maquette is nil after round-trip")
	}
	if got.Maquette.Timestamp == nil || got.Maquette.Timestamp.Format != "Jan 2 15:04:05" {
		t.Errorf("timestamp format = %v, want Jan 2 15:04:05", got.Maquette.Timestamp)
	}
	if got.Maquette.Source == nil || got.Maquette.Source.Regex == "" {
		t.Error("source regex lost after round-trip")
	}
	if got.Maquette.Severity == nil || got.Maquette.Severity.Keywords["ERROR"] != "error" {
		t.Error("severity keywords lost after round-trip")
	}
}

func TestSQLite_InstanceNoMaquette(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()
	s.PutDomain(ctx, &domain.Domain{ID: "d1", Name: "test", CreatedAt: time.Now().UTC()})
	s.PutEnvironment(ctx, &domain.Environment{ID: "e1", DomainID: "d1", Name: "env", CreatedAt: time.Now().UTC()})
	s.PutSession(ctx, &domain.Session{ID: "s1", EnvironmentID: "e1", Name: "sess", StartedAt: time.Now().UTC()})

	inst := &domain.Instance{
		ID: "i2", SessionID: "s1", Name: "no-maquette",
		StartedAt: time.Now().UTC(),
	}
	if err := s.PutInstance(ctx, inst); err != nil {
		t.Fatalf("PutInstance: %v", err)
	}

	got, err := s.GetInstance(ctx, "i2")
	if err != nil {
		t.Fatalf("GetInstance: %v", err)
	}
	if got.Maquette != nil {
		t.Errorf("maquette = %v, want nil", got.Maquette)
	}
}

func TestSQLite_Aliases(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()

	if err := s.SetAlias(ctx, "uuid-abc", "my-alias"); err != nil {
		t.Fatalf("SetAlias: %v", err)
	}

	id, err := s.ResolveAlias(ctx, "my-alias")
	if err != nil {
		t.Fatalf("ResolveAlias: %v", err)
	}
	if id != "uuid-abc" {
		t.Errorf("ResolveAlias = %q, want uuid-abc", id)
	}
}
