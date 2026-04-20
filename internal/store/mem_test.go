package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dpopsuev/chronolog/internal/domain"
	"github.com/dpopsuev/chronolog/internal/port"
)

func TestMemStore_CascadeCRUD(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	s := NewMemStore()

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

	domains, err := s.ListDomains(ctx)
	if err != nil {
		t.Fatalf("ListDomains: %v", err)
	}
	if len(domains) != 1 {
		t.Fatalf("ListDomains len = %d, want 1", len(domains))
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
		t.Fatalf("ListEnvironments len = %d, want 1", len(envs))
	}

	sess := &domain.Session{ID: "s1", EnvironmentID: "e1", Name: "dec-20", StartedAt: time.Now().UTC()}
	if err := s.PutSession(ctx, sess); err != nil {
		t.Fatalf("PutSession: %v", err)
	}

	sessions, err := s.ListSessions(ctx, "e1")
	if err != nil {
		t.Fatalf("ListSessions: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("ListSessions len = %d, want 1", len(sessions))
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
		t.Fatalf("ListInstances len = %d, want 1", len(instances))
	}
}

func TestMemStore_EventCRUD(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	s := NewMemStore()

	now := time.Now().UTC()
	e1 := &domain.Event{ID: "ev1", InstanceID: "i1", Timestamp: now, TimeConfidence: domain.ConfidenceRFC3339, Message: "offset 3ns", RawLine: "2025-12-20T14:59:20Z offset 3ns", CreatedAt: now}
	e2 := &domain.Event{ID: "ev2", InstanceID: "i1", Timestamp: now.Add(time.Second), TimeConfidence: domain.ConfidenceRFC3339, Message: "FREERUN published", RawLine: "2025-12-20T14:59:21Z FREERUN published", CreatedAt: now}

	if err := s.PutEvent(ctx, e1); err != nil {
		t.Fatalf("PutEvent e1: %v", err)
	}
	if err := s.PutEvent(ctx, e2); err != nil {
		t.Fatalf("PutEvent e2: %v", err)
	}

	got, err := s.GetEvent(ctx, "ev1")
	if err != nil {
		t.Fatalf("GetEvent: %v", err)
	}
	if got.Message != "offset 3ns" {
		t.Errorf("message = %q, want %q", got.Message, "offset 3ns")
	}

	events, err := s.ListEvents(ctx, "i1", port.EventFilter{})
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("ListEvents len = %d, want 2", len(events))
	}
	if events[0].ID != "ev1" {
		t.Errorf("first event = %q, want ev1 (sorted by time)", events[0].ID)
	}

	events, err = s.ListEvents(ctx, "i1", port.EventFilter{Limit: 1})
	if err != nil {
		t.Fatalf("ListEvents limit: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("ListEvents limit len = %d, want 1", len(events))
	}

	if err := s.DeleteEvent(ctx, "ev1"); err != nil {
		t.Fatalf("DeleteEvent: %v", err)
	}
	_, err = s.GetEvent(ctx, "ev1")
	if !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("GetEvent after delete: got %v, want ErrNotFound", err)
	}
}

func TestMemStore_SearchEvents(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	s := NewMemStore()

	now := time.Now().UTC()
	s.PutEvent(ctx, &domain.Event{ID: "ev1", InstanceID: "i1", Message: "offset 3ns", RawLine: "offset 3ns", CreatedAt: now})
	s.PutEvent(ctx, &domain.Event{ID: "ev2", InstanceID: "i1", Message: "FREERUN published", RawLine: "FREERUN published", CreatedAt: now})
	s.PutEvent(ctx, &domain.Event{ID: "ev3", InstanceID: "i1", Message: "offset 5ns", RawLine: "offset 5ns", CreatedAt: now})

	results, err := s.SearchEvents(ctx, "FREERUN", 10)
	if err != nil {
		t.Fatalf("SearchEvents: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("SearchEvents len = %d, want 1", len(results))
	}
	if results[0].ID != "ev2" {
		t.Errorf("SearchEvents result = %q, want ev2", results[0].ID)
	}

	results, err = s.SearchEvents(ctx, "offset", 10)
	if err != nil {
		t.Fatalf("SearchEvents offset: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("SearchEvents offset len = %d, want 2", len(results))
	}
}

func TestMemStore_Edges(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	s := NewMemStore()

	e := domain.Edge{FromID: "ev1", Relation: domain.RelContains, ToID: "ev2"}
	if err := s.AddEdge(ctx, e); err != nil {
		t.Fatalf("AddEdge: %v", err)
	}

	if err := s.AddEdge(ctx, e); err != nil {
		t.Fatalf("AddEdge duplicate: %v", err)
	}

	out, err := s.Neighbors(ctx, "ev1", domain.RelContains, port.Outgoing)
	if err != nil {
		t.Fatalf("Neighbors outgoing: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("Neighbors outgoing len = %d, want 1", len(out))
	}

	in, err := s.Neighbors(ctx, "ev2", domain.RelContains, port.Incoming)
	if err != nil {
		t.Fatalf("Neighbors incoming: %v", err)
	}
	if len(in) != 1 {
		t.Fatalf("Neighbors incoming len = %d, want 1", len(in))
	}

	if err := s.RemoveEdge(ctx, e); err != nil {
		t.Fatalf("RemoveEdge: %v", err)
	}

	out, err = s.Neighbors(ctx, "ev1", "", port.Both)
	if err != nil {
		t.Fatalf("Neighbors after remove: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("Neighbors after remove len = %d, want 0", len(out))
	}
}

func TestMemStore_Aliases(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	s := NewMemStore()

	if err := s.SetAlias(ctx, "uuid-123", "my-domain"); err != nil {
		t.Fatalf("SetAlias: %v", err)
	}

	id, err := s.ResolveAlias(ctx, "my-domain")
	if err != nil {
		t.Fatalf("ResolveAlias: %v", err)
	}
	if id != "uuid-123" {
		t.Errorf("ResolveAlias = %q, want uuid-123", id)
	}

	_, err = s.ResolveAlias(ctx, "nonexistent")
	if !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("ResolveAlias nonexistent: got %v, want ErrNotFound", err)
	}
}

func TestMemStore_SchemaVersion(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	s := NewMemStore()

	v, err := s.SchemaVersion(ctx)
	if err != nil {
		t.Fatalf("SchemaVersion: %v", err)
	}
	if v != 1 {
		t.Errorf("SchemaVersion = %d, want 1", v)
	}

	if err := s.SetSchemaVersion(ctx, 2); err != nil {
		t.Fatalf("SetSchemaVersion: %v", err)
	}

	v, err = s.SchemaVersion(ctx)
	if err != nil {
		t.Fatalf("SchemaVersion after set: %v", err)
	}
	if v != 2 {
		t.Errorf("SchemaVersion after set = %d, want 2", v)
	}
}
