package store

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/dpopsuev/chronolog/internal/domain"
	"github.com/dpopsuev/chronolog/internal/port"
)

var _ port.Store = (*MemStore)(nil)

// MemStore is an in-memory store for testing.
type MemStore struct {
	mu            sync.RWMutex
	events        map[string]*domain.Event
	domains       map[string]*domain.Domain
	environments  map[string]*domain.Environment
	sessions      map[string]*domain.Session
	instances     map[string]*domain.Instance
	phases        map[string]*domain.Phase
	bookmarks     map[string]*domain.Bookmark
	highlights    map[string]*domain.Highlight
	services      map[string]*domain.Service
	codebases     map[string]*domain.Codebase
	buckets       map[string]*domain.Bucket
	edges         []domain.Edge
	aliases       map[string]string
	schemaVersion int
}

// NewMemStore creates a new in-memory store.
func NewMemStore() *MemStore {
	return &MemStore{
		events:        make(map[string]*domain.Event),
		domains:       make(map[string]*domain.Domain),
		environments:  make(map[string]*domain.Environment),
		sessions:      make(map[string]*domain.Session),
		instances:     make(map[string]*domain.Instance),
		phases:        make(map[string]*domain.Phase),
		bookmarks:     make(map[string]*domain.Bookmark),
		highlights:    make(map[string]*domain.Highlight),
		services:      make(map[string]*domain.Service),
		codebases:     make(map[string]*domain.Codebase),
		buckets:       make(map[string]*domain.Bucket),
		aliases:       make(map[string]string),
		schemaVersion: 1,
	}
}

func (m *MemStore) Close() error { return nil }

// --- EventStore ---

func (m *MemStore) PutEvent(_ context.Context, e *domain.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events[e.ID] = e
	return nil
}

func (m *MemStore) GetEvent(_ context.Context, id string) (*domain.Event, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	e, ok := m.events[id]
	if !ok {
		return nil, fmt.Errorf("event %q: %w", id, domain.ErrNotFound)
	}
	return e, nil
}

func (m *MemStore) ListEvents(_ context.Context, instanceID string, filter port.EventFilter) ([]*domain.Event, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*domain.Event, 0, len(m.events))
	for _, e := range m.events {
		if e.InstanceID != instanceID {
			continue
		}
		if filter.After != nil && e.Timestamp.Before(*filter.After) {
			continue
		}
		if filter.Before != nil && e.Timestamp.After(*filter.Before) {
			continue
		}
		result = append(result, e)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Timestamp.Before(result[j].Timestamp)
	})
	if filter.Offset > 0 && filter.Offset < len(result) {
		result = result[filter.Offset:]
	}
	if filter.Limit > 0 && filter.Limit < len(result) {
		result = result[:filter.Limit]
	}
	return result, nil
}

func (m *MemStore) DeleteEvent(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.events, id)
	return nil
}

func (m *MemStore) UpdateEventLabels(_ context.Context, id string, labels map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.events[id]
	if !ok {
		return domain.ErrNotFound
	}
	e.Labels = labels
	return nil
}

func (m *MemStore) SearchEvents(_ context.Context, query string, limit int) ([]*domain.Event, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*domain.Event
	q := strings.ToLower(query)
	for _, e := range m.events {
		if strings.Contains(strings.ToLower(e.Message), q) ||
			strings.Contains(strings.ToLower(e.RawLine), q) {
			result = append(result, e)
		}
	}
	if limit > 0 && limit < len(result) {
		result = result[:limit]
	}
	return result, nil
}

// --- CascadeStore ---

func (m *MemStore) PutDomain(_ context.Context, d *domain.Domain) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.domains[d.ID] = d
	return nil
}

func (m *MemStore) GetDomain(_ context.Context, id string) (*domain.Domain, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	d, ok := m.domains[id]
	if !ok {
		return nil, fmt.Errorf("domain %q: %w", id, domain.ErrNotFound)
	}
	return d, nil
}

func (m *MemStore) ListDomains(_ context.Context) ([]*domain.Domain, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*domain.Domain, 0, len(m.domains))
	for _, d := range m.domains {
		result = append(result, d)
	}
	return result, nil
}

func (m *MemStore) PutEnvironment(_ context.Context, e *domain.Environment) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.environments[e.ID] = e
	return nil
}

func (m *MemStore) GetEnvironment(_ context.Context, id string) (*domain.Environment, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	e, ok := m.environments[id]
	if !ok {
		return nil, fmt.Errorf("environment %q: %w", id, domain.ErrNotFound)
	}
	return e, nil
}

func (m *MemStore) ListEnvironments(_ context.Context, domainID string) ([]*domain.Environment, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*domain.Environment
	for _, e := range m.environments {
		if e.DomainID == domainID {
			result = append(result, e)
		}
	}
	return result, nil
}

func (m *MemStore) PutSession(_ context.Context, s *domain.Session) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sessions[s.ID] = s
	return nil
}

func (m *MemStore) GetSession(_ context.Context, id string) (*domain.Session, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sessions[id]
	if !ok {
		return nil, fmt.Errorf("session %q: %w", id, domain.ErrNotFound)
	}
	return s, nil
}

func (m *MemStore) ListSessions(_ context.Context, envID string) ([]*domain.Session, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*domain.Session
	for _, s := range m.sessions {
		if s.EnvironmentID == envID {
			result = append(result, s)
		}
	}
	return result, nil
}

func (m *MemStore) PutInstance(_ context.Context, i *domain.Instance) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.instances[i.ID] = i
	return nil
}

func (m *MemStore) GetInstance(_ context.Context, id string) (*domain.Instance, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	i, ok := m.instances[id]
	if !ok {
		return nil, fmt.Errorf("instance %q: %w", id, domain.ErrNotFound)
	}
	return i, nil
}

func (m *MemStore) ListInstances(_ context.Context, sessionID string) ([]*domain.Instance, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*domain.Instance
	for _, i := range m.instances {
		if i.SessionID == sessionID {
			result = append(result, i)
		}
	}
	return result, nil
}

func (m *MemStore) PutPhase(_ context.Context, p *domain.Phase) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.phases[p.ID] = p
	return nil
}

func (m *MemStore) ListPhases(_ context.Context, instanceID string) ([]*domain.Phase, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*domain.Phase
	for _, p := range m.phases {
		if p.InstanceID == instanceID {
			result = append(result, p)
		}
	}
	return result, nil
}

// --- GraphStore ---

func (m *MemStore) AddEdge(_ context.Context, e domain.Edge) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, existing := range m.edges {
		if existing.FromID == e.FromID && existing.Relation == e.Relation && existing.ToID == e.ToID {
			return nil
		}
	}
	m.edges = append(m.edges, e)
	return nil
}

func (m *MemStore) RemoveEdge(_ context.Context, e domain.Edge) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, existing := range m.edges {
		if existing.FromID == e.FromID && existing.Relation == e.Relation && existing.ToID == e.ToID {
			m.edges = append(m.edges[:i], m.edges[i+1:]...)
			return nil
		}
	}
	return nil
}

func (m *MemStore) Neighbors(_ context.Context, id, relation string, dir port.Direction) ([]domain.Edge, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []domain.Edge
	for _, e := range m.edges {
		if relation != "" && e.Relation != relation {
			continue
		}
		switch dir {
		case port.Outgoing:
			if e.FromID == id {
				result = append(result, e)
			}
		case port.Incoming:
			if e.ToID == id {
				result = append(result, e)
			}
		case port.Both:
			if e.FromID == id || e.ToID == id {
				result = append(result, e)
			}
		}
	}
	return result, nil
}

// --- AnnotationStore ---

func (m *MemStore) PutBookmark(_ context.Context, b *domain.Bookmark) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bookmarks[b.ID] = b
	return nil
}

func (m *MemStore) ListBookmarks(_ context.Context, eventID string) ([]*domain.Bookmark, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*domain.Bookmark
	for _, b := range m.bookmarks {
		if b.EventID == eventID {
			result = append(result, b)
		}
	}
	return result, nil
}

func (m *MemStore) PutHighlight(_ context.Context, h *domain.Highlight) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.highlights[h.ID] = h
	return nil
}

func (m *MemStore) ListHighlights(_ context.Context, eventID string) ([]*domain.Highlight, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []*domain.Highlight
	for _, h := range m.highlights {
		if h.EventID == eventID {
			result = append(result, h)
		}
	}
	return result, nil
}

// --- RegistryStore ---

func (m *MemStore) PutService(_ context.Context, s *domain.Service) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.services[s.ID] = s
	return nil
}

func (m *MemStore) GetService(_ context.Context, id string) (*domain.Service, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.services[id]
	if !ok {
		return nil, fmt.Errorf("service %q: %w", id, domain.ErrNotFound)
	}
	return s, nil
}

func (m *MemStore) ListServices(_ context.Context) ([]*domain.Service, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*domain.Service, 0, len(m.services))
	for _, s := range m.services {
		result = append(result, s)
	}
	return result, nil
}

func (m *MemStore) PutCodebase(_ context.Context, c *domain.Codebase) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.codebases[c.ID] = c
	return nil
}

func (m *MemStore) GetCodebase(_ context.Context, id string) (*domain.Codebase, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c, ok := m.codebases[id]
	if !ok {
		return nil, fmt.Errorf("codebase %q: %w", id, domain.ErrNotFound)
	}
	return c, nil
}

func (m *MemStore) ListCodebases(_ context.Context) ([]*domain.Codebase, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*domain.Codebase, 0, len(m.codebases))
	for _, c := range m.codebases {
		result = append(result, c)
	}
	return result, nil
}

// --- BucketStore ---

func (m *MemStore) PutBucket(_ context.Context, b *domain.Bucket) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.buckets[b.ID] = b
	return nil
}

func (m *MemStore) GetBucket(_ context.Context, id string) (*domain.Bucket, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.buckets[id]
	if !ok {
		return nil, fmt.Errorf("bucket %q: %w", id, domain.ErrNotFound)
	}
	return b, nil
}

func (m *MemStore) ListBuckets(_ context.Context) ([]*domain.Bucket, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*domain.Bucket, 0, len(m.buckets))
	for _, b := range m.buckets {
		result = append(result, b)
	}
	return result, nil
}

func (m *MemStore) DeleteBucket(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.buckets, id)
	return nil
}

// --- MetaStore ---

func (m *MemStore) SetAlias(_ context.Context, id, alias string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.aliases[alias] = id
	return nil
}

func (m *MemStore) ResolveAlias(_ context.Context, alias string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	id, ok := m.aliases[alias]
	if !ok {
		return "", fmt.Errorf("alias %q: %w", alias, domain.ErrNotFound)
	}
	return id, nil
}

func (m *MemStore) SchemaVersion(_ context.Context) (int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.schemaVersion, nil
}

func (m *MemStore) SetSchemaVersion(_ context.Context, version int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.schemaVersion = version
	return nil
}
