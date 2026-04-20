package port

import (
	"context"

	"github.com/dpopsuev/chronolog/internal/domain"
)

// CascadeStore manages the six-level hierarchy.
type CascadeStore interface {
	PutDomain(ctx context.Context, d *domain.Domain) error
	GetDomain(ctx context.Context, id string) (*domain.Domain, error)
	ListDomains(ctx context.Context) ([]*domain.Domain, error)

	PutEnvironment(ctx context.Context, e *domain.Environment) error
	GetEnvironment(ctx context.Context, id string) (*domain.Environment, error)
	ListEnvironments(ctx context.Context, domainID string) ([]*domain.Environment, error)

	PutSession(ctx context.Context, s *domain.Session) error
	GetSession(ctx context.Context, id string) (*domain.Session, error)
	ListSessions(ctx context.Context, envID string) ([]*domain.Session, error)

	PutInstance(ctx context.Context, i *domain.Instance) error
	GetInstance(ctx context.Context, id string) (*domain.Instance, error)
	ListInstances(ctx context.Context, sessionID string) ([]*domain.Instance, error)

	PutPhase(ctx context.Context, p *domain.Phase) error
	ListPhases(ctx context.Context, instanceID string) ([]*domain.Phase, error)
}
