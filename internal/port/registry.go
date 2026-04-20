package port

import (
	"context"

	"github.com/dpopsuev/chronolog/internal/domain"
)

// RegistryStore manages services and codebases.
type RegistryStore interface {
	PutService(ctx context.Context, s *domain.Service) error
	GetService(ctx context.Context, id string) (*domain.Service, error)
	ListServices(ctx context.Context) ([]*domain.Service, error)

	PutCodebase(ctx context.Context, c *domain.Codebase) error
	GetCodebase(ctx context.Context, id string) (*domain.Codebase, error)
	ListCodebases(ctx context.Context) ([]*domain.Codebase, error)
}
