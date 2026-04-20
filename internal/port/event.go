package port

import (
	"context"
	"time"

	"github.com/dpopsuev/chronolog/internal/domain"
)

// EventFilter constrains event list queries.
type EventFilter struct {
	After  *time.Time
	Before *time.Time
	Limit  int
	Offset int
}

// EventStore manages log events within instances.
type EventStore interface {
	PutEvent(ctx context.Context, event *domain.Event) error
	GetEvent(ctx context.Context, id string) (*domain.Event, error)
	ListEvents(ctx context.Context, instanceID string, filter EventFilter) ([]*domain.Event, error)
	DeleteEvent(ctx context.Context, id string) error
	SearchEvents(ctx context.Context, query string, limit int) ([]*domain.Event, error)
}
