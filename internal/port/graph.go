package port

import (
	"context"

	"github.com/dpopsuev/chronolog/internal/domain"
)

// Direction specifies edge traversal direction.
type Direction int

const (
	Outgoing Direction = iota
	Incoming
	Both
)

// GraphStore manages edges in the graph.
type GraphStore interface {
	AddEdge(ctx context.Context, e domain.Edge) error
	RemoveEdge(ctx context.Context, e domain.Edge) error
	Neighbors(ctx context.Context, id, relation string, dir Direction) ([]domain.Edge, error)
}
