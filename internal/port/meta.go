package port

import "context"

// MetaStore manages aliases and schema versioning.
type MetaStore interface {
	SetAlias(ctx context.Context, id, alias string) error
	ResolveAlias(ctx context.Context, alias string) (string, error)
	SchemaVersion(ctx context.Context) (int, error)
	SetSchemaVersion(ctx context.Context, version int) error
}
