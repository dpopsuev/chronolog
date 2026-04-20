package store

import "github.com/dpopsuev/chronolog/internal/port"

// Type aliases re-exporting port types for convenience.
type (
	Store           = port.Store
	EventStore      = port.EventStore
	CascadeStore    = port.CascadeStore
	GraphStore      = port.GraphStore
	AnnotationStore = port.AnnotationStore
	RegistryStore   = port.RegistryStore
	BucketStore     = port.BucketStore
	MetaStore       = port.MetaStore
	EventFilter     = port.EventFilter
)

// Compile-time checks.
var _ port.Store = (*MemStore)(nil)
