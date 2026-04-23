package port

// Store composes all role-specific store interfaces into a single facade.
type Store interface {
	EventStore
	CascadeStore
	GraphStore
	AnnotationStore
	RegistryStore
	BucketStore
	MetaStore
	CaseStore
	Close() error
}
