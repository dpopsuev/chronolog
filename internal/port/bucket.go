package port

import (
	"context"

	"github.com/dpopsuev/chronolog/internal/domain"
)

// BucketStore manages reusable investigation profiles.
type BucketStore interface {
	PutBucket(ctx context.Context, b *domain.Bucket) error
	GetBucket(ctx context.Context, id string) (*domain.Bucket, error)
	ListBuckets(ctx context.Context) ([]*domain.Bucket, error)
	DeleteBucket(ctx context.Context, id string) error
}
