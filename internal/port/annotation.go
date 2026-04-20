package port

import (
	"context"

	"github.com/dpopsuev/chronolog/internal/domain"
)

// AnnotationStore manages bookmarks and highlights.
type AnnotationStore interface {
	PutBookmark(ctx context.Context, b *domain.Bookmark) error
	ListBookmarks(ctx context.Context, eventID string) ([]*domain.Bookmark, error)

	PutHighlight(ctx context.Context, h *domain.Highlight) error
	ListHighlights(ctx context.Context, eventID string) ([]*domain.Highlight, error)
}
