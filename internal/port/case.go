package port

import (
	"context"

	"github.com/dpopsuev/chronolog/internal/domain"
)

// CaseStore manages investigation cases with symptoms, root causes, and transcripts.
type CaseStore interface {
	PutCase(ctx context.Context, c *domain.Case) error
	GetCase(ctx context.Context, id string) (*domain.Case, error)
	ListCases(ctx context.Context) ([]*domain.Case, error)

	PutSymptom(ctx context.Context, s *domain.Symptom) error
	ListSymptoms(ctx context.Context, caseID string) ([]*domain.Symptom, error)

	PutRootCause(ctx context.Context, rc *domain.RootCause) error
	GetRootCause(ctx context.Context, caseID string) (*domain.RootCause, error)

	PutTranscriptEntry(ctx context.Context, te *domain.TranscriptEntry) error
	ListTranscriptEntries(ctx context.Context, caseID string) ([]*domain.TranscriptEntry, error)
}
