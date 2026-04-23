package domain

import "time"

// Case is an investigation container — tracks symptoms, root cause, and transcript.
type Case struct {
	ID        string     `json:"id"`
	Title     string     `json:"title"`
	Status    string     `json:"status"`
	CreatedAt time.Time  `json:"created_at"`
	ClosedAt  *time.Time `json:"closed_at,omitempty"`
}

// Symptom is an observed behavior linked to evidence events.
type Symptom struct {
	ID          string    `json:"id"`
	CaseID      string    `json:"case_id"`
	Description string    `json:"description"`
	EventID     string    `json:"event_id,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

// RootCause is the confirmed cause of an investigation.
type RootCause struct {
	ID          string    `json:"id"`
	CaseID      string    `json:"case_id"`
	Description string    `json:"description"`
	EventID     string    `json:"event_id,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

// TranscriptEntry is one step in the investigation audit trail.
type TranscriptEntry struct {
	ID        string    `json:"id"`
	CaseID    string    `json:"case_id"`
	Seq       int       `json:"seq"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}
