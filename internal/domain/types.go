package domain

import (
	"errors"
	"time"
)

// Sentinel errors for store lookups.
var (
	ErrNotFound      = errors.New("not found")
	ErrUnknownAction = errors.New("unknown action")
)

// Event is the atomic unit of the Chronolog timeline — one log line, one event.
type Event struct {
	ID             string            `json:"id"`
	InstanceID     string            `json:"instance_id"`
	Timestamp      time.Time         `json:"timestamp"`
	TimeConfidence string            `json:"time_confidence"`
	Message        string            `json:"message"`
	Source         string            `json:"source"`
	SourceHash     string            `json:"source_hash"`
	LineNumber     int               `json:"line_number"`
	RawLine        string            `json:"raw_line"`
	Labels         map[string]string `json:"labels,omitempty"`
	CreatedAt      time.Time         `json:"created_at"`
}

// Edge is a directed relationship between two entities in the graph.
type Edge struct {
	FromID   string `json:"from_id"`
	Relation string `json:"relation"`
	ToID     string `json:"to_id"`
}

// Well-known edge relations.
const (
	RelContains   = "contains"
	RelPrecedes   = "precedes"
	RelTracesTo   = "traces_to"
	RelProducedBy = "produced_by"
	RelGroupedIn  = "grouped_in"
)

// Time confidence values.
const (
	ConfidenceRFC3339 = "rfc3339"
	ConfidenceUnknown = "unknown"
)

// CodeLocation references a source code line that produced a log entry.
type CodeLocation struct {
	File       string `json:"file"`
	Line       int    `json:"line"`
	Function   string `json:"function,omitempty"`
	Repository string `json:"repository,omitempty"`
}

// Service is a running process that produces log entries (e.g., ptp4l, phc2sys).
type Service struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// Codebase is a source code repository that builds one or more services.
type Codebase struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	RepoURL  string `json:"repo_url,omitempty"`
	RootPath string `json:"root_path,omitempty"`
}

// Bucket is a reusable investigation profile with filters and defaults.
type Bucket struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Query       string `json:"query,omitempty"`
}
