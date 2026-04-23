package domain

import (
	"errors"
	"time"
)

// Sentinel errors.
var (
	ErrNotFound              = errors.New("not found")
	ErrUnknownAction         = errors.New("unknown action")
	ErrDuplicateAlias        = errors.New("duplicate alias")
	ErrInvalidInput          = errors.New("invalid input")
	ErrSchemaVersionMismatch = errors.New("schema version mismatch")
	ErrSourceNotFound        = errors.New("source not found")
	ErrInstanceRequired      = errors.New("instance_id is required")
	ErrGitNotConfigured      = errors.New("git runner not configured")
	ErrNoCodeRef             = errors.New("no file:line reference in event message")
	ErrNoCodebases           = errors.New("no codebases registered")
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
	Collector      string            `json:"collector,omitempty"`
	FileHash       string            `json:"file_hash,omitempty"`
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

// BlameResult holds git blame output for a single line.
type BlameResult struct {
	Author     string    `json:"author"`
	CommitHash string    `json:"commit_hash"`
	Date       time.Time `json:"date"`
	Subject    string    `json:"subject"`
}

// GitCommit represents a commit from git log output.
type GitCommit struct {
	Hash    string    `json:"hash"`
	Author  string    `json:"author"`
	Date    time.Time `json:"date"`
	Subject string    `json:"subject"`
	Files   []string  `json:"files,omitempty"`
}

// Template represents a collapsed group of log lines sharing the same pattern.
type Template struct {
	Pattern   string    `json:"pattern"`
	Count     int       `json:"count"`
	FirstSeen time.Time `json:"first_seen"`
	LastSeen  time.Time `json:"last_seen"`
	Variables []string  `json:"variables,omitempty"`
}
