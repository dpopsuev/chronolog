package mcp

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	batterymcp "github.com/dpopsuev/battery/mcp"
	"github.com/dpopsuev/battery/server"
	"github.com/dpopsuev/battery/tool"
	"github.com/dpopsuev/chronolog/internal/domain"
	"github.com/dpopsuev/chronolog/internal/parser"
	"github.com/dpopsuev/chronolog/internal/port"
	"github.com/google/uuid"
)

var chronologSchema = json.RawMessage(`{
	"type": "object",
	"properties": {
		"action": {"type": "string", "enum": ["create_domain", "list_domains", "create_environment", "list_environments", "create_session", "list_sessions", "create_instance", "list_instances", "create_phase", "list_phases", "create_bucket", "list_buckets", "get_bucket", "delete_bucket", "set_immutable", "verify_integrity"], "description": "Cascade lifecycle action"},
		"name": {"type": "string", "description": "Name for the new entity"},
		"domain_id": {"type": "string", "description": "Parent domain UUID (for environment)"},
		"environment_id": {"type": "string", "description": "Parent environment UUID (for session)"},
		"session_id": {"type": "string", "description": "Parent session UUID (for instance)"},
		"instance_id": {"type": "string", "description": "Parent instance UUID (for phase)"},
		"bucket_id": {"type": "string", "description": "Bucket UUID"},
		"description": {"type": "string"},
		"alias": {"type": "string", "description": "Mutable human-friendly alias"},
		"label": {"type": "string", "description": "Phase label"},
		"query": {"type": "string", "description": "Bucket saved query"}
	},
	"required": ["action"]
}`)

var intakeSchema = json.RawMessage(`{
	"type": "object",
	"properties": {
		"action": {"type": "string", "enum": ["add_source", "list_sources", "remove_source", "test_maquette", "quick_intake"], "description": "Intake action"},
		"instance_id": {"type": "string", "description": "UUID of the target instance"},
		"source": {"type": "string", "description": "Source label (e.g. filename or service name)"},
		"lines": {"type": "array", "items": {"type": "string"}, "description": "Log lines to ingest. Caller must read the file and split into lines — intake does not read files."},
		"file_path": {"type": "string", "description": "Path to log file on disk. Alternative to lines[] — Chronolog reads and splits the file."},
		"command": {"type": "string", "description": "Shell command whose stdout is ingested line by line. Alternative to lines[] and file_path — Chronolog runs the command via sh -c."},
		"collector": {"type": "string", "description": "Who/what collected this source (optional provenance)"},
		"file_hash": {"type": "string", "description": "SHA256 hash of the original file (optional provenance)"}
	},
	"required": ["action"]
}`)

var graphSchema = json.RawMessage(`{
	"type": "object",
	"properties": {
		"action": {"type": "string", "enum": ["add_edge", "remove_edge", "merge", "collapse", "purge", "add_bookmark", "list_bookmarks", "add_highlight", "list_highlights", "register_service", "list_services", "register_codebase", "list_codebases", "label_event", "unlabel_event", "list_labels", "auto_trace", "blame", "change_window"], "description": "Graph action"},
		"instance_id": {"type": "string", "description": "Instance UUID (for merge/collapse/purge)"},
		"from_id": {"type": "string", "description": "Source node UUID (for edges)"},
		"relation": {"type": "string", "enum": ["contains", "precedes", "traces_to", "produced_by", "grouped_in"], "description": "Edge relation type"},
		"to_id": {"type": "string", "description": "Target node UUID (for edges)"},
		"event_id": {"type": "string", "description": "Event UUID (for bookmarks/highlights/labels/blame)"},
		"label": {"type": "string"},
		"note": {"type": "string"},
		"substring": {"type": "string", "description": "Text to highlight in event"},
		"name": {"type": "string", "description": "Service or codebase name"},
		"description": {"type": "string"},
		"repo_url": {"type": "string"},
		"root_path": {"type": "string"},
		"key": {"type": "string", "description": "Label key (for label_event/unlabel_event)"},
		"value": {"type": "string", "description": "Label value (for label_event)"},
		"codebase_id": {"type": "string", "description": "Codebase UUID (for auto_trace/change_window)"},
		"after": {"type": "string", "description": "Start time for change_window (RFC3339)"},
		"before": {"type": "string", "description": "End time for change_window (RFC3339)"}
	},
	"required": ["action"]
}`)

var querySchema = json.RawMessage(`{
	"type": "object",
	"properties": {
		"action": {"type": "string", "enum": ["timeline", "search", "around", "correlations", "trace_to_code", "trace_from_code", "search_by_label", "search_by_bookmark", "suspects", "time_of_defect", "recurrence", "summarize"], "description": "Query action"},
		"instance_id": {"type": "string", "description": "Instance UUID (for timeline/scoped queries)"},
		"session_id": {"type": "string", "description": "Session UUID (for scoped queries)"},
		"environment_id": {"type": "string", "description": "Environment UUID (for recurrence)"},
		"event_id": {"type": "string", "description": "Event UUID (for around/correlations/trace)"},
		"query": {"type": "string", "description": "FTS5 search query (for search)"},
		"key": {"type": "string", "description": "Label key (for search_by_label/suspects)"},
		"value": {"type": "string", "description": "Label value (for search_by_label/suspects)"},
		"pattern": {"type": "string", "description": "Log pattern to match (for time_of_defect/recurrence)"},
		"label": {"type": "string", "description": "Bookmark label filter (for search_by_bookmark)"},
		"limit": {"type": "integer", "description": "Max results (default 100)"},
		"window": {"type": "integer", "description": "Correlation window in seconds (default 5)"},
		"cursor": {"type": "string", "description": "Opaque pagination cursor returned by previous list query"},
		"response_format": {"type": "string", "enum": ["detailed", "concise"], "description": "Output verbosity. detailed returns full objects; concise returns compact summaries"}
	},
	"required": ["action"]
}`)

var diffSchema = json.RawMessage(`{
	"type": "object",
	"properties": {
		"action": {"type": "string", "enum": ["instance_diff", "session_diff", "environment_diff", "hot_cold_map", "regression_check", "set_baseline"], "description": "Diff action"},
		"instance_a": {"type": "string", "description": "First instance UUID"},
		"instance_b": {"type": "string", "description": "Second instance UUID"},
		"session_id": {"type": "string", "description": "Session UUID (for session_diff/regression_check)"},
		"baseline_session_id": {"type": "string", "description": "Baseline session UUID (for regression_check)"},
		"environment_a": {"type": "string", "description": "First environment UUID"},
		"environment_b": {"type": "string", "description": "Second environment UUID"},
		"limit": {"type": "integer", "description": "Max events per instance (default 50)"},
		"threshold": {"type": "integer", "description": "Max new patterns before fail (default 0)"}
	},
	"required": ["action"]
}`)

var projectionSchema = json.RawMessage(`{
	"type": "object",
	"properties": {
		"action": {"type": "string", "enum": ["scalar", "vector", "heatmap", "cube", "export"], "description": "Projection action"},
		"instance_id": {"type": "string", "description": "Instance UUID (for scalar/export)"},
		"session_id": {"type": "string", "description": "Session UUID (for vector/heatmap/cube/export)"},
		"filter": {"type": "string", "description": "Case-insensitive substring filter on event messages"}
	},
	"required": ["action"]
}`)

// Slog attribute key constants.
const (
	logKeySource     = "source"
	logKeyLine       = "line"
	logKeyError      = "error"
	logKeyAction     = "action"
	logKeyTool       = "tool"
	logKeyInstanceID = "instance_id"
	logKeyEventID    = "event_id"
	logKeyCount      = "count"
	logKeySessionID  = "session_id"
	logKeyRelation   = "relation"
	logKeyLabelKey   = "label_key"
	logKeyCodebaseID = "codebase_id"
)

const (
	queryDefaultLimit = 100
	queryMaxLimit     = 500

	responseFormatDetailed = "detailed"
	responseFormatConcise  = "concise"
)

const instructions = "Chronolog consolidates multiple log sources into a single clean " +
	"traversable timeline with code traceability. Many logs in, one clean signal out. " +
	"Six-level cascade hierarchy: Domain → Environment → Session → Instance → Phase → Event. " +
	"Domain is the tree root (broadest scope). " +
	"Environment = system under test. Session = one test run. " +
	"Instance = one test's logs. Phase = time block (Before/During/After Test). Event = one log line. " +
	"Git semantics: intake = working directory (staging, no processing), " +
	"graph merge = git commit (confirmed edges). " +
	"Workflow: chronolog create_domain → create_environment → create_session → create_instance, " +
	"then intake add_source to stage log lines, graph merge to create edges, " +
	"then query timeline/search/around/correlations to read events. " +
	"List-style query actions support cursor pagination and response_format (detailed|concise). " +
	"All IDs are UUIDs. Named aliases are mutable. " +
	"One log line = one event. Unstructured lines stored with time_confidence: unknown, not dropped. " +
	"Idempotent intake: re-adding same source produces no duplicates. " +
	"FTS5 full-text search available via query search action. " +
	"Diff compares instances/sessions/environments. Projection provides scalar/vector/heatmap/cube analytics. " +
	"Forensic investigation: graph label_event/unlabel_event/list_labels to tag events, " +
	"query search_by_label/search_by_bookmark to find tagged evidence, " +
	"query suspects to rank sources by co-occurrence with labeled events, " +
	"query time_of_defect to pinpoint when healthy became unhealthy, " +
	"query recurrence to check if a pattern appeared in previous sessions, " +
	"graph auto_trace/blame/change_window for git code traceability, " +
	"diff regression_check to compare test runs against a healthy baseline. " +
	"Case tool for investigation lifecycle: open_case, add_symptom, set_root_cause, " +
	"append_transcript (replayable), close_case. " +
	"Label conventions: category=symptom, category=suspect, category=smoking_gun, category=red_herring. " +
	"Intake supports lines[] (caller-provided), file_path (read from disk), command (sh -c stdout), and test_maquette (dry-run parser validation)."

// NewServer creates the Chronolog MCP server with all 7 tools registered.
func NewServer(s port.Store, version string) *batterymcp.Server {
	bsrv := batterymcp.NewServer("chronolog", version).
		WithInstructions(instructions)

	h := &handler{
		store:     s,
		git:       execGitRunner{},
		caseViews: make(map[string]*caseView),
	}

	bsrv.Tool(server.ToolMeta{
		Name: "chronolog",
		Description: "Cascade lifecycle management. Create and list the six-level hierarchy. " +
			"Actions: create_domain (name, description, alias), list_domains, " +
			"create_environment (name, domain_id, alias), list_environments (domain_id), " +
			"create_session (name, environment_id, alias), list_sessions (environment_id), " +
			"create_instance (name, session_id, alias), list_instances (session_id). " +
			"Always create top-down: domain first, then environment, session, instance.",
		Keywords:    []string{"domain", "environment", "session", "instance", "cascade", "create", "list"},
		Categories:  []string{"lifecycle"},
		InputSchema: chronologSchema,
	}, h.handleChronolog)

	bsrv.Tool(server.ToolMeta{
		Name: "intake",
		Description: "Log source staging — the receiving dock. Add log lines to an instance for later processing. " +
			"Actions: add_source (instance_id, source, lines[]|file_path|command — stages lines, parses RFC3339 timestamps, " +
			"stores as events with source attribution; unrecognized timestamps get time_confidence: unknown), " +
			"list_sources (instance_id — shows source files and event counts), " +
			"remove_source (instance_id, source — removes all events from a source and retracts edges). " +
			"Idempotent: re-adding the same source+line produces no duplicates.",
		Keywords:    []string{"source", "log", "add", "stage", "ingest", "lines", "remove"},
		Categories:  []string{"intake"},
		InputSchema: intakeSchema,
	}, h.handleIntake)

	bsrv.Tool(server.ToolMeta{
		Name: "graph",
		Description: "Graph edge, annotation, and relationship management. " +
			"Actions: add_edge (from_id, relation, to_id), remove_edge (from_id, relation, to_id), " +
			"merge (instance_id — create contains+precedes edges for all events), " +
			"collapse (instance_id — templatize events into patterns), " +
			"purge (instance_id — delete events with no edges), " +
			"add_bookmark (event_id, label, note), list_bookmarks (event_id), " +
			"add_highlight (event_id, substring, label, note), list_highlights (event_id), " +
			"register_service (name, description), list_services, " +
			"register_codebase (name, repo_url, root_path), list_codebases. " +
			"Relations: contains, precedes, traces_to, produced_by, grouped_in.",
		Keywords:    []string{"edge", "merge", "annotate", "bookmark", "highlight", "relation", "collapse", "purge"},
		Categories:  []string{"graph"},
		InputSchema: graphSchema,
	}, h.handleGraph)

	bsrv.Tool(server.ToolMeta{
		Name: "query",
		Description: "Timeline query, full-text search, and event analysis. " +
			"Actions: timeline (instance_id, limit — events sorted by timestamp), " +
			"search (query, limit — FTS5 full-text search), " +
			"around (event_id, limit — context events around a target), " +
			"correlations (event_id, window — events from other sources within time window), " +
			"trace_to_code (event_id — outgoing traces_to edges), " +
			"trace_from_code (event_id — incoming traces_to edges). " +
			"Optional cursor pagination via cursor/next_cursor and verbosity control via response_format (detailed|concise).",
		Keywords:    []string{"timeline", "search", "trace", "query", "events", "fts", "around", "correlations"},
		Categories:  []string{"query"},
		InputSchema: querySchema,
	}, h.handleQuery)

	bsrv.Tool(server.ToolMeta{
		Name: "diff",
		Description: "Hot-cold comparison between instances, sessions, or environments. " +
			"Actions: instance_diff (instance_a, instance_b — compare event templates), " +
			"session_diff (session_id — compare consecutive instance pairs), " +
			"environment_diff (environment_a, environment_b — compare all events), " +
			"hot_cold_map (alias for instance_diff).",
		Keywords:    []string{"diff", "compare", "hot", "cold", "regression"},
		Categories:  []string{"analysis"},
		InputSchema: diffSchema,
	}, h.handleDiff)

	bsrv.Tool(server.ToolMeta{
		Name: "projection",
		Description: "Tensor query and data export. Project cascade levels as axes with configurable metrics. " +
			"Actions: scalar (instance_id, filter — event count), " +
			"vector (session_id, filter — count per instance), " +
			"heatmap (session_id — instances x sources matrix), " +
			"cube (session_id — instances x sources x hours 3D array), " +
			"export (instance_id or session_id — wrapped output with metadata).",
		Keywords:    []string{"scalar", "vector", "heatmap", "cube", "export", "tensor"},
		Categories:  []string{"projection"},
		InputSchema: projectionSchema,
	}, h.handleProjection)

	bsrv.Tool(server.ToolMeta{
		Name: "case",
		Description: "Investigation lifecycle — open, track, and close forensic cases. " +
			"Actions: open_case (title), close_case (case_id), list_cases, get_case (case_id — full case with symptoms/root_cause/transcript), " +
			"add_symptom (case_id, description, event_id), list_symptoms (case_id), " +
			"set_root_cause (case_id, description, event_id — link to smoking gun), get_root_cause (case_id), " +
			"append_transcript (case_id, content), get_transcript (case_id).",
		Keywords:    []string{"case", "investigation", "symptom", "root_cause", "transcript", "rca"},
		Categories:  []string{"investigation"},
		InputSchema: caseSchema,
	}, h.handleCase)

	return bsrv
}

type handler struct {
	store      port.Store
	git        GitRunner
	caseViewMu sync.Mutex
	caseViews  map[string]*caseView
}

func (h *handler) resolveID(ctx context.Context, id string) string {
	if id == "" {
		return ""
	}
	resolved, err := h.store.ResolveAlias(ctx, id)
	if err == nil {
		return resolved
	}
	return id
}

func (h *handler) autoAlias(ctx context.Context, id, name, alias string) {
	a := alias
	if a == "" {
		a = name
	}
	if a != "" {
		_ = h.store.SetAlias(ctx, id, a)
	}
}

// --- chronolog tool ---

type chronologInput struct {
	Action        string `json:"action"`
	Name          string `json:"name,omitempty"`
	DomainID      string `json:"domain_id,omitempty"`
	EnvironmentID string `json:"environment_id,omitempty"`
	SessionID     string `json:"session_id,omitempty"`
	InstanceID    string `json:"instance_id,omitempty"`
	BucketID      string `json:"bucket_id,omitempty"`
	Description   string `json:"description,omitempty"`
	Alias         string `json:"alias,omitempty"`
	Label         string `json:"label,omitempty"`
	Query         string `json:"query,omitempty"`
}

func (h *handler) handleChronolog(ctx context.Context, raw json.RawMessage) (tool.Result, error) { //nolint:gocyclo,funlen // action dispatch switch
	var in chronologInput
	if err := json.Unmarshal(raw, &in); err != nil {
		return tool.ErrorResult(err), nil
	}
	slog.DebugContext(ctx, "handler entry", slog.String(logKeyTool, "chronolog"), slog.String(logKeyAction, in.Action))
	in.DomainID = h.resolveID(ctx, in.DomainID)
	in.EnvironmentID = h.resolveID(ctx, in.EnvironmentID)
	in.SessionID = h.resolveID(ctx, in.SessionID)
	in.InstanceID = h.resolveID(ctx, in.InstanceID)
	in.BucketID = h.resolveID(ctx, in.BucketID)
	switch in.Action {
	case "create_domain":
		d := &domain.Domain{ID: uuid.NewString(), Name: in.Name, Alias: in.Alias, Description: in.Description, CreatedAt: time.Now().UTC()}
		if err := h.store.PutDomain(ctx, d); err != nil {
			return tool.ErrorResult(err), nil
		}
		h.autoAlias(ctx, d.ID, d.Name, d.Alias)
		return jsonResult(d)
	case "list_domains":
		ds, err := h.store.ListDomains(ctx)
		if err != nil {
			return tool.ErrorResult(err), nil
		}
		return jsonResult(ds)
	case "create_environment":
		e := &domain.Environment{ID: uuid.NewString(), DomainID: in.DomainID, Name: in.Name, Alias: in.Alias, CreatedAt: time.Now().UTC()}
		if err := h.store.PutEnvironment(ctx, e); err != nil {
			return tool.ErrorResult(err), nil
		}
		h.autoAlias(ctx, e.ID, e.Name, e.Alias)
		return jsonResult(e)
	case "list_environments":
		es, err := h.store.ListEnvironments(ctx, in.DomainID)
		if err != nil {
			return tool.ErrorResult(err), nil
		}
		return jsonResult(es)
	case "create_session":
		s := &domain.Session{ID: uuid.NewString(), EnvironmentID: in.EnvironmentID, Name: in.Name, Alias: in.Alias, StartedAt: time.Now().UTC()}
		if err := h.store.PutSession(ctx, s); err != nil {
			return tool.ErrorResult(err), nil
		}
		h.autoAlias(ctx, s.ID, s.Name, s.Alias)
		return jsonResult(s)
	case "list_sessions":
		ss, err := h.store.ListSessions(ctx, in.EnvironmentID)
		if err != nil {
			return tool.ErrorResult(err), nil
		}
		return jsonResult(ss)
	case "create_instance":
		i := &domain.Instance{ID: uuid.NewString(), SessionID: in.SessionID, Name: in.Name, Alias: in.Alias, StartedAt: time.Now().UTC()}
		if err := h.store.PutInstance(ctx, i); err != nil {
			return tool.ErrorResult(err), nil
		}
		h.autoAlias(ctx, i.ID, i.Name, i.Alias)
		return jsonResult(i)
	case "list_instances":
		is, err := h.store.ListInstances(ctx, in.SessionID)
		if err != nil {
			return tool.ErrorResult(err), nil
		}
		return jsonResult(is)
	case "create_phase":
		if in.InstanceID == "" {
			return tool.ErrorResult(fmt.Errorf("instance_id: %w", domain.ErrInstanceRequired)), nil
		}
		p := &domain.Phase{ID: uuid.NewString(), InstanceID: in.InstanceID, Name: in.Name, Label: in.Label, StartedAt: time.Now().UTC()}
		if err := h.store.PutPhase(ctx, p); err != nil {
			return tool.ErrorResult(err), nil
		}
		return jsonResult(p)
	case "list_phases":
		if in.InstanceID == "" {
			return tool.ErrorResult(fmt.Errorf("instance_id: %w", domain.ErrInstanceRequired)), nil
		}
		ps, err := h.store.ListPhases(ctx, in.InstanceID)
		if err != nil {
			return tool.ErrorResult(err), nil
		}
		return jsonResult(ps)
	case "create_bucket":
		b := &domain.Bucket{ID: uuid.NewString(), Name: in.Name, Description: in.Description, Query: in.Query}
		if err := h.store.PutBucket(ctx, b); err != nil {
			return tool.ErrorResult(err), nil
		}
		return jsonResult(b)
	case "list_buckets":
		bs, err := h.store.ListBuckets(ctx)
		if err != nil {
			return tool.ErrorResult(err), nil
		}
		return jsonResult(bs)
	case "get_bucket":
		b, err := h.store.GetBucket(ctx, in.BucketID)
		if err != nil {
			return tool.ErrorResult(err), nil
		}
		return jsonResult(b)
	case "delete_bucket":
		if err := h.store.DeleteBucket(ctx, in.BucketID); err != nil {
			return tool.ErrorResult(err), nil
		}
		return jsonResult(map[string]any{"deleted": true})
	case "set_immutable":
		if in.InstanceID == "" {
			return tool.ErrorResult(fmt.Errorf("instance_id: %w", domain.ErrInstanceRequired)), nil
		}
		inst, err := h.store.GetInstance(ctx, in.InstanceID)
		if err != nil {
			return tool.ErrorResult(err), nil
		}
		inst.Immutable = true
		if err := h.store.PutInstance(ctx, inst); err != nil {
			return tool.ErrorResult(err), nil
		}
		return jsonResult(inst)
	case "verify_integrity":
		if in.InstanceID == "" {
			return tool.ErrorResult(fmt.Errorf("instance_id: %w", domain.ErrInstanceRequired)), nil
		}
		events, err := h.store.ListEvents(ctx, in.InstanceID, port.EventFilter{Limit: 100000})
		if err != nil {
			return tool.ErrorResult(err), nil
		}
		var broken []map[string]any
		for _, e := range events {
			expected := hashLine(e.Source, e.RawLine)
			if e.SourceHash != expected {
				broken = append(broken, map[string]any{"event_id": e.ID, "expected": expected, "actual": e.SourceHash})
			}
		}
		return jsonResult(map[string]any{"valid": len(broken) == 0, "events_checked": len(events), "broken": broken})
	default:
		return tool.ErrorResult(fmt.Errorf("chronolog action %q: %w", in.Action, domain.ErrUnknownAction)), nil
	}
}

// --- intake tool ---

type intakeSource struct {
	Source   string   `json:"source"`
	Lines    []string `json:"lines,omitempty"`
	FilePath string   `json:"file_path,omitempty"`
	Command  string   `json:"command,omitempty"`
}

type intakeInput struct {
	Action     string           `json:"action"`
	InstanceID string           `json:"instance_id,omitempty"`
	Source     string           `json:"source,omitempty"`
	Lines      []string         `json:"lines,omitempty"`
	FilePath   string           `json:"file_path,omitempty"`
	Command    string           `json:"command,omitempty"`
	Collector  string           `json:"collector,omitempty"`
	FileHash   string           `json:"file_hash,omitempty"`
	Maquette   *domain.Maquette `json:"maquette,omitempty"`
	Name       string           `json:"name,omitempty"`
	Sources    []intakeSource   `json:"sources,omitempty"`
}

func (h *handler) handleIntake(ctx context.Context, raw json.RawMessage) (tool.Result, error) {
	var in intakeInput
	if err := json.Unmarshal(raw, &in); err != nil {
		return tool.ErrorResult(err), nil
	}
	slog.DebugContext(ctx, "handler entry", slog.String(logKeyTool, "intake"), slog.String(logKeyAction, in.Action))
	in.InstanceID = h.resolveID(ctx, in.InstanceID)
	switch in.Action {
	case "add_source":
		return h.addSource(ctx, in)
	case "list_sources":
		events, err := h.store.ListEvents(ctx, in.InstanceID, port.EventFilter{Limit: 100000})
		if err != nil {
			return tool.ErrorResult(err), nil
		}
		type sourceInfo struct {
			Source    string `json:"source"`
			Count     int    `json:"count"`
			Collector string `json:"collector,omitempty"`
			FileHash  string `json:"file_hash,omitempty"`
		}
		infoMap := make(map[string]*sourceInfo)
		for _, e := range events {
			si, ok := infoMap[e.Source]
			if !ok {
				si = &sourceInfo{Source: e.Source, Collector: e.Collector, FileHash: e.FileHash}
				infoMap[e.Source] = si
			}
			si.Count++
		}
		result := make([]*sourceInfo, 0, len(infoMap))
		for _, si := range infoMap {
			result = append(result, si)
		}
		return jsonResult(result)
	case "remove_source":
		return h.removeSource(ctx, in)
	case "test_maquette":
		return h.testMaquette(ctx, in)
	case "quick_intake":
		return h.quickIntake(ctx, in)
	default:
		return tool.ErrorResult(fmt.Errorf("intake action %q: %w", in.Action, domain.ErrUnknownAction)), nil
	}
}

func (h *handler) addSource(ctx context.Context, in intakeInput) (tool.Result, error) {
	if in.InstanceID == "" {
		return tool.ErrorResult(fmt.Errorf("instance_id: %w", domain.ErrInstanceRequired)), nil
	}
	if in.Source == "" {
		return tool.ErrorResult(fmt.Errorf("source: %w", domain.ErrInvalidInput)), nil
	}
	if len(in.Lines) == 0 && in.FilePath != "" {
		lines, err := readFileLines(in.FilePath)
		if err != nil {
			return tool.ErrorResult(err), nil
		}
		in.Lines = lines
	}
	if len(in.Lines) == 0 && in.Command != "" {
		lines, err := readCommandLines(ctx, in.Command)
		if err != nil {
			return tool.ErrorResult(err), nil
		}
		in.Lines = lines
	}
	if len(in.Lines) == 0 {
		return tool.ErrorResult(fmt.Errorf("lines, file_path, or command required: %w", domain.ErrInvalidInput)), nil
	}

	maq := in.Maquette
	if maq != nil {
		if inst, err := h.store.GetInstance(ctx, in.InstanceID); err == nil {
			inst.Maquette = maq
			_ = h.store.PutInstance(ctx, inst)
		}
	} else {
		if inst, err := h.store.GetInstance(ctx, in.InstanceID); err == nil && inst.Maquette != nil {
			maq = inst.Maquette
		}
	}

	compiled, err := parser.Compile(maq)
	if err != nil {
		return tool.ErrorResult(fmt.Errorf("maquette: %w", err)), nil
	}

	var added int
	for i, line := range in.Lines {
		hash := hashLine(in.Source, line)
		lineNum := i + 1

		result := parser.ParseWithMaquette(line, compiled)

		event := &domain.Event{
			ID:             uuid.NewString(),
			InstanceID:     in.InstanceID,
			Timestamp:      result.Timestamp,
			TimeConfidence: result.TimeConfidence,
			Message:        line,
			Source:         in.Source,
			SourceHash:     hash,
			LineNumber:     lineNum,
			RawLine:        line,
			Labels:         result.Labels,
			Collector:      in.Collector,
			FileHash:       in.FileHash,
			CreatedAt:      time.Now().UTC(),
		}

		if err := h.store.PutEvent(ctx, event); err != nil {
			slog.WarnContext(ctx, "failed to store event",
				slog.String(logKeySource, in.Source),
				slog.Int(logKeyLine, lineNum),
				slog.String(logKeyError, err.Error()),
			)
			continue
		}
		added++
	}
	return jsonResult(map[string]any{"added": added, "source": in.Source, "total_lines": len(in.Lines)})
}

func (h *handler) removeSource(ctx context.Context, in intakeInput) (tool.Result, error) {
	if in.InstanceID == "" {
		return tool.ErrorResult(fmt.Errorf("instance_id: %w", domain.ErrInstanceRequired)), nil
	}
	if in.Source == "" {
		return tool.ErrorResult(fmt.Errorf("source: %w", domain.ErrInvalidInput)), nil
	}
	if inst, err := h.store.GetInstance(ctx, in.InstanceID); err == nil && inst.Immutable {
		return tool.ErrorResult(fmt.Errorf("instance is immutable — evidence cannot be modified: %w", domain.ErrInvalidInput)), nil
	}
	events, err := h.store.ListEvents(ctx, in.InstanceID, port.EventFilter{Limit: 10000})
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	var removed int
	for _, e := range events {
		if e.Source != in.Source {
			continue
		}
		edges, err := h.store.Neighbors(ctx, e.ID, "", port.Both)
		if err != nil {
			return tool.ErrorResult(err), nil
		}
		for _, edge := range edges {
			if rErr := h.store.RemoveEdge(ctx, edge); rErr != nil {
				return tool.ErrorResult(rErr), nil
			}
		}
		if dErr := h.store.DeleteEvent(ctx, e.ID); dErr != nil {
			return tool.ErrorResult(dErr), nil
		}
		removed++
	}
	if removed == 0 {
		return tool.ErrorResult(fmt.Errorf("source %q in instance %q: %w", in.Source, in.InstanceID, domain.ErrSourceNotFound)), nil
	}
	return jsonResult(map[string]any{"removed": removed, "source": in.Source})
}

func readFileLines(path string) ([]string, error) {
	if strings.Contains(path, "..") {
		return nil, fmt.Errorf("file_path contains '..': %w", domain.ErrInvalidInput)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("file_path: %w", err)
	}
	data, err := os.ReadFile(abs)
	if err != nil {
		return nil, fmt.Errorf("file_path: %w", err)
	}
	raw := strings.TrimRight(string(data), "\n")
	if raw == "" {
		return nil, fmt.Errorf("file is empty: %w", domain.ErrInvalidInput)
	}
	return strings.Split(raw, "\n"), nil
}

func (h *handler) testMaquette(_ context.Context, in intakeInput) (tool.Result, error) {
	if in.Maquette == nil {
		return tool.ErrorResult(fmt.Errorf("maquette: %w", domain.ErrInvalidInput)), nil
	}
	lines := in.Lines
	if len(lines) == 0 && in.FilePath != "" {
		var err error
		lines, err = readFileLines(in.FilePath)
		if err != nil {
			return tool.ErrorResult(err), nil
		}
	}
	if len(lines) == 0 {
		return tool.ErrorResult(fmt.Errorf("lines or file_path required: %w", domain.ErrInvalidInput)), nil
	}
	compiled, err := parser.Compile(in.Maquette)
	if err != nil {
		return tool.ErrorResult(fmt.Errorf("maquette: %w", err)), nil
	}
	type lineResult struct {
		Line           string            `json:"line"`
		Timestamp      string            `json:"timestamp"`
		TimeConfidence string            `json:"time_confidence"`
		Labels         map[string]string `json:"labels,omitempty"`
	}
	results := make([]lineResult, 0, len(lines))
	for _, line := range lines {
		pr := parser.ParseWithMaquette(line, compiled)
		ts := ""
		if !pr.Timestamp.IsZero() {
			ts = pr.Timestamp.Format(time.RFC3339Nano)
		}
		results = append(results, lineResult{
			Line: line, Timestamp: ts, TimeConfidence: pr.TimeConfidence, Labels: pr.Labels,
		})
	}
	return jsonResult(results)
}

func readCommandLines(ctx context.Context, command string) ([]string, error) {
	cmd := exec.CommandContext(ctx, "sh", "-c", command)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("command %q: %w", command, err)
	}
	raw := strings.TrimRight(string(out), "\n")
	if raw == "" {
		return nil, fmt.Errorf("command produced no output: %w", domain.ErrInvalidInput)
	}
	slog.DebugContext(ctx, "command piped", slog.String(logKeySource, command))
	return strings.Split(raw, "\n"), nil
}

func (h *handler) quickIntake(ctx context.Context, in intakeInput) (tool.Result, error) { //nolint:funlen,gocyclo // orchestration method
	if in.Name == "" {
		return tool.ErrorResult(fmt.Errorf("name: %w", domain.ErrInvalidInput)), nil
	}
	if len(in.Sources) == 0 {
		return tool.ErrorResult(fmt.Errorf("sources: %w", domain.ErrInvalidInput)), nil
	}

	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "unknown"
	}

	domID := h.resolveID(ctx, hostname)
	if _, err := h.store.GetDomain(ctx, domID); err != nil {
		d := &domain.Domain{ID: uuid.NewString(), Name: hostname, CreatedAt: time.Now().UTC()}
		if pErr := h.store.PutDomain(ctx, d); pErr != nil {
			return tool.ErrorResult(pErr), nil
		}
		h.autoAlias(ctx, d.ID, d.Name, "")
		domID = d.ID
	}

	envName := "default"
	envID := h.resolveID(ctx, envName)
	if _, err := h.store.GetEnvironment(ctx, envID); err != nil {
		e := &domain.Environment{ID: uuid.NewString(), DomainID: domID, Name: envName, CreatedAt: time.Now().UTC()}
		if pErr := h.store.PutEnvironment(ctx, e); pErr != nil {
			return tool.ErrorResult(pErr), nil
		}
		h.autoAlias(ctx, e.ID, e.Name, "")
		envID = e.ID
	}

	sess := &domain.Session{ID: uuid.NewString(), EnvironmentID: envID, Name: time.Now().UTC().Format("2006-01-02T15:04:05"), StartedAt: time.Now().UTC()}
	if err := h.store.PutSession(ctx, sess); err != nil {
		return tool.ErrorResult(err), nil
	}

	inst := &domain.Instance{ID: uuid.NewString(), SessionID: sess.ID, Name: in.Name, StartedAt: time.Now().UTC()}
	if err := h.store.PutInstance(ctx, inst); err != nil {
		return tool.ErrorResult(err), nil
	}
	h.autoAlias(ctx, inst.ID, inst.Name, "")

	var totalEvents int
	for _, src := range in.Sources {
		sub := intakeInput{
			InstanceID: inst.ID,
			Source:     src.Source,
			Lines:      src.Lines,
			FilePath:   src.FilePath,
			Command:    src.Command,
		}
		if len(sub.Lines) == 0 && sub.FilePath != "" {
			lines, err := readFileLines(sub.FilePath)
			if err != nil {
				return tool.ErrorResult(err), nil
			}
			sub.Lines = lines
		}
		if len(sub.Lines) == 0 && sub.Command != "" {
			lines, err := readCommandLines(ctx, sub.Command)
			if err != nil {
				return tool.ErrorResult(err), nil
			}
			sub.Lines = lines
		}
		if len(sub.Lines) == 0 {
			continue
		}
		res, err := h.addSource(ctx, sub)
		if err != nil {
			return tool.ErrorResult(err), nil
		}
		if res.IsError {
			return res, nil
		}
		totalEvents += len(sub.Lines)
	}

	if _, err := h.mergeInstance(ctx, &graphInput{InstanceID: inst.ID}); err != nil {
		return tool.ErrorResult(err), nil
	}

	slog.DebugContext(ctx, "quick_intake completed", slog.String(logKeyInstanceID, inst.ID), slog.Int(logKeyCount, totalEvents))
	return jsonResult(map[string]any{
		"instance_id": inst.ID,
		"session_id":  sess.ID,
		"name":        in.Name,
		"sources":     len(in.Sources),
		"events":      totalEvents,
	})
}

func hashLine(source, line string) string {
	h := sha256.Sum256([]byte(source + "\x00" + line))
	return hex.EncodeToString(h[:16])
}

// --- graph tool ---

type graphInput struct {
	Action      string `json:"action"`
	FromID      string `json:"from_id,omitempty"`
	Relation    string `json:"relation,omitempty"`
	ToID        string `json:"to_id,omitempty"`
	EventID     string `json:"event_id,omitempty"`
	Label       string `json:"label,omitempty"`
	Note        string `json:"note,omitempty"`
	Substring   string `json:"substring,omitempty"`
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
	RepoURL     string `json:"repo_url,omitempty"`
	RootPath    string `json:"root_path,omitempty"`
	InstanceID  string `json:"instance_id,omitempty"`
	Key         string `json:"key,omitempty"`
	Value       string `json:"value,omitempty"`
	CodebaseID  string `json:"codebase_id,omitempty"`
	After       string `json:"after,omitempty"`
	Before      string `json:"before,omitempty"`
}

func (h *handler) handleGraph(ctx context.Context, raw json.RawMessage) (tool.Result, error) { //nolint:gocyclo // action dispatch switch
	var in graphInput
	if err := json.Unmarshal(raw, &in); err != nil {
		return tool.ErrorResult(err), nil
	}
	slog.DebugContext(ctx, "handler entry", slog.String(logKeyTool, "graph"), slog.String(logKeyAction, in.Action))
	in.InstanceID = h.resolveID(ctx, in.InstanceID)
	in.EventID = h.resolveID(ctx, in.EventID)
	in.CodebaseID = h.resolveID(ctx, in.CodebaseID)
	switch in.Action {
	case "add_edge":
		e := domain.Edge{FromID: in.FromID, Relation: in.Relation, ToID: in.ToID}
		if err := h.store.AddEdge(ctx, e); err != nil {
			return tool.ErrorResult(err), nil
		}
		return jsonResult(e)
	case "remove_edge":
		e := domain.Edge{FromID: in.FromID, Relation: in.Relation, ToID: in.ToID}
		if err := h.store.RemoveEdge(ctx, e); err != nil {
			return tool.ErrorResult(err), nil
		}
		return tool.TextResult("edge removed"), nil
	case "add_bookmark":
		return h.addBookmark(ctx, &in)
	case "list_bookmarks":
		return h.listBookmarks(ctx, &in)
	case "add_highlight":
		return h.addHighlight(ctx, &in)
	case "list_highlights":
		return h.listHighlights(ctx, &in)
	case "register_service":
		return h.registerService(ctx, &in)
	case "list_services":
		return h.listServices(ctx)
	case "register_codebase":
		return h.registerCodebase(ctx, &in)
	case "list_codebases":
		return h.listCodebases(ctx)
	case "merge":
		return h.mergeInstance(ctx, &in)
	case "collapse":
		return h.collapseInstance(ctx, &in)
	case "purge":
		return h.purgeInstance(ctx, &in)
	case "label_event":
		return h.labelEvent(ctx, &in)
	case "unlabel_event":
		return h.unlabelEvent(ctx, &in)
	case "list_labels":
		return h.listLabels(ctx, &in)
	case "auto_trace":
		return h.autoTrace(ctx, &in)
	case "blame":
		return h.blameEvent(ctx, &in)
	case "change_window":
		return h.changeWindow(ctx, &in)
	default:
		return tool.ErrorResult(fmt.Errorf("graph action %q: %w", in.Action, domain.ErrUnknownAction)), nil
	}
}

func (h *handler) addBookmark(ctx context.Context, in *graphInput) (tool.Result, error) {
	if in.EventID == "" {
		return tool.ErrorResult(fmt.Errorf("event_id: %w", domain.ErrInvalidInput)), nil
	}
	b := &domain.Bookmark{ID: uuid.NewString(), EventID: in.EventID, Label: in.Label, Note: in.Note, CreatedAt: time.Now().UTC()}
	if err := h.store.PutBookmark(ctx, b); err != nil {
		return tool.ErrorResult(err), nil
	}
	return jsonResult(b)
}

func (h *handler) listBookmarks(ctx context.Context, in *graphInput) (tool.Result, error) {
	if in.EventID == "" {
		return tool.ErrorResult(fmt.Errorf("event_id: %w", domain.ErrInvalidInput)), nil
	}
	bs, err := h.store.ListBookmarks(ctx, in.EventID)
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	return jsonResult(bs)
}

func (h *handler) addHighlight(ctx context.Context, in *graphInput) (tool.Result, error) {
	if in.EventID == "" || in.Substring == "" {
		return tool.ErrorResult(fmt.Errorf("event_id and substring: %w", domain.ErrInvalidInput)), nil
	}
	hl := &domain.Highlight{ID: uuid.NewString(), EventID: in.EventID, Substring: in.Substring, Label: in.Label, Note: in.Note, CreatedAt: time.Now().UTC()}
	if err := h.store.PutHighlight(ctx, hl); err != nil {
		return tool.ErrorResult(err), nil
	}
	return jsonResult(hl)
}

func (h *handler) listHighlights(ctx context.Context, in *graphInput) (tool.Result, error) {
	if in.EventID == "" {
		return tool.ErrorResult(fmt.Errorf("event_id: %w", domain.ErrInvalidInput)), nil
	}
	hs, err := h.store.ListHighlights(ctx, in.EventID)
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	return jsonResult(hs)
}

func (h *handler) labelEvent(ctx context.Context, in *graphInput) (tool.Result, error) {
	if in.EventID == "" || in.Key == "" {
		return tool.ErrorResult(fmt.Errorf("event_id and key: %w", domain.ErrInvalidInput)), nil
	}
	e, err := h.store.GetEvent(ctx, in.EventID)
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	if e.Labels == nil {
		e.Labels = make(map[string]string)
	}
	e.Labels[in.Key] = in.Value
	if err := h.store.UpdateEventLabels(ctx, e.ID, e.Labels); err != nil {
		return tool.ErrorResult(err), nil
	}
	slog.DebugContext(ctx, "label set", slog.String(logKeyEventID, e.ID), slog.String(logKeyLabelKey, in.Key))
	return jsonResult(e)
}

func (h *handler) unlabelEvent(ctx context.Context, in *graphInput) (tool.Result, error) {
	if in.EventID == "" || in.Key == "" {
		return tool.ErrorResult(fmt.Errorf("event_id and key: %w", domain.ErrInvalidInput)), nil
	}
	e, err := h.store.GetEvent(ctx, in.EventID)
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	delete(e.Labels, in.Key)
	if err := h.store.UpdateEventLabels(ctx, e.ID, e.Labels); err != nil {
		return tool.ErrorResult(err), nil
	}
	slog.DebugContext(ctx, "label removed", slog.String(logKeyEventID, e.ID), slog.String(logKeyLabelKey, in.Key))
	return jsonResult(e)
}

func (h *handler) listLabels(ctx context.Context, in *graphInput) (tool.Result, error) {
	if in.EventID == "" {
		return tool.ErrorResult(fmt.Errorf("event_id: %w", domain.ErrInvalidInput)), nil
	}
	e, err := h.store.GetEvent(ctx, in.EventID)
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	return jsonResult(e.Labels)
}

func (h *handler) registerService(ctx context.Context, in *graphInput) (tool.Result, error) {
	if in.Name == "" {
		return tool.ErrorResult(fmt.Errorf("name: %w", domain.ErrInvalidInput)), nil
	}
	svc := &domain.Service{ID: uuid.NewString(), Name: in.Name, Description: in.Description}
	if err := h.store.PutService(ctx, svc); err != nil {
		return tool.ErrorResult(err), nil
	}
	return jsonResult(svc)
}

func (h *handler) listServices(ctx context.Context) (tool.Result, error) {
	svcs, err := h.store.ListServices(ctx)
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	return jsonResult(svcs)
}

func (h *handler) registerCodebase(ctx context.Context, in *graphInput) (tool.Result, error) {
	if in.Name == "" {
		return tool.ErrorResult(fmt.Errorf("name: %w", domain.ErrInvalidInput)), nil
	}
	cb := &domain.Codebase{ID: uuid.NewString(), Name: in.Name, RepoURL: in.RepoURL, RootPath: in.RootPath}
	if err := h.store.PutCodebase(ctx, cb); err != nil {
		return tool.ErrorResult(err), nil
	}
	return jsonResult(cb)
}

func (h *handler) listCodebases(ctx context.Context) (tool.Result, error) {
	cbs, err := h.store.ListCodebases(ctx)
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	return jsonResult(cbs)
}

func (h *handler) mergeInstance(ctx context.Context, in *graphInput) (tool.Result, error) {
	if in.InstanceID == "" {
		return tool.ErrorResult(fmt.Errorf("instance_id: %w", domain.ErrInstanceRequired)), nil
	}
	events, err := h.store.ListEvents(ctx, in.InstanceID, port.EventFilter{Limit: 100000})
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	var edgeCount int
	for i, e := range events {
		contains := domain.Edge{FromID: in.InstanceID, Relation: domain.RelContains, ToID: e.ID}
		if err := h.store.AddEdge(ctx, contains); err != nil {
			return tool.ErrorResult(err), nil
		}
		edgeCount++
		if i > 0 {
			precedes := domain.Edge{FromID: events[i-1].ID, Relation: domain.RelPrecedes, ToID: e.ID}
			if err := h.store.AddEdge(ctx, precedes); err != nil {
				return tool.ErrorResult(err), nil
			}
			edgeCount++
		}
	}
	slog.DebugContext(ctx, "merge completed", slog.String(logKeyInstanceID, in.InstanceID), slog.Int(logKeyCount, edgeCount))
	return jsonResult(map[string]any{"instance_id": in.InstanceID, "events": len(events), "edges_created": edgeCount})
}

func (h *handler) collapseInstance(ctx context.Context, in *graphInput) (tool.Result, error) {
	if in.InstanceID == "" {
		return tool.ErrorResult(fmt.Errorf("instance_id: %w", domain.ErrInstanceRequired)), nil
	}
	events, err := h.store.ListEvents(ctx, in.InstanceID, port.EventFilter{Limit: 100000})
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	templates := extractTemplates(events)
	return jsonResult(templates)
}

func (h *handler) purgeInstance(ctx context.Context, in *graphInput) (tool.Result, error) {
	if in.InstanceID == "" {
		return tool.ErrorResult(fmt.Errorf("instance_id: %w", domain.ErrInstanceRequired)), nil
	}
	if inst, err := h.store.GetInstance(ctx, in.InstanceID); err == nil && inst.Immutable {
		return tool.ErrorResult(fmt.Errorf("instance is immutable — evidence cannot be modified: %w", domain.ErrInvalidInput)), nil
	}
	events, err := h.store.ListEvents(ctx, in.InstanceID, port.EventFilter{Limit: 100000})
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	var purged int
	for _, e := range events {
		edges, nErr := h.store.Neighbors(ctx, e.ID, "", port.Both)
		if nErr != nil {
			return tool.ErrorResult(nErr), nil
		}
		if len(edges) == 0 {
			if dErr := h.store.DeleteEvent(ctx, e.ID); dErr != nil {
				return tool.ErrorResult(dErr), nil
			}
			purged++
		}
	}
	slog.DebugContext(ctx, "purge completed", slog.String(logKeyInstanceID, in.InstanceID), slog.Int(logKeyCount, purged))
	return jsonResult(map[string]any{"instance_id": in.InstanceID, "purged": purged})
}

// --- query tool ---

type queryInput struct {
	Action         string `json:"action"`
	InstanceID     string `json:"instance_id,omitempty"`
	SessionID      string `json:"session_id,omitempty"`
	EnvironmentID  string `json:"environment_id,omitempty"`
	EventID        string `json:"event_id,omitempty"`
	Query          string `json:"query,omitempty"`
	Key            string `json:"key,omitempty"`
	Value          string `json:"value,omitempty"`
	Pattern        string `json:"pattern,omitempty"`
	Label          string `json:"label,omitempty"`
	Limit          int    `json:"limit,omitempty"`
	Window         int    `json:"window,omitempty"`
	Cursor         string `json:"cursor,omitempty"`
	ResponseFormat string `json:"response_format,omitempty"`
}

func (h *handler) handleQuery(ctx context.Context, raw json.RawMessage) (tool.Result, error) { //nolint:gocyclo // action dispatch switch
	var in queryInput
	if err := json.Unmarshal(raw, &in); err != nil {
		return tool.ErrorResult(err), nil
	}
	slog.DebugContext(ctx, "handler entry", slog.String(logKeyTool, "query"), slog.String(logKeyAction, in.Action))
	in.InstanceID = h.resolveID(ctx, in.InstanceID)
	in.SessionID = h.resolveID(ctx, in.SessionID)
	in.EnvironmentID = h.resolveID(ctx, in.EnvironmentID)
	in.EventID = h.resolveID(ctx, in.EventID)
	limit := clampQueryLimit(in.Limit)
	offset, err := decodeCursor(in.Cursor)
	if err != nil {
		return tool.ErrorResult(fmt.Errorf("cursor: %w", err)), nil
	}
	format, err := normalizeResponseFormat(in.ResponseFormat)
	if err != nil {
		return tool.ErrorResult(fmt.Errorf("response_format: %w", err)), nil
	}
	switch in.Action {
	case "timeline":
		return h.queryTimeline(ctx, in, limit, offset, format)
	case "search":
		return h.querySearch(ctx, in, limit, offset, format)
	case "around":
		return h.queryAround(ctx, in, limit, offset, format)
	case "correlations":
		return h.queryCorrelations(ctx, in)
	case "trace_to_code":
		return h.traceCode(ctx, in, port.Outgoing)
	case "trace_from_code":
		return h.traceCode(ctx, in, port.Incoming)
	case "summarize":
		return h.summarize(ctx, in)
	case "search_by_label":
		return h.searchByLabel(ctx, in)
	case "search_by_bookmark":
		return h.searchByBookmark(ctx, in)
	case "suspects":
		return h.suspects(ctx, in)
	case "time_of_defect":
		return h.timeOfDefect(ctx, in)
	case "recurrence":
		return h.recurrence(ctx, in)
	default:
		return tool.ErrorResult(fmt.Errorf("query action %q: %w", in.Action, domain.ErrUnknownAction)), nil
	}
}

func (h *handler) queryTimeline(ctx context.Context, in queryInput, limit, offset int, format string) (tool.Result, error) {
	if in.InstanceID == "" {
		return tool.ErrorResult(fmt.Errorf("instance_id: %w", domain.ErrInstanceRequired)), nil
	}
	events, err := h.store.ListEvents(ctx, in.InstanceID, port.EventFilter{Limit: limit + 1, Offset: offset})
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	hasMore := len(events) > limit
	if hasMore {
		events = events[:limit]
	}
	payload := formatEvents(events, format)
	if !wantsEnvelope(in) {
		return jsonResult(payload)
	}
	return jsonResult(buildPage(payload, in.Action, format, limit, offset, hasMore))
}

func (h *handler) querySearch(ctx context.Context, in queryInput, limit, offset int, format string) (tool.Result, error) {
	searchFetch := (offset + limit + 1) * 2
	events, err := h.store.SearchEvents(ctx, in.Query, searchFetch)
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	if in.InstanceID != "" || in.SessionID != "" {
		var filtered []*domain.Event
		scope := make(map[string]bool)
		if in.InstanceID != "" {
			scope[in.InstanceID] = true
		}
		if in.SessionID != "" {
			insts, _ := h.store.ListInstances(ctx, in.SessionID)
			for _, inst := range insts {
				scope[inst.ID] = true
			}
		}
		for _, e := range events {
			if scope[e.InstanceID] {
				filtered = append(filtered, e)
			}
		}
		events = filtered
	}
	start := offset
	if start > len(events) {
		start = len(events)
	}
	end := start + limit
	if end > len(events) {
		end = len(events)
	}
	page := events[start:end]
	hasMore := end < len(events)
	payload := formatEvents(page, format)
	if !wantsEnvelope(in) {
		return jsonResult(payload)
	}
	result := buildPage(payload, in.Action, format, limit, offset, hasMore)
	result["total_matches"] = len(events)
	return jsonResult(result)
}

func (h *handler) queryAround(ctx context.Context, in queryInput, limit, offset int, format string) (tool.Result, error) {
	if in.EventID == "" {
		return tool.ErrorResult(fmt.Errorf("event_id: %w", domain.ErrInvalidInput)), nil
	}
	target, err := h.store.GetEvent(ctx, in.EventID)
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	events, err := h.store.ListEvents(ctx, target.InstanceID, port.EventFilter{Limit: 10000})
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	radius := limit / 2
	if radius <= 0 {
		radius = 5
	}
	idx := -1
	for i, e := range events {
		if e.ID == in.EventID {
			idx = i
			break
		}
	}
	if idx < 0 {
		return jsonResult([]any{})
	}
	start := idx - radius
	if start < 0 {
		start = 0
	}
	end := idx + radius + 1
	if end > len(events) {
		end = len(events)
	}
	windowEvents := events[start:end]
	if !wantsEnvelope(in) {
		return jsonResult(formatEvents(windowEvents, format))
	}
	pageStart := offset
	if pageStart > len(windowEvents) {
		pageStart = len(windowEvents)
	}
	pageEnd := pageStart + limit
	if pageEnd > len(windowEvents) {
		pageEnd = len(windowEvents)
	}
	page := windowEvents[pageStart:pageEnd]
	hasMore := pageEnd < len(windowEvents)
	payload := formatEvents(page, format)
	result := buildPage(payload, in.Action, format, limit, offset, hasMore)
	result["window_total"] = len(windowEvents)
	return jsonResult(result)
}

func (h *handler) queryCorrelations(ctx context.Context, in queryInput) (tool.Result, error) {
	if in.EventID == "" {
		return tool.ErrorResult(fmt.Errorf("event_id: %w", domain.ErrInvalidInput)), nil
	}
	target, err := h.store.GetEvent(ctx, in.EventID)
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	window := time.Duration(in.Window) * time.Second
	if window <= 0 {
		window = 5 * time.Second
	}
	after := target.Timestamp.Add(-window)
	before := target.Timestamp.Add(window)
	events, err := h.store.ListEvents(ctx, target.InstanceID, port.EventFilter{After: &after, Before: &before, Limit: 10000})
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	grouped := make(map[string][]*domain.Event)
	for _, e := range events {
		if e.Source == target.Source {
			continue
		}
		grouped[e.Source] = append(grouped[e.Source], e)
	}
	return jsonResult(grouped)
}

func (h *handler) traceCode(ctx context.Context, in queryInput, dir port.Direction) (tool.Result, error) {
	if in.EventID == "" {
		return tool.ErrorResult(fmt.Errorf("event_id: %w", domain.ErrInvalidInput)), nil
	}
	edges, err := h.store.Neighbors(ctx, in.EventID, domain.RelTracesTo, dir)
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	return jsonResult(edges)
}

// --- diff tool ---

type diffInput struct {
	Action            string `json:"action"`
	InstanceA         string `json:"instance_a,omitempty"`
	InstanceB         string `json:"instance_b,omitempty"`
	SessionID         string `json:"session_id,omitempty"`
	BaselineSessionID string `json:"baseline_session_id,omitempty"`
	EnvironmentA      string `json:"environment_a,omitempty"`
	EnvironmentB      string `json:"environment_b,omitempty"`
	Limit             int    `json:"limit,omitempty"`
	Threshold         int    `json:"threshold,omitempty"`
}

func (h *handler) handleDiff(ctx context.Context, raw json.RawMessage) (tool.Result, error) {
	var in diffInput
	if err := json.Unmarshal(raw, &in); err != nil {
		return tool.ErrorResult(err), nil
	}
	slog.DebugContext(ctx, "handler entry", slog.String(logKeyTool, "diff"), slog.String(logKeyAction, in.Action))
	in.SessionID = h.resolveID(ctx, in.SessionID)
	in.BaselineSessionID = h.resolveID(ctx, in.BaselineSessionID)
	in.InstanceA = h.resolveID(ctx, in.InstanceA)
	in.InstanceB = h.resolveID(ctx, in.InstanceB)
	in.EnvironmentA = h.resolveID(ctx, in.EnvironmentA)
	in.EnvironmentB = h.resolveID(ctx, in.EnvironmentB)
	switch in.Action {
	case "instance_diff", "hot_cold_map":
		return h.instanceDiff(ctx, in)
	case "session_diff":
		return h.sessionDiff(ctx, in)
	case "environment_diff":
		return h.environmentDiff(ctx, in)
	case "regression_check":
		return h.regressionCheck(ctx, in)
	case "set_baseline":
		if in.SessionID == "" {
			return tool.ErrorResult(fmt.Errorf("session_id: %w", domain.ErrInvalidInput)), nil
		}
		if err := h.store.SetAlias(ctx, in.SessionID, "baseline"); err != nil {
			return tool.ErrorResult(err), nil
		}
		return jsonResult(map[string]any{"baseline_session_id": in.SessionID})
	default:
		return tool.ErrorResult(fmt.Errorf("diff action %q: %w", in.Action, domain.ErrUnknownAction)), nil
	}
}

// --- projection tool ---

type projectionInput struct {
	Action     string `json:"action"`
	InstanceID string `json:"instance_id,omitempty"`
	SessionID  string `json:"session_id,omitempty"`
	Filter     string `json:"filter,omitempty"`
	AxisX      string `json:"axis_x,omitempty"`
	AxisY      string `json:"axis_y,omitempty"`
	AxisZ      string `json:"axis_z,omitempty"`
	Format     string `json:"format,omitempty"`
	Limit      int    `json:"limit,omitempty"`
}

func (h *handler) handleProjection(ctx context.Context, raw json.RawMessage) (tool.Result, error) {
	var in projectionInput
	if err := json.Unmarshal(raw, &in); err != nil {
		return tool.ErrorResult(err), nil
	}
	slog.DebugContext(ctx, "handler entry", slog.String(logKeyTool, "projection"), slog.String(logKeyAction, in.Action))
	in.InstanceID = h.resolveID(ctx, in.InstanceID)
	in.SessionID = h.resolveID(ctx, in.SessionID)
	switch in.Action {
	case "scalar":
		return h.projScalar(ctx, in)
	case "vector":
		return h.projVector(ctx, in)
	case "heatmap":
		return h.projHeatmap(ctx, in)
	case "cube":
		return h.projCube(ctx, in)
	case "export":
		return h.projExport(ctx, in)
	default:
		return tool.ErrorResult(fmt.Errorf("projection action %q: %w", in.Action, domain.ErrUnknownAction)), nil
	}
}

// --- diff implementation ---

func (h *handler) instanceDiff(ctx context.Context, in diffInput) (tool.Result, error) {
	if in.InstanceA == "" || in.InstanceB == "" {
		return tool.ErrorResult(fmt.Errorf("instance_a and instance_b: %w", domain.ErrInvalidInput)), nil
	}
	eventsA, err := h.store.ListEvents(ctx, in.InstanceA, port.EventFilter{Limit: 100000})
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	eventsB, err := h.store.ListEvents(ctx, in.InstanceB, port.EventFilter{Limit: 100000})
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	return jsonResult(computeDiff(eventsA, eventsB))
}

func (h *handler) sessionDiff(ctx context.Context, in diffInput) (tool.Result, error) {
	if in.SessionID == "" {
		return tool.ErrorResult(fmt.Errorf("session_id: %w", domain.ErrInvalidInput)), nil
	}
	instances, err := h.store.ListInstances(ctx, in.SessionID)
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	sort.Slice(instances, func(i, j int) bool {
		return instances[i].StartedAt.Before(instances[j].StartedAt)
	})
	limit := in.Limit
	if limit <= 0 {
		limit = 50
	}
	var diffs []map[string]any
	for i := 0; i+1 < len(instances); i++ {
		evA, err := h.store.ListEvents(ctx, instances[i].ID, port.EventFilter{Limit: limit})
		if err != nil {
			return tool.ErrorResult(err), nil
		}
		evB, err := h.store.ListEvents(ctx, instances[i+1].ID, port.EventFilter{Limit: limit})
		if err != nil {
			return tool.ErrorResult(err), nil
		}
		diff := computeDiff(evA, evB)
		diff["instance_a"] = instances[i].ID
		diff["instance_b"] = instances[i+1].ID
		diffs = append(diffs, diff)
	}
	return jsonResult(diffs)
}

func (h *handler) environmentDiff(ctx context.Context, in diffInput) (tool.Result, error) {
	if in.EnvironmentA == "" || in.EnvironmentB == "" {
		return tool.ErrorResult(fmt.Errorf("environment_a and environment_b: %w", domain.ErrInvalidInput)), nil
	}
	eventsA, err := h.collectEnvEvents(ctx, in.EnvironmentA)
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	eventsB, err := h.collectEnvEvents(ctx, in.EnvironmentB)
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	return jsonResult(computeDiff(eventsA, eventsB))
}

func (h *handler) collectEnvEvents(ctx context.Context, envID string) ([]*domain.Event, error) {
	sessions, err := h.store.ListSessions(ctx, envID)
	if err != nil {
		return nil, err
	}
	var allEvents []*domain.Event
	for _, sess := range sessions {
		instances, iErr := h.store.ListInstances(ctx, sess.ID)
		if iErr != nil {
			return nil, iErr
		}
		for _, inst := range instances {
			events, eErr := h.store.ListEvents(ctx, inst.ID, port.EventFilter{Limit: 100000})
			if eErr != nil {
				return nil, eErr
			}
			allEvents = append(allEvents, events...)
		}
	}
	return allEvents, nil
}

// --- projection implementation ---

func (h *handler) projScalar(ctx context.Context, in projectionInput) (tool.Result, error) {
	if in.InstanceID == "" {
		return tool.ErrorResult(fmt.Errorf("instance_id: %w", domain.ErrInstanceRequired)), nil
	}
	events, err := h.store.ListEvents(ctx, in.InstanceID, port.EventFilter{Limit: 100000})
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	count := filterCount(events, in.Filter)
	return jsonResult(map[string]any{"value": count})
}

func (h *handler) projVector(ctx context.Context, in projectionInput) (tool.Result, error) {
	if in.SessionID == "" {
		return tool.ErrorResult(fmt.Errorf("session_id: %w", domain.ErrInvalidInput)), nil
	}
	instances, err := h.store.ListInstances(ctx, in.SessionID)
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	labels := make([]string, 0, len(instances))
	values := make([]int, 0, len(instances))
	for _, inst := range instances {
		events, eErr := h.store.ListEvents(ctx, inst.ID, port.EventFilter{Limit: 100000})
		if eErr != nil {
			return tool.ErrorResult(eErr), nil
		}
		labels = append(labels, inst.Name)
		values = append(values, filterCount(events, in.Filter))
	}
	return jsonResult(map[string]any{"labels": labels, "values": values})
}

func (h *handler) projHeatmap(ctx context.Context, in projectionInput) (tool.Result, error) {
	if in.SessionID == "" {
		return tool.ErrorResult(fmt.Errorf("session_id: %w", domain.ErrInvalidInput)), nil
	}
	instances, err := h.store.ListInstances(ctx, in.SessionID)
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	instEvents := make(map[string][]*domain.Event, len(instances))
	allSources := make(map[string]int)
	xLabels := make([]string, 0, len(instances))
	for _, inst := range instances {
		events, eErr := h.store.ListEvents(ctx, inst.ID, port.EventFilter{Limit: 100000})
		if eErr != nil {
			return tool.ErrorResult(eErr), nil
		}
		instEvents[inst.ID] = events
		xLabels = append(xLabels, inst.Name)
		for _, e := range events {
			allSources[e.Source] = 0
		}
	}
	yLabels := sortedKeys(allSources)
	cells := make([][]int, len(instances))
	for i, inst := range instances {
		row := make([]int, len(yLabels))
		srcIdx := make(map[string]int, len(yLabels))
		for j, s := range yLabels {
			srcIdx[s] = j
		}
		for _, e := range instEvents[inst.ID] {
			row[srcIdx[e.Source]]++
		}
		cells[i] = row
	}
	return jsonResult(map[string]any{"x_labels": xLabels, "y_labels": yLabels, "cells": cells})
}

func (h *handler) projCube(ctx context.Context, in projectionInput) (tool.Result, error) {
	if in.SessionID == "" {
		return tool.ErrorResult(fmt.Errorf("session_id: %w", domain.ErrInvalidInput)), nil
	}
	instances, err := h.store.ListInstances(ctx, in.SessionID)
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	allSources := make(map[string]int)
	allHours := make(map[int]bool)
	type eventGroup struct {
		events []*domain.Event
		name   string
	}
	groups := make([]eventGroup, 0, len(instances))
	for _, inst := range instances {
		events, eErr := h.store.ListEvents(ctx, inst.ID, port.EventFilter{Limit: 100000})
		if eErr != nil {
			return tool.ErrorResult(eErr), nil
		}
		groups = append(groups, eventGroup{events: events, name: inst.Name})
		for _, e := range events {
			allSources[e.Source] = 0
			allHours[e.Timestamp.Hour()] = true
		}
	}
	sourceList := sortedKeys(allSources)
	hourList := make([]int, 0, len(allHours))
	for h := range allHours {
		hourList = append(hourList, h)
	}
	sort.Ints(hourList)
	axes := []map[string]any{
		{"name": "instance", "labels": instNames(instances)},
		{"name": "source", "labels": sourceList},
		{"name": "hour", "labels": hourList},
	}
	srcIdx := make(map[string]int, len(sourceList))
	for i, s := range sourceList {
		srcIdx[s] = i
	}
	hourIdx := make(map[int]int, len(hourList))
	for i, h := range hourList {
		hourIdx[h] = i
	}
	cells := make([][][]int, len(groups))
	for gi, g := range groups {
		cells[gi] = make([][]int, len(sourceList))
		for si := range sourceList {
			cells[gi][si] = make([]int, len(hourList))
		}
		for _, e := range g.events {
			cells[gi][srcIdx[e.Source]][hourIdx[e.Timestamp.Hour()]]++
		}
	}
	return jsonResult(map[string]any{"axes": axes, "cells": cells})
}

func (h *handler) projExport(ctx context.Context, in projectionInput) (tool.Result, error) {
	var inner any
	switch {
	case in.InstanceID != "":
		events, err := h.store.ListEvents(ctx, in.InstanceID, port.EventFilter{Limit: 100000})
		if err != nil {
			return tool.ErrorResult(err), nil
		}
		inner = map[string]any{"value": filterCount(events, in.Filter)}
	case in.SessionID != "":
		instances, err := h.store.ListInstances(ctx, in.SessionID)
		if err != nil {
			return tool.ErrorResult(err), nil
		}
		labels := make([]string, 0, len(instances))
		values := make([]int, 0, len(instances))
		for _, inst := range instances {
			events, eErr := h.store.ListEvents(ctx, inst.ID, port.EventFilter{Limit: 100000})
			if eErr != nil {
				return tool.ErrorResult(eErr), nil
			}
			labels = append(labels, inst.Name)
			values = append(values, filterCount(events, in.Filter))
		}
		inner = map[string]any{"labels": labels, "values": values}
	default:
		return tool.ErrorResult(fmt.Errorf("instance_id or session_id: %w", domain.ErrInvalidInput)), nil
	}
	return jsonResult(map[string]any{
		"format":  "chronolog/export",
		"version": 1,
		"filter":  in.Filter,
		"data":    inner,
	})
}

func instNames(instances []*domain.Instance) []string {
	names := make([]string, len(instances))
	for i, inst := range instances {
		names[i] = inst.Name
	}
	return names
}

func filterCount(events []*domain.Event, filter string) int {
	if filter == "" {
		return len(events)
	}
	var count int
	lf := strings.ToLower(filter)
	for _, e := range events {
		if strings.Contains(strings.ToLower(e.Message), lf) {
			count++
		}
	}
	return count
}

// --- helpers ---

type eventSummary struct {
	ID             string            `json:"id"`
	InstanceID     string            `json:"instance_id"`
	Timestamp      time.Time         `json:"timestamp"`
	TimeConfidence string            `json:"time_confidence"`
	Message        string            `json:"message"`
	Source         string            `json:"source"`
	Labels         map[string]string `json:"labels,omitempty"`
}

func clampQueryLimit(limit int) int {
	if limit <= 0 {
		return queryDefaultLimit
	}
	if limit > queryMaxLimit {
		return queryMaxLimit
	}
	return limit
}

func normalizeResponseFormat(format string) (string, error) {
	if format == "" {
		return responseFormatDetailed, nil
	}
	switch strings.ToLower(format) {
	case responseFormatDetailed:
		return responseFormatDetailed, nil
	case responseFormatConcise:
		return responseFormatConcise, nil
	default:
		return "", fmt.Errorf("unsupported value %q: %w", format, domain.ErrInvalidInput)
	}
}

func wantsEnvelope(in queryInput) bool {
	return in.Cursor != "" || in.ResponseFormat != ""
}

func formatEvents(events []*domain.Event, format string) any {
	if format != responseFormatConcise {
		return events
	}
	out := make([]eventSummary, 0, len(events))
	for _, e := range events {
		out = append(out, eventSummary{
			ID:             e.ID,
			InstanceID:     e.InstanceID,
			Timestamp:      e.Timestamp,
			TimeConfidence: e.TimeConfidence,
			Message:        e.Message,
			Source:         e.Source,
			Labels:         e.Labels,
		})
	}
	return out
}

func buildPage(items any, action, format string, limit, offset int, hasMore bool) map[string]any {
	page := map[string]any{
		"action":          action,
		"response_format": format,
		"limit":           limit,
		"cursor":          encodeCursor(offset),
		"count":           sliceLen(items),
		"items":           items,
		"truncated":       hasMore,
	}
	if hasMore {
		next := encodeCursor(offset + sliceLen(items))
		page["next_cursor"] = next
		page["hint"] = "Pass next_cursor as cursor to continue pagination."
	}
	return page
}

func sliceLen(items any) int {
	switch v := items.(type) {
	case []*domain.Event:
		return len(v)
	case []eventSummary:
		return len(v)
	default:
		return 0
	}
}

func decodeCursor(cursor string) (int, error) {
	if cursor == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, domain.ErrInvalidInput
	}
	offset, err := strconv.Atoi(string(raw))
	if err != nil || offset < 0 {
		return 0, domain.ErrInvalidInput
	}
	return offset, nil
}

func encodeCursor(offset int) string {
	if offset < 0 {
		offset = 0
	}
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(offset)))
}

func jsonResult(data any) (tool.Result, error) {
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	return tool.TextResult(string(b)), nil
}
