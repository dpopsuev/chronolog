package mcp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
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
		"action": {"type": "string", "enum": ["add_source", "list_sources", "remove_source"], "description": "Intake action"},
		"instance_id": {"type": "string", "description": "UUID of the target instance"},
		"source": {"type": "string", "description": "Source label (e.g. filename or service name)"},
		"lines": {"type": "array", "items": {"type": "string"}, "description": "Log lines to ingest. Caller must read the file and split into lines — intake does not read files."}
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
		"action": {"type": "string", "enum": ["timeline", "search", "around", "correlations", "trace_to_code", "trace_from_code", "search_by_label", "search_by_bookmark", "suspects", "time_of_defect", "recurrence"], "description": "Query action"},
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
		"window": {"type": "integer", "description": "Correlation window in seconds (default 5)"}
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
	"All IDs are UUIDs. Named aliases are mutable. " +
	"One log line = one event. Unstructured lines stored with time_confidence: unknown, not dropped. " +
	"Idempotent intake: re-adding same source produces no duplicates. " +
	"FTS5 full-text search available via query search action. " +
	"Diff compares instances/sessions/environments. Projection provides scalar/vector/heatmap/cube analytics."

// NewServer creates the Chronolog MCP server with all 6 tools registered.
func NewServer(s port.Store, version string) *batterymcp.Server {
	bsrv := batterymcp.NewServer("chronolog", version).
		WithInstructions(instructions)

	h := &handler{store: s}

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
			"Actions: add_source (instance_id, source, lines[] — stages lines, parses RFC3339 timestamps, " +
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
			"trace_from_code (event_id — incoming traces_to edges).",
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

	return bsrv
}

type handler struct {
	store port.Store
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

func (h *handler) handleChronolog(ctx context.Context, raw json.RawMessage) (tool.Result, error) {
	var in chronologInput
	if err := json.Unmarshal(raw, &in); err != nil {
		return tool.ErrorResult(err), nil
	}
	slog.DebugContext(ctx, "handler entry", slog.String(logKeyTool, "chronolog"), slog.String(logKeyAction, in.Action))
	switch in.Action {
	case "create_domain":
		d := &domain.Domain{ID: uuid.NewString(), Name: in.Name, Alias: in.Alias, Description: in.Description, CreatedAt: time.Now().UTC()}
		if err := h.store.PutDomain(ctx, d); err != nil {
			return tool.ErrorResult(err), nil
		}
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
		return jsonResult(i)
	case "list_instances":
		is, err := h.store.ListInstances(ctx, in.SessionID)
		if err != nil {
			return tool.ErrorResult(err), nil
		}
		return jsonResult(is)
	case "create_phase", "list_phases":
		return jsonResult(map[string]any{"status": "stub"})
	case "create_bucket", "list_buckets", "get_bucket", "delete_bucket":
		return jsonResult(map[string]any{"status": "stub"})
	case "set_immutable", "verify_integrity":
		return jsonResult(map[string]any{"status": "stub"})
	case "open_case":
		return jsonResult(map[string]any{"status": "stub"})
	default:
		return tool.ErrorResult(fmt.Errorf("chronolog action %q: %w", in.Action, domain.ErrUnknownAction)), nil
	}
}

// --- intake tool ---

type intakeInput struct {
	Action     string   `json:"action"`
	InstanceID string   `json:"instance_id,omitempty"`
	Source     string   `json:"source,omitempty"`
	Lines      []string `json:"lines,omitempty"`
}

func (h *handler) handleIntake(ctx context.Context, raw json.RawMessage) (tool.Result, error) {
	var in intakeInput
	if err := json.Unmarshal(raw, &in); err != nil {
		return tool.ErrorResult(err), nil
	}
	slog.DebugContext(ctx, "handler entry", slog.String(logKeyTool, "intake"), slog.String(logKeyAction, in.Action))
	switch in.Action {
	case "add_source":
		return h.addSource(ctx, in)
	case "list_sources":
		events, err := h.store.ListEvents(ctx, in.InstanceID, port.EventFilter{Limit: 1000})
		if err != nil {
			return tool.ErrorResult(err), nil
		}
		sources := make(map[string]int)
		for _, e := range events {
			sources[e.Source]++
		}
		return jsonResult(sources)
	case "remove_source":
		return h.removeSource(ctx, in)
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
	if len(in.Lines) == 0 {
		return tool.ErrorResult(fmt.Errorf("lines: non-empty array required — read the file and pass individual lines, intake does not read files: %w", domain.ErrInvalidInput)), nil
	}
	var added int
	for i, line := range in.Lines {
		hash := hashLine(in.Source, line)
		lineNum := i + 1

		ts, confidence := parser.Parse(line)

		event := &domain.Event{
			ID:             uuid.NewString(),
			InstanceID:     in.InstanceID,
			Timestamp:      ts,
			TimeConfidence: confidence,
			Message:        line,
			Source:         in.Source,
			SourceHash:     hash,
			LineNumber:     lineNum,
			RawLine:        line,
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

func (h *handler) handleGraph(ctx context.Context, raw json.RawMessage) (tool.Result, error) {
	var in graphInput
	if err := json.Unmarshal(raw, &in); err != nil {
		return tool.ErrorResult(err), nil
	}
	slog.DebugContext(ctx, "handler entry", slog.String(logKeyTool, "graph"), slog.String(logKeyAction, in.Action))
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
	case "label_event", "unlabel_event", "list_labels":
		return jsonResult(map[string]any{"status": "stub"})
	case "auto_trace", "blame", "change_window":
		return jsonResult(map[string]any{"status": "stub"})
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
	Action        string `json:"action"`
	InstanceID    string `json:"instance_id,omitempty"`
	SessionID     string `json:"session_id,omitempty"`
	EnvironmentID string `json:"environment_id,omitempty"`
	EventID       string `json:"event_id,omitempty"`
	Query         string `json:"query,omitempty"`
	Key           string `json:"key,omitempty"`
	Value         string `json:"value,omitempty"`
	Pattern       string `json:"pattern,omitempty"`
	Label         string `json:"label,omitempty"`
	Limit         int    `json:"limit,omitempty"`
	Window        int    `json:"window,omitempty"`
}

func (h *handler) handleQuery(ctx context.Context, raw json.RawMessage) (tool.Result, error) {
	var in queryInput
	if err := json.Unmarshal(raw, &in); err != nil {
		return tool.ErrorResult(err), nil
	}
	slog.DebugContext(ctx, "handler entry", slog.String(logKeyTool, "query"), slog.String(logKeyAction, in.Action))
	limit := in.Limit
	if limit <= 0 {
		limit = 100
	}
	switch in.Action {
	case "timeline":
		events, err := h.store.ListEvents(ctx, in.InstanceID, port.EventFilter{Limit: limit})
		if err != nil {
			return tool.ErrorResult(err), nil
		}
		return jsonResult(events)
	case "search":
		events, err := h.store.SearchEvents(ctx, in.Query, limit)
		if err != nil {
			return tool.ErrorResult(err), nil
		}
		return jsonResult(events)
	case "around":
		return h.queryAround(ctx, in, limit)
	case "correlations":
		return h.queryCorrelations(ctx, in)
	case "trace_to_code":
		return h.traceCode(ctx, in, port.Outgoing)
	case "trace_from_code":
		return h.traceCode(ctx, in, port.Incoming)
	case "search_by_label", "search_by_bookmark", "suspects", "time_of_defect", "recurrence":
		return jsonResult(map[string]any{"status": "stub"})
	default:
		return tool.ErrorResult(fmt.Errorf("query action %q: %w", in.Action, domain.ErrUnknownAction)), nil
	}
}

func (h *handler) queryAround(ctx context.Context, in queryInput, limit int) (tool.Result, error) {
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
	return jsonResult(events[start:end])
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
	switch in.Action {
	case "instance_diff", "hot_cold_map":
		return h.instanceDiff(ctx, in)
	case "session_diff":
		return h.sessionDiff(ctx, in)
	case "environment_diff":
		return h.environmentDiff(ctx, in)
	case "regression_check", "set_baseline":
		return jsonResult(map[string]any{"status": "stub"})
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

func jsonResult(data any) (tool.Result, error) {
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	return tool.TextResult(string(b)), nil
}
