package mcp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	batterymcp "github.com/dpopsuev/battery/mcp"
	"github.com/dpopsuev/battery/server"
	"github.com/dpopsuev/battery/tool"
	"github.com/dpopsuev/chronolog/internal/domain"
	"github.com/dpopsuev/chronolog/internal/parser"
	"github.com/dpopsuev/chronolog/internal/port"
	"github.com/google/uuid"
)

// Slog attribute key constants.
const (
	logKeySource = "source"
	logKeyLine   = "line"
	logKeyError  = "error"
)

const instructions = "Chronolog consolidates multiple log sources into a single clean " +
	"traversable timeline with code traceability. Six tools: chronolog (cascade lifecycle), " +
	"intake (log source staging), graph (merge + edges), query (timeline + FTS5 search), " +
	"diff (hot-cold comparison), projection (tensor export)."

// NewServer creates the Chronolog MCP server with all 6 tools registered.
func NewServer(s port.Store, version string) *batterymcp.Server {
	bsrv := batterymcp.NewServer("chronolog", version).
		WithInstructions(instructions)

	h := &handler{store: s}

	bsrv.Tool(server.ToolMeta{
		Name:        "chronolog",
		Description: "Cascade lifecycle. Actions: create_domain, list_domains, create_environment, list_environments, create_session, list_sessions, create_instance, list_instances.",
		Keywords:    []string{"domain", "environment", "session", "instance", "cascade"},
		Categories:  []string{"lifecycle"},
	}, h.handleChronolog)

	bsrv.Tool(server.ToolMeta{
		Name:        "intake",
		Description: "Log source staging. Actions: add_source, remove_source, list_sources. Front door only — no processing.",
		Keywords:    []string{"source", "log", "add", "stage"},
		Categories:  []string{"intake"},
	}, h.handleIntake)

	bsrv.Tool(server.ToolMeta{
		Name:        "graph",
		Description: "Graph operations. Actions: add_edge, remove_edge, merge (stub), add_bookmark, add_highlight, register_service, register_codebase, collapse (stub).",
		Keywords:    []string{"edge", "merge", "annotate", "bookmark", "highlight"},
		Categories:  []string{"graph"},
	}, h.handleGraph)

	bsrv.Tool(server.ToolMeta{
		Name:        "query",
		Description: "Timeline query and search. Actions: timeline, search, trace_to_code (stub), trace_from_code (stub), around (stub).",
		Keywords:    []string{"timeline", "search", "trace", "query"},
		Categories:  []string{"query"},
	}, h.handleQuery)

	bsrv.Tool(server.ToolMeta{
		Name:        "diff",
		Description: "Instance and session comparison. Actions: instance_diff (stub), session_diff (stub), environment_diff (stub), hot_cold_map (stub).",
		Keywords:    []string{"diff", "compare", "hot", "cold"},
		Categories:  []string{"analysis"},
	}, h.handleDiff)

	bsrv.Tool(server.ToolMeta{
		Name:        "projection",
		Description: "Tensor query and export. Actions: scalar (stub), vector (stub), heatmap (stub), cube (stub), export (stub).",
		Keywords:    []string{"scalar", "vector", "heatmap", "cube", "export"},
		Categories:  []string{"projection"},
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
	Description   string `json:"description,omitempty"`
	Alias         string `json:"alias,omitempty"`
}

func (h *handler) handleChronolog(ctx context.Context, raw json.RawMessage) (tool.Result, error) {
	var in chronologInput
	if err := json.Unmarshal(raw, &in); err != nil {
		return tool.ErrorResult(err), nil
	}
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
		return stub("remove_source")
	default:
		return tool.ErrorResult(fmt.Errorf("intake action %q: %w", in.Action, domain.ErrUnknownAction)), nil
	}
}

func (h *handler) addSource(ctx context.Context, in intakeInput) (tool.Result, error) {
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

func hashLine(source, line string) string {
	h := sha256.Sum256([]byte(source + "\x00" + line))
	return hex.EncodeToString(h[:16])
}

// --- graph tool ---

type graphInput struct {
	Action   string `json:"action"`
	FromID   string `json:"from_id,omitempty"`
	Relation string `json:"relation,omitempty"`
	ToID     string `json:"to_id,omitempty"`
}

func (h *handler) handleGraph(ctx context.Context, raw json.RawMessage) (tool.Result, error) {
	var in graphInput
	if err := json.Unmarshal(raw, &in); err != nil {
		return tool.ErrorResult(err), nil
	}
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
	default:
		return stub(in.Action)
	}
}

// --- query tool ---

type queryInput struct {
	Action     string `json:"action"`
	InstanceID string `json:"instance_id,omitempty"`
	Query      string `json:"query,omitempty"`
	Limit      int    `json:"limit,omitempty"`
}

func (h *handler) handleQuery(ctx context.Context, raw json.RawMessage) (tool.Result, error) {
	var in queryInput
	if err := json.Unmarshal(raw, &in); err != nil {
		return tool.ErrorResult(err), nil
	}
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
	default:
		return stub(in.Action)
	}
}

// --- diff tool ---

func (h *handler) handleDiff(_ context.Context, raw json.RawMessage) (tool.Result, error) {
	var in struct {
		Action string `json:"action"`
	}
	if err := json.Unmarshal(raw, &in); err != nil {
		return tool.ErrorResult(err), nil
	}
	return stub(in.Action)
}

// --- projection tool ---

func (h *handler) handleProjection(_ context.Context, raw json.RawMessage) (tool.Result, error) {
	var in struct {
		Action string `json:"action"`
	}
	if err := json.Unmarshal(raw, &in); err != nil {
		return tool.ErrorResult(err), nil
	}
	return stub(in.Action)
}

// --- helpers ---

func stub(action string) (tool.Result, error) {
	return tool.TextResult(fmt.Sprintf("stub: %s not yet implemented", action)), nil
}

func jsonResult(data any) (tool.Result, error) {
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	return tool.TextResult(string(b)), nil
}
