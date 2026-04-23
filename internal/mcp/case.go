package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/dpopsuev/battery/tool"
	"github.com/dpopsuev/chronolog/internal/domain"
	"github.com/google/uuid"
)

const logKeyCaseID = "case_id"

type caseInput struct {
	Action      string `json:"action"`
	CaseID      string `json:"case_id,omitempty"`
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
	EventID     string `json:"event_id,omitempty"`
	Content     string `json:"content,omitempty"`
	Verdict     string `json:"verdict,omitempty"`
}

var caseSchema = json.RawMessage(`{
	"type": "object",
	"properties": {
		"action": {"type": "string", "enum": ["open_case", "close_case", "list_cases", "get_case", "add_symptom", "list_symptoms", "set_root_cause", "get_root_cause", "append_transcript", "get_transcript"], "description": "Case lifecycle action"},
		"case_id": {"type": "string", "description": "Case UUID"},
		"title": {"type": "string", "description": "Case title (for open_case)"},
		"description": {"type": "string", "description": "Symptom description or root cause description"},
		"event_id": {"type": "string", "description": "Evidence event UUID (for add_symptom/set_root_cause)"},
		"content": {"type": "string", "description": "Transcript entry content"},
		"verdict": {"type": "string", "description": "Case verdict (for close_case)"}
	},
	"required": ["action"]
}`)

func (h *handler) handleCase(ctx context.Context, raw json.RawMessage) (tool.Result, error) {
	var in caseInput
	if err := json.Unmarshal(raw, &in); err != nil {
		return tool.ErrorResult(err), nil
	}
	slog.DebugContext(ctx, "handler entry", slog.String(logKeyTool, "case"), slog.String(logKeyAction, in.Action))
	switch in.Action {
	case "open_case":
		return h.openCase(ctx, in)
	case "close_case":
		return h.closeCase(ctx, in)
	case "list_cases":
		cs, err := h.store.ListCases(ctx)
		if err != nil {
			return tool.ErrorResult(err), nil
		}
		return jsonResult(cs)
	case "get_case":
		return h.getCase(ctx, in)
	case "add_symptom":
		return h.addSymptom(ctx, in)
	case "list_symptoms":
		ss, err := h.store.ListSymptoms(ctx, in.CaseID)
		if err != nil {
			return tool.ErrorResult(err), nil
		}
		return jsonResult(ss)
	case "set_root_cause":
		return h.setRootCause(ctx, in)
	case "get_root_cause":
		rc, err := h.store.GetRootCause(ctx, in.CaseID)
		if err != nil {
			return tool.ErrorResult(err), nil
		}
		return jsonResult(rc)
	case "append_transcript":
		return h.appendTranscript(ctx, in)
	case "get_transcript":
		entries, err := h.store.ListTranscriptEntries(ctx, in.CaseID)
		if err != nil {
			return tool.ErrorResult(err), nil
		}
		return jsonResult(entries)
	default:
		return tool.ErrorResult(fmt.Errorf("case action %q: %w", in.Action, domain.ErrUnknownAction)), nil
	}
}

func (h *handler) openCase(ctx context.Context, in caseInput) (tool.Result, error) {
	if in.Title == "" {
		return tool.ErrorResult(fmt.Errorf("title: %w", domain.ErrInvalidInput)), nil
	}
	c := &domain.Case{ID: uuid.NewString(), Title: in.Title, Status: "open", CreatedAt: time.Now().UTC()}
	if err := h.store.PutCase(ctx, c); err != nil {
		return tool.ErrorResult(err), nil
	}
	slog.DebugContext(ctx, "case opened", slog.String(logKeyCaseID, c.ID))
	return jsonResult(c)
}

func (h *handler) closeCase(ctx context.Context, in caseInput) (tool.Result, error) {
	if in.CaseID == "" {
		return tool.ErrorResult(fmt.Errorf("case_id: %w", domain.ErrInvalidInput)), nil
	}
	c, err := h.store.GetCase(ctx, in.CaseID)
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	now := time.Now().UTC()
	c.Status = "closed"
	c.ClosedAt = &now
	if err := h.store.PutCase(ctx, c); err != nil {
		return tool.ErrorResult(err), nil
	}
	slog.DebugContext(ctx, "case closed", slog.String(logKeyCaseID, c.ID))
	return jsonResult(c)
}

func (h *handler) getCase(ctx context.Context, in caseInput) (tool.Result, error) {
	if in.CaseID == "" {
		return tool.ErrorResult(fmt.Errorf("case_id: %w", domain.ErrInvalidInput)), nil
	}
	c, err := h.store.GetCase(ctx, in.CaseID)
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	symptoms, _ := h.store.ListSymptoms(ctx, in.CaseID)
	rootCause, _ := h.store.GetRootCause(ctx, in.CaseID)
	transcript, _ := h.store.ListTranscriptEntries(ctx, in.CaseID)
	return jsonResult(map[string]any{
		"case":       c,
		"symptoms":   symptoms,
		"root_cause": rootCause,
		"transcript": transcript,
	})
}

func (h *handler) addSymptom(ctx context.Context, in caseInput) (tool.Result, error) {
	if in.CaseID == "" || in.Description == "" {
		return tool.ErrorResult(fmt.Errorf("case_id and description: %w", domain.ErrInvalidInput)), nil
	}
	s := &domain.Symptom{ID: uuid.NewString(), CaseID: in.CaseID, Description: in.Description, EventID: in.EventID, CreatedAt: time.Now().UTC()}
	if err := h.store.PutSymptom(ctx, s); err != nil {
		return tool.ErrorResult(err), nil
	}
	return jsonResult(s)
}

func (h *handler) setRootCause(ctx context.Context, in caseInput) (tool.Result, error) {
	if in.CaseID == "" || in.Description == "" {
		return tool.ErrorResult(fmt.Errorf("case_id and description: %w", domain.ErrInvalidInput)), nil
	}
	rc := &domain.RootCause{ID: uuid.NewString(), CaseID: in.CaseID, Description: in.Description, EventID: in.EventID, CreatedAt: time.Now().UTC()}
	if err := h.store.PutRootCause(ctx, rc); err != nil {
		return tool.ErrorResult(err), nil
	}
	slog.DebugContext(ctx, "root cause set", slog.String(logKeyCaseID, in.CaseID))
	return jsonResult(rc)
}

func (h *handler) appendTranscript(ctx context.Context, in caseInput) (tool.Result, error) {
	if in.CaseID == "" || in.Content == "" {
		return tool.ErrorResult(fmt.Errorf("case_id and content: %w", domain.ErrInvalidInput)), nil
	}
	entries, err := h.store.ListTranscriptEntries(ctx, in.CaseID)
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	te := &domain.TranscriptEntry{
		ID:        uuid.NewString(),
		CaseID:    in.CaseID,
		Seq:       len(entries) + 1,
		Content:   in.Content,
		CreatedAt: time.Now().UTC(),
	}
	if err := h.store.PutTranscriptEntry(ctx, te); err != nil {
		return tool.ErrorResult(err), nil
	}
	return jsonResult(te)
}
