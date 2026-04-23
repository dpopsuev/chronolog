package mcp

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/dpopsuev/battery/tool"
	"github.com/dpopsuev/chronolog/internal/domain"
)

// GitRunner abstracts git operations for testability.
type GitRunner interface {
	Blame(ctx context.Context, repoPath, file string, line int) (*domain.BlameResult, error)
	Log(ctx context.Context, repoPath string, after, before time.Time) ([]*domain.GitCommit, error)
}

func (h *handler) autoTrace(ctx context.Context, in *graphInput) (tool.Result, error) {
	if in.InstanceID == "" || in.CodebaseID == "" {
		return tool.ErrorResult(fmt.Errorf("instance_id and codebase_id: %w", domain.ErrInvalidInput)), nil
	}
	slog.DebugContext(ctx, "auto_trace", slog.String(logKeyInstanceID, in.InstanceID))
	return jsonResult(map[string]any{"traces_created": 0, "status": "stub"})
}

func (h *handler) blameEvent(ctx context.Context, in *graphInput) (tool.Result, error) {
	if in.EventID == "" {
		return tool.ErrorResult(fmt.Errorf("event_id: %w", domain.ErrInvalidInput)), nil
	}
	slog.DebugContext(ctx, "blame", slog.String(logKeyEventID, in.EventID))
	return jsonResult(map[string]any{"status": "stub"})
}

func (h *handler) changeWindow(ctx context.Context, in *graphInput) (tool.Result, error) {
	if in.CodebaseID == "" {
		return tool.ErrorResult(fmt.Errorf("codebase_id: %w", domain.ErrInvalidInput)), nil
	}
	slog.DebugContext(ctx, "change_window", slog.String(logKeyCodebaseID, in.CodebaseID))
	return jsonResult(map[string]any{"commits": []any{}, "status": "stub"})
}
