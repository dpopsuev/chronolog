package mcp

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/dpopsuev/battery/tool"
	"github.com/dpopsuev/chronolog/internal/domain"
	"github.com/dpopsuev/chronolog/internal/port"
)

// GitRunner abstracts git operations for testability.
type GitRunner interface {
	Blame(ctx context.Context, repoPath, file string, line int) (*domain.BlameResult, error)
	Log(ctx context.Context, repoPath string, after, before time.Time) ([]*domain.GitCommit, error)
}

// execGitRunner runs real git commands via os/exec.
type execGitRunner struct{}

func (execGitRunner) Blame(ctx context.Context, repoPath, file string, line int) (*domain.BlameResult, error) {
	lineRange := fmt.Sprintf("-L%d,%d", line, line)
	cmd := exec.CommandContext(ctx, "git", "blame", "--porcelain", lineRange, "--", file)
	cmd.Dir = repoPath
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git blame: %w", err)
	}
	return parseBlameOutput(string(out))
}

func parseBlameOutput(raw string) (*domain.BlameResult, error) {
	result := &domain.BlameResult{}
	scanner := bufio.NewScanner(strings.NewReader(raw))
	if scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 1 {
			result.CommitHash = fields[0]
		}
	}
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "author "):
			result.Author = strings.TrimPrefix(line, "author ")
		case strings.HasPrefix(line, "author-time "):
			ts := strings.TrimPrefix(line, "author-time ")
			if epoch, err := strconv.ParseInt(ts, 10, 64); err == nil {
				result.Date = time.Unix(epoch, 0).UTC()
			}
		case strings.HasPrefix(line, "summary "):
			result.Subject = strings.TrimPrefix(line, "summary ")
		}
	}
	return result, nil
}

func (execGitRunner) Log(ctx context.Context, repoPath string, after, before time.Time) ([]*domain.GitCommit, error) {
	args := []string{"log", "--format=%H|%an|%aI|%s", "--name-only"}
	if !after.IsZero() {
		args = append(args, "--after="+after.Format(time.RFC3339))
	}
	if !before.IsZero() {
		args = append(args, "--before="+before.Format(time.RFC3339))
	}
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = repoPath
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git log: %w", err)
	}
	return parseLogOutput(string(out))
}

func parseLogOutput(raw string) ([]*domain.GitCommit, error) {
	var commits []*domain.GitCommit
	var current *domain.GitCommit
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "|", 4)
		if len(parts) == 4 && len(parts[0]) >= 40 {
			if current != nil {
				commits = append(commits, current)
			}
			t, _ := time.Parse(time.RFC3339, parts[2])
			current = &domain.GitCommit{Hash: parts[0], Author: parts[1], Date: t, Subject: parts[3]}
		} else if current != nil {
			current.Files = append(current.Files, line)
		}
	}
	if current != nil {
		commits = append(commits, current)
	}
	return commits, nil
}

var reFileLineRef = regexp.MustCompile(`([a-zA-Z0-9_/.-]+\.\w+):(\d+)`)

func (h *handler) autoTrace(ctx context.Context, in *graphInput) (tool.Result, error) {
	if in.InstanceID == "" || in.CodebaseID == "" {
		return tool.ErrorResult(fmt.Errorf("instance_id and codebase_id: %w", domain.ErrInvalidInput)), nil
	}
	if h.git == nil {
		return tool.ErrorResult(domain.ErrGitNotConfigured), nil
	}
	cb, err := h.store.GetCodebase(ctx, in.CodebaseID)
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	repoPath, err := filepath.Abs(cb.RootPath)
	if err != nil || strings.Contains(repoPath, "..") {
		return tool.ErrorResult(fmt.Errorf("invalid codebase path: %w", domain.ErrInvalidInput)), nil
	}
	events, err := h.store.ListEvents(ctx, in.InstanceID, port.EventFilter{Limit: 100000})
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	var tracesCreated int
	for _, e := range events {
		matches := reFileLineRef.FindStringSubmatch(e.Message)
		if len(matches) < 3 {
			continue
		}
		file := matches[1]
		lineNum, _ := strconv.Atoi(matches[2])
		edge := domain.Edge{FromID: e.ID, Relation: domain.RelTracesTo, ToID: fmt.Sprintf("code:%s:%d", file, lineNum)}
		if aErr := h.store.AddEdge(ctx, edge); aErr != nil {
			continue
		}
		tracesCreated++
	}
	slog.DebugContext(ctx, "auto_trace completed", slog.String(logKeyInstanceID, in.InstanceID), slog.Int(logKeyCount, tracesCreated))
	return jsonResult(map[string]any{"traces_created": tracesCreated, "instance_id": in.InstanceID})
}

func (h *handler) blameEvent(ctx context.Context, in *graphInput) (tool.Result, error) {
	if in.EventID == "" {
		return tool.ErrorResult(fmt.Errorf("event_id: %w", domain.ErrInvalidInput)), nil
	}
	if h.git == nil {
		return tool.ErrorResult(domain.ErrGitNotConfigured), nil
	}
	e, err := h.store.GetEvent(ctx, in.EventID)
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	matches := reFileLineRef.FindStringSubmatch(e.Message)
	if len(matches) < 3 {
		return tool.ErrorResult(domain.ErrNoCodeRef), nil
	}
	file := matches[1]
	lineNum, _ := strconv.Atoi(matches[2])

	edges, err := h.store.Neighbors(ctx, in.EventID, domain.RelTracesTo, port.Outgoing)
	if err != nil {
		return tool.ErrorResult(err), nil
	}

	codebases, _ := h.store.ListCodebases(ctx)
	if len(codebases) == 0 {
		return tool.ErrorResult(domain.ErrNoCodebases), nil
	}
	repoPath, _ := filepath.Abs(codebases[0].RootPath)

	result, bErr := h.git.Blame(ctx, repoPath, file, lineNum)
	if bErr != nil {
		return tool.ErrorResult(bErr), nil
	}
	slog.DebugContext(ctx, "blame completed", slog.String(logKeyEventID, in.EventID))
	return jsonResult(map[string]any{"blame": result, "file": file, "line": lineNum, "edges": len(edges)})
}

func (h *handler) changeWindow(ctx context.Context, in *graphInput) (tool.Result, error) {
	if in.CodebaseID == "" {
		return tool.ErrorResult(fmt.Errorf("codebase_id: %w", domain.ErrInvalidInput)), nil
	}
	if h.git == nil {
		return tool.ErrorResult(domain.ErrGitNotConfigured), nil
	}
	cb, err := h.store.GetCodebase(ctx, in.CodebaseID)
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	repoPath, _ := filepath.Abs(cb.RootPath)

	var after, before time.Time
	if in.After != "" {
		after, _ = time.Parse(time.RFC3339, in.After)
	}
	if in.Before != "" {
		before, _ = time.Parse(time.RFC3339, in.Before)
	}

	commits, lErr := h.git.Log(ctx, repoPath, after, before)
	if lErr != nil {
		return tool.ErrorResult(lErr), nil
	}
	slog.DebugContext(ctx, "change_window completed", slog.String(logKeyCodebaseID, in.CodebaseID), slog.Int(logKeyCount, len(commits)))
	return jsonResult(map[string]any{"commits": commits, "codebase_id": in.CodebaseID})
}
