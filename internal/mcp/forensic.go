package mcp

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/dpopsuev/battery/tool"
	"github.com/dpopsuev/chronolog/internal/domain"
	"github.com/dpopsuev/chronolog/internal/port"
)

// collectScopedEvents gathers events bounded by instance_id or session_id scope.
func (h *handler) collectScopedEvents(ctx context.Context, instanceID, sessionID string) ([]*domain.Event, error) {
	if instanceID != "" {
		return h.store.ListEvents(ctx, instanceID, port.EventFilter{Limit: 100000})
	}
	if sessionID != "" {
		instances, err := h.store.ListInstances(ctx, sessionID)
		if err != nil {
			return nil, err
		}
		var all []*domain.Event
		for _, inst := range instances {
			events, eErr := h.store.ListEvents(ctx, inst.ID, port.EventFilter{Limit: 100000})
			if eErr != nil {
				return nil, eErr
			}
			all = append(all, events...)
		}
		return all, nil
	}
	return nil, fmt.Errorf("instance_id or session_id: %w", domain.ErrInvalidInput)
}

func (h *handler) searchByLabel(ctx context.Context, in queryInput) (tool.Result, error) {
	if in.Key == "" || in.Value == "" {
		return tool.ErrorResult(fmt.Errorf("key and value: %w", domain.ErrInvalidInput)), nil
	}
	events, err := h.collectScopedEvents(ctx, in.InstanceID, in.SessionID)
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	var matched []*domain.Event
	for _, e := range events {
		if e.Labels[in.Key] == in.Value {
			matched = append(matched, e)
		}
	}
	slog.DebugContext(ctx, "search_by_label completed", slog.Int(logKeyCount, len(matched)))
	return jsonResult(matched)
}

func (h *handler) searchByBookmark(ctx context.Context, in queryInput) (tool.Result, error) {
	events, err := h.collectScopedEvents(ctx, in.InstanceID, in.SessionID)
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	type bookmarkedEvent struct {
		Bookmark *domain.Bookmark `json:"bookmark"`
		Event    *domain.Event    `json:"event"`
	}
	var results []bookmarkedEvent
	for _, e := range events {
		bms, bErr := h.store.ListBookmarks(ctx, e.ID)
		if bErr != nil {
			return tool.ErrorResult(bErr), nil
		}
		for _, b := range bms {
			if in.Label == "" || b.Label == in.Label {
				results = append(results, bookmarkedEvent{Bookmark: b, Event: e})
			}
		}
	}
	slog.DebugContext(ctx, "search_by_bookmark completed", slog.Int(logKeyCount, len(results)))
	return jsonResult(results)
}

// findCorrelated returns events from other sources within a time window of the target.
func (h *handler) findCorrelated(ctx context.Context, target *domain.Event, windowSec int) (map[string][]*domain.Event, error) {
	window := time.Duration(windowSec) * time.Second
	if window <= 0 {
		window = 5 * time.Second
	}
	after := target.Timestamp.Add(-window)
	before := target.Timestamp.Add(window)
	events, err := h.store.ListEvents(ctx, target.InstanceID, port.EventFilter{After: &after, Before: &before, Limit: 10000})
	if err != nil {
		return nil, err
	}
	grouped := make(map[string][]*domain.Event)
	for _, e := range events {
		if e.Source == target.Source {
			continue
		}
		grouped[e.Source] = append(grouped[e.Source], e)
	}
	return grouped, nil
}

func (h *handler) suspects(ctx context.Context, in queryInput) (tool.Result, error) {
	if in.Key == "" || in.Value == "" {
		return tool.ErrorResult(fmt.Errorf("key and value: %w", domain.ErrInvalidInput)), nil
	}
	events, err := h.collectScopedEvents(ctx, in.InstanceID, in.SessionID)
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	var labeled []*domain.Event
	for _, e := range events {
		if e.Labels[in.Key] == in.Value {
			labeled = append(labeled, e)
		}
	}
	if len(labeled) == 0 {
		return jsonResult([]any{})
	}

	sourceCounts := make(map[string]int)
	for _, e := range labeled {
		correlated, cErr := h.findCorrelated(ctx, e, in.Window)
		if cErr != nil {
			return tool.ErrorResult(cErr), nil
		}
		for src := range correlated {
			sourceCounts[src]++
		}
	}

	type suspect struct {
		Source           string  `json:"source"`
		Count            int     `json:"count"`
		CoOccurrenceRate float64 `json:"co_occurrence_rate"`
	}
	suspects := make([]suspect, 0, len(sourceCounts))
	for src, cnt := range sourceCounts {
		suspects = append(suspects, suspect{Source: src, Count: cnt, CoOccurrenceRate: float64(cnt) / float64(len(labeled))})
	}
	sort.Slice(suspects, func(i, j int) bool { return suspects[i].Count > suspects[j].Count })

	slog.DebugContext(ctx, "suspects completed", slog.Int(logKeyCount, len(suspects)))
	return jsonResult(suspects)
}

func (h *handler) timeOfDefect(ctx context.Context, in queryInput) (tool.Result, error) {
	if in.Pattern == "" && (in.Key == "" || in.Value == "") {
		return tool.ErrorResult(fmt.Errorf("pattern or key+value: %w", domain.ErrInvalidInput)), nil
	}

	matchesEvent := func(e *domain.Event) bool {
		if in.Pattern != "" {
			return strings.Contains(e.Message, in.Pattern)
		}
		return e.Labels[in.Key] == in.Value
	}

	if in.InstanceID != "" {
		events, err := h.store.ListEvents(ctx, in.InstanceID, port.EventFilter{Limit: 100000})
		if err != nil {
			return tool.ErrorResult(err), nil
		}
		for i, e := range events {
			if matchesEvent(e) {
				result := map[string]any{
					"first_defect":  e,
					"transition_at": e.Timestamp,
				}
				if i > 0 {
					result["last_healthy"] = events[i-1]
					result["gap"] = e.Timestamp.Sub(events[i-1].Timestamp).String()
				}
				return jsonResult(result)
			}
		}
		return jsonResult(map[string]any{"first_defect": nil})
	}

	if in.SessionID != "" {
		return h.timeOfDefectAcrossInstances(ctx, in.SessionID, matchesEvent)
	}

	return tool.ErrorResult(fmt.Errorf("instance_id or session_id: %w", domain.ErrInvalidInput)), nil
}

func (h *handler) timeOfDefectAcrossInstances(ctx context.Context, sessionID string, matches func(*domain.Event) bool) (tool.Result, error) {
	instances, err := h.store.ListInstances(ctx, sessionID)
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	sort.Slice(instances, func(i, j int) bool { return instances[i].StartedAt.Before(instances[j].StartedAt) })

	var lastHealthy *domain.Instance
	for _, inst := range instances {
		events, eErr := h.store.ListEvents(ctx, inst.ID, port.EventFilter{Limit: 100000})
		if eErr != nil {
			return tool.ErrorResult(eErr), nil
		}
		found := false
		for _, e := range events {
			if matches(e) {
				found = true
				break
			}
		}
		if found {
			result := map[string]any{"first_instance_with_defect": inst}
			if lastHealthy != nil {
				result["last_instance_without_defect"] = lastHealthy
			}
			return jsonResult(result)
		}
		lastHealthy = inst
	}
	return jsonResult(map[string]any{"first_instance_with_defect": nil})
}

func (h *handler) recurrence(ctx context.Context, in queryInput) (tool.Result, error) {
	if in.Pattern == "" && (in.Key == "" || in.Value == "") {
		return tool.ErrorResult(fmt.Errorf("pattern or key+value: %w", domain.ErrInvalidInput)), nil
	}
	if in.EnvironmentID == "" && in.SessionID == "" {
		return tool.ErrorResult(fmt.Errorf("environment_id or session_id: %w", domain.ErrInvalidInput)), nil
	}

	var sessions []*domain.Session
	var err error
	if in.EnvironmentID != "" {
		sessions, err = h.store.ListSessions(ctx, in.EnvironmentID)
	} else {
		s, sErr := h.store.GetSession(ctx, in.SessionID)
		if sErr != nil {
			return tool.ErrorResult(sErr), nil
		}
		sessions, err = h.store.ListSessions(ctx, s.EnvironmentID)
	}
	if err != nil {
		return tool.ErrorResult(err), nil
	}

	targetPattern := in.Pattern
	type sessionResult struct {
		SessionID   string `json:"session_id"`
		SessionName string `json:"session_name"`
		Present     bool   `json:"present"`
		Count       int    `json:"count"`
	}

	results := make([]sessionResult, 0, len(sessions))
	var presentCount int
	for _, sess := range sessions {
		instances, iErr := h.store.ListInstances(ctx, sess.ID)
		if iErr != nil {
			return tool.ErrorResult(iErr), nil
		}
		var count int
		for _, inst := range instances {
			events, eErr := h.store.ListEvents(ctx, inst.ID, port.EventFilter{Limit: 100000})
			if eErr != nil {
				return tool.ErrorResult(eErr), nil
			}
			if targetPattern != "" {
				templates := extractTemplates(events)
				for _, tmpl := range templates {
					if strings.Contains(tmpl.Pattern, targetPattern) || strings.Contains(targetPattern, tmpl.Pattern) {
						count += tmpl.Count
					}
				}
			} else {
				for _, e := range events {
					if e.Labels[in.Key] == in.Value {
						count++
					}
				}
			}
		}
		present := count > 0
		if present {
			presentCount++
		}
		results = append(results, sessionResult{SessionID: sess.ID, SessionName: sess.Name, Present: present, Count: count})
	}

	slog.DebugContext(ctx, "recurrence completed", slog.Int(logKeyCount, presentCount))
	return jsonResult(map[string]any{
		"pattern":          targetPattern,
		"sessions":         results,
		"total_sessions":   len(sessions),
		"present_sessions": presentCount,
	})
}

func (h *handler) regressionCheck(ctx context.Context, in diffInput) (tool.Result, error) {
	if in.SessionID == "" || in.BaselineSessionID == "" {
		return tool.ErrorResult(fmt.Errorf("session_id and baseline_session_id: %w", domain.ErrInvalidInput)), nil
	}
	currentEvents, err := h.collectSessionEvents(ctx, in.SessionID)
	if err != nil {
		return tool.ErrorResult(err), nil
	}
	baselineEvents, err := h.collectSessionEvents(ctx, in.BaselineSessionID)
	if err != nil {
		return tool.ErrorResult(err), nil
	}

	diff := computeDiff(currentEvents, baselineEvents)
	hotPatterns, _ := diff["hot"].([]map[string]any)

	var newPatterns []map[string]any
	for _, p := range hotPatterns {
		if side, _ := p["side"].(string); side == "A" {
			newPatterns = append(newPatterns, p)
		}
	}

	threshold := in.Threshold
	verdict := "pass"
	if len(newPatterns) > threshold {
		verdict = "fail"
	}

	slog.DebugContext(ctx, "regression_check completed",
		slog.String(logKeySessionID, in.SessionID),
		slog.Int(logKeyCount, len(newPatterns)),
	)
	return jsonResult(map[string]any{
		"verdict":      verdict,
		"hot_count":    len(newPatterns),
		"threshold":    threshold,
		"new_patterns": newPatterns,
	})
}

func (h *handler) collectSessionEvents(ctx context.Context, sessionID string) ([]*domain.Event, error) {
	instances, err := h.store.ListInstances(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	var all []*domain.Event
	for _, inst := range instances {
		events, eErr := h.store.ListEvents(ctx, inst.ID, port.EventFilter{Limit: 100000})
		if eErr != nil {
			return nil, eErr
		}
		all = append(all, events...)
	}
	return all, nil
}
