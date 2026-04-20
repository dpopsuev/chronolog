package mcp

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/dpopsuev/battery/tool"
	"github.com/dpopsuev/chronolog/internal/store"
)

func TestRoundTrip_CreateCascade_Ingest_Query(t *testing.T) { //nolint:funlen // integration test spanning full pipeline
	s := store.NewMemStore()
	h := &handler{store: s}

	domainRes := call(t, h.handleChronolog, map[string]any{
		"action": "create_domain",
		"name":   "OpenShift PTP",
	})
	domainID := extractID(t, domainRes)

	envRes := call(t, h.handleChronolog, map[string]any{
		"action":    "create_environment",
		"name":      "4.16",
		"domain_id": domainID,
	})
	envID := extractID(t, envRes)

	sessRes := call(t, h.handleChronolog, map[string]any{
		"action":         "create_session",
		"name":           "dec-20",
		"environment_id": envID,
	})
	sessID := extractID(t, sessRes)

	instRes := call(t, h.handleChronolog, map[string]any{
		"action":     "create_instance",
		"name":       "ptp_bc_freerun",
		"session_id": sessID,
	})
	instID := extractID(t, instRes)

	call(t, h.handleIntake, map[string]any{
		"action":      "add_source",
		"instance_id": instID,
		"source":      "cloud-events.log",
		"lines": []string{
			"2025-12-20T14:59:20Z offset 3ns",
			"2025-12-20T14:59:21Z FREERUN published",
			"2025-12-20T14:59:22Z holdover state s1",
		},
	})

	timelineRes := call(t, h.handleQuery, map[string]any{
		"action":      "timeline",
		"instance_id": instID,
	})
	events := extractArray(t, timelineRes)
	if len(events) != 3 {
		t.Fatalf("timeline events = %d, want 3", len(events))
	}

	first := events[0].(map[string]any)
	if msg, ok := first["message"].(string); !ok || msg != "2025-12-20T14:59:20Z offset 3ns" {
		t.Errorf("first event message = %v, want offset 3ns line", first["message"])
	}

	searchRes := call(t, h.handleQuery, map[string]any{
		"action": "search",
		"query":  "FREERUN",
	})
	searchEvents := extractArray(t, searchRes)
	if len(searchEvents) != 1 {
		t.Fatalf("search results = %d, want 1", len(searchEvents))
	}
}

func TestStub_Diff(t *testing.T) {
	s := store.NewMemStore()
	h := &handler{store: s}

	res := call(t, h.handleDiff, map[string]any{"action": "instance_diff"})
	text := resultText(t, res)
	if text != "stub: instance_diff not yet implemented" {
		t.Errorf("diff stub = %q, want stub message", text)
	}
}

func TestStub_Projection(t *testing.T) {
	s := store.NewMemStore()
	h := &handler{store: s}

	res := call(t, h.handleProjection, map[string]any{"action": "heatmap"})
	text := resultText(t, res)
	if text != "stub: heatmap not yet implemented" {
		t.Errorf("projection stub = %q, want stub message", text)
	}
}

func call(t *testing.T, fn func(context.Context, json.RawMessage) (tool.Result, error), input map[string]any) tool.Result { //nolint:revive // t before ctx is standard test convention
	t.Helper()
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("marshal input: %v", err)
	}
	res, err := fn(context.Background(), raw)
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if res.IsError {
		t.Fatalf("handler returned error result: %s", resultText(t, res))
	}
	return res
}

func resultText(t *testing.T, res tool.Result) string {
	t.Helper()
	text := res.Text()
	if text == "" {
		t.Fatal("empty result text")
	}
	return text
}

func extractID(t *testing.T, res tool.Result) string {
	t.Helper()
	text := resultText(t, res)
	var obj map[string]any
	if err := json.Unmarshal([]byte(text), &obj); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	id, ok := obj["id"].(string)
	if !ok {
		t.Fatalf("no id in result: %v", obj)
	}
	return id
}

func extractArray(t *testing.T, res tool.Result) []any {
	t.Helper()
	text := resultText(t, res)
	var arr []any
	if err := json.Unmarshal([]byte(text), &arr); err != nil {
		t.Fatalf("unmarshal array: %v", err)
	}
	return arr
}
