package mcp

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/dpopsuev/battery/tool"
	"github.com/dpopsuev/chronolog/internal/domain"
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

func TestAddBookmark(t *testing.T) {
	h, instID, eventIDs := setupWithEvents(t, 1)
	call(t, h.handleGraph, map[string]any{
		"action":   "add_bookmark",
		"event_id": eventIDs[0],
		"label":    "important",
		"note":     "check this",
	})
	res := call(t, h.handleGraph, map[string]any{
		"action":   "list_bookmarks",
		"event_id": eventIDs[0],
	})
	arr := extractArray(t, res)
	if len(arr) != 1 {
		t.Fatalf("list_bookmarks len = %d, want 1", len(arr))
	}
	_ = instID
}

func TestAddHighlight(t *testing.T) {
	h, _, eventIDs := setupWithEvents(t, 1)
	call(t, h.handleGraph, map[string]any{
		"action":    "add_highlight",
		"event_id":  eventIDs[0],
		"substring": "offset",
		"label":     "key-metric",
	})
	res := call(t, h.handleGraph, map[string]any{
		"action":   "list_highlights",
		"event_id": eventIDs[0],
	})
	arr := extractArray(t, res)
	if len(arr) != 1 {
		t.Fatalf("list_highlights len = %d, want 1", len(arr))
	}
}

func TestRegisterService(t *testing.T) {
	s := store.NewMemStore()
	h := &handler{store: s}
	call(t, h.handleGraph, map[string]any{
		"action":      "register_service",
		"name":        "ptp4l",
		"description": "PTP boundary clock",
	})
	res := call(t, h.handleGraph, map[string]any{
		"action": "list_services",
	})
	arr := extractArray(t, res)
	if len(arr) != 1 {
		t.Fatalf("list_services len = %d, want 1", len(arr))
	}
}

func TestRegisterCodebase(t *testing.T) {
	s := store.NewMemStore()
	h := &handler{store: s}
	call(t, h.handleGraph, map[string]any{
		"action":    "register_codebase",
		"name":      "linuxptp",
		"repo_url":  "https://github.com/richardcochran/linuxptp",
		"root_path": "/src",
	})
	res := call(t, h.handleGraph, map[string]any{
		"action": "list_codebases",
	})
	arr := extractArray(t, res)
	if len(arr) != 1 {
		t.Fatalf("list_codebases len = %d, want 1", len(arr))
	}
}

func TestRemoveSource(t *testing.T) {
	h, instID, _ := setupWithEvents(t, 0)
	// Add two different sources
	call(t, h.handleIntake, map[string]any{
		"action":      "add_source",
		"instance_id": instID,
		"source":      "a.log",
		"lines":       []string{"2025-01-01T00:00:01Z line-a1", "2025-01-01T00:00:02Z line-a2"},
	})
	call(t, h.handleIntake, map[string]any{
		"action":      "add_source",
		"instance_id": instID,
		"source":      "b.log",
		"lines":       []string{"2025-01-01T00:00:03Z line-b1"},
	})
	// Remove source a.log
	call(t, h.handleIntake, map[string]any{
		"action":      "remove_source",
		"instance_id": instID,
		"source":      "a.log",
	})
	// Verify only b.log remains
	res := call(t, h.handleIntake, map[string]any{
		"action":      "list_sources",
		"instance_id": instID,
	})
	text := resultText(t, res)
	var sources map[string]any
	if err := json.Unmarshal([]byte(text), &sources); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(sources) != 1 {
		t.Fatalf("sources = %d, want 1 (only b.log)", len(sources))
	}
	if _, ok := sources["b.log"]; !ok {
		t.Errorf("expected b.log to remain, got %v", sources)
	}
}

func TestRemoveSource_InvalidInput(t *testing.T) {
	s := store.NewMemStore()
	h := &handler{store: s}
	res := callExpectError(t, h.handleIntake, map[string]any{
		"action":      "remove_source",
		"instance_id": "",
		"source":      "a.log",
	})
	text := res.Text()
	if text == "" {
		t.Fatal("expected error result")
	}
}

func TestAround(t *testing.T) {
	h, _, eventIDs := setupWithEvents(t, 10)
	res := call(t, h.handleQuery, map[string]any{
		"action":   "around",
		"event_id": eventIDs[5],
		"limit":    6,
	})
	arr := extractArray(t, res)
	// radius = 6/2 = 3, so should return events[2..8] = 7 events
	if len(arr) != 7 {
		t.Fatalf("around len = %d, want 7", len(arr))
	}
}

func TestCorrelations(t *testing.T) {
	h, instID, _ := setupWithEvents(t, 0)
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	ctx := context.Background()
	// Add events from different sources at nearby times
	anchorID := "anchor-ev"
	h.store.PutEvent(ctx, &domain.Event{
		ID: anchorID, InstanceID: instID, Timestamp: base,
		TimeConfidence: domain.ConfidenceRFC3339, Message: "anchor event",
		Source: "source-a", SourceHash: "ha1", LineNumber: 1, RawLine: "anchor", CreatedAt: base,
	})
	h.store.PutEvent(ctx, &domain.Event{
		ID: "corr-ev1", InstanceID: instID, Timestamp: base.Add(2 * time.Second),
		TimeConfidence: domain.ConfidenceRFC3339, Message: "correlated event",
		Source: "source-b", SourceHash: "hb1", LineNumber: 1, RawLine: "corr1", CreatedAt: base,
	})
	h.store.PutEvent(ctx, &domain.Event{
		ID: "far-ev1", InstanceID: instID, Timestamp: base.Add(30 * time.Second),
		TimeConfidence: domain.ConfidenceRFC3339, Message: "far event",
		Source: "source-c", SourceHash: "hc1", LineNumber: 1, RawLine: "far", CreatedAt: base,
	})

	res := call(t, h.handleQuery, map[string]any{
		"action":   "correlations",
		"event_id": anchorID,
	})
	text := resultText(t, res)
	var grouped map[string]any
	if err := json.Unmarshal([]byte(text), &grouped); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := grouped["source-b"]; !ok {
		t.Errorf("expected source-b in correlations, got %v", grouped)
	}
	if _, ok := grouped["source-c"]; ok {
		t.Errorf("source-c should not be in 5s window correlations")
	}
}

func TestCorrelations_CustomWindow(t *testing.T) {
	h, instID, _ := setupWithEvents(t, 0)
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	ctx := context.Background()
	anchorID := "anchor-ev"
	h.store.PutEvent(ctx, &domain.Event{
		ID: anchorID, InstanceID: instID, Timestamp: base,
		TimeConfidence: domain.ConfidenceRFC3339, Message: "anchor",
		Source: "a", SourceHash: "ha2", LineNumber: 1, RawLine: "anchor", CreatedAt: base,
	})
	h.store.PutEvent(ctx, &domain.Event{
		ID: "wide-ev", InstanceID: instID, Timestamp: base.Add(15 * time.Second),
		TimeConfidence: domain.ConfidenceRFC3339, Message: "wide",
		Source: "b", SourceHash: "hb2", LineNumber: 1, RawLine: "wide", CreatedAt: base,
	})

	res := call(t, h.handleQuery, map[string]any{
		"action":   "correlations",
		"event_id": anchorID,
		"window":   20,
	})
	text := resultText(t, res)
	var grouped map[string]any
	if err := json.Unmarshal([]byte(text), &grouped); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := grouped["b"]; !ok {
		t.Errorf("expected source b in 20s window, got %v", grouped)
	}
}

func TestMerge(t *testing.T) {
	h, instID, eventIDs := setupWithEvents(t, 3)
	res := call(t, h.handleGraph, map[string]any{
		"action":      "merge",
		"instance_id": instID,
	})
	text := resultText(t, res)
	var result map[string]any
	if err := json.Unmarshal([]byte(text), &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// 3 contains edges + 2 precedes edges = 5
	edges := int(result["edges_created"].(float64))
	if edges != 5 {
		t.Errorf("edges_created = %d, want 5", edges)
	}
	_ = eventIDs
}

func TestMerge_EmptyInstance(t *testing.T) {
	h, instID, _ := setupWithEvents(t, 0)
	res := call(t, h.handleGraph, map[string]any{
		"action":      "merge",
		"instance_id": instID,
	})
	text := resultText(t, res)
	var result map[string]any
	if err := json.Unmarshal([]byte(text), &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if int(result["edges_created"].(float64)) != 0 {
		t.Error("expected 0 edges for empty instance")
	}
}

func TestTraceToCode(t *testing.T) {
	h, _, eventIDs := setupWithEvents(t, 1)
	ctx := context.Background()
	// Add a traces_to edge
	h.store.AddEdge(ctx, domain.Edge{FromID: eventIDs[0], Relation: domain.RelTracesTo, ToID: "code-loc-1"})

	res := call(t, h.handleQuery, map[string]any{
		"action":   "trace_to_code",
		"event_id": eventIDs[0],
	})
	arr := extractArray(t, res)
	if len(arr) != 1 {
		t.Fatalf("trace_to_code len = %d, want 1", len(arr))
	}
}

func TestTraceFromCode(t *testing.T) {
	h, _, eventIDs := setupWithEvents(t, 1)
	ctx := context.Background()
	h.store.AddEdge(ctx, domain.Edge{FromID: "code-loc-1", Relation: domain.RelTracesTo, ToID: eventIDs[0]})

	res := call(t, h.handleQuery, map[string]any{
		"action":   "trace_from_code",
		"event_id": eventIDs[0],
	})
	arr := extractArray(t, res)
	if len(arr) != 1 {
		t.Fatalf("trace_from_code len = %d, want 1", len(arr))
	}
}

func TestCollapse(t *testing.T) {
	h, instID, _ := setupWithEvents(t, 0)
	ctx := context.Background()
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	// Add repeated pattern events
	for i := range 5 {
		ts := base.Add(time.Duration(i) * time.Second)
		h.store.PutEvent(ctx, &domain.Event{
			ID: idStr("col", i), InstanceID: instID, Timestamp: ts,
			TimeConfidence: domain.ConfidenceRFC3339,
			Message:        "offset 3ns s1", Source: "a.log",
			SourceHash: idStr("ch", i), LineNumber: i + 1,
			RawLine: "offset 3ns s1", CreatedAt: ts,
		})
	}
	// Add one unique event
	h.store.PutEvent(ctx, &domain.Event{
		ID: "unique-ev", InstanceID: instID, Timestamp: base.Add(10 * time.Second),
		TimeConfidence: domain.ConfidenceRFC3339,
		Message:        "FREERUN published", Source: "a.log",
		SourceHash: "chu", LineNumber: 100,
		RawLine: "FREERUN published", CreatedAt: base,
	})

	res := call(t, h.handleGraph, map[string]any{
		"action":      "collapse",
		"instance_id": instID,
	})
	arr := extractArray(t, res)
	if len(arr) < 2 {
		t.Fatalf("collapse templates = %d, want >= 2", len(arr))
	}
	// First template should be the repeated one (count 5)
	first := arr[0].(map[string]any)
	if int(first["count"].(float64)) != 5 {
		t.Errorf("first template count = %v, want 5", first["count"])
	}
}

func TestTemplatize_Numbers(t *testing.T) {
	pattern, vars := templatize("offset 3ns delay 42ms")
	if len(vars) < 2 {
		t.Fatalf("expected at least 2 variables, got %d", len(vars))
	}
	if pattern == "offset 3ns delay 42ms" {
		t.Error("pattern should not match original")
	}
}

func TestTemplatize_UUID(t *testing.T) {
	pattern, vars := templatize("request 550e8400-e29b-41d4-a716-446655440000 completed")
	if len(vars) == 0 {
		t.Fatal("expected UUID variable")
	}
	if pattern == "request 550e8400-e29b-41d4-a716-446655440000 completed" {
		t.Error("pattern should replace UUID")
	}
}

func TestTemplatize_Mixed(t *testing.T) {
	pattern, vars := templatize("2025-01-01T00:00:00Z offset 3ns id=550e8400-e29b-41d4-a716-446655440000")
	if len(vars) < 2 {
		t.Fatalf("expected >= 2 variables, got %d", len(vars))
	}
	_ = pattern
}

func TestPurge(t *testing.T) {
	h, instID, _ := setupWithEvents(t, 3)
	// Events have no edges, so all should be purged
	res := call(t, h.handleGraph, map[string]any{
		"action":      "purge",
		"instance_id": instID,
	})
	text := resultText(t, res)
	var result map[string]any
	if err := json.Unmarshal([]byte(text), &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if int(result["purged"].(float64)) != 3 {
		t.Errorf("purged = %v, want 3", result["purged"])
	}
}

func TestPurge_EmptyInstance(t *testing.T) {
	h, instID, _ := setupWithEvents(t, 0)
	res := call(t, h.handleGraph, map[string]any{
		"action":      "purge",
		"instance_id": instID,
	})
	text := resultText(t, res)
	var result map[string]any
	if err := json.Unmarshal([]byte(text), &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if int(result["purged"].(float64)) != 0 {
		t.Errorf("purged = %v, want 0", result["purged"])
	}
}

func TestInstanceDiff(t *testing.T) {
	h, _, _ := setupWithEvents(t, 0)
	ctx := context.Background()
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	instA := createInstance(t, h)
	instB := createInstance(t, h)

	// Same pattern in both
	h.store.PutEvent(ctx, &domain.Event{ID: "da1", InstanceID: instA, Timestamp: base, TimeConfidence: domain.ConfidenceRFC3339, Message: "common offset 3ns", Source: "a.log", SourceHash: "dha1", LineNumber: 1, RawLine: "common", CreatedAt: base})
	h.store.PutEvent(ctx, &domain.Event{ID: "db1", InstanceID: instB, Timestamp: base, TimeConfidence: domain.ConfidenceRFC3339, Message: "common offset 5ns", Source: "a.log", SourceHash: "dhb1", LineNumber: 1, RawLine: "common", CreatedAt: base})
	// Unique to A
	h.store.PutEvent(ctx, &domain.Event{ID: "da2", InstanceID: instA, Timestamp: base.Add(time.Second), TimeConfidence: domain.ConfidenceRFC3339, Message: "only-in-a special", Source: "a.log", SourceHash: "dha2", LineNumber: 2, RawLine: "only-a", CreatedAt: base})

	res := call(t, h.handleDiff, map[string]any{
		"action":     "instance_diff",
		"instance_a": instA,
		"instance_b": instB,
	})
	text := resultText(t, res)
	var diff map[string]any
	if err := json.Unmarshal([]byte(text), &diff); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// Should have hot (only-in-a) and cold (common) categories
	if diff["hot"] == nil && diff["cold"] == nil {
		t.Error("expected non-empty diff result")
	}
}

func TestHotColdMap(t *testing.T) {
	h, _, _ := setupWithEvents(t, 0)
	ctx := context.Background()
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	instA := createInstance(t, h)
	instB := createInstance(t, h)

	h.store.PutEvent(ctx, &domain.Event{ID: "hc1", InstanceID: instA, Timestamp: base, TimeConfidence: domain.ConfidenceRFC3339, Message: "hot-cold test", Source: "a.log", SourceHash: "hhc1", LineNumber: 1, RawLine: "hc", CreatedAt: base})
	h.store.PutEvent(ctx, &domain.Event{ID: "hc2", InstanceID: instB, Timestamp: base, TimeConfidence: domain.ConfidenceRFC3339, Message: "hot-cold test", Source: "a.log", SourceHash: "hhc2", LineNumber: 1, RawLine: "hc", CreatedAt: base})

	res := call(t, h.handleDiff, map[string]any{
		"action":     "hot_cold_map",
		"instance_a": instA,
		"instance_b": instB,
	})
	text := resultText(t, res)
	var diff map[string]any
	if err := json.Unmarshal([]byte(text), &diff); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
}

func TestSessionDiff(t *testing.T) {
	h, _, _ := setupWithEvents(t, 0)
	ctx := context.Background()
	sessID := extractID(t, call(t, h.handleChronolog, map[string]any{
		"action":         "create_session",
		"name":           "test-session",
		"environment_id": "env-placeholder",
	}))
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	inst1 := extractID(t, call(t, h.handleChronolog, map[string]any{
		"action": "create_instance", "name": "inst-1", "session_id": sessID,
	}))
	inst2 := extractID(t, call(t, h.handleChronolog, map[string]any{
		"action": "create_instance", "name": "inst-2", "session_id": sessID,
	}))

	h.store.PutEvent(ctx, &domain.Event{ID: "sd1", InstanceID: inst1, Timestamp: base, TimeConfidence: domain.ConfidenceRFC3339, Message: "session ev 1", Source: "a.log", SourceHash: "hsd1", LineNumber: 1, RawLine: "sd1", CreatedAt: base})
	h.store.PutEvent(ctx, &domain.Event{ID: "sd2", InstanceID: inst2, Timestamp: base.Add(time.Minute), TimeConfidence: domain.ConfidenceRFC3339, Message: "session ev 2", Source: "b.log", SourceHash: "hsd2", LineNumber: 1, RawLine: "sd2", CreatedAt: base})

	res := call(t, h.handleDiff, map[string]any{
		"action":     "session_diff",
		"session_id": sessID,
	})
	arr := extractArray(t, res)
	if len(arr) != 1 {
		t.Fatalf("session_diff len = %d, want 1 (one pair)", len(arr))
	}
}

func TestEnvironmentDiff(t *testing.T) {
	h, _, _ := setupWithEvents(t, 0)
	ctx := context.Background()
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	domID := extractID(t, call(t, h.handleChronolog, map[string]any{"action": "create_domain", "name": "env-diff-test"}))
	envA := extractID(t, call(t, h.handleChronolog, map[string]any{"action": "create_environment", "name": "env-a", "domain_id": domID}))
	envB := extractID(t, call(t, h.handleChronolog, map[string]any{"action": "create_environment", "name": "env-b", "domain_id": domID}))
	sessA := extractID(t, call(t, h.handleChronolog, map[string]any{"action": "create_session", "name": "s-a", "environment_id": envA}))
	sessB := extractID(t, call(t, h.handleChronolog, map[string]any{"action": "create_session", "name": "s-b", "environment_id": envB}))
	instA := extractID(t, call(t, h.handleChronolog, map[string]any{"action": "create_instance", "name": "i-a", "session_id": sessA}))
	instB := extractID(t, call(t, h.handleChronolog, map[string]any{"action": "create_instance", "name": "i-b", "session_id": sessB}))

	h.store.PutEvent(ctx, &domain.Event{ID: "ed1", InstanceID: instA, Timestamp: base, TimeConfidence: domain.ConfidenceRFC3339, Message: "env event shared", Source: "a.log", SourceHash: "hed1", LineNumber: 1, RawLine: "ed1", CreatedAt: base})
	h.store.PutEvent(ctx, &domain.Event{ID: "ed2", InstanceID: instB, Timestamp: base, TimeConfidence: domain.ConfidenceRFC3339, Message: "env event shared", Source: "a.log", SourceHash: "hed2", LineNumber: 1, RawLine: "ed2", CreatedAt: base})

	res := call(t, h.handleDiff, map[string]any{
		"action":        "environment_diff",
		"environment_a": envA,
		"environment_b": envB,
	})
	text := resultText(t, res)
	var diff map[string]any
	if err := json.Unmarshal([]byte(text), &diff); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
}

func TestScalar(t *testing.T) {
	h, instID, _ := setupWithEvents(t, 5)
	res := call(t, h.handleProjection, map[string]any{
		"action":      "scalar",
		"instance_id": instID,
	})
	text := resultText(t, res)
	var result map[string]any
	if err := json.Unmarshal([]byte(text), &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if int(result["value"].(float64)) != 5 {
		t.Errorf("scalar value = %v, want 5", result["value"])
	}
}

func TestVector(t *testing.T) {
	h, _, _ := setupWithEvents(t, 0)
	ctx := context.Background()
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	sessID := extractID(t, call(t, h.handleChronolog, map[string]any{
		"action": "create_session", "name": "vec-session", "environment_id": "env-ph",
	}))
	inst1 := extractID(t, call(t, h.handleChronolog, map[string]any{
		"action": "create_instance", "name": "vec-i1", "session_id": sessID,
	}))
	inst2 := extractID(t, call(t, h.handleChronolog, map[string]any{
		"action": "create_instance", "name": "vec-i2", "session_id": sessID,
	}))

	h.store.PutEvent(ctx, &domain.Event{ID: "v1", InstanceID: inst1, Timestamp: base, TimeConfidence: domain.ConfidenceRFC3339, Message: "v1", Source: "a.log", SourceHash: "hv1", LineNumber: 1, RawLine: "v1", CreatedAt: base})
	h.store.PutEvent(ctx, &domain.Event{ID: "v2", InstanceID: inst1, Timestamp: base.Add(time.Second), TimeConfidence: domain.ConfidenceRFC3339, Message: "v2", Source: "a.log", SourceHash: "hv2", LineNumber: 2, RawLine: "v2", CreatedAt: base})
	h.store.PutEvent(ctx, &domain.Event{ID: "v3", InstanceID: inst2, Timestamp: base, TimeConfidence: domain.ConfidenceRFC3339, Message: "v3", Source: "a.log", SourceHash: "hv3", LineNumber: 1, RawLine: "v3", CreatedAt: base})

	res := call(t, h.handleProjection, map[string]any{
		"action":     "vector",
		"session_id": sessID,
	})
	text := resultText(t, res)
	var result map[string]any
	if err := json.Unmarshal([]byte(text), &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	labels := result["labels"].([]any)
	values := result["values"].([]any)
	if len(labels) != 2 || len(values) != 2 {
		t.Fatalf("vector labels=%d values=%d, want 2 each", len(labels), len(values))
	}
}

func TestHeatmap(t *testing.T) {
	h, _, _ := setupWithEvents(t, 0)
	ctx := context.Background()
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	sessID := extractID(t, call(t, h.handleChronolog, map[string]any{
		"action": "create_session", "name": "heat-session", "environment_id": "env-ph",
	}))
	inst1 := extractID(t, call(t, h.handleChronolog, map[string]any{
		"action": "create_instance", "name": "heat-i1", "session_id": sessID,
	}))

	h.store.PutEvent(ctx, &domain.Event{ID: "hm1", InstanceID: inst1, Timestamp: base, TimeConfidence: domain.ConfidenceRFC3339, Message: "hm1", Source: "a.log", SourceHash: "hhm1", LineNumber: 1, RawLine: "hm1", CreatedAt: base})
	h.store.PutEvent(ctx, &domain.Event{ID: "hm2", InstanceID: inst1, Timestamp: base.Add(time.Second), TimeConfidence: domain.ConfidenceRFC3339, Message: "hm2", Source: "b.log", SourceHash: "hhm2", LineNumber: 1, RawLine: "hm2", CreatedAt: base})

	res := call(t, h.handleProjection, map[string]any{
		"action":     "heatmap",
		"session_id": sessID,
	})
	text := resultText(t, res)
	var result map[string]any
	if err := json.Unmarshal([]byte(text), &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	yLabels := result["y_labels"].([]any)
	if len(yLabels) != 2 {
		t.Errorf("y_labels = %d, want 2 sources", len(yLabels))
	}
}

func TestCube(t *testing.T) {
	h, _, _ := setupWithEvents(t, 0)
	ctx := context.Background()
	base := time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)

	sessID := extractID(t, call(t, h.handleChronolog, map[string]any{
		"action": "create_session", "name": "cube-session", "environment_id": "env-ph",
	}))
	inst1 := extractID(t, call(t, h.handleChronolog, map[string]any{
		"action": "create_instance", "name": "cube-i1", "session_id": sessID,
	}))

	h.store.PutEvent(ctx, &domain.Event{ID: "cu1", InstanceID: inst1, Timestamp: base, TimeConfidence: domain.ConfidenceRFC3339, Message: "cu1", Source: "a.log", SourceHash: "hcu1", LineNumber: 1, RawLine: "cu1", CreatedAt: base})

	res := call(t, h.handleProjection, map[string]any{
		"action":     "cube",
		"session_id": sessID,
	})
	text := resultText(t, res)
	var result map[string]any
	if err := json.Unmarshal([]byte(text), &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	axes := result["axes"].([]any)
	if len(axes) != 3 {
		t.Errorf("axes = %d, want 3", len(axes))
	}
}

func TestExport(t *testing.T) {
	h, instID, _ := setupWithEvents(t, 3)
	res := call(t, h.handleProjection, map[string]any{
		"action":      "export",
		"instance_id": instID,
	})
	text := resultText(t, res)
	var result map[string]any
	if err := json.Unmarshal([]byte(text), &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if result["format"] != "chronolog/export" {
		t.Errorf("format = %v, want chronolog/export", result["format"])
	}
	data := result["data"].(map[string]any)
	if int(data["value"].(float64)) != 3 {
		t.Errorf("export value = %v, want 3", data["value"])
	}
}

func TestE2E_FullPipeline(t *testing.T) { //nolint:funlen // E2E integration test
	s := store.NewMemStore()
	h := &handler{store: s}
	ctx := context.Background()

	// Create cascade
	domID := extractID(t, call(t, h.handleChronolog, map[string]any{"action": "create_domain", "name": "e2e-domain"}))
	envID := extractID(t, call(t, h.handleChronolog, map[string]any{"action": "create_environment", "name": "e2e-env", "domain_id": domID}))
	sessID := extractID(t, call(t, h.handleChronolog, map[string]any{"action": "create_session", "name": "e2e-session", "environment_id": envID}))
	instID := extractID(t, call(t, h.handleChronolog, map[string]any{"action": "create_instance", "name": "e2e-inst", "session_id": sessID}))

	// Intake 3 sources
	call(t, h.handleIntake, map[string]any{
		"action": "add_source", "instance_id": instID, "source": "syslog",
		"lines": []string{"2025-01-01T00:00:01Z offset 3ns", "2025-01-01T00:00:02Z offset 5ns", "2025-01-01T00:00:03Z offset 7ns"},
	})
	call(t, h.handleIntake, map[string]any{
		"action": "add_source", "instance_id": instID, "source": "events.log",
		"lines": []string{"2025-01-01T00:00:01Z FREERUN published", "2025-01-01T00:00:04Z LOCKED achieved"},
	})
	call(t, h.handleIntake, map[string]any{
		"action": "add_source", "instance_id": instID, "source": "cloud.log",
		"lines": []string{"2025-01-01T00:00:02Z cloud sync ok"},
	})

	// Merge
	call(t, h.handleGraph, map[string]any{"action": "merge", "instance_id": instID})

	// Timeline
	tlRes := call(t, h.handleQuery, map[string]any{"action": "timeline", "instance_id": instID})
	tlEvents := extractArray(t, tlRes)
	if len(tlEvents) != 6 {
		t.Fatalf("timeline events = %d, want 6", len(tlEvents))
	}

	// Search
	searchRes := call(t, h.handleQuery, map[string]any{"action": "search", "query": "FREERUN"})
	searchArr := extractArray(t, searchRes)
	if len(searchArr) != 1 {
		t.Fatalf("search FREERUN = %d, want 1", len(searchArr))
	}

	// Around — use first event ID
	firstEvID := tlEvents[0].(map[string]any)["id"].(string)
	call(t, h.handleQuery, map[string]any{"action": "around", "event_id": firstEvID, "limit": 4})

	// Correlations
	call(t, h.handleQuery, map[string]any{"action": "correlations", "event_id": firstEvID})

	// Instance diff — create second instance for comparison
	inst2ID := extractID(t, call(t, h.handleChronolog, map[string]any{"action": "create_instance", "name": "e2e-inst2", "session_id": sessID}))
	call(t, h.handleIntake, map[string]any{
		"action": "add_source", "instance_id": inst2ID, "source": "syslog",
		"lines": []string{"2025-01-01T01:00:01Z offset 10ns", "2025-01-01T01:00:02Z offset 12ns"},
	})
	call(t, h.handleDiff, map[string]any{"action": "instance_diff", "instance_a": instID, "instance_b": inst2ID})

	// Scalar
	scalarRes := call(t, h.handleProjection, map[string]any{"action": "scalar", "instance_id": instID})
	text := resultText(t, scalarRes)
	var scalarResult map[string]any
	if err := json.Unmarshal([]byte(text), &scalarResult); err != nil {
		t.Fatalf("unmarshal scalar: %v", err)
	}
	if int(scalarResult["value"].(float64)) != 6 {
		t.Errorf("scalar = %v, want 6", scalarResult["value"])
	}

	// Vector
	call(t, h.handleProjection, map[string]any{"action": "vector", "session_id": sessID})

	// Collapse
	call(t, h.handleGraph, map[string]any{"action": "collapse", "instance_id": instID})

	// Bookmark
	call(t, h.handleGraph, map[string]any{"action": "add_bookmark", "event_id": firstEvID, "label": "e2e"})

	// Highlight
	call(t, h.handleGraph, map[string]any{"action": "add_highlight", "event_id": firstEvID, "substring": "offset", "label": "metric"})

	// Register service/codebase
	call(t, h.handleGraph, map[string]any{"action": "register_service", "name": "ptp4l"})
	call(t, h.handleGraph, map[string]any{"action": "register_codebase", "name": "linuxptp"})

	// Purge (should be 0 since we merged)
	purgeRes := call(t, h.handleGraph, map[string]any{"action": "purge", "instance_id": instID})
	text = resultText(t, purgeRes)
	var purgeResult map[string]any
	if err := json.Unmarshal([]byte(text), &purgeResult); err != nil {
		t.Fatalf("unmarshal purge: %v", err)
	}
	if int(purgeResult["purged"].(float64)) != 0 {
		t.Errorf("purge after merge = %v, want 0", purgeResult["purged"])
	}

	// Add orphan events and purge them
	base := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	s.PutEvent(ctx, &domain.Event{ID: "orphan1", InstanceID: instID, Timestamp: base, TimeConfidence: domain.ConfidenceRFC3339, Message: "orphan", Source: "x.log", SourceHash: "ho1", LineNumber: 1, RawLine: "orphan", CreatedAt: base})
	s.PutEvent(ctx, &domain.Event{ID: "orphan2", InstanceID: instID, Timestamp: base.Add(time.Second), TimeConfidence: domain.ConfidenceRFC3339, Message: "orphan2", Source: "x.log", SourceHash: "ho2", LineNumber: 2, RawLine: "orphan2", CreatedAt: base})

	purgeRes2 := call(t, h.handleGraph, map[string]any{"action": "purge", "instance_id": instID})
	text = resultText(t, purgeRes2)
	var purgeResult2 map[string]any
	if err := json.Unmarshal([]byte(text), &purgeResult2); err != nil {
		t.Fatalf("unmarshal purge2: %v", err)
	}
	if int(purgeResult2["purged"].(float64)) != 2 {
		t.Errorf("purge orphans = %v, want 2", purgeResult2["purged"])
	}
}

// --- test helpers ---

func setupWithEvents(t *testing.T, n int) (h *handler, instID string, eventIDs []string) {
	t.Helper()
	s := store.NewMemStore()
	h = &handler{store: s}
	ctx := context.Background()
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	instID = "test-instance"
	s.PutInstance(ctx, &domain.Instance{ID: instID, SessionID: "test-session", Name: "test", StartedAt: base})

	eventIDs = make([]string, n)
	for i := range n {
		eid := idStr("ev", i)
		eventIDs[i] = eid
		ts := base.Add(time.Duration(i) * time.Second)
		s.PutEvent(ctx, &domain.Event{
			ID: eid, InstanceID: instID, Timestamp: ts,
			TimeConfidence: domain.ConfidenceRFC3339,
			Message:        idStr("msg-", i), Source: "a.log",
			SourceHash: idStr("h", i), LineNumber: i + 1,
			RawLine: idStr("raw-", i), CreatedAt: ts,
		})
	}
	return h, instID, eventIDs
}

func createInstance(t *testing.T, h *handler) string {
	t.Helper()
	res := call(t, h.handleChronolog, map[string]any{
		"action":     "create_instance",
		"name":       "diff-inst",
		"session_id": "test-session",
	})
	return extractID(t, res)
}

func idStr(prefix string, i int) string {
	return prefix + string(rune('0'+i))
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

func callExpectError(t *testing.T, fn func(context.Context, json.RawMessage) (tool.Result, error), input map[string]any) tool.Result {
	t.Helper()
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("marshal input: %v", err)
	}
	res, err := fn(context.Background(), raw)
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if !res.IsError {
		t.Fatalf("expected error result, got success: %s", res.Text())
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
