package mcp

import (
	"strings"
	"testing"

	"github.com/dpopsuev/chronolog/internal/store"
)

func TestForensicInvestigationWorkflow(t *testing.T) { //nolint:funlen // e2e integration test spanning full forensic pipeline
	s := store.NewMemStore()
	h := &handler{store: s}

	// ---- Setup: build cascade hierarchy ----
	// Domain
	domRes := call(t, h.handleChronolog, map[string]any{
		"action": "create_domain", "name": "PTP Investigation",
	})
	domID := extractID(t, domRes)

	// Environment
	envRes := call(t, h.handleChronolog, map[string]any{
		"action": "create_environment", "name": "4.16", "domain_id": domID,
	})
	envID := extractID(t, envRes)

	// Session 1 (healthy baseline)
	sess1Res := call(t, h.handleChronolog, map[string]any{
		"action": "create_session", "name": "baseline-run", "environment_id": envID,
	})
	sess1ID := extractID(t, sess1Res)

	// Session 2 (defective)
	sess2Res := call(t, h.handleChronolog, map[string]any{
		"action": "create_session", "name": "defect-run", "environment_id": envID,
	})
	sess2ID := extractID(t, sess2Res)

	// Instances
	inst1Res := call(t, h.handleChronolog, map[string]any{
		"action": "create_instance", "name": "inst-healthy", "session_id": sess1ID,
	})
	inst1ID := extractID(t, inst1Res)

	inst2Res := call(t, h.handleChronolog, map[string]any{
		"action": "create_instance", "name": "inst-defect", "session_id": sess2ID,
	})
	inst2ID := extractID(t, inst2Res)

	// ---- Ingest logs ----
	call(t, h.handleIntake, map[string]any{
		"action": "add_source", "instance_id": inst1ID, "source": "syslog",
		"lines": []string{
			"2025-01-01T00:00:01Z clock sync OK offset 3ns",
			"2025-01-01T00:00:02Z clock sync OK offset 5ns",
			"2025-01-01T00:00:03Z clock sync OK offset 2ns",
		},
	})
	call(t, h.handleIntake, map[string]any{
		"action": "add_source", "instance_id": inst2ID, "source": "syslog",
		"lines": []string{
			"2025-01-01T01:00:01Z clock sync OK offset 3ns",
			"2025-01-01T01:00:02Z clock sync OK offset 5ns",
			"2025-01-01T01:00:03Z CLOCK_UNSYNC offset exceeded threshold",
			"2025-01-01T01:00:04Z CLOCK_UNSYNC recovery failed",
		},
	})
	call(t, h.handleIntake, map[string]any{
		"action": "add_source", "instance_id": inst2ID, "source": "events.log",
		"lines": []string{
			"2025-01-01T01:00:02Z ptp4l started",
			"2025-01-01T01:00:03Z ptp4l FREERUN detected",
		},
	})

	// Merge both instances
	call(t, h.handleGraph, map[string]any{"action": "merge", "instance_id": inst1ID})
	call(t, h.handleGraph, map[string]any{"action": "merge", "instance_id": inst2ID})

	// Get event IDs from defect instance for labeling
	timelineRes := call(t, h.handleQuery, map[string]any{
		"action": "timeline", "instance_id": inst2ID, "limit": 10,
	})
	events := extractArray(t, timelineRes)
	if len(events) != 6 {
		t.Fatalf("expected 6 events in defect instance, got %d", len(events))
	}
	defectEventID := events[2].(map[string]any)["id"].(string)
	_ = defectEventID

	// ---- Phase 1: Phases ----
	t.Run("create_phase", func(t *testing.T) {
		res := call(t, h.handleChronolog, map[string]any{
			"action": "create_phase", "instance_id": inst2ID, "name": "during_test",
		})
		phaseID := extractID(t, res)
		if phaseID == "" {
			t.Fatal("expected non-empty phase ID")
		}
	})

	t.Run("list_phases", func(t *testing.T) {
		res := call(t, h.handleChronolog, map[string]any{
			"action": "list_phases", "instance_id": inst2ID,
		})
		phases := extractArray(t, res)
		if len(phases) != 1 {
			t.Fatalf("expected 1 phase, got %d", len(phases))
		}
	})

	// ---- Phase 2: Labels ----
	t.Run("label_event", func(t *testing.T) {
		call(t, h.handleGraph, map[string]any{
			"action": "label_event", "event_id": defectEventID, "key": "category", "value": "smoking_gun",
		})
	})

	t.Run("list_labels", func(t *testing.T) {
		res := call(t, h.handleGraph, map[string]any{
			"action": "list_labels", "event_id": defectEventID,
		})
		text := resultText(t, res)
		if !strings.Contains(text, "smoking_gun") {
			t.Fatalf("expected smoking_gun label, got %s", text)
		}
	})

	t.Run("unlabel_event", func(t *testing.T) {
		call(t, h.handleGraph, map[string]any{
			"action": "unlabel_event", "event_id": defectEventID, "key": "category",
		})
		res := call(t, h.handleGraph, map[string]any{
			"action": "list_labels", "event_id": defectEventID,
		})
		text := resultText(t, res)
		if strings.Contains(text, "smoking_gun") {
			t.Fatal("expected smoking_gun label removed after unlabel")
		}
	})

	// Re-label for subsequent tests
	call(t, h.handleGraph, map[string]any{
		"action": "label_event", "event_id": defectEventID, "key": "category", "value": "smoking_gun",
	})

	// ---- Phase 3: Label/Bookmark queries ----
	t.Run("search_by_label", func(t *testing.T) {
		call(t, h.handleQuery, map[string]any{
			"action": "search_by_label", "key": "category", "value": "smoking_gun", "instance_id": inst2ID,
		})
	})

	t.Run("search_by_bookmark", func(t *testing.T) {
		call(t, h.handleQuery, map[string]any{
			"action": "search_by_bookmark", "label": "suspicious", "instance_id": inst2ID,
		})
	})

	// ---- Phase 4: Forensic analysis ----
	t.Run("suspects", func(t *testing.T) {
		call(t, h.handleQuery, map[string]any{
			"action": "suspects", "key": "category", "value": "symptom", "instance_id": inst2ID,
		})
	})

	t.Run("time_of_defect", func(t *testing.T) {
		call(t, h.handleQuery, map[string]any{
			"action": "time_of_defect", "pattern": "CLOCK_UNSYNC", "instance_id": inst2ID,
		})
	})

	t.Run("recurrence", func(t *testing.T) {
		call(t, h.handleQuery, map[string]any{
			"action": "recurrence", "pattern": "CLOCK_UNSYNC", "environment_id": envID,
		})
	})

	// ---- Phase 5: Buckets ----
	t.Run("create_bucket", func(t *testing.T) {
		call(t, h.handleChronolog, map[string]any{
			"action": "create_bucket", "name": "clock-issues", "query": "CLOCK_UNSYNC",
		})
	})

	t.Run("list_buckets", func(t *testing.T) {
		call(t, h.handleChronolog, map[string]any{
			"action": "list_buckets",
		})
	})

	// ---- Phase 6: Case lifecycle ----
	t.Run("open_case", func(t *testing.T) {
		call(t, h.handleChronolog, map[string]any{
			"action": "open_case", "title": "Clock sync regression",
		})
	})

	// ---- Phase 7: Immutability ----
	t.Run("set_immutable", func(t *testing.T) {
		call(t, h.handleChronolog, map[string]any{
			"action": "set_immutable", "instance_id": inst2ID,
		})
	})

	t.Run("verify_integrity", func(t *testing.T) {
		call(t, h.handleChronolog, map[string]any{
			"action": "verify_integrity", "instance_id": inst2ID,
		})
	})

	// ---- Phase 8: Regression gate ----
	t.Run("regression_check", func(t *testing.T) {
		call(t, h.handleDiff, map[string]any{
			"action": "regression_check", "session_id": sess2ID, "baseline_session_id": sess1ID,
		})
	})
}
