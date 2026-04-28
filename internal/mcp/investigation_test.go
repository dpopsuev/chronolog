package mcp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dpopsuev/chronolog/internal/domain"
	"github.com/dpopsuev/chronolog/internal/store"
)

func TestForensicInvestigationWorkflow(t *testing.T) { //nolint:funlen,gocyclo // e2e integration test spanning full forensic pipeline
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
		res := call(t, h.handleQuery, map[string]any{
			"action": "search_by_label", "key": "category", "value": "smoking_gun", "instance_id": inst2ID,
		})
		found := extractArray(t, res)
		if len(found) != 1 {
			t.Fatalf("expected 1 labeled event, got %d", len(found))
		}
	})

	t.Run("search_by_bookmark", func(t *testing.T) {
		// Add a bookmark first
		call(t, h.handleGraph, map[string]any{
			"action": "add_bookmark", "event_id": defectEventID, "label": "suspicious", "note": "timing anomaly",
		})
		res := call(t, h.handleQuery, map[string]any{
			"action": "search_by_bookmark", "label": "suspicious", "instance_id": inst2ID,
		})
		found := extractArray(t, res)
		if len(found) != 1 {
			t.Fatalf("expected 1 bookmarked event, got %d", len(found))
		}
	})

	// ---- Phase 4: Forensic analysis ----
	t.Run("suspects", func(t *testing.T) {
		res := call(t, h.handleQuery, map[string]any{
			"action": "suspects", "key": "category", "value": "smoking_gun", "instance_id": inst2ID, "window": 10,
		})
		found := extractArray(t, res)
		if len(found) == 0 {
			t.Fatal("expected at least one suspect source")
		}
	})

	t.Run("time_of_defect", func(t *testing.T) {
		res := call(t, h.handleQuery, map[string]any{
			"action": "time_of_defect", "pattern": "CLOCK_UNSYNC", "instance_id": inst2ID,
		})
		text := resultText(t, res)
		if !strings.Contains(text, "last_healthy") {
			t.Fatalf("expected last_healthy in result, got %s", text)
		}
		if !strings.Contains(text, "first_defect") {
			t.Fatalf("expected first_defect in result, got %s", text)
		}
	})

	t.Run("recurrence", func(t *testing.T) {
		res := call(t, h.handleQuery, map[string]any{
			"action": "recurrence", "pattern": "CLOCK_UNSYNC", "environment_id": envID,
		})
		text := resultText(t, res)
		if !strings.Contains(text, "present_sessions") {
			t.Fatalf("expected present_sessions in result, got %s", text)
		}
	})

	// ---- Phase 5: Buckets ----
	var bucketID string
	t.Run("create_bucket", func(t *testing.T) {
		res := call(t, h.handleChronolog, map[string]any{
			"action": "create_bucket", "name": "clock-issues", "query": "CLOCK_UNSYNC",
		})
		bucketID = extractID(t, res)
		if bucketID == "" {
			t.Fatal("expected non-empty bucket ID")
		}
	})

	t.Run("list_buckets", func(t *testing.T) {
		res := call(t, h.handleChronolog, map[string]any{
			"action": "list_buckets",
		})
		buckets := extractArray(t, res)
		if len(buckets) != 1 {
			t.Fatalf("expected 1 bucket, got %d", len(buckets))
		}
	})

	// ---- Phase 6: Case lifecycle ----
	var caseID string
	t.Run("open_case", func(t *testing.T) {
		res := call(t, h.handleCase, map[string]any{
			"action": "open_case", "title": "Clock sync regression",
		})
		caseID = extractID(t, res)
	})

	t.Run("add_symptom", func(t *testing.T) {
		call(t, h.handleCase, map[string]any{
			"action": "add_symptom", "case_id": caseID, "description": "CLOCK_UNSYNC errors in syslog", "event_id": defectEventID,
		})
	})

	t.Run("append_transcript", func(t *testing.T) {
		call(t, h.handleCase, map[string]any{
			"action": "append_transcript", "case_id": caseID, "content": "Ran suspects — syslog correlates",
		})
	})

	t.Run("append_replayable_transcript", func(t *testing.T) {
		call(t, h.handleCase, map[string]any{
			"action":      "append_transcript",
			"case_id":     caseID,
			"content":     "Ran time_of_defect on CLOCK_UNSYNC",
			"tool":        "query",
			"tool_action": "time_of_defect",
			"params":      `{"action":"time_of_defect","pattern":"CLOCK_UNSYNC","instance_id":"` + inst2ID + `"}`,
		})
	})

	t.Run("replay_transcript", func(t *testing.T) {
		res := call(t, h.handleCase, map[string]any{
			"action": "replay_transcript", "case_id": caseID,
		})
		text := resultText(t, res)
		if !strings.Contains(text, "reproducible") {
			t.Fatalf("expected reproducible field, got %s", text)
		}
	})

	t.Run("set_root_cause", func(t *testing.T) {
		call(t, h.handleCase, map[string]any{
			"action": "set_root_cause", "case_id": caseID, "description": "Clock sync refactor removed mutex", "event_id": defectEventID,
		})
	})

	t.Run("close_case", func(t *testing.T) {
		res := call(t, h.handleCase, map[string]any{
			"action": "close_case", "case_id": caseID,
		})
		text := resultText(t, res)
		if !strings.Contains(text, "closed") {
			t.Fatalf("expected closed status, got %s", text)
		}
	})

	t.Run("get_case", func(t *testing.T) {
		res := call(t, h.handleCase, map[string]any{
			"action": "get_case", "case_id": caseID,
		})
		text := resultText(t, res)
		if !strings.Contains(text, "CLOCK_UNSYNC") || !strings.Contains(text, "Clock sync refactor") {
			t.Fatalf("expected full case with symptoms and root cause, got %s", text)
		}
	})

	// ---- Phase 7: Immutability ----
	t.Run("set_immutable", func(t *testing.T) {
		res := call(t, h.handleChronolog, map[string]any{
			"action": "set_immutable", "instance_id": inst2ID,
		})
		text := resultText(t, res)
		if !strings.Contains(text, `"immutable": true`) {
			t.Fatalf("expected immutable=true, got %s", text)
		}
	})

	t.Run("immutable_blocks_remove", func(t *testing.T) {
		callExpectError(t, h.handleIntake, map[string]any{
			"action": "remove_source", "instance_id": inst2ID, "source": "syslog",
		})
	})

	t.Run("verify_integrity", func(t *testing.T) {
		res := call(t, h.handleChronolog, map[string]any{
			"action": "verify_integrity", "instance_id": inst2ID,
		})
		text := resultText(t, res)
		if !strings.Contains(text, `"valid": true`) {
			t.Fatalf("expected valid=true, got %s", text)
		}
	})

	// ---- Phase 8: Regression gate ----
	t.Run("regression_check", func(t *testing.T) {
		res := call(t, h.handleDiff, map[string]any{
			"action": "regression_check", "session_id": sess2ID, "baseline_session_id": sess1ID,
		})
		text := resultText(t, res)
		if !strings.Contains(text, `"verdict"`) {
			t.Fatalf("expected verdict in result, got %s", text)
		}
	})
}

func TestTestMaquette(t *testing.T) {
	s := store.NewMemStore()
	h := &handler{store: s}

	maq := &domain.Maquette{
		Timestamp: &domain.MaquetteTimestamp{
			Regex:  `(\w{3}\s+\d+\s+\d{2}:\d{2}:\d{2})`,
			Format: "Jan  2 15:04:05",
		},
		Severity: &domain.MaquetteSeverity{
			Keywords: map[string]string{"error": "error", "warn": "warning", "info": "info"},
		},
	}

	t.Run("happy_path", func(t *testing.T) {
		res := call(t, h.handleIntake, map[string]any{
			"action":   "test_maquette",
			"maquette": maq,
			"lines":    []string{"Jan  5 14:23:01 myhost syslogd: info message", "Jan  5 14:23:02 myhost kernel: error something broke"},
		})
		arr := extractArray(t, res)
		if len(arr) != 2 {
			t.Fatalf("expected 2 results, got %d", len(arr))
		}
		text := resultText(t, res)
		if !strings.Contains(text, "maquette") {
			t.Fatalf("expected time_confidence=maquette, got %s", text)
		}
	})

	t.Run("bad_regex", func(t *testing.T) {
		callExpectError(t, h.handleIntake, map[string]any{
			"action":   "test_maquette",
			"maquette": &domain.Maquette{Timestamp: &domain.MaquetteTimestamp{Regex: "[invalid"}},
			"lines":    []string{"test"},
		})
	})

	t.Run("no_maquette", func(t *testing.T) {
		callExpectError(t, h.handleIntake, map[string]any{
			"action": "test_maquette",
			"lines":  []string{"test"},
		})
	})
}

func TestAddSourceFilePath(t *testing.T) {
	s := store.NewMemStore()
	h := &handler{store: s}

	call(t, h.handleChronolog, map[string]any{"action": "create_domain", "name": "test"})
	call(t, h.handleChronolog, map[string]any{"action": "create_environment", "name": "test", "domain_id": "need-real-id"})

	ctx := t
	_ = ctx

	instID := setupInstance(t, h)

	tmpFile := filepath.Join(t.TempDir(), "test.log")
	os.WriteFile(tmpFile, []byte("2025-01-01T00:00:01Z line one\n2025-01-01T00:00:02Z line two\n2025-01-01T00:00:03Z line three\n"), 0o644)

	t.Run("ingest_from_file", func(t *testing.T) {
		res := call(t, h.handleIntake, map[string]any{
			"action": "add_source", "instance_id": instID, "source": "file.log", "file_path": tmpFile,
		})
		text := resultText(t, res)
		if !strings.Contains(text, `"added": 3`) {
			t.Fatalf("expected 3 lines ingested, got %s", text)
		}
	})

	t.Run("path_traversal_rejected", func(t *testing.T) {
		callExpectError(t, h.handleIntake, map[string]any{
			"action": "add_source", "instance_id": instID, "source": "bad", "file_path": "/etc/../etc/passwd",
		})
	})

	t.Run("nonexistent_file", func(t *testing.T) {
		callExpectError(t, h.handleIntake, map[string]any{
			"action": "add_source", "instance_id": instID, "source": "nope", "file_path": "/tmp/nonexistent-chronolog-test-file",
		})
	})
}

func TestListSourcesProvenance(t *testing.T) {
	s := store.NewMemStore()
	h := &handler{store: s}
	instID := setupInstance(t, h)

	call(t, h.handleIntake, map[string]any{
		"action": "add_source", "instance_id": instID, "source": "kernel",
		"lines":     []string{"2025-01-01T00:00:01Z test line"},
		"collector": "journalctl -k", "file_hash": "sha256:abc123",
	})

	res := call(t, h.handleIntake, map[string]any{
		"action": "list_sources", "instance_id": instID,
	})
	text := resultText(t, res)
	if !strings.Contains(text, "journalctl -k") {
		t.Fatalf("expected collector in list_sources, got %s", text)
	}
	if !strings.Contains(text, "sha256:abc123") {
		t.Fatalf("expected file_hash in list_sources, got %s", text)
	}
}

func setupInstance(t *testing.T, h *handler) string {
	t.Helper()
	s := h.store
	ctx := t
	_ = ctx
	domRes := call(t, h.handleChronolog, map[string]any{"action": "create_domain", "name": "fp-test"})
	domID := extractID(t, domRes)
	envRes := call(t, h.handleChronolog, map[string]any{"action": "create_environment", "name": "fp-env", "domain_id": domID})
	envID := extractID(t, envRes)
	sessRes := call(t, h.handleChronolog, map[string]any{"action": "create_session", "name": "fp-sess", "environment_id": envID})
	sessID := extractID(t, sessRes)
	instRes := call(t, h.handleChronolog, map[string]any{"action": "create_instance", "name": "fp-inst", "session_id": sessID})
	_ = s
	return extractID(t, instRes)
}

func TestAliasResolution(t *testing.T) {
	s := store.NewMemStore()
	h := &handler{store: s}

	// Create domain → get UUID
	domRes := call(t, h.handleChronolog, map[string]any{"action": "create_domain", "name": "alias-domain"})
	domID := extractID(t, domRes)

	// Create environment using the UUID (normal flow)
	call(t, h.handleChronolog, map[string]any{"action": "create_environment", "name": "alias-env", "domain_id": domID})

	// Now use the NAME to list environments — should resolve alias → UUID
	t.Run("list_environments_by_name", func(t *testing.T) {
		res := call(t, h.handleChronolog, map[string]any{
			"action": "list_environments", "domain_id": "alias-domain",
		})
		envs := extractArray(t, res)
		if len(envs) != 1 {
			t.Fatalf("expected 1 environment via alias, got %d", len(envs))
		}
	})
}

func TestQuickIntake(t *testing.T) {
	s := store.NewMemStore()
	h := &handler{store: s}

	tmpFile := filepath.Join(t.TempDir(), "quick.log")
	os.WriteFile(tmpFile, []byte("2025-01-01T00:00:01Z line one\n2025-01-01T00:00:02Z line two\n"), 0o644)

	res := call(t, h.handleIntake, map[string]any{
		"action": "quick_intake",
		"name":   "quick-test",
		"sources": []map[string]any{
			{"source": "testlog", "file_path": tmpFile},
		},
	})
	text := resultText(t, res)
	if !strings.Contains(text, "instance_id") {
		t.Fatalf("expected instance_id in result, got %s", text)
	}
	if !strings.Contains(text, `"events": 2`) {
		t.Fatalf("expected 2 events, got %s", text)
	}

	// Verify we can query by the alias
	call(t, h.handleQuery, map[string]any{
		"action": "timeline", "instance_id": "quick-test", "limit": 10,
	})
}

func TestCommandPiping(t *testing.T) {
	s := store.NewMemStore()
	h := &handler{store: s}
	instID := setupInstance(t, h)

	res := call(t, h.handleIntake, map[string]any{
		"action": "add_source", "instance_id": instID, "source": "echo-test",
		"command": "echo '2025-01-01T00:00:01Z piped line one' && echo '2025-01-01T00:00:02Z piped line two'",
	})
	text := resultText(t, res)
	if !strings.Contains(text, `"added": 2`) {
		t.Fatalf("expected 2 lines from command, got %s", text)
	}
}
