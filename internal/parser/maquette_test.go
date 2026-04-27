package parser

import (
	"testing"
	"time"

	"github.com/dpopsuev/chronolog/internal/domain"
)

func TestParseWithMaquette_SyslogTimestamp(t *testing.T) {
	m := &domain.Maquette{
		Timestamp: &domain.MaquetteTimestamp{
			Regex:  `^(\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})`,
			Format: "Jan 2 15:04:05",
		},
	}
	cm, err := Compile(m)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	line := "Dec 20 04:57:41 helix73 systemd[1]: Starting Kubernetes"
	result := ParseWithMaquette(line, cm)
	if result.TimeConfidence != domain.ConfidenceMaquette {
		t.Errorf("confidence = %q, want %q", result.TimeConfidence, domain.ConfidenceMaquette)
	}
	if result.Timestamp.Month() != time.December {
		t.Errorf("month = %v, want December", result.Timestamp.Month())
	}
	if result.Timestamp.Day() != 20 {
		t.Errorf("day = %d, want 20", result.Timestamp.Day())
	}
	if result.Timestamp.Hour() != 4 {
		t.Errorf("hour = %d, want 4", result.Timestamp.Hour())
	}
}

func TestParseWithMaquette_NilFallsBackToRFC3339(t *testing.T) {
	line := "2025-12-20T06:56:37Z some log message"
	result := ParseWithMaquette(line, nil)
	if result.TimeConfidence != domain.ConfidenceRFC3339 {
		t.Errorf("confidence = %q, want %q", result.TimeConfidence, domain.ConfidenceRFC3339)
	}
	if result.Timestamp.Year() != 2025 {
		t.Errorf("year = %d, want 2025", result.Timestamp.Year())
	}
}

func TestParseWithMaquette_TimestampFailsFallsBack(t *testing.T) {
	m := &domain.Maquette{
		Timestamp: &domain.MaquetteTimestamp{
			Regex:  `^(\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})`,
			Format: "Jan 2 15:04:05",
		},
	}
	cm, err := Compile(m)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	line := "2025-12-20T06:56:37Z this line has RFC3339 not syslog"
	result := ParseWithMaquette(line, cm)
	if result.TimeConfidence != domain.ConfidenceRFC3339 {
		t.Errorf("confidence = %q, want %q", result.TimeConfidence, domain.ConfidenceRFC3339)
	}
}

func TestParseWithMaquette_SourceExtraction(t *testing.T) {
	m := &domain.Maquette{
		Source: &domain.MaquetteSource{
			Regex: `\w+\s+\d+\s+\d{2}:\d{2}:\d{2}\s+\S+\s+(?P<source>\S+?)[\[:]`,
		},
	}
	cm, err := Compile(m)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	line := "Dec 20 04:57:41 helix73 systemd[1]: Starting Kubernetes"
	result := ParseWithMaquette(line, cm)
	if result.Labels[domain.LabelLogSource] != "systemd" {
		t.Errorf("log_source = %q, want systemd", result.Labels[domain.LabelLogSource])
	}
}

func TestParseWithMaquette_SeverityExtraction(t *testing.T) {
	m := &domain.Maquette{
		Severity: &domain.MaquetteSeverity{
			Keywords: map[string]string{
				"ERROR":   "error",
				"WARNING": "warning",
				"INFO":    "info",
			},
		},
	}
	cm, err := Compile(m)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	line := "2025-01-01T00:00:01Z ERROR something went wrong"
	result := ParseWithMaquette(line, cm)
	if result.Labels[domain.LabelSeverity] != "error" {
		t.Errorf("severity = %q, want error", result.Labels[domain.LabelSeverity])
	}
}

func TestParseWithMaquette_SeverityNotFound(t *testing.T) {
	m := &domain.Maquette{
		Severity: &domain.MaquetteSeverity{
			Keywords: map[string]string{"FATAL": "fatal"},
		},
	}
	cm, err := Compile(m)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	line := "2025-01-01T00:00:01Z just a normal line"
	result := ParseWithMaquette(line, cm)
	if _, ok := result.Labels[domain.LabelSeverity]; ok {
		t.Errorf("expected no severity label, got %q", result.Labels[domain.LabelSeverity])
	}
}

func TestCompile_InvalidRegex(t *testing.T) {
	m := &domain.Maquette{
		Timestamp: &domain.MaquetteTimestamp{
			Regex:  `(unclosed`,
			Format: "Jan 2 15:04:05",
		},
	}
	_, err := Compile(m)
	if err == nil {
		t.Fatal("expected error for invalid regex")
	}
}

func TestCompile_InvalidSourceRegex(t *testing.T) {
	m := &domain.Maquette{
		Source: &domain.MaquetteSource{
			Regex: `(unclosed`,
		},
	}
	_, err := Compile(m)
	if err == nil {
		t.Fatal("expected error for invalid source regex")
	}
}

func TestCompile_Nil(t *testing.T) {
	cm, err := Compile(nil)
	if err != nil {
		t.Fatalf("Compile(nil) error: %v", err)
	}
	if cm != nil {
		t.Fatal("expected nil CompiledMaquette for nil input")
	}
}

func TestParseWithMaquette_DmesgTimestamp(t *testing.T) {
	m := &domain.Maquette{
		Timestamp: &domain.MaquetteTimestamp{
			Regex:  `\[\s*(\d+\.\d+)\]`,
			Format: "",
		},
	}
	cm, err := Compile(m)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	line := "[    5.123456] Linux version 5.14.0"
	result := ParseWithMaquette(line, cm)
	if result.TimeConfidence != domain.ConfidenceMaquette {
		t.Errorf("confidence = %q, want maquette", result.TimeConfidence)
	}
	if result.Timestamp.IsZero() {
		t.Error("expected non-zero timestamp for dmesg")
	}
}

func TestParseWithMaquette_JournalctlTimestamp(t *testing.T) {
	m := &domain.Maquette{
		Timestamp: &domain.MaquetteTimestamp{
			Regex:  `^(\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})`,
			Format: "Jan 2 15:04:05",
		},
		Source: &domain.MaquetteSource{
			Regex: `\w+\s+\d+\s+\d{2}:\d{2}:\d{2}\s+\S+\s+(?P<source>\S+?)[\[:]`,
		},
		Severity: &domain.MaquetteSeverity{
			Keywords: map[string]string{
				"Failed": "error",
				"error":  "error",
			},
		},
	}
	cm, err := Compile(m)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	line := "Apr 27 18:11:03 myhost earlyoom[1636329]: Failed with result 'exit-code'."
	result := ParseWithMaquette(line, cm)
	if result.TimeConfidence != domain.ConfidenceMaquette {
		t.Errorf("confidence = %q, want maquette", result.TimeConfidence)
	}
	if result.Timestamp.Month() != time.April {
		t.Errorf("month = %v, want April", result.Timestamp.Month())
	}
	if result.Labels[domain.LabelLogSource] != "earlyoom" {
		t.Errorf("log_source = %q, want earlyoom", result.Labels[domain.LabelLogSource])
	}
	if result.Labels[domain.LabelSeverity] != "error" {
		t.Errorf("severity = %q, want error", result.Labels[domain.LabelSeverity])
	}
}

func TestParseWithMaquette_AllFieldsCombined(t *testing.T) {
	m := &domain.Maquette{
		Timestamp: &domain.MaquetteTimestamp{
			Regex:  `^(\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2})`,
			Format: "Jan 2 15:04:05",
		},
		Source: &domain.MaquetteSource{
			Regex: `\w+\s+\d+\s+\d{2}:\d{2}:\d{2}\s+\S+\s+(?P<source>\S+?)[\[:]`,
		},
		Severity: &domain.MaquetteSeverity{
			Keywords: map[string]string{"WARNING": "warning"},
		},
	}
	cm, err := Compile(m)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	line := "Dec 20 04:57:41 helix73 kubelet[999]: WARNING Node not ready"
	result := ParseWithMaquette(line, cm)
	if result.TimeConfidence != domain.ConfidenceMaquette {
		t.Errorf("confidence = %q, want maquette", result.TimeConfidence)
	}
	if result.Labels[domain.LabelLogSource] != "kubelet" {
		t.Errorf("log_source = %q, want kubelet", result.Labels[domain.LabelLogSource])
	}
	if result.Labels[domain.LabelSeverity] != "warning" {
		t.Errorf("severity = %q, want warning", result.Labels[domain.LabelSeverity])
	}
}
