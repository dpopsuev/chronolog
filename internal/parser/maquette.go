package parser

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/dpopsuev/chronolog/internal/domain"
)

// ParseResult holds all fields extracted from a line by a maquette.
type ParseResult struct {
	Timestamp      time.Time
	TimeConfidence string
	Labels         map[string]string
}

// CompiledMaquette holds pre-compiled regexes for repeated use across lines.
type CompiledMaquette struct {
	tsRegex    *regexp.Regexp
	tsFormat   string
	srcRegex   *regexp.Regexp
	srcNameIdx int
	sevWords   map[string]string
}

// Compile prepares a Maquette for repeated use. Returns nil for nil input.
func Compile(m *domain.Maquette) (*CompiledMaquette, error) {
	if m == nil {
		return nil, nil
	}
	cm := &CompiledMaquette{}
	if m.Timestamp != nil && m.Timestamp.Regex != "" {
		r, err := regexp.Compile(m.Timestamp.Regex)
		if err != nil {
			return nil, fmt.Errorf("timestamp regex: %w", err)
		}
		cm.tsRegex = r
		cm.tsFormat = m.Timestamp.Format
	}
	if m.Source != nil && m.Source.Regex != "" {
		r, err := regexp.Compile(m.Source.Regex)
		if err != nil {
			return nil, fmt.Errorf("source regex: %w", err)
		}
		cm.srcRegex = r
		cm.srcNameIdx = -1
		for i, name := range r.SubexpNames() {
			if name == "source" {
				cm.srcNameIdx = i
				break
			}
		}
	}
	if m.Severity != nil {
		cm.sevWords = m.Severity.Keywords
	}
	return cm, nil
}

// ParseWithMaquette extracts timestamp, source, and severity from a line.
// If cm is nil, falls back to RFC3339-only Parse().
func ParseWithMaquette(line string, cm *CompiledMaquette) ParseResult {
	result := ParseResult{Labels: make(map[string]string)}

	if cm == nil {
		result.Timestamp, result.TimeConfidence = Parse(line)
		return result
	}

	if cm.tsRegex != nil {
		if ts, ok := tryMaquetteTimestamp(line, cm); ok {
			result.Timestamp = ts
			result.TimeConfidence = domain.ConfidenceMaquette
		}
	}
	if result.TimeConfidence == "" {
		result.Timestamp, result.TimeConfidence = Parse(line)
	}

	if cm.srcRegex != nil {
		if src := extractSource(line, cm); src != "" {
			result.Labels[domain.LabelLogSource] = src
		}
	}
	if sev := extractSeverity(line, cm); sev != "" {
		result.Labels[domain.LabelSeverity] = sev
	}

	return result
}

func tryMaquetteTimestamp(line string, cm *CompiledMaquette) (time.Time, bool) {
	matches := cm.tsRegex.FindStringSubmatch(line)
	if len(matches) < 2 {
		return time.Time{}, false
	}
	raw := matches[1]

	if cm.tsFormat == "" {
		secs, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return time.Time{}, false
		}
		whole := int64(secs)
		frac := int64((secs - float64(whole)) * 1e9)
		return time.Unix(whole, frac).UTC(), true
	}

	ts, err := time.Parse(cm.tsFormat, raw)
	if err != nil {
		return time.Time{}, false
	}

	if ts.Year() == 0 {
		now := time.Now()
		ts = ts.AddDate(now.Year(), 0, 0)
		if ts.After(now.Add(24 * time.Hour)) {
			ts = ts.AddDate(-1, 0, 0)
		}
	}

	return ts, true
}

func extractSource(line string, cm *CompiledMaquette) string {
	matches := cm.srcRegex.FindStringSubmatch(line)
	if cm.srcNameIdx >= 0 && cm.srcNameIdx < len(matches) {
		return matches[cm.srcNameIdx]
	}
	if len(matches) > 1 {
		return matches[1]
	}
	return ""
}

func extractSeverity(line string, cm *CompiledMaquette) string {
	for keyword, level := range cm.sevWords {
		if strings.Contains(line, keyword) {
			return level
		}
	}
	return ""
}
