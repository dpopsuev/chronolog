package parser

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/dpopsuev/chronolog/internal/domain"
)

// Slog attribute key constants for the parser layer.
const (
	logKeyLine = "line"
)

// Parse detects and parses a timestamp from a log line.
// Returns the parsed time and a confidence string.
func Parse(line string) (ts time.Time, confidence string) {
	if t, ok := tryRFC3339(line); ok {
		return t, domain.ConfidenceRFC3339
	}
	if line != "" {
		slog.DebugContext(context.Background(), "timestamp confidence unknown", slog.String(logKeyLine, line))
	}
	return time.Time{}, domain.ConfidenceUnknown
}

func tryRFC3339(line string) (time.Time, bool) {
	for _, start := range findTimestampCandidates(line) {
		for _, end := range []int{30, 35, 40, 20, 25} {
			if start+end > len(line) {
				continue
			}
			candidate := line[start : start+end]
			if t, err := time.Parse(time.RFC3339Nano, candidate); err == nil {
				return t, true
			}
			if t, err := time.Parse(time.RFC3339, candidate); err == nil {
				return t, true
			}
		}
	}
	return time.Time{}, false
}

func findTimestampCandidates(line string) []int {
	var positions []int
	positions = append(positions, 0)
	for i, c := range line {
		if c == '[' || c == ' ' || c == '\t' {
			next := i + 1
			if next < len(line) && isDigit(line[next]) {
				positions = append(positions, next)
			}
		}
	}
	if idx := strings.IndexByte(line, 'T'); idx > 4 {
		start := idx - 4
		for start > 0 && (isDigit(line[start-1]) || line[start-1] == '-') {
			start--
		}
		positions = append(positions, start)
	}
	return positions
}

func isDigit(b byte) bool {
	return b >= '0' && b <= '9'
}
