package mcp

import (
	"regexp"
	"sort"

	"github.com/dpopsuev/chronolog/internal/domain"
)

// Regex patterns for templatizing log messages.
var (
	reUUID   = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)
	reHex    = regexp.MustCompile(`(?:^|[^0-9a-zA-Z])0x[0-9a-fA-F]+`)
	reTS     = regexp.MustCompile(`\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?`)
	reNumber = regexp.MustCompile(`\d+(?:\.\d+)?`)
)

const templatePlaceholder = "<*>"

// templatize replaces variable parts of a log message with <*> placeholders.
// Returns the pattern and a slice of extracted variable values.
func templatize(msg string) (pattern string, variables []string) {
	collect := func(re *regexp.Regexp, s string) string {
		return re.ReplaceAllStringFunc(s, func(match string) string {
			variables = append(variables, match)
			return templatePlaceholder
		})
	}
	pattern = collect(reUUID, msg)
	pattern = collect(reTS, pattern)
	pattern = collect(reHex, pattern)
	pattern = collect(reNumber, pattern)
	return pattern, variables
}

// extractTemplates groups events by templatized pattern and returns sorted templates.
func extractTemplates(events []*domain.Event) []domain.Template {
	type accumulator struct {
		tmpl domain.Template
		seen map[string]bool
	}
	groups := make(map[string]*accumulator)
	for _, e := range events {
		pattern, vars := templatize(e.Message)
		acc, ok := groups[pattern]
		if !ok {
			acc = &accumulator{
				tmpl: domain.Template{Pattern: pattern, FirstSeen: e.Timestamp, LastSeen: e.Timestamp},
				seen: make(map[string]bool),
			}
			groups[pattern] = acc
		}
		acc.tmpl.Count++
		if e.Timestamp.Before(acc.tmpl.FirstSeen) {
			acc.tmpl.FirstSeen = e.Timestamp
		}
		if e.Timestamp.After(acc.tmpl.LastSeen) {
			acc.tmpl.LastSeen = e.Timestamp
		}
		for _, v := range vars {
			if !acc.seen[v] && len(acc.tmpl.Variables) < 10 {
				acc.seen[v] = true
				acc.tmpl.Variables = append(acc.tmpl.Variables, v)
			}
		}
	}
	result := make([]domain.Template, 0, len(groups))
	for _, acc := range groups {
		result = append(result, acc.tmpl)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].Count > result[j].Count
	})
	return result
}

// templateSet builds a map of pattern->Template from events.
func templateSet(events []*domain.Event) map[string]domain.Template {
	templates := extractTemplates(events)
	m := make(map[string]domain.Template, len(templates))
	for _, t := range templates {
		m[t.Pattern] = t
	}
	return m
}

// timingDiffers compares whether two templates have significantly different timing.
func timingDiffers(a, b domain.Template) bool {
	durA := a.LastSeen.Sub(a.FirstSeen)
	durB := b.LastSeen.Sub(b.FirstSeen)
	if durA == 0 && durB == 0 {
		return false
	}
	if durA == 0 || durB == 0 {
		return true
	}
	ratio := float64(durA) / float64(durB)
	return ratio > 2.0 || ratio < 0.5
}

// computeDiff compares two sets of events and categorizes patterns.
func computeDiff(eventsA, eventsB []*domain.Event) map[string]any {
	setA := templateSet(eventsA)
	setB := templateSet(eventsB)
	var hot, cold, warm []map[string]any
	for pattern, tA := range setA {
		tB, inBoth := setB[pattern]
		if !inBoth {
			hot = append(hot, map[string]any{"pattern": pattern, "side": "A", "count": tA.Count})
			continue
		}
		entry := map[string]any{"pattern": pattern, "count_a": tA.Count, "count_b": tB.Count}
		if timingDiffers(tA, tB) {
			warm = append(warm, entry)
		} else {
			cold = append(cold, entry)
		}
	}
	for pattern, tB := range setB {
		if _, inA := setA[pattern]; !inA {
			hot = append(hot, map[string]any{"pattern": pattern, "side": "B", "count": tB.Count})
		}
	}
	return map[string]any{"hot": hot, "cold": cold, "warm": warm}
}

// sortedKeys returns sorted keys from a string-keyed map.
func sortedKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
