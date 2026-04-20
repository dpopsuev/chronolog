package parser

import (
	"testing"
	"time"

	"github.com/dpopsuev/chronolog/internal/domain"
)

func TestParse_RFC3339(t *testing.T) {
	t.Helper()
	tests := []struct {
		name       string
		line       string
		wantConf   string
		wantYear   int
		wantMonth  time.Month
		wantDay    int
		wantHour   int
		wantMinute int
	}{
		{
			name:       "plain RFC3339",
			line:       "2025-12-20T06:56:37Z some log message",
			wantConf:   domain.ConfidenceRFC3339,
			wantYear:   2025,
			wantMonth:  time.December,
			wantDay:    20,
			wantHour:   6,
			wantMinute: 56,
		},
		{
			name:       "RFC3339 with nanoseconds",
			line:       "2025-12-20T06:56:37.576917676Z I1220 06:56:37.576576 main.go:49",
			wantConf:   domain.ConfidenceRFC3339,
			wantYear:   2025,
			wantMonth:  time.December,
			wantDay:    20,
			wantHour:   6,
			wantMinute: 56,
		},
		{
			name:       "RFC3339 in brackets",
			line:       "[2025-12-20T14:59:21Z] ERROR cloud-event-proxy publishing FREERUN",
			wantConf:   domain.ConfidenceRFC3339,
			wantYear:   2025,
			wantMonth:  time.December,
			wantDay:    20,
			wantHour:   14,
			wantMinute: 59,
		},
		{
			name:       "RFC3339 with timezone offset",
			line:       "2025-12-21T08:47:47+05:00 some message",
			wantConf:   domain.ConfidenceRFC3339,
			wantYear:   2025,
			wantMonth:  time.December,
			wantDay:    21,
			wantHour:   8,
			wantMinute: 47,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, conf := Parse(tt.line)
			if conf != tt.wantConf {
				t.Errorf("confidence = %q, want %q", conf, tt.wantConf)
			}
			if ts.Year() != tt.wantYear {
				t.Errorf("year = %d, want %d", ts.Year(), tt.wantYear)
			}
			if ts.Month() != tt.wantMonth {
				t.Errorf("month = %v, want %v", ts.Month(), tt.wantMonth)
			}
			if ts.Day() != tt.wantDay {
				t.Errorf("day = %d, want %d", ts.Day(), tt.wantDay)
			}
			if ts.Hour() != tt.wantHour {
				t.Errorf("hour = %d, want %d", ts.Hour(), tt.wantHour)
			}
			if ts.Minute() != tt.wantMinute {
				t.Errorf("minute = %d, want %d", ts.Minute(), tt.wantMinute)
			}
		})
	}
}

func TestParse_Unknown(t *testing.T) {
	t.Helper()
	tests := []struct {
		name string
		line string
	}{
		{"plain text", "ERROR something happened"},
		{"syslog format", "Dec 20 04:57:41.832171 helix73 systemd[1]: Starting Kubernetes"},
		{"empty line", ""},
		{"dmesg format", "[    0.000000] Linux version 5.14.0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts, conf := Parse(tt.line)
			if conf != domain.ConfidenceUnknown {
				t.Errorf("confidence = %q, want %q", conf, domain.ConfidenceUnknown)
			}
			if !ts.IsZero() {
				t.Errorf("timestamp = %v, want zero", ts)
			}
		})
	}
}
