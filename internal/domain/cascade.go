package domain

import "time"

// Domain is the tree root of the cascade hierarchy — the broadest scope.
type Domain struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Alias       string    `json:"alias,omitempty"`
	Description string    `json:"description,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

// Environment is the system under test identity (OCP version, host, config).
type Environment struct {
	ID        string    `json:"id"`
	DomainID  string    `json:"domain_id"`
	Name      string    `json:"name"`
	Alias     string    `json:"alias,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// Session is one full serial test run — ordered sequence of instances.
type Session struct {
	ID            string     `json:"id"`
	EnvironmentID string     `json:"environment_id"`
	Name          string     `json:"name"`
	Alias         string     `json:"alias,omitempty"`
	StartedAt     time.Time  `json:"started_at"`
	EndedAt       *time.Time `json:"ended_at,omitempty"`
}

// Instance is one test's consolidated log — all sources merged into one timeline.
type Instance struct {
	ID            string     `json:"id"`
	SessionID     string     `json:"session_id"`
	Name          string     `json:"name"`
	Alias         string     `json:"alias,omitempty"`
	SourcePattern string     `json:"source_pattern,omitempty"`
	StartedAt     time.Time  `json:"started_at"`
	EndedAt       *time.Time `json:"ended_at,omitempty"`
}

// Phase is a time block within an instance (Before Test, Test, After Test).
type Phase struct {
	ID         string     `json:"id"`
	InstanceID string     `json:"instance_id"`
	Name       string     `json:"name"`
	Label      string     `json:"label,omitempty"`
	StartedAt  time.Time  `json:"started_at"`
	EndedAt    *time.Time `json:"ended_at,omitempty"`
}
