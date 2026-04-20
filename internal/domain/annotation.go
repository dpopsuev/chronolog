package domain

import "time"

// Bookmark annotates a single event with a tag and note.
type Bookmark struct {
	ID        string    `json:"id"`
	EventID   string    `json:"event_id"`
	Label     string    `json:"label"`
	Note      string    `json:"note,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// Highlight marks a specific substring within an event as significant.
type Highlight struct {
	ID        string    `json:"id"`
	EventID   string    `json:"event_id"`
	Substring string    `json:"substring"`
	Label     string    `json:"label,omitempty"`
	Note      string    `json:"note,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}
