// Package memory provides API-only clients for AHP personal and project memory.
// Physical bank IDs and database access are never part of the agent contract.
package memory

import "time"

// Scope identifies the logical memory owned by the verified subject.
type Scope string

const (
	Personal Scope = "personal"
	Project  Scope = "project"
)

type RememberRequest struct {
	ProjectID  string   `json:"project_id,omitempty"`
	Target     Scope    `json:"target"`
	Text       string   `json:"text"`
	SourceRefs []string `json:"source_refs"`
}
type SearchRequest struct {
	ProjectID string `json:"project_id,omitempty"`
	Scope     string `json:"scope"`
	Query     string `json:"query"`
}
type ContextRequest struct {
	ProjectID       string `json:"project_id"`
	Query           string `json:"query"`
	IncludePersonal bool   `json:"include_personal"`
}
type SettingsRequest struct {
	ProjectID       string `json:"project_id,omitempty"`
	PersonalEnabled *bool  `json:"personal_enabled,omitempty"`
	ProjectEnabled  *bool  `json:"project_enabled,omitempty"`
	UsePersonal     *bool  `json:"use_personal,omitempty"`
	IdleDays        *int   `json:"idle_days,omitempty"`
}
type ClearRequest struct {
	ProjectID string `json:"project_id,omitempty"`
	Scope     Scope  `json:"scope"`
}
type CorrectRequest struct {
	Scope     Scope  `json:"scope"`
	ProjectID string `json:"project_id,omitempty"`
	Text      string `json:"text"`
	Reason    string `json:"reason"`
}

type Settings struct {
	PersonalEnabled bool      `json:"personal_enabled"`
	ProjectEnabled  bool      `json:"project_enabled"`
	UsePersonal     bool      `json:"use_personal"`
	IdleDays        int       `json:"idle_days"`
	LastActivity    time.Time `json:"last_activity,omitempty"`
}

type Fact struct {
	Ref         string            `json:"memory_ref"`
	Scope       Scope             `json:"scope"`
	Text        string            `json:"text"`
	Type        string            `json:"type,omitempty"`
	MentionedAt string            `json:"mentioned_at,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}
type SearchResult struct {
	Facts    []Fact  `json:"facts"`
	Degraded []Scope `json:"degraded,omitempty"`
}
type Operation struct {
	ID    string `json:"id"`
	State string `json:"state"`
	Scope Scope  `json:"scope"`
	Error string `json:"error,omitempty"`
}

type Status struct {
	Scope             Scope      `json:"scope"`
	State             string     `json:"state"`
	Generation        int        `json:"generation"`
	CleanupPending    bool       `json:"cleanup_pending"`
	PendingOperations int        `json:"pending_operations"`
	FailedOperations  int        `json:"failed_operations"`
	ExpiresAt         *time.Time `json:"expires_at,omitempty"`
}

type Page struct {
	Facts  []Fact `json:"facts"`
	Total  int    `json:"total"`
	Limit  int    `json:"limit"`
	Offset int    `json:"offset"`
}

// ClearResult acknowledges generation closure, not completed native erasure.
type ClearResult struct {
	State string `json:"state"`
}
