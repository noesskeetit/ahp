package memory

import (
	"context"
	"time"
)

type Status struct {
	Scope             Scope      `json:"scope"`
	State             string     `json:"state"`
	Generation        int        `json:"generation"`
	CleanupPending    bool       `json:"cleanup_pending"`
	PendingOperations int        `json:"pending_operations"`
	FailedOperations  int        `json:"failed_operations"`
	ExpiresAt         *time.Time `json:"expires_at,omitempty"`
}

func (s *Service) Status(ctx context.Context, id Identity, scope Scope, project string) (Status, error) {
	out := Status{Scope: scope, State: "empty"}
	if !scope.Valid() || (scope == Project && project == "") {
		return out, Fail(400, "invalid_scope", "select personal or a current project")
	}
	err := s.updateIdentity(ctx, id, func(tx Tx) error {
		if project != "" {
			p, err := s.project(tx, project)
			if err != nil {
				return err
			}
			if err := s.expire(tx, p); err != nil {
				return err
			}
			if scope == Project && !p.Deleted {
				at := p.LastActivity.Add(time.Duration(p.IdleDays) * 24 * time.Hour)
				out.ExpiresAt = &at
			}
		}
		banks, err := list[Bank](tx, "bank")
		if err != nil {
			return err
		}
		var selected Bank
		for _, b := range banks {
			if b.Scope != scope || (scope == Project && b.ProjectID != project) {
				continue
			}
			if b.State == "deleting" {
				out.CleanupPending = true
			}
			if b.Generation > selected.Generation {
				selected = b
			}
		}
		if selected.ID != "" {
			out.State = selected.State
			out.Generation = selected.Generation
		}
		on, err := s.enabled(tx, scope, project)
		if err != nil {
			return err
		}
		if !on && out.State == "active" {
			out.State = "disabled"
		}
		jobs, err := list[Job](tx, "job")
		if err != nil {
			return err
		}
		for _, j := range jobs {
			if j.BankID == selected.ID {
				if j.State == "queued" || j.State == "submitted" {
					out.PendingOperations++
				}
				if j.State == "failed" {
					out.FailedOperations++
				}
			}
		}
		return nil
	})
	return out, err
}
