package memory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

func (s *Service) schedule(tx Tx) error {
	if preserved, err := preserveTenant(tx); err != nil {
		return err
	} else if preserved {
		return tx.Schedule(time.Time{})
	}
	var next time.Time
	add := func(at time.Time) {
		if !at.IsZero() && (next.IsZero() || at.Before(next)) {
			next = at
		}
	}
	banks, err := list[Bank](tx, "bank")
	if err != nil {
		return err
	}
	for _, b := range banks {
		if b.State != "active" {
			add(b.NextPurge)
			continue
		}
		if b.Scope == Project {
			p, err := s.project(tx, b.ProjectID)
			if err != nil {
				return err
			}
			at := p.LastActivity.Add(time.Duration(p.IdleDays) * 24 * time.Hour)
			live, err := s.liveRun(tx, p.ID)
			if err != nil {
				return err
			}
			if live && !at.After(s.Now()) {
				at = s.Now().Add(time.Minute)
			}
			add(at)
		}
	}
	cs, err := list[Cursor](tx, "cursor")
	if err != nil {
		return err
	}
	for _, c := range cs {
		if len(c.Pending) == 0 || c.JobID != "" {
			continue
		}
		var b Bank
		if err := tx.Get("bank", c.BankID, &b); err != nil {
			return err
		}
		if b.State != "active" {
			continue
		}
		batch, wait := s.window(b.Scope)
		at := c.PendingSince.Add(wait)
		if c.Flush || len(c.Pending) >= batch {
			at = s.Now()
		}
		add(at)
	}
	jobs, err := list[Job](tx, "job")
	if err != nil {
		return err
	}
	for _, j := range jobs {
		if j.State == "queued" || j.State == "submitted" {
			at := j.NextAt
			if j.LeaseUntil.After(at) {
				at = j.LeaseUntil
			}
			add(at)
		}
		if !j.FinishedAt.IsZero() && !j.PayloadExpired {
			add(j.FinishedAt.Add(s.Config.PayloadRetention))
		}
	}
	turns, err := list[Turn](tx, "turn")
	if err != nil {
		return err
	}
	for _, t := range turns {
		if len(t.Request.Sources) > 0 {
			at := t.ReceivedAt.Add(s.Config.PayloadRetention)
			if !at.After(s.Now()) {
				at = s.Now().Add(time.Hour)
			}
			add(at)
		}
	}
	return tx.Schedule(next)
}
func (s *Service) window(scope Scope) (int, time.Duration) {
	if scope == Personal {
		return s.Config.PersonalBatch, s.Config.PersonalWait
	}
	return s.Config.ProjectBatch, s.Config.ProjectWait
}

func (s *Service) createJobs(tx Tx) error {
	projects, err := list[ProjectState](tx, "project")
	if err != nil {
		return err
	}
	for _, p := range projects {
		if err := s.expire(tx, p); err != nil {
			return err
		}
	}
	cs, err := list[Cursor](tx, "cursor")
	if err != nil {
		return err
	}
	for _, c := range cs {
		if c.JobID != "" || len(c.Pending) == 0 {
			continue
		}
		var b Bank
		if err := tx.Get("bank", c.BankID, &b); err != nil {
			return err
		}
		enabled, err := s.enabled(tx, b.Scope, c.ProjectID)
		if err != nil {
			return err
		}
		if b.State != "active" || !enabled {
			c.Pending = nil
			c.Flush = false
			if err := tx.Put("cursor", c.ID, c); err != nil {
				return err
			}
			continue
		}
		batch, wait := s.window(b.Scope)
		if !c.Flush && len(c.Pending) < batch && s.Now().Before(c.PendingSince.Add(wait)) {
			continue
		}
		if len(c.Pending) < batch {
			batch = len(c.Pending)
		}
		j := Job{ID: newID(), Kind: "retain", BankID: b.ID, Scope: b.Scope, ProjectID: c.ProjectID, CursorID: c.ID, State: "queued", NextAt: s.Now(), CreatedAt: s.Now()}
		var content strings.Builder
		refs := []string{}
		labels := []string{}
		floor, err := s.sourceFloor(tx, b, c.ProjectID)
		if err != nil {
			return err
		}
		for _, turnID := range c.Pending[:batch] {
			var t Turn
			if err := tx.Get("turn", turnID, &t); err != nil {
				return err
			}
			var part strings.Builder
			for _, source := range t.Request.Sources {
				if allows(source, b.Scope) && !source.At.Before(floor) {
					fmt.Fprintf(&part, "[%s %s source=%s]\n%s\n", source.At.Format(time.RFC3339Nano), source.Role, source.Ref, source.Text)
				}
			}
			if content.Len() > 0 && content.Len()+part.Len() > 256*1024 {
				break
			}
			content.WriteString(part.String())
			j.TurnIDs = append(j.TurnIDs, turnID)
			j.Identity = t.Identity
			labels = append(labels, t.Identity.Labels...)
			for _, source := range t.Request.Sources {
				if allows(source, b.Scope) && !source.At.Before(floor) {
					refs = append(refs, source.Ref)
					labels = append(labels, source.Labels...)
					source.Text = ""
					j.Sources = append(j.Sources, source)
				}
			}
		}
		j.Content = content.String()
		refJSON, _ := json.Marshal(unique(refs))
		labelJSON, _ := json.Marshal(unique(labels))
		j.Metadata = map[string]string{"tenant_id": j.Identity.TenantID, "user_id": j.Identity.UserID, "project_id": c.ProjectID, "session_id": c.SessionID, "source_refs": string(refJSON), "source_labels": string(labelJSON), "mission_version": MissionVersion}
		b.Labels = unique(append(b.Labels, labels...))
		b.Revision++
		if err := tx.Put("bank", b.ID, b); err != nil {
			return err
		}
		if err := tx.Put("job", j.ID, j); err != nil {
			return err
		}
		c.JobID = j.ID
		if err := tx.Put("cursor", c.ID, c); err != nil {
			return err
		}
	}
	return nil
}
func (s *Service) allowed(tx Tx, j Job) (Bank, bool, error) {
	var b Bank
	if err := tx.Get("bank", j.BankID, &b); err != nil {
		return b, false, err
	}
	if frozen, err := tx.TenantFrozen(); err != nil {
		return b, false, err
	} else if frozen {
		return b, false, nil
	}
	if fenced, err := subjectFenced(tx); err != nil {
		return b, false, err
	} else if fenced {
		return b, false, nil
	}
	if b.State != "active" {
		return b, false, nil
	}
	on, err := s.enabled(tx, j.Scope, j.ProjectID)
	if err != nil || !on {
		return b, false, err
	}
	u, err := s.user(tx)
	if err != nil {
		return b, false, err
	}
	if j.Scope == Personal && u.Since.After(j.CreatedAt) {
		return b, false, nil
	}
	if j.ProjectID != "" {
		p, err := s.project(tx, j.ProjectID)
		if err != nil {
			return b, false, err
		}
		since := p.ProjectSince
		if j.Scope == Personal {
			since = p.PersonalSince
		}
		if since.After(j.CreatedAt) {
			return b, false, nil
		}
	}
	return b, true, nil
}
func (s *Service) claim(ctx context.Context, owner string) (Job, bool, error) {
	var picked Job
	found := false
	err := s.update(ctx, owner, func(tx Tx) error {
		if preserved, err := preserveTenant(tx); err != nil {
			return err
		} else if preserved {
			return nil
		}
		if err := s.createJobs(tx); err != nil {
			return err
		}
		jobs, err := list[Job](tx, "job")
		if err != nil {
			return err
		}
		sort.Slice(jobs, func(a, b int) bool {
			if (jobs[a].CursorID == "") != (jobs[b].CursorID == "") {
				return jobs[a].CursorID == ""
			}
			return jobs[a].CreatedAt.Before(jobs[b].CreatedAt)
		})
		for _, j := range jobs {
			if (j.State != "queued" && j.State != "submitted") || j.NextAt.After(s.Now()) || j.LeaseUntil.After(s.Now()) {
				continue
			}
			j.LeaseToken = newID()
			j.LeaseUntil = s.Now().Add(s.Config.Lease)
			j.Attempts++
			picked = j
			found = true
			return tx.Put("job", j.ID, j)
		}
		return nil
	})
	return picked, found, err
}

// Tick performs bounded work. Multiple instances are safe: persisted job leases
// fence completions, while native operation_id fences ambiguous submissions.
func (s *Service) Tick(ctx context.Context) error {
	owners, err := s.Store.Due(ctx, s.Now(), 100)
	if err != nil {
		return err
	}
	for _, owner := range owners {
		if err := s.WorkOwner(ctx, owner); err != nil {
			return err
		}
	}
	return nil
}
func (s *Service) WorkOwner(ctx context.Context, owner string) error {
	j, ok, err := s.claim(ctx, owner)
	if err != nil {
		return err
	}
	if ok {
		if err := s.execute(ctx, owner, j); err != nil {
			return err
		}
	}
	if err := s.purgeOne(ctx, owner); err != nil {
		return err
	}
	return s.prune(ctx, owner)
}
func (s *Service) execute(parent context.Context, owner string, j Job) error {
	ctx, cancel := context.WithTimeout(parent, s.Config.Lease/2)
	defer cancel()
	var b Bank
	permitted := false
	err := s.Store.Within(ctx, owner, func(tx Tx) error { var err error; b, permitted, err = s.allowed(tx, j); return err })
	if err != nil {
		return err
	}
	if !permitted {
		if j.Attempts > 1 || j.State == "submitted" {
			if err := s.Engine.Cancel(ctx, b.ID, j.ID); err != nil {
				return s.finish(parent, owner, j, j.State, err)
			}
		}
		return s.finish(parent, owner, j, "cancelled", nil)
	}
	if err := j.Identity.Validate(s.Now()); err != nil {
		return s.denyJob(ctx, parent, owner, b, j, err)
	}
	access := AccessRequest{Action: "memory:write", ProjectID: j.ProjectID, Scopes: []Scope{j.Scope}, Sources: j.Sources}
	if err := s.Access.Revalidate(ctx, j.Identity, access); err != nil {
		var domain *Error
		if errors.As(err, &domain) && domain.Status < 500 {
			return s.denyJob(ctx, parent, owner, b, j, err)
		}
		return s.finish(parent, owner, j, "", err)
	}
	if j.Kind != "retain" {
		err := s.Engine.Correct(ctx, b, j)
		if err != nil {
			return s.finish(parent, owner, j, "", err)
		}
		return s.finish(parent, owner, j, "succeeded", nil)
	}
	state, err := s.Engine.Status(ctx, b.ID, j.ID)
	if err != nil {
		return s.finish(parent, owner, j, "", err)
	}
	switch state {
	case "completed":
		// The retain completed upstream. Record how many facts were actually
		// committed so the durable Operation carries an observable extraction
		// outcome (a "completed with 0 facts" signal) instead of silently
		// promising a fact that was never stored. Best-effort and non-blocking:
		// an engine that cannot report it, or an error, leaves the operation
		// exactly as before. This never changes success semantics - a correct
		// extract-nothing result is still a success.
		if stats, ok := s.Engine.(RetainStatsEngine); ok {
			if committed, err := stats.RetainFactsCommitted(ctx, b.ID, j.ID); err == nil {
				if j.Metadata == nil {
					j.Metadata = map[string]string{}
				}
				j.Metadata["facts_committed"] = strconv.Itoa(committed)
			}
		}
		return s.finish(parent, owner, j, "succeeded", nil)
	case "failed", "cancelled":
		return s.finish(parent, owner, j, "failed", Fail(422, "upstream_operation_failed", "Hindsight operation ended without success"))
	case "pending", "processing":
		return s.finish(parent, owner, j, "submitted", nil)
	case "not_found":
		if err := s.Engine.Configure(ctx, b); err != nil {
			return s.finish(parent, owner, j, "", err)
		}
		// A clear/settings update may have raced with the network call.
		if err := s.Store.Within(ctx, owner, func(tx Tx) error { var err error; _, permitted, err = s.allowed(tx, j); return err }); err != nil {
			return err
		}
		if !permitted {
			return s.finish(parent, owner, j, "cancelled", nil)
		}
		// Native Status/Configure can outlive the grant or race revocation.
		// Revalidate immediately before sending source content. This is a last
		// successful authorization boundary, not an atomic IAM/native transaction.
		if err := j.Identity.Validate(s.Now()); err != nil {
			return s.denyJob(ctx, parent, owner, b, j, err)
		}
		if err := s.Access.Revalidate(ctx, j.Identity, access); err != nil {
			var domain *Error
			if errors.As(err, &domain) && domain.Status < 500 {
				return s.denyJob(ctx, parent, owner, b, j, err)
			}
			return s.finish(parent, owner, j, "", err)
		}
		if err := s.Engine.Retain(ctx, b, j); err != nil {
			return s.finish(parent, owner, j, "", err)
		}
		return s.finish(parent, owner, j, "submitted", nil)
	default:
		return s.finish(parent, owner, j, "", Fail(502, "invalid_upstream_status", "Hindsight returned an unknown operation state"))
	}
}
func (s *Service) denyJob(ctx, parent context.Context, owner string, b Bank, j Job, cause error) error {
	if j.Kind == "retain" && (j.Attempts > 1 || j.State == "submitted") {
		if err := s.Engine.Cancel(ctx, b.ID, j.ID); err != nil {
			return s.finish(parent, owner, j, "", err)
		}
	}
	return s.finish(parent, owner, j, "failed", cause)
}
func (s *Service) finish(ctx context.Context, owner string, claimed Job, state string, cause error) error {
	return s.update(ctx, owner, func(tx Tx) error {
		var j Job
		if err := tx.Get("job", claimed.ID, &j); err != nil {
			return err
		}
		if j.LeaseToken != claimed.LeaseToken {
			return nil
		}
		j.LeaseToken = ""
		j.LeaseUntil = time.Time{}
		if cause != nil {
			j.Failures++
			j.Error = "dependency_unavailable"
			var typed *Error
			if errors.As(cause, &typed) {
				j.Error = typed.Code
				if typed.Status < 500 {
					state = "failed"
				}
			}
			if j.Failures >= s.Config.MaxAttempts {
				state = "failed"
			}
			if state == "" {
				state = j.State
			}
			backoff := time.Second * time.Duration(1<<min(j.Failures, 8))
			j.NextAt = s.Now().Add(backoff)
		} else {
			j.Error = ""
			j.Failures = 0
			j.NextAt = s.Now().Add(s.Config.Poll)
		}
	// finish reloads the job inside the transaction, so any observable recorded
	// by the caller on the claimed copy (e.g. facts_committed, which requires a
	// network read that cannot happen inside the store transaction) must be
	// carried over explicitly rather than lost on the reload.
	if v, ok := claimed.Metadata["facts_committed"]; ok {
		if j.Metadata == nil {
			j.Metadata = map[string]string{}
		}
		j.Metadata["facts_committed"] = v
	}
		if state != "" {
			j.State = state
		}
		if j.State == "succeeded" || j.State == "failed" || j.State == "cancelled" {
			j.FinishedAt = s.Now()
		}
		if err := tx.Put("job", j.ID, j); err != nil {
			return err
		}
		if j.State == "succeeded" {
			var b Bank
			if err := tx.Get("bank", j.BankID, &b); err != nil {
				return err
			}
			b.Revision++
			if err := tx.Put("bank", b.ID, b); err != nil {
				return err
			}
		}
		if j.CursorID != "" && (j.State == "succeeded" || j.State == "cancelled") {
			var c Cursor
			if err := tx.Get("cursor", j.CursorID, &c); err != nil {
				return err
			}
			if c.JobID == j.ID {
				remove := map[string]bool{}
				for _, turnID := range j.TurnIDs {
					remove[turnID] = true
					if j.State == "succeeded" {
						var t Turn
						if err := tx.Get("turn", turnID, &t); err != nil {
							return err
						}
						if t.Request.Sequence > c.ProcessedSequence {
							c.ProcessedSequence = t.Request.Sequence
						}
					}
				}
				pending := []string{}
				for _, turnID := range c.Pending {
					if !remove[turnID] {
						pending = append(pending, turnID)
					}
				}
				c.Pending = pending
				c.JobID = ""
				if len(pending) > 0 {
					var t Turn
					if err := tx.Get("turn", pending[0], &t); err != nil {
						return err
					}
					c.PendingSince = t.ReceivedAt
				} else {
					c.PendingSince = time.Time{}
					c.Flush = false
				}
				if err := tx.Put("cursor", c.ID, c); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (s *Service) purgeOne(ctx context.Context, owner string) error {
	var bank Bank
	found := false
	var jobs []Job
	err := s.update(ctx, owner, func(tx Tx) error {
		if preserved, err := preserveTenant(tx); err != nil {
			return err
		} else if preserved {
			return nil
		}
		banks, err := list[Bank](tx, "bank")
		if err != nil {
			return err
		}
		allJobs, err := list[Job](tx, "job")
		if err != nil {
			return err
		}
		for _, b := range banks {
			if b.State == "active" || b.NextPurge.After(s.Now()) {
				continue
			}
			busy := false
			for _, j := range allJobs {
				if j.BankID == b.ID && j.LeaseUntil.After(s.Now()) {
					busy = true
				}
			}
			if busy {
				continue
			}
			bank = b
			found = true
			for _, j := range allJobs {
				if j.BankID == b.ID {
					jobs = append(jobs, j)
				}
			}
			b.NextPurge = s.Now().Add(s.Config.PurgeGrace)
			return tx.Put("bank", b.ID, b)
		}
		return nil
	})
	if err != nil || !found {
		return err
	}
	for _, j := range jobs {
		if j.Kind != "retain" || j.Attempts == 0 {
			continue
		}
		status, err := s.Engine.Status(ctx, bank.ID, j.ID)
		if err != nil {
			return err
		}
		if status == "pending" || status == "processing" {
			if err := s.Engine.Cancel(ctx, bank.ID, j.ID); err != nil {
				return err
			}
			return nil
		}
	}
	if err := s.Engine.DeleteBank(ctx, bank.ID); err != nil {
		return err
	}
	return s.update(ctx, owner, func(tx Tx) error {
		var b Bank
		if err := tx.Get("bank", bank.ID, &b); err != nil {
			return err
		}
		if b.State == "active" {
			return Fail(500, "generation_invariant", "a closed bank became active")
		}
		b.State = "deleted"
		b.NextPurge = s.Now().Add(s.Config.AuditInterval)
		if err := tx.Put("bank", b.ID, b); err != nil {
			return err
		}
		current, err := list[Job](tx, "job")
		if err != nil {
			return err
		}
		for _, j := range current {
			if j.BankID == bank.ID {
				if j.State != "succeeded" && j.State != "failed" {
					j.State = "cancelled"
				}
				j.FinishedAt = s.Now()
				j.Content = ""
				j.Sources = nil
				j.Metadata = nil
				j.Reason = ""
				j.PayloadExpired = true
				j.LeaseToken = ""
				j.LeaseUntil = time.Time{}
				if err := tx.Put("job", j.ID, j); err != nil {
					return err
				}
			}
		}
		return nil
	})
}
func (s *Service) prune(ctx context.Context, owner string) error {
	return s.update(ctx, owner, func(tx Tx) error {
		if preserved, err := preserveTenant(tx); err != nil {
			return err
		} else if preserved {
			return nil
		}
		jobs, err := list[Job](tx, "job")
		if err != nil {
			return err
		}
		for _, j := range jobs {
			terminal := j.State == "succeeded" || j.State == "cancelled" || j.State == "failed"
			if !terminal || j.PayloadExpired || j.FinishedAt.Add(s.Config.PayloadRetention).After(s.Now()) {
				continue
			}
			if j.State == "failed" && j.CursorID != "" {
				var c Cursor
				if err := tx.Get("cursor", j.CursorID, &c); err != nil {
					return err
				}
				if c.JobID == j.ID {
					remove := map[string]bool{}
					for _, id := range j.TurnIDs {
						remove[id] = true
					}
					pending := []string{}
					for _, id := range c.Pending {
						if !remove[id] {
							pending = append(pending, id)
						}
					}
					c.Pending = pending
					c.JobID = ""
					c.PendingSince = time.Time{}
					if len(pending) > 0 {
						var turn Turn
						if err := tx.Get("turn", pending[0], &turn); err != nil {
							return err
						}
						c.PendingSince = turn.ReceivedAt
					} else {
						c.Flush = false
					}
					if err := tx.Put("cursor", c.ID, c); err != nil {
						return err
					}
				}
			}
			j.Content = ""
			j.Sources = nil
			j.Metadata = nil
			j.Reason = ""
			j.PayloadExpired = true
			if err := tx.Put("job", j.ID, j); err != nil {
				return err
			}
		}
		cs, err := list[Cursor](tx, "cursor")
		if err != nil {
			return err
		}
		pending := map[string]bool{}
		for _, c := range cs {
			for _, id := range c.Pending {
				pending[id] = true
			}
		}
		turns, err := list[Turn](tx, "turn")
		if err != nil {
			return err
		}
		for _, t := range turns {
			if len(t.Request.Sources) > 0 && !pending[t.ID] && !t.ReceivedAt.Add(s.Config.PayloadRetention).After(s.Now()) {
				t.Request.Sources = nil
				if err := tx.Put("turn", t.ID, t); err != nil {
					return err
				}
			}
		}
		return nil
	})
}
