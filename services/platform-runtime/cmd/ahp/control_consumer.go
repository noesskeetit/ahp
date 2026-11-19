package main

import (
	"context"
	"encoding/json"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"time"
)

// No HTTP decoder or bearer can construct this authority. It is derived only
// from a durable job admitted while the administrative human was still live.
// It grants deletion alone; reads, proposals, hold changes and new jobs still
// require their ordinary authorization.
type deletionAuthorityKey struct{}
type deletionAuthority struct{ job, tenant, subject, kind string }

func withDeletionAuthority(ctx context.Context, job state.ControlDeletion) context.Context {
	return context.WithValue(ctx, deletionAuthorityKey{}, deletionAuthority{job.ID, job.Tenant, job.Subject, job.Kind})
}
func permitsDeletion(ctx context.Context, scope contracts.Scope) bool {
	a, ok := ctx.Value(deletionAuthorityKey{}).(deletionAuthority)
	return ok && a.job != "" && a.tenant == scope.Tenant && (a.kind == "tenant" || a.subject == scope.Subject)
}
func (c *stateDeletionCoordinator) runControlDeletionConsumer(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		_ = c.consumeControlDeletions(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
func (c *stateDeletionCoordinator) consumeControlDeletions(ctx context.Context) error {
	jobs, err := c.state.PendingControlDeletions(ctx, 32)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Bounded attempts do not retain an HTTP context, token or unbounded lock.
		attempt, cancel := context.WithTimeout(ctx, 30*time.Second)
		err = c.resumeControlDeletion(attempt, job)
		cancel()
		// One failing owner must not starve unrelated tenants in this batch.
		if err != nil {
			continue
		}
	}
	return nil
}
func (c *stateDeletionCoordinator) resumeControlDeletion(ctx context.Context, job state.ControlDeletion) error {
	if !job.ServiceAuthorized {
		return contracts.NewError(contracts.ErrForbidden, "ahp: deletion has no durable service authority")
	}
	var plan subjectPlan
	if err := json.Unmarshal(job.Plan, &plan); err != nil {
		return err
	}
	scope := plan.Scope
	if err := scope.Validate(); err != nil {
		return err
	}
	if scope.Tenant != job.Tenant || (job.Kind != "tenant" && scope.Subject != job.Subject) {
		return contracts.NewError(contracts.ErrForbidden, "ahp: deletion authority does not match its immutable target")
	}
	owners, ok := c.cascade.(subjectCascade)
	if !ok || owners.workspaceOwner() == nil {
		return contracts.NewError(contracts.ErrUnavailable, "ahp: deletion owners unavailable")
	}
	var release func()
	var err error
	if job.Kind == "tenant" {
		release, err = c.state.LockTenantControl(ctx, job.Tenant)
	} else {
		release, err = c.state.LockSubjectControl(ctx, job.Tenant, job.Subject)
	}
	if err != nil {
		return err
	}
	defer release()
	// Re-read after the lock so another consumer's completion cannot be lost.
	job, err = c.state.GetControlDeletion(ctx, job.Tenant, job.Actor, job.Key)
	if err != nil {
		return err
	}
	if job.Kind == "tenant" {
		_, err = c.continueControlTenant(withDeletionAuthority(ctx, job), scope, job)
	} else {
		_, err = c.continueControlSubject(withDeletionAuthority(ctx, job), scope, job, owners)
	}
	return err
}
