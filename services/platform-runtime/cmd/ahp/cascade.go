package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/stateapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
	"github.com/jackc/pgx/v5"
)

// The PDF makes deletion a control-plane cascade: "Удаление. Каскадом по
// контексту, субъекту и tenant", "удаление thread без хука агента", and the
// State API guarantee list ends with "каскадное удаление". State owns the
// thread; the artifact plane owns the bytes. Neither may reach into the other's
// schema, so the composition root - the only place that legitimately holds both
// planes - wires one onto the other: after State has really deleted a thread
// (or a subject), the artifacts written in that context are tombstoned through
// the artifact plane's own API, honouring its legal holds.
//
// This is deliberately not a hook in the agent, the SDK or the executor: a
// deleted thread must stop serving its artifacts even if the deploy that
// produced them is offline, uncooperative or written in a language that never
// heard of this file.

// cascadeAudience/capability mirror what the State face itself demands, so the
// cascade gate cannot accept a caller the State handler would refuse.
const (
	cascadeAudience   = "ahp"
	cascadeCapability = "state"
	// cascadeBodyLimit matches the State facade's own request bound.
	cascadeBodyLimit = 1 << 20
)

// cascadeCounters is the numeric outcome of one artifact cascade, for logs and
// for the acceptance record. It carries no identifiers and no token material.
type cascadeCounters struct {
	Tombstoned int `json:"tombstoned"`
	Held       int `json:"held"`
	// Referenced counts artifacts the cascade matched and deliberately left
	// alone because a durable reference outside the artifact plane still names
	// them (DEF-H24-15): a workspace candidate or revision, a turn frame, a
	// memory record. The deletion still succeeds; the bytes stay in use.
	Referenced     int `json:"referenced"`
	AlreadyDeleted int `json:"already_deleted"`
}

// artifactCascade is the artifact-plane half of the deletion cascade. Two
// implementations exist: the in-process one (composed topology, role "all") and
// the mTLS one that a separated State deployment uses to reach its data plane.
type artifactCascade interface {
	// cascadeThread tombstones the artifacts of scope written under one of
	// contextIDs or produced by one of taskIDs. An empty set simply matches
	// nothing.
	cascadeThread(ctx context.Context, scope contracts.Scope, contextIDs, taskIDs []string) (cascadeCounters, error)
	// cascadeSubject tombstones every artifact of scope.
	cascadeSubject(ctx context.Context, scope contracts.Scope) (cascadeCounters, error)
}

// withDeletionCascade installs completion behind the State facade's own auth.
// State commits dependency identifiers in its deletion transaction; retries use
// that receipt rather than attempting to reconstruct vanished rows.
func (s *server) withDeletionCascade(h http.Handler, st *state.Store, _ *auth.Verifier, control ...ControlDeletionConfig) http.Handler {
	cfg := ControlDeletionConfig{}
	if len(control) > 0 {
		cfg = control[0]
	}
	// Every owner mounts before this consumer; detach its registry from later
	// configuration changes so a durable retry never races a map mutation.
	cfg.TenantOwners = snapshotTenantOwners(cfg.TenantOwners)
	cfg.SubjectOwners = snapshotSubjectOwners(cfg.SubjectOwners)
	cfg.RequiredSubjectOwners = append([]string(nil), cfg.RequiredSubjectOwners...)
	coordinator := &stateDeletionCoordinator{state: st, cascade: s.cascade, control: cfg}
	wrapped, err := stateapi.WithDeletionCoordinator(h, coordinator)
	if err != nil {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrUnavailable, "ahp: deletion coordinator is not configured"))
		})
	}
	if cfg.ConsumerContext != nil {
		go coordinator.runControlDeletionConsumer(cfg.ConsumerContext)
	}
	return wrapped
}

type memoryDeletion interface {
	checkMemorySubject(context.Context, contracts.Scope) error
	deleteMemorySubject(context.Context, contracts.Scope, string) error
}
type stateDeletionCoordinator struct {
	state   *state.Store
	cascade artifactCascade
	control ControlDeletionConfig
}

func (c *stateDeletionCoordinator) Ready(ctx context.Context, scope contracts.Scope, operation string) error {
	if c.cascade == nil {
		return contracts.NewError(contracts.ErrUnavailable, "ahp: artifact deletion owner is unavailable")
	}
	if operation == "DeleteSubject" {
		if _, err := c.subjectOwnerNames(); err != nil {
			return err
		}
		hook, ok := c.cascade.(memoryDeletion)
		if !ok {
			return contracts.NewError(contracts.ErrUnavailable, "ahp: memory deletion owner is unavailable")
		}
		if err := hook.checkMemorySubject(ctx, scope); err != nil {
			return err
		}
	}
	return nil
}
func (c *stateDeletionCoordinator) Complete(ctx context.Context, scope contracts.Scope, operation, resource string) error {
	if err := c.Ready(ctx, scope, operation); err != nil {
		return err
	}
	receipt, err := c.state.GetDeletion(ctx, scope, operation, resource)
	if err != nil {
		return err
	}
	if receipt.Completed {
		return nil
	}
	// Erase subject memories before artifact reference evaluation, so memory
	// evidence removed by this same operation cannot keep orphan bytes alive.
	if !receipt.MemoryDone {
		hook := c.cascade.(memoryDeletion)
		if err = hook.deleteMemorySubject(ctx, scope, "delete-"+receipt.ID); err != nil {
			return contracts.NewError(contracts.ErrUnavailable, "ahp: memory deletion incomplete; retry with the same key")
		}
		if err = c.state.CompleteDeletionStep(ctx, scope, receipt.ID, "memory"); err != nil {
			return err
		}
	}
	if !receipt.ArtifactsDone {
		if operation == "DeleteSubject" && receipt.HeldCount == 0 {
			_, err = c.cascade.cascadeSubject(ctx, scope)
		} else {
			_, err = c.cascade.cascadeThread(ctx, scope, receipt.ContextIDs, receipt.TaskIDs)
		}
		if err != nil {
			return contracts.NewError(contracts.ErrUnavailable, "ahp: artifact deletion incomplete; retry with the same key")
		}
		if err = c.state.CompleteDeletionStep(ctx, scope, receipt.ID, "artifacts"); err != nil {
			return err
		}
	}
	return c.state.CompleteDeletionStep(ctx, scope, receipt.ID, "complete")
}

// taskContext resolves the context (thread) a task belongs to, scoped to the
// task's own tenant/deployment/subject. State is the source of that mapping;
// this reads it through the state store's pool rather than duplicating any of
// State's logic. It is used to stamp the context claim of a run's task token:
// without it every artifact a run writes would carry a context that names no
// thread, and the cascade would have nothing to select on.
func taskContext(ctx context.Context, st *state.Store, scope contracts.Scope, taskID string) string {
	if st == nil || taskID == "" {
		return ""
	}
	const q = `SELECT t.context_id
	             FROM ahp_state.threads t
	             JOIN ahp_state.runs r ON r.thread_id = t.id
	             WHERE r.task_id = $1 AND t.tenant = $2 AND t.deployment = $3 AND t.subject = $4
	             ORDER BY r.sequence DESC
	             LIMIT 1`
	var contextID string
	err := st.Pool().QueryRow(ctx, q, taskID, scope.Tenant, scope.Deployment, scope.Subject).
		Scan(&contextID)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			slog.Warn("task token context lookup failed", "err", err)
		}
		return ""
	}
	return contextID
}
