// Package identity: root grant creation.
package identity

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// errReplay signals an idempotent replay: the stored grant, not an error
// body, is the answer, so the transaction returns the original grant instead
// of writing anything.
var errReplay = errors.New("identity: idempotent replay")

// Default ceilings of a fresh root when the caller does not state narrower
// ones live in store.go (DefaultMaxDepth, DefaultMaxFanout, DefaultMaxActive)
// alongside the hard ceilings they may never exceed.

// CreateRoot opens (or replays) a delegation tree for one task.
//
// The task id is the idempotency key inside the caller scope: replaying the
// identical body returns the original grant, replaying it with a different
// body fails with KEY_REUSE_MISMATCH. The grant inherits the label set of its
// context journal, creating that journal on first use. A revoked subject can
// never open a root, and the request deployment must equal the caller scope
// deployment so a body can never choose where it runs.
func (s *Store) CreateRoot(ctx context.Context, scope contracts.Scope, r RootRequest) (Grant, error) {
	if err := scope.Validate(); err != nil {
		return Grant{}, err
	}
	if r.Deployment == "" {
		return Grant{}, invalid("identity: root request deployment is required")
	}
	if r.Deployment != scope.Deployment {
		return Grant{}, forbidden("identity: root request deployment does not match the caller scope")
	}
	if err := validateTask(r.Task); err != nil {
		return Grant{}, err
	}
	if err := validateContext(r.Context); err != nil {
		return Grant{}, err
	}
	caps, err := normalizeCapabilities(r.Capabilities)
	if err != nil {
		return Grant{}, err
	}
	if err := s.requireContainers(ctx, scope, caps, "", 0); err != nil {
		return Grant{}, err
	}
	ttl, err := normalizeTTL(r.TTL)
	if err != nil {
		return Grant{}, err
	}
	// Limits are normalized before the digest so an omitted limit and an
	// explicit default are the same canonical request.
	maxDepth, maxFanout, maxActive, err := normalizeLimits(r)
	if err != nil {
		return Grant{}, err
	}
	digest, err := rootDigest(scope, r, caps, ttl, maxDepth, maxFanout, maxActive, r.AllowSelf)
	if err != nil {
		return Grant{}, err
	}

	var out Grant
	err = s.runTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))", "identity-subject-erasure/"+scope.Tenant+"/"+scope.Subject); err != nil {
			return err
		}
		// Serialise identical (scope, task) creates before any read or
		// insert: concurrent replays then observe one another's grant
		// instead of racing the scope-task unique index.
		if err := lockTaskKey(ctx, tx, scope, r.Task); err != nil {
			return err
		}
		// Same-scope context reference, checked before anything else can
		// fail: a context id that exists under another scope is invisible
		// here and reports NOT_FOUND, so a foreign reference never leaks
		// the other scope's data and never reaches the global contexts_pk.
		if _, found, err := readContext(ctx, tx, scope, r.Context); err != nil {
			return err
		} else if !found {
			// Not yet created in this scope: claim it now. A concurrent
			// or foreign claim surfaces as the contexts_pk violation.
			if _, err := ensureContext(ctx, tx, scope, r.Context, labels.Set{}); err != nil {
				if isContextForeignClaim(err) || isUniqueViolation(err) {
					return contracts.NewError(contracts.ErrNotFound,
						"identity: context id belongs to another scope")
				}
				return err
			}
		}
		revoked, err := subjectRevoked(ctx, tx, scope)
		if err != nil {
			return err
		}
		if revoked {
			return forbidden("identity: subject is revoked and may not start a new root task")
		}

		existing, found, err := findGrant(ctx, tx, scope, r.Task)
		if err != nil {
			return err
		}
		if found {
			if err := replay(existing, digest); err != nil {
				if errors.Is(err, errReplay) {
					out = existing
					return nil
				}
				return err
			}
			out = existing
			return nil
		}

		// Same-scope context reference: ensureContext is keyed by the full
		// caller scope, so a context id that already exists under another
		// scope can never be adopted; the insert would hit the global
		// contexts_pk and is reported as NOT_FOUND below. The id is bound
		// into the body digest, so a mismatched body reusing a foreign
		// context id reports KEY_REUSE without leaking the original body.
		journal, err := ensureContext(ctx, tx, scope, r.Context, labels.Set{})
		if err != nil {
			if isContextForeignClaim(err) || isUniqueViolation(err) {
				return contracts.NewError(contracts.ErrNotFound,
					"identity: context id belongs to another scope")
			}
			return err
		}

		id := contracts.NewID()
		q := `INSERT INTO ahp_identity.grants_root
    (id, root, parent, tenant, deployment, subject, task, context_id,
     status, epoch, depth, actors, capabilities,
     labels_subjects, labels_workspaces, labels_taints,
     max_depth, max_fanout, max_active, allow_self, body_digest,
     issued_at, expires_at)
VALUES ($1, $1, '', $2, $3, $4, $5, $6,
     'active', 1, 0, $7, $8, $9, $10, $11,
     $12, $13, $14, $15, $16, clock_timestamp(),
     clock_timestamp() + make_interval(secs => $17))
RETURNING 'root' AS kind, ` + grantColumns
		created, found, err := scanGrants(tx.Query(ctx, q,
			id, scope.Tenant, scope.Deployment, scope.Subject, r.Task, r.Context,
			[]string{scope.Deployment}, caps,
			nonNil(journal.Subjects), nonNil(journal.Workspaces), nonNil(journal.Taints),
			maxDepth, maxFanout, maxActive, r.AllowSelf,
			digest, ttlSeconds(ttl),
		))
		if err != nil {
			if isUniqueViolation(err) {
				return contracts.NewError(contracts.ErrConflict,
					"identity: root task was created concurrently; read it and retry")
			}
			if isForeignKeyViolation(err) {
				return forbidden("identity: subject was revoked concurrently and may not start a root task")
			}
			return fmt.Errorf("identity: create root: %w", err)
		}
		// Read back by RETURNING, not by a second (scope, task) predicate: see
		// the same choice in delegate.go and the R4 measurement behind it.
		if !found {
			return contracts.NewError(contracts.ErrConflict, "identity: root grant vanished during creation")
		}
		out = created
		return nil
	})
	if err != nil {
		return Grant{}, err
	}
	return out, nil
}

// replay decides what a repeated create under an existing task id means: an
// identical body is a replay the caller must observe, a different body is a
// key reuse.
func replay(existing Grant, digest string) error {
	if existing.bodyDigest != digest {
		return contracts.NewError(contracts.ErrKeyReuse,
			"identity: task id was already used with a different canonical request")
	}
	return errReplay
}

// Get returns one grant by task id inside the caller scope. A grant belonging
// to another tenant, deployment or subject is NOT_FOUND, so existence never
// leaks across isolation boundaries.
func (s *Store) Get(ctx context.Context, scope contracts.Scope, task string) (Grant, error) {
	if err := scope.Validate(); err != nil {
		return Grant{}, err
	}
	if err := validateTask(task); err != nil {
		return Grant{}, err
	}
	var out Grant
	err := s.runTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		g, found, err := findGrant(ctx, tx, scope, task)
		if err != nil {
			return err
		}
		if !found {
			return contracts.NewError(contracts.ErrNotFound, "identity: grant not found")
		}
		out = g
		return nil
	})
	if err != nil {
		return Grant{}, err
	}
	return out, nil
}

// Evaluate is the live authority check for one capability on one task.
//
// It re-reads the grant and its whole ancestor chain under the root lock and
// rejects unless every level is active and unexpired by the PostgreSQL clock
// and the requested capability is present verbatim. A revoked or expired
// ancestor fails even while the leaf row still says active.
//
// This is the current-grant half of authorisation: the resource-level IAM
// adapter is wired separately and nothing here validates corporate IAM.
func (s *Store) Evaluate(ctx context.Context, scope contracts.Scope, task, capability string) (Grant, error) {
	if err := scope.Validate(); err != nil {
		return Grant{}, err
	}
	if err := validateTask(task); err != nil {
		return Grant{}, err
	}
	if capability == "" {
		return Grant{}, invalid("identity: capability must not be empty")
	}
	if capability != stringsTrim(capability) || stringsContainsAny(capability, "*?%[") {
		return Grant{}, forbidden("identity: wildcard or malformed capabilities are never authorised")
	}

	var out Grant
	err := s.runTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		g, found, err := findGrant(ctx, tx, scope, task)
		if err != nil {
			return err
		}
		if !found {
			return contracts.NewError(contracts.ErrNotFound, "identity: grant not found")
		}
		if err := lockRoot(ctx, tx, g.Root); err != nil {
			return err
		}
		g, found, err = findGrant(ctx, tx, scope, task)
		if err != nil {
			return err
		}
		if !found {
			return contracts.NewError(contracts.ErrNotFound, "identity: grant not found")
		}
		if err := checkUsable(ctx, tx, g); err != nil {
			return err
		}
		// checkUsable walks the ancestor chain from grants_child, so a root
		// leaf needs its own status and expiry checked here as well.
		if g.Status != StatusActive {
			return forbidden("identity: grant is revoked")
		}
		var alive bool
		if err := tx.QueryRow(ctx,
			`SELECT clock_timestamp() < $1::timestamptz`, g.ExpiresAt).Scan(&alive); err != nil {
			return fmt.Errorf("identity: check root expiry: %w", err)
		}
		if !alive {
			return forbidden("identity: grant has expired")
		}
		held := false
		for _, c := range g.Capabilities {
			if c == capability {
				held = true
				break
			}
		}
		if !held {
			return forbidden("identity: grant does not hold capability " + capability)
		}
		out = g
		return nil
	})
	if err != nil {
		return Grant{}, err
	}
	return out, nil
}
