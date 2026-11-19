// Package identity: label helpers and the trusted AddLabels entry point.
//
// The transactional context journal and grant label accumulation live in
// journal.go; this file carries label-only utilities plus AddLabels.

package identity

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// stringsTrim and stringsContainsAny are thin stdlib aliases kept so the
// authorisation paths read as explicit, single-purpose guards.
func stringsTrim(v string) string             { return strings.TrimSpace(v) }
func stringsContainsAny(v, chars string) bool { return strings.ContainsAny(v, chars) }

// ContextLabels reads the label journal of one conversation context: the set the
// journal has accumulated for it under this exact scope.
//
// It is read-only and scope-bound like every other identity read -- a context of
// another tenant/deployment/subject is simply not found, so existence never leaks.
// It exists because a routing or egress decision needs the ceiling of the DATA in
// flight, and the only place that ceiling is kept is this journal: a deployment
// record says what a worker may reach, the context journal says what this
// conversation has become while it ran (labels only accumulate).
func (s *Store) ContextLabels(ctx context.Context, scope contracts.Scope, contextID string) (labels.Set, bool, error) {
	if err := scope.Validate(); err != nil {
		return labels.Set{}, false, err
	}
	if stringsTrim(contextID) == "" {
		return labels.Set{}, false, contracts.NewError(contracts.ErrInvalid, "context id must not be empty")
	}
	var out labels.Set
	var found bool
	err := s.runTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		set, ok, err := readContext(ctx, tx, scope, contextID)
		if err != nil {
			return err
		}
		out, found = set, ok
		return nil
	})
	if err != nil {
		return labels.Set{}, false, err
	}
	return out, found, nil
}

// AddLabels widens the visibility set of one task and propagates it up the
// whole delegation chain in a single transaction.
//
// The added set is validated and normalized into its canonical sorted,
// de-duplicated union form; labels only ever accumulate. Under the root
// advisory lock the target must be usable, and every grant of the ancestor
// chain (root included) plus the context journal of each level is widened, so
// a private child can never be read through an untainted parent. Sibling
// grants and their contexts are never touched. A revoked subject is denied
// here exactly as it is for delegation. The freshly read grant is returned.
func (s *Store) AddLabels(ctx context.Context, scope contracts.Scope, task string,
	added labels.Set) (Grant, error) {
	if err := scope.Validate(); err != nil {
		return Grant{}, err
	}
	if err := validateTask(task); err != nil {
		return Grant{}, err
	}
	if err := validateLabelSet(added); err != nil {
		return Grant{}, err
	}
	added = nonnilSet(labels.Merge(added))

	var out Grant
	err := s.runTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		if err := denyIfSubjectRevoked(ctx, tx, scope); err != nil {
			return err
		}
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

		chain, err := ancestorChain(ctx, tx, g)
		if err != nil {
			return err
		}
		for _, node := range chain {
			if err := accumulateGrantLabels(ctx, tx, node.ID, added); err != nil {
				return err
			}
			if _, err := ensureContext(ctx, tx, node.Scope(), node.Context, added); err != nil {
				return err
			}
		}

		fresh, found, err := findGrant(ctx, tx, scope, task)
		if err != nil {
			return err
		}
		if !found {
			return contracts.NewError(contracts.ErrConflict, "identity: grant vanished while labelling")
		}
		out = fresh
		return nil
	})
	if err != nil {
		return Grant{}, err
	}
	return out, nil
}
