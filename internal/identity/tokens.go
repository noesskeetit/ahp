// Package identity: task token minting and authorisation.
//
// MintTask turns one live grant into a short-lived Ed25519 task token bound to
// the workload identity of the grant scope. Authorize is the transport
// callback that re-checks every presented token against the live grant, so a
// revocation takes effect on the next request even though the token itself is
// still cryptographically valid.

package identity

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// DefaultTokenTTL is the lifetime of a minted task token when the caller does
// not ask for a shorter one. It is always additionally clamped to the
// remaining lifetime of the underlying grant.
const DefaultTokenTTL = 5 * time.Minute

// minTokenTTL is the smallest lifetime that can be minted. A grant that is
// about to expire fails closed instead of producing a token that dies in
// flight.
const minTokenTTL = time.Second

// MintTask mints a task token for one grant of the caller scope.
//
// The grant is re-read under the root advisory lock and must be usable: the
// subject must carry no tombstone and the whole ancestor chain must be active
// and unexpired by the PostgreSQL clock. The token lifetime defaults to
// DefaultTokenTTL, is invalid when negative or above MaxTokenTTL, and is
// clamped to the remaining grant lifetime; a remaining lifetime below
// minTokenTTL fails closed. A grant is never extended, and no local clock
// difference can widen the expiry: the ceiling is the grant expiry read from
// the database.
//
// The token itself is never stored. Only the audit row of issued_tokens is
// written inside the same transaction: grant id, workload identity and the
// granted expiry, never the credential. A token returned after a concurrent
// revocation is harmless because every request re-checks the live grant
// through Authorize.
func (s *Store) MintTask(ctx context.Context, scope contracts.Scope, task string,
	signer *auth.Signer, audience string, requestedTTL time.Duration) (string, error) {
	return s.mintTask(ctx, scope, task, signer, audience, requestedTTL, "", false)
}

// MintAuthorityTask mints the per-session ingress authority token for the
// human -> Space -> scoped-task browser exchange. It is identical to MintTask
// except that the signed token carries the optional ctx_scope=authority claim,
// which widens its Artifacts read scope to the subject+tenant (a human
// downloads their own artifacts across conversations). Only the browser-exchange
// mint calls it; delegated children and renewals go through MintTask and so
// never inherit the claim.
func (s *Store) MintAuthorityTask(ctx context.Context, scope contracts.Scope, task string,
	signer *auth.Signer, audience string, requestedTTL time.Duration) (string, error) {
	return s.mintTask(ctx, scope, task, signer, audience, requestedTTL, auth.CtxScopeAuthority, false)
}

func (s *Store) mintTask(ctx context.Context, scope contracts.Scope, task string,
	signer *auth.Signer, audience string, requestedTTL time.Duration, ctxScope string, workloadBound bool) (string, error) {

	if err := scope.Validate(); err != nil {
		return "", err
	}
	if err := validateTask(task); err != nil {
		return "", err
	}
	if signer == nil {
		return "", invalid("identity: a signer is required to mint a task token")
	}
	if audience == "" {
		return "", invalid("identity: audience is required to mint a task token")
	}
	switch {
	case requestedTTL < 0:
		return "", invalid("identity: token ttl must not be negative")
	case requestedTTL == 0:
		requestedTTL = DefaultTokenTTL
	case requestedTTL > MaxTokenTTL:
		return "", invalid("identity: token ttl exceeds the 15 minute maximum")
	}

	// A workload-only call first resolves the owner for policy evaluation. The
	// same constrained lookup is repeated inside the mint transaction, so a
	// changed owner or a root/child collision cannot select a different subject.
	var ownerID string
	if workloadBound {
		err := s.runTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
			g, found, err := findWorkloadGrant(ctx, tx, scope, task)
			if err != nil {
				return err
			}
			if !found {
				return issueDenied()
			}
			scope, ownerID = g.Scope(), g.ID
			return nil
		})
		if err != nil {
			return "", err
		}
	}
	find := func(ctx context.Context, tx pgx.Tx) (Grant, bool, error) {
		if !workloadBound {
			return findGrant(ctx, tx, scope, task)
		}
		g, found, err := findWorkloadGrant(ctx, tx, scope, task)
		if err != nil {
			return Grant{}, false, err
		}
		if !found || g.ID != ownerID || g.Scope() != scope {
			return Grant{}, false, issueDenied()
		}
		return g, true, nil
	}

	if s.resources != nil {
		g, err := s.Get(ctx, scope, task)
		if err != nil {
			return "", err
		}
		if err := s.requireContainers(ctx, scope, g.Capabilities, g.ID, g.Epoch); err != nil {
			return "", err
		}
	}
	workload := auth.WorkloadID(scope)
	var (
		token string
		ttl   time.Duration
	)
	err := s.runTx(ctx, func(ctx context.Context, tx pgx.Tx) error {
		g, found, err := find(ctx, tx)
		if err != nil {
			return err
		}
		if !found {
			return contracts.NewError(contracts.ErrNotFound, "identity: grant not found")
		}
		if err := lockRoot(ctx, tx, g.Root); err != nil {
			return err
		}
		g, found, err = find(ctx, tx)
		if err != nil {
			return err
		}
		if !found {
			return contracts.NewError(contracts.ErrNotFound, "identity: grant not found")
		}
		if revoked, err := subjectRevoked(ctx, tx, scope); err != nil {
			return err
		} else if revoked {
			return forbidden("identity: subject is revoked and may not mint a task token")
		}
		if err := checkUsable(ctx, tx, g); err != nil {
			return err
		}

		// The remaining lifetime is measured by the PostgreSQL clock, so a
		// skew between this process and the database can never extend a
		// grant: the expiry ceiling is the stored grant expiry.
		var remainingSeconds float64
		if err := tx.QueryRow(ctx,
			`SELECT GREATEST(EXTRACT(EPOCH FROM ($1 - clock_timestamp())), 0)::double precision`,
			g.ExpiresAt).Scan(&remainingSeconds); err != nil {
			return fmt.Errorf("identity: read grant remaining lifetime: %w", err)
		}
		remaining := time.Duration(remainingSeconds * float64(time.Second)).Truncate(time.Second)

		ttl = requestedTTL
		if remaining < ttl {
			ttl = remaining
		}
		if ttl < minTokenTTL {
			return forbidden("identity: grant expires too soon to mint a task token")
		}

		claims := auth.Claims{
			Subject:    g.Subject,
			Audience:   audience,
			Kind:       auth.KindTask,
			Tenant:     g.Tenant,
			Deployment: g.Deployment,
			Task:       g.Task,
			Root:       g.Root,
			Context:    g.Context,
			CtxScope:   ctxScope,
			Grant:      g.ID,
			Depth:      g.Depth,
			Actors:     append([]string(nil), g.Actors...),
		}
		// Local Ed25519 signing only: no external call ever happens while
		// this transaction is open.
		signed, err := signer.Sign(claims, ttl)
		if err != nil {
			return err
		}

		// Audit only: grant id, workload and expiry. The credential is never
		// persisted anywhere.
		if _, err := tx.Exec(ctx, `
		    INSERT INTO ahp_identity.issued_tokens (grant_id, workload, expires_at)
		    VALUES ($1, $2, clock_timestamp() + make_interval(secs => $3))
		    ON CONFLICT (grant_id) DO UPDATE
		        SET workload = EXCLUDED.workload,
		            expires_at = EXCLUDED.expires_at,
		            created_at = clock_timestamp()`,
			g.ID, workload, ttlSeconds(ttl)); err != nil {
			return fmt.Errorf("identity: record issued token: %w", err)
		}

		token = signed
		return nil
	})
	if err != nil {
		return "", err
	}
	return token, nil
}

// Authorize is the transport.Authorize callback for task endpoints.
//
// It accepts nothing that does not describe itself as a task access token,
// then re-evaluates the grant the token names against the live database: the
// grant must exist inside the token scope, must be active and unexpired along
// its whole ancestor chain and must hold the requested capability verbatim.
// Grant id, root, context, depth and actor chain must match the token exactly,
// and the token subject must be the original subject the grant belongs to, so
// a token can never be replayed against another grant, context or actor
// chain. Every mismatch is FORBIDDEN; the error never quotes token contents.
func (s *Store) Authorize(ctx context.Context, claims auth.Claims, capability string) error {
	if claims.Kind != auth.KindTask {
		return forbidden("identity: only task tokens may authorise a workload call")
	}
	if claims.TokenUse != auth.TokenUse {
		return forbidden("identity: token_use must be an access token")
	}
	if capability == "" {
		return invalid("identity: capability must not be empty")
	}
	if capability != stringsTrim(capability) || stringsContainsAny(capability, "*?%[") {
		return forbidden("identity: wildcard or malformed capabilities are never authorised")
	}
	scope := claims.Scope()
	if err := scope.Validate(); err != nil {
		return forbidden("identity: token scope is not a valid isolation scope")
	}
	if claims.Grant == "" || claims.Root == "" || claims.Context == "" || claims.Task == "" {
		return forbidden("identity: token is missing the delegation claims")
	}
	if len(claims.Actors) == 0 {
		return forbidden("identity: token carries an empty actor chain")
	}

	grant, err := s.Evaluate(ctx, scope, claims.Task, capability)
	if err != nil {
		if contracts.Code(err) == contracts.ErrNotFound {
			return forbidden("identity: the grant named by the token does not exist in its scope")
		}
		return err
	}
	if grant.ID != claims.Grant {
		return forbidden("identity: token grant id does not match the live grant")
	}
	if grant.Root != claims.Root {
		return forbidden("identity: token root does not match the live grant")
	}
	if grant.Context != claims.Context {
		return forbidden("identity: token context does not match the live grant")
	}
	if grant.Depth != claims.Depth {
		return forbidden("identity: token depth does not match the live grant")
	}
	if !sameStrings(grant.Actors, claims.Actors) {
		return forbidden("identity: token actor chain does not match the live grant")
	}
	return s.requireContainers(ctx, scope, []string{capability}, grant.ID, grant.Epoch)
}

// sameStrings compares two string slices element by element.
func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// compile-time guard: Authorize must stay compatible with the transport
// authorisation callback shape.
var _ = func(ctx context.Context, claims auth.Claims, capability string, s *Store) error {
	return s.Authorize(ctx, claims, capability)
}

// stringsNew trims and validates nothing; it exists so the authorisation
// guards above read as single-purpose checks.
var _ = strings.TrimSpace
