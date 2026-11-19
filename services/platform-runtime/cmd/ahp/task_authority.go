package main

import (
	"context"
	"time"

	"github.com/ahp-platform/ahp/internal/stateapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// runTokenTTL is the lifetime State asks Identity for when it renews a run's task token on
// ClaimRun. It stays under Identity's 15 minute hard ceiling and matches the lifetime the dev
// issuer used, so a renewed credential is never shorter than the lease it was issued for.
const runTokenTTL = 10 * time.Minute

// Authority modes reported at startup. The mode is never a secret and never optional at
// runtime: whichever one is chosen, the operator is told which credential the runs carry.
const (
	authorityIdentityMode = "identity"
	authorityDevMode      = "dev"
)

// grantTokenRenewer is the part of the Identity authority State needs in order to stop
// fabricating run credentials: renew a token for an EXISTING grant, and re-check a presented
// token against the live grant. *identity.Store implements it; the test doubles it.
type grantTokenRenewer interface {
	MintTask(ctx context.Context, scope contracts.Scope, task string, signer *auth.Signer,
		audience string, ttl time.Duration) (string, error)
	Authorize(ctx context.Context, claims auth.Claims, capability string) error
}

// stateTaskAuthority is State's task-plane credential policy: how a run's token is produced on
// ClaimRun, and what makes a presented token sufficient for a capability.
type stateTaskAuthority struct {
	issue     stateapi.TaskIssuer
	authorize transport.Authorize
	mode      string
}

// permissiveDevAuthority is the legacy shortcut: State mints its own claims and accepts them
// without asking Identity. It is NOT the PDF authority chain - a credential like this cannot
// revoke anything, because it never names a live grant - so callers must log the mode.
func permissiveDevAuthority(issue stateapi.TaskIssuer) stateTaskAuthority {
	return stateTaskAuthority{
		issue:     issue,
		authorize: func(context.Context, auth.Claims, string) error { return nil },
		mode:      authorityDevMode,
	}
}

// selectStateTaskAuthority decides who owns the run credential.
//
// ClaimRun renews the token a run executes with, and Space presents that very token to
// roots/{task}/cancel to revoke the run's OWN grant. Claims written by State itself (grant id
// "dev", empty actor chain) can never satisfy Identity's Authorize, so a self-minted run
// credential is a credential that cannot be cancelled. Once the Agent Space human composition is
// mounted the authority chain IS the product, so State asks Identity to renew the live child
// grant and re-checks every task-plane call against it.
func selectStateTaskAuthority(browserCompositionEnabled bool, renewer grantTokenRenewer,
	signer *auth.Signer, devIssue stateapi.TaskIssuer) stateTaskAuthority {
	if !browserCompositionEnabled || renewer == nil || signer == nil {
		return permissiveDevAuthority(devIssue)
	}
	return stateTaskAuthority{
		issue: func(ctx context.Context, scope contracts.Scope, taskID string) (string, error) {
			return renewer.MintTask(ctx, scope, taskID, signer, stateapiAudience, runTokenTTL)
		},
		authorize: renewer.Authorize,
		mode:      authorityIdentityMode,
	}
}

// stateapiAudience is the fixed task-token audience of the State API (internal/stateapi); the
// renewal has to ask for exactly the audience State's verifier expects.
const stateapiAudience = "ahp"
