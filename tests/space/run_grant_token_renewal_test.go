package space_test

import (
	"context"
	"crypto/ed25519"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// A run's credential is renewed by State on ClaimRun, and that renewed token is exactly what
// Space later presents to roots/{task}/cancel to revoke the run's OWN grant. Measured on the
// composed stand: the run's child grant was live and did hold identity:cancel, yet CancelTurn
// was refused, because the token ClaimRun handed out was manufactured locally (Grant="dev",
// Context="dev", Depth=0, no actor chain) and identity.Store.Authorize must reject claims that
// do not describe the live grant. These assertions pin the boundary the wiring has to respect:
//
//   - a token renewed THROUGH Identity for the live child grant authorizes identity:cancel;
//   - a token with the same scope/task but locally fabricated delegation claims never does;
//   - once the run's grant is cancelled, even the genuine renewed token is refused, so a
//     cancelled run cannot keep acting on its credential.

const renewalAudience = "ahp"

func renewalHarness(t *testing.T) (*identity.Store, *auth.Signer, *auth.Verifier, contracts.Scope) {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		t.Skip("set AHP_TEST_DATABASE_URL to run the run-grant token renewal test")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	store := identity.New(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("identity migrate: %v", err)
	}
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	signer, err := auth.NewSigner("ahp", "dev-1", priv)
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	verifier := auth.NewVerifier("ahp", map[string]ed25519.PublicKey{"dev-1": pub})
	suffix := strings.NewReplacer("-", "", "_", "").Replace(contracts.NewID())
	scope := contracts.Scope{Tenant: "t-" + suffix, Deployment: "spacesvc", Subject: "subj-" + suffix}
	return store, signer, verifier, scope
}

// renewLiveRunGrant builds the stand's real shape: a human ingress authority root (allow_self,
// because Space delegates its own run under its own deployment) plus the server-owned child run
// task that carries the run's capabilities.
func renewLiveRunGrant(t *testing.T, store *identity.Store, ctx context.Context, scope contracts.Scope, rootTask, childTask string) {
	t.Helper()
	humanCaps := []string{"space:access", "artifact:read", "identity:delegate", "identity:cancel", "task:delegate"}
	// Context ids carry a GLOBAL primary key, so a context name belongs to exactly one scope:
	// every scope allocates its own ids instead of reusing a pretty name across runs.
	ctxID := suffixOf(scope) + "-c-" + childTask
	if _, err := store.CreateRoot(ctx, scope, identity.RootRequest{
		Task: rootTask, Context: ctxID, Deployment: scope.Deployment,
		Capabilities: humanCaps, TTL: time.Hour, AllowSelf: true,
	}); err != nil {
		t.Fatalf("create root: %v", err)
	}
	if _, err := store.Delegate(ctx, scope, rootTask, identity.DelegateRequest{
		Task: childTask, Context: ctxID, Deployment: scope.Deployment,
		Capabilities: []string{"space:access", "artifact:read", "identity:cancel"}, TTL: 15 * time.Minute,
	}); err != nil {
		t.Fatalf("delegate run grant: %v", err)
	}
}

func suffixOf(s contracts.Scope) string { return strings.TrimPrefix(s.Tenant, "t-") }

func TestRenewedRunTokenAuthorizesItsOwnCancel(t *testing.T) {
	store, signer, verifier, scope := renewalHarness(t)
	ctx := context.Background()
	rootTask := "space-authority:demo::renew-root"
	childTask := "rtask-demo:renew-child"
	renewLiveRunGrant(t, store, ctx, scope, rootTask, childTask)

	// This is what the fixed State wiring does on ClaimRun: ask Identity to renew the token of
	// the run's existing child grant, never to invent claims locally.
	renewed, err := store.MintTask(ctx, scope, childTask, signer, renewalAudience, 10*time.Minute)
	if err != nil {
		t.Fatalf("ClaimRun renewal must succeed on a live child grant: %v", err)
	}
	claims, err := verifier.Verify(renewed, renewalAudience, auth.KindTask, auth.WorkloadID(scope))
	if err != nil {
		t.Fatalf("verify renewed token: %v", err)
	}
	if err := store.Authorize(ctx, claims, "identity:cancel"); err != nil {
		t.Fatalf("a run token renewed through Identity must authorize its own cancel: %v", err)
	}

	// The token the stand used to hand out: same scope, same task, but delegation claims
	// fabricated by the caller. Authorize must refuse it, which is exactly why the old wiring
	// could never cancel a run.
	fabricated := claims
	fabricated.Grant = "dev"
	fabricated.Root = childTask
	fabricated.Context = "dev"
	fabricated.Depth = 0
	fabricated.Actors = nil
	if err := store.Authorize(ctx, fabricated, "identity:cancel"); err == nil {
		t.Fatal("locally fabricated delegation claims must never authorize a cancel")
	} else if !strings.Contains(err.Error(), "identity:") {
		t.Fatalf("expected an Identity refusal, got %v", err)
	}

	// After the run's own grant is cancelled, the genuine renewed token is dead too: cancel is
	// not a status flag, it revokes the credential.
	if err := store.CancelSubtree(ctx, scope, childTask); err != nil {
		t.Fatalf("cancel subtree: %v", err)
	}
	if err := store.Authorize(ctx, claims, "identity:cancel"); err == nil {
		t.Fatal("a cancelled run grant must not authorize anything any more")
	}
	if _, err := store.MintTask(ctx, scope, childTask, signer, renewalAudience, time.Minute); err == nil {
		t.Fatal("ClaimRun must not renew a token for a cancelled grant")
	}
}
