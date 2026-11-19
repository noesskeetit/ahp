package registry_test

// D-REG-1 / D-REG-2 (independent verification 20261110, reports/mac-director/
// registry-deploy-admission-verification-20261110.md): the deploy-admission gate
// judged every expiry against a clock it could not be shown, and it accepted a
// verdict stamped in the FUTURE as immediately current. Both are clock-authority
// defects, so both are asserted here by MOVING the supply-chain clock instead of
// sleeping or hoping the wall clock crosses a boundary mid-run.
//
// The clock seam is registry.TrustRoots.WithClock. Store.now() already defers to
// the trust roots, so one offset moves certificate validity, attestation
// lifetime, admission freshness and verdict freshness together -- which is what
// makes "admission expired while the verdicts are still fresh" reachable at all
// (D-REG-2's uncovered branch, internal/registry/admission.go requireDeployAdmission).
//
// These legs run against the real PostgreSQL instance and real operator PKI
// (newSupplyEnv), not the synthetic store: the freshness plane being measured is
// the PKI-governed one.

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

// clockedSupplyEnv is newSupplyEnv with a controllable supply-chain clock. The
// offset starts at zero: certificates are minted from and verified against real
// wall time, so a fixture that began on a shifted clock would fail in the PKI
// rather than in the gate under test. Legs shift the clock explicitly.
type clockedSupplyEnv struct {
	*supplyEnv
	offset atomic.Int64 // clock shift in nanoseconds, applied to every supply-chain reading
}

func newClockSupplyEnv(t *testing.T) *clockedSupplyEnv {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn(t))
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	auth := newAuthority(t, "operator-root-"+randSuffix(t))
	roots, err := registry.NewTrustRootsFromPEM(auth.caPEM)
	if err != nil {
		t.Fatalf("trust roots: %v", err)
	}
	e := &clockedSupplyEnv{}
	shifted := roots.WithClock(func() time.Time {
		return time.Now().UTC().Add(time.Duration(e.offset.Load()))
	})
	store := registry.New(pool, registry.WithEvalGate(evalsHarness(t)), registry.WithSupplyChain(shifted))
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("clocked supply migrate: %v", err)
	}
	e.supplyEnv = &supplyEnv{store: store, tenant: "t-" + randSuffix(t), auth: auth, pool: pool}
	return e
}

// shift moves the whole supply-chain clock by d from real time.
func (c *clockedSupplyEnv) shift(d time.Duration) { c.offset.Store(int64(d)) }

// requireFutureVerdictRefusal asserts an error is the specific clock-skew refusal:
// CONFLICT carries the plane, the reason names the cause, and the message says
// "ahead of the registry clock" -- not the generic "failing or expired" wording a
// stale verdict produces. Asserting the wording is the point: a future verdict
// that got rejected as "expired" would hide a broken authority clock behind an
// operational-looking refusal.
func requireFutureVerdictRefusal(t *testing.T, err error, what string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: want refusal for a verdict ahead of the registry clock, got success", what)
	}
	if code := contracts.Code(err); code != contracts.ErrConflict {
		t.Fatalf("%s: code = %s, want %s (err: %v)", what, code, contracts.ErrConflict, err)
	}
	if reason := contracts.ReasonOf(err); reason != registry.ReasonVerdictCheckedInFuture {
		t.Fatalf("%s: reason = %q, want %q (err: %v)", what, reason, registry.ReasonVerdictCheckedInFuture, err)
	}
	if msg := err.Error(); !strings.Contains(msg, "ahead of the registry clock") {
		t.Fatalf("%s: message must name the clock skew, got %q", what, msg)
	}
}

// (1) D-REG-1 write side. A security or restart-chaos clearance whose checked_at
// lies an hour ahead must be rejected when it is recorded, with a reason a client
// can branch on; a clearance inside the skew allowance is still accepted, so the
// guard compensates realistic clock skew instead of banning future stamps.
func TestSecurityAndChaosVerdictsRejectFutureCheckedAt(t *testing.T) {
	ctx := context.Background()
	e := newSupplyEnv(t)
	im := e.publishImage(t, "futurestamp")

	for _, kind := range []string{"security", "chaos"} {
		future := time.Now().UTC().Add(time.Hour)
		var err error
		switch kind {
		case "security":
			_, err = e.store.RecordSecurityVerdict(ctx, registry.SecurityVerdict{
				OCIDigest: "sha256:" + im.digestHex, AgentID: im.agent, Outcome: "pass",
				Source: "sec-authority", CheckedAt: future, ExpiresAt: future.Add(24 * time.Hour),
			})
		case "chaos":
			_, err = e.store.RecordChaosVerdict(ctx, registry.ChaosVerdict{
				OCIDigest: "sha256:" + im.digestHex, AgentID: im.agent, Outcome: "pass",
				Source: "stage-restart-chaos", CheckedAt: future, ExpiresAt: future.Add(24 * time.Hour),
			})
		}
		if err == nil {
			t.Fatalf("%s verdict with checked_at 1h in the future was accepted", kind)
		}
		if code := contracts.Code(err); code != contracts.ErrInvalid {
			t.Fatalf("%s future verdict: code = %s, want %s (err: %v)", kind, code, contracts.ErrInvalid, err)
		}
		if reason := contracts.ReasonOf(err); reason != registry.ReasonVerdictCheckedInFuture {
			t.Fatalf("%s future verdict: reason = %q, want %q", kind, reason, registry.ReasonVerdictCheckedInFuture)
		}
	}

	// Inside the allowance the write still succeeds: the guard is a skew bound,
	// not a ban on sub-second disagreement between authorities.
	near := time.Now().UTC().Add(30 * time.Second)
	if _, err := e.store.RecordSecurityVerdict(ctx, registry.SecurityVerdict{
		OCIDigest: "sha256:" + im.digestHex, AgentID: im.agent, Outcome: "pass",
		Source: "sec-authority", CheckedAt: near, ExpiresAt: near.Add(24 * time.Hour),
	}); err != nil {
		t.Fatalf("security verdict 30s ahead must be inside the skew allowance: %v", err)
	}
	if _, err := e.store.RecordChaosVerdict(ctx, registry.ChaosVerdict{
		OCIDigest: "sha256:" + im.digestHex, AgentID: im.agent, Outcome: "pass",
		Source: "stage-restart-chaos", CheckedAt: near, ExpiresAt: near.Add(24 * time.Hour),
	}); err != nil {
		t.Fatalf("chaos verdict 30s ahead must be inside the skew allowance: %v", err)
	}
}

// (2) D-REG-1 read side plus D-REG-2, on one promotion fixture. The same
// installation, the same two admitted images, three clock positions:
//
//	offset  0 : promotion is allowed  (control -- the fixture is genuinely clean)
//	offset +2h: the supply-chain record has expired while BOTH verdicts are still
//	            inside their 24h window. This is internal/registry/admission.go
//	            "supply-chain record ... has expired", a branch no leg reached
//	            before, because reaching it meant either waiting an hour or moving
//	            a clock.
//	offset -2h: the registry clock runs behind the clock that stamped the
//	            clearances, so the stored verdicts sit two hours in its future.
//	            They must be refused as a clock-skew refusal, not accepted (the
//	            old behaviour: now.After(ExpiresAt) is trivially false) and not
//	            reported as expired.
func TestPromoteExpiryAndFutureVerdictsUnderShiftedClock(t *testing.T) {
	ctx := context.Background()
	e := newClockSupplyEnv(t)

	// A: the currently serving image, cleared and admitted at real time.
	a := e.publishImage(t, "clockA")
	a.secID = e.recordSecurity(t, a, "pass", time.Now().UTC(), time.Now().Add(24*time.Hour))
	e.recordChaos(t, a, "pass", time.Now().UTC(), time.Now().Add(24*time.Hour))
	e.admit(t, a)
	inst := e.install(t, a, "stage")

	// B: the promotion target, same agent, cleared and admitted at real time.
	bm := manifest(t, "supply.clockskew")
	bImageRef, bDigestHex, _ := makeImage(t, "clockB-"+randSuffix(t))
	bm.Components[0].Image = bImageRef
	bv := publish(t, e.store, a.agent, bm)
	sbom, sbomDigest := makeSBOM(t, bImageRef)
	b := imageEnv{agent: a.agent, version: bv, imageRef: bImageRef, digestHex: bDigestHex, sbom: sbom, sbomDigest: sbomDigest}
	b.secID = e.recordSecurity(t, b, "pass", time.Now().UTC(), time.Now().Add(24*time.Hour))
	e.recordChaos(t, b, "pass", time.Now().UTC(), time.Now().Add(24*time.Hour))
	e.admit(t, b)

	// Control: at the real clock every plane is current and promotion succeeds.
	if _, err := e.store.Promote(ctx, e.tenant, inst.ID, bv.ID); err != nil {
		t.Fatalf("promotion with a clean clock must succeed: %v", err)
	}

	// D-REG-2: the supply-chain record expires first (its lifetime is the
	// attestation's, an hour), the security/chaos windows are 24h. Advancing the
	// clock past the record but not the verdicts must block promotion, and the
	// refusal must name the supply-chain record rather than a verdict.
	e.shift(2 * time.Hour)
	_, err := e.store.Promote(ctx, e.tenant, inst.ID, a.version.ID)
	if err == nil {
		t.Fatal("promotion onto a supply-chain record that expired while verdicts stayed fresh must be refused")
	}
	if code := contracts.Code(err); code != contracts.ErrConflict {
		t.Fatalf("expired-admission promotion: code = %s, want %s (err: %v)", code, contracts.ErrConflict, err)
	}
	msg := err.Error()
	if !strings.Contains(msg, "supply-chain record") || !strings.Contains(msg, "has expired") {
		t.Fatalf("expired-admission promotion must name the expired supply-chain record, got %q", msg)
	}
	if strings.Contains(msg, "security verdict") || strings.Contains(msg, "restart-chaos") {
		t.Fatalf("verdicts are still fresh at +2h; refusal must not blame them, got %q", msg)
	}

	// D-REG-1 read side: two hours BEHIND the clock that stamped the clearances.
	// The verdicts are not expired and they pass; only their timestamp leads the
	// reading clock, and that alone must stop the promotion.
	e.shift(-2 * time.Hour)
	_, err = e.store.Promote(ctx, e.tenant, inst.ID, a.version.ID)
	requireFutureVerdictRefusal(t, err, "promotion reading a clearance stamped ahead of the registry clock")

	// The refusal is about clock authority, not about the plane being empty or
	// failing: with the clock back in agreement the same promotion is accepted.
	e.shift(0)
	if _, err := e.store.Promote(ctx, e.tenant, inst.ID, a.version.ID); err != nil {
		t.Fatalf("promotion must be allowed again once the clocks agree: %v", err)
	}
}
