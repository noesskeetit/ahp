package harness

import (
	"crypto/ed25519"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
)

// mintTaskToken mirrors the self-sign path the harness used to carry (and no
// longer does): it signs a KindTask token exactly the way the old TokenMint did,
// leaving Actors nil so auth.Signer serialises an empty "act" claim. It lives in
// the test only -- production has no signing path -- so the test can prove the
// 403 root cause and that the guard catches it.
func mintTaskToken(t *testing.T, actors []string) string {
	t.Helper()
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = byte(i)
	}
	priv := ed25519.NewKeyFromSeed(seed)
	signer, err := auth.NewSigner("ahp", "dev-1", priv)
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	claims := auth.Claims{
		Audience:   "ahp",
		Kind:       auth.KindTask,
		Tenant:     "prof-run",
		Deployment: "prof-deploy",
		Subject:    "prof-load",
		Task:       "task-1",
		Root:       "task-1",
		Context:    "ctx-1",
		Grant:      "g-1",
		Actors:     actors,
	}
	tok, err := signer.Sign(claims, 60*time.Second)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return tok
}

// The empty actor chain is exactly what the platform answers with 403 "identity:
// token carries an empty actor chain" (internal/identity/tokens.go). This test
// pins the mechanism the State plane used to trip on, so a regression to a
// self-signed token is caught in the harness, not in a live run.
func TestSelfMintedTokenCarriesEmptyActorChain(t *testing.T) {
	tok := mintTaskToken(t, nil)
	actors, err := TokenActorChain(tok)
	if err != nil {
		t.Fatalf("TokenActorChain: %v", err)
	}
	if len(actors) != 0 {
		t.Fatalf("a self-signed token must decode to an empty actor chain (the 403 root cause); got %v", actors)
	}
}

// RequireNonEmptyActorChain is the fail-closed gate: it must refuse the empty
// chain and say which 403 it is pre-empting. Neutering this guard (always return
// nil) is the RED that makes this test and its sibling fail.
func TestRequireNonEmptyActorChainRefusesEmptyChain(t *testing.T) {
	tok := mintTaskToken(t, nil)
	err := RequireNonEmptyActorChain(tok, "state")
	if err == nil {
		t.Fatalf("guard must refuse a token with an empty actor chain instead of letting it reach CreateThread")
	}
	if !strings.Contains(err.Error(), "actor chain") {
		t.Fatalf("guard error should name the actor-chain 403; got %q", err.Error())
	}
}

// The live exchange yields a token whose actor chain is the live grant's chain, so
// the guard passes it. This is the GREEN half: a real product credential is not
// mistaken for a forged self-sign.
func TestRequireNonEmptyActorChainAcceptsLiveChain(t *testing.T) {
	tok := mintTaskToken(t, []string{"human:profiler", "prof-deploy"})
	if err := RequireNonEmptyActorChain(tok, "state"); err != nil {
		t.Fatalf("a token with a real actor chain must pass the guard; got %v", err)
	}
}

// A token that is not a JWS cannot be trusted either: the guard refuses it rather
// than defaulting to "authorized".
func TestRequireNonEmptyActorChainRefusesGarbage(t *testing.T) {
	if err := RequireNonEmptyActorChain("not-a-jwt", "state"); err == nil {
		t.Fatalf("an unreadable token must not pass the actor-chain guard")
	}
}

// The composition record the artifact publishes: every path the harness can take
// is a live product exchange with harness_self_signed=false and the state
// capability named, so acceptance can see at a glance the harness minted nothing.
func TestAuthorizationLedgerRecordsEverySessionNotLastWins(t *testing.T) {
	l := NewAuthorizationLedger()
	if l.HarnessSelfSigned {
		t.Fatalf("no harness authorization path may report harness_self_signed=true")
	}
	l.RecordPlane(PlaneMechanism("state", []string{"state"}))
	l.RecordPlane(PlaneMechanism("state", []string{"state"})) // idempotent per plane
	if len(l.Planes) != 1 {
		t.Fatalf("a plane must be recorded once; got %d", len(l.Planes))
	}
	if l.Planes[0].ProvisioningSeam == "" || l.Planes[0].TokenIssuer == "" || l.Planes[0].ExchangeURL == "" {
		t.Fatalf("plane mechanism must be fully stated, not blank: %+v", l.Planes[0])
	}
	if len(l.Planes[0].Capabilities) != 1 || l.Planes[0].Capabilities[0] != "state" {
		t.Fatalf("state plane must name capability [state]; got %v", l.Planes[0].Capabilities)
	}
	// Two worker sessions on the same report must BOTH survive -- the pre-fix code
	// overwrote rep.Authorization so a multi-worker ladder reported only its last
	// session. And each session's rotation count is read live at materialize time.
	var r1 int64 = 2
	var r2 int64 = 7
	l.RecordSession(SessionAuthorization{Tag: "c1", Plane: "state", Task: "t1", ExchangerDeployment: "dep1", Capabilities: []string{"state"}},
		func() int64 { return r1 }, func() string { return "" }, func() bool { return true })
	l.RecordSession(SessionAuthorization{Tag: "c5", Plane: "state", Task: "t2", ExchangerDeployment: "dep2", Capabilities: []string{"state"}},
		func() int64 { return r2 }, func() string { return "" }, func() bool { return true })
	l.materialize()
	if len(l.Sessions) != 2 {
		t.Fatalf("every session must be recorded, not last-wins; got %d", len(l.Sessions))
	}
	if l.Sessions[0].Tag != "c1" || l.Sessions[1].Tag != "c5" {
		t.Fatalf("sessions must keep their identity and order: %+v", l.Sessions)
	}
	if l.Sessions[0].TokenRotations != 2 || l.Sessions[1].TokenRotations != 7 {
		t.Fatalf("each session's live rotation count must be recorded; got %d and %d", l.Sessions[0].TokenRotations, l.Sessions[1].TokenRotations)
	}
	if !l.Sessions[0].RotationOutsideMeasurementWindow || !l.Sessions[1].RotationOutsideMeasurementWindow {
		t.Fatalf("live-rotated sessions must mark rotation_outside_measurement_window=true")
	}
}
