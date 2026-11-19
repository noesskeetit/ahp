package gateway

// Legs for the artifact an Invoke named. The audit fact these legs answer to is
// that nothing connected a chosen runtime to the request that ran: no pin row, no
// digest in task metadata, so a substitute could not be detected. Verification
// lives on the receiving side because only the receiver can refuse before
// anything runs.
//
// Scope note for the integrator: these legs prove the RECEIVING side's decision
// table. The end-to-end leg -- two real installation images where the invoke
// demonstrably reaches the one it selected -- belongs to ARCH-03 and is NOT_RUN
// here; nothing in this file claims it.

import (
	"context"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Well-formed artifact digests: the canonical shape the registry publishes.
var (
	invDigestA = "sha256:" + strings.Repeat("a1", 32)
	invDigestB = "sha256:" + strings.Repeat("b2", 32)
)

// fakeInstallations is the registry surface this plane needs, and nothing more.
type fakeInstallations struct {
	version string
	digest  string
	err     error
	calls   int
	tenant  string
	id      string
}

func (f *fakeInstallations) InstallationArtifact(_ context.Context, tenant, installationID string) (string, string, error) {
	f.calls++
	f.tenant = tenant
	f.id = installationID
	if f.err != nil {
		return "", "", f.err
	}
	return f.version, f.digest, nil
}

func invocationClaim(installation, version, digest string) map[string]any {
	return map[string]any{
		MetaInvocation: map[string]any{
			"installation_id": installation,
			"version_id":      version,
			"version_digest":  digest,
		},
	}
}

func serviceWith(reader InstalledArtifactReader) *Service {
	return &Service{cfg: Config{Invocations: reader}}
}

func reasonOf(t *testing.T, err error) string {
	t.Helper()
	if reason := contracts.ReasonOf(err); reason != "" {
		return reason
	}
	t.Fatalf("refusal %v carries no contract reason; an operator cannot act on it", err)
	return ""
}

// TestNoClaimChangesNothing: the frames every existing acceptance path sends carry
// no invocation claim, and their behaviour must be identical to today's.
func TestNoClaimChangesNothing(t *testing.T) {
	reader := &fakeInstallations{version: "v1", digest: invDigestA}
	pinned, err := serviceWith(reader).verifyInvocation(context.Background(), contracts.Scope{Tenant: "t1"}, false, map[string]any{"ahp.other": "x"})
	if err != nil {
		t.Fatalf("a frame with no claim was refused: %v", err)
	}
	if pinned != nil {
		t.Fatalf("a frame with no claim gained a pin record: %+v", pinned)
	}
	if pinned.metadata() != nil {
		t.Fatal("an absent claim must journal nothing")
	}
	if reader.calls != 0 {
		t.Fatalf("the registry was read %d times for a frame that asked nothing", reader.calls)
	}
}

// TestSubstitutedArtifactIsRefused is the core refusal: the installation serves one
// digest, the invocation names another.
func TestSubstitutedArtifactIsRefused(t *testing.T) {
	reader := &fakeInstallations{version: "v-actual", digest: invDigestA}
	pinned, err := serviceWith(reader).verifyInvocation(context.Background(),
		contracts.Scope{Tenant: "t1"}, false, invocationClaim("inst-1", "v-named", invDigestB))
	if err == nil {
		t.Fatalf("a substituted artifact was accepted and pinned as %+v", pinned)
	}
	if got := reasonOf(t, err); got != ReasonInvocationMismatch {
		t.Fatalf("reason = %q, want %q", got, ReasonInvocationMismatch)
	}
	msg := err.Error()
	// Both identities are named: an operator reading one log line must be able to
	// say which artifact answered and which one was asked for.
	if !strings.Contains(msg, invDigestA) || !strings.Contains(msg, invDigestB) {
		t.Fatalf("the refusal must name both the served and the named digest, got: %s", msg)
	}
	if !strings.Contains(msg, "v-actual") || !strings.Contains(msg, "v-named") {
		t.Fatalf("the refusal must name both versions, got: %s", msg)
	}
	if reader.tenant != "t1" {
		t.Fatalf("the installation was read under tenant %q: the check must be tenant-scoped", reader.tenant)
	}
}

// TestMatchingClaimPinsTheArtifact: the positive case, with exactly the two markers
// this plane journals.
func TestMatchingClaimPinsTheArtifact(t *testing.T) {
	reader := &fakeInstallations{version: "v1", digest: invDigestA}
	pinned, err := serviceWith(reader).verifyInvocation(context.Background(),
		contracts.Scope{Tenant: "t1"}, false, invocationClaim("inst-1", "v1", invDigestA))
	if err != nil {
		t.Fatalf("a matching claim was refused: %v", err)
	}
	md := pinned.metadata()
	if md[MetaPinnedVersion] != "v1" || md[MetaPinnedDigest] != invDigestA {
		t.Fatalf("journal markers = %v, want the verified version and digest", md)
	}
	if reader.id != "inst-1" || reader.tenant != "t1" {
		t.Fatalf("read the installation %q of tenant %q, want inst-1/t1", reader.id, reader.tenant)
	}
}

// TestUnverifiableClaimIsRefusedNotIgnored: an unusable claim must never degrade
// into "no claim was made".
func TestUnverifiableClaimIsRefusedNotIgnored(t *testing.T) {
	cases := map[string]map[string]any{
		"claim is not an object": {MetaInvocation: "inst-1,v1,sha256:aa"},
		"installation missing":   {MetaInvocation: map[string]any{"version_id": "v1", "version_digest": invDigestA}},
		"version missing":        {MetaInvocation: map[string]any{"installation_id": "i1", "version_digest": invDigestA}},
		"digest missing":         {MetaInvocation: map[string]any{"installation_id": "i1", "version_id": "v1"}},
		"digest not canonical":   {MetaInvocation: map[string]any{"installation_id": "i1", "version_id": "v1", "version_digest": "abcd1234"}},
		"digest uppercase":       {MetaInvocation: map[string]any{"installation_id": "i1", "version_id": "v1", "version_digest": strings.ToUpper(invDigestA)}},
		"identifiers unbounded":  {MetaInvocation: map[string]any{"installation_id": strings.Repeat("i", 200), "version_id": "v1", "version_digest": invDigestA}},
	}
	for name, claim := range cases {
		t.Run(name, func(t *testing.T) {
			reader := &fakeInstallations{version: "v1", digest: invDigestA}
			_, err := serviceWith(reader).verifyInvocation(context.Background(), contracts.Scope{Tenant: "t1"}, false, claim)
			if err == nil {
				t.Fatal("an unusable claim was accepted")
			}
			if got := reasonOf(t, err); got != ReasonInvocationInvalid {
				t.Fatalf("reason = %q, want %q", got, ReasonInvocationInvalid)
			}
			if reader.calls != 0 {
				t.Fatalf("the registry was read for a claim that never parsed (%d reads)", reader.calls)
			}
		})
	}
}

// TestClaimAtADeploymentThatVerifiesNothingIsRefused: no registry read mounted is
// not a licence to trust the claim.
func TestClaimAtADeploymentThatVerifiesNothingIsRefused(t *testing.T) {
	_, err := serviceWith(nil).verifyInvocation(context.Background(),
		contracts.Scope{Tenant: "t1"}, false, invocationClaim("inst-1", "v1", invDigestA))
	if err == nil {
		t.Fatal("an unverifiable claim was trusted")
	}
	if got := reasonOf(t, err); got != ReasonInvocationUnmounted {
		t.Fatalf("reason = %q, want %q", got, ReasonInvocationUnmounted)
	}
}

// TestUnknownInstallationAnswersAsAbsent: the answer for another tenant's
// installation must be the answer for a non-existent one.
func TestUnknownInstallationAnswersAsAbsent(t *testing.T) {
	reader := &fakeInstallations{err: contracts.NewError(contracts.ErrNotFound, "no such installation")}
	_, err := serviceWith(reader).verifyInvocation(context.Background(),
		contracts.Scope{Tenant: "t1"}, false, invocationClaim("inst-other-tenant", "v1", invDigestA))
	if err == nil {
		t.Fatal("an installation this tenant does not have was accepted")
	}
	if got := reasonOf(t, err); got != ReasonInvocationUnknown {
		t.Fatalf("reason = %q, want %q", got, ReasonInvocationUnknown)
	}
	if strings.Contains(err.Error(), "no such installation") {
		t.Fatalf("the refusal repeated the registry's wording: %v", err)
	}
}

// TestFollowUpDoesNotRebind: the artifact of a task is fixed when the task is
// created; a later message carrying a different claim must not move it.
func TestFollowUpDoesNotRebind(t *testing.T) {
	reader := &fakeInstallations{version: "v2", digest: invDigestB}
	pinned, err := serviceWith(reader).verifyInvocation(context.Background(),
		contracts.Scope{Tenant: "t1"}, true, invocationClaim("inst-1", "v2", invDigestB))
	if err != nil {
		t.Fatalf("a follow-up was refused instead of leaving the existing pin alone: %v", err)
	}
	if pinned != nil {
		t.Fatalf("a follow-up produced a new pin: %+v", pinned)
	}
	if reader.calls != 0 {
		t.Fatal("a follow-up re-read the installation")
	}
}

// TestCallerCannotAuthorThePinMarkers: the markers are this plane's. A caller that
// states its own pin would replace verification with an assertion.
func TestCallerCannotAuthorThePinMarkers(t *testing.T) {
	for _, key := range []string{MetaPinnedVersion, MetaPinnedDigest} {
		if !isInvocationMarker(key) {
			t.Fatalf("%s must be gateway-authored only", key)
		}
	}
	for _, key := range []string{MetaInvocation, "ahp.root", "ahp.depth", "somebody-elses-field"} {
		if isInvocationMarker(key) {
			t.Fatalf("%q is not a pin marker and must not be stripped as one", key)
		}
	}
}

// --- the canonical carrier shape the registry's own Invoke sends today ---

func ahpCarrier(installation, version, digest string) map[string]any {
	return map[string]any{
		MetaAhpCarrier: map[string]any{
			"task_id": "t-1", "tenant": "t1", "agent_id": "agent-1",
			"installation_id": installation, "version_id": version, "version_digest": digest,
		},
	}
}

// TestCanonicalCarrierIsVerified: the registry's Invoke dispatcher sends the
// artifact triple inside "ahp". Verification wired to a key nothing sends would be
// decoration, so this shape is the live one.
func TestCanonicalCarrierIsVerified(t *testing.T) {
	reader := &fakeInstallations{version: "v1", digest: invDigestA}
	pinned, err := serviceWith(reader).verifyInvocation(context.Background(),
		contracts.Scope{Tenant: "t1"}, false, ahpCarrier("inst-1", "v1", invDigestA))
	if err != nil {
		t.Fatalf("the carrier shape the product actually sends was refused: %v", err)
	}
	md := pinned.metadata()
	if md[MetaPinnedVersion] != "v1" || md[MetaPinnedDigest] != invDigestA {
		t.Fatalf("journal markers = %v, want the verified version and digest", md)
	}
}

// TestSubstitutionOverTheCanonicalCarrierIsRefused: same refusal, real wire shape.
func TestSubstitutionOverTheCanonicalCarrierIsRefused(t *testing.T) {
	reader := &fakeInstallations{version: "v1", digest: invDigestA}
	_, err := serviceWith(reader).verifyInvocation(context.Background(),
		contracts.Scope{Tenant: "t1"}, false, ahpCarrier("inst-1", "v1", invDigestB))
	if err == nil {
		t.Fatal("a substituted artifact over the canonical carrier was accepted")
	}
	if got := reasonOf(t, err); got != ReasonInvocationMismatch {
		t.Fatalf("reason = %q, want %q", got, ReasonInvocationMismatch)
	}
}

// TestIncompleteCarrierIsLeftAlone: the evals runner sends agent_id and
// version_digest without installation_id or version_id, and every older path sends
// nothing. Neither becomes a refusal -- that is the accepted-behaviour boundary --
// and the limit it leaves is documented on claimFromCarrier.
func TestIncompleteCarrierIsLeftAlone(t *testing.T) {
	reader := &fakeInstallations{version: "v1", digest: invDigestA}
	evalsShape := map[string]any{MetaAhpCarrier: map[string]any{
		"agent_id": "agent-1", "version_digest": invDigestA, "eval_case": "case-7", "task_id": "t-9",
	}}
	pinned, err := serviceWith(reader).verifyInvocation(context.Background(), contracts.Scope{Tenant: "t1"}, false, evalsShape)
	if err != nil {
		t.Fatalf("the evals carrier was refused: %v", err)
	}
	if pinned != nil {
		t.Fatalf("an incomplete carrier produced a pin: %+v", pinned)
	}
	if reader.calls != 0 {
		t.Fatalf("an incomplete carrier read the installation %d times", reader.calls)
	}
}

// TestCarrierWithAnUnusableDigestIsRefused: once all three identities are stated,
// a malformed digest is a broken claim, not an absent one.
func TestCarrierWithAnUnusableDigestIsRefused(t *testing.T) {
	reader := &fakeInstallations{version: "v1", digest: invDigestA}
	_, err := serviceWith(reader).verifyInvocation(context.Background(),
		contracts.Scope{Tenant: "t1"}, false, ahpCarrier("inst-1", "v1", "not-a-digest"))
	if err == nil {
		t.Fatal("a carrier naming a non-digest was accepted")
	}
	if got := reasonOf(t, err); got != ReasonInvocationInvalid {
		t.Fatalf("reason = %q, want %q", got, ReasonInvocationInvalid)
	}
}
