package gateway

// An Invoke says which artifact it selected; this plane checks it before running.
//
// The audit fact (root audit, live canon 6fd2d0b614de, binary sha 9b352b4c...):
// the A2A entry of this product admitted tasks that named no artifact. The
// registry resolved the selected version and dropped it on the way to the wire,
// ahp_registry.task_pins held 0 rows against 5 installations and 124 accepted
// tasks, and no task metadata mentioned a version, a digest or an installation.
// So "the invoke ran the runtime that was selected" could not be true or false --
// nothing recorded either way, and a different artifact answering the call was
// undetectable. Root refuses exactly that: PASS means the selected entity
// demonstrably served the request.
//
// The receiving side is the only place that can make that true, because it is the
// only place that can refuse BEFORE anything runs. So:
//
//   - a claim that cannot be decoded is refused (not ignored);
//   - a deployment that cannot verify it refuses too -- trusting an unverifiable
//     artifact claim is the failure mode, not the fallback;
//   - a claim that does not match what this tenant's installation actually serves
//     is refused with a reason naming both identities;
//   - a verified claim is journalled on the task by THIS plane
//     (ahp.pinned_version / ahp.pinned_digest). A caller cannot author those
//     markers: like the tree and attestation markers, caller-supplied copies are
//     deleted before the row is written.
//
// No frame without a claim behaves differently: existing acceptance paths send
// none, and they run exactly as they do today.

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// artifactDigest is the canonical digest shape an artifact claim may name. The
// registry publishes digests in exactly this form (ahp_registry.versions has a
// CHECK for it), so a claim in any other spelling cannot match anything and is
// refused as unusable rather than as a mismatch.
var artifactDigest = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

const (
	// MetaInvocation is the SendMessage metadata key under which an Invoke states
	// the artifact it selected. The sending half is internal/registry (its
	// InvocationMetadataKey names the same literal).
	MetaInvocation = "ahp.invocation"
	// MetaPinnedVersion and MetaPinnedDigest are this plane's record of the
	// artifact a task was verified against. Gateway-authored only.
	MetaPinnedVersion = "ahp.pinned_version"
	MetaPinnedDigest  = "ahp.pinned_digest"
)

// Refusal reasons of the artifact check.
const (
	// ReasonInvocationInvalid: the claim is present but unusable -- a field
	// missing, an id too long, a digest that is not a digest.
	ReasonInvocationInvalid = "invocation_claim_unusable"
	// ReasonInvocationUnmounted: a claim arrived at a deployment with no registry
	// read to check it against.
	ReasonInvocationUnmounted = "invocation_verifier_unmounted"
	// ReasonInvocationUnknown: no installation of THIS tenant answers to that id.
	// Same answer as "not installed", so a probe cannot learn that an id exists.
	ReasonInvocationUnknown = "invoked_artifact_not_installed"
	// ReasonInvocationMismatch: the installation serves a different artifact than
	// the one named -- the substitution this check exists to refuse.
	ReasonInvocationMismatch = "invoked_artifact_mismatch"
)

// InstalledArtifactReader is the whole Registry surface this plane needs: what a
// tenant's installation serves right now, version id and digest together. The
// composition root adapts *registry.Store to it; the execution plane does not
// import the registry.
type InstalledArtifactReader interface {
	InstallationArtifact(ctx context.Context, tenant, installationID string) (versionID, digest string, err error)
}

// pinnedArtifact is a verified claim.
type pinnedArtifact struct {
	installationID string
	versionID      string
	digest         string
}

// metadata renders what this plane journals on the accepted task. Nil for an
// unverified (absent) claim, so a task that never made one gains no field.
func (p *pinnedArtifact) metadata() map[string]any {
	if p == nil {
		return nil
	}
	return map[string]any{MetaPinnedVersion: p.versionID, MetaPinnedDigest: p.digest}
}

// isInvocationMarker reports whether a metadata key may only be written by this
// plane. A caller that states its own pin would replace verification with
// assertion, which is the hole the tree and attestation markers already close.
func isInvocationMarker(key string) bool {
	return key == MetaPinnedVersion || key == MetaPinnedDigest
}

// decodeInvocation reads the claim. Only the exact JSON object shape is accepted:
// a stringified blob or a partially filled object is a claim that cannot be
// checked, and "could not read it" must never become "no claim was made".
func decodeInvocation(raw any) (installation, version, digest string, err error) {
	obj, ok := raw.(map[string]any)
	if !ok {
		return "", "", "", fmt.Errorf("%s must be an object naming installation_id, version_id and version_digest", MetaInvocation)
	}
	installation, ok = obj["installation_id"].(string)
	if !ok || installation == "" {
		return "", "", "", fmt.Errorf("%s.installation_id is required", MetaInvocation)
	}
	version, ok = obj["version_id"].(string)
	if !ok || version == "" {
		return "", "", "", fmt.Errorf("%s.version_id is required", MetaInvocation)
	}
	digest, ok = obj["version_digest"].(string)
	if !ok || !artifactDigest.MatchString(digest) {
		return "", "", "", fmt.Errorf("%s.version_digest must be sha256:<64 lowercase hex>", MetaInvocation)
	}
	if len(installation) > maxArtifactID || len(version) > maxArtifactID {
		return "", "", "", fmt.Errorf("%s identifiers are bounded to %d bytes", MetaInvocation, maxArtifactID)
	}
	return installation, version, digest, nil
}

// MetaAhpCarrier is the metadata object the registry's own Invoke dispatcher uses
// today (internal/registry/invokeapi.go) to send the selected artifact alongside
// the message: {"ahp": {"installation_id", "version_id", "version_digest", ...}}.
// Reading only this older explicit key would have left the verification dead --
// nothing on the canonical wire sends it -- so the receiving side accepts both
// spellings.
const MetaAhpCarrier = "ahp"

// claimFromCarrier reads the artifact identity out of the canonical carrier
// object. It reports "no claim" -- not a refusal -- unless the object states all
// three identities, which is what the registry's Invoke sends.
//
// This is the honest limit of the design, stated where the code is: verification
// can only check a claim that was made. A frame that omits the triple (the evals
// runner sends agent_id and version_digest only, and every pre-existing
// acceptance path sends nothing) is admitted exactly as it is today, and remains
// unverifiable. Closing that gap needs either a deployment that requires a claim
// on the invoke path, or a receiving side that derives the artifact from its own
// identity instead of the frame; both change accepted behaviour and belong to a
// decision by the architect, not to this check.
func claimFromCarrier(raw any) (installation, version, digest string, claimed bool) {
	obj, ok := raw.(map[string]any)
	if !ok {
		return "", "", "", false
	}
	pick := func(key string) string { v, _ := obj[key].(string); return strings.TrimSpace(v) }
	installation, version, digest = pick("installation_id"), pick("version_id"), pick("version_digest")
	if installation == "" || version == "" || digest == "" {
		return "", "", "", false
	}
	return installation, version, digest, true
}

// maxArtifactID bounds the ids of a claim to something a registry row can hold.
const maxArtifactID = 128

// verifyInvocation checks the claim an Invoke made against this tenant's
// installation, before a task exists and before anything is spent.
//
// A follow-up never re-binds: the artifact of a task is decided when it is
// created, and a later message cannot move it (the same rule the registry's
// first-pin-wins enforces on its side).
func (s *Service) verifyInvocation(ctx context.Context, scope contracts.Scope, followUp bool, md map[string]any) (*pinnedArtifact, error) {
	if followUp {
		return nil, nil
	}
	var installation, version, digest string
	if raw, claimed := md[MetaInvocation]; claimed {
		var err error
		installation, version, digest, err = decodeInvocation(raw)
		if err != nil {
			return nil, contracts.NewReasoned(contracts.ErrInvalid, ReasonInvocationInvalid, "gateway: "+err.Error())
		}
	} else {
		carrierInstallation, carrierVersion, carrierDigest, claimed := claimFromCarrier(md[MetaAhpCarrier])
		if !claimed {
			return nil, nil
		}
		installation, version, digest = carrierInstallation, carrierVersion, carrierDigest
		if !artifactDigest.MatchString(digest) {
			return nil, contracts.NewReasoned(contracts.ErrInvalid, ReasonInvocationInvalid,
				"gateway: "+MetaAhpCarrier+".version_digest must be sha256:<64 lowercase hex")
		}
		if len(installation) > maxArtifactID || len(version) > maxArtifactID {
			return nil, contracts.NewReasoned(contracts.ErrInvalid, ReasonInvocationInvalid,
				fmt.Sprintf("gateway: %s identifiers are bounded to %d bytes", MetaAhpCarrier, maxArtifactID))
		}
	}
	if s.cfg.Invocations == nil {
		return nil, contracts.NewReasoned(contracts.ErrConflict, ReasonInvocationUnmounted,
			"gateway: this deployment verifies no registry artifact, so an invocation claim is refused instead of trusted")
	}
	haveVersion, haveDigest, err := s.cfg.Invocations.InstallationArtifact(ctx, scope.Tenant, installation)
	if err != nil {
		if contracts.Code(err) == contracts.ErrNotFound {
			return nil, contracts.NewReasoned(contracts.ErrForbidden, ReasonInvocationUnknown,
				"gateway: the invoked artifact is not installed for this tenant")
		}
		return nil, fmt.Errorf("gateway: read invoked artifact: %w", err)
	}
	if haveVersion != version || haveDigest != digest {
		return nil, contracts.NewReasoned(contracts.ErrConflict, ReasonInvocationMismatch,
			fmt.Sprintf("gateway: installation %s serves version %s digest %s, the invocation named version %s digest %s",
				installation, haveVersion, haveDigest, version, digest))
	}
	return &pinnedArtifact{installationID: installation, versionID: version, digest: digest}, nil
}
