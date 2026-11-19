package tools

import (
	"context"
	"encoding/json"

	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// ProvenanceSchema labels the label object this package writes. A reader can
// tell an unknown future shape from a known one instead of guessing.
const ProvenanceSchema = "tools-result-v1"

// Provenance label keys. They mirror the gateway's grant-binding convention
// (internal/gateway/grant_binding.go): the labels are authored by the gateway
// from a VERIFIED credential, never asserted by the caller.
const (
	ProvKeySchema     = "schema"
	ProvKeyTask       = "grant_task"
	ProvKeyContext    = "grant_context"
	ProvKeyDeployment = "grant_deployment"
	ProvKeySource     = "source"
	// ArtifactRefKind marks an out-of-line result. The payload then carries
	// immutable references only; the bytes come from the Artifacts owner API
	// under the reader's CURRENT authority.
	ArtifactRefKind = "artifact_ref"
)

// defaultArtifactThreshold is the result size above which an injected
// ArtifactSink is consulted. It stays under the ledger's 1 MiB result bound so
// the inline envelope never overflows.
const defaultArtifactThreshold = 256 << 10

// CallProvenance is the caller-independent description of the authority behind
// one effect. Only a verified credential may populate it: the HTTP face builds
// it from verified claims, so a task cannot choose the labels it will be
// checked against later.
type CallProvenance struct {
	TaskID    string
	ContextID string
}

// provenanceLabels is the stored shape. Source is written server-side because
// the connection that produced the result is a fact about this service's
// configuration, not about the caller.
type provenanceLabels struct {
	Schema     string `json:"schema"`
	Task       string `json:"grant_task,omitempty"`
	Context    string `json:"grant_context,omitempty"`
	Deployment string `json:"grant_deployment,omitempty"`
	Source     string `json:"source,omitempty"`
}

// marshal renders the labels for the ledger. An empty provenance (a Go caller
// that carried no verified binding, e.g. the legacy Call) yields nil so the
// column stays NULL and GetResult can tell "no provenance" apart from
// "provenance of an unknown context".
func (p CallProvenance) marshal(connection string) json.RawMessage {
	if p.TaskID == "" && p.ContextID == "" {
		return nil
	}
	raw, err := json.Marshal(provenanceLabels{
		Schema: ProvenanceSchema, Task: p.TaskID, Context: p.ContextID, Source: connection,
	})
	if err != nil {
		// Unreachable for this struct; provenance must never break the effect.
		return nil
	}
	return raw
}

// GrantChecker revalidates a personal connection grant at READ time without
// contacting the external provider: reading a durable result must not refresh
// or re-mint an OAuth credential. *OAuthManager is the production
// implementation; tests may inject a fixture, which is then explicitly a
// fixture and not evidence about a real external provider.
type GrantChecker interface {
	ConsentActive(ctx context.Context, scope contracts.Scope, connection string) (bool, error)
}

// ArtifactSink stores an oversized result out of line and returns its
// immutable reference. Implementations must be owner-scoped and must not
// mutate the ledger: the ledger keeps the reference, the bytes live in
// Artifacts, and there is no second ledger.
type ArtifactSink interface {
	PutImmutable(ctx context.Context, scope contracts.Scope, payload []byte) (artifactID, digest string, err error)
}

// ResultReader is the verified reading authority. ContextID is the security
// boundary of a durable result: the labels recorded at dispatch bind the
// result to the context that paid for the effect, and a same-subject credential
// from another context does not inherit them.
type ResultReader struct {
	TaskID    string
	ContextID string
}

// ResultRef is the immutable description of an out-of-line result.
type ResultRef struct {
	ArtifactID string `json:"artifact_id"`
	Version    string `json:"version,omitempty"`
	Digest     string `json:"digest"`
	Size       int64  `json:"size"`
}

// ResultView is the owner's result. Storage "inline" carries the raw stored
// MCP CallToolResult verbatim in Payload; storage "artifact" carries only Ref
// and the caller fetches the bytes through the Artifacts owner API with its
// current authority. Request arguments and credentials are never part of it.
type ResultView struct {
	Key        string          `json:"key"`
	Target     string          `json:"target"`
	Status     string          `json:"status"`
	Digest     string          `json:"digest"`
	Storage    string          `json:"result_storage"`
	Provenance json.RawMessage `json:"provenance"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	Ref        *ResultRef      `json:"artifact_ref,omitempty"`
}

// GetResult returns the stored result of ONE successful operation to the owner
// authority that performed it. Every gate fails closed and none of them
// re-dispatches the effect:
//
//   - unknown key, or a key owned by another tenant/deployment/subject: NOT_FOUND
//     (the ledger's own scope check; nothing is leaked about existence);
//   - ACCEPTED/DISPATCHED: CONFLICT "pending"; SIDE_EFFECT_UNKNOWN: CONFLICT
//     "uncertain"; FAILED/FENCED: CONFLICT "no result" -- all explicit, and the
//     caller must not resend, so no second external effect is possible here;
//   - no provenance, or provenance without a context label: FORBIDDEN. Missing
//     legacy labels are never invented from the reading credential;
//   - a different context under the SAME subject: FORBIDDEN. A personal
//     credential taints only the context that consented, so replaying a read
//     into a sibling context of the same human must not transfer it;
//   - a tool/connection that is no longer configured: CONFLICT (stale). The
//     result was produced by a configuration the reader can no longer be
//     granted against;
//   - a personal connection whose consent was revoked since the effect:
//     FORBIDDEN, checked locally without refreshing anything.
func (s *Service) GetResult(ctx context.Context, scope contracts.Scope, reader ResultReader, key string) (ResultView, error) {
	if err := scope.Validate(); err != nil {
		return ResultView{}, err
	}
	if key == "" || len(key) > 128 {
		return ResultView{}, contracts.NewError(contracts.ErrInvalid, "tools: key must be 1..128 bytes")
	}
	if reader.ContextID == "" {
		return ResultView{}, contracts.NewError(contracts.ErrForbidden,
			"tools: result requires a reading credential with a verified context")
	}
	op, err := s.ledger.Get(ctx, scope, key)
	if err != nil {
		return ResultView{}, err
	}
	switch op.Status {
	case operations.StatusSucceeded:
		// only a successful stored result is ever released
	case operations.StatusAccepted, operations.StatusDispatched:
		return ResultView{}, contracts.NewError(contracts.ErrConflict,
			"tools: operation has no result yet (status "+op.Status+"); poll Get, never resend")
	case operations.StatusSideEffectUnknown:
		return ResultView{}, contracts.NewError(contracts.ErrConflict,
			"tools: outcome uncertain; reconcile before any result exists")
	default:
		return ResultView{}, contracts.NewError(contracts.ErrConflict,
			"tools: operation finished without a result (status "+op.Status+")")
	}

	if len(op.Provenance) == 0 {
		return ResultView{}, contracts.NewError(contracts.ErrForbidden,
			"tools: no provenance recorded for this result; refusing to invent one")
	}
	var labels provenanceLabels
	if err := json.Unmarshal(op.Provenance, &labels); err != nil {
		return ResultView{}, contracts.NewError(contracts.ErrForbidden,
			"tools: unrecognised provenance; refusing to interpret it")
	}
	if labels.Context == "" {
		return ResultView{}, contracts.NewError(contracts.ErrForbidden,
			"tools: provenance carries no context label; refusing to invent one")
	}
	if labels.Context != reader.ContextID {
		return ResultView{}, contracts.NewError(contracts.ErrForbidden,
			"tools: result was produced in a different context")
	}

	// Current-configuration revalidation: a result whose producing
	// tool/connection no longer exists cannot be re-granted against anything.
	spec, ok := s.cfg.Tools[op.Target]
	if !ok {
		return ResultView{}, contracts.NewError(contracts.ErrConflict,
			"tools: the tool that produced this result is no longer configured")
	}
	if _, ok := s.cfg.Connections[spec.Connection]; !ok {
		return ResultView{}, contracts.NewError(contracts.ErrConflict,
			"tools: the connection that produced this result is no longer configured")
	}
	if s.cfg.Grants != nil {
		active, gerr := s.cfg.Grants.ConsentActive(ctx, scope, spec.Connection)
		if gerr != nil {
			return ResultView{}, gerr
		}
		if !active {
			return ResultView{}, contracts.NewError(contracts.ErrForbidden,
				"tools: the personal connection grant behind this result is no longer active")
		}
	}

	view := ResultView{
		Key: op.Key, Target: op.Target, Status: op.Status, Digest: op.Digest,
		Storage: "inline", Provenance: op.Provenance,
	}
	if ref, ok := parseArtifactRef(op.Result); ok {
		view.Storage = "artifact"
		view.Ref = ref
		return view, nil
	}
	view.Payload = op.Result
	return view, nil
}

// spill decides whether a completion goes out of line. With no sink wired it
// returns handled=false and the caller keeps the accepted behaviour (the
// ledger's own size check fails the completion explicitly).
func (s *Service) spill(ctx context.Context, scope contracts.Scope, payload []byte) (json.RawMessage, bool, error) {
	if s.cfg.Artifacts == nil {
		return nil, false, nil
	}
	if len(payload) <= s.artifactThreshold() {
		return nil, false, nil
	}
	id, digest, err := s.cfg.Artifacts.PutImmutable(ctx, scope, payload)
	if err != nil {
		return nil, false, err
	}
	raw, merr := json.Marshal(map[string]any{
		"kind":        ArtifactRefKind,
		"artifact_id": id,
		"digest":      digest,
		"size":        len(payload),
	})
	if merr != nil {
		return nil, false, merr
	}
	return raw, true, nil
}

func (s *Service) artifactThreshold() int {
	if threshold := s.cfg.ArtifactThreshold; threshold > 0 {
		return threshold
	}
	return defaultArtifactThreshold
}

// parseArtifactRef recognises the out-of-line result envelope.
func parseArtifactRef(raw json.RawMessage) (*ResultRef, bool) {
	if len(raw) == 0 {
		return nil, false
	}
	var probe struct {
		Kind string `json:"kind"`
		ResultRef
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, false
	}
	if probe.Kind != ArtifactRefKind || probe.ArtifactID == "" {
		return nil, false
	}
	return &probe.ResultRef, true
}
