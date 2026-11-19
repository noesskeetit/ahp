package tools

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/sdk/go/internal/clienthttp"
)

// unknown reports a disagreement between what the owner asserted about a result
// and what the SDK can verify. It is not a caller error and not a retry hint.
func unknown(message string) error {
	return &clienthttp.Error{Code: "UNKNOWN", Message: "tools sdk: " + message}
}

func invalid(message string) error {
	return contracts.NewError(contracts.ErrInvalid, "tools sdk: "+message)
}

// ResultStorage tells a caller where the result bytes actually live.
const (
	StorageInline   = "inline"
	StorageArtifact = "artifact"
)

// ResultProvenance is the immutable provenance the owner recorded with the
// dispatch grant. It is echoed, never negotiated: the owner derived it from the
// verified credential that dispatched the effect, so a reader cannot choose the
// binding its read is checked against.
type ResultProvenance struct {
	Schema       string `json:"schema"`
	GrantTask    string `json:"grant_task,omitempty"`
	GrantContext string `json:"grant_context,omitempty"`
	Source       string `json:"source,omitempty"`
}

// ArtifactRef points at the immutable artifact holding an oversized result.
type ArtifactRef struct {
	ArtifactID string `json:"artifact_id"`
	Digest     string `json:"digest"`
	Size       int64  `json:"size"`
}

// ToolResult is the owner's stored result for one operation key.
//
// It exists only for a SUCCEEDED, provenance-bearing row. The owner answers
// every other state with an error rather than with an empty or partial result,
// and the SDK passes that classification through unchanged:
//
//   - NOT_FOUND  -- the key is absent OR belongs to another tenant, deployment
//     or subject; existence is deliberately not disclosed.
//   - FORBIDDEN  -- no verified context, missing or foreign provenance, another
//     context of the same subject, a revoked personal grant, or a non-task
//     bearer (including a reconciled row, whose labels nobody may invent).
//   - CONFLICT   -- pending, uncertain, failed, or stale tool/connection. This
//     is a decision: poll Get, never re-send the effect.
//
// Payload carries the raw MCP CallToolResult unchanged and only when Storage is
// StorageInline. Oversized results carry ArtifactRef instead; fetch the bytes
// with FetchResultBytes, which verifies the digest.
type ToolResult struct {
	Key        string           `json:"key"`
	Target     string           `json:"target"`
	Status     string           `json:"status"`
	Digest     string           `json:"digest"`
	Storage    string           `json:"result_storage"`
	Provenance ResultProvenance `json:"provenance"`
	Payload    json.RawMessage  `json:"payload,omitempty"`
	Ref        *ArtifactRef     `json:"artifact_ref,omitempty"`
}

// ToolResult reads the stored result of one key. It is a pure read: it never
// dispatches, never fences, and never retries, so an uncertain or pending row
// cannot be nudged into a second effect by a curious reader.
func (c *Client) ToolResult(ctx context.Context, key string) (ToolResult, error) {
	var out ToolResult
	if e := clienthttp.Required(key); e != nil {
		return out, e
	}
	e := c.wire.Call(ctx, "/v1/tools/GetResult", map[string]string{"key": key}, &out, false)
	if e != nil {
		return out, e
	}
	// A 200 that contradicts its own shape is a contract violation, not a
	// result: an inline body that is absent or an artifact row that carries
	// bytes in both places would let a caller act on the wrong payload.
	if err := out.validate(); err != nil {
		return ToolResult{}, err
	}
	return out, nil
}

func (r ToolResult) validate() error {
	switch r.Storage {
	case StorageInline:
		if len(r.Payload) == 0 {
			return unknown("owner reported an inline result with no payload")
		}
		if r.Ref != nil {
			return unknown("inline result also carries an artifact reference")
		}
	case StorageArtifact:
		if r.Ref == nil || r.Ref.ArtifactID == "" || r.Ref.Digest == "" {
			return unknown("artifact result carries no verifiable reference")
		}
		if len(r.Payload) != 0 {
			return unknown("artifact result also carries an inline payload")
		}
	default:
		return unknown(fmt.Sprintf("unknown result_storage %q", r.Storage))
	}
	return nil
}

// ArtifactReader is the slice of the Artifacts owner API this package needs;
// *artifacts.Client satisfies it. Bytes are fetched under the CALLER's current
// authority -- the result read grants no extra storage capability, so a revoke
// between the two calls fails closed at the Artifacts owner.
type ArtifactReader interface {
	Download(ctx context.Context, id string) ([]byte, error)
}

// FetchResultBytes retrieves an artifact-stored result and verifies its digest
// before returning anything. A mismatch is reported instead of handed back: a
// caller cannot tell a wrong byte stream from a wrong tool effect.
func FetchResultBytes(ctx context.Context, reader ArtifactReader, res ToolResult) ([]byte, error) {
	if reader == nil {
		return nil, invalid("no artifact reader supplied")
	}
	if res.Storage != StorageArtifact || res.Ref == nil {
		return nil, invalid("result is not artifact-stored; use Payload")
	}
	body, err := reader.Download(ctx, res.Ref.ArtifactID)
	if err != nil {
		return nil, err
	}
	want := res.Ref.Digest
	if res.Digest != "" && res.Digest != want {
		return nil, unknown("result digest and artifact digest disagree")
	}
	if !digestMatches(want, body) {
		return nil, unknown("stored bytes do not match the declared digest")
	}
	if res.Ref.Size > 0 && int64(len(body)) != res.Ref.Size {
		return nil, unknown("stored bytes do not match the declared size")
	}
	return body, nil
}

func digestMatches(digest string, body []byte) bool {
	sum := sha256.Sum256(body)
	want, err := hex.DecodeString(strings.TrimSpace(strings.TrimPrefix(digest, "sha256:")))
	if err != nil {
		return false
	}
	return subtle.ConstantTimeCompare(sum[:], want) == 1
}
