package artifactsapi

// C1 (reviewer condition, reports/mac-director/artifacts-read-parity-27b16124-review-20261110.md):
// the FACE must keep the 403-vs-404 discriminator that the read-parity mask
// depends on. Mutation M3 in that review masked authorization refusals too: the
// store-level legs went red, but internal/artifactsapi stayed green, i.e. nothing
// on the wire would have noticed a revoked policy turning into a 404.
//
// These legs run through the real routes and the real transport.WriteError, on the
// SAME artifact id, and pin:
//   - a policy that revoked the read answers 403 with its machine reason;
//   - a same-tenant subject excluded by the artifact's labels answers 404, byte for
//     byte like a missing id;
//   - the two answers are not equal, so a mask that swallows authorization cannot
//     pass unnoticed.

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// wirePolicyReason is the machine-readable cause the test policy attaches to a
// revoked read; it must survive to the response body.
const wirePolicyReason = "policy_revoked_artifact_read"

// rawOp posts one operation and returns the status and the RAW body, because the
// discriminator is about bytes on the wire, not about a decoded map.
func rawOp(t *testing.T, e *env, op, token, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, e.srv.URL+"/v1/artifacts/"+op, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := e.srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var sb strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		sb.Write(buf[:n])
		if err != nil {
			break
		}
	}
	return resp.StatusCode, sb.String()
}

func TestLifecycleReadWireKeepsRevokedPolicyApartFromLabelExclusion(t *testing.T) {
	var (
		victim atomic.Value // string: the artifact id whose read the policy revokes
		hits   atomic.Int32
	)
	victim.Store("")
	// The revocation is per subject, as a real policy is: it is the OWNER's read
	// that got revoked. A caller the labels already exclude must still get the
	// masked 404 below, otherwise the two refusals would not be separable here.
	policy := func(_ context.Context, scope contracts.Scope, action, resource string) error {
		id, _ := victim.Load().(string)
		if action == "read" && id != "" && scope.Subject == alice && resource == "artifact:"+id {
			hits.Add(1)
			return contracts.NewReasoned(contracts.ErrForbidden, wirePolicyReason,
				"artifacts: read revoked by policy for this artifact")
		}
		return nil
	}
	e := newEnvWithAuthorize(t, policy)
	tok := e.signTask(t, alice)
	id := e.uploadSeal(t, tok, "k/wire-parity", "wire-parity-body", labels.Set{})
	victim.Store(id)
	otherTok := e.signTask(t, mallory)
	absent := mustJSON(t, map[string]any{"id": "art-wire-parity-absent"})
	here := mustJSON(t, map[string]any{"id": id})

	for _, op := range []string{"GetLifecycle", "Readiness"} {
		revokedStatus, revokedBody := rawOp(t, e, op, tok, here)
		if revokedStatus != http.StatusForbidden {
			t.Fatalf("%s: a revoked read must answer 403, got %d body=%s", op, revokedStatus, revokedBody)
		}
		if !strings.Contains(revokedBody, wirePolicyReason) {
			t.Fatalf("%s: the 403 must carry the policy reason %q, body=%s", op, wirePolicyReason, revokedBody)
		}
		if !strings.Contains(revokedBody, `"code":"FORBIDDEN"`) {
			t.Fatalf("%s: the 403 must carry the contract code, body=%s", op, revokedBody)
		}

		foreignStatus, foreignBody := rawOp(t, e, op, otherTok, here)
		missingStatus, missingBody := rawOp(t, e, op, otherTok, absent)
		if foreignStatus != http.StatusNotFound || missingStatus != http.StatusNotFound {
			t.Fatalf("%s: label exclusion and missing id must both answer 404, got %d and %d",
				op, foreignStatus, missingStatus)
		}
		if foreignBody != missingBody {
			t.Fatalf("%s: label exclusion must render byte for byte like a missing id: %q vs %q",
				op, foreignBody, missingBody)
		}
		if revokedBody == foreignBody || revokedStatus == foreignStatus {
			t.Fatalf("%s: a revoked policy must not be indistinguishable from a label exclusion (status %d, body %q)",
				op, revokedStatus, revokedBody)
		}
	}
	if hits.Load() == 0 {
		t.Fatal("the revoked policy never fired - these legs would pass vacuously")
	}
}
