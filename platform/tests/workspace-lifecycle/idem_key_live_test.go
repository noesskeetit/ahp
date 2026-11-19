//go:build ahp_exec && ahp_data

package lifecyclelive

// DEF-H24-8 on the deployed front, where it actually mattered. The runner derives
// its upload key from (name, digest), so as long as a tombstone reserved the key,
// saving the same file again after its conversation was deleted failed forever --
// and it failed with a message that pointed at nothing the user could act on.
//
// This leg goes through the mounted front over mTLS with a scoped task token and
// asserts the released-key contract end to end: delete then begin the same key
// again is a NEW generation, while the key of an artifact that is still here stays
// taken. Every identifier carries a per-run nonce, so it re-runs on a stand that
// already holds earlier generations.

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
)

func TestUploadKeyReleasedByDeletionLive(t *testing.T) {
	if strings.TrimSpace(os.Getenv("AHP_LIFECYCLE_LIVE")) != "1" {
		t.Skip("AHP_LIFECYCLE_LIVE != 1: the leg needs the running front (bring the stand up, then set it; not a silent pass)")
	}
	f := newFront(t)
	run := nonce(t)
	contextID := "h23idem" + run
	owner, ownerTask := tokenTask(t, 0, "h23idem-task-"+run, contextID, "artifacts")
	if ownerTask == "" {
		t.Fatal("the exchange returned no granted task id")
	}
	key := "h23idem-" + run

	seal := func(body []byte) string {
		t.Helper()
		status, code, payload := f.call(t, "BeginUpload", owner, map[string]any{
			"key": key, "name": "release-" + run + ".bin", "mime": "application/octet-stream",
			"sha256": shaHex(body), "size": len(body),
		})
		mustOK(t, "BeginUpload", status, code, payload)
		var up struct {
			ID           string `json:"id"`
			PresignedURL string `json:"presigned_url"`
		}
		if err := json.Unmarshal(payload, &up); err != nil || up.ID == "" || up.PresignedURL == "" {
			t.Fatalf("BeginUpload shape unexpected: %v", err)
		}
		req, _ := http.NewRequest(http.MethodPut, up.PresignedURL, bytes.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Skipf("presigned PUT unreachable from this host (%T): the object store address the front presigns with is not routable here; not a silent pass", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			t.Fatalf("presigned PUT: status %d", resp.StatusCode)
		}
		status, code, payload = f.call(t, "Seal", owner, map[string]any{"id": up.ID})
		mustOK(t, "Seal", status, code, payload)
		return up.ID
	}

	first := seal([]byte("first generation " + run))
	t.Logf("first generation sealed: id=%s key=%s", first, key)

	// A key whose artifact is still here is taken: a different request under it is
	// refused, which is the half of the contract that must NOT loosen.
	status, _, payload := f.call(t, "BeginUpload", owner, map[string]any{
		"key": key, "name": "release-" + run + ".bin", "mime": "application/octet-stream",
		"sha256": shaHex([]byte("competing request " + run)), "size": 20,
	})
	if status == http.StatusOK {
		t.Fatalf("a live artifact's key must not accept another request: got 200 %s", redact(string(payload)))
	}
	// The platform's A2A convention keeps the status at 400 and carries the exact
	// condition in "reason" (stateapi/consumer asserts this shape), so the reason
	// is what says whether the refusal is the right one.
	var refusal struct {
		Code   string `json:"code"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(payload, &refusal); err != nil || refusal.Reason != "KEY_REUSE_MISMATCH" {
		t.Fatalf("a live key must be refused with reason=KEY_REUSE_MISMATCH: %d body=%s", status, redact(string(payload)))
	}
	t.Logf("live key refused as required: code=%s reason=%s", refusal.Code, refusal.Reason)

	if status, code, payload := f.call(t, "Delete", owner, map[string]any{"id": first, "key": "del-" + run}); status != http.StatusOK {
		t.Fatalf("Delete: want 200, got %d code=%s body=%s", status, code, redact(string(payload)))
	}

	// The defect lived here: this call answered CONFLICT "upload key was deleted".
	second := seal([]byte("second generation " + run))
	if second == first {
		t.Fatalf("a released key must start a new generation, not echo the tombstone")
	}
	t.Logf("key released by deletion: same key %s produced a new generation id=%s", key, second)

	// The new generation is a real artifact: its bytes come back under the digest
	// the front recorded, read out of band through the presigned-free Open face.
	_, _, got := f.call(t, "Get", owner, map[string]any{"id": second})
	var ref struct {
		SHA256 string `json:"sha256"`
		Size   int    `json:"size"`
	}
	if err := json.Unmarshal(got, &ref); err != nil || ref.SHA256 != shaHex([]byte("second generation "+run)) {
		t.Fatalf("the new generation must answer with its own digest: %s", redact(string(got)))
	}
	if _, code, payload := f.call(t, "Get", owner, map[string]any{"id": first}); code != "NOT_FOUND" {
		t.Fatalf("the tombstoned generation stays absent, got code=%s body=%s", code, redact(string(payload)))
	}
}
