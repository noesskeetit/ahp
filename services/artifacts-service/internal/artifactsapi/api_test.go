package artifactsapi

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"strconv"
	"testing"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

func TestNewRejectsNilDependencies(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	verifier := auth.NewVerifier(testIssuer, map[string]ed25519.PublicKey{testKID: pub})
	authorize := func(context.Context, auth.Claims, string) error { return nil }
	var store *artifacts.Store
	if _, err := New(store, verifier, authorize); err == nil {
		t.Error("nil store accepted")
	}
	if _, err := New(&artifacts.Store{}, nil, authorize); err == nil {
		t.Error("nil verifier accepted")
	}
	if _, err := New(&artifacts.Store{}, verifier, nil); err == nil {
		t.Error("nil authorize accepted")
	}
}

func TestUnknownRouteAndMethod(t *testing.T) {
	e := newEnv(t)
	tok := e.signTask(t, alice)
	// Unknown op.
	if code, _ := e.call(t, "NoSuchOp", tok, `{}`); code != http.StatusNotFound {
		t.Errorf("unknown op status %d, want 404", code)
	}
	// Wrong method.
	req, _ := http.NewRequest(http.MethodGet, e.srv.URL+"/v1/artifacts/Get", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := e.srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("GET status %d, want 404", resp.StatusCode)
	}
	// Missing token: task middleware rejects (401/403), never reaches store.
	if code, _ := e.call(t, "Get", "", `{"id":"x"}`); code == http.StatusOK {
		t.Errorf("unauthenticated Get succeeded")
	}
}

// TestUploadSealDownloadRange covers acceptance #1 and #2 over HTTP: a real
// upload seals to an immutable version, downloads match, and Range yields 206.
func TestUploadSealDownloadRange(t *testing.T) {
	e := newEnv(t)
	tok := e.signTask(t, alice)
	body := "hello-artifact-world"
	id := e.uploadSeal(t, tok, "k/1", body, labels.Set{})

	code, ref := e.call(t, "Get", tok, mustJSON(t, map[string]any{"id": id}))
	mustStatus(t, code, http.StatusOK, ref)
	if ref["sha256"] != shaHex(body) {
		t.Fatalf("Get sha mismatch: %v", ref["sha256"])
	}

	status, got, hdr := e.downloadRaw(t, tok, mustJSON(t, map[string]any{"id": id}), "")
	if status != http.StatusOK || got != body {
		t.Fatalf("full download: status=%d body=%q", status, got)
	}
	if hdr.Get("ETag") == "" {
		t.Error("missing ETag")
	}

	status, got, hdr = e.downloadRaw(t, tok, mustJSON(t, map[string]any{"id": id}), "bytes=6-10")
	if status != http.StatusPartialContent || got != body[6:11] {
		t.Fatalf("range download: status=%d body=%q", status, got)
	}
	if hdr.Get("Content-Range") == "" {
		t.Error("missing Content-Range on 206")
	}

	// Past-end range -> 416, and the 416 must name the complete length. The
	// regression this pins: Content-Range was "bytes */?", which is not a
	// length, so a client that overshot could not learn the object's size and
	// could not recompute a range (measured: 29 B object, bytes=100000-100001).
	status, _, hdr = e.downloadRaw(t, tok, mustJSON(t, map[string]any{"id": id}), "bytes=100000-100001")
	if status != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("past-end range status %d, want 416", status)
	}
	if want := "bytes */" + strconv.Itoa(len(body)); hdr.Get("Content-Range") != want {
		t.Errorf("416 Content-Range = %q, want %q", hdr.Get("Content-Range"), want)
	}

	// Open-ended range: the answer must promise exactly size-offset bytes.
	// The regression this pins: end was computed as offset+size-1, so
	// "bytes=6-" on an 18-byte body answered "bytes 6-23/18" with CL=18 and
	// streamed 12 - clients hung waiting for 6 bytes that do not exist.
	status, got, hdr = e.downloadRaw(t, tok, mustJSON(t, map[string]any{"id": id}), "bytes=6-")
	if status != http.StatusPartialContent || got != body[6:] {
		t.Fatalf("open-ended range: status=%d body=%q", status, got)
	}
	wantCR := "bytes 6-" + strconv.Itoa(len(body)-1) + "/" + strconv.Itoa(len(body))
	if cr := hdr.Get("Content-Range"); cr != wantCR {
		t.Errorf("open-ended Content-Range = %q, want %q", cr, wantCR)
	}
	if cl, want := hdr.Get("Content-Length"), strconv.Itoa(len(body)-6); cl != want {
		t.Errorf("open-ended Content-Length = %q, want %q", cl, want)
	}
}

// TestPrivateFactNoEscape covers acceptance #5: a private artifact cannot be
// read or published by another subject, nor published to a workspace sink.
func TestPrivateFactNoEscape(t *testing.T) {
	e := newEnv(t)
	aliceTok := e.signTask(t, alice)
	malloryTok := e.signTask(t, mallory)
	id := e.uploadSeal(t, aliceTok, "k/priv", "secret-note", labels.Set{})

	// Foreign subject cannot read the private artifact.
	if code, _ := e.call(t, "Get", malloryTok, mustJSON(t, map[string]any{"id": id})); code == http.StatusOK {
		t.Fatal("mallory read alice's private artifact")
	}
	if status, _, _ := e.downloadRaw(t, malloryTok, mustJSON(t, map[string]any{"id": id}), ""); status == http.StatusOK {
		t.Fatal("mallory downloaded alice's private artifact")
	}
	// Private fact cannot be published to a workspace sink directly.
	code, _ := e.call(t, "CheckPublication", aliceTok, mustJSON(t, map[string]any{
		"ids":  []string{id},
		"sink": labels.Audience{Kind: labels.KindWorkspace, Workspace: "w1"},
	}))
	if code == http.StatusOK {
		t.Fatal("private artifact published to workspace sink")
	}
	// Owner may publish to a workspace they are a member of.
	code, _ = e.call(t, "Publish", aliceTok, mustJSON(t, map[string]any{"id": id, "workspace": "w1"}))
	mustStatus(t, code, http.StatusOK, nil)
	// With no membership hook configured, even the owner's read of the now
	// workspace-restricted artifact fails closed: knowing the id is not
	// membership, so the private fact still cannot escape by publication.
	if code, _ := e.call(t, "Get", aliceTok, mustJSON(t, map[string]any{"id": id})); code == http.StatusOK {
		t.Fatal("workspace artifact readable without a membership check")
	}
}

func TestPublishOnlyOwnerAndMembership(t *testing.T) {
	e := newEnv(t, artifacts.WithWorkspaceAccess(
		func(_ context.Context, scope contracts.Scope, ws string) error {
			if ws == "w1" && scope.Subject == alice {
				return nil
			}
			return contracts.NewError(contracts.ErrForbidden, "not a member")
		}))
	aliceTok := e.signTask(t, alice)
	malloryTok := e.signTask(t, mallory)
	id := e.uploadSeal(t, aliceTok, "k/p", "shared-body", labels.Set{})

	// Non-owner cannot publish.
	if code, _ := e.call(t, "Publish", malloryTok, mustJSON(t, map[string]any{"id": id, "workspace": "w1"})); code == http.StatusOK {
		t.Fatal("non-owner published")
	}
	// Owner publishes.
	if code, _ := e.call(t, "Publish", aliceTok, mustJSON(t, map[string]any{"id": id, "workspace": "w1"})); code != http.StatusOK {
		t.Fatalf("owner publish failed: %d", code)
	}
	// Re-publish to a different workspace -> conflict.
	code, _ := e.call(t, "Publish", aliceTok, mustJSON(t, map[string]any{"id": id, "workspace": "w2"}))
	if code != http.StatusConflict {
		t.Fatalf("republish status %d, want 409", code)
	}
}

func TestLifecycleGateOverHTTP(t *testing.T) {
	e := newEnv(t)
	tok := e.signTask(t, alice)
	id := e.uploadSeal(t, tok, "k/lc", "lifecycle-body", labels.Set{})

	// legal hold blocks delete.
	mustStatus(t, statusOf(t, e, tok, "SetLegalHold", map[string]any{"id": id, "on": true}), http.StatusOK, nil)
	if code, _ := e.call(t, "Delete", tok, mustJSON(t, map[string]any{"id": id, "key": "k/lc"})); code == http.StatusOK {
		t.Fatal("delete succeeded under legal hold")
	}
	mustStatus(t, statusOf(t, e, tok, "SetLegalHold", map[string]any{"id": id, "on": false}), http.StatusOK, nil)
	mustStatus(t, statusOf(t, e, tok, "Delete", map[string]any{"id": id, "key": "k/lc"}), http.StatusOK, nil)

	// derivation readiness over HTTP.
	d := "k/r"
	rid := e.uploadSeal(t, tok, d, "derive-body", labels.Set{})
	rd := statusOf(t, e, tok, "Readiness", map[string]any{"id": rid})
	mustStatus(t, rd, http.StatusOK, nil)
}

func statusOf(t *testing.T, e *env, tok, op string, body map[string]any) int {
	t.Helper()
	code, _ := e.call(t, op, tok, mustJSON(t, body))
	return code
}
