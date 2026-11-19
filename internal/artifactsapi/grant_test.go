package artifactsapi

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// openGrantRaw fetches bytes through OpenDownloadGrant with an optional Range.
func (e *env) openGrantRaw(t *testing.T, token, grant, rangeHdr string) (int, string, http.Header) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, e.srv.URL+"/v1/artifacts/OpenDownloadGrant",
		bytes.NewReader([]byte(`{"grant":"`+grant+`"}`)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	if rangeHdr != "" {
		req.Header.Set("Range", rangeHdr)
	}
	resp, err := e.srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), resp.Header
}

func reasonOf(body map[string]any) string {
	r, _ := body["reason"].(string)
	return r
}

// mintGrant is one DownloadGrant call returning the issued grant token.
func mintGrant(t *testing.T, e *env, token string, body map[string]any) (int, map[string]any) {
	t.Helper()
	return e.call(t, "DownloadGrant", token, mustJSON(t, body))
}

// TestHTTPDownloadGrantServesTheExactVersionOnce is the positive control of the
// contract (leg e): the proxy path returns byte-for-byte the sealed version,
// honours Range, and a one-time grant answers its second use with a
// machine-readable refusal.
func TestHTTPDownloadGrantServesTheExactVersionOnce(t *testing.T) {
	e := newEnv(t)
	tok := e.signTask(t, alice)
	body := "точная версия для прокси\n"
	id := e.uploadSeal(t, tok, "k-"+t.Name(), body, labels.Set{})

	_, meta := e.call(t, "Get", tok, mustJSON(t, map[string]any{"id": id}))
	version, _ := meta["version"].(string)
	if version == "" {
		t.Fatalf("Get returned no sealed version: %v", meta)
	}

	code, grant := mintGrant(t, e, tok, map[string]any{"id": id, "version": version})
	mustStatus(t, code, http.StatusOK, grant)
	token, _ := grant["grant"].(string)
	if token == "" {
		t.Fatalf("no grant token in %v", grant)
	}
	if grant["artifact_id"] != id || grant["version"] != version {
		t.Fatalf("grant does not name the requested artifact/version: %v", grant)
	}
	if grant["one_time"] != true {
		t.Fatalf("a grant without an explicit use count must be one-time: %v", grant)
	}
	if sha, _ := grant["sha256"].(string); sha == "" || sha != meta["sha256"] {
		t.Fatalf("grant sha256 = %v, metadata sha256 = %v", grant["sha256"], meta["sha256"])
	}
	if exp, _ := grant["expires_at"].(string); exp == "" {
		t.Fatalf("grant carries no expiry: %v", grant)
	}

	status, served, hdr := e.openGrantRaw(t, tok, token, "")
	if status != http.StatusOK {
		t.Fatalf("OpenDownloadGrant status = %d", status)
	}
	if served != body {
		t.Fatalf("granted bytes differ from the sealed version")
	}
	sum := sha256.Sum256([]byte(served))
	if got := hex.EncodeToString(sum[:]); got != meta["sha256"] {
		t.Fatalf("granted sha256 = %s, metadata = %v", got, meta["sha256"])
	}
	if hdr.Get("ETag") != `"`+meta["sha256"].(string)+`"` {
		t.Fatalf("ETag = %q, want the version digest", hdr.Get("ETag"))
	}

	status, served, hdr = e.openGrantRaw(t, tok, token, "")
	if status != http.StatusForbidden || reasonOf(mustMap(t, served)) != "grant_used" {
		t.Fatalf("second use: status = %d body = %s", status, served)
	}
	if hdr.Get("Location") != "" || hdr.Get("X-Amz-Security-Token") != "" {
		t.Fatalf("a granted read must be proxied, not redirected or presigned: %v", hdr)
	}
}

// TestHTTPDownloadGrantRefusesLatestAndForeignRecipient is legs (a) and the
// "точная версия" half: "latest" is not a version, and a grant may not name
// another recipient. Both answers are machine-readable, none is a 200.
func TestHTTPDownloadGrantRefusesLatestAndForeignRecipient(t *testing.T) {
	e := newEnv(t)
	tok := e.signTask(t, alice)
	id := e.uploadSeal(t, tok, "k-"+t.Name(), "recipient and version\n", labels.Set{})
	_, meta := e.call(t, "Get", tok, mustJSON(t, map[string]any{"id": id}))
	version, _ := meta["version"].(string)

	cases := []struct {
		name       string
		body       map[string]any
		wantStatus int
		wantReason string
	}{
		{"no version", map[string]any{"id": id}, http.StatusBadRequest, "grant_version_required"},
		{"latest", map[string]any{"id": id, "version": "latest"}, http.StatusBadRequest, "grant_version_required"},
		{"another recipient", map[string]any{"id": id, "version": version, "recipient": mallory},
			http.StatusForbidden, "grant_recipient_mismatch"},
		{"a version that was never sealed", map[string]any{"id": id, "version": "no-such-version"},
			http.StatusForbidden, "grant_version_mismatch"},
		{"a lifetime under the floor", map[string]any{"id": id, "version": version, "ttl_seconds": 1},
			http.StatusBadRequest, ""},
	}
	for _, tc := range cases {
		code, out := mintGrant(t, e, tok, tc.body)
		if code != tc.wantStatus {
			t.Errorf("%s: status = %d, want %d (body %v)", tc.name, code, tc.wantStatus, out)
			continue
		}
		if tc.wantReason != "" && reasonOf(out) != tc.wantReason {
			t.Errorf("%s: reason = %q, want %q (body %v)", tc.name, reasonOf(out), tc.wantReason, out)
		}
	}
}

// TestHTTPDownloadGrantLifecycleRefuses is leg (d) over HTTP: a legal hold
// refuses issuance AND consumption, a tombstone answers 404 with its reason.
func TestHTTPDownloadGrantLifecycleRefuses(t *testing.T) {
	e := newEnv(t)
	tok := e.signTask(t, alice)
	id := e.uploadSeal(t, tok, "k-"+t.Name(), "lifecycle over http\n", labels.Set{})
	_, meta := e.call(t, "Get", tok, mustJSON(t, map[string]any{"id": id}))
	version, _ := meta["version"].(string)

	code, out := mintGrant(t, e, tok, map[string]any{"id": id, "version": version})
	mustStatus(t, code, http.StatusOK, out)
	token, _ := out["grant"].(string)

	if code = statusOf(t, e, tok, "SetLegalHold", map[string]any{"id": id, "on": true}); code != http.StatusOK {
		t.Fatalf("SetLegalHold on: status = %d", code)
	}
	code, out = mintGrant(t, e, tok, map[string]any{"id": id, "version": version})
	if code != http.StatusForbidden || reasonOf(out) != "artifact_legal_hold" {
		t.Fatalf("mint under legal hold: status = %d body = %v", code, out)
	}
	if status, served, _ := e.openGrantRaw(t, tok, token, ""); status != http.StatusForbidden ||
		reasonOf(mustMap(t, served)) != "artifact_legal_hold" {
		t.Fatalf("consume under legal hold: status = %d body = %s", status, served)
	}
	if code = statusOf(t, e, tok, "SetLegalHold", map[string]any{"id": id, "on": false}); code != http.StatusOK {
		t.Fatalf("SetLegalHold off: status = %d", code)
	}

	if code = statusOf(t, e, tok, "Delete", map[string]any{"id": id, "key": "k-delete-" + t.Name()}); code != http.StatusOK {
		t.Fatalf("Delete: status = %d", code)
	}
	if status, served, _ := e.openGrantRaw(t, tok, token, ""); status != http.StatusNotFound ||
		reasonOf(mustMap(t, served)) != "artifact_deleted" {
		t.Fatalf("consume after delete: status = %d body = %s", status, served)
	}
	code, out = mintGrant(t, e, tok, map[string]any{"id": id, "version": version})
	if code != http.StatusNotFound {
		t.Fatalf("mint on a tombstone: status = %d body = %v", code, out)
	}
}

// TestHTTPDownloadGrantCannotCrossTheConversation: the grant is issued under the
// caller's own conversation gate, so a token of another chat of the same subject
// cannot manufacture a download of this chat's artifact.
func TestHTTPDownloadGrantCannotCrossTheConversation(t *testing.T) {
	e := newEnv(t)
	owner := e.signBound(t, alice, "ctx-owner", "task-owner")
	other := e.signBound(t, alice, "ctx-other", "task-other")
	id := e.uploadSeal(t, owner, "k-"+t.Name(), "файл одного чата\n", labels.Set{})
	_, meta := e.call(t, "Get", owner, mustJSON(t, map[string]any{"id": id}))
	version, _ := meta["version"].(string)

	code, out := mintGrant(t, e, other, map[string]any{"id": id, "version": version})
	if code != http.StatusForbidden {
		t.Fatalf("another conversation minted a grant: status = %d body = %v", code, out)
	}
	// The owner still can, and the bytes come back exactly.
	code, out = mintGrant(t, e, owner, map[string]any{"id": id, "version": version})
	mustStatus(t, code, http.StatusOK, out)
	token, _ := out["grant"].(string)
	if status, served, _ := e.openGrantRaw(t, owner, token, ""); status != http.StatusOK {
		t.Fatalf("owner consumption: status = %d body = %s", status, served)
	}
}

// TestHTTPGrantRoutesRejectHumanTokens is leg (c): the grant surface is the
// machine surface. A human JWT reaching Artifacts directly - with no workload
// certificate behind it - is refused in a way a client can tell apart from a
// successful download and from a server fault.
func TestHTTPGrantRoutesRejectHumanTokens(t *testing.T) {
	e := newEnv(t)
	// A real human token, as the human edge issues one: no deployment, no task,
	// no grant claim - which is precisely why it cannot be a workload credential.
	human, err := e.signer.Sign(auth.Claims{
		Audience: audience, Kind: auth.KindHuman,
		Tenant: e.tenant, Subject: alice,
	}, 5*time.Minute)
	if err != nil {
		t.Fatalf("sign human token: %v", err)
	}
	for _, op := range []string{"DownloadGrant", "OpenDownloadGrant"} {
		code, out := e.call(t, op, human, `{"id":"a-1","version":"v-1","grant":"x"}`)
		if code == http.StatusOK || code >= http.StatusInternalServerError {
			t.Fatalf("%s with a human token: status = %d body = %v", op, code, out)
		}
		if code != http.StatusForbidden {
			t.Fatalf("%s with a human token: status = %d, want 403 (body %v)", op, code, out)
		}
	}
}

func jsonDecode(raw []byte, v any) error { return json.Unmarshal(raw, v) }

func mustMap(t *testing.T, raw string) map[string]any {
	t.Helper()
	out := map[string]any{}
	if err := jsonDecode([]byte(raw), &out); err != nil {
		t.Fatalf("body is not a JSON object: %q", raw)
	}
	return out
}
