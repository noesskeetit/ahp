//go:build ahp_exec && ahp_data

// Package lifecyclelive exercises the artifact/workspace data lifecycle (PDF section 8)
// THROUGH THE MOUNTED FRONT: mTLS plus a scoped Identity task token, the same path a
// real run takes. It is deliberately not an in-process test - the point is what the
// deployed plane answers.
package lifecyclelive

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// ---------- stand plumbing ----------

func needEnv(t *testing.T, key, fallback string) string {
	t.Helper()
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	if fallback != "" {
		return fallback
	}
	t.Skipf("%s is unset and there is no default: the stand must be running (not a silent pass)", key)
	return ""
}

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("wd: %v", err)
	}
	return filepath.Dir(filepath.Dir(wd))
}

func caFile(t *testing.T) string {
	t.Helper()
	// AHP_DEPLOYED_CA takes the certificate or the directory holding it, matching
	// tests/toolgw-live/scoped-token-provider.mjs: one value has to work for every
	// harness on the stand, otherwise people trip on a path that looks right.
	if v := strings.TrimSpace(os.Getenv("AHP_DEPLOYED_CA")); v != "" {
		if st, err := os.Stat(v); err == nil && st.IsDir() {
			return filepath.Join(v, "ca.crt")
		}
		return v
	}
	return filepath.Join(repoRoot(t), "infra", "certs", "ca", "ca.crt")
}

type front struct {
	base   string
	client *http.Client
}

func newFront(t *testing.T) *front {
	t.Helper()
	return newFrontFrom(t, clientDir())
}

// newFrontFrom is newFront for a workload whose client certificate lives in
// another directory. The tenant a request is scoped to comes from the
// certificate, so a second-tenant leg needs the second tenant's certificate --
// infra/gen-client.sh issues one from the same CA as an ordinary fixture.
func newFrontFrom(t *testing.T, dir string) *front {
	t.Helper()
	base := needEnv(t, "AHP_DEPLOYED_URL", "https://127.0.0.1:29612")
	cert, err := tls.LoadX509KeyPair(filepath.Join(dir, "client.crt"), filepath.Join(dir, "client.key"))
	if err != nil {
		t.Skipf("client certificate pair unavailable in %s (infra/gen-client.sh): %v (not a silent pass)", dir, err)
	}
	caPEM, err := os.ReadFile(caFile(t))
	if err != nil {
		t.Skipf("stand CA unreadable at %s: %v (set AHP_DEPLOYED_CA; not a silent pass)", caFile(t), err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatalf("CA file %s parsed no certificate", caFile(t))
	}
	return &front{base: strings.TrimRight(base, "/"), client: &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			Certificates: []tls.Certificate{cert}, RootCAs: pool, MinVersion: tls.VersionTLS12,
		}},
	}}
}

// token mints a scoped task token through the canonical Identity chain
// (login -> /auth/token -> /v1/identity/control/exchange), asking for specific
// capabilities. userIndex selects the acceptance human (0 = owner tenant).
func token(t *testing.T, userIndex int, task, contextID string, caps ...string) string {
	t.Helper()
	tok, _ := tokenTask(t, userIndex, task, contextID, caps...)
	return tok
}

// tokenTask is token plus the task identifier the exchange actually granted: the
// grant id and the requested name are not the same string, and delegation is
// only legal under the granted one.
func tokenTask(t *testing.T, userIndex int, task, contextID string, caps ...string) (string, string) {
	t.Helper()
	certDir := clientDir()
	root := repoRoot(t)
	args := []string{
		filepath.Join(root, "scripts", "identity-task-token.mjs"),
		"--origin", needEnv(t, "AHP_DEPLOYED_URL", "https://127.0.0.1:29612"),
		"--user-index", strconv.Itoa(userIndex),
		"--task", task,
	}
	if contextID != "" {
		args = append(args, "--context", contextID)
	}
	if len(caps) > 0 {
		args = append(args, "--capabilities", strings.Join(caps, ","))
	}
	cmd := exec.Command("node", args...)
	cmd.Dir = root
	cmd.Env = append(os.Environ(),
		"AHP_ACCEPTANCE_USERS_FILE="+needEnv(t, "AHP_ACCEPTANCE_USERS_FILE", ""),
		"AHP_TLS_CLIENT_CERT="+filepath.Join(certDir, "client.crt"),
		"AHP_TLS_CLIENT_KEY="+filepath.Join(certDir, "client.key"),
		"AHP_TLS_CA="+caFile(t),
	)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		t.Fatalf("token provider (user %d, task %s) failed: %v: %s",
			userIndex, task, err, redact(errb.String()+out.String()))
	}
	var got struct {
		OK     bool   `json:"ok"`
		Token  string `json:"token"`
		Tenant string `json:"tenant"`
		Task   string `json:"task"`
	}
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("token provider output unparsable: %v", err)
	}
	if !got.OK || got.Token == "" {
		t.Fatalf("token provider returned no token (user %d)", userIndex)
	}
	t.Logf("token: tenant=%s granted_task=%s task_len=%d", got.Tenant, got.Task, len(got.Token))
	return got.Token, got.Task
}

// delegateToken mints a CHILD task token under parentTask for another
// conversation. The child is minted by Store.MintTask, so unlike the
// browser-exchange credential it carries no ctx_scope=authority claim: it is
// bound to exactly one conversation, which is what a real run's credential is.
func (f *front) delegateToken(t *testing.T, parent, parentTask, childTask, contextID string, caps ...string) (string, bool) {
	t.Helper()
	body := map[string]any{"task": childTask, "context": contextID, "ttl": "5m",
		"capabilities": caps}
	status, code, payload := f.callPath(t, "/v1/identity/roots/"+parentTask+"/delegate", parent, body)
	if status != http.StatusCreated {
		t.Logf("delegate %s under %s: %d %s %s", childTask, parentTask, status, code, redact(string(payload)))
		return "", false
	}
	var got struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(payload, &got); err != nil || got.Token == "" {
		t.Fatalf("delegate answered 201 without a child token: %v", err)
	}
	return got.Token, true
}

func clientDir() string {
	if v := strings.TrimSpace(os.Getenv("AHP_CLIENT_CERT_DIR")); v != "" {
		return v
	}
	return "/tmp/h21/certs"
}

func redact(s string) string {
	if len(s) > 300 {
		return s[:300] + "..."
	}
	return s
}

// call posts one artifact-plane operation and returns status, code and body.
func (f *front) call(t *testing.T, op, bearer string, body any) (int, string, []byte) {
	return f.callPath(t, "/v1/artifacts/"+op, bearer, body)
}

// callPath is call for the operations that live outside the artifact prefix.
func (f *front) callPath(t *testing.T, path, bearer string, body any) (int, string, []byte) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal %s: %v", path, err)
	}
	req, err := http.NewRequest(http.MethodPost, f.base+path, bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("request %s: %v", path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := f.client.Do(req)
	if err != nil {
		t.Fatalf("%s transport: %T", path, err)
	}
	defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()
	payload, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, codeOf(payload), payload
}

// getBytes is Download, which answers with bytes rather than JSON.
func (f *front) getBytes(t *testing.T, op, bearer string, body any, extraHeader [2]string) (*http.Response, []byte) {
	t.Helper()
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, f.base+"/v1/artifacts/"+op, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+bearer)
	if extraHeader[0] != "" {
		req.Header.Set(extraHeader[0], extraHeader[1])
	}
	resp, err := f.client.Do(req)
	if err != nil {
		t.Fatalf("%s transport: %T", op, err)
	}
	defer func() { _ = resp.Body.Close() }()
	payload, _ := io.ReadAll(resp.Body)
	return resp, payload
}

func codeOf(payload []byte) string {
	var e struct {
		Code string `json:"code"`
	}
	if json.Unmarshal(payload, &e) != nil {
		return ""
	}
	return e.Code
}

func mustOK(t *testing.T, op string, status int, code string, payload []byte) {
	t.Helper()
	if status != http.StatusOK {
		t.Fatalf("%s: want 200, got %d code=%s body=%s", op, status, code, redact(string(payload)))
	}
}

func nonce(t *testing.T) string {
	t.Helper()
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("nonce: %v", err)
	}
	return hex.EncodeToString(b[:])
}

func shaHex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// ---------- the journey ----------

// TestArtifactLifecycleLive walks one artifact through the mounted front:
// upload -> seal (version + digest pinned) -> download verified out of band ->
// retention/legal hold visible -> a second credential in another context and in
// another tenant can neither read nor delete what it does not own -> the
// durable-retention plane (GC, pins, Restore) is refused to a workload token at
// the authorize seam -> delete (tombstone) makes both faces uniformly absent and
// a repeat delete cannot resurrect or damage anything.
//
// Every identifier carries a per-run nonce, so the journey re-runs on a stand
// that already holds previous artifacts, and the GC is never invoked for real
// against a shared tenant.
func TestArtifactLifecycleLive(t *testing.T) {
	if strings.TrimSpace(os.Getenv("AHP_LIFECYCLE_LIVE")) != "1" {
		t.Skip("AHP_LIFECYCLE_LIVE != 1: the lifecycle legs need the running front (bring the stand up, then set it; not a silent pass)")
	}
	f := newFront(t)
	run := nonce(t)
	ctxA := "h23lca" + run
	ctxB := "h23lcb" + run
	owner, ownerTask := tokenTask(t, 0, "h23lc-task-"+run, ctxA, "artifacts", "workspace", "identity:delegate")
	if ownerTask == "" {
		t.Fatal("the exchange returned no granted task id")
	}

	body := []byte("lifecycle journey payload " + run)
	digest := shaHex(body)
	key := "k-" + run

	// --- upload -> seal ---
	status, code, payload := f.call(t, "BeginUpload", owner, map[string]any{
		"key": key, "name": "journey.bin", "mime": "application/octet-stream",
		"sha256": digest, "size": len(body),
	})
	mustOK(t, "BeginUpload", status, code, payload)
	var up struct {
		ID           string `json:"id"`
		PresignedURL string `json:"presigned_url"`
		SHA256       string `json:"sha256"`
	}
	if err := json.Unmarshal(payload, &up); err != nil || up.ID == "" || up.PresignedURL == "" {
		t.Fatalf("BeginUpload shape unexpected: %v id=%q url_set=%v", err, up.ID, up.PresignedURL != "")
	}
	putReq, _ := http.NewRequest(http.MethodPut, up.PresignedURL, bytes.NewReader(body))
	putResp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		t.Skipf("presigned PUT unreachable from this host (%T): the object store address the front presigns with is not routable here; not a silent pass", err)
	}
	_, _ = io.Copy(io.Discard, putResp.Body)
	_ = putResp.Body.Close()
	if putResp.StatusCode/100 != 2 {
		t.Fatalf("presigned PUT: status %d", putResp.StatusCode)
	}

	status, code, payload = f.call(t, "Seal", owner, map[string]any{"id": up.ID})
	mustOK(t, "Seal", status, code, payload)
	var ref struct {
		ID, Version, SHA256, Tenant, Subject, ContextID, TaskID string
		Size                                                    int64
		Deployment                                              string
	}
	// ArtifactRef is flat JSON; decode generically to avoid depending on field order.
	var rawRef map[string]any
	if err := json.Unmarshal(payload, &rawRef); err != nil {
		t.Fatalf("Seal payload unparsable: %v", err)
	}
	str := func(k string) string { s, _ := rawRef[k].(string); return s }
	ref.ID, ref.Version, ref.SHA256 = str("id"), str("version"), str("sha256")
	ref.Tenant, ref.Subject, ref.ContextID, ref.TaskID = str("tenant"), str("subject"), str("context_id"), str("task_id")
	ref.Deployment = str("deployment")
	if n, ok := rawRef["size"].(float64); ok {
		ref.Size = int64(n)
	}
	if ref.Version == "" {
		t.Fatal("seal must pin an exact object version (immutable artifact): version empty")
	}
	if ref.SHA256 != digest {
		t.Fatalf("sealed digest must equal the digest of the bytes put: sealed=%s local=%s", ref.SHA256, digest)
	}
	if ref.Size != int64(len(body)) {
		t.Fatalf("sealed size %d != put size %d", ref.Size, len(body))
	}
	if ref.ContextID != ctxA {
		t.Fatalf("binding must come from the verified token: row context=%q token context=%q", ref.ContextID, ctxA)
	}
	if ref.Subject == "" || ref.Deployment == "" || ref.Tenant == "" {
		t.Fatalf("provenance columns must be frozen from the credential: %+v", rawRef)
	}
	t.Logf("sealed: version=%s sha=%s.. ctx=%s subj=%s deploy=%s size=%d",
		ref.Version, ref.SHA256[:12], ref.ContextID, ref.Subject, ref.Deployment, ref.Size)

	// --- download, verified out of band ---
	resp, got := f.getBytes(t, "Download", owner, map[string]any{"id": up.ID}, [2]string{})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Download: want 200, got %d body=%s", resp.StatusCode, redact(string(got)))
	}
	if got2 := shaHex(got); got2 != digest {
		t.Fatalf("downloaded bytes must hash to the sealed digest: got %s want %s", got2, digest)
	}
	if etag := resp.Header.Get("ETag"); etag != `"`+digest+`"` {
		t.Fatalf("ETag must be the sealed digest, got %q", etag)
	}
	// One byte range, answered as a range.
	resp, part := f.getBytes(t, "Download", owner, map[string]any{"id": up.ID}, [2]string{"Range", "bytes=0-3"})
	if resp.StatusCode != http.StatusPartialContent || len(part) != 4 {
		t.Fatalf("range download: want 206/4 bytes, got %d/%d", resp.StatusCode, len(part))
	}
	if cr := resp.Header.Get("Content-Range"); !strings.HasSuffix(cr, "/"+strconv.FormatInt(ref.Size, 10)) {
		t.Fatalf("Content-Range must carry the sealed total size, got %q", cr)
	}

	// --- lifecycle: legal hold is visible and truthful ---
	status, code, payload = f.call(t, "SetLegalHold", owner, map[string]any{"id": up.ID, "on": true})
	mustOK(t, "SetLegalHold", status, code, payload)
	status, code, payload = f.call(t, "GetLifecycle", owner, map[string]any{"id": up.ID})
	mustOK(t, "GetLifecycle", status, code, payload)
	var lc struct {
		Deleted   *bool `json:"deleted"`
		LegalHold *bool `json:"legal_hold"`
	}
	if err := json.Unmarshal(payload, &lc); err != nil {
		t.Fatalf("GetLifecycle shape: %v (%s)", err, redact(string(payload)))
	}
	if lc.LegalHold == nil || !*lc.LegalHold {
		t.Fatalf("legal hold must be reported after being set: %s", redact(string(payload)))
	}
	if lc.Deleted == nil || *lc.Deleted {
		t.Fatal("a live artifact must report deleted=false")
	}

	// --- retention: a floor set by the caller is honoured and reported back ---
	floor := time.Now().UTC().Add(30 * 24 * time.Hour).Truncate(time.Second)
	status, code, payload = f.call(t, "SetRetention", owner, map[string]any{"id": up.ID, "retain_until": floor})
	mustOK(t, "SetRetention", status, code, payload)
	status, code, payload = f.call(t, "GetLifecycle", owner, map[string]any{"id": up.ID})
	mustOK(t, "GetLifecycle(after SetRetention)", status, code, payload)
	var withFloor struct {
		RetainUntil *time.Time `json:"retain_until"`
	}
	if err := json.Unmarshal(payload, &withFloor); err != nil {
		t.Fatalf("GetLifecycle shape: %v (%s)", err, redact(string(payload)))
	}
	if withFloor.RetainUntil == nil || withFloor.RetainUntil.Before(floor.Add(-time.Minute)) {
		t.Fatalf("the requested retention floor must be reported, want >=%s got %v", floor.Format(time.RFC3339), withFloor.RetainUntil)
	}
	t.Logf("retention floor honoured: retain_until=%s", withFloor.RetainUntil.Format(time.RFC3339))
	// --- a run bound to another conversation cannot read it ---
	// The child credential is minted by Store.Delegate + MintTask and carries no
	// ctx_scope=authority claim, so it is bound to exactly one conversation: this
	// is the credential a real run holds. The owner's browser-exchange credential
	// is authority-scoped by design (a human reads their own artifacts across
	// conversations, internal/artifacts/read_scope.go), so it proves nothing about
	// this boundary. Asserted while the artifact is live: a refusal here can only
	// come from the binding, not from a tombstone.
	child, delegated := f.delegateToken(t, owner, ownerTask, "h23lc-task-b-"+run, ctxB, "artifacts")
	if !delegated {
		// Not a pass and not somebody else's bug: the only credential a harness
		// can obtain from outside is the browser-exchange authority token, and the
		// delegation authority refuses a same-deployment child under it ("would
		// repeat an actor in the chain"; allow_self is an inherited ceiling a
		// client cannot grant). A conversation-bound credential therefore has to
		// come from a real Space run, which is recorded as an unverified leg in
		// the report rather than faked here.
		t.Log("cross-conversation read gate NOT exercised: no non-authority credential obtainable from outside (see report U-1)")
	}
	if delegated {
		if st, c, g := f.call(t, "Get", child, map[string]any{"id": up.ID}); st == http.StatusOK {
			t.Fatal("a run bound to another conversation must not read this artifact")
		} else {
			if st != http.StatusForbidden && st != http.StatusNotFound {
				t.Fatalf("cross-conversation Get: want 403/404, got %d %s %s", st, c, redact(string(g)))
			}
			t.Logf("cross-conversation Get refused: %d %s", st, c)
		}
		if resp, got := f.getBytes(t, "Download", child, map[string]any{"id": up.ID}, [2]string{}); resp.StatusCode == http.StatusOK || shaHex(got) == digest {
			t.Fatalf("a run bound to another conversation must not download the bytes: status=%d", resp.StatusCode)
		}
		if st, c, _ := f.call(t, "Delete", child, map[string]any{"id": up.ID, "key": key + "-child"}); st == http.StatusOK {
			t.Fatal("a run bound to another conversation must not tombstone this artifact")
		} else {
			t.Logf("cross-conversation Delete refused: %d %s", st, c)
		}
		if st, c, _ := f.call(t, "Get", owner, map[string]any{"id": up.ID}); st != http.StatusOK {
			t.Fatalf("the owner's artifact must be intact after a foreign attempt: got %d %s", st, c)
		}
	}
	// --- another tenant ---
	// The foreign caller speaks with its own attested workload when the fixture
	// provides one (AHP_LIFECYCLE_FOREIGN_CLIENT_DIR, issued by
	// infra/gen-client.sh <tenant> <deployment>): a foreign Bearer pushed through
	// the owner's certificate dies at the certificate/token binding, which is a
	// different boundary than the one this leg is about.
	front2 := f
	foreignDir := strings.TrimSpace(os.Getenv("AHP_LIFECYCLE_FOREIGN_CLIENT_DIR"))
	if foreignDir != "" {
		front2 = newFrontFrom(t, foreignDir)
	}
	var foreignTok string
	var ok bool
	if foreignDir != "" {
		foreignTok, ok = tokenSoftFrom(t, 1, foreignDir, "h23lc-task-f-"+run, "h23lcf"+run, "artifacts")
	} else {
		foreignTok, ok = tokenSoft(t, 1, "h23lc-task-f-"+run, "h23lcf"+run, "artifacts")
	}
	if !ok {
		t.Log("foreign-tenant token unavailable (no artifacts capability granted to that human, or no second attested workload): cross-tenant leg not exercised")
	} else {
		if foreignDir == "" {
			t.Log("foreign token minted through the owner's workload: the leg runs, but a second-tenant certificate (AHP_LIFECYCLE_FOREIGN_CLIENT_DIR) would make it the stronger form")
		}
		if st, c, _ := front2.call(t, "Get", foreignTok, map[string]any{"id": up.ID}); st == http.StatusOK {
			t.Fatal("a foreign tenant must not read the artifact")
		} else {
			t.Logf("foreign tenant Get refused with %d %s", st, c)
		}
		if st, c, _ := front2.call(t, "Delete", foreignTok, map[string]any{"id": up.ID, "key": key}); st == http.StatusOK {
			t.Fatal("a foreign tenant must not delete the artifact")
		} else {
			t.Logf("foreign tenant Delete refused with %d %s", st, c)
		}
		if st, _, _ := f.call(t, "Get", owner, map[string]any{"id": up.ID}); st != http.StatusOK {
			t.Fatalf("a foreign attempt must leave the owner's row untouched: got %d", st)
		}
	}

	// --- the durable-retention plane: what a workload token may and may not do ---
	// The sweep itself stays refused to a workload whatever the build: no request
	// handler may ask this store to collect bytes, and the scheduler runs GC on
	// its own store whose authorizer admits only the action "gc"
	// (cmd/ahp/mount_gc.go). Pins and Restore are different: they are the owner's
	// own retention controls over the owner's own artifacts, bounded by the
	// verified tenant/subject inside the store - so W-2/W-3 put them on the mount
	// allowlist (cmd/ahp/role_data.go artifactActions) and this leg proves the
	// boundary is still the scope, not the allowlist.
	if st, c, payload := f.call(t, "GC", owner, map[string]any{"key": "h23-gc-" + run, "dry_run": true}); st != http.StatusForbidden || c != "FORBIDDEN" {
		t.Fatalf("GC with a task token must stay refused by design: got %d %s %s", st, c, redact(string(payload)))
	}
	if strings.Contains(string(payload), digest) || strings.Contains(string(payload), up.PresignedURL) {
		t.Fatal("the GC refusal must not carry artifact bytes or grant material")
	}
	t.Logf("sweep refused at the authorize seam: GC -> 403 (action %q)", "gc")

	runRetentionPlane(t, f, front2, owner, foreignTok, up.ID, key, digest, run)

	// --- delete -> both faces uniformly absent ---
	if st, c, _ := f.call(t, "SetLegalHold", owner, map[string]any{"id": up.ID, "on": false}); st != http.StatusOK {
		t.Fatalf("clearing the test hold: %d %s", st, c)
	}
	if st, c, _ := f.call(t, "Delete", owner, map[string]any{"id": up.ID, "key": key}); st != http.StatusOK {
		t.Fatalf("tombstone: %d %s", st, c)
	}
	if st, c, _ := f.call(t, "Get", owner, map[string]any{"id": up.ID}); st != http.StatusNotFound || c != "NOT_FOUND" {
		t.Fatalf("Get after delete: want 404 NOT_FOUND, got %d %s", st, c)
	}
	if resp, got := f.getBytes(t, "Download", owner, map[string]any{"id": up.ID}, [2]string{}); resp.StatusCode != http.StatusNotFound || codeOf(got) != "NOT_FOUND" {
		t.Fatalf("Download after delete: want 404 NOT_FOUND, got %d %s", resp.StatusCode, codeOf(got))
	}
	// Delete must be idempotent from the caller's point of view: a second
	// tombstone of the same id reports absence, not a partial success.
	if st, c, _ := f.call(t, "Delete", owner, map[string]any{"id": up.ID, "key": key + "-again"}); st == http.StatusOK {
		t.Log("second delete reported success (tolerated: tombstone already set)")
	} else if st != http.StatusNotFound && st != http.StatusConflict {
		t.Fatalf("second delete: want 404/409 or 200, got %d %s", st, c)
	}
	if resp, got := f.getBytes(t, "Download", owner, map[string]any{"id": up.ID}, [2]string{}); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("bytes must stay absent after a repeat delete attempt: got %d %s", resp.StatusCode, codeOf(got))
	}
	// The lifecycle face must agree with the object face: either it reports the
	// row as deleted, or it no longer reports it at all. It must never claim the
	// tombstoned artifact is live.
	if st, c, lcBody := f.call(t, "GetLifecycle", owner, map[string]any{"id": up.ID}); st == http.StatusOK {
		var lc struct {
			Deleted *bool `json:"deleted"`
		}
		if err := json.Unmarshal(lcBody, &lc); err != nil {
			t.Fatalf("GetLifecycle after delete: %v (%s)", err, redact(string(lcBody)))
		}
		if lc.Deleted == nil || !*lc.Deleted {
			t.Fatalf("a tombstoned artifact must report deleted=true: %s", redact(string(lcBody)))
		}
	} else {
		t.Logf("lifecycle of a tombstoned artifact is reported as %d %s", st, c)
	}
}

// tokenSoft returns ok=false (never a fatal) when the requested human cannot be
// exchanged for that capability, so a missing grant reads as "leg not exercised"
// rather than as a pass or as somebody else's bug.
func tokenSoft(t *testing.T, userIndex int, task, contextID string, caps ...string) (string, bool) {
	t.Helper()
	return tokenSoftFrom(t, userIndex, clientDir(), task, contextID, caps...)
}

// tokenSoftFrom is tokenSoft for a caller that presents a different client
// certificate: Identity refuses an exchange whose certificate tenant and user
// tenant differ (internal/identityapi/browser.go, azp/tenant boundary), so a
// cross-tenant leg has to be minted by that tenant's own attested workload.
func tokenSoftFrom(t *testing.T, userIndex int, certDir, task, contextID string, caps ...string) (string, bool) {
	t.Helper()
	root := repoRoot(t)
	args := []string{
		filepath.Join(root, "scripts", "identity-task-token.mjs"),
		"--origin", needEnv(t, "AHP_DEPLOYED_URL", "https://127.0.0.1:29612"),
		"--user-index", strconv.Itoa(userIndex), "--task", task, "--context", contextID,
		"--capabilities", strings.Join(caps, ","),
	}
	cmd := exec.Command("node", args...)
	cmd.Dir = root
	cmd.Env = append(os.Environ(),
		"AHP_ACCEPTANCE_USERS_FILE="+needEnv(t, "AHP_ACCEPTANCE_USERS_FILE", ""),
		"AHP_TLS_CLIENT_CERT="+filepath.Join(certDir, "client.crt"),
		"AHP_TLS_CLIENT_KEY="+filepath.Join(certDir, "client.key"),
		"AHP_TLS_CA="+caFile(t),
	)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		t.Logf("token provider refused (user %d): %s", userIndex, redact(errb.String()+out.String()))
		return "", false
	}
	var got struct {
		OK    bool   `json:"ok"`
		Token string `json:"token"`
	}
	if json.Unmarshal(out.Bytes(), &got) != nil || !got.OK || got.Token == "" {
		t.Logf("token provider returned no token for user %d", userIndex)
		return "", false
	}
	return got.Token, true
}

// ---------- W-2/W-3: the owner-reachable retention controls ----------

// pinActions are the two operations W-2 puts on the mount allowlist; Restore is
// W-3. All three must be bound to the verified scope inside the store.
var w23DeniedUntilFix = []struct {
	op, action string
}{
	{"PinRef", "pin"},
	{"UnpinRef", "pin"},
	{"Restore", "restore"},
}

// retentionBody builds the request body each operation accepts: pins name an
// artifact from a reference, Restore names the artifact itself. The front
// refuses unknown fields, so one body cannot serve both.
func retentionBody(op, artifactID string, pinBody map[string]any) map[string]any {
	if op == "Restore" {
		return map[string]any{"id": artifactID}
	}
	return pinBody
}

// runRetentionPlane drives pin/unpin and restore THROUGH THE MOUNTED FRONT.
//
// AHP_W23_EXPECT=red reproduces the defect on a front built without the fix:
// all three answers are 403 FORBIDDEN, i.e. the retention controls PDF section 8
// asks for are unreachable for the owner. AHP_W23_EXPECT=green asserts the
// fixed behaviour: the owner reaches them, the pin is durable outside this
// process, unpin removes it, Restore returns the exact sealed bytes, and another
// tenant is refused on the same ids. Without the variable the leg is skipped
// with a reason rather than passing vacuously.
func runRetentionPlane(t *testing.T, f, foreign *front, owner, foreignTok, artifactID, key, digest, run string) {
	t.Helper()
	mode := strings.TrimSpace(os.Getenv("AHP_W23_EXPECT"))
	if mode == "" {
		t.Skip("AHP_W23_EXPECT unset: pin/restore legs need an explicit red (unfixed front) or green (fixed front) expectation (not a silent pass)")
	}
	pinKind, pinRef := "task", "h23-pin-"+run
	pinBody := map[string]any{"kind": pinKind, "ref_id": pinRef, "artifact_id": artifactID}

	if mode == "red" {
		for _, d := range w23DeniedUntilFix {
			st, c, payload := f.call(t, d.op, owner, retentionBody(d.op, artifactID, pinBody))
			if st != http.StatusForbidden || c != "FORBIDDEN" {
				t.Fatalf("%s on an unfixed front: want 403 FORBIDDEN, got %d %s %s", d.op, st, c, redact(string(payload)))
			}
			if strings.Contains(string(payload), digest) {
				t.Fatal("a refusal must not carry the artifact digest")
			}
			t.Logf("W-2/W-3 reproduced on the deployed front: %s -> 403 (action %q is outside the allowlist)", d.op, d.action)
		}
		if st, c, payload := f.call(t, "ListPins", owner, map[string]any{"kind": pinKind, "ref_id": pinRef}); st != http.StatusOK {
			t.Fatalf("ListPins is a read and must stay admitted: got %d %s", st, c)
		} else if strings.Contains(string(payload), pinRef) {
			t.Fatal("a refused pin must not have been registered")
		}
		return
	}
	if mode != "green" {
		t.Fatalf("AHP_W23_EXPECT must be red or green, got %q", mode)
	}

	// A pin taken by the owner protects the owner's artifact, and the durable
	// registry - not this process's memory - is what says so.
	if st, c, payload := f.call(t, "PinRef", owner, pinBody); st != http.StatusOK {
		t.Fatalf("PinRef must be reachable to the owner of the artifact: got %d %s %s", st, c, redact(string(payload)))
	}
	st, c, payload := f.call(t, "ListPins", owner, map[string]any{"kind": pinKind, "ref_id": pinRef})
	if st != http.StatusOK || !strings.Contains(string(payload), pinRef) {
		t.Fatalf("the pin just taken must be listed: %d %s %s", st, c, redact(string(payload)))
	}
	if !strings.Contains(string(payload), artifactID) {
		t.Fatalf("the listed pin must name the artifact it protects: %s", redact(string(payload)))
	}
	if rows, ran := durablePinCount(t, pinRef); ran {
		if rows != "1" {
			t.Fatalf("the pin must exist in the durable registry outside the front process: rows=%s", rows)
		}
		t.Logf("durable gc_pins row present out of process (ref_id=%s)", pinRef)
	}
	if st, c, payload := f.call(t, "UnpinRef", owner, pinBody); st != http.StatusOK {
		t.Fatalf("UnpinRef must be reachable to the owner: got %d %s %s", st, c, redact(string(payload)))
	}
	if st, c, payload := f.call(t, "ListPins", owner, map[string]any{"kind": pinKind, "ref_id": pinRef}); st != http.StatusOK ||
		strings.Contains(string(payload), pinRef) {
		t.Fatalf("an unregistered pin must stop being listed: %d %s %s", st, c, redact(string(payload)))
	}
	t.Logf("pin/unpin reachable to the owner and durable: %s registered then withdrawn", pinRef)

	// Another tenant holding its own credential cannot pin or restore this row;
	// the refusal is the same shape whether or not it could guess the id.
	if foreignTok != "" {
		for _, op := range []string{"PinRef", "Restore"} {
			if st, c, _ := foreign.call(t, op, foreignTok, retentionBody(op, artifactID, pinBody)); st == http.StatusOK {
				t.Fatalf("a foreign tenant must not be able to %s this artifact", op)
			} else {
				t.Logf("foreign tenant %s refused: %d %s", op, st, c)
			}
		}
		if st, _, _ := f.call(t, "ListPins", owner, map[string]any{"kind": pinKind, "ref_id": pinRef}); st != http.StatusOK {
			t.Fatalf("the owner must still control their own pins after a foreign attempt: got %d", st)
		}
	} else {
		t.Log("no foreign-tenant credential available: cross-tenant pin/restore deny not exercised (see report U-2)")
	}

	// Restore is the owner's own undo: tombstone, then the same bytes again. The
	// hold from the earlier leg has to come off first, and every call uses a
	// distinct idempotency key so the journey keeps its own key unused. The
	// artifact is left live so the ordinary delete leg below still walks its own
	// transitions.
	if st, c, payload := f.call(t, "SetLegalHold", owner, map[string]any{"id": artifactID, "on": false}); st != http.StatusOK {
		t.Fatalf("clearing the hold before the restore leg: %d %s %s", st, c, redact(string(payload)))
	}
	if st, c, payload := f.call(t, "Delete", owner, map[string]any{"id": artifactID, "key": key + "-w23-tombstone"}); st != http.StatusOK {
		t.Fatalf("tombstone before restore: %d %s %s", st, c, redact(string(payload)))
	}
	if st, c, _ := f.call(t, "Get", owner, map[string]any{"id": artifactID}); st != http.StatusNotFound {
		t.Fatalf("the artifact must be absent before Restore: got %d %s", st, c)
	}
	if st, c, payload := f.call(t, "Restore", owner, map[string]any{"id": artifactID}); st != http.StatusOK {
		t.Fatalf("Restore must be reachable to the owner: got %d %s %s", st, c, redact(string(payload)))
	}
	if resp, got := f.getBytes(t, "Download", owner, map[string]any{"id": artifactID}, [2]string{}); resp.StatusCode != http.StatusOK || shaHex(got) != digest {
		t.Fatalf("a restored artifact must serve the exact sealed bytes: status=%d", resp.StatusCode)
	}
	t.Log("delete -> tombstone -> Restore -> Download verified against the sealed digest")
}

// spiffeTenant matches the workload identity a client certificate carries
// (pkg/transport parseWorkloadURI): the tenant the store scopes pins to.
var spiffeTenant = regexp.MustCompile(`^spiffe://ahp/tenant/([^/]+)/deployment/[^/]+$`)

var safeSQLLiteral = regexp.MustCompile(`^[a-z0-9_][a-z0-9_-]{0,63}$`)

// durablePinCount reads the pin registry from OUTSIDE the front process, so the
// proof is a durable row rather than the front's own answer. The tenant comes
// from the client certificate this test authenticates with - the same source the
// server uses - never from a request body. Returns ran=false when no read path
// is configured or available, which the caller reports as an unexercised leg.
func durablePinCount(t *testing.T, pinRef string) (string, bool) {
	t.Helper()
	cert, err := x509.ParseCertificate(mustClientDER(t))
	if err != nil {
		t.Logf("client certificate unreadable for the tenant lookup: %v", err)
		return "", false
	}
	tenant := ""
	for _, uri := range cert.URIs {
		if m := spiffeTenant.FindStringSubmatch(uri.String()); m != nil {
			tenant = m[1]
		}
	}
	if !safeSQLLiteral.MatchString(tenant) {
		t.Logf("no usable tenant in the client certificate workload identity: leg not exercised")
		return "", false
	}
	container := strings.TrimSpace(os.Getenv("AHP_LIFECYCLE_PSQL_CONTAINER"))
	dsn := strings.TrimSpace(os.Getenv("AHP_LIFECYCLE_DB_URL"))
	query := `SELECT count(*) FROM ahp_artifacts.gc_pins WHERE tenant = '` + tenant + `' AND ref_id = '` + pinRef + `'`
	var argv []string
	switch {
	case container != "":
		user := strings.TrimSpace(os.Getenv("AHP_LIFECYCLE_PSQL_USER"))
		db := strings.TrimSpace(os.Getenv("AHP_LIFECYCLE_PSQL_DB"))
		if user == "" || db == "" || !safeSQLLiteral.MatchString(user) || !safeSQLLiteral.MatchString(db) {
			t.Log("AHP_LIFECYCLE_PSQL_USER/DB unset or unexpected shape: leg not exercised")
			return "", false
		}
		argv = []string{"exec", container, "psql", "-U", user, "-d", db, "-tA", "-c", query}
	case dsn != "":
		argv = []string{dsn, "-tA", "-c", query}
	default:
		t.Log("neither AHP_LIFECYCLE_PSQL_CONTAINER nor AHP_LIFECYCLE_DB_URL set: the out-of-process registry read was not attempted (not a silent pass)")
		return "", false
	}
	name := "psql"
	if container != "" {
		name = "docker"
	}
	cmd := exec.Command(name, argv...)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		t.Skipf("out-of-process registry read unavailable (%T): %s", err, redact(errb.String()))
	}
	return strings.TrimSpace(out.String()), true
}

// mustClientDER returns the DER bytes of the client certificate this harness
// authenticates with.
func mustClientDER(t *testing.T) []byte {
	t.Helper()
	pemBytes, err := os.ReadFile(filepath.Join(clientDir(), "client.crt"))
	if err != nil {
		t.Fatalf("client certificate: %v", err)
	}
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		t.Fatal("client.crt holds no PEM block")
	}
	return block.Bytes
}
