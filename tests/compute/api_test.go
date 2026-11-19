package compute_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/internal/compute/computeapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestAPIRejectsNilDependencies proves the facade fails closed at construction:
// a missing store, verifier or authorize is a wiring error (ErrInvalid), while a
// nil runner is a legitimate read-only deployment.
func TestAPIRejectsNilDependencies(t *testing.T) {
	store := newStore(t)
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier(testIssuer, map[string]ed25519.PublicKey{testKID: pub})
	authorize := func(context.Context, auth.Claims, string) error { return nil }

	if _, err := computeapi.New(nil, verifier, authorize, nil); err == nil {
		t.Fatal("nil store must fail closed")
	} else if contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("nil store code = %v, want INVALID", contracts.Code(err))
	}
	if _, err := computeapi.New(store, nil, authorize, nil); err == nil {
		t.Fatal("nil verifier must fail closed")
	}
	if _, err := computeapi.New(store, verifier, nil, nil); err == nil {
		t.Fatal("nil authorize must fail closed")
	}
	if _, err := computeapi.New(store, verifier, authorize, nil); err != nil {
		t.Fatalf("nil runner must be allowed (read-only host): %v", err)
	}
}

// TestAPIUnknownRoute proves routing precedes authentication: an unknown
// operation and a non-POST both 404 without a token ever being consulted.
func TestAPIUnknownRoute(t *testing.T) {
	env := newAPIEnv(t, nil)
	tok := env.signTask(t, "task-unknown")

	code, body := env.call(t, "NoSuchOperation", tok, "{}")
	mustStatus(t, code, http.StatusNotFound, body)

	// A bare GET on a known operation is also an unknown route.
	req, err := http.NewRequest(http.MethodGet, env.srv.URL+"/v1/compute/CreateComputer", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := env.srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET status = %d, want 404", resp.StatusCode)
	}
}

// TestAPIUnauthenticated proves a missing bearer token is rejected before any
// state mutation. transport.Task maps it to FORBIDDEN (403), not 401.
func TestAPIUnauthenticated(t *testing.T) {
	env := newAPIEnv(t, nil)
	code, body := env.call(t, "CreateComputer", "", `{"class":"S","profile":"ephemeral","key":"k"}`)
	mustStatus(t, code, http.StatusForbidden, body)
}

// TestAPICreateComputerHappy is the API-level lock on the quota-resolution fix:
// a create without an explicit quota must come back with the class default
// materialised in the response body.
func TestAPICreateComputerHappy(t *testing.T) {
	env := newAPIEnv(t, nil)
	tok := env.signTask(t, "task-create")

	code, body := env.call(t, "CreateComputer", tok,
		`{"class":"S","profile":"ephemeral","key":"k-happy"}`)
	mustStatus(t, code, http.StatusOK, body)

	if id, _ := body["id"].(string); id == "" {
		t.Fatalf("expected non-empty id, body %v", body)
	}
	if got, _ := body["class"].(string); got != "S" {
		t.Fatalf("class = %q, want S", got)
	}
	if got, _ := body["profile"].(string); got != "ephemeral" {
		t.Fatalf("profile = %q, want ephemeral", got)
	}
	quota, _ := body["quota"].(map[string]any)
	if quota == nil {
		t.Fatalf("expected quota object, body %v", body)
	}
	mp, _ := quota["max_pids"].(float64)
	if int(mp) != 65536 {
		t.Fatalf("class S max_pids = %v, want 65536 (quota default not resolved)", quota["max_pids"])
	}
}

// TestAPIPersonalOnClassCForbidden encodes the security invariant that a
// personal Computer (an employee's own machine) only exists in trusted class S.
func TestAPIPersonalOnClassCForbidden(t *testing.T) {
	env := newAPIEnv(t, nil)
	tok := env.signTask(t, "task-personal-c")

	code, body := env.call(t, "CreateComputer", tok,
		`{"class":"C","profile":"personal","key":"k-p"}`)
	mustStatus(t, code, http.StatusForbidden, body)
}

// TestAPIIdempotentCreate proves the create key is idempotent: replaying the
// same scoped key returns the same Computer rather than a second one.
func TestAPIIdempotentCreate(t *testing.T) {
	env := newAPIEnv(t, nil)
	tok := env.signTask(t, "task-idem")

	_, first := env.call(t, "CreateComputer", tok,
		`{"class":"S","profile":"ephemeral","key":"k-same"}`)
	_, second := env.call(t, "CreateComputer", tok,
		`{"class":"S","profile":"ephemeral","key":"k-same"}`)

	id1, _ := first["id"].(string)
	id2, _ := second["id"].(string)
	if id1 == "" || id1 != id2 {
		t.Fatalf("idempotency broken: id1=%q id2=%q", id1, id2)
	}
}

// TestAPIRunHostBackend exercises the full HTTP path against the only real
// backend available on this host (the jail-hosted exec backend for class S).
func TestAPIRunHostBackend(t *testing.T) {
	runner := hostRunner(t, newStore(t), t.TempDir())
	env := newAPIEnv(t, runner)
	tok := env.signTask(t, "task-run")

	_, created := env.call(t, "CreateComputer", tok,
		`{"class":"S","profile":"ephemeral","key":"k-run"}`)
	cid, _ := created["id"].(string)
	if cid == "" {
		t.Fatalf("create failed: %v", created)
	}

	code, body := env.call(t, "Run", tok, `{
		"computer_id":`+jsonString(cid)+`,
		"key":"cmd-run-1",
		"command":"echo hi"
	}`)
	mustStatus(t, code, http.StatusOK, body)

	if ec, _ := body["exit_code"].(float64); int(ec) != 0 {
		t.Fatalf("exit_code = %v, want 0 (body %v)", body["exit_code"], body)
	}
	if so, _ := body["stdout"].(string); so != "hi\n" {
		t.Fatalf("stdout = %q, want %q", so, "hi\n")
	}
}

// TestAPIRunClassXUnavailable proves that a class X computer, which requires a
// micro-VM backend this host does not provide, surfaces as 503 UNAVAILABLE
// rather than silently degrading to a weaker isolation class.
func TestAPIRunClassXUnavailable(t *testing.T) {
	runner := hostRunner(t, newStore(t), t.TempDir())
	env := newAPIEnv(t, runner)
	tok := env.signTask(t, "task-x")

	_, created := env.call(t, "CreateComputer", tok,
		`{"class":"X","profile":"ephemeral","key":"k-x"}`)
	cid, _ := created["id"].(string)
	if cid == "" {
		t.Fatalf("create failed: %v", created)
	}

	code, body := env.call(t, "Run", tok, `{
		"computer_id":`+jsonString(cid)+`,
		"key":"cmd-x-1",
		"command":"echo hi"
	}`)
	mustStatus(t, code, http.StatusServiceUnavailable, body)
	if codeName, _ := body["code"].(string); codeName != "UNAVAILABLE" {
		t.Fatalf("error code = %q, want UNAVAILABLE", codeName)
	}
}

// TestAPIRunWithoutRunnerForbidden proves a store-only (read-only) host
// refuses to run commands rather than pretending to execute them.
func TestAPIRunWithoutRunnerForbidden(t *testing.T) {
	env := newAPIEnv(t, nil)
	tok := env.signTask(t, "task-norunner")
	code, body := env.call(t, "Run", tok, `{"computer_id":"c-1","key":"k","command":"echo hi"}`)
	mustStatus(t, code, http.StatusForbidden, body)
}

// TestAPIExportWithoutRunnerForbidden is the export analogue of the above.
func TestAPIExportWithoutRunnerForbidden(t *testing.T) {
	env := newAPIEnv(t, nil)
	tok := env.signTask(t, "task-noexport")
	code, body := env.call(t, "Export", tok, `{"computer_id":"c-1","path":"out.bin"}`)
	mustStatus(t, code, http.StatusForbidden, body)
}

// TestAPICommitWorkspaceHappyAndConflict exercises the project copy-on-write
// path over HTTP: a first commit advances main to revision 1, a second commit
// against a stale expected_head is a 409 WORKSPACE_CONFLICT, and the head is
// readable back.
func TestAPICommitWorkspaceHappyAndConflict(t *testing.T) {
	env := newAPIEnv(t, nil)
	tok := env.signTask(t, "task-ws")

	_, created := env.call(t, "CreateComputer", tok,
		`{"class":"S","profile":"project","workspace_id":"ws-1","key":"k-ws"}`)
	cid, _ := created["id"].(string)
	if cid == "" {
		t.Fatalf("create project failed: %v", created)
	}

	ref := `"computer_id":` + jsonString(cid)

	code, head := env.call(t, "CommitWorkspace", tok,
		`{`+ref+`,"expected_head":0,"digest":"sha256:aaa"}`)
	mustStatus(t, code, http.StatusOK, head)
	if rev, _ := head["revision"].(float64); int(rev) != 1 {
		t.Fatalf("first revision = %v, want 1 (body %v)", head["revision"], head)
	}
	if br, _ := head["branch"].(string); br != "main" {
		t.Fatalf("first branch = %q, want main", br)
	}

	// Stale head now conflicts.
	code, body := env.call(t, "CommitWorkspace", tok,
		`{`+ref+`,"expected_head":0,"digest":"sha256:bbb"}`)
	mustStatus(t, code, http.StatusConflict, body)
	if codeName, _ := body["code"].(string); codeName != compute.ErrWorkspaceConflict {
		t.Fatalf("conflict code = %q, want %q", codeName, compute.ErrWorkspaceConflict)
	}

	code, head = env.call(t, "WorkspaceHead", tok, `{`+ref+`}`)
	mustStatus(t, code, http.StatusOK, head)
	if rev, _ := head["revision"].(float64); int(rev) != 1 {
		t.Fatalf("head revision = %v, want 1", head["revision"])
	}
}

// jsonString quotes a value for embedding in a hand-built JSON body without
// pulling in an extra marshal of the whole document.
func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}
