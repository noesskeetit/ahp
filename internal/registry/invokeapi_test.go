package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// fakeDispatcher records the last InvokeRequest and returns a canned task id, so
// the test can prove the handler derives tenant/agent/version from the
// installation row (never the body) and forwards the caller's bearer.
type fakeDispatcher struct {
	called  int
	last    InvokeRequest
	taskID  string
	wantErr error
}

func (f *fakeDispatcher) Invoke(_ context.Context, req InvokeRequest) (string, error) {
	f.called++
	f.last = req
	if f.wantErr != nil {
		return "", f.wantErr
	}
	return f.taskID, nil
}

func callInvoke(t *testing.T, hh *HTTPHandler, body any, scope contracts.Scope, bearer string) (int, map[string]any) {
	t.Helper()
	fn, ok := hh.routes["Invoke"]
	if !ok {
		t.Fatalf("registry route Invoke is NOT wired (would be 404 unknown route)")
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/v1/registry/Invoke", bytes.NewReader(raw))
	req = req.WithContext(transport.WithPrincipal(req.Context(), transport.Principal{Scope: scope}))
	if bearer != "" {
		req.Header.Set("authorization", "Bearer "+bearer)
	}
	w := httptest.NewRecorder()
	fn(w, req)
	out := map[string]any{}
	if b := w.Body.Bytes(); len(b) > 0 {
		_ = json.Unmarshal(b, &out)
	}
	return w.Code, out
}

func TestRegistryInvokeFullPath(t *testing.T) {
	hh, evalStore := newHTTPFixture(t)
	suf := httpSuffix(t)
	publisher := "pub-h26-" + suf
	tenant := "t-h26-" + suf
	agent := "agent-h26-" + suf
	scope := contracts.Scope{Tenant: tenant, Deployment: "d-h26-" + suf, Subject: publisher}

	// publish an eval-passed version, then install it.
	if code, out := httpCall(t, hh, "RegisterAgent", map[string]any{"agent_id": agent}, scope); code != http.StatusOK {
		t.Fatalf("register: %d %v", code, out)
	}
	m := validManifest()
	m.State.Compat = "c" + suf
	digest, err := contracts.Digest(m)
	if err != nil {
		t.Fatal(err)
	}
	httpPassVerdict(t, evalStore, digest)
	code, out := httpCall(t, hh, "PutVersion", map[string]any{"agent_id": agent, "digest": digest, "manifest": m}, scope)
	if code != http.StatusOK {
		t.Fatalf("publish: %d %v", code, out)
	}
	version := nestedID(t, out, "version")
	code, out = httpCall(t, hh, "Install", map[string]any{"agent_id": agent, "version_id": version, "environment": "dev", "key": "k-" + suf, "granted": m.Capabilities}, scope)
	if code != http.StatusOK {
		t.Fatalf("install: %d %v", code, out)
	}
	installation := nestedID(t, out, "installation")

	// Invoke WITHOUT a dispatcher wired -> fail-closed (409), never 404.
	if code, _ := callInvoke(t, hh, map[string]any{"installation_id": installation}, scope, "tok"); code != http.StatusConflict {
		t.Fatalf("Invoke with no dispatcher must fail closed 409, got %d", code)
	}

	// Wire the fake dispatcher and invoke. The caller's bearer must pass through.
	fd := &fakeDispatcher{taskID: "task-" + suf}
	if err := AttachInvokeDispatcher(hh, fd); err != nil {
		t.Fatalf("attach: %v", err)
	}
	code, out = callInvoke(t, hh, map[string]any{"installation_id": installation, "input": "do the thing"}, scope, "bearer-"+suf)
	if code != http.StatusOK || out["task_id"] != "task-"+suf {
		t.Fatalf("Invoke: %d %v", code, out)
	}
	if fd.called != 1 {
		t.Fatalf("dispatcher should be called exactly once, got %d", fd.called)
	}
	if fd.last.Tenant != tenant {
		t.Fatalf("invoke tenant must come from the cert (%s), got %s", tenant, fd.last.Tenant)
	}
	if fd.last.AgentID != agent || fd.last.VersionID != version || fd.last.InstallationID != installation {
		t.Fatalf("invoke must derive agent/version from the installation row, got %+v", fd.last)
	}
	if fd.last.VersionDigest != digest {
		t.Fatalf("invoke pin digest must be the installed version's digest, want %s got %s", digest, fd.last.VersionDigest)
	}
	if fd.last.Token != "bearer-"+suf {
		t.Fatalf("invoke must forward the caller bearer, got %q", fd.last.Token)
	}

	// A foreign tenant invoking the owner's installation is scoped out and the
	// dispatcher is NOT called.
	foreign := contracts.Scope{Tenant: "x-h26-" + suf, Deployment: "d-h26-" + suf, Subject: publisher}
	before := fd.called
	if code, _ := callInvoke(t, hh, map[string]any{"installation_id": installation}, foreign, "tok"); code != http.StatusNotFound {
		t.Fatalf("foreign Invoke must be 404, got %d", code)
	}
	if fd.called != before {
		t.Fatalf("dispatcher must not run for a foreign tenant")
	}

	// The body cannot smuggle a version/agent override.
	if code, _ := callInvoke(t, hh, map[string]any{"installation_id": installation, "version_id": "evil"}, scope, "tok"); code != http.StatusBadRequest {
		t.Fatalf("body-injected version_id must be rejected 400, got %d", code)
	}
}

func TestRegistryInvokeUnknownInstallation(t *testing.T) {
	hh, _ := newHTTPFixture(t)
	suf := httpSuffix(t)
	scope := contracts.Scope{Tenant: "t-h26-" + suf, Deployment: "d-h26-" + suf, Subject: "pub-h26-" + suf}
	fd := &fakeDispatcher{taskID: "x"}
	if err := AttachInvokeDispatcher(hh, fd); err != nil {
		t.Fatal(err)
	}
	if code, _ := callInvoke(t, hh, map[string]any{"installation_id": "inst-nope-" + suf}, scope, "tok"); code != http.StatusNotFound {
		t.Fatalf("unknown installation must be 404, got %d", code)
	}
	if fd.called != 0 {
		t.Fatalf("dispatcher must not run for an unknown installation")
	}
}
