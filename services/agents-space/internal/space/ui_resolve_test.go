package space

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// The human edge's resolver is a pipe with two jobs: give a person the id+exact
// version a digest names, and never become a way to read somebody else's metadata or
// to smuggle the browser's credential to the machine plane. These tests hold it to
// exactly that, against an httptest Artifacts stand.

const testDigest = "1146a4c81194d9a9eecfad4477d2c12dfc8e74d770ae855c7b840d9463930c9e"

func resolveProxyFor(t *testing.T, upstream *httptest.Server) http.Handler {
	t.Helper()
	h, err := NewResolveProxy(ResolveProxyConfig{
		ArtifactsURL: upstream.URL,
		Client:       upstream.Client(),
		CallTimeout:  5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewResolveProxy: %v", err)
	}
	return h
}

func withTask(h http.Handler, token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.ServeHTTP(w, r.WithContext(WithScopedTask(r.Context(), token)))
	})
}

func TestResolveProxyAnswersWithUpstreamVersions(t *testing.T) {
	var gotPath, gotMethod, gotAuth string
	var gotBody map[string]string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod, gotAuth = r.URL.Path, r.Method, r.Header.Get("Authorization")
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		_ = json.NewEncoder(w).Encode(map[string]any{"sha256": testDigest, "versions": []map[string]any{
			{"artifact_id": "art-1", "version": "ver-1", "sha256": testDigest,
				"size": 5, "name": "c-1.txt", "mime": "text/plain"},
		}})
	}))
	defer up.Close()

	srv := httptest.NewServer(withTask(resolveProxyFor(t, up), "task-token-abc"))
	defer srv.Close()

	resp, err := http.Get(srv.URL + UIResolvePath + "?sha256=" + testDigest)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
	if gotPath != "/v1/artifacts/Resolve" || gotMethod != http.MethodPost {
		t.Fatalf("machine call shape: %s %s", gotMethod, gotPath)
	}
	if gotAuth != "Bearer task-token-abc" {
		t.Fatal("resolver must carry the exchanged task token, nothing else")
	}
	if gotBody["sha256"] != testDigest {
		t.Fatalf("digest not forwarded: %v", gotBody)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "no-store" {
		t.Fatalf("resolver answers must not be cacheable, got Cache-Control %q", cc)
	}
	var out struct {
		SHA256   string `json:"sha256"`
		Versions []struct {
			ArtifactID string `json:"artifact_id"`
			Version    string `json:"version"`
		} `json:"versions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.SHA256 != testDigest || len(out.Versions) != 1 || out.Versions[0].ArtifactID != "art-1" ||
		out.Versions[0].Version != "ver-1" {
		t.Fatalf("answer not passed through: %+v", out)
	}
}

func TestResolveProxyRefusesQuestionWithoutDigest(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("upstream must not be called for a malformed question")
	}))
	defer up.Close()
	srv := httptest.NewServer(withTask(resolveProxyFor(t, up), "task-token-abc"))
	defer srv.Close()

	resp, err := http.Get(srv.URL + UIResolvePath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", resp.StatusCode)
	}
	var env struct {
		Code   string `json:"code"`
		Reason string `json:"reason"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&env)
	if env.Code != contracts.ErrInvalid || env.Reason != ReasonDigestRequired {
		t.Fatalf("want INVALID/digest_required, got %s/%s", env.Code, env.Reason)
	}
}

// A browser's human JWT must never reach the machine plane: without an exchanged
// task token the route refuses instead of forwarding.
func TestResolveProxyRefusesWithoutExchangedTaskToken(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("upstream must not be called without a task token")
	}))
	defer up.Close()
	srv := httptest.NewServer(resolveProxyFor(t, up))
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+UIResolvePath+"?sha256="+testDigest, nil)
	req.Header.Set("Authorization", "Bearer human-session-jwt-must-not-be-forwarded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status %d, want 403", resp.StatusCode)
	}
}

// Artifacts' "not yours" must arrive as Artifacts said it, and must not gain fields.
func TestResolveProxyPreservesNotFoundAndLeaksNothing(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"code":"NOT_FOUND","message":"artifact not found"}`))
	}))
	defer up.Close()
	srv := httptest.NewServer(withTask(resolveProxyFor(t, up), "task-token-abc"))
	defer srv.Close()

	resp, err := http.Get(srv.URL + UIResolvePath + "?sha256=" + testDigest)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status %d, want the upstream 404 preserved", resp.StatusCode)
	}
	body := readBodyForTest(t, resp)
	for _, banned := range []string{"object", "bucket", "label", "task", "tenant", "prompt"} {
		if strings.Contains(strings.ToLower(body), banned) {
			t.Fatalf("refusal body mentions %q: %s", banned, body)
		}
	}
}

func TestResolveProxyRequiresTheSHAParamNameAndIgnoresStrays(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		if len(body) != 1 {
			t.Errorf("machine call must carry only the digest, got %v", body)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"sha256": testDigest, "versions": []any{}})
	}))
	defer up.Close()
	srv := httptest.NewServer(withTask(resolveProxyFor(t, up), "task-token-abc"))
	defer srv.Close()

	resp, err := http.Get(srv.URL + UIResolvePath + "?sha256=" + testDigest + "&tenant=other-tenant&subject=mallory")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200 with stray query params ignored", resp.StatusCode)
	}
}

func TestResolveProxyRefusesToBuildWithoutIdentity(t *testing.T) {
	if _, err := NewResolveProxy(ResolveProxyConfig{Client: http.DefaultClient}); err == nil {
		t.Fatal("no Artifacts URL must refuse: the resolver would have nothing to ask")
	}
	if _, err := NewResolveProxy(ResolveProxyConfig{ArtifactsURL: "artifacts.internal:8083"}); err == nil {
		t.Fatal("no workload client must refuse")
	}
	if _, err := NewResolveProxy(ResolveProxyConfig{ArtifactsURL: "https://a.internal:8083", Client: http.DefaultClient}); err != nil {
		t.Fatalf("a complete config must build: %v", err)
	}
}

func TestResolveProxyIsGetOnly(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer up.Close()
	srv := httptest.NewServer(withTask(resolveProxyFor(t, up), "task-token-abc"))
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+UIResolvePath, strings.NewReader(`{"sha256":"`+testDigest+`"}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("the human resolver is a read: POST must not be routed, got %d", resp.StatusCode)
	}
}

func TestResolveContextCancellationStopsTheMachineCall(t *testing.T) {
	// A caller who walks away must not leave the machine call running: the resolver
	// inherits the caller's context, so an abandoned lookup cannot pin an Artifacts
	// worker. No keep-alives here, so closing the stands is deterministic.
	release := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	noKeep := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	h, err := NewResolveProxy(ResolveProxyConfig{ArtifactsURL: up.URL, Client: noKeep, CallTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("NewResolveProxy: %v", err)
	}
	srv := httptest.NewServer(withTask(h, "task-token-abc"))

	defer func() {
		srv.Close()
		close(release)
		up.CloseClientConnections()
		up.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+UIResolvePath+"?sha256="+testDigest, nil)
	resp, err := http.DefaultClient.Do(req)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatalf("expected the cancelled caller to fail, got %d", resp.StatusCode)
	}
}

func readBodyForTest(t *testing.T, resp *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(b)
}
