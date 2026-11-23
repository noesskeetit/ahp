//go:build unix

package vmruntime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
)

func injectorTestGuest() Guest {
	exec := Execution{
		Scope:     contracts.Scope{Tenant: "tenant", Deployment: "agent", Subject: "alice"},
		SessionID: "session/alpha", Harness: "openclaw", LaunchTurnSeq: 7, Generation: 3,
	}
	exec.ID = journal.DeterministicExecutionID(exec.Scope, exec.SessionID, exec.LaunchTurnSeq, exec.Generation)
	return Guest{Execution: exec, Attempt: 2}
}

func injectorTestSocket(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "ahpi-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "x.sock")
}

func injectorTestJWT(expiry time.Time) string {
	payload := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"exp":%d}`, expiry.Unix())))
	return "eyJhbGciOiJFZERTQSJ9." + payload + ".unused"
}

func injectorTestClient(socket string) *http.Client {
	return &http.Client{
		Timeout: 3 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", socket)
			},
		},
	}
}

func injectorTestRequest(t *testing.T, client *http.Client, method, path, body string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, "http://guest"+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer guest-owned")
	req.Header.Set("X-Forwarded-For", "guest-owned")
	req.Header.Set("Cookie", "guest-auth=1")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, raw
}

func TestInjectorUsesFixedStateAndAttemptToken(t *testing.T) {
	guest := injectorTestGuest()
	socket := injectorTestSocket(t)
	served := make(chan *http.Request, 3)
	var mints atomic.Int64
	var scopes atomic.Int64
	token := injectorTestJWT(time.Now().Add(5 * time.Minute))
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served <- r
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Set-Cookie", "upstream-auth=1")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer upstream.Close()
	i, err := NewInjector(InjectorConfig{
		StateURL: upstream.URL,
		ClientFor: func(_ context.Context, scope contracts.Scope) (*http.Client, error) {
			if scope == guest.Execution.Scope {
				scopes.Add(1)
			}
			return upstream.Client(), nil
		},
		MintToken: func(_ context.Context, exec Execution) (string, error) {
			if exec != guest.Execution {
				return "", errors.New("wrong execution")
			}
			mints.Add(1)
			return token, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := i.Start(ctx, guest, socket); err != nil {
		t.Fatal(err)
	}
	defer i.Close(guest)
	cancel()
	if info, err := os.Stat(socket); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("socket permissions are wrong: %v %v", info, err)
	}
	if !i.Listening(guest) {
		t.Fatal("canceling the launcher also canceled the attempt")
	}
	client := injectorTestClient(socket)
	defer client.CloseIdleConnections()
	resp, body := injectorTestRequest(t, client, http.MethodGet, "/v1/runtime/bootstrap", "")
	var bootstrap Execution
	if err := json.Unmarshal(body, &bootstrap); err != nil || bootstrap != guest.Execution || resp.StatusCode != http.StatusOK {
		t.Fatalf("bootstrap status %d: %s (%v)", resp.StatusCode, body, err)
	}
	if mints.Load() != 0 || len(served) != 0 {
		t.Fatal("bootstrap reached upstream or minted a token")
	}
	for _, route := range []struct{ input, forwarded string }{
		{"/v1/state/sessions/session%2Falpha/events?limit=2&after_seq=1", "/v1/state/sessions/session%2Falpha/events?after_seq=1&limit=2"},
		{"/v1/state/kv?scope_type=session&key=one&key=two&session_id=session%2Falpha", "/v1/state/kv?key=one&key=two&scope_type=session&session_id=session%2Falpha"},
	} {
		resp, body := injectorTestRequest(t, client, http.MethodGet, route.input, "")
		if resp.StatusCode != http.StatusOK || string(body) != `{"ok":true}` || resp.Header.Get("Set-Cookie") != "" {
			t.Fatalf("response %d %s, headers %v", resp.StatusCode, body, resp.Header)
		}
		forwarded := <-served
		if forwarded.RequestURI != route.forwarded || forwarded.Host == "guest" ||
			forwarded.Header.Get("Authorization") != "Bearer "+token ||
			forwarded.Header.Get("X-Forwarded-For") != "" || forwarded.Header.Get("Cookie") != "" {
			t.Fatalf("unexpected upstream request %s host=%s headers=%v", forwarded.RequestURI, forwarded.Host, forwarded.Header)
		}
	}
	if mints.Load() != 1 || scopes.Load() != 2 {
		t.Fatalf("token mints = %d, scoped clients = %d", mints.Load(), scopes.Load())
	}
}

func TestInjectorRejectsUnscopedRoutesAndSelectorsLocally(t *testing.T) {
	guest := injectorTestGuest()
	socket := injectorTestSocket(t)
	var outbound atomic.Int64
	i, err := NewInjector(InjectorConfig{
		StateURL: "https://unused.invalid",
		ClientFor: func(context.Context, contracts.Scope) (*http.Client, error) {
			outbound.Add(1)
			return nil, errors.New("unexpected network dispatch")
		},
		MintToken: func(context.Context, Execution) (string, error) {
			outbound.Add(1)
			return "", errors.New("unexpected token mint")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := i.Start(context.Background(), guest, socket); err != nil {
		t.Fatal(err)
	}
	defer i.Close(guest)
	client := injectorTestClient(socket)
	defer client.CloseIdleConnections()
	session := "/v1/state/sessions/session%2Falpha"
	for _, tc := range []struct{ method, path, body string }{
		{"GET", "/v1/state/sessions", ""},
		{"GET", "/v1/state/sessions/session-sibling/events", ""},
		{"DELETE", session, ""},
		{"PATCH", session + "/legal-hold", `{"held":false}`},
		{"POST", "/v1/state/sessions:enqueue", "{}"},
		{"POST", session + "/vm:cancel", "{}"},
		{"POST", session + "/vm:requeue-fenced", "{}"},
		{"GET", "/internal/v1/state/vm-reconcile", ""},
		{"HEAD", session, ""},
		{"GET", session + "?view=catalog", ""},
		{"GET", session + "/events?state_namespace=shared", ""},
		{"GET", session + "/events?after_seq=1&%61fter_seq=2", ""},
		{"GET", "/v1/state/sessions/session%2falpha/events", ""},
		{"GET", session + "/../events", ""},
		{"POST", session + "/checkpoints/a%2Fb/writes", "{}"},
		{"POST", session + "/vm:claim", `{"generation":2,"execution_id":"` + guest.Execution.ID + `"}`},
		{"POST", session + "/vm:claim", `{"generation":3,"execution_id":"wrong"}`},
		{"POST", session + ":heartbeat", `{"release":true}`},
		{"POST", session + ":heartbeat", `{"release":true,"Release":false}`},
		{"POST", session + ":heartbeat", `{"release":false,"release":false}`},
		{"POST", session + ":heartbeat", `{"release":false,"session_id":"wrong"}`},
		{"POST", session + "/events:append", `{"kv":[{"scope_type":"user"}]}`},
		{"GET", "/v1/state/kv?scope_type=session&session_id=session%2Falpha&session_id=wrong", ""},
		{"GET", "/v1/state/kv?scope_type=session&session_id=session%2Falpha;session_id=wrong", ""},
		{"GET", "/v1/state/kv?scope_type=user&session_id=session%2Falpha", ""},
		{"POST", "/v1/state/kv:mutate", `{"session_id":"session/alpha","kv":[{"scope_type":"user"}]}`},
		{"POST", "/v1/state/aux:mutate", `{"session_id":"wrong"}`},
		{"POST", "/v1/state/intents:mutate", `{"session_id":"wrong"}`},
		{"GET", "/v1/state/aux:mutate?session_id=session%2Falpha&state_namespace=other", ""},
	} {
		t.Run(tc.method+" "+tc.path+" "+tc.body, func(t *testing.T) {
			resp, body := injectorTestRequest(t, client, tc.method, tc.path, tc.body)
			if resp.StatusCode != http.StatusForbidden {
				t.Fatalf("status %d: %s", resp.StatusCode, body)
			}
		})
	}
	if outbound.Load() != 0 {
		t.Fatalf("local denials dispatched %d credential or network calls", outbound.Load())
	}
}

func TestInjectorCloseCancelsInFlightStateRequest(t *testing.T) {
	guest := injectorTestGuest()
	socket := injectorTestSocket(t)
	entered, stopped := make(chan struct{}), make(chan struct{})
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
		close(stopped)
	}))
	defer upstream.Close()
	i, err := NewInjector(InjectorConfig{
		StateURL: upstream.URL,
		ClientFor: func(context.Context, contracts.Scope) (*http.Client, error) {
			return upstream.Client(), nil
		},
		MintToken: func(context.Context, Execution) (string, error) {
			return injectorTestJWT(time.Now().Add(5 * time.Minute)), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := i.Start(context.Background(), guest, socket); err != nil {
		t.Fatal(err)
	}
	defer i.Close(guest)
	client := injectorTestClient(socket)
	defer client.CloseIdleConnections()
	guestDone := make(chan error, 1)
	go func() {
		resp, err := client.Get("http://guest/v1/state/sessions/session%2Falpha")
		if resp != nil {
			resp.Body.Close()
		}
		guestDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("upstream request never began")
	}
	fence := guest
	fence.Execution.Harness, fence.Execution.LaunchTurnSeq = "", 0
	if err := i.Close(fence); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("closing the attempt did not cancel the upstream request")
	}
	if err := <-guestDone; err == nil {
		t.Fatal("guest request completed after its attempt closed")
	}
	if err := i.Close(guest); err != nil || i.Listening(guest) {
		t.Fatalf("idempotent close: %v", err)
	}
	if _, err := os.Lstat(socket); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket survived close: %v", err)
	}
}

func TestInjectorPreservesAllowedJSONAndStateSessionPaths(t *testing.T) {
	guest := injectorTestGuest()
	guest.Execution.SessionID = ".."
	guest.Execution.ID = journal.DeterministicExecutionID(guest.Execution.Scope, guest.Execution.SessionID, guest.Execution.LaunchTurnSeq, guest.Execution.Generation)
	socket := injectorTestSocket(t)
	bodies := make(chan string, 3)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/state/sessions/{session}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("session") != ".." {
			t.Errorf("forwarded session %q", r.PathValue("session"))
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /v1/state/sessions/{session}/vm:claim", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("session") != ".." {
			t.Errorf("forwarded session %q", r.PathValue("session"))
		}
		raw, _ := io.ReadAll(r.Body)
		bodies <- string(raw)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /v1/state/sessions/{session}/checkpoints/{checkpoint}/writes", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("session") != ".." || r.PathValue("checkpoint") != "." {
			t.Errorf("forwarded session %q checkpoint %q", r.PathValue("session"), r.PathValue("checkpoint"))
		}
		raw, _ := io.ReadAll(r.Body)
		bodies <- string(raw)
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /v1/state/aux:mutate", func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		bodies <- string(raw)
		w.WriteHeader(http.StatusNoContent)
	})
	upstream := httptest.NewTLSServer(mux)
	defer upstream.Close()
	i, err := NewInjector(InjectorConfig{
		StateURL: upstream.URL,
		ClientFor: func(context.Context, contracts.Scope) (*http.Client, error) {
			return upstream.Client(), nil
		},
		MintToken: func(context.Context, Execution) (string, error) {
			return injectorTestJWT(time.Now().Add(5 * time.Minute)), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := i.Start(t.Context(), guest, socket); err != nil {
		t.Fatal(err)
	}
	defer i.Close(guest)
	client := injectorTestClient(socket)
	defer client.CloseIdleConnections()
	resp, body := injectorTestRequest(t, client, http.MethodGet, "/v1/state/sessions/..", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("session status returned %d: %s", resp.StatusCode, body)
	}
	for _, route := range []struct{ path, json string }{
		{"/v1/state/sessions/../vm:claim", `{ "execution_id": "` + guest.Execution.ID + `", "generation": 3, "turn_seq": 7, "ttl_ms": 2000 }`},
		{"/v1/state/sessions/../checkpoints/./writes", `{"lease_epoch":8,"namespace":"ns","writes":[]}`},
		{"/v1/state/aux:mutate", `{"session_id":"..","lease_epoch":8,"transition":{"call_id":"<existing>","status":"SUCCEEDED","gateway_operation_id":"g"}}`},
	} {
		resp, body := injectorTestRequest(t, client, http.MethodPost, route.path, route.json)
		if resp.StatusCode != http.StatusNoContent {
			t.Errorf("%s returned %d: %s", route.path, resp.StatusCode, body)
			continue
		}
		if forwarded := <-bodies; forwarded != route.json {
			t.Errorf("changed valid JSON: %s", forwarded)
		}
	}
}

func TestInjectorCheckpointBodyLimits(t *testing.T) {
	guest := injectorTestGuest()
	socket := injectorTestSocket(t)
	type forwardedRequest struct{ path, body string }
	forwarded := make(chan forwardedRequest, 4)
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read forwarded checkpoint body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		forwarded <- forwardedRequest{r.RequestURI, string(body)}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()
	i, err := NewInjector(InjectorConfig{
		StateURL: upstream.URL,
		ClientFor: func(context.Context, contracts.Scope) (*http.Client, error) {
			return upstream.Client(), nil
		},
		MintToken: func(context.Context, Execution) (string, error) {
			return injectorTestJWT(time.Now().Add(5 * time.Minute)), nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := i.Start(t.Context(), guest, socket); err != nil {
		t.Fatal(err)
	}
	defer i.Close(guest)
	client := injectorTestClient(socket)
	defer client.CloseIdleConnections()

	// Each inline value fits the State API's 256 KiB limit. Their aggregate
	// encoding exceeds the injector's default body limit before any padding.
	payload := journal.Payload{Inline: []byte(strings.Repeat("x", 256<<10))}
	writes := make([]journal.PendingWrite, 4)
	for n := range writes {
		writes[n] = journal.PendingWrite{TaskID: "task", Index: int32(n), Channel: "result", Codec: "raw", Payload: payload}
	}
	checkpoint := journal.CheckpointCommit{
		TurnSeq: 7, Key: "check-1", Writes: writes,
		Checkpoint: journal.Checkpoint{
			ID: "cp-1", Namespace: "ns", Codec: "raw", Metadata: json.RawMessage(`{}`),
			ChannelVersions: json.RawMessage(`{}`), Payload: payload,
		},
	}
	const maxBody = 3 << 20
	const session = "/v1/state/sessions/session%2Falpha"
	for _, route := range []struct {
		name, path string
		body       any
	}{
		{"commit", session + "/checkpoints", struct {
			Epoch int64 `json:"lease_epoch"`
			journal.CheckpointCommit
		}{8, checkpoint}},
		{"pending_writes", session + "/checkpoints/cp-1/writes", map[string]any{
			"lease_epoch": 8, "turn_seq": 7, "namespace": "ns", "writes": writes,
		}},
	} {
		t.Run(route.name, func(t *testing.T) {
			raw, err := json.Marshal(route.body)
			if err != nil {
				t.Fatal(err)
			}
			if len(raw) <= 1<<20 || len(raw) > maxBody {
				t.Fatalf("fixture size %d does not exercise the checkpoint limit", len(raw))
			}
			atLimit := string(raw) + strings.Repeat(" ", maxBody-len(raw))
			resp, body := injectorTestRequest(t, client, http.MethodPost, route.path, atLimit)
			if resp.StatusCode != http.StatusNoContent {
				t.Errorf("%d-byte checkpoint body (%d-byte JSON) returned %d: %s", len(atLimit), len(raw), resp.StatusCode, body)
			} else if got := <-forwarded; got.path != route.path || got.body != atLimit {
				t.Errorf("upstream received altered checkpoint request for %s (%d bytes)", got.path, len(got.body))
			}
			resp, body = injectorTestRequest(t, client, http.MethodPost, route.path, atLimit+" ")
			if resp.StatusCode != http.StatusForbidden || len(forwarded) != 0 {
				t.Errorf("oversized checkpoint body returned %d with %d upstream requests: %s", resp.StatusCode, len(forwarded), body)
			}
		})
	}
}

func TestInjectorRejectsUpstreamRedirectAndRefreshesToken(t *testing.T) {
	guest := injectorTestGuest()
	socket := injectorTestSocket(t)
	var redirected atomic.Int64
	other := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirected.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer other.Close()
	nearExpiry := injectorTestJWT(time.Now().Add(15 * time.Second))
	fresh := injectorTestJWT(time.Now().Add(5 * time.Minute))
	auth := make(chan string, 3)
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth <- r.Header.Get("Authorization")
		http.Redirect(w, r, other.URL+"/outside", http.StatusTemporaryRedirect)
	}))
	defer upstream.Close()
	var mints atomic.Int64
	i, err := NewInjector(InjectorConfig{
		StateURL: upstream.URL,
		ClientFor: func(context.Context, contracts.Scope) (*http.Client, error) {
			return upstream.Client(), nil
		},
		MintToken: func(context.Context, Execution) (string, error) {
			if mints.Add(1) == 1 {
				return nearExpiry, nil
			}
			return fresh, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := i.Start(t.Context(), guest, socket); err != nil {
		t.Fatal(err)
	}
	defer i.Close(guest)
	client := injectorTestClient(socket)
	defer client.CloseIdleConnections()
	for _, expected := range []string{nearExpiry, fresh, fresh} {
		resp, body := injectorTestRequest(t, client, http.MethodGet, "/v1/state/sessions/session%2Falpha", "")
		if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Location") != "" {
			t.Fatalf("redirect exposed to guest: status %d, headers %v, body %s", resp.StatusCode, resp.Header, body)
		}
		if sent := <-auth; sent != "Bearer "+expected {
			t.Fatal("upstream received the wrong credentials")
		}
	}
	if mints.Load() != 2 || redirected.Load() != 0 {
		t.Fatalf("mints = %d, redirects followed = %d", mints.Load(), redirected.Load())
	}
}
