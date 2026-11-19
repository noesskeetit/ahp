package agentworkload

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/sdk/go/state"
)

func TestNativeMessageCannotContradictAuthenticatedContextOrTask(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := auth.NewSigner("ahp", "test", priv)
	gateway := "spiffe://ahp/tenant/acme/deployment/gateway"
	for _, field := range []string{"contextId", "taskId"} {
		t.Run(field, func(t *testing.T) {
			calls := 0
			stateAPI := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; http.Error(w, "unexpected State mutation", 503) }))
			defer stateAPI.Close()
			s, err := New(Config{Tenant: "acme", Deployment: "agent", AgentID: "a", Digest: "sha256:abc", Image: "a@sha256:abc", Profile: "agent", StateURL: stateAPI.URL, ModelURL: stateAPI.URL, Worker: "worker", GatewayWorkloads: []string{gateway}, Verifier: auth.NewVerifier("ahp", map[string]ed25519.PublicKey{"test": pub}), Client: stateAPI.Client()})
			if err != nil {
				t.Fatal(err)
			}
			message := map[string]any{"messageId": "message", "contextId": "context", "parts": []any{map[string]string{"kind": "text", "text": "hello"}}}
			message[field] = "somebody-else"
			body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "rpc", "method": "SendMessage", "params": map[string]any{"message": message, "metadata": map[string]any{"ahp": map[string]string{"task_id": "task", "agent_id": "a", "version_digest": "sha256:abc"}}}})
			token, err := signer.Sign(auth.Claims{Audience: "ahp", Kind: auth.KindTask, Tenant: "acme", Deployment: "agent", Subject: "user", Task: "task", Context: "context", Root: "root", Grant: "grant"}, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodPost, "https://agent/a2a", bytes.NewReader(body))
			req.Header.Set("Authorization", "Bearer "+token)
			uri, _ := url.Parse(gateway)
			req.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{URIs: []*url.URL{uri}}}}}
			w := httptest.NewRecorder()
			s.ServeHTTP(w, req)
			if w.Code != http.StatusForbidden || calls != 0 {
				t.Fatalf("contradictory message reached State: code=%d calls=%d", w.Code, calls)
			}
		})
	}
}

func TestNativeWorkerLeavesAnotherPublishedDigestPending(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var mu sync.Mutex
	status := "pending"
	polled := make(chan struct{}, 1)
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/state/ClaimRun":
			var in struct {
				Type string `json:"payload_type"`
			}
			_ = json.NewDecoder(r.Body).Decode(&in)
			if in.Type == "" {
				mu.Lock()
				status = "running"
				mu.Unlock()
				_ = json.NewEncoder(w).Encode(map[string]any{"task_token": "token", "lease": state.Lease{Thread: state.Thread{ID: "thread"}, Run: state.Run{ID: "other", ThreadID: "thread", Payload: json.RawMessage(`{"text":"other version","digest":"sha256:other","profile":"agent"}`)}, Worker: "worker", Epoch: 1}})
			} else {
				_, _ = w.Write([]byte(`{"lease":null}`))
			}
			select {
			case polled <- struct{}{}:
			default:
			}
		case "/v1/state/Release":
			mu.Lock()
			status = "failed"
			mu.Unlock()
			_, _ = w.Write([]byte(`{"ok":true}`))
		default:
			t.Errorf("unrelated State call %s", r.URL.Path)
			http.Error(w, "unrelated", 400)
		}
	}))
	defer api.Close()
	client, err := state.New(state.Config{BaseURL: api.URL, HTTPClient: api.Client(), TokenSource: func(context.Context) (string, error) { return "token", nil }})
	if err != nil {
		t.Fatal(err)
	}
	s := &Service{cfg: Config{Digest: "sha256:published", Profile: "agent", Worker: "worker"}, state: client}
	done := make(chan struct{})
	go func() { defer close(done); s.Run(ctx) }()
	select {
	case <-polled:
	case <-ctx.Done():
		t.Fatal("worker never polled")
	}
	cancel()
	<-done
	mu.Lock()
	defer mu.Unlock()
	if status != "pending" {
		t.Fatalf("worker stole another digest's FIFO head: %s", status)
	}
}

func TestGatewayAndTargetCredentialsAreIndependentlyBound(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	signer, err := auth.NewSigner("ahp", "test", priv)
	if err != nil {
		t.Fatal(err)
	}
	gateway := "spiffe://ahp/tenant/acme/deployment/gateway"
	s, err := New(Config{Tenant: "acme", Deployment: "agent-a", AgentID: "a", Digest: "sha256:abc", Image: "example/a@sha256:abc", Profile: "agent", StateURL: "https://localhost", ModelURL: "https://localhost", Worker: "worker", GatewayWorkloads: []string{gateway}, Verifier: auth.NewVerifier("ahp", map[string]ed25519.PublicKey{"test": pub}), Client: &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}}}})
	if err != nil {
		t.Fatal(err)
	}
	makeRequest := func(deployment, sender string) *http.Request {
		token, err := signer.Sign(auth.Claims{Audience: "ahp", Kind: auth.KindTask, Tenant: "acme", Deployment: deployment, Subject: "user", Task: "task", Root: "root", Context: "context", Grant: "grant", Depth: 1}, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "https://agent/a2a", nil)
		u, _ := url.Parse(sender)
		req.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{URIs: []*url.URL{u}}}}}
		req.Header.Set("Authorization", "Bearer "+token)
		return req
	}
	if _, _, err := s.authenticate(makeRequest("agent-a", gateway)); err != nil {
		t.Fatalf("proper cross-deployment gateway hop denied: %v", err)
	}
	for _, r := range []*http.Request{makeRequest("gateway", gateway), makeRequest("agent-b", gateway), makeRequest("agent-a", "spiffe://ahp/tenant/acme/deployment/rogue")} {
		if _, _, err := s.authenticate(r); err == nil {
			t.Fatal("mismatched workload/token admitted")
		}
	}
	r := makeRequest("agent-a", gateway)
	r.TLS.VerifiedChains = nil
	if _, _, err := s.authenticate(r); err == nil {
		t.Fatal("unverified chain admitted")
	}
}

func TestCompletedTaskLoadsCheckpointBlob(t *testing.T) {
	for _, unavailable := range []bool{false, true} {
		t.Run(fmt.Sprint(unavailable), func(t *testing.T) {
			partReads := 0
			api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/state/GetCheckpoint":
					json.NewEncoder(w).Encode(state.Checkpoint{ID: "checkpoint", Namespace: "run", Parts: []state.CheckpointPart{{Channel: "output", Version: "1"}}})
				case "/v1/state/GetPart":
					partReads++
					if unavailable {
						http.Error(w, "unavailable", 503)
						return
					}
					json.NewEncoder(w).Encode(map[string]any{"data": []byte("persisted model answer")})
				default:
					t.Errorf("unexpected path %s", r.URL.Path)
				}
			}))
			defer api.Close()
			client, err := state.New(state.Config{BaseURL: api.URL, HTTPClient: api.Client(), TokenSource: func(context.Context) (string, error) { return "token", nil }})
			if err != nil {
				t.Fatal(err)
			}
			s := &Service{state: client}
			w := httptest.NewRecorder()
			s.taskResponse(context.Background(), w, json.RawMessage(`"rpc"`), "task", state.Run{ID: "run", ThreadID: "thread", Status: state.RunDone})
			if partReads != 1 {
				t.Fatalf("blob reads=%d", partReads)
			}
			var out struct {
				Result struct {
					Artifacts []struct{ Parts []struct{ Text string } }
				}
				Error json.RawMessage
			}
			if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
				t.Fatal(err)
			}
			if unavailable {
				if len(out.Error) == 0 {
					t.Fatal("missing blob returned a completed result")
				}
				return
			}
			if len(out.Result.Artifacts) != 1 || out.Result.Artifacts[0].Parts[0].Text != "persisted model answer" {
				t.Fatalf("lost checkpoint output: %s", w.Body.String())
			}
		})
	}
}

func TestReadinessRequiresLiveClaimLoop(t *testing.T) {
	s := &Service{cfg: Config{AgentID: "agent", Digest: "digest", Image: "image"}}
	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if w.Code != 503 {
		t.Fatal("startup marked ready without State API")
	}
	s.healthy.Store(time.Now().Unix())
	w = httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if w.Code != 200 {
		t.Fatal("healthy worker not ready")
	}
	s.draining.Store(true)
	w = httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if w.Code != 503 {
		t.Fatal("draining worker ready")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.Run(ctx)
}

func TestPinnedGenerationBudgetIsBounded(t *testing.T) {
	calls := 0
	var budget int
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var body struct {
			MaxTokens int `json:"max_tokens"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		budget = body.MaxTokens
		json.NewEncoder(w).Encode(generation{ID: "g", Status: "COMPLETED", Output: "answer"})
	}))
	defer api.Close()
	s := &Service{cfg: Config{ModelURL: api.URL, Client: api.Client()}}
	for _, tc := range []struct {
		config string
		want   int
	}{{`{}`, 4096}, {`{"max_tokens":2048}`, 2048}, {`{"max_tokens":127}`, 0}, {`{"max_tokens":4097}`, 0}} {
		before := calls
		out, err := s.generate(context.Background(), "run", payload{Config: json.RawMessage(tc.config), Text: "question"})
		if tc.want == 0 {
			if err == nil || calls != before {
				t.Fatalf("invalid budget reached provider: %s", tc.config)
			}
			continue
		}
		if err != nil || out != "answer" || budget != tc.want {
			t.Fatalf("budget=%d err=%v", budget, err)
		}
	}
}

// DEF-RELEASE-REASON-1, agent runtime. This binary is the one plane that knows why a
// claimed run stopped, and until now it ended the run with a status nobody explained and
// then discarded the door's answer (`_ =`). Both halves are pinned against a fake State:
// the reason must travel on the wire, and a refusal must be counted where an operator can
// read it instead of dropped.
func TestTerminalReleaseCarriesTheReasonAndARefusalIsNotSwallowed(t *testing.T) {
	for _, refused := range []bool{false, true} {
		t.Run(fmt.Sprintf("door_refuses=%v", refused), func(t *testing.T) {
			var releases []map[string]any
			api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/state/Release" {
					t.Errorf("unexpected State call %s - the mismatched payload must fail before any model or checkpoint work", r.URL.Path)
					http.Error(w, "unexpected", 400)
					return
				}
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Errorf("release body is not readable JSON: %v", err)
				}
				releases = append(releases, body)
				w.Header().Set("Content-Type", "application/json")
				if refused {
					w.WriteHeader(http.StatusBadRequest)
					_ = json.NewEncoder(w).Encode(map[string]string{"code": "INVALID_ARGUMENT",
						"message": "state: INVALID_ARGUMENT: release may only end a run with a stated reason (close_reason and/or failure_cause)"})
					return
				}
				_, _ = w.Write([]byte(`{"ok":true}`))
			}))
			defer api.Close()
			client, err := state.New(state.Config{BaseURL: api.URL, HTTPClient: api.Client(),
				TokenSource: func(ctx context.Context) (string, error) {
					v, _ := ctx.Value(tokenKey{}).(string)
					return v, nil
				}})
			if err != nil {
				t.Fatal(err)
			}
			s := &Service{cfg: Config{Digest: "sha256:published", Profile: "agent"}, state: client}
			// A payload published by a different build: the run cannot be executed here,
			// which is the cheapest honest failure this runtime can produce on demand.
			s.execute(context.Background(), &state.Claimed{Lease: state.Lease{
				Thread: state.Thread{ID: "thread-1"},
				Run:    state.Run{ID: "run-1", ThreadID: "thread-1", Payload: json.RawMessage(`{"text":"x","digest":"sha256:someone-elses-build","profile":"agent"}`)},
				Worker: "worker-1", Epoch: 4,
			}, TaskToken: "task-token"})

			if len(releases) != 1 {
				t.Fatalf("the runtime gets one terminal release per claim, got %d: %v", len(releases), releases)
			}
			body := releases[0]
			if body["status"] != state.RunFailed {
				t.Fatalf("status=%v, want failed", body["status"])
			}
			if body["close_reason"] != "error" {
				t.Errorf("close_reason=%v, want the dictionary word error", body["close_reason"])
			}
			cause, _ := body["failure_cause"].(string)
			if !strings.Contains(cause, "digest") {
				t.Errorf("failure_cause must name the branch that ended the run, got %q", cause)
			}
			s.healthy.Store(time.Now().Unix())
			w := httptest.NewRecorder()
			s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))
			var readiness struct {
				Ready                bool  `json:"ready"`
				DurableCloseRefusals int64 `json:"durable_close_refusals"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &readiness); err != nil {
				t.Fatal(err)
			}
			want := int64(0)
			if refused {
				want = 1
			}
			if readiness.DurableCloseRefusals != want {
				t.Fatalf("durable_close_refusals=%d, want %d (body=%s)", readiness.DurableCloseRefusals, want, w.Body.String())
			}
		})
	}
}

// The honest happy path keeps its wire form: a run that ends completed states no reason,
// because the checkpoint IS its verdict. If this ever starts sending a key, the payload
// of every done run in the fleet changes shape.
func TestCompletedRunEndsWithoutAReason(t *testing.T) {
	var releases []map[string]any
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/state/GetCheckpoint":
			_ = json.NewEncoder(w).Encode(state.Checkpoint{ID: "cp-1", Namespace: "run-1"})
		case "/v1/state/Release":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("release body is not readable JSON: %v", err)
			}
			releases = append(releases, body)
			_, _ = w.Write([]byte(`{"ok":true}`))
		default:
			t.Errorf("unexpected State call %s", r.URL.Path)
			http.Error(w, "unexpected", 400)
		}
	}))
	defer api.Close()
	client, err := state.New(state.Config{BaseURL: api.URL, HTTPClient: api.Client(),
		TokenSource: func(context.Context) (string, error) { return "task-token", nil }})
	if err != nil {
		t.Fatal(err)
	}
	s := &Service{cfg: Config{Digest: "sha256:published", Profile: "agent"}, state: client}
	s.execute(context.Background(), &state.Claimed{Lease: state.Lease{
		Thread: state.Thread{ID: "thread-1"},
		Run:    state.Run{ID: "run-1", ThreadID: "thread-1", Payload: json.RawMessage(`{"text":"x","digest":"sha256:published","profile":"agent"}`)},
		Worker: "worker-1", Epoch: 4,
	}, TaskToken: "task-token"})
	if len(releases) != 1 {
		t.Fatalf("one release, got %d", len(releases))
	}
	if releases[0]["status"] != state.RunDone {
		t.Fatalf("status=%v, want done", releases[0]["status"])
	}
	for _, key := range []string{"close_reason", "failure_cause"} {
		if _, present := releases[0][key]; present {
			t.Errorf("a completed run must send no %q key: %v", key, releases[0])
		}
	}
	if s.closeRefusals.Load() != 0 {
		t.Errorf("an accepted release must not count as a refusal: %d", s.closeRefusals.Load())
	}
}
