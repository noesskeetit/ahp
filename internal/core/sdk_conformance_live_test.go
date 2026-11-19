//go:build sdk_conformance

package core

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/stateapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	sdkstate "github.com/ahp-platform/ahp/sdk/go/state"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Opt in with -tags sdk_conformance. The selected test refuses missing real
// infrastructure rather than turning a missing DSN/Python into a green skip.
func TestSDKRuntimeRealOwners(t *testing.T) {
	sdkRuntimeRealOwners(t, []string{"sync-saver", "mixed-store", "async-saver", "hitl", "pending-restart", "failure", "standalone"})
}

func TestSDKEffectsRealOwners(t *testing.T) {
	sdkRuntimeRealOwners(t, []string{"effects-loop", "effects-positional", "effects-unknown", "effects-legacy-completed", "effects-legacy-unknown", "effects-legacy-dispatched"})
}

func sdkRuntimeRealOwners(t *testing.T, scenarios []string) {
	dsn, python := os.Getenv("AHP_TEST_DATABASE_URL"), os.Getenv("AHP_SDK_PYTHON")
	if dsn == "" || python == "" {
		t.Fatal("real PG and AHP_SDK_PYTHON required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal("PG configuration")
	}
	defer pool.Close()
	ids := identity.New(pool)
	if err = ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	st := state.New(pool)
	if err = st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := auth.NewSigner("sdk-runtime", "k", priv)
	verifier := auth.NewVerifier("sdk-runtime", map[string]ed25519.PublicKey{"k": pub})
	handler, err := stateapi.New(st, verifier, ids.Authorize, func(ctx context.Context, s contracts.Scope, task string) (string, error) {
		return ids.MintTask(ctx, s, task, signer, "ahp", 10*time.Minute)
	})
	if err != nil {
		t.Fatal(err)
	}
	var failAt, checkpointCalls atomic.Int64
	var effectLock sync.Mutex
	var effects []int
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Synthetic tool counter (not a model or Tool Gateway); persists across
		// Python process restarts so an extra physical dispatch is observable.
		if r.URL.Path == "/fixture/effect" {
			var input struct {
				N int `json:"n"`
			}
			if json.NewDecoder(r.Body).Decode(&input) != nil {
				http.Error(w, "bad fixture", 400)
				return
			}
			effectLock.Lock()
			effects = append(effects, input.N)
			effectLock.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]int{"n": input.N})
			return
		}
		if r.URL.Path == "/v1/state/PutCheckpoint" && failAt.Load() > 0 && checkpointCalls.Add(1) == failAt.Load() {
			http.Error(w, `{"code":"UNAVAILABLE","message":"injected checkpoint failure"}`, 503)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	ca := newPayloadCA(t)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	server.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	server.StartTLS()
	defer server.Close()
	scope := contracts.Scope{Tenant: fmt.Sprintf("sdk-runtime-%d", time.Now().UnixNano()), Deployment: "runtime", Subject: "alice"}
	cert := ca.clientCert(t, auth.WorkloadID(scope))
	httpClient := server.Client()
	transport := httpClient.Transport.(*http.Transport).Clone()
	transport.TLSClientConfig.Certificates = []tls.Certificate{cert}
	httpClient.Transport = transport
	dir := t.TempDir()
	write := func(name string, data []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, data, 0600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	key, _ := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	caFile := write("ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.TLS.Certificates[0].Certificate[0]}))
	certFile := write("client.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}))
	keyFile := write("key.pem", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}))
	for _, scenario := range scenarios {
		t.Run(scenario, func(t *testing.T) {
			effectLock.Lock()
			effects = nil
			effectLock.Unlock()
			contextID := scope.Tenant + "-" + scenario
			grant, e := ids.CreateRoot(ctx, scope, identity.RootRequest{Task: scenario, Context: contextID, Deployment: scope.Deployment, Capabilities: []string{"state"}, TTL: 10 * time.Minute})
			if e != nil {
				t.Fatal(e)
			}
			token, e := ids.MintTask(ctx, scope, grant.Task, signer, "ahp", 10*time.Minute)
			if e != nil {
				t.Fatal(e)
			}
			client, e := sdkstate.New(sdkstate.Config{BaseURL: server.URL, HTTPClient: httpClient, TokenSource: func(context.Context) (string, error) { return token, nil }})
			if e != nil {
				t.Fatal(e)
			}
			thread, e := client.CreateThread(ctx, contextID, scenario+"-thread")
			if e != nil {
				t.Fatal(e)
			}
			run, e := client.CreateRun(ctx, thread.ID, scenario+"-run", json.RawMessage(`{}`))
			if e != nil {
				t.Fatal(e)
			}
			claim, e := client.ClaimRunFor(ctx, "runtime-worker", 120000, run.ID)
			if e != nil || claim == nil {
				t.Fatal("claim", e)
			}
			hand := map[string]any{"base_url": server.URL, "ca_file": caFile, "cert_file": certFile, "key_file": keyFile, "task_token": claim.TaskToken, "thread_id": thread.ID, "fence": sdkstate.Fence{RunID: run.ID, Worker: "runtime-worker", Epoch: claim.Lease.Epoch}}
			raw, _ := json.Marshal(hand)
			file := write(scenario+".json", raw)

			if scenario == "standalone" {
				binary := filepath.Join(dir, "standalone-go")
				build := exec.CommandContext(ctx, "go", "build", "-o", binary, "../../sdk/examples/standalone-go")
				if out, e := build.CombinedOutput(); e != nil {
					t.Fatalf("build example: %v %s", e, out)
				}
				commands := [][]string{
					{binary, file, "write"},
					{python, "../../sdk/examples/standalone-python/state_client.py", file},
					{binary, file, "write"}, // exact retry in a fresh process
					{python, "../../sdk/examples/standalone-python/state_client.py", file},
					{binary, file, "verify"},
				}
				for _, args := range commands {
					command := exec.CommandContext(ctx, args[0], args[1:]...)
					command.Env = append(os.Environ(), "PYTHONPATH=../../sdk/python")
					output, e := command.CombinedOutput()
					if e != nil {
						t.Fatalf("standalone example: %v\n%s", e, output)
					}
					t.Log(string(output))
				}
				return
			}
			phases := []string{scenario}
			if scenario == "hitl" || scenario == "pending-restart" || (scenario == "effects-loop" || scenario == "effects-positional" || scenario == "effects-unknown") {
				phases = []string{scenario + "-write", scenario + "-read"}
			}
			for _, phase := range phases {
				checkpointCalls.Store(0)
				failAt.Store(0)
				if phase == "failure" {
					failAt.Store(3)
				}
				cmd := exec.CommandContext(ctx, python, "../../sdk/python/ahp_state/tests/conformance_live.py", file, phase)
				cmd.Env = append(os.Environ(), "PYTHONPATH=../../sdk/python")
				output, e := cmd.CombinedOutput()
				if e != nil {
					t.Fatalf("%s: %v\n%s", phase, e, output)
				}
				t.Log(string(output))
			}
			if strings.HasPrefix(scenario, "effects-") {
				effectLock.Lock()
				observed := append([]int(nil), effects...)
				effectLock.Unlock()
				if scenario == "effects-unknown" || strings.HasPrefix(scenario, "effects-legacy-") {
					if len(observed) != 1 || observed[0] != 1 {
						t.Fatalf("UNKNOWN was redispatched: %v", observed)
					}
				} else if len(observed) != 2 || observed[0] != 1 || observed[1] != 2 {
					t.Fatalf("physical effects=%v, want [1 2]", observed)
				}
			}
		})
	}
}
