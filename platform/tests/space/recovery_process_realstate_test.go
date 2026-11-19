//go:build state_integration

package space_test

// These tests kill real OS processes: the shipped Python/LangGraph entry point
// and a State/Identity/Coordinator HTTP plane. The business-effect HTTP server is
// a deliberately non-idempotent fixture, not a model or a claim of LLM E2E.

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/stateapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

type recoveryPlaneConfig struct {
	DSN, Address, SigningKey, CA, ServerCert, ServerKey, ClientCert, ClientKey string
}

// TestRecoveryPlaneProcess is only a subprocess entry. Its transport, authority
// callbacks, State and Coordinator are production implementations, with real PG.
func TestRecoveryPlaneProcess(t *testing.T) {
	name := os.Getenv("AHP_RECOVERY_PLANE_CONFIG")
	if name == "" {
		return
	}
	var c recoveryPlaneConfig
	recoveryReadJSON(t, name, &c)
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, c.DSN)
	if err != nil {
		t.Fatal("recovery pool configuration failed")
	}
	defer pool.Close()
	st, ids := state.New(pool), identity.New(pool)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	key, err := base64.StdEncoding.DecodeString(c.SigningKey)
	if err != nil {
		t.Fatal("invalid isolated signing key")
	}
	signer, err := auth.NewSigner(cIssuer, cKID, ed25519.PrivateKey(key))
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier(cIssuer, map[string]ed25519.PublicKey{cKID: signer.Public()})
	h, err := stateapi.New(st, verifier, ids.Authorize,
		func(ctx context.Context, sc contracts.Scope, task string) (string, error) {
			return ids.MintTask(ctx, sc, task, signer, "ahp", 30*time.Second)
		})
	if err != nil {
		t.Fatal(err)
	}
	client := recoveryHTTPClient(t, c)
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: space.NewStateClient("https://"+c.Address, client), WorkerID: "recovery-plane",
	})
	mux := http.NewServeMux()
	mux.Handle("/v1/state/", h)
	// This is a service-authenticated coordinator face, not a browser login
	// fixture. It tests the same durable HITL handlers under real task authority.
	mux.Handle("/v1/space/", space.NewHandler(space.HandlerConfig{
		Coordinator: coord, Auth: transport.Task(verifier, "ahp", "space", ids.Authorize),
	}))
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := pool.Ping(r.Context()); err != nil {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
	})
	caPEM, err := os.ReadFile(c.CA)
	if err != nil {
		t.Fatal(err)
	}
	ca := x509.NewCertPool()
	if !ca.AppendCertsFromPEM(caPEM) {
		t.Fatal("invalid recovery CA")
	}
	srv := &http.Server{Addr: c.Address, Handler: mux,
		TLSConfig:         &tls.Config{MinVersion: tls.VersionTLS12, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: ca},
		ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServeTLS(c.ServerCert, c.ServerKey); err != nil {
		t.Fatal(err)
	}
}

type recoveryChild struct {
	cmd  *exec.Cmd
	log  *os.File
	once sync.Once
}

func recoveryStart(t *testing.T, command, cwd, logPath string, args []string, env []string) *recoveryChild {
	t.Helper()
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(command, args...)
	cmd.Dir, cmd.Env, cmd.Stdout, cmd.Stderr = cwd, env, f, f
	if err := cmd.Start(); err != nil {
		f.Close()
		t.Fatal(err)
	}
	p := &recoveryChild{cmd: cmd, log: f}
	t.Cleanup(p.kill)
	return p
}

func (p *recoveryChild) kill() {
	p.once.Do(func() { _ = p.cmd.Process.Kill(); _ = p.cmd.Wait(); _ = p.log.Close() })
}

type recoveryEnv struct {
	t                                    *testing.T
	ctx                                  context.Context
	pool                                 *pgxpool.Pool
	ids                                  *identity.Store
	signer                               *auth.Signer
	scope                                contracts.Scope
	cfg                                  recoveryPlaneConfig
	configPath, dir, source, python, url string
	client                               *http.Client
	plane                                *recoveryChild
	generation                           int
}

func recoveryNew(t *testing.T) *recoveryEnv {
	t.Helper()
	python := os.Getenv("AHP_RECOVERY_PYTHON")
	if python == "" {
		t.Skip("set AHP_RECOVERY_PYTHON to the installed sdk/python interpreter")
	}
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("AHP_TEST_DATABASE_URL is mandatory for process recovery")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	t.Cleanup(cancel)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal("recovery pool configuration failed")
	}
	t.Cleanup(pool.Close)
	ids := identity.New(pool)
	if err := ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS public.recovery_effect_receipts
		(id text PRIMARY KEY, experiment text NOT NULL, request jsonb NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner(cIssuer, cKID, private)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if evidence := os.Getenv("AHP_RECOVERY_EVIDENCE_DIR"); evidence != "" {
		if err := os.MkdirAll(evidence, 0700); err != nil {
			t.Fatal(err)
		}
		kept, err := os.MkdirTemp(evidence, strings.ReplaceAll(t.Name(), "/", "-")+"-")
		if err != nil {
			t.Fatal(err)
		}
		dir = kept
	}
	scope := contracts.Scope{Tenant: "recovery-" + contracts.NewID(), Deployment: "sdk", Subject: "alice"}
	ca := cNewCA(t)
	clientCert := ca.clientCert(t, cWorkloadURI(scope.Tenant, scope.Deployment))
	clientKey, err := x509.MarshalPKCS8PrivateKey(clientCert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serverTemplate := &x509.Certificate{SerialNumber: big.NewInt(3), Subject: pkix.Name{CommonName: "recovery"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, ca.cert, &serverKey.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	serverPKCS8, err := x509.MarshalPKCS8PrivateKey(serverKey)
	if err != nil {
		t.Fatal(err)
	}
	write := func(name string, data []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, data, 0600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := ln.Addr().String()
	_ = ln.Close()
	cfg := recoveryPlaneConfig{DSN: dsn, Address: address, SigningKey: base64.StdEncoding.EncodeToString(private),
		CA:         write("ca.pem", ca.pem),
		ClientCert: write("client.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientCert.Certificate[0]})),
		ClientKey:  write("client.key", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: clientKey})),
		ServerCert: write("server.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER})),
		ServerKey:  write("server.key", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: serverPKCS8}))}
	data, _ := json.Marshal(cfg)
	configPath := write("plane.json", data)
	source, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	e := &recoveryEnv{t: t, ctx: ctx, pool: pool, ids: ids, signer: signer, scope: scope,
		cfg: cfg, configPath: configPath, dir: dir, source: source, python: python, url: "https://" + address,
		client: recoveryHTTPClient(t, cfg)}
	e.restart()
	return e
}

func recoveryHTTPClient(t *testing.T, c recoveryPlaneConfig) *http.Client {
	t.Helper()
	ca, err := os.ReadFile(c.CA)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		t.Fatal("invalid isolated CA")
	}
	cert, err := tls.LoadX509KeyPair(c.ClientCert, c.ClientKey)
	if err != nil {
		t.Fatal(err)
	}
	tr := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, Certificates: []tls.Certificate{cert}}}
	t.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: tr, Timeout: 10 * time.Second}
}

func recoveryReadJSON(t *testing.T, name string, out any) {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, out); err != nil {
		t.Fatal("invalid private recovery configuration")
	}
}

func recoveryEnvironment(updates map[string]string) []string {
	out := []string{}
	for _, pair := range os.Environ() {
		key, _, _ := strings.Cut(pair, "=")
		if _, exists := updates[key]; !exists {
			out = append(out, pair)
		}
	}
	for key, value := range updates {
		out = append(out, key+"="+value)
	}
	return out
}

func (e *recoveryEnv) restart() {
	if e.plane != nil {
		e.plane.kill()
	}
	e.generation++
	exe, err := os.Executable()
	if err != nil {
		e.t.Fatal(err)
	}
	e.plane = recoveryStart(e.t, exe, e.source, filepath.Join(e.dir, fmt.Sprintf("plane-%d.log", e.generation)),
		[]string{"-test.run=^TestRecoveryPlaneProcess$", "-test.timeout=150s"},
		recoveryEnvironment(map[string]string{"AHP_RECOVERY_PLANE_CONFIG": e.configPath}))
	e.wait("isolated plane readiness", func() bool {
		r, err := e.client.Get(e.url + "/readyz")
		if err != nil {
			return false
		}
		defer r.Body.Close()
		return r.StatusCode == 200
	})
}

func (e *recoveryEnv) wait(description string, ready func() bool) {
	e.t.Helper()
	deadline := time.Now().Add(35 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	e.t.Fatalf("timeout: %s; private subprocess logs under %s", description, e.dir)
}

func (e *recoveryEnv) token(subject, task string, ttl time.Duration, create bool) string {
	e.t.Helper()
	scope := e.scope
	scope.Subject = subject
	if create {
		_, err := e.ids.CreateRoot(e.ctx, scope, identity.RootRequest{Task: task, Context: task, Deployment: scope.Deployment,
			Capabilities: []string{"state", "space"}, TTL: 2 * time.Minute})
		if err != nil {
			e.t.Fatal(err)
		}
	}
	token, err := e.ids.MintTask(e.ctx, scope, task, e.signer, "ahp", ttl)
	if err != nil {
		e.t.Fatal(err)
	}
	return token
}

func (e *recoveryEnv) post(path, token string, body any, want int) map[string]any {
	e.t.Helper()
	b, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(e.ctx, "POST", e.url+path, bytes.NewReader(b))
	if err != nil {
		e.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	r, err := e.client.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer r.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(r.Body).Decode(&out); err != nil {
		e.t.Fatal(err)
	}
	if r.StatusCode != want {
		e.t.Fatalf("%s returned HTTP %d, expected %d; code=%v reason=%v", path, r.StatusCode, want, out["code"], out["reason"])
	}
	return out
}

func (e *recoveryEnv) state(op, token string, body any) map[string]any {
	return e.post("/v1/state/"+op, token, body, 200)
}

func (e *recoveryEnv) queue(token, key string) (string, string) {
	accepted := e.post("/v1/space/AcceptTurn", token, map[string]any{
		"context_id": key, "key": key, "input": map[string]any{"request": map[string]any{"amount": 7}},
	}, 200)
	return accepted["thread_id"].(string), accepted["turn_id"].(string)
}

func (e *recoveryEnv) worker(token, name, world string) *recoveryChild {
	handshake := map[string]any{"base_url": e.url, "ca_file": e.cfg.CA, "cert_file": e.cfg.ClientCert,
		"key_file": e.cfg.ClientKey, "task_token": token}
	b, _ := json.Marshal(handshake)
	p := filepath.Join(e.dir, name+"-handshake.json")
	if err := os.WriteFile(p, b, 0600); err != nil {
		e.t.Fatal(err)
	}
	return recoveryStart(e.t, e.python, filepath.Join(e.source, "sdk/python"), filepath.Join(e.dir, name+".log"),
		[]string{"-m", "ahp_state.container"}, recoveryEnvironment(map[string]string{
			"PYTHONPATH": filepath.Join(e.source, "sdk/python") + string(os.PathListSeparator) +
				filepath.Join(e.source, "sdk/python/examples/billing_agent") + string(os.PathListSeparator) +
				filepath.Join(e.source, "tests/space/fixtures"),
			"AHP_AGENT_MODULE": "agent", "AHP_STATE_HANDSHAKE": p, "AHP_WORKER_ID": name,
			"AHP_LEASE_TTL_MS": "1800", "AHP_HEARTBEAT_MS": "350", "AHP_IDLE_WAIT_S": "0.05", "AHP_MAX_TURNS": "0",
			"AHP_EFFECT_TRANSPORT": "recovery_transport:charge", "AHP_EFFECT_NODE": "charge", "AHP_RECOVERY_WORLD_URL": world,
			"HTTP_PROXY": "", "HTTPS_PROXY": "", "ALL_PROXY": "", "NO_PROXY": "127.0.0.1,localhost"}))
}

func (e *recoveryEnv) parked(token, thread, run string) string {
	e.wait("durable HITL gate", func() bool {
		r := e.state("GetRun", token, map[string]any{"run_id": run})
		return r["status"] == "waiting" && r["wait_for"] == "hitl_response"
	})
	inbox := e.state("ReadInbox", token, map[string]any{"thread_id": thread, "after": 0, "limit": 100})
	for _, item := range inbox["events"].([]any) {
		ev := item.(map[string]any)
		if key, ok := ev["key"].(string); ok && strings.HasPrefix(key, "hitl:") {
			return strings.TrimPrefix(key, "hitl:")
		}
	}
	e.t.Fatal("waiting run has no durable HITL request")
	return ""
}

func (e *recoveryEnv) answer(token, thread, gate string, want int) {
	e.post("/v1/space/RespondHITL", token, map[string]any{"thread_id": thread,
		"hitl_id": gate, "approved": true, "response": "approved"}, want)
}

func (e *recoveryEnv) world(dropACK bool) (string, func() int, func() string) {
	experiment := contracts.NewID()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/tools/Call" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		b, err := io.ReadAll(io.LimitReader(r.Body, 1024))
		var call struct {
			Key       string          `json:"key"`
			Tool      string          `json:"tool"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if err != nil || json.Unmarshal(b, &call) != nil || call.Key == "" || call.Tool != "billing.charge" || len(call.Arguments) == 0 {
			w.WriteHeader(400)
			return
		}
		id := contracts.NewID()
		if _, err := e.pool.Exec(e.ctx, `INSERT INTO public.recovery_effect_receipts(id,experiment,request) VALUES($1,$2,$3::jsonb)`, id, experiment, string(b)); err != nil {
			w.WriteHeader(500)
			return
		}
		if dropACK {
			<-r.Context().Done()
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"receipt": id})
	}))
	cert, err := tls.LoadX509KeyPair(e.cfg.ServerCert, e.cfg.ServerKey)
	if err != nil {
		e.t.Fatal(err)
	}
	ca, err := os.ReadFile(e.cfg.CA)
	if err != nil {
		e.t.Fatal(err)
	}
	clients := x509.NewCertPool()
	if !clients.AppendCertsFromPEM(ca) {
		e.t.Fatal("invalid recovery fixture CA")
	}
	srv.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert},
		ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clients}
	srv.StartTLS()
	e.t.Cleanup(srv.Close)
	count := func() int {
		var n int
		if err := e.pool.QueryRow(e.ctx, `SELECT count(*) FROM public.recovery_effect_receipts WHERE experiment=$1`, experiment).Scan(&n); err != nil {
			e.t.Fatal(err)
		}
		return n
	}
	receipt := func() string {
		var id string
		if err := e.pool.QueryRow(e.ctx, `SELECT id FROM public.recovery_effect_receipts WHERE experiment=$1`, experiment).Scan(&id); err != nil {
			e.t.Fatal(err)
		}
		return id
	}
	return srv.URL, count, receipt
}

func TestSDKProcessRecovery_LostEffectACKAndPlaneRestart(t *testing.T) {
	e := recoveryNew(t)
	task := "lost-ack-" + contracts.NewID()
	token := e.token("alice", task, 90*time.Second, true)
	thread, run := e.queue(token, task)
	world, count, receipt := e.world(true)
	first := e.worker(token, "lost-ack-first", world)
	gate := e.parked(token, thread, run)
	e.answer(token, thread, gate, 200)
	e.wait("external effect committed without ACK", func() bool { return count() == 1 })
	before := e.state("GetThread", token, map[string]any{"thread_id": thread})
	first.kill()
	e.restart() // loses every in-process State/Identity/Coordinator object
	second := e.worker(token, "lost-ack-second", world)
	e.wait("fresh worker reports unknown instead of replaying effect", func() bool {
		r := e.state("GetRun", token, map[string]any{"run_id": run})
		return r["status"] == "waiting" && r["wait_for"] == "reconcile:effect-0"
	})
	second.kill()
	cold := e.post("/v1/space/GetTurn", token, map[string]any{"turn_id": run}, 200)
	if cold["status"] != "waiting" {
		t.Fatalf("cold coordinator misclassified unknown wait: %v", cold["status"])
	}
	after := e.state("GetThread", token, map[string]any{"thread_id": thread})
	if after["epoch"].(float64) <= before["epoch"].(float64) {
		t.Fatal("recovery did not acquire a fresh epoch")
	}
	// Reconciliation must be possible from the public durable event, without
	// guessing a logical node name or inspecting the owner's database. LangGraph
	// identities are task IDs, so even the same logical node can have many keys.
	stream := e.state("ReadStream", token, map[string]any{"thread_id": thread, "after": 0, "limit": 100})
	var intentNode string
	var intentDigest string
	for _, row := range stream["events"].([]any) {
		event := row.(map[string]any)
		if event["kind"] != "agent_awaiting_reconciliation" {
			continue
		}
		payload := event["payload"].(map[string]any)
		if payload["run_id"] == run && payload["call"] == "effect-0" {
			intentNode, _ = payload["node"].(string)
			intentDigest, _ = payload["digest"].(string)
		}
	}
	if intentNode == "" || intentNode == "charge" {
		t.Fatal("reconciliation event must identify the durable LangGraph task, run and call")
	}
	intent := e.state("GetIntent", token, map[string]any{"run_id": run, "node": intentNode, "call": "effect-0"})
	if intent["status"] != "unknown" {
		t.Fatalf("ambiguous effect status=%v, expected unknown", intent["status"])
	}
	if intentDigest == "" || intent["digest"] != intentDigest {
		t.Fatal("reconciliation event fingerprint does not identify the stored intent")
	}
	if count() != 1 {
		t.Fatalf("external effect executed %d times", count())
	}
	e.post("/v1/state/UpdateIntent", token, map[string]any{
		"fence": map[string]any{"run_id": run, "worker": "lost-ack-first", "epoch": before["epoch"]},
		"node":  intentNode, "call": "effect-0", "status": "succeeded", "result": map[string]any{"receipt": "zombie"},
	}, 409)
	other := e.token("bob", "other-"+contracts.NewID(), time.Minute, true)
	e.post("/v1/state/GetIntent", other, map[string]any{"run_id": run, "node": intentNode, "call": "effect-0"}, 404)
	e.post("/v1/space/GetTurn", other, map[string]any{"turn_id": run}, 404)
	// Explicit reconciliation is the only route out of uncertainty. Its evidence
	// is the external fixture's committed receipt, not an assumed successful retry.
	e.state("Enqueue", token, map[string]any{"thread_id": thread, "key": "reconcile-wake", "kind": "reconcile:effect-0", "payload": map[string]any{}})
	claim := e.state("ClaimRun", "", map[string]any{"worker": "reconciler", "ttl_ms": 10000})
	lease := claim["lease"].(map[string]any)
	fence := map[string]any{"run_id": run, "worker": "reconciler", "epoch": lease["epoch"]}
	fresh := claim["task_token"].(string)
	e.state("ReconcileIntent", fresh, map[string]any{"fence": fence, "node": intentNode, "call": "effect-0",
		"status": "succeeded", "digest": intent["digest"], "result": map[string]any{"receipt": receipt()}})
	e.state("Release", fresh, map[string]any{"run_id": run, "worker": "reconciler", "epoch": lease["epoch"], "status": "pending"})
	third := e.worker(token, "lost-ack-third", world)
	e.wait("reconciled graph completes", func() bool { return e.state("GetRun", token, map[string]any{"run_id": run})["status"] == "done" })
	third.kill()
	e.restart()
	completed := e.post("/v1/space/GetTurn", token, map[string]any{"turn_id": run}, 200)
	if completed["status"] != "done" {
		t.Fatalf("cold coordinator did not recover completion: %v", completed["status"])
	}
	if count() != 1 {
		t.Fatalf("reconciled replay executed external effect %d times", count())
	}
	t.Log("real SDK+plane SIGKILL/restart: higher epoch, unknown parked, owner denial, explicit reconciliation, effects=1")
}

func TestSDKProcessRecovery_HITLTokenExpiryAndRevocation(t *testing.T) {
	for _, revoke := range []bool{false, true} {
		t.Run(fmt.Sprintf("revoke_%t", revoke), func(t *testing.T) {
			e := recoveryNew(t)
			task := "hitl-" + contracts.NewID()
			token := e.token("alice", task, time.Minute, true)
			thread, run := e.queue(token, task)
			world, count, _ := e.world(false)
			first := e.worker(token, "hitl-first", world)
			gate := e.parked(token, thread, run)
			first.kill()
			short := e.token("alice", task, time.Second, false)
			e.restart()
			cold := e.post("/v1/space/GetTurn", token, map[string]any{"turn_id": run}, 200)
			if cold["status"] != "awaiting_hitl" {
				t.Fatalf("cold coordinator lost HITL state: %v", cold["status"])
			}
			time.Sleep(1100 * time.Millisecond)
			e.answer(short, thread, gate, 403)
			other := e.token("bob", "foreign-"+contracts.NewID(), time.Minute, true)
			e.answer(other, thread, gate, 404)
			fresh := e.token("alice", task, time.Minute, false)
			if revoke {
				if err := e.ids.CancelSubtree(e.ctx, e.scope, task); err != nil {
					t.Fatal(err)
				}
				e.answer(fresh, thread, gate, 403)
				if _, err := e.ids.MintTask(e.ctx, e.scope, task, e.signer, "ahp", time.Minute); err == nil {
					t.Fatal("revoked grant renewed")
				}
				if count() != 0 {
					t.Fatal("revoked HITL executed an effect")
				}
				t.Log("HITL survives plane restart; expired credential/foreign owner/revoked grant denied; effects=0")
				return
			}
			e.answer(fresh, thread, gate, 200)
			second := e.worker(fresh, "hitl-second", world)
			e.wait("HITL fresh credential resumes same run", func() bool { return e.state("GetRun", fresh, map[string]any{"run_id": run})["status"] == "done" })
			second.kill()
			if count() != 1 {
				t.Fatalf("HITL resume executed %d effects", count())
			}
			t.Log("HITL survives plane restart and JWT expiry; fresh same-grant credential resumes, effects=1")
		})
	}
}
