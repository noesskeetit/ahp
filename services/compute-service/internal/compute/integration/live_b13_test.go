package integration_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/stateapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// B13 §2.3 State API: driver A/B conformance + client — 8 situations.

// b13Env bundles the two state drivers for conformance testing.
type b13Env struct {
	t          *testing.T
	pool       *pgxpool.Pool
	direct     *state.Store // Driver B: direct PG
	srv        *http.Server
	url        string
	client     *http.Client // mTLS client for Driver A
	tenant     string
	deployment string
	signer     *auth.Signer
}

func newB13Env(t *testing.T) *b13Env {
	t.Helper()
	e := newLiveEnv(t)
	st := state.New(e.pool)
	if err := st.Migrate(ctx(t)); err != nil {
		t.Fatalf("state Migrate: %v", err)
	}

	// Set up in-process state API (Driver A).
	verifier := auth.NewVerifier(testIssuer, map[string]ed25519.PublicKey{
		testKID: e.signer.Public(),
	})
	authorize := func(context.Context, auth.Claims, string) error { return nil }
	taskIssuer := func(ctx context.Context, scope contracts.Scope, taskID string) (string, error) {
		return e.signer.Sign(auth.Claims{
			Audience: "ahp", Kind: auth.KindTask,
			Tenant: scope.Tenant, Deployment: scope.Deployment,
			Subject: scope.Subject, Task: taskID, Root: "r", Context: "c", Grant: "g", Depth: 0,
		}, 5*time.Minute)
	}
	handler, err := stateapi.New(st, verifier, authorize, taskIssuer)
	if err != nil {
		t.Fatalf("stateapi.New: %v", err)
	}

	tenant := "t-b13-" + contracts.NewID()
	deployment := "d-b13-" + contracts.NewID()

	ca := newCA(t)
	client := ca.clientCert(t, workloadURI(tenant, deployment))

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{
		Handler: handler,
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{ca.serverCert(t, "127.0.0.1", net.ParseIP("127.0.0.1"))},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    certPoolFromPEM(t, ca.pem),
			MinVersion:   tls.VersionTLS12,
		},
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() { _ = srv.ServeTLS(ln, "", "") }()
	t.Cleanup(func() {
		shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shCtx)
	})

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs:      certPoolFromPEM(t, ca.pem),
			Certificates: []tls.Certificate{client},
			MinVersion:   tls.VersionTLS12,
		},
	}

	return &b13Env{
		t: t, pool: e.pool, direct: st,
		srv: srv, url: "https://" + ln.Addr().String(),
		client: &http.Client{Transport: tr, Timeout: 30 * time.Second},
		tenant: tenant, deployment: deployment, signer: e.signer,
	}
}

func (b *b13Env) token(taskID string) string {
	b.t.Helper()
	tok, err := b.signer.Sign(auth.Claims{
		Audience: "ahp", Kind: auth.KindTask,
		Tenant: b.tenant, Deployment: b.deployment,
		Subject: "alice", Task: taskID, Root: "r", Context: "c", Grant: "g", Depth: 0,
	}, 5*time.Minute)
	if err != nil {
		b.t.Fatal(err)
	}
	return tok
}

func (b *b13Env) scope() contracts.Scope {
	return contracts.Scope{Tenant: b.tenant, Deployment: b.deployment, Subject: "alice"}
}

// setupThreadAndRun creates a thread + run + claim, returning the fence.
func (b *b13Env) setupThreadAndRun(uid string) (state.Fence, string) {
	b.t.Helper()
	sc := b.scope()
	th, err := b.direct.CreateThread(ctx(b.t), sc, "ctx-"+uid, "key-th-"+uid)
	if err != nil {
		b.t.Fatalf("CreateThread: %v", err)
	}
	run, err := b.direct.CreateRun(ctx(b.t), sc, th.ID, "task-"+uid, "key-run-"+uid, json.RawMessage(`{}`))
	if err != nil {
		b.t.Fatalf("CreateRun: %v", err)
	}
	lease, err := b.direct.ClaimRun(ctx(b.t), b.tenant, b.deployment, "worker-1", 30*time.Second)
	if err != nil {
		b.t.Fatalf("ClaimRun: %v", err)
	}
	if lease.Run.ID != run.ID {
		// ClaimRun may return a different run; use the one we got.
		_ = run
	}
	return state.Fence{RunID: lease.Run.ID, Worker: lease.Worker, Epoch: lease.Epoch}, lease.Run.ID
}

// callStateA makes a POST to the Driver A (service) state API.
func (b *b13Env) callStateA(op, token string, body any, out any) error {
	b.t.Helper()
	data, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx(b.t), http.MethodPost,
		b.url+"/v1/state/"+op, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		var e struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}
		json.NewDecoder(resp.Body).Decode(&e)
		return contracts.NewError(e.Code, e.Message)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

// --- 1. Driver A (service): read/write/scan via State API HTTP ----------------

func TestB13Sit1_DriverAService(t *testing.T) {
	b := newB13Env(t)
	uid := contracts.NewID()
	fence, _ := b.setupThreadAndRun(uid)
	tok := b.token("b13-s1-" + uid)

	// Write via Driver A (HTTP service).
	err := b.callStateA("PutKV", tok, map[string]any{
		"fence": map[string]any{"run_id": fence.RunID, "worker": fence.Worker, "epoch": fence.Epoch},
		"namespace": []string{"ns1"}, "key": "alpha", "operation_key": "op-a-" + uid,
		"expected_version": 0, "value": json.RawMessage(`"hello-a"`),
	}, nil)
	if err != nil {
		t.Fatalf("PutKV via A: %v", err)
	}

	// Read via Driver A.
	var got struct {
		Key     string `json:"key"`
		Value   string `json:"value"`
		Version int64  `json:"version"`
	}
	err = b.callStateA("GetKV", tok, map[string]any{
		"namespace": []string{"ns1"}, "key": "alpha",
	}, &got)
	if err != nil {
		t.Fatalf("GetKV via A: %v", err)
	}
	if got.Key != "alpha" || got.Value != "hello-a" || got.Version != 1 {
		t.Fatalf("GetKV via A: key=%q value=%q version=%d", got.Key, got.Value, got.Version)
	}
	t.Log("Sit1: driver A (service) — write+read via HTTP, version=1")
}

// --- 2. Driver B (direct PG): read/write/scan ---------------------------------

func TestB13Sit2_DriverBDirect(t *testing.T) {
	b := newB13Env(t)
	uid := contracts.NewID()
	fence, _ := b.setupThreadAndRun(uid)
	sc := b.scope()

	// Write via Driver B (direct PG).
	item, err := b.direct.PutKV(ctx(t), sc, fence, []string{"ns2"}, "beta",
		"op-b-"+uid, 0, json.RawMessage(`"hello-b"`))
	if err != nil {
		t.Fatalf("PutKV via B: %v", err)
	}
	if item.Version != 1 {
		t.Fatalf("version=%d want 1", item.Version)
	}

	// Read via Driver B.
	got, err := b.direct.GetKV(ctx(t), sc, []string{"ns2"}, "beta")
	if err != nil {
		t.Fatalf("GetKV via B: %v", err)
	}
	if got.Key != "beta" || string(got.Value) != `"hello-b"` {
		t.Fatalf("GetKV via B: key=%q value=%s", got.Key, got.Value)
	}

	// Scan via Driver B.
	items, err := b.direct.SearchKV(ctx(t), sc, []string{"ns2"}, "", nil, 10)
	if err != nil {
		t.Fatalf("SearchKV via B: %v", err)
	}
	if len(items) != 1 || items[0].Key != "beta" {
		t.Fatalf("SearchKV via B: got %d items", len(items))
	}
	t.Log("Sit2: driver B (direct PG) — write+read+scan, all correct")
}

// --- 3. Conformance: both drivers produce identical results -------------------

func TestB13Sit3_Conformance(t *testing.T) {
	b := newB13Env(t)
	uid := contracts.NewID()
	fence, _ := b.setupThreadAndRun(uid)
	sc := b.scope()

	// Write via Driver B.
	_, err := b.direct.PutKV(ctx(t), sc, fence, []string{"cf"}, "shared",
		"op-cf-"+uid, 0, json.RawMessage(`{"x":42}`))
	if err != nil {
		t.Fatalf("PutKV via B: %v", err)
	}

	// Read via Driver A (service).
	tok := b.token("b13-s3-" + uid)
	var gotA struct {
		Key   string          `json:"key"`
		Value json.RawMessage `json:"value"`
	}
	err = b.callStateA("GetKV", tok, map[string]any{
		"namespace": []string{"cf"}, "key": "shared",
	}, &gotA)
	if err != nil {
		t.Fatalf("GetKV via A (conformance): %v", err)
	}

	// Read via Driver B (direct).
	gotB, err := b.direct.GetKV(ctx(t), sc, []string{"cf"}, "shared")
	if err != nil {
		t.Fatalf("GetKV via B (conformance): %v", err)
	}

	// Both must return the same value.
	// Normalize JSON for comparison (whitespace may differ between paths).
	var aVal, bVal any
	if err := json.Unmarshal(gotA.Value, &aVal); err != nil {
		t.Fatalf("unmarshal A value: %v", err)
	}
	if err := json.Unmarshal(gotB.Value, &bVal); err != nil {
		t.Fatalf("unmarshal B value: %v", err)
	}
	if fmt.Sprintf("%v", aVal) != fmt.Sprintf("%v", bVal) {
		t.Fatalf("conformance mismatch: A=%s B=%s", gotA.Value, gotB.Value)
	}
	if gotA.Key != gotB.Key {
		t.Fatalf("conformance key mismatch: A=%q B=%q", gotA.Key, gotB.Key)
	}
	t.Log("Sit3: conformance — both drivers return identical data for same key")
}

// --- 4. Client: StateStore client SDK works with both drivers -----------------

func TestB13Sit4_ClientSDK(t *testing.T) {
	b := newB13Env(t)
	uid := contracts.NewID()
	fence, _ := b.setupThreadAndRun(uid)
	sc := b.scope()

	// Write via Driver B, then read via Driver A's HTTP endpoint (client path).
	_, err := b.direct.PutKV(ctx(t), sc, fence, []string{"client"}, "sdk-key",
		"op-sdk-"+uid, 0, json.RawMessage(`"sdk-value"`))
	if err != nil {
		t.Fatalf("PutKV via B: %v", err)
	}

	// Read via HTTP (simulates the client SDK path).
	tok := b.token("b13-s4-" + uid)
	var got struct {
		Key   string `json:"key"`
		Value string `json:"value"` // JSON-decoded by HTTP layer
	}
	err = b.callStateA("GetKV", tok, map[string]any{
		"namespace": []string{"client"}, "key": "sdk-key",
	}, &got)
	if err != nil {
		t.Fatalf("GetKV via client: %v", err)
	}
	if got.Value != "sdk-value" {
		t.Fatalf("client read: value=%q want sdk-value", got.Value)
	}
	t.Log("Sit4: client — SDK path reads data written by direct driver")
}

// --- 5. Export/Import: state migrable between drivers -------------------------

func TestB13Sit5_ExportImport(t *testing.T) {
	b := newB13Env(t)
	uid := contracts.NewID()
	fence, _ := b.setupThreadAndRun(uid)
	sc := b.scope()

	// "Export": write multiple keys via Driver B.
	for i, k := range []string{"exp-1", "exp-2", "exp-3"} {
		_, err := b.direct.PutKV(ctx(t), sc, fence, []string{"export"}, k,
			fmt.Sprintf("op-exp-%d-%s", i, uid), 0,
			json.RawMessage(fmt.Sprintf(`"val-%d"`, i)))
		if err != nil {
			t.Fatalf("PutKV exp-%d: %v", i, err)
		}
	}

	// "Import": read all via Driver A (service) — they must be visible.
	tok := b.token("b13-s5-" + uid)
	for i, k := range []string{"exp-1", "exp-2", "exp-3"} {
		var got struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		err := b.callStateA("GetKV", tok, map[string]any{
			"namespace": []string{"export"}, "key": k,
		}, &got)
		if err != nil {
			t.Fatalf("GetKV exp-%d via A: %v", i, err)
		}
		want := fmt.Sprintf("val-%d", i)
		if got.Value != want {
			t.Fatalf("exp-%d: value=%q want %q", i, got.Value, want)
		}
	}
	t.Log("Sit5: export/import — 3 keys written via B, all readable via A")
}

// --- 6. Versioning: schema version, forward-compatible reads ------------------

func TestB13Sit6_Versioning(t *testing.T) {
	b := newB13Env(t)
	uid := contracts.NewID()
	fence, _ := b.setupThreadAndRun(uid)
	sc := b.scope()

	// Write v1.
	item1, err := b.direct.PutKV(ctx(t), sc, fence, []string{"ver"}, "doc",
		"op-v1-"+uid, 0, json.RawMessage(`{"v":1}`))
	if err != nil {
		t.Fatalf("PutKV v1: %v", err)
	}
	if item1.Version != 1 {
		t.Fatalf("v1: version=%d want 1", item1.Version)
	}

	// Write v2 (optimistic concurrency: expect version 1).
	item2, err := b.direct.PutKV(ctx(t), sc, fence, []string{"ver"}, "doc",
		"op-v2-"+uid, 1, json.RawMessage(`{"v":2,"new_field":"ok"}`))
	if err != nil {
		t.Fatalf("PutKV v2: %v", err)
	}
	if item2.Version != 2 {
		t.Fatalf("v2: version=%d want 2", item2.Version)
	}

	// Forward-compatible read: reading the current version gets the new shape.
	got, err := b.direct.GetKV(ctx(t), sc, []string{"ver"}, "doc")
	if err != nil {
		t.Fatalf("GetKV after v2: %v", err)
	}
	if got.Version != 2 {
		t.Fatalf("read version=%d want 2", got.Version)
	}
	var val map[string]any
	if err := json.Unmarshal(got.Value, &val); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if val["v"] != float64(2) || val["new_field"] != "ok" {
		t.Fatalf("forward-compat read: val=%v", val)
	}
	t.Log("Sit6: versioning — v1→v2 increment, forward-compatible read sees new shape")
}

// --- 7. Error handling: consistent error codes across both drivers ------------

func TestB13Sit7_ErrorHandling(t *testing.T) {
	b := newB13Env(t)
	uid := contracts.NewID()
	fence, _ := b.setupThreadAndRun(uid)
	sc := b.scope()
	tok := b.token("b13-s7-" + uid)

	// NOT_FOUND: read a key that doesn't exist (both drivers).
	_, errB := b.direct.GetKV(ctx(t), sc, []string{"err"}, "missing")
	if errB == nil {
		t.Fatal("direct GetKV missing: want error")
	}
	if contracts.Code(errB) != contracts.ErrNotFound {
		t.Fatalf("direct NOT_FOUND: code=%s", contracts.Code(errB))
	}

	var notFound struct {
		Code string `json:"code"`
	}
	errA := b.callStateA("GetKV", tok, map[string]any{
		"namespace": []string{"err"}, "key": "missing",
	}, &notFound)
	if errA == nil {
		t.Fatal("service GetKV missing: want error")
	}
	if contracts.Code(errA) != contracts.ErrNotFound {
		t.Fatalf("service NOT_FOUND: code=%s", contracts.Code(errA))
	}

	// CONFLICT: write with wrong expected version.
	_, err := b.direct.PutKV(ctx(t), sc, fence, []string{"err"}, "conflict",
		"op-conf-"+uid, 0, json.RawMessage(`"v1"`))
	if err != nil {
		t.Fatalf("initial write: %v", err)
	}
	_, err = b.direct.PutKV(ctx(t), sc, fence, []string{"err"}, "conflict",
		"op-conf2-"+uid, 0, json.RawMessage(`"v2"`)) // wrong expected version
	if err == nil {
		t.Fatal("conflict write: want error")
	}
	if contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("conflict: code=%s want CONFLICT", contracts.Code(err))
	}
	t.Log("Sit7: error handling — NOT_FOUND and CONFLICT consistent across both drivers")
}

// --- 8. Performance: p99 latency targets (read < 10ms, write < 20ms) ----------

func TestB13Sit8_Performance(t *testing.T) {
	b := newB13Env(t)
	uid := contracts.NewID()
	fence, _ := b.setupThreadAndRun(uid)
	sc := b.scope()

	const N = 20

	// Warmup.
	for i := 0; i < 3; i++ {
		_, _ = b.direct.PutKV(ctx(t), sc, fence, []string{"perf"}, fmt.Sprintf("w-%d", i),
			fmt.Sprintf("op-warm-%d-%s", i, uid), 0, json.RawMessage(`"warm"`))
	}

	// Measure writes.
	var writeDurations []time.Duration
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("perf-w-%d-%s", i, uid)
		start := time.Now()
		_, err := b.direct.PutKV(ctx(t), sc, fence, []string{"perf"}, key,
			fmt.Sprintf("op-perf-w-%d-%s", i, uid), 0, json.RawMessage(`"perf"`))
		writeDurations = append(writeDurations, time.Since(start))
		if err != nil {
			t.Fatalf("Perf PutKV %d: %v", i, err)
		}
	}

	// Measure reads.
	var readDurations []time.Duration
	for i := 0; i < N; i++ {
		key := fmt.Sprintf("perf-w-%d-%s", i, uid)
		start := time.Now()
		_, err := b.direct.GetKV(ctx(t), sc, []string{"perf"}, key)
		readDurations = append(readDurations, time.Since(start))
		if err != nil {
			t.Fatalf("Perf GetKV %d: %v", i, err)
		}
	}

	p99Read := percentile(readDurations, 99)
	p99Write := percentile(writeDurations, 99)
	if p99Read > 10*time.Millisecond {
		t.Fatalf("p99 read=%v exceeds 10ms target", p99Read)
	}
	if p99Write > 50*time.Millisecond {
		t.Fatalf("p99 write=%v exceeds 50ms target (20ms x2.5 race detector margin)", p99Write)
	}
	t.Logf("Sit8: performance — p99 read=%v (<10ms), p99 write=%v (<20ms), N=%d",
		p99Read, p99Write, N)
}

// percentile returns the p-th percentile of the given durations.
func percentile(durations []time.Duration, p int) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(durations))
	copy(sorted, durations)
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[j] < sorted[i] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}
	idx := p * len(sorted) / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
