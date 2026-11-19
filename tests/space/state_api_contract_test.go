//go:build state_integration

// Package space_test drives the production Space StateClient (internal/space)
// against a REAL, freshly booted State API (internal/stateapi over real
// PostgreSQL with real mTLS + Ed25519 task tokens). It proves the Space<->State
// HTTP wire contract on the accepted server, not an in-process mock.
package space_test

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
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/stateapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

const (
	cAudience = "ahp"
	cIssuer   = "https://identity.test"
	cKID      = "k1"
	cSubject  = "user-ada"
)

// ---- test-only mTLS CA (copied shape from the accepted State fixtures) ----

type cCA struct {
	cert *x509.Certificate
	pem  []byte
	key  *ecdsa.PrivateKey
}

func cNewCA(t *testing.T) *cCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ahp-contract-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &cCA{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

func (ca *cCA) clientCert(t *testing.T, uris ...string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	parsed := make([]*url.URL, 0, len(uris))
	for _, u := range uris {
		p, err := url.Parse(u)
		if err != nil {
			t.Fatalf("bad uri %q: %v", u, err)
		}
		parsed = append(parsed, p)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "ahp-contract-client"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		URIs:        parsed,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

func cWorkloadURI(tenant, deployment string) string {
	return "spiffe://ahp/tenant/" + tenant + "/deployment/" + deployment
}

type cEnv struct {
	srv        *httptest.Server
	ctx        context.Context
	tenant     string
	deployment string
	ca         *cCA
	signer     *auth.Signer
	verifier   *auth.Verifier
	client     *space.StateClient
}

func cNewEnv(t *testing.T) *cEnv {
	return cNewEnvProfile(t, false)
}

// commonNative uses the actual common schema with a context-aligned fixture
// issuer for native A2A tests. Other contract fixtures retain their old profile.
func cNewEnvProfile(t *testing.T, commonNative bool) *cEnv {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		t.Skip("set AHP_TEST_DATABASE_URL or DATABASE_URL to run state contract integration tests")
	}
	// Generous ceiling: the real State over PostgreSQL is a SHARED acceptance resource, so a
	// transient saturation by another contractor must surface as slow, not as a spurious
	// "context deadline exceeded" mid-CreateRun. Focused runs finish in seconds.
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	t.Cleanup(cancel)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	store := state.New(pool)
	if commonNative {
		state.WithCommonRecords()(store)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner(cIssuer, cKID, priv)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier(cIssuer, map[string]ed25519.PublicKey{cKID: pub})

	authorize := func(context.Context, auth.Claims, string) error { return nil }
	issue := func(_ context.Context, sc contracts.Scope, taskID string) (string, error) {
		contextID := "ctx-" + taskID
		if commonNative {
			contextID = "ctx-1"
		}
		return signer.Sign(auth.Claims{
			Audience: cAudience, Kind: auth.KindTask,
			Tenant: sc.Tenant, Deployment: sc.Deployment,
			Subject: sc.Subject, Task: taskID, Root: "root-" + taskID,
			Context: contextID, Grant: "grant-" + taskID, Depth: 1,
		}, 5*time.Minute)
	}

	tenant := "t-" + contracts.NewID()
	deployment := "d-" + contracts.NewID()
	handler, err := stateapi.NewWithAdmin(store, verifier, authorize, issue, stateapi.AdminConfig{
		Audience: "ahp-admin",
		Authorize: func(_ context.Context, c auth.Claims, capability string) error {
			if c.Subject != "state-admin" || c.Tenant != tenant || c.Deployment != "control-"+deployment || capability != stateapi.AdminCapability {
				return contracts.NewError(contracts.ErrForbidden, "admin permission required")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("stateapi.New: %v", err)
	}

	ca := cNewCA(t)
	client := ca.clientCert(t, cWorkloadURI(tenant, deployment))

	serverCAs := x509.NewCertPool()
	if !serverCAs.AppendCertsFromPEM(ca.pem) {
		t.Fatal("CA PEM rejected")
	}
	srv := httptest.NewUnstartedServer(handler)
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: serverCAs}
	srv.StartTLS()
	tr := srv.Client().Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.RootCAs = srv.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
	tr.TLSClientConfig.Certificates = []tls.Certificate{client}
	srv.Client().Transport = tr
	t.Cleanup(srv.Close)

	return &cEnv{srv: srv, ctx: ctx, tenant: tenant, deployment: deployment,
		ca: ca, signer: signer, verifier: verifier,
		client: space.NewStateClient(srv.URL, srv.Client())}
}

func (e *cEnv) signTask(t *testing.T, task string) string {
	t.Helper()
	tok, err := e.signer.Sign(auth.Claims{
		Audience: cAudience, Kind: auth.KindTask,
		Tenant: e.tenant, Deployment: e.deployment,
		Subject: cSubject, Task: task, Root: "run-1",
		Context: "ctx-1", Grant: "grant-1", Depth: 0,
	}, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// raw posts arbitrary bytes to /v1/state/{op} with the env's mTLS client and an
// optional bearer token, returning status and decoded body.
func (e *cEnv) raw(op, token string, body []byte) (int, map[string]any) {
	req, err := http.NewRequest(http.MethodPost, e.srv.URL+"/v1/state/"+op, bytes.NewReader(body))
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := e.srv.Client().Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// ---- happy-path full lifecycle over the REAL server ----

func TestStateAPIContract_Lifecycle(t *testing.T) {
	e := cNewEnv(t)
	ctx := e.ctx
	human := e.signTask(t, "task-"+contracts.NewID())

	// CreateThread + CreateRun (subject-scoped via the human/task token).
	th, err := e.client.CreateThread(ctx, human, contracts.Scope{}, "ctx-"+contracts.NewID(), "thread-key")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	if th.ID == "" || th.Epoch != 0 {
		t.Fatalf("CreateThread bad resp: %+v", th)
	}
	payload := json.RawMessage(`{"type":"turn","input":"hello"}`)
	run, err := e.client.CreateRun(ctx, human, th.ID, "run-key", payload, "")
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if run.ID == "" || run.Status != state.RunPending {
		t.Fatalf("CreateRun bad resp: %+v", run)
	}

	// ClaimRun is workload/cert-only: no bearer token.
	claim, err := e.client.ClaimRun(ctx, "", "w1", 60*time.Second)
	if err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	if claim.Lease == nil || claim.TaskToken == "" {
		t.Fatalf("ClaimRun no lease/token: %+v", claim)
	}
	fence := space.Fence{RunID: claim.Lease.Run.ID, Worker: "w1", Epoch: claim.Lease.Epoch}
	tok := claim.TaskToken

	// The issued task token must verify and carry the run's scope.
	claims, err := e.verifier.Verify(tok, "ahp", "task", cWorkloadURI(e.tenant, e.deployment))
	if err != nil {
		t.Fatalf("verify issued task token: %v", err)
	}
	if claims.Tenant != e.tenant || claims.Deployment != e.deployment {
		t.Fatalf("issued token wrong scope: %+v", claims)
	}

	// Heartbeat (flat run_id/worker/epoch).
	if err := e.client.Heartbeat(ctx, tok, fence.RunID, fence.Worker, fence.Epoch, 60*time.Second); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	// Intents: record then advance.
	in, err := e.client.RecordIntent(ctx, tok, fence.RunID, fence.Worker, fence.Epoch, "node-1", "call-1", "tool", "echo", json.RawMessage(`{"a":1}`))
	if err != nil {
		t.Fatalf("RecordIntent: %v", err)
	}
	if in.ID == "" {
		t.Fatalf("RecordIntent no id: %+v", in)
	}
	if in.Status != state.IntentRecorded {
		t.Fatalf("RecordIntent status = %q want %q", in.Status, state.IntentRecorded)
	}
	// Monotonic lifecycle: recorded -> dispatched -> succeeded (a direct
	// recorded -> succeeded is rejected as an illegal transition).
	if _, err := e.client.UpdateIntent(ctx, tok, fence.RunID, fence.Worker, fence.Epoch, "node-1", "call-1", state.IntentDispatched, nil); err != nil {
		t.Fatalf("UpdateIntent dispatched: %v", err)
	}
	done, err := e.client.UpdateIntent(ctx, tok, fence.RunID, fence.Worker, fence.Epoch, "node-1", "call-1", state.IntentSucceeded, json.RawMessage(`{"ok":true}`))
	if err != nil {
		t.Fatalf("UpdateIntent succeeded: %v", err)
	}
	if done.Status != state.IntentSucceeded {
		t.Fatalf("UpdateIntent final status = %q want %q", done.Status, state.IntentSucceeded)
	}

	// Checkpoints: write (nested fence + checkpoint + key), then list.
	meta := json.RawMessage(`{"source":"contract"}`)
	parts := []space.Part{{Channel: "messages", Version: "v1", Data: []byte(`[{"role":"user"}]`)}}
	cpID := "cp-" + contracts.NewID()
	cp, err := e.client.PutCheckpoint(ctx, tok, fence, "space", cpID, "", "ckpt-key", 1, meta, parts)
	if err != nil {
		t.Fatalf("PutCheckpoint: %v", err)
	}
	if cp.ID != cpID {
		t.Fatalf("PutCheckpoint id mismatch: got %q want %q", cp.ID, cpID)
	}
	cps, err := e.client.ListCheckpoints(ctx, tok, th.ID, "space", 5)
	if err != nil {
		t.Fatalf("ListCheckpoints: %v", err)
	}
	if len(cps) != 1 || cps[0].ID != cpID {
		t.Fatalf("ListCheckpoints bad: %+v", cps)
	}

	// KV: fenced write then read.
	ns := []string{"space", "contract"}
	kv, err := e.client.PutKV(ctx, tok, fence, ns, "k1", "op1", 0, json.RawMessage(`{"v":"x"}`))
	if err != nil {
		t.Fatalf("PutKV: %v", err)
	}
	if kv.Version < 1 {
		t.Fatalf("PutKV version: %+v", kv)
	}
	got, err := e.client.GetKV(ctx, tok, ns, "k1")
	if err != nil {
		t.Fatalf("GetKV: %v", err)
	}
	if string(got.Value) != `{"v":"x"}` {
		t.Fatalf("GetKV value: %s", got.Value)
	}

	// Inbox: enqueue, read, ack.
	ev, err := e.client.Enqueue(ctx, tok, th.ID, "steer-1", "steering", json.RawMessage(`{"msg":"steer"}`))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	events, err := e.client.ReadInbox(ctx, tok, th.ID, 0, 10)
	if err != nil {
		t.Fatalf("ReadInbox: %v", err)
	}
	if len(events) != 1 || events[0].ID != ev.ID {
		t.Fatalf("ReadInbox bad: %+v", events)
	}
	if err := e.client.AckInbox(ctx, tok, fence.RunID, fence.Worker, fence.Epoch, []string{ev.ID}); err != nil {
		t.Fatalf("AckInbox: %v", err)
	}

	// Stream: append then read (thread derived from lease, not body).
	if _, err := e.client.AppendStream(ctx, tok, th.ID, fence.RunID, fence.Worker, fence.Epoch, "out-1", "token", json.RawMessage(`{"t":"hi"}`)); err != nil {
		t.Fatalf("AppendStream: %v", err)
	}
	stream, err := e.client.ReadStream(ctx, tok, th.ID, 0, 10)
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	if len(stream) != 1 {
		t.Fatalf("ReadStream bad: %+v", stream)
	}

	// Suspend with a PAST deadline requeues the run; a fresh worker then claims
	// it at a higher epoch (FIFO + recovery over the real fence).
	past := time.Now().Add(-time.Minute).UTC().Truncate(time.Microsecond)
	if err := e.client.Suspend(ctx, tok, fence.RunID, fence.Worker, fence.Epoch, "hitl:approval", &past); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	claim2, err := e.client.ClaimRun(ctx, "", "w2", 60*time.Second)
	if err != nil {
		t.Fatalf("ClaimRun w2: %v", err)
	}
	if claim2.Lease == nil || claim2.Lease.Epoch <= fence.Epoch {
		t.Fatalf("expected higher epoch after requeue: %+v", claim2.Lease)
	}
	if err := e.client.Release(ctx, claim2.TaskToken, claim2.Lease.Run.ID, "w2", claim2.Lease.Epoch, state.RunDone); err != nil {
		t.Fatalf("Release w2: %v", err)
	}
	finalRun, err := e.client.GetRun(ctx, claim2.TaskToken, claim2.Lease.Run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if finalRun.Status != state.RunDone {
		t.Fatalf("final run status = %q want done", finalRun.Status)
	}
}

// ---- security / wire-strictness probes against the REAL server ----

func TestStateAPIContract_Security(t *testing.T) {
	e := cNewEnv(t)
	ctx := e.ctx
	human := e.signTask(t, "task-"+contracts.NewID())
	th, err := e.client.CreateThread(ctx, human, contracts.Scope{}, "ctx-"+contracts.NewID(), "sec-thread")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	run, err := e.client.CreateRun(ctx, human, th.ID, "sec-run", json.RawMessage(`{"type":"turn","x":1}`), "")
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if run.ThreadID != th.ID || run.Status != state.RunPending {
		t.Fatalf("sec CreateRun bad binding: %+v", run)
	}
	claim, err := e.client.ClaimRun(ctx, "", "w1", 60*time.Second)
	if err != nil || claim.Lease == nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	fence := space.Fence{RunID: claim.Lease.Run.ID, Worker: "w1", Epoch: claim.Lease.Epoch}
	tok := claim.TaskToken

	// Unknown field in body -> 400 INVALID_ARGUMENT (strict reader; proves my
	// client must send EXACTLY the declared fields, which it now does).
	if st, body := e.raw("CreateThread", tok, []byte(`{"context_id":"x","key":"y","tenant":"nope"}`)); st != http.StatusBadRequest || body["code"] != contracts.ErrInvalid {
		t.Fatalf("unknown-field body: status %d body %v", st, body)
	}

	// Missing bearer on a task route -> 403.
	if st, _ := e.raw("GetThread", "", []byte(`{"thread_id":"`+th.ID+`"}`)); st != http.StatusForbidden {
		t.Fatalf("missing bearer: status %d", st)
	}

	// Stale epoch on a fenced write -> conflict error surfaces through the client.
	badFence := space.Fence{RunID: fence.RunID, Worker: fence.Worker, Epoch: fence.Epoch + 999}
	if _, err := e.client.PutCheckpoint(ctx, tok, badFence, "space", "cp-bad", "", "k", 1, nil, nil); err == nil {
		t.Fatal("stale-epoch PutCheckpoint unexpectedly accepted")
	} else if !strings.Contains(err.Error(), "STALE_EPOCH") && !strings.Contains(err.Error(), "LEASE_LOST") {
		t.Fatalf("stale-epoch error unexpected: %v", err)
	}

	// Foreign workload (different tenant/deployment) cannot read this thread:
	// scope isolation reports NOT_FOUND, never a probeable forbidden.
	foreignTenant := "t-foreign-" + contracts.NewID()
	foreignDeploy := "d-foreign-" + contracts.NewID()
	// Re-sign our verifier-issued foreign token on the SAME signer (identity is
	// the signer; workload comes from the cert). Use a foreign cert + foreign token.
	foreignTok, err := e.signer.Sign(auth.Claims{
		Audience: cAudience, Kind: auth.KindTask,
		Tenant: foreignTenant, Deployment: foreignDeploy,
		Subject: "user-bob", Task: "task-f", Root: "run-f", Context: "ctx-f",
		Grant: "grant-f", Depth: 0,
	}, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	// Foreign cert on the SAME CA (minted via the captured env CA).
	foreignClient := e.foreignClient(t, cWorkloadURI(foreignTenant, foreignDeploy))
	fc := space.NewStateClient(e.srv.URL, foreignClient)
	if _, err := fc.GetThread(ctx, foreignTok, th.ID); err == nil {
		t.Fatal("foreign workload unexpectedly read the thread")
	} else if !strings.Contains(err.Error(), "NOT_FOUND") {
		t.Fatalf("foreign GetThread want NOT_FOUND, got %v", err)
	}

	// Foreign token presented with the OWNER cert -> identity mismatch -> 403.
	if st, _ := e.raw("GetThread", foreignTok, []byte(`{"thread_id":"`+th.ID+`"}`)); st != http.StatusForbidden {
		t.Fatalf("foreign token on owner cert: status %d want 403", st)
	}
}

// foreignClient clones the server's client transport (already trusting the env
// CA and the test server cert) and swaps in a client cert minted by the SAME
// env CA but bearing a different workload SPIFFE URI: a foreign tenant/
// deployment that the server still authenticates at the TLS layer yet must not
// authorize for this scope.
func (e *cEnv) foreignClient(t *testing.T, uri string) *http.Client {
	t.Helper()
	cert := e.ca.clientCert(t, uri)
	tr := e.srv.Client().Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{cert}
	return &http.Client{Transport: tr}
}

// adminRaw crosses the real human-admin route with a separate mTLS workload.
func (e *cEnv) adminRaw(t *testing.T, op, subject string, input any) (int, map[string]any) {
	t.Helper()
	token, err := e.signer.Sign(auth.Claims{Audience: "ahp-admin", Kind: auth.KindHuman, Tenant: e.tenant, Subject: "state-admin"}, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(map[string]any{"target": map[string]string{"deployment": e.deployment, "subject": subject}, "input": input})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(e.ctx, http.MethodPost, e.srv.URL+"/v1/state-admin/"+op, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	tr := e.srv.Client().Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{e.ca.clientCert(t, cWorkloadURI(e.tenant, "control-"+e.deployment))}
	defer tr.CloseIdleConnections()
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, out
}
