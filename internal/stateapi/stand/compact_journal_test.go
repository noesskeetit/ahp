package stand

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/statejournal"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestCompactJournalLostHTTPReply(t *testing.T) {
	dsn := os.Getenv("AHP_JOURNAL_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires isolated PostgreSQL")
	}
	if !strings.HasPrefix(databaseName(dsn), "ahp_carbon_compact") {
		t.Fatal("use an isolated compact-journal database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := statejournal.New(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner(issuer, keyID, private)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier(issuer, map[string]ed25519.PublicKey{keyID: public})
	handler, err := statejournal.NewHTTP(store, verifier, func(_ context.Context, _ auth.Claims, capability string) error {
		if capability == "state.shared" {
			return contracts.NewError(contracts.ErrForbidden, "shared state is not granted in this test")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	const base = "/v1/state/sessions/journal-test"
	tenant := "test-" + rand.Text()
	deployment := "langgraph"
	cert, key, certPEM := caPair()
	cas := x509.NewCertPool()
	if !cas.AppendCertsFromPEM(certPEM) {
		t.Fatal("test CA was rejected")
	}
	serverCert, _, _ := leaf(cert, key, "localhost", true, nil)
	clientCert, _, _ := leaf(cert, key, "worker", false, []string{workloadURI(tenant, deployment)})
	var lost atomic.Bool
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		eligible := r.URL.Path == base+"/events:append" || r.URL.Path == base+"/checkpoints"
		if r.Method == http.MethodPost && eligible && r.Header.Get("X-Test-Lose-Reply") == "yes" {
			// Execute the real HTTP handler first. A successful recorded reply
			// means the transaction committed. Drop the TCP response afterward.
			reply := httptest.NewRecorder()
			handler.ServeHTTP(reply, r)
			if reply.Code != http.StatusOK {
				t.Errorf("discarded request did not commit: %d %s", reply.Code, reply.Body.String())
			}
			connection, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("drop connection: %v", err)
				return
			}
			lost.Store(true)
			_ = connection.Close()
			return
		}
		handler.ServeHTTP(w, r)
	}))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{serverCert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: cas, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	t.Cleanup(server.Close)
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: cas, Certificates: []tls.Certificate{clientCert}, MinVersion: tls.VersionTLS12},
	}}
	t.Cleanup(client.CloseIdleConnections)

	mint := func(task, tokenTenant, subject string) string {
		t.Helper()
		token, err := signer.Sign(auth.Claims{Audience: audience, Kind: auth.KindTask,
			Tenant: tokenTenant, Deployment: deployment, Subject: subject,
			Task: task, Root: "root", Context: "test", Grant: "grant"}, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	worker := mint("worker-a", tenant, "subject")
	replacement := mint("worker-b", tenant, "subject")
	send := func(method, path, token, body string, drop bool) (int, []byte, error) {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, method, server.URL+path, bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		if drop {
			req.Header.Set("X-Test-Lose-Reply", "yes")
		}
		response, err := client.Do(req)
		if err != nil {
			return 0, nil, err
		}
		defer response.Body.Close()
		payload, err := io.ReadAll(response.Body)
		return response.StatusCode, payload, err
	}
	expect := func(method, path, token, body string, want int) []byte {
		t.Helper()
		code, payload, err := send(method, path, token, body, false)
		if err != nil || code != want {
			t.Fatalf("%s %s: want %d, got %d %s (err %v)", method, path, want, code, payload, err)
		}
		return payload
	}

	acquire := `{"session_id":"journal-test","harness_type":"langgraph","format_version":1}`
	payload := expect(http.MethodPost, "/v1/state/sessions:acquire", worker, acquire, http.StatusOK)
	var lease statejournal.Lease
	if err := json.Unmarshal(payload, &lease); err != nil || lease.Epoch != 1 || lease.Worker != "worker-a" {
		t.Fatalf("initial lease: %+v (%v)", lease, err)
	}

	shared := `{"lease_epoch":1,"key":"shared","expected_seq":0,"events":[{"id":"before","type":"message","role":"assistant","codec":"text","payload":{"inline":"b2s="}}],"kv":[{"scope_type":"user","namespace":"adk","key":"profile","expected_version":0,"payload":{"inline":"b2s="}}]}`
	expect(http.MethodPost, base+"/events:append", worker, shared, http.StatusForbidden)
	expect(http.MethodGet, base+"/events?state_namespace=adk", worker, "", http.StatusForbidden)
	expect(http.MethodGet, "/v1/state/aux:mutate?state_namespace=adk", worker, "", http.StatusForbidden)

	appendEvent := `{"lease_epoch":1,"key":"lost-1","expected_seq":0,"events":[{"id":"n1","type":"message","role":"assistant","codec":"text","payload":{"inline":"b2s="}}]}`
	if _, _, err := send(http.MethodPost, base+"/events:append", worker, appendEvent, true); err == nil || !lost.Load() {
		t.Fatalf("server accepted the append but its reply must be lost, got %v", err)
	}
	payload = expect(http.MethodPost, base+"/events:append", worker, appendEvent, http.StatusOK)
	var receipt statejournal.AppendReceipt
	if err := json.Unmarshal(payload, &receipt); err != nil || receipt.FirstSeq != 1 || receipt.HeadSeq != 1 {
		t.Fatalf("idempotent replay receipt: %+v (%v)", receipt, err)
	}
	expect(http.MethodPost, base+"/events:append", replacement, appendEvent, http.StatusConflict)
	expect(http.MethodPost, base+"/events:append", worker,
		strings.Replace(appendEvent, "b2s=", "bm8=", 1), http.StatusBadRequest)

	writes := `{"lease_epoch":1,"namespace":"","writes":[{"task_id":"a","task_path":"pregel/a","index":-3,"channel":"__interrupt__","codec":"msgpack","payload":{"inline":"/w=="}}]}`
	expect(http.MethodPost, base+"/checkpoints/cp1/writes", worker, writes, http.StatusOK)
	checkpoint := `{"lease_epoch":1,"key":"snapshot1","checkpoint":{"namespace":"","id":"cp1","snapshot_seq":1,"metadata":{},"channel_versions":{},"codec":"msgpack","payload":{"inline":"gA=="}}}`
	lost.Store(false)
	if _, _, err := send(http.MethodPost, base+"/checkpoints", worker, checkpoint, true); err == nil || !lost.Load() {
		t.Fatalf("server accepted the checkpoint but its reply must be lost, got %v", err)
	}
	payload = expect(http.MethodPost, base+"/checkpoints", worker, checkpoint, http.StatusOK)
	var checkpointReceipt struct {
		ID string `json:"checkpoint_id"`
	}
	if err := json.Unmarshal(payload, &checkpointReceipt); err != nil || checkpointReceipt.ID != "cp1" {
		t.Fatalf("checkpoint replay receipt: %+v (%v)", checkpointReceipt, err)
	}
	expect(http.MethodPost, base+":heartbeat", worker, `{"lease_epoch":1,"release":true}`, http.StatusOK)

	payload = expect(http.MethodPost, "/v1/state/sessions:acquire", replacement, acquire, http.StatusOK)
	if err := json.Unmarshal(payload, &lease); err != nil || lease.Epoch != 2 || lease.Worker != "worker-b" {
		t.Fatalf("takeover lease: %+v (%v)", lease, err)
	}
	expect(http.MethodPost, base+"/events:append", worker, appendEvent, http.StatusConflict)
	payload = expect(http.MethodGet, base+"/events", replacement, "", http.StatusOK)
	var page statejournal.EventPage
	if err := json.Unmarshal(payload, &page); err != nil || len(page.Events) != 1 || page.HeadSeq != 1 {
		t.Fatalf("journal duplicated the lost reply: %+v (%v)", page, err)
	}
	payload = expect(http.MethodGet, base+"/checkpoints", replacement, "", http.StatusOK)
	var tuple statejournal.CheckpointTuple
	if err := json.Unmarshal(payload, &tuple); err != nil || tuple.Checkpoint.ID != "cp1" ||
		len(tuple.Writes) != 1 || tuple.Writes[0].Index != -3 || tuple.Writes[0].TaskPath != "pregel/a" {
		t.Fatalf("cold checkpoint lost pending writes: %+v (%v)", tuple, err)
	}
	expect(http.MethodGet, base+"/events", mint("other", tenant, "other-subject"), "", http.StatusNotFound)
	expect(http.MethodGet, base+"/events", mint("other", "foreign", "subject"), "", http.StatusForbidden)
	payload = expect(http.MethodGet, base+"/checkpoints?list=true", replacement, "", http.StatusOK)
	if strings.Count(string(payload), `"cp1"`) != 1 {
		t.Fatalf("checkpoint replay inserted a second row: %s", payload)
	}
}
