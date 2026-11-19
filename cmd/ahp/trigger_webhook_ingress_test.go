package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/triggers"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

type webhookFixtureSecret struct{ owner contracts.Scope }

func (s webhookFixtureSecret) Resolve(_ context.Context, scope contracts.Scope, _ string) ([]byte, error) {
	if scope != s.owner {
		return nil, contracts.NewError(contracts.ErrForbidden, "wrong secret owner")
	}
	return []byte("fixture-webhook-secret"), nil
}

// Identity, PostgreSQL, real Ed25519 claims and ingress workload mTLS are used.
// The A2A destination is an explicitly recorded HTTP transport double: it proves
// the actual delivered message and credential, not a provider/LLM completion.
func TestTriggerWebhookIngressRealIdentityAndHTTP(t *testing.T) {
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("isolated PostgreSQL required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ids := identity.New(pool)
	if err := ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	suffix := randHexTest()
	scope := contracts.Scope{Tenant: "webhook-" + suffix, Deployment: "agent-runtime", Subject: "alice"}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner("webhook-test", "key", priv)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier("webhook-test", map[string]ed25519.PublicKey{"key": pub})
	conversation := "conversation-" + suffix
	_, err = ids.CreateRoot(ctx, scope, identity.RootRequest{Task: "live-parent", Context: conversation, Deployment: scope.Deployment, Capabilities: []string{"execution", "triggers"}, TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var delivered []json.RawMessage
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		claims, err := verifier.Verify(token, "ahp", auth.KindTask, auth.WorkloadID(scope))
		if err != nil || claims.Subject != scope.Subject || claims.Task != "live-parent" || claims.Context != conversation {
			t.Errorf("bad delegated credential: err=%v task=%s subject=%s", err, claims.Task, claims.Subject)
			http.Error(w, "forbidden", 403)
			return
		}
		if err := ids.Authorize(r.Context(), claims, "execution"); err != nil {
			t.Errorf("not live authority: %v", err)
			http.Error(w, "forbidden", 403)
			return
		}
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		delivered = append(delivered, body)
		mu.Unlock()
		if strings.Contains(string(body), "AMBIGUOUS-RESPONSE") {
			// The receiver accepted the request but the response connection
			// disappears: transport cannot prove whether the effect completed.
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = conn.Close()
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"task":{"id":"accepted-webhook-task"}}}`))
	}))
	defer destination.Close()
	dispatch, err := triggers.NewA2ADispatcher(triggers.A2AConfig{URL: destination.URL})
	if err != nil {
		t.Fatal(err)
	}
	authority := identityTriggerAuthority{store: ids, signer: signer, verifier: verifier}
	schema := "ahp_hook_" + suffix
	svc, err := triggers.New(ctx, pool, schema, triggers.Config{Issuer: authority, Dispatcher: dispatch, Secrets: webhookFixtureSecret{scope}, Tasks: authority})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = pool.Exec(context.Background(), `DROP SCHEMA "`+schema+`" CASCADE`) }()
	tr, err := svc.CreateWebhook(ctx, scope, triggers.CreateWebhookSpec{ParentTask: "live-parent", DelegationRef: "live-parent", Target: "agent:echo", SecretRef: "hook", MaxFires: 5, Delegated: true, Payload: json.RawMessage(`{"text":"Process this event"}`)})
	if err != nil {
		t.Fatal(err)
	}
	ingress, err := triggers.NewWebhookHandler(svc, []string{"public-edge"})
	if err != nil {
		t.Fatal(err)
	}
	crud, err := triggers.NewHTTPHandler(svc, verifier, ids.Authorize)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle(triggers.WebhookPathPrefix, ingress)
	mux.Handle("/v1/triggers/", crud)
	denied, err := triggers.NewWebhookHandler(svc, nil)
	if err != nil {
		t.Fatal(err)
	}
	mux.Handle("/deny/", denied)
	ca := newMountCA(t)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	front := httptest.NewUnstartedServer(mux)
	front.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	front.StartTLS()
	defer front.Close()
	client := func(tenant, deployment string) *http.Client {
		tr := front.Client().Transport.(*http.Transport).Clone()
		tr.TLSClientConfig.Certificates = []tls.Certificate{ca.clientCert(t, auth.WorkloadID(contracts.Scope{Tenant: tenant, Deployment: deployment}))}
		t.Cleanup(tr.CloseIdleConnections)
		return &http.Client{Transport: tr}
	}
	edge := client(scope.Tenant, "public-edge")
	post := func(hc *http.Client, path, id, signature string, body []byte) (int, map[string]any) {
		t.Helper()
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, front.URL+path, bytes.NewReader(body))
		req.Header.Set(triggers.WebhookMessageIDHeader, id)
		req.Header.Set(triggers.WebhookSignatureHeader, signature)
		req.Header.Set("X-Tenant", "forged")
		req.Header.Set("X-Subject", "mallory")
		resp, err := hc.Do(req)
		if err != nil {
			t.Error(err)
			return 0, nil
		}
		defer resp.Body.Close()
		var out map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}
	path := triggers.WebhookPathPrefix + tr.TriggerID
	body := []byte(`{"event":"REAL-DELIVERY-CONTENT","tenant":"forged","subject":"mallory"}`)
	sign := func(id string, b []byte) string {
		return triggers.SignWebhookDelivery([]byte("fixture-webhook-secret"), tr.TriggerID, id, b)
	}
	for _, tc := range []struct {
		name          string
		hc            *http.Client
		path, id, sig string
		body          []byte
		want          int
	}{
		{"bad_mac", edge, path, "same-id", "bad", body, 403},
		{"signature_binds_message_id", edge, path, "another-id", sign("same-id", body), body, 403},
		{"signature_binds_body", edge, path, "same-id", sign("same-id", body), []byte(`{"event":"tampered"}`), 403},
		{"empty_allowlist", edge, "/deny/" + tr.TriggerID, "same-id", sign("same-id", body), body, 403},
		{"missing_delivery_id", edge, path, "", sign("", body), body, 400},
		{"unexpected_suffix", edge, path + "/extra", "same-id", sign("same-id", body), body, 404},
		{"untrusted_workload", client(scope.Tenant, "agent-runtime"), path, "same-id", sign("same-id", body), body, 403},
		{"foreign_tenant", client("other-tenant", "public-edge"), path, "same-id", sign("same-id", body), body, 404},
		{"oversized", edge, path, "same-id", "bad", bytes.Repeat([]byte("x"), triggers.WebhookMaxBody+1), 413},
		{"unknown_trigger", edge, triggers.WebhookPathPrefix + "missing", "same-id", sign("same-id", body), body, 404},
		{"no_anonymous_crud", edge, "/v1/triggers/CreateWebhook", "same-id", sign("same-id", body), body, 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, out := post(tc.hc, tc.path, tc.id, tc.sig, tc.body)
			if code != tc.want {
				t.Fatalf("code=%d want=%d body=%v", code, tc.want, out)
			}
		})
	}
	var operations, admissions int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM "`+schema+`".operations`).Scan(&operations); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM "`+schema+`".webhook_deliveries`).Scan(&admissions); err != nil {
		t.Fatal(err)
	}
	if operations != 0 || admissions != 0 {
		t.Fatalf("rejected requests consumed state: operations=%d admissions=%d", operations, admissions)
	}
	recorder := httptest.NewRecorder()
	plain := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	plain.Header.Set("X-Workload-Identity", auth.WorkloadID(contracts.Scope{Tenant: scope.Tenant, Deployment: "public-edge"}))
	ingress.ServeHTTP(recorder, plain)
	if recorder.Code != 403 {
		t.Fatalf("unsigned workload headers accepted: %d", recorder.Code)
	}
	code, receipt := post(edge, path, "same-id", sign("same-id", body), body)
	if code != 200 || receipt["status"] != "SUCCEEDED" {
		t.Fatalf("valid delivery=%d %v", code, receipt)
	}
	if _, ok := receipt["scope"]; ok {
		t.Fatal("receipt exposes owner scope")
	}
	code, replay := post(edge, path, "same-id", sign("same-id", body), body)
	if code != 200 || replay["operation_key"] != receipt["operation_key"] {
		t.Fatalf("replay=%d %v", code, replay)
	}
	changed := []byte(`{"event":"different"}`)
	code, out := post(edge, path, "same-id", sign("same-id", changed), changed)
	if code != 400 || out["reason"] != contracts.ErrKeyReuse {
		t.Fatalf("body mismatch=%d %v", code, out)
	}
	mu.Lock()
	calls := append([]json.RawMessage(nil), delivered...)
	mu.Unlock()
	if len(calls) != 1 || !strings.Contains(string(calls[0]), "REAL-DELIVERY-CONTENT") || !strings.Contains(string(calls[0]), "Process this event") {
		t.Fatalf("A2A did not receive configuration and original event: %s", calls)
	}

	create := func(max int64) triggers.Trigger {
		t.Helper()
		tr, err := svc.CreateWebhook(ctx, scope, triggers.CreateWebhookSpec{ParentTask: "live-parent", DelegationRef: "live-parent", Target: "agent:echo", SecretRef: "hook", MaxFires: max, Delegated: true})
		if err != nil {
			t.Fatal(err)
		}
		return tr
	}
	send := func(tr triggers.Trigger, id string, body []byte) (int, map[string]any) {
		return post(edge, triggers.WebhookPathPrefix+tr.TriggerID, id, triggers.SignWebhookDelivery([]byte("fixture-webhook-secret"), tr.TriggerID, id, body), body)
	}
	count := func() int { mu.Lock(); defer mu.Unlock(); return len(delivered) }
	t.Run("binary_body_is_lossless", func(t *testing.T) {
		tr := create(1)
		binary := []byte{0, 255, 1, 10, 254}
		before := count()
		code, out := send(tr, "binary", binary)
		if code != 200 || out["status"] != "SUCCEEDED" {
			t.Fatalf("binary delivery=%d %v", code, out)
		}
		mu.Lock()
		request := append([]byte(nil), delivered[before]...)
		mu.Unlock()
		var wire struct {
			Params struct {
				Message struct {
					Parts []struct {
						Text string `json:"text"`
					} `json:"parts"`
				} `json:"message"`
			} `json:"params"`
		}
		if err := json.Unmarshal(request, &wire); err != nil {
			t.Fatal(err)
		}
		if len(wire.Params.Message.Parts) != 1 {
			t.Fatalf("parts=%v", wire.Params.Message.Parts)
		}
		var payload struct {
			Webhook struct {
				BodyBase64 string `json:"body_base64"`
				MessageID  string `json:"message_id"`
			} `json:"webhook"`
		}
		if err := json.Unmarshal([]byte(wire.Params.Message.Parts[0].Text), &payload); err != nil {
			t.Fatal(err)
		}
		got, err := base64.StdEncoding.DecodeString(payload.Webhook.BodyBase64)
		if err != nil || !bytes.Equal(got, binary) || payload.Webhook.MessageID != "binary" {
			t.Fatalf("body corrupted: %x %v", got, err)
		}
	})
	t.Run("concurrent_retries_and_max_fires", func(t *testing.T) {
		tr := create(3)
		before := count()
		for _, same := range []bool{true, false} {
			start := make(chan struct{})
			codes := make(chan int, 12)
			var wg sync.WaitGroup
			for i := 0; i < 12; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					id := "same"
					if !same {
						id = fmt.Sprintf("different-%d", i)
					}
					code, _ := send(tr, id, []byte(`{"event":"concurrent"}`))
					codes <- code
				}(i)
			}
			close(start)
			wg.Wait()
			close(codes)
			success := 0
			for code := range codes {
				if code == 200 {
					success++
				} else if same || code != 403 {
					t.Errorf("unexpected concurrent receipt=%d same=%v", code, same)
				}
			}
			want := 2
			if same {
				want = 12
			}
			if success != want {
				t.Fatalf("accepted HTTP receipts=%d want=%d", success, want)
			}
		}
		if got := count() - before; got != 3 {
			t.Fatalf("MaxFires=3 delivered %d HTTP requests", got)
		}
		stored, err := svc.Get(ctx, scope, tr.TriggerID)
		if err != nil || stored.FiredCount != 3 || stored.Status != triggers.StatusExhausted {
			t.Fatalf("durable bound=%+v err=%v", stored, err)
		}
	})
	unknown := create(1)
	unknownBody := []byte(`{"event":"AMBIGUOUS-RESPONSE"}`)
	beforeUnknown := count()
	code, out = send(unknown, "lost-response", unknownBody)
	if code != 500 {
		t.Fatalf("lost response must not claim success: %d %v", code, out)
	}
	if count() != beforeUnknown+1 {
		t.Fatal("ambiguous request did not reach HTTP receiver")
	}

	// Shut down the HTTP ingress and reconnect PostgreSQL with a fresh Service;
	// the retry's receipt must come from durable state, without A2A replay.
	front.Close()
	restartedPool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer restartedPool.Close()
	restarted, err := triggers.New(ctx, restartedPool, schema, triggers.Config{Issuer: authority, Dispatcher: dispatch, Secrets: webhookFixtureSecret{scope}, Tasks: authority})
	if err != nil {
		t.Fatal(err)
	}
	restartedIngress, err := triggers.NewWebhookHandler(restarted, []string{"public-edge"})
	if err != nil {
		t.Fatal(err)
	}
	front = httptest.NewUnstartedServer(restartedIngress)
	front.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	front.StartTLS()
	defer front.Close()
	edge = client(scope.Tenant, "public-edge")
	code, out = send(unknown, "lost-response", unknownBody)
	if code != 200 || out["status"] != "SIDE_EFFECT_UNKNOWN" || count() != beforeUnknown+1 {
		t.Fatalf("UNKNOWN retried after restart: code=%d receipt=%v calls=%d", code, out, count()-beforeUnknown)
	}
	code, out = send(unknown, "new-after-unknown", unknownBody)
	if code != 403 || count() != beforeUnknown+1 {
		t.Fatalf("UNKNOWN returned fire slot: %d %v", code, out)
	}
	code, out = send(tr, "same-id", body)
	if code != 200 || out["operation_key"] != receipt["operation_key"] || count() != beforeUnknown+1 {
		t.Fatalf("completed replay after restart: %d %v", code, out)
	}

	t.Run("explicit_revoke", func(t *testing.T) {
		tr := create(2)
		before := count()
		if _, err := svc.Revoke(ctx, scope, tr.TriggerID); err != nil {
			t.Fatal(err)
		}
		code, out := send(tr, "new", body)
		if code != 403 || count() != before {
			t.Fatalf("revoked trigger reached receiver: %d %v", code, out)
		}
	})
	beforeRevocation := count()
	if err := ids.CancelSubtree(ctx, scope, "live-parent"); err != nil {
		t.Fatal(err)
	}
	code, out = post(edge, path, "after-revoke", sign("after-revoke", body), body)
	if code != 403 {
		t.Fatalf("revoked delegation delivered: %d %v", code, out)
	}
	mu.Lock()
	n := len(delivered)
	mu.Unlock()
	if n != beforeRevocation {
		t.Fatalf("revoked authority dispatched again: before=%d after=%d", beforeRevocation, n)
	}
}
