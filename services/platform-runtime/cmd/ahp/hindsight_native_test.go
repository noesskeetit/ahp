//go:build ahp_data

package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	memsdk "github.com/ahp-platform/ahp/sdk/go/memory"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestHindsightNativeAgentAuthority is opt-in because it performs native LLM
// extraction. It uses the production mount, real PG/Identity, and a TLS socket;
// no protocol fixture or pre-existing personal Hindsight bank is involved.
func TestHindsightNativeAgentAuthority(t *testing.T) {
	origin := os.Getenv("AHP_TEST_HINDSIGHT_URL")
	if origin == "" {
		t.Skip("dedicated AHP-owned native Hindsight is not configured")
	}
	u, err := url.Parse(origin)
	if err != nil || u.Port() == "8889" || os.Getenv("AHP_TEST_HINDSIGHT_OWNED") != "1" {
		t.Fatal("an explicitly AHP-owned Hindsight deployment is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("real isolated PostgreSQL is required")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal("invalid test database configuration")
	}
	defer pool.Close()
	ids := identity.New(pool)
	if err := ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	st := state.New(pool)
	if err := migrateStateAll(ctx, st); err != nil {
		t.Fatal(err)
	}
	scope := contracts.Scope{Tenant: fmt.Sprintf("native-memory-%d", time.Now().UnixNano()), Subject: "alice", Deployment: "agent"}
	contextID := scope.Tenant + ":context"
	_, err = ids.CreateRoot(ctx, scope, identity.RootRequest{Task: "memory-task", Context: contextID, Deployment: scope.Deployment, Capabilities: []string{"state", "memory", "memory:review", "memory:delete"}, TTL: 10 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner("memory-native", "test", key)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier("memory-native", map[string]ed25519.PublicKey{"test": pub})
	token, err := ids.MintTask(ctx, scope, "memory-task", signer, "ahp", 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	thread, err := st.CreateThread(ctx, scope, contextID, "memory-thread")
	if err != nil {
		t.Fatal(err)
	}
	const fact = "Alice prefers all technical explanations in Russian and requests concise answers with exact test evidence."
	payload, _ := json.Marshal(map[string]any{"input": fact})
	run, err := st.CreateRun(ctx, scope, thread.ID, "memory-task", "memory-run", payload)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("AHP_MEMORY_HINDSIGHT_URL", origin)
	t.Setenv("AHP_MEMORY_BACKEND", "hindsight")
	s := &server{pool: pool, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if _, _, err := s.buildDataHandler(ctx); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	if err := s.tryMountMemory(ctx, mux, verifier, ids.Authorize, authorityIdentityMode, identityGrantLabels{evaluate: ids}); err != nil {
		t.Fatal(err)
	}
	ca := newTestCA(t)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	srv := httptest.NewUnstartedServer(mux)
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots, MinVersion: tls.VersionTLS12}
	srv.StartTLS()
	defer srv.Close()
	hc := srv.Client()
	tr := hc.Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{ca.clientCert(t, auth.WorkloadID(scope))}
	hc.Transport = tr
	client, err := memsdk.New(srv.URL, func(context.Context) (string, error) { return token, nil }, hc)
	if err != nil {
		t.Fatal(err)
	}
	issue := func(other contracts.Scope, task string, authority bool) string {
		t.Helper()
		otherContext := contextID
		if other != scope {
			otherContext = other.Tenant + ":" + other.Subject + ":context"
		}
		_, e := ids.CreateRoot(ctx, other, identity.RootRequest{Task: task, Context: otherContext, Deployment: other.Deployment, Capabilities: []string{"memory", "memory:review", "memory:delete"}, TTL: 10 * time.Minute})
		if e != nil {
			t.Fatal(e)
		}
		var value string
		if authority {
			value, e = ids.MintAuthorityTask(ctx, other, task, signer, "ahp", 5*time.Minute)
		} else {
			value, e = ids.MintTask(ctx, other, task, signer, "ahp", 5*time.Minute)
		}
		if e != nil {
			t.Fatal(e)
		}
		return value
	}
	withToken := func(value string, transport *http.Client) *memsdk.Client {
		c, e := memsdk.New(srv.URL, func(context.Context) (string, error) { return value, nil }, transport)
		if e != nil {
			t.Fatal(e)
		}
		return c
	}
	denial := func(e error, status int) {
		t.Helper()
		var api *memsdk.Error
		if !errors.As(e, &api) || api.Status != status || api.Code == "invalid_response" {
			t.Fatalf("expected platform denial %d, got %v", status, e)
		}
	}
	otherSubject := scope
	otherSubject.Subject = "bob"
	otherSubjectToken := issue(otherSubject, "bob-task", false)
	otherTenant := scope
	otherTenant.Tenant += "-other"
	otherTenantToken := issue(otherTenant, "tenant-task", false)
	otherHTTP := *hc
	otherTransport := tr.Clone()
	otherCert := ca.clientCert(t, auth.WorkloadID(otherTenant))
	otherTransport.TLSClientConfig.Certificates = []tls.Certificate{otherCert}
	otherHTTP.Transport = otherTransport
	humanToken, e := signer.Sign(auth.Claims{Audience: "ahp", Kind: auth.KindHuman, Tenant: scope.Tenant, Subject: scope.Subject, JTI: "memory-human"}, time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	_, e = withToken(humanToken, hc).Settings(ctx, "")
	denial(e, 403)
	_, e = withToken(otherTenantToken, hc).Settings(ctx, "")
	denial(e, 403)
	_, e = client.Search(ctx, memsdk.SearchRequest{ProjectID: "foreign-workspace", Scope: "project", Query: "facts"})
	denial(e, 403)
	_, e = client.Management().UpdateSettings(ctx, "agent-settings", memsdk.SettingsRequest{})
	denial(e, 403)
	_, e = client.Management().Clear(ctx, "agent-clear", memsdk.ClearRequest{Scope: memsdk.Personal})
	denial(e, 403)
	req := memsdk.RememberRequest{Target: memsdk.Personal, Text: fact, SourceRefs: []string{"state://runs/" + run.ID + "/input"}}
	op, err := client.Remember(ctx, "native-remember", req)
	if err != nil {
		t.Fatal(err)
	}
	again, err := client.Remember(ctx, "native-remember", req)
	if err != nil || op.ID == "" || again.ID != op.ID {
		t.Fatalf("idempotency: operation=%+v repeated=%+v error=%v", op, again, err)
	}
	t.Logf("native operation=%s initial_state=%s tenant=%s", op.ID, op.State, scope.Tenant)
	for _, foreign := range []*memsdk.Client{withToken(otherSubjectToken, hc), withToken(otherTenantToken, &otherHTTP)} {
		_, e = foreign.Operation(ctx, op.ID, memsdk.Personal, "")
		denial(e, 404)
		items, e := foreign.Search(ctx, memsdk.SearchRequest{Scope: "personal", Query: "Alice"})
		if e != nil || len(items.Facts) != 0 {
			t.Fatalf("foreign owner leaked facts: count=%d error=%v", len(items.Facts), e)
		}
	}
	changed := req
	changed.Text += " Changed payload."
	_, e = client.Remember(ctx, "native-remember", changed)
	denial(e, 409)
	for op.State != "succeeded" {
		select {
		case <-ctx.Done():
			t.Fatalf("native extraction did not complete: last_state=%s error=%s", op.State, op.Error)
		case <-time.After(time.Second):
		}
		op, err = client.Operation(ctx, op.ID, memsdk.Personal, "")
		if err != nil {
			t.Fatal(err)
		}
		if op.State == "failed" || op.State == "cancelled" {
			t.Fatalf("native extraction: %+v", op)
		}
	}
	result, err := client.Search(ctx, memsdk.SearchRequest{Scope: "personal", Query: "Which language and answer format does Alice prefer?"})
	if err != nil || len(result.Facts) == 0 || len(result.Degraded) != 0 {
		t.Fatalf("native recall facts=%d degraded=%v err=%v", len(result.Facts), result.Degraded, err)
	}
	found := false
	for _, item := range result.Facts {
		if strings.Contains(strings.ToLower(item.Text), "russian") || strings.Contains(strings.ToLower(item.Text), "рус") {
			found = true
		}
	}
	if !found {
		t.Fatal("native recall did not recover the stated language preference")
	}
	t.Logf("native completed operation=%s recalled_facts=%d", op.ID, len(result.Facts))
	managementToken := issue(scope, "management-task", true)
	management := withToken(managementToken, hc).Management()
	disabled := false
	if _, e = management.UpdateSettings(ctx, "native-disable", memsdk.SettingsRequest{PersonalEnabled: &disabled}); e != nil {
		t.Fatal(e)
	}
	_, e = client.Search(ctx, memsdk.SearchRequest{Scope: "personal", Query: "Alice"})
	denial(e, 409)
	enabled := true
	if _, e = management.UpdateSettings(ctx, "native-enable", memsdk.SettingsRequest{PersonalEnabled: &enabled}); e != nil {
		t.Fatal(e)
	}
	revokedToken := issue(scope, "revoked-task", false)
	if e = ids.CancelSubtree(ctx, scope, "revoked-task"); e != nil {
		t.Fatal(e)
	}
	config := map[string]string{"url": srv.URL, "token": token, "authority_token": managementToken, "foreign_subject_token": otherSubjectToken, "foreign_tenant_token": otherTenantToken, "revoked_token": revokedToken, "human_token": humanToken, "operation_id": op.ID, "source_ref": req.SourceRefs[0]}
	runNativePythonMemory(t, ctx, srv, config, tr.TLSClientConfig.Certificates[0], otherCert)
	clear, e := management.Clear(ctx, "native-clear", memsdk.ClearRequest{Scope: memsdk.Personal})
	if e != nil || clear.State != "deleting" {
		t.Fatalf("clear: %+v error=%v", clear, e)
	}
	if cleared, e := client.Search(ctx, memsdk.SearchRequest{Scope: "personal", Query: "Alice"}); e != nil || len(cleared.Facts) != 0 {
		t.Fatalf("clear left retrieval visible: %v", e)
	}
	runNativeProjectCompletion(t, ctx, s, ids, verifier, signer, mux, srv.URL, hc, scope)
	if err := ids.CancelSubtree(ctx, scope, "memory-task"); err != nil {
		t.Fatal(err)
	}
	_, err = client.Search(ctx, memsdk.SearchRequest{Scope: "personal", Query: "Alice"})
	var denied *memsdk.Error
	if !errors.As(err, &denied) || denied.Status != http.StatusForbidden {
		t.Fatalf("revoked grant recall must fail closed: %v", err)
	}
	t.Log("verified: Go and Python SDK native retain/recall; subject/tenant isolation; human/workload/project denial; management purpose; idempotency; clear; revocation")
}

func runNativePythonMemory(t *testing.T, ctx context.Context, srv *httptest.Server, config map[string]string, client, other tls.Certificate) {
	t.Helper()
	dir := t.TempDir()
	write := func(name string, value []byte) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, value, 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	config["ca"] = write("server.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}))
	for name, cert := range map[string]tls.Certificate{"client": client, "foreign_tenant": other} {
		key, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
		if err != nil {
			t.Fatal(err)
		}
		config[name+"_cert"] = write(name+".pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}))
		config[name+"_key"] = write(name+".key", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}))
	}
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	configFile := write("config.json", raw)
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(ctx, "python3", "sdk/python/tests/memory_live_contract.py", configFile)
	command.Dir = root
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "sdk/python"))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("Python Memory native contract: %v\n%s", err, output)
	}
	t.Log(strings.TrimSpace(string(output)))
}
