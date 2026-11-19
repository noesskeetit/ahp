//go:build ahp_data

package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/memory"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/workspace"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Real PG, live Identity and Workspace, real mTLS. No native extraction or
// embeddings in this contract leg: it deliberately selects explicit lexical.
func TestSupplementalPDFMemoryPreservesPrivateHindsight(t *testing.T) {
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires isolated real PG and own data-plane configuration")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ids := identity.New(pool)
	if err = ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	st := state.New(pool)
	if err = migrateStateAll(ctx, st); err != nil {
		t.Fatal(err)
	}
	scope := contracts.Scope{Tenant: fmt.Sprintf("shared-memory-%d", time.Now().UnixNano()), Subject: "alice", Deployment: "agent"}
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner("shared-memory", "test", key)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier("shared-memory", map[string]ed25519.PublicKey{"test": pub})
	issue := func(subject, task string) string {
		t.Helper()
		sc := scope
		sc.Subject = subject
		_, e := ids.CreateRoot(ctx, sc, identity.RootRequest{Task: task, Context: scope.Tenant + ":context-" + task, Deployment: sc.Deployment, Capabilities: []string{"state", "memory", "memory:review", "memory:delete"}, TTL: time.Hour})
		if e != nil {
			t.Fatal(e)
		}
		token, e := ids.MintTask(ctx, sc, task, signer, "ahp", 5*time.Minute)
		if e != nil {
			t.Fatal(e)
		}
		return token
	}
	alice := issue("alice", "alice")
	bob := issue("bob", "bob")
	t.Setenv("AHP_MEMORY_BACKEND", "hindsight")
	t.Setenv("AHP_MEMORY_HINDSIGHT_URL", "http://127.0.0.1:59998")
	t.Setenv("AHP_ENABLE_SHARED_MEMORY", "1")
	t.Setenv("AHP_MEMORY_SEARCH_MODE", "lexical")
	t.Setenv("AHP_MEMORY_EMBEDDING_BASE_URL", "")
	s := &server{pool: pool, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if _, _, err = s.buildDataHandler(ctx); err != nil {
		t.Fatal(err)
	}
	art, _, _, err := s.newDataPlanes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ws, err := workspace.New(pool, art, func(context.Context, contracts.Scope, string, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err = ws.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	project, err := ws.Create(ctx, scope, "shared-project")
	if err != nil {
		t.Fatal(err)
	}
	if err = ws.AddMember(ctx, scope, project.ID, "bob"); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	if err = s.tryMountMemory(ctx, mux, verifier, ids.Authorize, authorityIdentityMode, identityGrantLabels{evaluate: ids}); err != nil {
		t.Fatal(err)
	}
	ca := newTestCA(t)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	srv := httptest.NewUnstartedServer(mux)
	srv.TLS = &tls.Config{MinVersion: tls.VersionTLS12, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	srv.StartTLS()
	defer srv.Close()
	client := srv.Client()
	tr := client.Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{ca.clientCert(t, auth.WorkloadID(scope))}
	client.Transport = tr
	call := func(token, path string, body any, out any) int {
		t.Helper()
		raw, e := json.Marshal(body)
		if e != nil {
			t.Fatal(e)
		}
		req, e := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+path, bytes.NewReader(raw))
		if e != nil {
			t.Fatal(e)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		res, e := client.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		defer res.Body.Close()
		data, e := io.ReadAll(res.Body)
		if e != nil {
			t.Fatal(e)
		}
		if out != nil && res.StatusCode == 200 {
			if e = json.Unmarshal(data, out); e != nil {
				t.Fatal(e)
			}
		}
		if res.StatusCode >= 500 {
			t.Fatalf("owner unavailable: %d %s", res.StatusCode, data)
		}
		return res.StatusCode
	}
	propose := func(token, kind, text, key string) memory.ProposeResult {
		t.Helper()
		req := memory.ProposeRequest{Kind: kind, Text: text, OpKey: key, SourceKind: "user_statement", Evidence: []memory.Evidence{{SourceKind: "user_statement", Detail: "synthetic contract fixture"}}}
		if kind == memory.KindWorkspace {
			req.Workspace = project.ID
		}
		var out memory.ProposeResult
		if code := call(token, "/v1/memory/shared/Propose", req, &out); code != 200 {
			t.Fatalf("supplemental PDF scope not mounted: %d", code)
		}
		if out.Memory.State != memory.StateProposed {
			t.Fatal("proposal bypassed review")
		}
		return out
	}
	shared := propose(alice, memory.KindWorkspace, "approved shared operating note", "shared")
	if code := call(bob, "/v1/memory/shared/Search", memory.SearchRequest{Kind: memory.KindWorkspace}, nil); code != 200 {
		t.Fatal(code)
	}
	var approved memory.Memory
	if code := call(alice, "/v1/memory/shared/Review", map[string]any{"id": shared.Memory.ID, "decision": "approve", "op_key": "approve-shared"}, &approved); code != 200 || approved.State != memory.StateActive {
		t.Fatal(code, approved.State)
	}
	var seen memory.Memory
	if code := call(bob, "/v1/memory/shared/Read", map[string]any{"id": shared.Memory.ID}, &seen); code != 200 || seen.Text != shared.Memory.Text {
		t.Fatal("live member cannot read published shared fact", code)
	}
	// The native private API still refuses a multi-owner project. No shared bank
	// was invented, and private settings remain on their original native route.
	if code := call(alice, "/v1/memory/Settings", map[string]any{}, nil); code != 200 {
		t.Fatal("native personal settings route changed", code)
	}
	authority, err := ids.MintAuthorityTask(ctx, scope, "alice", signer, "ahp", 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if code := call(authority, "/v1/memory/Search", map[string]any{"scope": "project", "project_id": project.ID, "query": "note"}, nil); code != 403 && code != 404 {
		t.Fatal("native private project widened to shared membership", code)
	}
	clean := issue("alice", "alice-clean")
	agentFact := propose(clean, memory.KindAgent, "common agent operational note", "agent-common")
	if code := call(clean, "/v1/memory/shared/Review", map[string]any{"id": agentFact.Memory.ID, "decision": "approve", "op_key": "approve-agent"}, nil); code != 200 {
		t.Fatal(code)
	}
	if code := call(bob, "/v1/memory/shared/Read", map[string]any{"id": agentFact.Memory.ID}, nil); code != 200 {
		t.Fatal("agent scope not shared with another subject", code)
	}
	if _, err = ids.AddLabels(ctx, scope, "alice", labels.Set{Subjects: []string{"alice"}}); err != nil {
		t.Fatal(err)
	}
	private := propose(alice, memory.KindWorkspace, "private fact in workspace rubric", "private")
	if code := call(alice, "/v1/memory/shared/Review", map[string]any{"id": private.Memory.ID, "decision": "approve", "op_key": "approve-private"}, nil); code != 200 {
		t.Fatal(code)
	}
	if code := call(bob, "/v1/memory/shared/Read", map[string]any{"id": private.Memory.ID}, nil); code != 404 {
		t.Fatal("private source escaped to another member", code)
	}
	req := memory.ProposeRequest{Kind: memory.KindAgent, Text: "must not escape", OpKey: "private-agent", SourceKind: "user_statement", Evidence: []memory.Evidence{{SourceKind: "user_statement"}}}
	if code := call(alice, "/v1/memory/shared/Propose", req, nil); code != 403 {
		t.Fatal("private ceiling entered common agent scope", code)
	}
	if err = ws.RemoveMember(ctx, scope, project.ID, "bob"); err != nil {
		t.Fatal(err)
	}
	if code := call(bob, "/v1/memory/shared/Read", map[string]any{"id": shared.Memory.ID}, nil); code != 404 {
		t.Fatal("revoked member retained shared memory access", code)
	}
	revokedWrite := memory.ProposeRequest{Kind: memory.KindWorkspace, Workspace: project.ID, Text: "must not write after removal", OpKey: "revoked-write", SourceKind: "user_statement", Evidence: []memory.Evidence{{SourceKind: "user_statement"}}}
	if code := call(bob, "/v1/memory/shared/Propose", revokedWrite, nil); code != 403 {
		t.Fatal("revoked member can still write shared memory", code)
	}
	cascade, ok := s.cascade.(*memoryAwareCascade)
	if !ok {
		t.Fatal("deletion owner missing")
	}
	owner, ok := cascade.memory.(hindsightSubjectOwner)
	if !ok || owner.legacyOwner == nil {
		t.Fatal("supplemental records omitted from existing native erasure owner")
	}
	// A failed native setup must not publish the already prepared supplemental route.
	t.Setenv("AHP_MEMORY_HINDSIGHT_URL", "")
	broken := &server{pool: pool, logger: s.logger, cascade: s.cascade}
	hidden := http.NewServeMux()
	if err = broken.tryMountMemory(ctx, hidden, verifier, ids.Authorize, authorityIdentityMode, identityGrantLabels{evaluate: ids}); err == nil {
		t.Fatal("missing native URL silently accepted")
	}
	response := httptest.NewRecorder()
	hidden.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/memory/shared/Read", bytes.NewReader([]byte(`{}`))))
	if response.Code != 404 {
		t.Fatal("failed combined setup published a partial memory API", response.Code)
	}
	t.Log("supplemental PDF store and private Hindsight mounted concurrently; review, live membership, privacy ceiling and erasure discovery passed; lexical contract test, no native extraction/semantic quality claim")
}

func TestSupplementalMemoryRequiresLiveIdentityBeforePublishing(t *testing.T) {
	t.Setenv("AHP_MEMORY_BACKEND", "hindsight")
	t.Setenv("AHP_ENABLE_SHARED_MEMORY", "1")
	mux := http.NewServeMux()
	if err := (&server{}).tryMountMemory(context.Background(), mux, nil, nil, authorityDevMode, nil); err == nil {
		t.Fatal("shared profile accepted absent live authority")
	}
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/v1/memory/shared/Propose", bytes.NewReader([]byte(`{}`))))
	if response.Code != 404 {
		t.Fatal("refused setup left a writable route")
	}
}
