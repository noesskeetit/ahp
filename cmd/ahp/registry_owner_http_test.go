package main

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

	"github.com/ahp-platform/ahp/internal/evals"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
	"github.com/jackc/pgx/v5/pgxpool"
)

// This is real PostgreSQL + HTTPS/mTLS API acceptance, not LLM acceptance.
// The runtime endpoint and evaluation content are explicit fixtures. Human
// history must read durable State without even probing that endpoint.
func TestRegistryOwnerHistoryHTTPReauthenticationAndMachineBoundary(t *testing.T) {
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires isolated real PostgreSQL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	ids, st, ev := identity.New(pool), state.New(pool), evals.NewStore(pool)
	rs := registry.New(pool, registry.WithEvalGate(ev), registry.WithSyntheticSupplyChain("owner HTTP wire fixture"))
	for _, migrate := range []func(context.Context) error{ids.Migrate, st.Migrate, ev.Migrate, rs.Migrate} {
		if err := migrate(ctx); err != nil {
			t.Fatal(err)
		}
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner("owner-history-test", "test", private)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier("owner-history-test", map[string]ed25519.PublicKey{"test": public})
	scope := contracts.Scope{Tenant: "history-" + strings.ToLower(contracts.NewID()), Deployment: "space", Subject: "owner"}
	agentID := "history-agent-" + strings.ToLower(contracts.NewID())
	var digest string
	image := "example/history@sha256:" + strings.Repeat("b", 64)
	const answer = "durable owner result after fresh login"
	childTask := "invoked-child-" + contracts.NewID()
	runtimeTask := "native-" + childTask
	var runtimeRequests atomic.Int32
	runtime := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		runtimeRequests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/readyz" {
			json.NewEncoder(w).Encode(map[string]any{"ready": true, "agent_id": agentID, "version_digest": digest, "image": image})
			return
		}
		var request struct {
			ID json.RawMessage `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": map[string]any{"id": runtimeTask, "status": map[string]string{"state": "completed"}}})
	}))
	defer runtime.Close()
	binding := registry.RuntimeBinding{Tenant: scope.Tenant, Deployment: "custom", Environment: registry.EnvironmentProd, AgentID: agentID, Digest: digest, Image: image, Endpoint: runtime.URL + "/a2a", ReadyURL: runtime.URL + "/readyz", Attestation: "operator:api-test-fixture"}
	manifest := registry.Manifest{Kind: registry.KindAgent, System: "history", Visibility: registry.VisibilityTenant, Audience: registry.AudienceShared, Class: registry.ClassC, Components: []registry.Component{{Name: "agent", A2A: true, Image: image, MinReplicas: 1, MaxReplicas: 1}}, State: registry.StateConfig{Partition: "subject", Compat: "v1"}, Limits: registry.Limits{Depth: 2, ActiveTasks: 4, TaskTimeout: "1m"}}
	digest, err = contracts.Digest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	binding.Digest = digest
	if _, err := ev.SetPolicy(ctx, evals.Policy{ID: "history-api-fixture-policy", TaskSetID: "history-api-fixture", RequiredTaskIDs: []string{"fixture"}, MaxAgeSeconds: 3600}); err != nil {
		t.Fatal(err)
	}
	if _, err := ev.Record(ctx, evals.RecordInput{AgentDigest: digest, TaskSetID: "history-api-fixture", Setup: map[string]any{"fixture": true}, Outcome: evals.OutcomePass, Tasks: []evals.TaskResult{{TaskID: "fixture", Passed: true, TerminalState: evals.TerminalCompleted}}, PolicyID: "history-api-fixture-policy", TargetKind: evals.TargetFixture}); err != nil {
		t.Fatal(err)
	}
	if _, err := rs.RegisterAgent(ctx, scope.Tenant, agentID); err != nil {
		t.Fatal(err)
	}
	version, err := rs.PutVersion(ctx, scope.Tenant, agentID, digest, manifest)
	if err != nil {
		t.Fatal(err)
	}
	installation, err := rs.Install(ctx, scope.Tenant, agentID, version.ID, registry.EnvironmentProd, "install", manifest.Capabilities)
	if err != nil {
		t.Fatal(err)
	}
	if err := rs.RegisterRuntime(ctx, binding); err != nil {
		t.Fatal(err)
	}
	if _, err := rs.PinForSubject(ctx, scope, installation.ID, childTask); err != nil {
		t.Fatal(err)
	}
	config, err := rs.ResolveConfig(ctx, scope.Tenant, installation.ID, childTask)
	if err != nil {
		t.Fatal(err)
	}
	authority := runtimeAuthority{ids: ids, registry: rs, verifier: verifier, signer: signer}
	permissions := []string{"registry", "identity:delegate", "state", "execution", "identity:cancel"}
	newParent := func(task string, ttl time.Duration) (identity.Grant, string) {
		t.Helper()
		grant, err := ids.CreateRoot(ctx, scope, identity.RootRequest{Task: task, Context: task + "-" + contracts.NewID(), Deployment: scope.Deployment, Capabilities: permissions, TTL: ttl})
		if err != nil {
			t.Fatal(err)
		}
		token, err := ids.MintTask(ctx, scope, task, signer, "ahp", 0)
		if err != nil {
			t.Fatal(err)
		}
		return grant, token
	}
	parent, parentToken := newParent("original-parent", 4*time.Second)
	req := registry.InvokeRequest{Scope: scope, TaskID: childTask, Tenant: scope.Tenant, VersionID: version.ID, VersionDigest: digest, Capabilities: manifest.Capabilities, Token: parentToken}
	if _, err := authority.IssueRuntimeToken(ctx, req, binding); err != nil {
		t.Fatal(err)
	}
	childScope := scope
	childScope.Deployment = binding.Deployment
	child, err := ids.Get(ctx, childScope, childTask)
	if err != nil {
		t.Fatal(err)
	}
	thread, err := st.CreateThread(ctx, childScope, child.Context, "thread")
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]any{"digest": digest, "config_revision": config.ConfigRevision})
	run, err := st.CreateRun(ctx, childScope, thread.ID, childTask, "run", payload)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := st.ClaimRunFor(ctx, scope.Tenant, binding.Deployment, "worker", run.ID, time.Minute)
	if err != nil || lease == nil {
		t.Fatalf("claim: %v", err)
	}
	metadata, _ := json.Marshal(map[string]any{"image": image, "version_digest": digest, "config_revision": config.ConfigRevision})
	if _, err := st.PutCheckpoint(ctx, childScope, state.Fence{RunID: run.ID, Worker: lease.Worker, Epoch: lease.Epoch}, state.Checkpoint{ID: "result", ThreadID: thread.ID, Namespace: run.ID, Version: 1, Metadata: metadata, Parts: []state.Part{{Channel: "output", Version: "v1", Data: []byte(answer)}}}, "result"); err != nil {
		t.Fatal(err)
	}
	if err := st.Release(ctx, childScope, run.ID, lease.Worker, lease.Epoch, state.RunDone); err != nil {
		t.Fatal(err)
	}
	dispatcher, err := registry.NewRoutedA2AInvokeDispatcher(rs, runtime.Client())
	if err != nil {
		t.Fatal(err)
	}
	dispatcher.SetRuntimeTokenIssuer(authority)
	reader := &registryStateObserver{ids: ids, registry: rs, state: st, images: map[string]bool{image: true}}
	newHandler := func() http.Handler {
		t.Helper()
		h, err := registry.NewHTTPHandler(rs, verifier, ids.Authorize)
		if err != nil {
			t.Fatal(err)
		}
		if err := registry.AttachInvokeDispatcher(h, dispatcher); err != nil {
			t.Fatal(err)
		}
		if err := registry.AttachOwnerObserver(h, reader, verifier, "ahp-browser", registryObservationAuthority(ids, []string{"space"})); err != nil {
			t.Fatal(err)
		}
		return h
	}
	ca := newMountCA(t)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	front := httptest.NewUnstartedServer(newHandler())
	front.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	front.StartTLS()
	defer front.Close()
	clientFor := func(deployment string) *http.Client {
		tr := front.Client().Transport.(*http.Transport).Clone()
		clientScope := scope
		clientScope.Deployment = deployment
		tr.TLSClientConfig.Certificates = []tls.Certificate{ca.clientCert(t, auth.WorkloadID(clientScope))}
		t.Cleanup(tr.CloseIdleConnections)
		return &http.Client{Transport: tr}
	}
	client := clientFor("space")
	if err := rs.AliasTaskPin(ctx, scope.Tenant, installation.ID, childTask, runtimeTask); err != nil {
		t.Fatal(err)
	}
	observedTask := runtimeTask
	call := func(t *testing.T, c *http.Client, token string, want int, contains string) {
		t.Helper()
		body, _ := json.Marshal(map[string]string{"installation_id": installation.ID, "task_id": observedTask})
		r, err := http.NewRequestWithContext(ctx, http.MethodPost, front.URL+"/v1/registry/GetInvokedTask", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("Content-Type", "application/json")
		res, err := c.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		data, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != want || (contains != "" && !bytes.Contains(data, []byte(contains))) {
			t.Fatalf("history status=%d want=%d expected=%q body=%s", res.StatusCode, want, contains, data)
		}
		if want != http.StatusOK && bytes.Contains(data, []byte(answer)) {
			t.Fatal("denied request disclosed output")
		}
	}
	t.Run("original_machine_parent_allowed", func(t *testing.T) { call(t, client, parentToken, http.StatusOK, childTask) })
	_, freshParentToken := newParent("fresh-unrelated-parent", time.Hour)
	t.Run("new_machine_parent_cannot_read_old_child", func(t *testing.T) { call(t, client, freshParentToken, http.StatusForbidden, "original parent grant") })
	// Wait for actual Identity expiry, not for an assumed duration or DB update.
	for {
		_, err := ids.Evaluate(ctx, scope, parent.Task, "registry")
		if contracts.Code(err) == contracts.ErrForbidden {
			break
		}
		if err != nil || ctx.Err() != nil {
			t.Fatalf("wait for execution expiry: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Run("expired_parent_token_denied", func(t *testing.T) { call(t, client, parentToken, http.StatusForbidden, "") })
	seedUser := func(subject string, permissions []string) identity.BrowserUser {
		t.Helper()
		u := identity.BrowserUser{UserID: "user-" + contracts.NewID(), Tenant: scope.Tenant, Subject: subject, DisplayName: subject, Email: subject + "@example.test"}
		if err := ids.SeedBrowserUser(ctx, u, u.UserID, "fixture-password-long-enough", []string{"user"}, permissions); err != nil {
			t.Fatal(err)
		}
		return u
	}
	login := func(u identity.BrowserUser) (identity.BrowserSession, string) {
		t.Helper()
		session, err := ids.StartBrowserSession(ctx, u.UserID)
		if err != nil {
			t.Fatal(err)
		}
		jti := contracts.NewID()
		if err := ids.BindBrowserToken(ctx, jti, session); err != nil {
			t.Fatal(err)
		}
		token, err := signer.Sign(auth.Claims{Audience: "ahp-browser", Kind: auth.KindHuman, Tenant: scope.Tenant, Subject: u.Subject, JTI: jti}, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return session, token
	}
	owner := seedUser(scope.Subject, []string{"registry"})
	oldSession, oldHuman := login(owner)
	t.Run("owner_reads_after_execution_expiry", func(t *testing.T) { call(t, client, oldHuman, http.StatusOK, answer) })
	if err := ids.RevokeBrowserSession(ctx, oldSession.ID); err != nil {
		t.Fatal(err)
	}
	_, freshHuman := login(owner)
	beforeHistory := runtimeRequests.Load()
	t.Run("new_login_reload_same_saved_result", func(t *testing.T) {
		for i := 0; i < 2; i++ {
			call(t, client, freshHuman, http.StatusOK, answer)
		}
	})
	t.Run("revoked_previous_human_session_denied", func(t *testing.T) { call(t, client, oldHuman, http.StatusForbidden, "") })
	_, foreignHuman := login(seedUser("other-user", []string{"registry"}))
	t.Run("other_user_denied", func(t *testing.T) { call(t, client, foreignHuman, http.StatusNotFound, "") })
	_, noPermission := login(seedUser("no-registry", []string{"space"}))
	t.Run("current_permission_required", func(t *testing.T) { call(t, client, noPermission, http.StatusForbidden, "") })
	t.Run("unapproved_edge_denied", func(t *testing.T) { call(t, clientFor("rogue"), freshHuman, http.StatusForbidden, "") })
	t.Run("undeclared_runtime_protocol_fails_closed", func(t *testing.T) {
		reader.images = nil
		call(t, client, freshHuman, http.StatusServiceUnavailable, "no declared ahp-agent-state-v1")
		reader.images = map[string]bool{image: true}
	})
	if runtimeRequests.Load() != beforeHistory {
		t.Fatal("human history contacted the agent runtime")
	}
	if _, err := ids.Evaluate(ctx, childScope, childTask, "state"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("owner history renewed execution authority: %v", err)
	}
	retained, err := ids.Get(ctx, childScope, childTask)
	if err != nil || retained.ID != child.ID || !retained.ExpiresAt.Equal(child.ExpiresAt) || retained.Epoch != child.Epoch {
		t.Fatalf("owner history changed original grant: %v", err)
	}
	// Historical labels are written through Identity, never raw SQL. A source
	// owner match alone must not export content forbidden to this private sink.
	for _, tc := range []struct {
		name string
		set  labels.Set
		want int
	}{
		{"owner_label_allowed", labels.Set{Subjects: []string{scope.Subject}}, http.StatusOK},
		{"foreign_subject_label_denied", labels.Set{Subjects: []string{"other-user"}}, http.StatusForbidden},
		{"secret_label_denied", labels.Set{Taints: []string{labels.TaintSecret}}, http.StatusForbidden},
		{"external_label_denied", labels.Set{Taints: []string{labels.TaintUntrustedExternal}}, http.StatusForbidden},
		{"workspace_without_membership_denied", labels.Set{Workspaces: []string{"restricted"}}, http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observedTask = tc.name + "-" + contracts.NewID()
			if _, err := rs.Pin(ctx, scope.Tenant, installation.ID, observedTask); err != nil {
				t.Fatal(err)
			}
			grant, err := ids.CreateRoot(ctx, childScope, identity.RootRequest{Task: observedTask, Context: observedTask, Deployment: childScope.Deployment, Capabilities: []string{"state"}, TTL: time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ids.AddLabels(ctx, childScope, observedTask, tc.set); err != nil {
				t.Fatal(err)
			}
			thread, err := st.CreateThread(ctx, childScope, grant.Context, observedTask)
			if err != nil {
				t.Fatal(err)
			}
			run, err := st.CreateRun(ctx, childScope, thread.ID, observedTask, observedTask, payload)
			if err != nil {
				t.Fatal(err)
			}
			lease, err := st.ClaimRunFor(ctx, scope.Tenant, childScope.Deployment, "worker", run.ID, time.Minute)
			if err != nil || lease == nil {
				t.Fatalf("claim labeled run: %v", err)
			}
			if _, err := st.PutCheckpoint(ctx, childScope, state.Fence{RunID: run.ID, Worker: lease.Worker, Epoch: lease.Epoch}, state.Checkpoint{ID: "result", ThreadID: thread.ID, Namespace: run.ID, Version: 1, Metadata: metadata, Parts: []state.Part{{Channel: "output", Version: "v1", Data: []byte(answer)}}}, "result"); err != nil {
				t.Fatal(err)
			}
			if err := st.Release(ctx, childScope, run.ID, lease.Worker, lease.Epoch, state.RunDone); err != nil {
				t.Fatal(err)
			}
			call(t, client, freshHuman, tc.want, "")
		})
	}
	t.Run("later_context_labels_restrict_old_task", func(t *testing.T) {
		observedTask = runtimeTask
		laterTask := "later-context-task-" + contracts.NewID()
		if _, err := ids.CreateRoot(ctx, childScope, identity.RootRequest{Task: laterTask, Context: child.Context, Deployment: childScope.Deployment, Capabilities: []string{"state"}, TTL: time.Hour}); err != nil {
			t.Fatal(err)
		}
		if _, err := ids.AddLabels(ctx, childScope, laterTask, labels.Set{Taints: []string{labels.TaintSecret}}); err != nil {
			t.Fatal(err)
		}
		// The old grant is intentionally unchanged: only the journal carries
		// the newer restriction from this independent task of the same context.
		oldGrant, err := ids.Get(ctx, childScope, childTask)
		if err != nil || len(oldGrant.Labels.Taints) != 0 {
			t.Fatalf("fixture requires unchanged old grant snapshot: %v", err)
		}
		call(t, client, freshHuman, http.StatusForbidden, "")
	})
}
