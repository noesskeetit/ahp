//go:build ahp_data

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/memory"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/workspace"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestControlDeleteSubjectCoversAllDeploymentsAndMemoryOnlyData(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, art, scope := cascadeEnv(t, ctx)
	st := state.New(pool)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	a, err := st.CreateThread(ctx, scope, "global-a", "a")
	if err != nil {
		t.Fatal(err)
	}
	second := scope
	second.Deployment = "another-deployment"
	b, err := st.CreateThread(ctx, second, "global-b", "b")
	if err != nil {
		t.Fatal(err)
	}
	firstArt := sealArtifact(t, ctx, art, scope, artifacts.Binding{ContextID: a.ContextID}, "first deployment")
	secondArt := sealArtifact(t, ctx, art, second, artifacts.Binding{ContextID: b.ContextID}, "second deployment")
	orphanScope := scope
	orphanScope.Deployment = "artifact-only"
	orphan := sealArtifact(t, ctx, art, orphanScope, artifacts.Binding{ContextID: "no-state-context"}, "artifact-only deployment")
	foreign := scope
	foreign.Subject = "other-human"
	kept := sealArtifact(t, ctx, art, foreign, artifacts.Binding{ContextID: "other-context"}, "must survive")
	srv := cascadeServer(t, st, art, scope)
	mem, err := memory.New(pool, art, func(context.Context, contracts.Scope, string, string) error { return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	memScope := scope
	memScope.Deployment = "memory-only"
	note, err := mem.Propose(ctx, memScope, memory.ProposeRequest{Kind: memory.KindUser, Text: "memory without any State", OpKey: "memory-only"})
	if err != nil {
		t.Fatal(err)
	}
	token, err := srv.signer.Sign(auth.Claims{Kind: auth.KindHuman, Audience: "ahp", Tenant: scope.Tenant, Subject: "compliance-admin"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	code, body := srv.adminCall(t, "ControlDeleteSubject", token, scope, `{"key":"global-subject"}`)
	if code != http.StatusOK {
		t.Fatalf("global subject deletion = %d %v", code, body)
	}
	for _, th := range []struct {
		scope contracts.Scope
		id    string
	}{{scope, a.ID}, {second, b.ID}} {
		if _, err := st.GetThread(ctx, th.scope, th.id); contracts.Code(err) != contracts.ErrNotFound {
			t.Errorf("State survived global deletion: %v", err)
		}
	}
	for _, item := range []struct {
		scope contracts.Scope
		id    string
	}{{scope, firstArt}, {second, secondArt}, {orphanScope, orphan}} {
		if _, err := art.Get(ctx, item.scope, item.id); contracts.Code(err) != contracts.ErrNotFound {
			t.Errorf("artifact survived global deletion: %v", err)
		}
	}
	if _, err := mem.Get(ctx, memScope, note.Memory.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("memory-only subject data survived: %v", err)
	}
	if _, err := art.Get(ctx, foreign, kept); err != nil {
		t.Fatalf("foreign subject was deleted: %v", err)
	}
	code, replayed := srv.adminCall(t, "ControlDeleteSubject", token, scope, `{"key":"global-subject"}`)
	if code != http.StatusOK || replayed["deletion_id"] != body["deletion_id"] {
		t.Fatalf("control deletion receipt replay changed: %d %v", code, replayed)
	}
}

func TestControlDeletionHookRetryUsesPersistedVersionAndOperationKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, art, scope := cascadeEnv(t, ctx)
	st := state.New(pool)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS public.root_delete_hook_rows(tenant text,subject text,PRIMARY KEY(tenant,subject));CREATE TABLE IF NOT EXISTS public.root_delete_hook_ops(operation_key text PRIMARY KEY);`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO public.root_delete_hook_rows VALUES($1,$2),($1,'foreign-owner')`, scope.Tenant, scope.Subject); err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	var effects atomic.Int32
	ca := newTestCA(t)
	hook := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || r.Header.Get("Authorization") != "" {
			http.Error(w, "control certificate required, no human bearer", 403)
			return
		}
		var in struct {
			Tenant  string `json:"tenant"`
			Subject string `json:"subject"`
			Key     string `json:"operation_key"`
			Job     string `json:"deletion_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Key == "" || r.Header.Get("Idempotency-Key") != in.Key {
			http.Error(w, "bad operation", 400)
			return
		}
		tx, err := pool.Begin(r.Context())
		if err != nil {
			http.Error(w, "db", 503)
			return
		}
		defer tx.Rollback(r.Context())
		tag, err := tx.Exec(r.Context(), `INSERT INTO public.root_delete_hook_ops VALUES($1) ON CONFLICT DO NOTHING`, in.Key)
		if err != nil {
			http.Error(w, "db", 503)
			return
		}
		if tag.RowsAffected() == 1 {
			if _, err = tx.Exec(r.Context(), `DELETE FROM public.root_delete_hook_rows WHERE tenant=$1 AND subject=$2`, in.Tenant, in.Subject); err != nil {
				http.Error(w, "db", 503)
				return
			}
			effects.Add(1)
		}
		if err = tx.Commit(r.Context()); err != nil {
			http.Error(w, "db", 503)
			return
		}
		if attempts.Add(1) == 1 {
			http.Error(w, "acknowledgement lost after commit", 503)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"status": "completed", "operation_key": in.Key})
	}))
	trusted := x509.NewCertPool()
	trusted.AppendCertsFromPEM(ca.pem)
	hook.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: trusted}
	hook.StartTLS()
	defer hook.Close()
	client := hook.Client()
	tr := client.Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{ca.clientCert(t, auth.WorkloadID(scope))}
	client.Transport = tr
	cfg := ControlDeletionConfig{HTTPClient: client, ResolveHook: func(context.Context, string, registry.DeletionTarget) (string, error) { return hook.URL + "/a2a", nil }}
	srv := cascadeServerConfigured(t, st, art, scope, nil, cfg)
	version := "v-" + contracts.NewID()
	installation := "i-" + contracts.NewID()
	agent := "own-table-" + contracts.NewID()
	manifest := `{"state":{"own_tables":true,"delete_hook":"/ahp/delete-subject"}}`
	if _, err := pool.Exec(ctx, `INSERT INTO ahp_registry.versions(id,agent_id,digest,manifest) VALUES($1,$2,$3,$4)`, version, agent, "sha256:"+strings.Repeat("a", 64), manifest); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO ahp_registry.installations(id,tenant,agent_id,deployment,environment,version_id,idem_key,request_digest) VALUES($1,$2,$3,'custom-deploy','dev',$4,$1,'fixture')`, installation, scope.Tenant, agent, version); err != nil {
		t.Fatal(err)
	}
	tok, err := srv.signer.Sign(auth.Claims{Kind: auth.KindHuman, Audience: "ahp", Tenant: scope.Tenant, Subject: "compliance-admin"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	code, body := srv.adminCall(t, "ControlDeleteSubject", tok, scope, `{"key":"hook-retry"}`)
	if code != http.StatusServiceUnavailable || body["state"] != "pending" {
		t.Fatalf("lost ack was not durable pending: %d %v", code, body)
	}
	// Missing mTLS configuration after restart must fail closed, preserving
	// the same pending job instead of panicking or using an insecure client.
	missing := cascadeServerConfigured(t, state.New(pool), art, scope, nil)
	missingToken, err := missing.signer.Sign(auth.Claims{Kind: auth.KindHuman, Audience: "ahp", Tenant: scope.Tenant, Subject: "compliance-admin"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	missingCode, missingBody := missing.adminCall(t, "ControlDeleteSubject", missingToken, scope, `{"key":"hook-retry"}`)
	if missingCode != http.StatusServiceUnavailable || missingBody["deletion_id"] != body["deletion_id"] {
		t.Fatalf("missing retry mTLS = %d %v", missingCode, missingBody)
	}
	// A new process cannot re-resolve the version now: the durable plan must
	// carry the selected endpoint, hook and operation identity across restart.
	cfg.ResolveHook = func(context.Context, string, registry.DeletionTarget) (string, error) {
		return "", errors.New("must not re-resolve a durable plan")
	}
	restarted := cascadeServerConfigured(t, state.New(pool), art, scope, nil, cfg)
	tok, err = restarted.signer.Sign(auth.Claims{Kind: auth.KindHuman, Audience: "ahp", Tenant: scope.Tenant, Subject: "compliance-admin"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	code, replayed := restarted.adminCall(t, "ControlDeleteSubject", tok, scope, `{"key":"hook-retry"}`)
	if code != http.StatusOK || replayed["deletion_id"] != body["deletion_id"] {
		t.Fatalf("hook retry = %d %v", code, replayed)
	}
	if attempts.Load() != 2 || effects.Load() != 1 {
		t.Fatalf("hook attempts=%d committed effects=%d", attempts.Load(), effects.Load())
	}
	var own, foreign int
	if err = pool.QueryRow(ctx, `SELECT count(*) FILTER(WHERE subject=$2),count(*) FILTER(WHERE subject='foreign-owner') FROM public.root_delete_hook_rows WHERE tenant=$1`, scope.Tenant, scope.Subject).Scan(&own, &foreign); err != nil {
		t.Fatal(err)
	}
	if own != 0 || foreign != 1 {
		t.Fatalf("custom data owner counts own=%d foreign=%d", own, foreign)
	}
}

func TestControlDeletionRejectsSelfBeforeMutation(t *testing.T) {
	ctx := context.Background()
	pool, art, scope := cascadeEnv(t, ctx)
	scope.Subject = "compliance-admin"
	st := state.New(pool)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	th, err := st.CreateThread(ctx, scope, "self-delete", "create")
	if err != nil {
		t.Fatal(err)
	}
	srv := cascadeServer(t, st, art, scope)
	tok, err := srv.signer.Sign(auth.Claims{Kind: auth.KindHuman, Audience: "ahp", Tenant: scope.Tenant, Subject: scope.Subject}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	code, body := srv.adminCall(t, "ControlDeleteSubject", tok, scope, `{"key":"self"}`)
	if code != http.StatusForbidden || body["reason"] != "self_deletion_requires_service_authority" {
		t.Fatalf("self-deletion = %d %v", code, body)
	}
	if _, err = st.GetThread(ctx, scope, th.ID); err != nil {
		t.Fatalf("self-deletion mutated State: %v", err)
	}
}

func TestDeletionHookPathsStayOnRegisteredOrigin(t *testing.T) {
	for _, path := range []string{"https://foreign.test/delete", "//foreign.test/delete", "../delete", "/x/../delete", "/delete?token=secret", "/delete#fragment", "/%2e%2e/delete"} {
		if _, err := deletionHookURL("https://agent.test/a2a", path); err == nil {
			t.Errorf("unsafe hook path accepted: %q", path)
		}
	}
	if got, err := deletionHookURL("https://agent.test/a2a", "ahp/delete-subject"); err != nil || got != "https://agent.test/ahp/delete-subject" {
		t.Fatalf("relative hook %q %v", got, err)
	}
}

type unavailableWorkspaceDeletion struct{ workspaceSubjectDeleter }

func (w unavailableWorkspaceDeletion) DeleteSubjectMemberships(context.Context, contracts.Scope, string) error {
	return errors.New("workspace unavailable")
}

func TestControlDeletionRechecksHoldAfterPartialExecution(t *testing.T) {
	ctx := context.Background()
	pool, art, scope := cascadeEnv(t, ctx)
	st := state.New(pool)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	th, err := st.CreateThread(ctx, scope, "hold-context", "create")
	if err != nil {
		t.Fatal(err)
	}
	srv := cascadeServerConfigured(t, st, art, scope, func(s *server) {
		c := s.cascade.(*memoryAwareCascade)
		c.workspace = unavailableWorkspaceDeletion{c.workspace}
	})
	mem, err := memory.New(pool, art, func(context.Context, contracts.Scope, string, string) error { return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	note, err := mem.Propose(ctx, scope, memory.ProposeRequest{Kind: memory.KindUser, Text: "held memory", OpKey: "held-memory"})
	if err != nil {
		t.Fatal(err)
	}
	tok, err := srv.signer.Sign(auth.Claims{Kind: auth.KindHuman, Audience: "ahp", Tenant: scope.Tenant, Subject: "compliance-admin"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	code, body := srv.adminCall(t, "ControlDeleteSubject", tok, scope, `{"key":"hold-retry"}`)
	if code != 503 || body["state"] != "pending" {
		t.Fatalf("partial %d %v", code, body)
	}
	if err = st.SetLegalhold(ctx, scope, "", "new hold after partial attempt"); err != nil {
		t.Fatal(err)
	}
	restarted := cascadeServer(t, st, art, scope)
	tok, err = restarted.signer.Sign(auth.Claims{Kind: auth.KindHuman, Audience: "ahp", Tenant: scope.Tenant, Subject: "compliance-admin"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	code, body = restarted.adminCall(t, "ControlDeleteSubject", tok, scope, `{"key":"hold-retry"}`)
	if code != 200 || body["state"] != "completed_with_holds" {
		t.Fatalf("held retry %d %v", code, body)
	}
	if _, err = st.GetThread(ctx, scope, th.ID); err != nil {
		t.Fatalf("held State lost: %v", err)
	}
	if _, err = mem.Get(ctx, scope, note.Memory.ID); err != nil {
		t.Fatalf("held Memory lost: %v", err)
	}
}

func TestControlDeletionPreservesSharedWorkspaceHistory(t *testing.T) {
	ctx := context.Background()
	pool, art, scope := cascadeEnv(t, ctx)
	st := state.New(pool)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	srv := cascadeServer(t, st, art, scope)
	ws, err := workspace.New(pool, art, func(context.Context, contracts.Scope, string, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	w, err := ws.Create(ctx, scope, "shared-"+contracts.NewID())
	if err != nil {
		t.Fatal(err)
	}
	if err = ws.AddMember(ctx, scope, w.ID, "foreign-owner"); err != nil {
		t.Fatal(err)
	}
	revision := contracts.NewID()
	if _, err = pool.Exec(ctx, `INSERT INTO ahp_workspace.revisions(id,workspace_id,seq,digest,author_subject) VALUES($1,$2,1,'shared-digest',$3)`, revision, w.ID, scope.Subject); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE ahp_workspace.workspaces SET head_rev=$1 WHERE id=$2`, revision, w.ID); err != nil {
		t.Fatal(err)
	}
	if err = ws.Pin(ctx, scope, w.ID, revision); err != nil {
		t.Fatal(err)
	}
	foreign := scope
	foreign.Subject = "foreign-owner"
	if err = ws.Pin(ctx, foreign, w.ID, revision); err != nil {
		t.Fatal(err)
	}
	tok, err := srv.signer.Sign(auth.Claims{Kind: auth.KindHuman, Audience: "ahp", Tenant: scope.Tenant, Subject: "compliance-admin"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	code, body := srv.adminCall(t, "ControlDeleteSubject", tok, scope, `{"key":"workspace-delete"}`)
	if code != 200 {
		t.Fatalf("workspace deletion %d %v", code, body)
	}
	var members, pins, revisions int
	if err = pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM ahp_workspace.members WHERE workspace_id=$1 AND subject=$2),(SELECT count(*) FROM ahp_workspace.pins WHERE workspace_id=$1 AND subject=$2),(SELECT count(*) FROM ahp_workspace.revisions WHERE workspace_id=$1)`, w.ID, scope.Subject).Scan(&members, &pins, &revisions); err != nil {
		t.Fatal(err)
	}
	if members != 0 || pins != 0 || revisions != 1 {
		t.Fatalf("own membership=%d pins=%d shared revisions=%d", members, pins, revisions)
	}
	if err = ws.IsMember(ctx, foreign, w.ID); err != nil {
		t.Fatalf("foreign membership lost: %v", err)
	}
}

func TestRegistryDeletionTargetsIncludePinnedOldVersions(t *testing.T) {
	ctx := context.Background()
	pool, art, scope := cascadeEnv(t, ctx)
	st := state.New(pool)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	_ = cascadeServer(t, st, art, scope)
	agent := "pins-" + contracts.NewID()
	installation := "i-" + contracts.NewID()
	current, old := contracts.NewID(), contracts.NewID()
	for i, v := range []string{current, old} {
		manifest := `{"state":{"own_tables":true,"delete_hook":"delete-subject"}}`
		if _, err := pool.Exec(ctx, `INSERT INTO ahp_registry.versions(id,agent_id,digest,manifest) VALUES($1,$2,$3,$4)`, v, agent, "sha256:"+strings.Repeat(string(rune('a'+i)), 64), manifest); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `INSERT INTO ahp_registry.installations(id,tenant,agent_id,deployment,environment,version_id,idem_key,request_digest) VALUES($1,$2,$3,'pinned-deploy','dev',$4,$1,'fixture')`, installation, scope.Tenant, agent, current); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO ahp_registry.task_pins(task_id,tenant,installation_id,version_id) VALUES($1,$2,$3,$4)`, contracts.NewID(), scope.Tenant, installation, old); err != nil {
		t.Fatal(err)
	}
	targets, err := registry.New(pool).DeletionTargets(ctx, scope.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 2 {
		t.Fatalf("expected current and old pinned custom data owners: %+v", targets)
	}
}
