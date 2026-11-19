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

func TestControlDeleteTenantIncludesUnknownSubjectsAndPreservesOtherTenant(t *testing.T) {
	ctx := context.Background()
	pool, art, scope := cascadeEnv(t, ctx)
	st := state.New(pool)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	stopped, stop := context.WithCancel(ctx)
	stop()
	var composed *server
	srv := cascadeServerConfigured(t, st, art, scope, func(s *server) { composed = s }, ControlDeletionConfig{ConsumerContext: stopped, TenantOwners: tenantTestOwners(t, pool)})
	targets := []contracts.Scope{scope, {Tenant: scope.Tenant, Deployment: "dep-2", Subject: "bob"}, {Tenant: scope.Tenant + "-foreign", Deployment: "foreign", Subject: "alice"}}
	var threads, artifactIDs []string
	for i, target := range targets {
		th, err := st.CreateThread(ctx, target, "ctx", "thread")
		if err != nil {
			t.Fatal(err)
		}
		threads = append(threads, th.ID)
		artifactIDs = append(artifactIDs, sealArtifact(t, ctx, art, target, artifacts.Binding{ContextID: th.ContextID}, string(rune('a'+i))))
	}
	mem, err := memory.New(pool, art, func(context.Context, contracts.Scope, string, string) error { return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	memoryOnly := scope
	memoryOnly.Subject = "only-in-memory"
	memoryOnly.Deployment = "memory-only"
	note, err := mem.Propose(ctx, memoryOnly, memory.ProposeRequest{Kind: memory.KindUser, Text: "memory-only subject", OpKey: "note"})
	if err != nil {
		t.Fatal(err)
	}
	ws, err := workspace.New(pool, art, func(context.Context, contracts.Scope, string, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	work, err := ws.Create(ctx, scope, "tenant-project")
	if err != nil {
		t.Fatal(err)
	}
	token, err := srv.signer.Sign(auth.Claims{Kind: auth.KindHuman, Audience: "ahp", Tenant: scope.Tenant, Subject: "compliance-admin"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	code, body := srv.adminCall(t, "ControlDeleteTenant", token, scope, `{"key":"tenant-erasure"}`)
	if code != http.StatusOK || body["state"] != "completed" {
		job, _ := st.GetControlDeletion(ctx, scope.Tenant, "compliance-admin", "tenant-erasure")
		c := stateDeletionCoordinator{state: st, cascade: composed.cascade}
		t.Fatalf("tenant deletion: %d %v steps=%v retry=%v", code, body, job.Steps, c.resumeControlDeletion(ctx, job))
	}
	for i, target := range targets {
		_, se := st.GetThread(ctx, target, threads[i])
		_, ae := art.Get(ctx, target, artifactIDs[i])
		if i < 2 {
			if contracts.Code(se) != contracts.ErrNotFound || contracts.Code(ae) != contracts.ErrNotFound {
				t.Fatalf("subject survives: %v %v", se, ae)
			}
		} else if se != nil || ae != nil {
			t.Fatalf("foreign tenant harmed: %v %v", se, ae)
		}
	}
	if _, err = mem.Get(ctx, memoryOnly, note.Memory.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("memory survives: %v", err)
	}
	var count int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM ahp_workspace.workspaces WHERE id=$1`, work.ID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("workspace survives: %d %v", count, err)
	}
	newScope := scope
	newScope.Subject = "never-seen-before"
	newScope.Deployment = "never-seen-deployment"
	if _, err = st.CreateThread(ctx, newScope, "late", "late"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("tenant resurrected: %v", err)
	}
	code, replay := srv.adminCall(t, "ControlDeleteTenant", token, scope, `{"key":"tenant-erasure"}`)
	if code != http.StatusOK || replay["deletion_id"] != body["deletion_id"] {
		t.Fatalf("tenant receipt changed: %d %v", code, replay)
	}
}

func TestTenantDeletionHonorsHoldAndFreezesRetainedWorkspace(t *testing.T) {
	ctx := context.Background()
	pool, art, scope := cascadeEnv(t, ctx)
	st := state.New(pool)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	stopped, stop := context.WithCancel(ctx)
	stop()
	srv := cascadeServerConfigured(t, st, art, scope, nil, ControlDeletionConfig{ConsumerContext: stopped, TenantOwners: tenantTestOwners(t, pool)})
	held, err := st.CreateThread(ctx, scope, "held", "held")
	if err != nil {
		t.Fatal(err)
	}
	if err = st.SetLegalhold(ctx, scope, held.ID, "legal hold"); err != nil {
		t.Fatal(err)
	}
	id := sealArtifact(t, ctx, art, scope, artifacts.Binding{ContextID: held.ContextID}, "held-content")
	ws, err := workspace.New(pool, art, func(context.Context, contracts.Scope, string, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	w, err := ws.Create(ctx, scope, "retained-project")
	if err != nil {
		t.Fatal(err)
	}
	cand, err := ws.CreateCandidate(ctx, scope, w.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	token, err := srv.signer.Sign(auth.Claims{Kind: auth.KindHuman, Audience: "ahp", Tenant: scope.Tenant, Subject: "compliance-admin"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	code, body := srv.adminCall(t, "ControlDeleteTenant", token, scope, `{"key":"tenant-held"}`)
	if code != http.StatusOK || body["state"] != "completed_with_holds" {
		t.Fatalf("held tenant: %d %v", code, body)
	}
	if _, err = st.GetThread(ctx, scope, held.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = art.Get(ctx, scope, id); err != nil {
		t.Fatal(err)
	}
	for name, call := range map[string]func() error{
		"create":    func() error { _, e := ws.Create(ctx, scope, "late"); return e },
		"candidate": func() error { _, e := ws.CreateCandidate(ctx, scope, w.ID, ""); return e },
		"update":    func() error { _, e := ws.UpdateCandidateFiles(ctx, scope, cand.ID, nil); return e },
		"check":     func() error { return ws.SetCheck(ctx, scope, cand.ID, "check", "passed", "") },
		"member":    func() error { return ws.AddMember(ctx, scope, w.ID, "new-subject") },
		"publish":   func() error { _, e := ws.Publish(ctx, scope, cand.ID, ""); return e },
	} {
		if err = call(); contracts.Code(err) != contracts.ErrForbidden {
			t.Errorf("retained workspace write %s admitted: %v", name, err)
		}
	}
}

func TestTenantDeletionResumesVersionedHookAfterUnknownAcknowledgement(t *testing.T) {
	ctx := context.Background()
	pool, art, scope := cascadeEnv(t, ctx)
	st := state.New(pool)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS public.tenant_hook_data(tenant text,subject text);CREATE TABLE IF NOT EXISTS public.tenant_hook_ops(op_key text PRIMARY KEY);`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO public.tenant_hook_data VALUES($1,'not-in-any-platform-scope'),($2,'foreign')`, scope.Tenant, scope.Tenant+"-other"); err != nil {
		t.Fatal(err)
	}
	var attempts, effects atomic.Int32
	ca := newTestCA(t)
	trusted := x509.NewCertPool()
	trusted.AppendCertsFromPEM(ca.pem)
	hook := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || r.Header.Get("Authorization") != "" {
			http.Error(w, "mTLS required", 403)
			return
		}
		var in struct {
			Tenant  string `json:"tenant"`
			Subject string `json:"subject"`
			Scope   string `json:"deletion_scope"`
			Key     string `json:"operation_key"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.Scope != "tenant" || in.Subject != "" || in.Key == "" {
			http.Error(w, "tenant contract required", 400)
			return
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Error(err)
			http.Error(w, "db", 503)
			return
		}
		defer tx.Rollback(ctx)
		tag, err := tx.Exec(ctx, `INSERT INTO public.tenant_hook_ops VALUES($1) ON CONFLICT DO NOTHING`, in.Key)
		if err != nil {
			t.Error(err)
			http.Error(w, "db", 503)
			return
		}
		if tag.RowsAffected() == 1 {
			if _, err = tx.Exec(ctx, `DELETE FROM public.tenant_hook_data WHERE tenant=$1`, in.Tenant); err != nil {
				t.Error(err)
				http.Error(w, "db", 503)
				return
			}
			effects.Add(1)
		}
		if err = tx.Commit(ctx); err != nil {
			t.Error(err)
			http.Error(w, "db", 503)
			return
		}
		if attempts.Add(1) == 1 {
			http.Error(w, "committed but ack lost", 503)
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"status": "completed", "operation_key": in.Key, "deletion_scope": "tenant"})
	}))
	hook.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: trusted}
	hook.StartTLS()
	defer hook.Close()
	client := hook.Client()
	tr := client.Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{ca.clientCert(t, auth.WorkloadID(scope))}
	client.Transport = tr
	stopped, stop := context.WithCancel(ctx)
	stop()
	cfg := ControlDeletionConfig{ConsumerContext: stopped, TenantOwners: tenantTestOwners(t, pool), HTTPClient: client, ResolveHook: func(context.Context, string, registry.DeletionTarget) (string, error) { return hook.URL + "/a2a", nil }}
	var composed *server
	srv := cascadeServerConfigured(t, st, art, scope, func(s *server) { composed = s }, cfg)
	version, agent, installation := contracts.NewID(), contracts.NewID(), contracts.NewID()
	if _, err := pool.Exec(ctx, `INSERT INTO ahp_registry.versions(id,agent_id,digest,manifest) VALUES($1,$2,$3,$4)`, version, agent, "sha256:"+strings.Repeat("a", 64), `{"state":{"own_tables":true,"delete_hook":"/delete"}}`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO ahp_registry.installations(id,tenant,agent_id,deployment,environment,version_id,idem_key,request_digest) VALUES($1,$2,$3,'custom','dev',$4,$1,'fixture')`, installation, scope.Tenant, agent, version); err != nil {
		t.Fatal(err)
	}
	th, err := st.CreateThread(ctx, scope, "before", "before")
	if err != nil {
		t.Fatal(err)
	}
	token, err := srv.signer.Sign(auth.Claims{Kind: auth.KindHuman, Audience: "ahp", Tenant: scope.Tenant, Subject: "compliance-admin"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	code, body := srv.adminCall(t, "ControlDeleteTenant", token, scope, `{"key":"tenant-hook"}`)
	if code != http.StatusServiceUnavailable || body["state"] != "pending" {
		t.Fatalf("lost ack: %d %v", code, body)
	}
	if _, err = st.GetThread(ctx, scope, th.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("State was not committed before crash: %v", err)
	}
	job, err := st.GetControlDeletion(ctx, scope.Tenant, "compliance-admin", "tenant-hook")
	if err != nil {
		t.Fatal(err)
	}
	cfg.ResolveHook = func(context.Context, string, registry.DeletionTarget) (string, error) {
		return "", errors.New("must use persisted endpoint")
	}
	restarted := stateDeletionCoordinator{state: state.New(pool), cascade: composed.cascade, control: cfg}
	if err = restarted.resumeControlDeletion(ctx, job); err != nil {
		t.Fatal(err)
	}
	job, err = st.GetControlDeletion(ctx, scope.Tenant, "compliance-admin", "tenant-hook")
	if err != nil || !job.Completed {
		t.Fatalf("not completed: %v %v", job.Completed, err)
	}
	if attempts.Load() != 2 || effects.Load() != 1 {
		t.Fatalf("hook was not idempotent: attempts=%d effects=%d", attempts.Load(), effects.Load())
	}
	var own, foreign int
	if err = pool.QueryRow(ctx, `SELECT count(*) FILTER(WHERE tenant=$1),count(*) FILTER(WHERE tenant=$2) FROM public.tenant_hook_data`, scope.Tenant, scope.Tenant+"-other").Scan(&own, &foreign); err != nil || own != 0 || foreign != 1 {
		t.Fatalf("tenant custom data: own=%d foreign=%d err=%v", own, foreign, err)
	}
}

func TestTenantDeletionMissingOwnerRefusesBeforeRevocation(t *testing.T) {
	ctx := context.Background()
	pool, art, scope := cascadeEnv(t, ctx)
	st := state.New(pool)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	stopped, stop := context.WithCancel(ctx)
	stop()
	srv := cascadeServerConfigured(t, st, art, scope, nil, ControlDeletionConfig{ConsumerContext: stopped})
	token, err := srv.signer.Sign(auth.Claims{Kind: auth.KindHuman, Audience: "ahp", Tenant: scope.Tenant, Subject: "compliance-admin"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	code, body := srv.adminCall(t, "ControlDeleteTenant", token, scope, `{"key":"missing-owner"}`)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("missing owner reported completion: %d %v", code, body)
	}
	if _, err = st.CreateThread(ctx, scope, "still-live", "still-live"); err != nil {
		t.Fatalf("tenant changed before full owner admission: %v", err)
	}
}

func TestTenantDeletionPreservesUnresolvedStateEffect(t *testing.T) {
	ctx := context.Background()
	pool, art, scope := cascadeEnv(t, ctx)
	st := state.New(pool)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	th, err := st.CreateThread(ctx, scope, "unknown", "unknown")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO ahp_state.runs(id,thread_id,task_id,status,sequence,payload,body_digest,idem_key) VALUES('unknown-'||$1,$1,'external','running',1,'{}','digest','run');`, th.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO ahp_state.intents(id,run_id,node,call,kind,target,digest,status) VALUES($1,'unknown-'||$1,'node','call','external','target','digest','unknown')`, th.ID); err != nil {
		t.Fatal(err)
	}
	stopped, stop := context.WithCancel(ctx)
	stop()
	srv := cascadeServerConfigured(t, st, art, scope, nil, ControlDeletionConfig{ConsumerContext: stopped, TenantOwners: tenantTestOwners(t, pool)})
	token, err := srv.signer.Sign(auth.Claims{Kind: auth.KindHuman, Audience: "ahp", Tenant: scope.Tenant, Subject: "compliance-admin"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	code, body := srv.adminCall(t, "ControlDeleteTenant", token, scope, `{"key":"unknown"}`)
	if code != http.StatusServiceUnavailable || body["state"] != "pending" {
		t.Fatalf("unknown effect deleted: %d %v", code, body)
	}
	job, err := st.GetControlDeletion(ctx, scope.Tenant, "compliance-admin", "unknown")
	if err != nil || job.Completed {
		t.Fatalf("false completion: %+v %v", job, err)
	}
	var status string
	if err = pool.QueryRow(ctx, `SELECT status FROM ahp_state.intents WHERE id=$1`, th.ID).Scan(&status); err != nil || status != "unknown" {
		t.Fatalf("lost effect evidence: %q %v", status, err)
	}
}
