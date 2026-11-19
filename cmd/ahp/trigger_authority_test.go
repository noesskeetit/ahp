package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/model/secretstore"
	"github.com/ahp-platform/ahp/internal/triggers"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

type triggerDispatchRecorder struct{ calls []triggers.FireRequest }

func (d *triggerDispatchRecorder) Fire(_ context.Context, _ contracts.Scope, r triggers.FireRequest) (triggers.FireResult, error) {
	d.calls = append(d.calls, r)
	return triggers.FireResult{TaskID: "fired-task"}, nil
}

func TestTriggerHTTPReferencesBindToVerifiedTask(t *testing.T) {
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires isolated PostgreSQL")
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
	scope := contracts.Scope{Tenant: "trg-" + suffix, Deployment: "runner", Subject: "alice"}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner("trigger-test", "key", priv)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier("trigger-test", map[string]ed25519.PublicKey{"key": pub})
	for _, task := range []string{"own", "foreign"} {
		_, err = ids.CreateRoot(ctx, scope, identity.RootRequest{Task: task, Context: suffix + "-" + task, Deployment: scope.Deployment, Capabilities: []string{"triggers", "execution"}, TTL: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
	}
	token, err := ids.MintTask(ctx, scope, "own", signer, "ahp", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	svc, err := triggers.New(ctx, pool, "ahp_trg_bind_"+suffix, triggers.Config{Issuer: identityTriggerAuthority{store: ids, signer: signer, verifier: verifier}, Dispatcher: &triggerDispatchRecorder{}, Secrets: &scopedVaultSecretResolver{}, Tasks: identityTriggerAuthority{store: ids, signer: signer, verifier: verifier}})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := triggers.NewHTTPHandler(svc, verifier, ids.Authorize)
	if err != nil {
		t.Fatal(err)
	}
	ca := newMountCA(t)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	front := httptest.NewUnstartedServer(handler)
	front.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	front.StartTLS()
	defer front.Close()
	tr := front.Client().Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{ca.clientCert(t, auth.WorkloadID(scope))}
	front.Client().Transport = tr
	for _, op := range []string{"CreateSchedule", "CreateWebhook"} {
		for _, which := range []string{"parent_task", "delegation_ref", "omitted"} {
			body := map[string]any{"target": "agent:echo", "interval": int64(time.Hour), "max_fires": 1, "secret_ref": "hook"}
			if op == "CreateSchedule" {
				delete(body, "secret_ref")
			} else {
				delete(body, "interval")
			}
			if which != "omitted" {
				body["parent_task"] = "own"
				body["delegation_ref"] = "own"
				body[which] = "foreign"
			}
			data, _ := json.Marshal(body)
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, front.URL+"/v1/triggers/"+op, strings.NewReader(string(data)))
			req.Header.Set("Authorization", "Bearer "+token)
			req.Header.Set("Content-Type", "application/json")
			res, err := front.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			var created triggers.Trigger
			json.NewDecoder(res.Body).Decode(&created)
			res.Body.Close()
			if which != "omitted" {
				if res.StatusCode != http.StatusForbidden {
					t.Errorf("%s accepted foreign %s: HTTP%d", op, which, res.StatusCode)
				}
			} else if res.StatusCode != http.StatusOK || created.ParentTask != "own" || created.DelegationRef != "own" {
				t.Errorf("%s did not bind omitted authority: HTTP%d parent=%s ref=%s", op, res.StatusCode, created.ParentTask, created.DelegationRef)
			}
		}
	}
	// FireNow is a user-scoped nudge, not the global background scheduler.
	foreignScope := scope
	foreignScope.Subject = "bob"
	_, err = ids.CreateRoot(ctx, foreignScope, identity.RootRequest{Task: "own", Context: suffix + "-bob", Deployment: scope.Deployment, Capabilities: []string{"triggers", "execution"}, TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	foreignTrigger, err := svc.CreateSchedule(ctx, foreignScope, triggers.CreateScheduleSpec{ParentTask: "own", DelegationRef: "own", Target: "agent:echo", Interval: time.Hour, MaxFires: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{"Get"} {
		data, _ := json.Marshal(map[string]string{"trigger_id": foreignTrigger.TriggerID})
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, front.URL+"/v1/triggers/"+op, strings.NewReader(string(data)))
		req.Header.Set("Authorization", "Bearer "+token)
		res, err := front.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != http.StatusNotFound {
			t.Errorf("%s crossed subject: HTTP%d", op, res.StatusCode)
		}
	}
	ownList, err := svc.List(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	for _, trig := range ownList {
		if trig.Subject != scope.Subject {
			t.Error("List exposed another subject")
		}
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, front.URL+"/v1/triggers/FireNow", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer "+token)
	res, err := front.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Fired int `json:"fired"`
	}
	json.NewDecoder(res.Body).Decode(&result)
	res.Body.Close()
	if res.StatusCode != http.StatusOK || result.Fired != 1 {
		t.Errorf("FireNow count crossed scope: HTTP%d fired=%d", res.StatusCode, result.Fired)
	}
	stored, err := svc.Get(ctx, foreignScope, foreignTrigger.TriggerID)
	if err != nil || stored.FiredCount != 0 || stored.Status != triggers.StatusActive {
		t.Errorf("FireNow/Revoke mutated foreign trigger: %+v %v", stored, err)
	}
	data, _ := json.Marshal(map[string]string{"trigger_id": foreignTrigger.TriggerID})
	revokeReq, _ := http.NewRequestWithContext(ctx, http.MethodPost, front.URL+"/v1/triggers/Revoke", strings.NewReader(string(data)))
	revokeReq.Header.Set("Authorization", "Bearer "+token)
	revoked, err := front.Client().Do(revokeReq)
	if err != nil {
		t.Fatal(err)
	}
	revoked.Body.Close()
	if revoked.StatusCode != http.StatusNotFound {
		t.Errorf("Revoke crossed subject: HTTP%d", revoked.StatusCode)
	}
}

func TestTriggerLiveIdentityReissueAndRevocationStopsSchedule(t *testing.T) {
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires isolated PostgreSQL")
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
	scope := contracts.Scope{Tenant: "trg-" + suffix, Deployment: "worker", Subject: "owner"}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner("triggers-authority", "key", priv)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier("triggers-authority", map[string]ed25519.PublicKey{"key": pub})
	authority := identityTriggerAuthority{store: ids, signer: signer, verifier: verifier}
	grant, err := ids.CreateRoot(ctx, scope, identity.RootRequest{Task: "live", Context: suffix + "-live", Deployment: scope.Deployment, Capabilities: []string{"triggers", "execution"}, TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	token, exp, err := authority.IssueToken(ctx, scope, "live", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := verifier.Verify(token, "ahp", auth.KindTask, auth.WorkloadID(scope))
	if err != nil || claims.Task != "live" || claims.Context != grant.Context || claims.Grant != grant.ID || exp.Unix() != claims.ExpiresAt || exp.After(grant.ExpiresAt) {
		t.Fatal("fire token not bound to the referenced live grant or expiry")
	}
	if err := ids.Authorize(ctx, claims, "execution"); err != nil {
		t.Fatal(err)
	}
	for _, ref := range []string{"missing", ""} {
		if _, _, err := authority.IssueToken(ctx, scope, ref, time.Minute); err == nil {
			t.Errorf("unresolved reference %q minted", ref)
		}
	}
	foreign := scope
	foreign.Subject = "other"
	if _, _, err := authority.IssueToken(ctx, foreign, "live", time.Minute); err == nil {
		t.Fatal("foreign owner resolved grant")
	}
	var grantExpires time.Time
	for _, tc := range []struct {
		task string
		ttl  time.Duration
		caps []string
	}{{"limited", time.Hour, []string{"triggers"}}, {"expired", 40 * time.Millisecond, []string{"execution"}}} {
		created, err := ids.CreateRoot(ctx, scope, identity.RootRequest{Task: tc.task, Context: suffix + tc.task, Deployment: scope.Deployment, Capabilities: tc.caps, TTL: tc.ttl})
		if err != nil {
			t.Fatal(err)
		}
		if tc.task == "expired" {
			grantExpires = created.ExpiresAt
		}
	}
	// Identity rounds grant lifetimes to SQL seconds; wait for the stored
	// deadline rather than the smaller duration originally requested.
	time.Sleep(time.Until(grantExpires) + 20*time.Millisecond)
	for _, ref := range []string{"limited", "expired"} {
		if _, _, err := authority.IssueToken(ctx, scope, ref, time.Minute); err == nil {
			t.Errorf("unusable %s grant minted", ref)
		}
		if active, err := authority.IsTaskActive(ctx, scope, ref); err != nil || active {
			t.Errorf("unusable %s grant active=%v err=%v", ref, active, err)
		}
	}
	dispatcher := &triggerDispatchRecorder{}
	svc, err := triggers.New(ctx, pool, "ahp_trg_live_"+suffix, triggers.Config{Issuer: authority, Dispatcher: dispatcher, Secrets: &scopedVaultSecretResolver{}, Tasks: authority, FireTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	var scheduled []triggers.Trigger
	for _, delegated := range []bool{false, true} {
		created, err := svc.CreateSchedule(ctx, scope, triggers.CreateScheduleSpec{ParentTask: "live", DelegationRef: "live", Target: "agent:echo", Interval: time.Millisecond, MaxFires: 3, Delegated: delegated})
		if err != nil {
			t.Fatal(err)
		}
		scheduled = append(scheduled, created)
	}
	if fired, err := svc.ProcessDue(ctx, time.Now(), 10); err != nil || fired != 2 {
		t.Fatalf("initial fires=%d err=%v", fired, err)
	}
	if len(dispatcher.calls) != 2 {
		t.Fatal("no actual dispatcher calls")
	}
	for _, fire := range dispatcher.calls {
		c, err := verifier.Verify(fire.Token, "ahp", auth.KindTask, auth.WorkloadID(scope))
		if err != nil || c.Task != "live" {
			t.Fatal("dispatch received a fabricated token")
		}
		if err := ids.Authorize(ctx, c, "execution"); err != nil {
			t.Fatal(err)
		}
	}
	if err := ids.CancelSubtree(ctx, scope, "live"); err != nil {
		t.Fatal(err)
	}
	if fired, err := svc.ProcessDue(ctx, time.Now().Add(time.Minute), 10); err != nil || fired != 0 {
		t.Fatalf("revoked grant fired=%d err=%v", fired, err)
	}
	if len(dispatcher.calls) != 2 {
		t.Fatal("revoked authority reached dispatcher")
	}
	for _, created := range scheduled {
		stored, err := svc.Get(ctx, scope, created.TriggerID)
		if err != nil || stored.Status != triggers.StatusRevoked {
			t.Fatalf("schedule not durably stopped: %+v %v", stored, err)
		}
		data, _ := json.Marshal(stored)
		for _, fire := range dispatcher.calls {
			if strings.Contains(string(data), fire.Token) {
				t.Fatal("trigger persisted bearer")
			}
		}
	}
}

func TestTriggerScopedVaultSecretResolver(t *testing.T) {
	var paths []string
	vault := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Vault-Token") != "scoped-vault-test" {
			t.Error("missing Vault authentication")
		}
		paths = append(paths, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		secret := "alice-secret"
		if strings.Contains(r.URL.Path, "/bob/") {
			secret = "bob-secret"
		}
		json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"data": map[string]string{"secret": secret}}})
	}))
	defer vault.Close()
	store, err := secretstore.New(vault.URL, "scoped-vault-test")
	if err != nil {
		t.Fatal(err)
	}
	resolver := &scopedVaultSecretResolver{store: store, paths: map[string]string{"hook": "secret/data/triggers/{tenant}/{deployment}/{subject}/hook"}}
	scope := contracts.Scope{Tenant: "tenant", Deployment: "worker", Subject: "alice"}
	first, err := resolver.Resolve(context.Background(), scope, "hook")
	if err != nil || string(first) != "alice-secret" {
		t.Fatal("first owner resolution failed")
	}
	scope.Subject = "bob"
	second, err := resolver.Resolve(context.Background(), scope, "hook")
	if err != nil || string(second) != "bob-secret" {
		t.Fatal("second owner resolution failed")
	}
	if len(paths) != 2 || paths[0] == paths[1] || !strings.Contains(paths[1], "/tenant/worker/bob/") {
		t.Fatal("Vault paths did not follow verified owner scope")
	}
	for _, ref := range []string{"missing", "secret/data/triggers/tenant/worker/alice/hook"} {
		if _, err := resolver.Resolve(context.Background(), scope, ref); err == nil {
			t.Fatal("raw or unknown path accepted")
		}
	}
	resolver.paths["ambiguous"] = "secret/data/{tenant}{subject}/{deployment}"
	if _, err := resolver.Resolve(context.Background(), scope, "ambiguous"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("ambiguous owner path accepted: %v", err)
	}
	resolver.paths["unscoped"] = "secret/data/global"
	if _, err := resolver.Resolve(context.Background(), scope, "unscoped"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("unscoped secret accepted: %v", err)
	}
	scope.Subject = ".."
	if _, err := resolver.Resolve(context.Background(), scope, "hook"); err == nil {
		t.Fatal("owner traversal accepted")
	}
	if len(paths) != 2 {
		t.Fatal("rejected reference reached Vault")
	}
	t.Setenv("AHP_TRIGGERS_VAULT_PATHS", "")
	t.Setenv("AHP_TRIGGERS_TEST_SECRET", "must-never-be-used")
	disabled, err := triggerSecretResolverFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	scope.Subject = "alice"
	if _, err := disabled.Resolve(context.Background(), scope, "hook"); err == nil {
		t.Fatal("ambient test secret bypassed Vault")
	}
}
