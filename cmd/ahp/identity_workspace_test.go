//go:build ahp_data

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestSyntheticProjectHTTPRevokesExistingTaskJWT(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if os.Getenv("AHP_TEST_DATABASE_URL") == "" || os.Getenv("AHP_S3_ENDPOINT") == "" {
		t.Fatal("real PostgreSQL/MinIO required")
	}
	pool, e := pgxpool.New(ctx, os.Getenv("AHP_TEST_DATABASE_URL"))
	if e != nil {
		t.Fatal(e)
	}
	defer pool.Close()
	t.Setenv("AHP_WORKSPACE_USER_EDGE_DEPLOYMENTS", "agent")
	t.Setenv("AHP_WORKSPACE_USER_AUDIENCE", "ahp-browser")
	t.Setenv("AHP_DEV_JWT_SEED", hex.EncodeToString(make([]byte, 32)))
	policy := identity.NewSyntheticResourcePolicy(pool)
	s := &server{pool: pool, iam: policy, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ids := s.identityStore()
	if e = ids.Migrate(ctx); e != nil {
		t.Fatal(e)
	}
	sc := contracts.Scope{Tenant: "project-http-" + randHexTest(), Subject: "owner", Deployment: "agent"}
	for _, r := range []identity.Resource{{Kind: "tenant", ID: sc.Tenant}, {Kind: "deployment", ID: sc.Deployment}} {
		if _, e = policy.PutRule(ctx, identity.ResourceRule{Tenant: sc.Tenant, Subject: sc.Subject, Resource: r, Action: "workspace", Allowed: true}, 0); e != nil {
			t.Fatal(e)
		}
	}
	task := "project-task-" + randHexTest()
	if _, e = ids.CreateRoot(ctx, sc, identity.RootRequest{Task: task, Context: task, Deployment: sc.Deployment, Capabilities: []string{"workspace"}, TTL: time.Minute}); e != nil {
		t.Fatal(e)
	}
	_, signer := devIdentityAuth()
	token, e := ids.MintTask(ctx, sc, task, signer, "ahp", time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	_, handler, e := s.buildDataHandler(ctx)
	if e != nil {
		t.Fatal(e)
	}
	ca := newMountCA(t)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	front := httptest.NewUnstartedServer(handler)
	front.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	front.StartTLS()
	defer front.Close()
	tr := front.Client().Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{ca.clientCert(t, auth.WorkloadID(sc))}
	front.Client().Transport = tr
	call := func(op string, want int) map[string]any {
		b, _ := json.Marshal(map[string]string{"workspace": "project"})
		r, _ := http.NewRequestWithContext(ctx, "POST", front.URL+"/v1/workspace/"+op, bytes.NewReader(b))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Authorization", "Bearer "+token)
		resp, e := front.Client().Do(r)
		if e != nil {
			t.Fatal(e)
		}
		defer resp.Body.Close()
		var result map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&result)
		if resp.StatusCode != want {
			t.Fatalf("%s status=%d want=%d result=%v", op, resp.StatusCode, want, result)
		}
		return result
	}
	w := call("Create", 200)
	id, ok := w["id"].(string)
	if !ok {
		t.Fatal("missing workspace owner id")
	}
	call("GetWorkspace", 200)
	if _, e = policy.PutRule(ctx, identity.ResourceRule{Tenant: sc.Tenant, Subject: sc.Subject, Resource: identity.Resource{Kind: "project", ID: id}, Action: "read", Allowed: false}, 0); e != nil {
		t.Fatal(e)
	}
	call("GetWorkspace", 403)
	// The same resource policy must also govern a human bearer carried by the
	// sanctioned edge, even while session, role and permissions remain live.
	if _, e = policy.PutRule(ctx, identity.ResourceRule{Tenant: sc.Tenant, Subject: sc.Subject, Resource: identity.Resource{Kind: "project", ID: id}, Action: "read", Allowed: true}, 1); e != nil {
		t.Fatal(e)
	}
	user := identity.BrowserUser{UserID: "user-" + randHexTest(), Tenant: sc.Tenant, Subject: sc.Subject}
	if e = ids.SeedBrowserUser(ctx, user, "login-"+randHexTest(), "synthetic-test-password", nil, []string{"workspace"}); e != nil {
		t.Fatal(e)
	}
	session, e := ids.StartBrowserSession(ctx, user.UserID)
	if e != nil {
		t.Fatal(e)
	}
	jti := "jti-" + randHexTest()
	if e = ids.BindBrowserToken(ctx, jti, session); e != nil {
		t.Fatal(e)
	}
	token, e = signer.Sign(auth.Claims{Kind: auth.KindHuman, Tenant: sc.Tenant, Subject: sc.Subject, Audience: "ahp-browser", JTI: jti}, time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	call("GetWorkspace", 200)
	tenantRule := identity.ResourceRule{Tenant: sc.Tenant, Subject: sc.Subject, Resource: identity.Resource{Kind: "tenant", ID: sc.Tenant}, Action: "workspace", Allowed: false}
	if _, e = policy.PutRule(ctx, tenantRule, 1); e != nil {
		t.Fatal(e)
	}
	call("GetWorkspace", 403)
	tenantRule.Allowed = true
	if _, e = policy.PutRule(ctx, tenantRule, 2); e != nil {
		t.Fatal(e)
	}
	call("GetWorkspace", 200)
	if _, e = policy.PutRule(ctx, identity.ResourceRule{Tenant: sc.Tenant, Subject: sc.Subject, Resource: identity.Resource{Kind: "deployment", ID: sc.Deployment}, Action: "workspace", Allowed: false}, 1); e != nil {
		t.Fatal(e)
	}
	call("GetWorkspace", 403)

	// The cryptographically valid JWT is unchanged; the fresh project policy,
	// resolved by owner ID rather than a caller-chosen resource, causes refusal.
}
