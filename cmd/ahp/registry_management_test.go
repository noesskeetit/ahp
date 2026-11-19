package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Real PG, real HTTPS/mTLS, real session lookup. The IAM users are synthetic.
func TestRegistryManagementLiveAuthority(t *testing.T) {
	testRegistryManagementLiveAuthority(t, false)
}

func TestSyntheticRegistryManagementHonorsResourceRevoke(t *testing.T) {
	testRegistryManagementLiveAuthority(t, true)
}

func testRegistryManagementLiveAuthority(t *testing.T, synthetic bool) {
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("isolated PostgreSQL required")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ids, store := identity.New(pool), registry.New(pool)
	policy := identity.NewSyntheticResourcePolicy(pool)
	if synthetic {
		ids = identity.New(pool, identity.WithResourceEvaluator(policy))
	}
	for _, m := range []func(context.Context) error{ids.Migrate, store.Migrate} {
		if err = m(ctx); err != nil {
			t.Fatal(err)
		}
	}
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := auth.NewSigner("profile-test", "key", priv)
	v := auth.NewVerifier("profile-test", map[string]ed25519.PublicKey{"key": pub})
	h, err := registry.NewHTTPHandler(store, v, ids.Authorize)
	if err != nil {
		t.Fatal(err)
	}
	// Mount uses this same function; current user rights are checked by Identity.
	if err = attachRegistryManagement(h, v, ids, []string{"space"}, "ahp-browser"); err != nil {
		t.Fatal(err)
	}
	ca := newMountCA(t)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	front := httptest.NewUnstartedServer(h)
	front.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	front.StartTLS()
	defer front.Close()
	tenant := "management-" + strings.ToLower(contracts.NewID())
	tr := front.Client().Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{ca.clientCert(t, auth.WorkloadID(contracts.Scope{Tenant: tenant, Deployment: "space"}))}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr}
	for _, tc := range []struct {
		name               string
		roles, permissions []string
		revoke             bool
		revokeResource     bool
		want               int
	}{
		{"user", []string{"user"}, []string{"registry"}, false, false, 403},
		{"admin", []string{"admin"}, []string{"registry"}, false, false, 200},
		{"permission", []string{"user"}, []string{"registry:admin"}, false, false, 200},
		{"revoked", []string{"admin"}, []string{"registry"}, true, false, 403},
		{"iam-role", []string{"admin"}, []string{"registry"}, false, true, 200},
		{"iam-permission", []string{"user"}, []string{"registry:admin"}, false, true, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u := identity.BrowserUser{UserID: "usr-" + contracts.NewID(), Tenant: tenant, Subject: tc.name}
			if err := ids.SeedBrowserUser(ctx, u, u.UserID, "test-password-long-enough", tc.roles, tc.permissions); err != nil {
				t.Fatal(err)
			}
			if synthetic {
				for _, resource := range []identity.Resource{{Kind: "tenant", ID: tenant}, {Kind: "deployment", ID: "space"}} {
					if _, err := policy.PutRule(ctx, identity.ResourceRule{Tenant: tenant, Subject: u.Subject, Resource: resource, Action: registry.AdminCapability, Allowed: true}, 0); err != nil {
						t.Fatal(err)
					}
				}
			}
			sess, err := ids.StartBrowserSession(ctx, u.UserID)
			if err != nil {
				t.Fatal(err)
			}
			jti := contracts.NewID()
			if err = ids.BindBrowserToken(ctx, jti, sess); err != nil {
				t.Fatal(err)
			}
			tok, err := signer.Sign(auth.Claims{Kind: auth.KindHuman, Audience: "ahp-browser", Tenant: tenant, Subject: u.Subject, JTI: jti}, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if tc.revoke {
				if err = ids.RevokeBrowserSession(ctx, sess.ID); err != nil {
					t.Fatal(err)
				}
			}
			want := tc.want
			if synthetic && tc.revokeResource {
				if _, err := policy.PutRule(ctx, identity.ResourceRule{Tenant: tenant, Subject: u.Subject, Resource: identity.Resource{Kind: "deployment", ID: "space"}, Action: registry.AdminCapability, Allowed: false}, 1); err != nil {
					t.Fatal(err)
				}
				want = http.StatusForbidden
			}
			body := `{"route":"` + tc.name + `","expected_revision":0,"document":{"model":"qwen"}}`
			req, _ := http.NewRequest("POST", front.URL+"/v1/registry/PutModelRoute", strings.NewReader(body))
			req.Header.Set("Authorization", "Bearer "+tok)
			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			b, _ := io.ReadAll(resp.Body)
			if resp.StatusCode != want {
				t.Fatalf("status=%d want=%d body=%s", resp.StatusCode, want, b)
			}
		})
	}
}
