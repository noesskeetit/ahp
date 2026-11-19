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
	"encoding/pem"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/identityapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSyntheticOIDCConfiguredHTTPLoginAndIAMRevoke(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if os.Getenv("AHP_TEST_DATABASE_URL") == "" {
		t.Fatal("real PostgreSQL required")
	}
	pool, err := pgxpool.New(ctx, os.Getenv("AHP_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	var issuer *httptest.Server
	issuer = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/discovery" {
			json.NewEncoder(w).Encode(map[string]string{"issuer": issuer.URL, "jwks_uri": issuer.URL + "/jwks"})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]string{"kty": "OKP", "crv": "Ed25519", "kid": "oidc", "alg": "EdDSA", "use": "sig", "x": base64.RawURLEncoding.EncodeToString(pub)}}})
	}))
	defer issuer.Close()
	caFile := filepath.Join(t.TempDir(), "issuer.pem")
	if err = os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: issuer.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{"AHP_AUTH_PROFILE": "synthetic-oidc", "AHP_IAM_PROFILE": "synthetic", "AHP_OIDC_ISSUER": issuer.URL, "AHP_OIDC_DISCOVERY_URL": issuer.URL + "/discovery", "AHP_OIDC_AUDIENCE": "public-ahp", "AHP_OIDC_CA_FILE": caFile} {
		t.Setenv(name, value)
	}
	owner := &server{pool: pool, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if err = owner.configureIdentityProfile(); err != nil {
		t.Fatal(err)
	}
	ids := owner.identityStore()
	if err = ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	tenant := "oidc-" + randHexTest()
	subject := "alice"
	signer, _ := auth.NewSigner("ahp", "internal", key)
	verifier := auth.NewVerifier("ahp", map[string]ed25519.PublicKey{"internal": pub})
	h := identityapi.Handler(identityapi.Config{Store: ids, Auth: owner.authenticator, Signer: signer, Verifier: verifier, Audience: "ahp", Browser: &identityapi.BrowserConfig{}})
	front := httptest.NewTLSServer(h)
	defer front.Close()
	assertion := func(changes map[string]any) string {
		c := map[string]any{"iss": issuer.URL, "aud": "public-ahp", "sub": subject, "tenant": tenant, "token_use": "access", "exp": time.Now().Add(time.Minute).Unix()}
		for k, v := range changes {
			c[k] = v
		}
		hb, _ := json.Marshal(map[string]string{"alg": "EdDSA", "typ": "at+jwt", "kid": "oidc"})
		pb, _ := json.Marshal(c)
		input := base64.RawURLEncoding.EncodeToString(hb) + "." + base64.RawURLEncoding.EncodeToString(pb)
		return input + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(key, []byte(input)))
	}
	login := func(changes map[string]any, inject bool, want int) {
		proof := assertion(changes)
		body := map[string]string{"access_token": proof}
		if inject {
			body["subject"] = "mallory"
		}
		b, _ := json.Marshal(body)
		r, _ := http.NewRequestWithContext(ctx, "POST", front.URL+"/u-ahp/v1/auth/login", bytes.NewReader(b))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-AHP-Session", "1")
		resp, e := front.Client().Do(r)
		if e != nil {
			t.Fatal(e)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != want {
			t.Fatalf("login=%d want=%d: %s", resp.StatusCode, want, raw)
		}
		if strings.Contains(string(raw), proof) {
			t.Fatal("issuer proof leaked to browser")
		}
		if want == 200 && len(resp.Cookies()) == 0 {
			t.Fatal("OIDC login did not create browser session")
		}
	}
	login(nil, false, 401)
	rule := identity.ResourceRule{Tenant: tenant, Subject: subject, Resource: identity.Resource{Kind: "tenant", ID: tenant}, Action: "login", Allowed: true}
	policy := owner.iam.(*identity.SyntheticResourcePolicy)
	if _, err = policy.PutRule(ctx, rule, 0); err != nil {
		t.Fatal(err)
	}
	login(nil, false, 200)
	for _, claims := range []map[string]any{{"aud": "foreign"}, {"iss": "https://foreign.example"}, {"token_use": "refresh"}, {"token_kind": "task", "task": "task"}} {
		login(claims, false, 401)
	}
	login(nil, true, 400)
	rule.Allowed = false
	if _, err = policy.PutRule(ctx, rule, 1); err != nil {
		t.Fatal(err)
	}
	login(nil, false, 401)
	resp, err := front.Client().Get(front.URL + "/u-ahp/v1/auth/config")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(raw), "synthetic-oidc") {
		t.Fatal("HTTP profile is not explicitly synthetic")
	}
}

func TestSyntheticIdentityExactSPIFFEHTTPGate(t *testing.T) {
	ca := newMountCA(t)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	cfg := identityapi.Config{RequireExactWorkloadIDs: true, ExchangerDeployments: []string{"space"}, ExchangerWorkloadIDs: []string{"spiffe://ahp/tenant/approved/deployment/space"}, ControlExchange: true, Verifier: auth.NewVerifier("ahp", nil)}
	front := httptest.NewUnstartedServer(identityapi.Handler(cfg))
	front.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	front.StartTLS()
	defer front.Close()
	for _, tc := range []struct {
		tenant string
		body   string
		want   int
	}{{"approved", `{}`, 400}, {"foreign", `{}`, 403}, {"approved", `{"human_token":"invalid","task":"task"}`, 403}} {
		client := *front.Client()
		tr := client.Transport.(*http.Transport).Clone()
		tr.TLSClientConfig.Certificates = []tls.Certificate{ca.clientCert(t, "spiffe://ahp/tenant/"+tc.tenant+"/deployment/space")}
		client.Transport = tr
		r, _ := http.NewRequest("POST", front.URL+"/v1/identity/control/exchange", strings.NewReader(tc.body))
		r.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Fatalf("tenant=%s got=%d want=%d", tc.tenant, resp.StatusCode, tc.want)
		}
	}
}
