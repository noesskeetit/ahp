package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/statejournal"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestSignedStateCapabilities(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner("state-test", "test", private)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier("state-test", map[string]ed25519.PublicKey{"test": public})
	claims := auth.Claims{Kind: auth.KindTask, Subject: "user", Tenant: "new-tenant", Deployment: "agent",
		Task: "worker", Root: "worker", Context: "ctx", Grant: "bound-grant", Audience: "ahp",
		Capabilities: []string{"state", "state.shared"}}
	token, err := signer.Sign(claims, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := verifier.Verify(token, "ahp", auth.KindTask, auth.WorkloadID(claims.Scope()))
	if err != nil {
		t.Fatal(err)
	}
	authorize, err := grantAuthorizer(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if err := authorize(context.Background(), verified, "state.shared"); err != nil {
		t.Fatal(err)
	}
	if err := authorize(context.Background(), verified, "state.retention"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("unsigned capability accepted: %v", err)
	}
	if _, err := verifier.Verify(token, "ahp", auth.KindTask, "spiffe://ahp/tenant/other/deployment/agent"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("workload mismatch accepted: %v", err)
	}
}

func TestGrantFileReloadDeniesRevokedScope(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	path := filepath.Join(t.TempDir(), "grants.json")
	write := func(id string) {
		t.Helper()
		data := []byte(`[{"tenant_id":"tenant","deployment_id":"dep","subject_id":"user","grant_id":"` +
			id + `","capabilities":["state"]}]`)
		if err := os.WriteFile(path+".tmp", data, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(path+".tmp", path); err != nil {
			t.Fatal(err)
		}
	}
	write("old")
	authorize, err := grantAuthorizer(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	claims := auth.Claims{Tenant: "tenant", Deployment: "dep", Subject: "user", Grant: "old"}
	if err := authorize(ctx, claims, "state"); err != nil {
		t.Fatal(err)
	}
	write("new")
	newer := claims
	newer.Grant = "new"
	deadline := time.Now().Add(4 * time.Second)
	for authorize(ctx, newer, "state") != nil {
		if time.Now().After(deadline) {
			t.Fatal("new grant not loaded")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := authorize(ctx, claims, "state"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("revoked grant remains usable: %v", err)
	}
}

func TestStateCoordinatorDeniesOfflineGrantPolicy(t *testing.T) {
	scope := contracts.Scope{Tenant: "tenant", Deployment: "coord", Subject: "service"}
	workload := auth.WorkloadID(scope)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner("state-test", "grant-test", priv)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier("state-test", map[string]ed25519.PublicKey{"grant-test": pub})
	const grantID = "coordinator"
	token, err := signer.Sign(auth.Claims{
		Kind: auth.KindTask, Audience: "ahp", Tenant: scope.Tenant, Deployment: scope.Deployment, Subject: scope.Subject,
		Task: "coord", Root: "coord", Context: "state-test", Grant: grantID, Capabilities: []string{"state", "state.coordinate"},
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	uri, err := url.Parse(workload)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "grants.json")
	if err := os.WriteFile(path, []byte(`[{"tenant_id":"tenant","deployment_id":"coord","subject_id":"service","grant_id":"coordinator","capabilities":["state","state.coordinate"]}]`), 0600); err != nil {
		t.Fatal(err)
	}
	for _, grantFile := range []string{"", path} {
		env := map[string]string{"AHP_STATE_GRANTS_FILE": grantFile, "AHP_STATE_COORDINATOR_WORKLOAD_IDS": workload}
		lookup := func(key string) string { return env[key] }
		authorize, identityPool, err := stateAuthorizer(t.Context(), lookup)
		if err != nil {
			t.Fatal(err)
		}
		handler, err := stateHandler(statejournal.New(nil), verifier, authorize, identityPool != nil, lookup)
		if err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			path string
			want int
		}{
			{"/internal/v1/state/vm-reconcile?status=invalid", http.StatusBadRequest},
			{"/internal/v1/state/coordinator/vm-reconcile?status=invalid", http.StatusForbidden},
		} {
			r := httptest.NewRequest(http.MethodGet, tc.path, nil)
			r.Header.Set("Authorization", "Bearer "+token)
			r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{URIs: []*url.URL{uri}}}}}
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Code != tc.want {
				t.Errorf("offline route %s returned %d, want %d", tc.path, w.Code, tc.want)
			}
		}
	}
}

func TestStateToolLeaseKeyConfigurationFailsClosed(t *testing.T) {
	pub, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	key, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	valid := filepath.Join(dir, "proof-private.pem")
	invalid := filepath.Join(dir, "invalid-proof.pem")
	if err := os.WriteFile(valid, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(invalid, []byte("invalid"), 0600); err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier("test", map[string]ed25519.PublicKey{"test": pub})
	for _, test := range []struct {
		name, key, issuer, kid string
		wantError              bool
	}{
		{name: "disabled"},
		{name: "enabled", key: valid, issuer: "state", kid: "proof"},
		{name: "no key", issuer: "state", kid: "proof", wantError: true},
		{name: "no issuer", key: valid, kid: "proof", wantError: true},
		{name: "no kid", key: valid, issuer: "state", wantError: true},
		{name: "bad key", key: invalid, issuer: "state", kid: "proof", wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			env := map[string]string{"AHP_STATE_TOOL_LEASE_KEY": test.key,
				"AHP_STATE_TOOL_LEASE_ISSUER": test.issuer, "AHP_STATE_TOOL_LEASE_KID": test.kid}
			_, err := stateHandler(statejournal.New(nil), verifier, authorizeClaims, false, func(k string) string { return env[k] })
			if (err != nil) != test.wantError {
				t.Fatalf("state handler start: %v, expected error=%t", err, test.wantError)
			}
		})
	}
}
