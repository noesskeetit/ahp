//go:build unix

package main

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestRuntimeToolDeploymentsAndTrustConfiguration(t *testing.T) {
	dir := t.TempDir()
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	pkcs8, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	fixture := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	t.Cleanup(fixture.Close)
	keyPath, caPath, badCA := filepath.Join(dir, "jwt.pem"), filepath.Join(dir, "ca.pem"), filepath.Join(dir, "bad.pem")
	for path, data := range map[string][]byte{
		keyPath: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8}),
		caPath:  pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: fixture.Certificate().Raw}),
		badCA:   []byte("invalid PEM"),
	} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("AHP_VM_RUNTIME_JWT_PRIVATE_KEY", keyPath)
	t.Setenv("AHP_VM_RUNTIME_JWT_ISSUER", "runtime-test")
	t.Setenv("AHP_VM_RUNTIME_JWT_KID", "runtime-test")
	t.Setenv("AHP_VM_RUNTIME_STATE_CA", caPath)
	t.Setenv("AHP_VM_RUNTIME_STATE_URL", fixture.URL)
	t.Setenv("AHP_VM_RUNTIME_WORKLOAD_CERT_DIR", dir)

	for _, test := range []struct {
		name        string
		deployments string
		origin      string
		ca          string
		allowed     bool
		invalid     bool
	}{
		{name: "default"},
		{name: "valid origin without grants", origin: fixture.URL},
		{name: "explicit grant with State CA fallback", origin: fixture.URL, deployments: " tenant/agent , other/team ", allowed: true},
		{name: "explicit grant with Tools CA", origin: fixture.URL, deployments: "tenant/agent", ca: caPath, allowed: true},
		{name: "no route", deployments: "tenant/agent", invalid: true},
		{name: "plaintext origin", deployments: "tenant/agent", origin: "http://gateway.invalid", invalid: true},
		{name: "origin has path", deployments: "tenant/agent", origin: fixture.URL + "/gateway", invalid: true},
		{name: "origin has fragment", deployments: "tenant/agent", origin: fixture.URL + "#", invalid: true},
		{name: "origin has invalid port", deployments: "tenant/agent", origin: "https://gateway.invalid:66000", invalid: true},
		{name: "bad CA", deployments: "tenant/agent", origin: fixture.URL, ca: badCA, invalid: true},
		{name: "missing CA file", deployments: "tenant/agent", origin: fixture.URL, ca: filepath.Join(dir, "missing.pem"), invalid: true},
		{name: "wildcard tenant", deployments: "*/agent", origin: fixture.URL, invalid: true},
		{name: "wildcard deployment", deployments: "tenant/*", origin: fixture.URL, invalid: true},
		{name: "wrong delimiter", deployments: "tenant:agent", origin: fixture.URL, invalid: true},
		{name: "empty entry", deployments: "tenant/agent,", origin: fixture.URL, invalid: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("AHP_VM_RUNTIME_TOOLS_DEPLOYMENTS", test.deployments)
			t.Setenv("AHP_VM_RUNTIME_TOOLS_URL", test.origin)
			t.Setenv("AHP_VM_RUNTIME_TOOLS_CA", test.ca)
			injector, allowed, err := runtimeInjector(nil)
			if test.invalid {
				if err == nil || injector != nil {
					t.Fatal("misconfigured tools policy did not fail startup")
				}
				return
			}
			if err != nil || injector == nil {
				t.Fatalf("could not configure injector: %v", err)
			}
			selected := contracts.Scope{Tenant: "tenant", Deployment: "agent", Subject: "alice"}
			if granted := allowed != nil && allowed(selected); granted != test.allowed {
				t.Fatalf("selected scope granted tools = %t, want %t", granted, test.allowed)
			}
			if allowed != nil {
				if !allowed(contracts.Scope{Tenant: "tenant", Deployment: "agent", Subject: "bob"}) ||
					allowed(contracts.Scope{Tenant: "other", Deployment: "agent", Subject: "alice"}) ||
					allowed(contracts.Scope{Tenant: "tenant", Deployment: "team", Subject: "alice"}) {
					t.Fatal("tools opt-in did not match exact tenant and deployment pairs")
				}
			}
		})
	}
}
