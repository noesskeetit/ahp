//go:build unix

package vmruntime

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/runcoord"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestCoordinatorDispatchesOverMTLSAndLiveGrant(t *testing.T) {
	ctx, manager, grants, target, _, proc := setup(t)
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner("vmruntime-test", "test-key", key)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier("vmruntime-test", map[string]ed25519.PublicKey{"test-key": pub})
	coordinator := contracts.Scope{Tenant: target.Scope.Tenant, Deployment: "coordinator", Subject: "service"}
	other := contracts.Scope{Tenant: target.Scope.Tenant, Deployment: "other", Subject: "service"}
	roots := x509.NewCertPool()
	coordinatorCert := clientCertificate(t, coordinator, roots)
	otherCert := clientCertificate(t, other, roots)
	listener := httptest.NewUnstartedServer(Handler(manager, verifier, []string{auth.WorkloadID(coordinator)}, grants.Authorize))
	listener.TLS = &tls.Config{MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	listener.StartTLS()
	t.Cleanup(listener.Close)

	dispatcherFor := func(scope contracts.Scope, cert tls.Certificate) runcoord.HTTPDispatcher {
		t.Helper()
		if _, err := grants.CreateRoot(ctx, scope, identity.RootRequest{
			Deployment: scope.Deployment, Task: "dispatch", Context: fmt.Sprintf("%s-%s-dispatch", scope.Tenant, scope.Deployment),
			Capabilities: []string{"runtime.dispatch"}, TTL: time.Hour,
		}); err != nil {
			t.Fatal(err)
		}
		token, err := grants.MintTask(ctx, scope, "dispatch", signer, "ahp", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		clientTransport := listener.Client().Transport.(*http.Transport).Clone()
		clientTransport.TLSClientConfig.Certificates = []tls.Certificate{cert}
		t.Cleanup(clientTransport.CloseIdleConnections)
		return runcoord.HTTPDispatcher{BaseURL: listener.URL,
			Client:      &http.Client{Transport: clientTransport, Timeout: 5 * time.Second},
			TokenSource: func(context.Context) (string, error) { return token, nil },
		}
	}
	good := dispatcherFor(coordinator, coordinatorCert)
	foreign := dispatcherFor(other, otherCert)
	exec := runcoord.Execution{Scope: target.Scope, SessionID: target.SessionID, Harness: target.Harness,
		LaunchTurnSeq: target.LaunchTurnSeq, Generation: target.Generation, ExecutionID: target.ID}
	if err := foreign.EnsureRunning(ctx, exec); err == nil || !strings.Contains(err.Error(), "HTTP 403") {
		t.Fatalf("foreign workload accepted: %v", err)
	}
	if err := good.EnsureRunning(ctx, exec); err != nil {
		t.Fatal(err)
	}
	if ok, _ := proc.Alive(ctx, Guest{Execution: target, Attempt: 1}); !ok {
		t.Fatal("server did not start the physical process")
	}
	exec.LaunchTurnSeq++
	if err := good.Fence(ctx, exec); err != nil {
		t.Fatal(err)
	}
	if ok, _ := proc.Alive(ctx, Guest{Execution: target, Attempt: 1}); ok {
		t.Fatal("runtime acknowledged fence before process exited")
	}
	if err := grants.CancelSubtree(ctx, coordinator, "dispatch"); err != nil {
		t.Fatal(err)
	}
	if err := good.Fence(ctx, exec); err == nil || !strings.Contains(err.Error(), "HTTP 403") {
		t.Fatalf("revoked coordinator grant accepted: %v", err)
	}
}

func clientCertificate(t *testing.T, scope contracts.Scope, roots *x509.CertPool) tls.Certificate {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	uri, err := url.Parse(auth.WorkloadID(scope))
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		URIs: []*url.URL{uri}, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots.AddCert(cert)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: cert}
}
