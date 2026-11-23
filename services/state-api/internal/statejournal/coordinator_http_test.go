package statejournal

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	wire "github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/ahp-platform/ahp/sdk/go/state/journal"
)

func TestStateCoordinatorRoutesRequireExactWorkloadAndLiveCapability(t *testing.T) {
	ctx, pool, store := journalTestStore(t)
	ids := identity.New(pool)
	if err := ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner("identity", "state-coordinator", priv)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier("identity", map[string]ed25519.PublicKey{"state-coordinator": pub})

	coordScope := contracts.Scope{Tenant: "coordinator-" + rand.Text(), Deployment: "coordinator", Subject: "service"}
	legacy := contracts.Scope{Tenant: coordScope.Tenant, Deployment: "coordinator-other", Subject: "service"}
	target := contracts.Scope{Tenant: "target-" + rand.Text(), Deployment: "project-a", Subject: "alice"}
	h, err := NewHTTPWithCoordinator(store, verifier, ids.Authorize, nil, []string{auth.WorkloadID(coordScope)})
	if err != nil {
		t.Fatal(err)
	}
	ca := x509.NewCertPool()
	coordCert := coordinatorHTTPTestCert(t, ca, coordScope, 1)
	legacyCert := coordinatorHTTPTestCert(t, ca, legacy, 2)
	srv := httptest.NewUnstartedServer(h)
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: ca}
	srv.StartTLS()
	t.Cleanup(srv.Close)

	mint := func(scope contracts.Scope, task string, capabilities ...string) string {
		t.Helper()
		if _, err := ids.CreateRoot(ctx, scope, identity.RootRequest{
			Task: task, Context: scope.Tenant + "-" + scope.Deployment + "-" + task, Deployment: scope.Deployment,
			Capabilities: append([]string{"state"}, capabilities...), TTL: time.Minute,
		}); err != nil {
			t.Fatal(err)
		}
		token, err := ids.MintTask(ctx, scope, task, signer, "ahp", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	coordToken := mint(coordScope, "global", "state.coordinate")
	missingToken := mint(coordScope, "missing")
	legacyToken := mint(legacy, "scoped", "state.coordinate")

	coordTransport := srv.Client().Transport.(*http.Transport).Clone()
	coordTransport.TLSClientConfig.Certificates = []tls.Certificate{coordCert}
	t.Cleanup(coordTransport.CloseIdleConnections)
	coordHTTP := &http.Client{Transport: coordTransport}
	legacyTransport := srv.Client().Transport.(*http.Transport).Clone()
	legacyTransport.TLSClientConfig.Certificates = []tls.Certificate{legacyCert}
	t.Cleanup(legacyTransport.CloseIdleConnections)
	legacyHTTP := &http.Client{Transport: legacyTransport}
	clientFor := func(httpClient *http.Client, token string) *journal.Client {
		t.Helper()
		client, err := journal.New(journal.Config{
			BaseURL: srv.URL, HTTPClient: httpClient,
			TokenSource: func(context.Context) (string, error) { return token, nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		return client
	}
	coord := clientFor(coordHTTP, coordToken).Coordinator()
	scoped := coord.ForScope(target)
	legacyClient := clientFor(legacyHTTP, legacyToken)
	withoutCapability := clientFor(coordHTTP, missingToken).Coordinator()
	input := journal.EnqueueRequest{SessionID: "managed", Harness: "go", FormatVersion: 1, Key: "turn",
		Input: journal.Payload{Inline: []byte(`{"message":"hello"}`)}, ExecutionMode: wire.ExecutionVMManaged}
	receipt, err := scoped.EnqueueTurn(ctx, input)
	if err != nil || receipt.Status != SessionQueued {
		t.Fatalf("cross-tenant enqueue: %+v, %v", receipt, err)
	}
	for _, other := range []contracts.Scope{
		{Tenant: target.Tenant, Deployment: "project-b", Subject: target.Subject},
		{Tenant: target.Tenant, Deployment: target.Deployment, Subject: "bob"},
		{Tenant: legacy.Tenant, Deployment: target.Deployment, Subject: target.Subject},
	} {
		otherClient := coord.ForScope(other)
		if _, err := otherClient.GetTurnStatus(ctx, input.SessionID); contracts.Code(err) != contracts.ErrNotFound {
			t.Fatalf("foreign status (%+v): %v", other, err)
		}
		if _, err := otherClient.CancelSession(ctx, input.SessionID, receipt.HeadSeq); contracts.Code(err) != contracts.ErrNotFound {
			t.Fatalf("foreign cancel (%+v): %v", other, err)
		}
	}
	for _, coordinator := range []*journal.CoordinatorClient{legacyClient.Coordinator(), withoutCapability} {
		if _, err := coordinator.ListGlobalRecoverableVMPage(ctx, SessionQueued, nil); contracts.Code(err) != contracts.ErrForbidden {
			t.Fatalf("unqualified caller read the global reconcile queue: %v", err)
		}
		if _, err := coordinator.ForScope(target).GetTurnStatus(ctx, input.SessionID); contracts.Code(err) != contracts.ErrForbidden {
			t.Fatalf("unqualified caller read the global session route: %v", err)
		}
	}

	if _, err := store.EnqueueTurn(ctx, legacy, input); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE ahp_journal.sessions SET queued_at=statement_timestamp()-interval '5 seconds'
		WHERE tenant_id IN ($1, $2) AND session_id=$3`, legacy.Tenant, target.Tenant, input.SessionID); err != nil {
		t.Fatal(err)
	}
	legacyPage, err := legacyClient.ListRecoverableVMPage(ctx, SessionQueued, nil)
	if err != nil || len(legacyPage.Sessions) != 1 || legacyPage.Sessions[0].Scope != legacy {
		t.Fatalf("legacy route escaped its authenticated scope: %+v, %v", legacyPage, err)
	}

	pull := input
	pull.SessionID, pull.ExecutionMode = "pull", wire.ExecutionPull
	if _, err := scoped.EnqueueTurn(ctx, pull); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("global coordinator admitted a pull session: %v", err)
	}
	if _, err := store.EnqueueTurn(ctx, target, pull); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueDotsTurn(ctx, target, "dots-task", wire.DotsEnqueueRequest{
		SessionID: "dots", Key: "dots-key", Input: pull.Input,
	}); err != nil {
		t.Fatal(err)
	}
	for _, session := range []string{"pull", "dots"} {
		if _, err := scoped.GetTurnStatus(ctx, session); contracts.Code(err) != contracts.ErrNotFound {
			t.Fatalf("coordinator status accessed %s: %v", session, err)
		}
		if _, err := scoped.InspectSession(ctx, session); contracts.Code(err) != contracts.ErrNotFound {
			t.Fatalf("coordinator resume accessed %s: %v", session, err)
		}
		if _, err := scoped.GetEvents(ctx, session, 0, 10, ""); contracts.Code(err) != contracts.ErrNotFound {
			t.Fatalf("coordinator events accessed %s: %v", session, err)
		}
		if _, err := scoped.CancelSession(ctx, session, 1); contracts.Code(err) != contracts.ErrNotFound {
			t.Fatalf("coordinator cancel accessed %s: %v", session, err)
		}
		if err := scoped.RequeueFencedVMTurn(ctx, session, journal.FenceRequeueRequest{TurnSeq: 1, Epoch: 1, ExecutionID: "exec-test"}); contracts.Code(err) != contracts.ErrConflict {
			t.Fatalf("coordinator requeue accessed %s: %v", session, err)
		}
	}
	if res, err := scoped.InspectSession(ctx, input.SessionID); err != nil || res.Status != SessionQueued {
		t.Fatalf("VM-managed resume: %+v, %v", res, err)
	}
	if page, err := scoped.GetEvents(ctx, input.SessionID, 0, 10, ""); err != nil || len(page.Events) != 1 {
		t.Fatalf("VM-managed events: %+v, %v", page, err)
	}
	if res, err := scoped.CancelSession(ctx, input.SessionID, receipt.HeadSeq); err != nil || res.Status != "INTERRUPTED" {
		t.Fatalf("VM-managed cancel: %+v, %v", res, err)
	}
	for _, test := range []struct {
		method, path, body, token string
		client                    *http.Client
		want                      int
	}{
		{"POST", "/scopes/target/project/alice/sessions:enqueue", "{", legacyToken, legacyHTTP, 403},
		{"GET", "/vm-reconcile?status=QUEUED&after=2026-11-23T00:00:00Z&after_session=session", "", coordToken, coordHTTP, 400},
		{"GET", "/scopes/target/project/alice%2Fbob/sessions/managed", "", coordToken, coordHTTP, 400},
	} {
		req, err := http.NewRequestWithContext(ctx, test.method, srv.URL+"/internal/v1/state/coordinator"+test.path, bytes.NewBufferString(test.body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+test.token)
		req.Header.Set("Content-Type", "application/json")
		res, err := test.client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != test.want {
			t.Fatalf("%s: status %d, want %d", test.path, res.StatusCode, test.want)
		}
	}

	if err := ids.CancelSubtree(ctx, coordScope, "global"); err != nil {
		t.Fatal(err)
	}
	if _, err := coord.ListGlobalRecoverableVMPage(ctx, SessionQueued, nil); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("old token retained global access after revocation: %v", err)
	}
}

func coordinatorHTTPTestCert(t *testing.T, roots *x509.CertPool, scope contracts.Scope, serial int64) tls.Certificate {
	t.Helper()
	uri, err := url.Parse(auth.WorkloadID(scope))
	if err != nil {
		t.Fatal(err)
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial), NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Minute),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		URIs: []*url.URL{uri},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, priv)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots.AddCert(cert)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv, Leaf: cert}
}
