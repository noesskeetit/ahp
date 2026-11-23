package statejournal

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
)

func TestDotsFIFOHTTPVerifiedTask(t *testing.T) {
	ctx, pool, store := journalTestStore(t)
	scope := contracts.Scope{Tenant: "tenant-" + rand.Text(), Deployment: "dots", Subject: "user"}
	ids := identity.New(pool)
	if err := ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	keys := func(issuer string) (*auth.Signer, *auth.Verifier) {
		t.Helper()
		pub, priv, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		signer, err := auth.NewSigner(issuer, "test", priv)
		if err != nil {
			t.Fatal(err)
		}
		return signer, auth.NewVerifier(issuer, map[string]ed25519.PublicKey{"test": pub})
	}
	signer, verifier := keys("identity")
	proofSigner, proofVerifier := keys("state")
	token := func(task string, extraCapabilities ...string) string {
		t.Helper()
		if _, err := ids.CreateRoot(ctx, scope, identity.RootRequest{Task: task, Context: "conversation-" + scope.Tenant, Deployment: scope.Deployment,
			Capabilities: append([]string{"state"}, extraCapabilities...), TTL: time.Minute}); err != nil {
			t.Fatal(err)
		}
		raw, err := ids.MintTask(ctx, scope, task, signer, "ahp", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	a, b := token("a"), token("b")
	plain, err := NewHTTPWithToolLeases(store, verifier, ids.Authorize, proofSigner)
	if err != nil {
		t.Fatal(err)
	}
	dots, err := NewHTTPWithDotsFIFO(store, verifier, ids.Authorize, proofSigner)
	if err != nil {
		t.Fatal(err)
	}

	uri, err := url.Parse(auth.WorkloadID(scope))
	if err != nil {
		t.Fatal(err)
	}
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Minute),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		URIs: []*url.URL{uri},
	}
	cert, err := x509.CreateCertificate(rand.Reader, clientTemplate, clientTemplate, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(cert)
	if err != nil {
		t.Fatal(err)
	}
	ca := x509.NewCertPool()
	ca.AddCert(leaf)
	start := func(handler http.Handler) *httptest.Server {
		t.Helper()
		srv := httptest.NewUnstartedServer(handler)
		srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: ca}
		srv.StartTLS()
		tr := srv.Client().Transport.(*http.Transport)
		tr.TLSClientConfig.Certificates = []tls.Certificate{{Certificate: [][]byte{cert}, PrivateKey: key, Leaf: leaf}}
		t.Cleanup(srv.Close)
		return srv
	}
	httpPlain, httpDots := start(plain), start(dots)
	post := func(srv *httptest.Server, path, credential string, body any, want int, out any) {
		t.Helper()
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+path, bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		if credential != "" {
			req.Header.Set("Authorization", "Bearer "+credential)
		}
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != want {
			raw, _ := io.ReadAll(resp.Body)
			t.Fatalf("POST %s: HTTP %d, expected %d: %s", path, resp.StatusCode, want, raw)
		}
		if out != nil {
			if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
				t.Fatal(err)
			}
		}
	}
	const enqueue = "/v1/state/sessions/dots:enqueue"
	const claim = "/v1/state/sessions/dots:claim-turn"
	post(httpPlain, enqueue, a, struct{}{}, http.StatusNotFound, nil)
	req := journal.DotsEnqueueRequest{SessionID: "chat", Key: "first", Input: Payload{Inline: []byte(`{}`)}}
	post(httpDots, enqueue, "", req, http.StatusForbidden, nil)
	spoof := struct {
		journal.DotsEnqueueRequest
		TaskID string `json:"task_id"`
	}{req, "other"}
	post(httpDots, enqueue, a, spoof, http.StatusBadRequest, nil)
	var first journal.DotsEnqueueReceipt
	post(httpDots, enqueue, a, req, http.StatusOK, &first)
	req.Key = "second"
	post(httpDots, enqueue, b, req, http.StatusOK, nil)
	want := struct {
		journal.DotsTurnLease
		Proof string `json:"tool_lease_proof"`
	}{}
	claimReq := claimTurnRequest{SessionID: "chat"}
	post(httpDots, "/v1/state/sessions:claim-turn", a, claimReq, http.StatusNotFound, nil)
	post(httpDots, claim, b, claimReq, http.StatusNotFound, nil)
	post(httpDots, claim, a, claimReq, http.StatusOK, &want)
	if want.TaskID != "a" || want.RunID != first.RunID || want.RunSequence != 1 || want.Epoch != 1 {
		t.Fatalf("claim didn't return the task-bound FIFO head: %+v", want.DotsTurnLease)
	}
	claims, err := verifier.Verify(a, "ahp", auth.KindTask, auth.WorkloadID(scope))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := proofVerifier.VerifyToolLease(want.Proof, claims, req.SessionID, want.Epoch); err != nil {
		t.Fatalf("lease proof bound to the wrong task or session: %v", err)
	}
	t.Run("VM claim requires launch identity and coordinator admission", func(t *testing.T) {
		coord := token("coord", "state.coordinate")
		const path = "/v1/state/sessions:enqueue"
		input := journal.EnqueueRequest{SessionID: "managed", Harness: "go", FormatVersion: 1, Key: "turn",
			ExecutionMode: journal.ExecutionVMManaged, Input: Payload{Inline: []byte(`{}`)}}
		post(httpPlain, path, a, input, http.StatusForbidden, nil)
		var receipt journal.EnqueueReceipt
		post(httpPlain, path, coord, input, http.StatusOK, &receipt)
		id := journal.DeterministicExecutionID(scope, input.SessionID, receipt.HeadSeq, 1)
		claim := journal.VMTurnClaimRequest{TurnSeq: receipt.HeadSeq, Generation: 1, ExecutionID: id}
		claimPath := "/v1/state/sessions/managed/vm:claim"
		post(httpPlain, claimPath, a, claim, http.StatusForbidden, nil)
		post(httpPlain, claimPath, token("unrelated-vm", "state.vm_claim"), claim, http.StatusForbidden, nil)
		var turn journal.TurnLease
		post(httpPlain, claimPath, token(id, "state.vm_claim"), claim, http.StatusOK, &turn)
		if turn.Worker != id || turn.InputSeq != receipt.HeadSeq || turn.Epoch != 1 {
			t.Fatalf("unexpected VM claim: %+v", turn)
		}
	})
	t.Run("VM-managed turn through the Go SDK", func(t *testing.T) {
		testVMManagedTurnSDK(t, httpPlain, scope, token)
	})
}
