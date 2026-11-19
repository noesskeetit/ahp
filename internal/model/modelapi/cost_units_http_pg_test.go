package modelapi

// The cost face is what the UI and an independent verifier read, so the promise
// "we report tokens, not money" has to hold in the SERVED BYTES, not only in the
// Go struct. Root's finding was that the cost answer reports tokens rather than
// currency; the honest resolution is to keep it that way and make the promise
// hard to break, because the pressure to print a money figure recurs.
//
// These legs drive the handler over a real TLS socket with the credential this
// face actually accepts -- a verified workload client certificate plus a signed
// task token, checked by the production transport.Task guard -- and examine the
// bytes. Nothing in the answer path is substituted: routing, handler, read model
// and database are the product's.
//
//     - no key anywhere in the answer is a currency denomination;
//     - the answer's key vocabulary is closed, so a future cost_rub fails here;
//     - no quantity is fractional, which is how a per-token price would show up;
//     - the two ledger denominations (tokens, steps) are served apart, and a
//       caller that adds them is warned by `units`;
//     - a foreign workload's credential does not get to read the numbers at all.

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// faceVocabulary is every key the cost face is allowed to serve. It is a closed
// list on purpose: the day someone adds cost_rub, this leg fails at review time
// instead of a customer's report.
var faceVocabulary = map[string]bool{
	"task_id": true, "units": true, "trees": true, "generated_at": true,
	"tree_root": true, "unit": true, "root": true, "nodes": true,
	"limit": true, "held_for_children": true, "spent": true, "open_holds": true,
	"committed": true, "remaining": true, "available_to_root": true, "exhausted": true,
	"generations": true, "tokens_in": true, "tokens_out": true, "tokens_total": true,
	"reconciles": true, "mismatches": true,
	"budget_id": true, "parent_budget_id": true, "task": true, "budget_key": true,
	"scope": true, "tenant": true, "deployment": true, "subject": true,
	"depth": true, "reserved": true, "closed": true, "settled_actual": true,
}

var faceCurrencyKey = regexp.MustCompile(`(?i)(usd|eur|rub|ruble|dollar|cent|money|price|tariff|fiat|currenc|invoice|cash|payable)`)

// faceUnits is the ledger's own denomination vocabulary.
var faceUnits = map[string]bool{"tokens": true, "steps": true, "unknown": true}

// costTrust is one throwaway trust domain: a CA, a server leaf, and the signing
// key the token verifier trusts.
type costTrust struct {
	ca     *x509.Certificate
	caKey  ed25519.PrivateKey
	pool   *x509.CertPool
	server tls.Certificate
	signer *auth.Signer
	pubKey ed25519.PublicKey
}

func newCostTrust(t *testing.T) *costTrust {
	t.Helper()
	seed := make([]byte, ed25519.SeedSize)
	if _, err := io.ReadFull(rand.Reader, seed); err != nil {
		t.Fatalf("signing seed: %v", err)
	}
	signer, err := auth.NewSigner("ahp", "h21-cost-test", ed25519.NewKeyFromSeed(seed))
	if err != nil {
		t.Fatalf("signer: %v", err)
	}

	caPub, caKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "h21-cost-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, caPub, caKey)
	if err != nil {
		t.Fatalf("ca cert: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse ca: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(caCert)

	return &costTrust{
		ca: caCert, caKey: caKey, pool: pool,
		server: costLeaf(t, caCert, caKey, "localhost", nil),
		signer: signer, pubKey: signer.Public(),
	}
}

// costLeaf signs one leaf certificate: a DNS name for the server, SPIFFE URIs for
// a workload.
func costLeaf(t *testing.T, ca *x509.Certificate, caKey ed25519.PrivateKey, dnsName string, uris []*url.URL) tls.Certificate {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: "h21-cost-leaf"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		URIs:         uris,
	}
	if dnsName != "" {
		tmpl.DNSNames = []string{dnsName}
		// httptest serves on 127.0.0.1, so a server leaf needs the IP as well as
		// the name -- otherwise the client verifies nothing about the server.
		tmpl.IPAddresses = []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, pub, caKey)
	if err != nil {
		t.Fatalf("leaf cert: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv}
}

// workloadCertFor is the certificate of the deployment that asks: its SPIFFE URI
// is the workload identity the transport derives the scope from.
func (ct *costTrust) workloadCertFor(t *testing.T, scope contracts.Scope) tls.Certificate {
	t.Helper()
	uri, err := url.Parse(auth.WorkloadID(scope))
	if err != nil {
		t.Fatalf("workload uri: %v", err)
	}
	return costLeaf(t, ct.ca, ct.caKey, "", []*url.URL{uri})
}

// taskTokenFor signs a real task credential for that workload -- the only
// credential this face accepts.
func (ct *costTrust) taskTokenFor(t *testing.T, scope contracts.Scope, taskID string) string {
	t.Helper()
	token, err := ct.signer.Sign(auth.Claims{
		Subject:    scope.Subject,
		Audience:   "ahp",
		Kind:       auth.KindTask,
		Tenant:     scope.Tenant,
		Deployment: scope.Deployment,
		Task:       taskID,
		Root:       taskID,
		Context:    "ctx-" + taskID,
		Grant:      "grant-" + taskID,
	}, 5*time.Minute)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return token
}

// costFace mounts the read face with the production guard and this trust domain's
// verification key.
func costFace(t *testing.T, budgets *model.BudgetStore, trust *costTrust) http.Handler {
	t.Helper()
	verifier := auth.NewVerifier("ahp", map[string]ed25519.PublicKey{"h21-cost-test": trust.pubKey})
	allow := func(context.Context, auth.Claims, string) error { return nil }
	h, err := NewHTTPHandler(budgets, verifier, allow)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	return h
}

// costCall is one served request; the zero fields build an anonymous call.
type costCall struct {
	scope          contracts.Scope
	taskID         string
	withCert       bool
	withCertTenant string // when set, the certificate claims this tenant instead
	withToken      bool
	// rawBody replaces the request body verbatim, for legs that ask the face to
	// do something it must not do.
	rawBody string
}

// serveCost posts over a real TLS socket and returns the served bytes.
func serveCost(t *testing.T, hh http.Handler, trust *costTrust, call costCall) (int, map[string]any) {
	t.Helper()
	srv := httptest.NewUnstartedServer(hh)
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{trust.server},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    trust.pool,
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)

	transportCfg := &tls.Config{RootCAs: trust.pool}
	if call.withCert {
		certScope := call.scope
		if call.withCertTenant != "" {
			certScope.Tenant = call.withCertTenant
		}
		transportCfg.Certificates = []tls.Certificate{trust.workloadCertFor(t, certScope)}
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: transportCfg}}

	body := []byte(call.rawBody)
	if call.rawBody == "" {
		body, _ = json.Marshal(map[string]string{"task_id": call.taskID})
	}
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/model/TreeCost", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if call.withToken {
		req.Header.Set("Authorization", "Bearer "+trust.taskTokenFor(t, call.scope, call.taskID))
	}
	resp, err := client.Do(req)
	if err != nil {
		// A refused handshake is an outcome, but never a cost answer.
		return http.StatusForbidden, map[string]any{"handshake": err.Error()}
	}
	defer resp.Body.Close()
	out := map[string]any{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode served bytes: %v", err)
	}
	return resp.StatusCode, out
}

// checkFaceBytes walks the served answer and refuses money vocabulary, unknown
// keys and fractional quantities.
func checkFaceBytes(t *testing.T, decoded map[string]any) {
	t.Helper()
	sawAccountingQuantity := false
	var walk func(v any, path string)
	walk = func(v any, path string) {
		switch node := v.(type) {
		case map[string]any:
			for k, val := range node {
				sub := path + "." + k
				if faceCurrencyKey.MatchString(k) {
					t.Fatalf("the served cost answer contains %q at %s: currency was attributed to a token ledger", k, sub)
				}
				if !faceVocabulary[k] {
					t.Fatalf("the cost face served an unlisted key %q at %s", k, sub)
				}
				switch val.(type) {
				case map[string]any, []any:
					walk(val, sub)
				default:
					if num, ok := val.(float64); ok {
						if num != float64(int64(num)) {
							t.Fatalf("%s = %v is fractional: token and step counts are integers, so this came from a price", sub, num)
						}
						sawAccountingQuantity = true
					}
				}
			}
		case []any:
			for i, el := range node {
				walk(el, fmt.Sprintf("%s[%d]", path, i))
			}
		}
	}
	walk(decoded, "")
	if !sawAccountingQuantity {
		t.Fatal("no accounting quantity was served: the guard matched nothing and proves nothing")
	}
}

// costPoolAndTree gives a leg a real budget tree and the face to ask about it.
func costPoolAndTree(t *testing.T, tag string, withObservations bool) (http.Handler, *costTrust, contracts.Scope, string, int64, *pgxpool.Pool) {
	t.Helper()
	_, budgets, pool := costFixture(t)
	trust := newCostTrust(t)
	scope, taskID, settled := seedTree(t, budgets, pool, tag, withObservations)
	return costFace(t, budgets, trust), trust, scope, taskID, settled, pool
}

// TestTreeCostFaceServesNoCurrencyOverTheWire is the served-bytes half of the
// honesty contract, with the numbers pinned so an empty answer cannot pass.
func TestTreeCostFaceServesNoCurrencyOverTheWire(t *testing.T) {
	hh, trust, scope, taskID, settled, _ := costPoolAndTree(t, "nocurr", true)

	code, out := serveCost(t, hh, trust, costCall{scope: scope, taskID: taskID, withCert: true, withToken: true})
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %v", code, out)
	}
	// Positive control: the tree and its numbers are really served.
	trees, ok := out["trees"].([]any)
	if !ok || len(trees) != 1 {
		t.Fatalf("trees = %v, want one tree", out["trees"])
	}
	tr, ok := trees[0].(map[string]any)
	if !ok {
		t.Fatalf("tree is not an object: %T", trees[0])
	}
	if got := tr["spent"]; got != float64(settled) {
		t.Fatalf("spent = %v, want %d (1427 parent + 100 child, in tokens)", got, settled)
	}
	units, ok := out["units"].([]any)
	if !ok || len(units) == 0 {
		t.Fatalf("units = %v, want a non-empty denomination list", out["units"])
	}
	for _, u := range units {
		name, ok := u.(string)
		if !ok || !faceUnits[name] {
			t.Fatalf("the face declared denomination %v: only %v are the ledger's own", u, faceUnits)
		}
	}
	checkFaceBytes(t, out)
}

// TestTreeCostFaceKeepsTwoDenominationsApartOverTheWire: one task carrying a
// token tree and a step tree is served as two trees, and `units` warns that a
// total across them would be meaningless.
func TestTreeCostFaceKeepsTwoDenominationsApartOverTheWire(t *testing.T) {
	_, budgets, _ := costFixture(t)
	trust := newCostTrust(t)
	ctx := context.Background()
	scope := costScope(fmt.Sprintf("h21apidenom%06d", time.Now().UnixNano()%1000000))
	taskID := fmt.Sprintf("task-denom-%06d", time.Now().UnixNano()%1000000)

	tokenRoot, err := budgets.Root(ctx, scope, "a2a-task:"+taskID, "execbridge-task-root", 20000)
	if err != nil {
		t.Fatalf("token root: %v", err)
	}
	if _, err := budgets.Reserve(ctx, scope, tokenRoot.ID, "gen:a", 700); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if _, err := budgets.Settle(ctx, scope, tokenRoot.ID, "gen:a", 661); err != nil {
		t.Fatalf("settle: %v", err)
	}
	stepRoot, err := budgets.Root(ctx, scope, "treedelegate-run:"+taskID, "treedelegate:run-steps", 4)
	if err != nil {
		t.Fatalf("step root: %v", err)
	}
	if _, err := budgets.Reserve(ctx, scope, stepRoot.ID, "core:step:0", 1); err != nil {
		t.Fatalf("step reserve: %v", err)
	}
	if _, err := budgets.Settle(ctx, scope, stepRoot.ID, "core:step:0", 1); err != nil {
		t.Fatalf("step settle: %v", err)
	}

	code, out := serveCost(t, costFace(t, budgets, trust), trust, costCall{scope: scope, taskID: taskID, withCert: true, withToken: true})
	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %v", code, out)
	}
	units, ok := out["units"].([]any)
	if !ok || len(units) != 2 {
		t.Fatalf("units = %v, want two denominations served apart", out["units"])
	}
	trees := out["trees"].([]any)
	if len(trees) != 2 {
		t.Fatalf("trees = %d, want two", len(trees))
	}
	spent := map[string]float64{}
	for _, raw := range trees {
		node, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("tree is not an object: %T", raw)
		}
		unit, ok := node["unit"].(string)
		if !ok || !faceUnits[unit] {
			t.Fatalf("served denomination %v is not the ledger's", node["unit"])
		}
		if _, clash := spent[unit]; clash {
			t.Fatalf("denomination %q served twice: a caller could not tell which total is which", unit)
		}
		num, ok := node["spent"].(float64)
		if !ok {
			t.Fatalf("spent is not a number in the %q tree: %v", unit, node["spent"])
		}
		spent[unit] = num
	}
	// Each number stays in its own unit. 661 tokens is not 1 step, and the answer
	// offers no sum of the two.
	if spent["tokens"] != 661 || spent["steps"] != 1 {
		t.Fatalf("spent per denomination = %v, want tokens=661 steps=1", spent)
	}
	checkFaceBytes(t, out)
}

// TestTreeCostFaceRefusesACredentialThatIsNotTheWorkloads is the boundary that
// keeps these numbers private: a token whose tenant differs from the verified
// certificate is refused, and an anonymous call gets nothing.
func TestTreeCostFaceRefusesACredentialThatIsNotTheWorkloads(t *testing.T) {
	_, budgets, pool := costFixture(t)
	trust := newCostTrust(t)
	ownerScope, ownerTask, _ := seedTree(t, budgets, pool, "cred", true)

	// Certificate says one tenant, the token says another.
	attacker := costScope("h21apiattacker")
	code, out := serveCost(t, costFace(t, budgets, trust), trust, costCall{
		scope: ownerScope, taskID: ownerTask,
		withCert: true, withCertTenant: attacker.Tenant, withToken: true,
	})
	if code == http.StatusOK {
		t.Fatalf("a certificate and a token from different tenants were served the tree: %v", out)
	}
	if body, err := json.Marshal(out); err == nil && faceCurrencyKey.MatchString(string(body)) {
		t.Fatalf("even the refusal leaked a currency field: %s", body)
	}

	// No credential at all: the handshake itself is refused, and no number leaks.
	code, out = serveCost(t, costFace(t, budgets, trust), trust, costCall{scope: ownerScope, taskID: ownerTask})
	if code == http.StatusOK {
		t.Fatalf("an anonymous caller was served a cost tree: %v", out)
	}
}

// TestTreeCostClientCannotDeclareADenomination closes the contract from the
// caller's side. The ledger derives a tree's denomination from the budget key
// that produced it (internal/model/tree_cost.go), so money is not something a
// request can assert -- and the face must not turn a client's claim into served
// bytes either. A request that demands USD is refused outright or answered in
// the ledger's own unit; both are honest, a currency-denominated answer is not.
func TestTreeCostClientCannotDeclareADenomination(t *testing.T) {
	hh, trust, scope, taskID, settled, _ := costPoolAndTree(t, "declare", true)
	body, err := json.Marshal(map[string]any{
		"task_id": taskID, "unit": "usd", "currency": "USD", "limit_unit": "dollars",
	})
	if err != nil {
		t.Fatal(err)
	}
	code, out := serveCost(t, hh, trust, costCall{scope: scope, taskID: taskID, withCert: true, withToken: true, rawBody: string(body)})
	if code == http.StatusOK {
		trees, ok := out["trees"].([]any)
		if !ok || len(trees) != 1 {
			t.Fatalf("status 200 without the tree the client asked for: %v", out)
		}
		tr := trees[0].(map[string]any)
		if tr["spent"] != float64(settled) {
			t.Fatalf("spent = %v, want the untouched ledger figure %d: the client's claim changed the accounting", tr["spent"], settled)
		}
		units, ok := out["units"].([]any)
		if !ok || len(units) == 0 {
			t.Fatalf("units = %v, want the ledger's own denominations", out["units"])
		}
		for _, u := range units {
			name, ok := u.(string)
			if !ok || !faceUnits[name] {
				t.Fatalf("the face served denomination %v because a client asked for it: the denomination belongs to the ledger, not the request", u)
			}
		}
	} else {
		if code != http.StatusBadRequest {
			t.Fatalf("status = %d: a request that declares a denomination is either refused as invalid or answered in the ledger's unit -- %v", code, out)
		}
		// A refusal is a structured error, not a stack trace or an echo of the
		// request: the client's own currency words must not come back as served
		// facts, and the reason must be machine-readable.
		if _, ok := out["code"]; !ok {
			t.Fatalf("the refusal carries no error code: %v", out)
		}
		assertServedNoMoney(t, out)
		return
	}
	checkFaceBytes(t, out)
}

// assertServedNoMoney walks an arbitrary answer and refuses any currency-shaped
// key, without requiring the cost answer's vocabulary (error envelopes are not
// cost answers).
func assertServedNoMoney(t *testing.T, decoded map[string]any) {
	t.Helper()
	leaves := 0
	var walk func(v any, path string)
	walk = func(v any, path string) {
		switch node := v.(type) {
		case map[string]any:
			for k, val := range node {
				if faceCurrencyKey.MatchString(k) {
					t.Fatalf("the face spoke in money at %s.%s", path, k)
				}
				walk(val, path+"."+k)
			}
		case []any:
			for i, val := range node {
				walk(val, fmt.Sprintf("%s[%d]", path, i))
			}
		default:
			leaves++
		}
	}
	walk(decoded, "")
	if leaves == 0 {
		t.Fatal("the answer has no leaves: a guard that matches nothing proves nothing")
	}
}
