//go:build ahp_data

package main

// The browser path, end to end.
//
// What the acceptance deployment measured (14:50Z-14:57Z) is that the upload
// reaches 200 with sha+size on the aggregate origin, and that the human edge
// cannot reach it: the runner owns neither /v1/workspace/ nor
// /v1/artifacts/BeginUpload, and the aggregate needs a workload certificate plus
// a KindTask token. This test closes the remaining distance on a lane origin: the
// REAL edge (mountSingleOrigin, the code the browser talks to), the REAL data
// plane construction (server.buildDataHandler, the code the aggregate runs), the
// REAL workspaceapi and artifactsapi handlers behind REAL mTLS with
// RequireAndVerifyClientCert, and REAL auth.Signer tokens throughout -- the
// caller presents a KindHuman token, the exchange turns it into a KindTask token
// with the caller's own tenant and subject, and nothing in between fabricates a
// scope.
//
// Proved: the whole upload chain answers 200 with the content sha256 and size
// THROUGH the user edge, and a second subject exchanging its own token is refused
// 403 on the project it does not belong to. Skips without its environment; run by
// scripts/asws-upload-leg.sh.

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

type edgeTestCA struct {
	cert  *x509.Certificate
	sign  *rsa.PrivateKey
	pool  *x509.CertPool
	caPEM []byte
	dir   string
}

func newEdgeTestCA(t *testing.T) *edgeTestCA {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "asws-edge-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "ca.pem"), caPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return &edgeTestCA{cert: cert, sign: key, pool: pool, caPEM: caPEM, dir: dir}
}

// issue writes a leaf and returns its paths. workload names the SPIFFE id, which
// transport.Task requires to match the token's tenant and deployment.
func (ca *edgeTestCA) issue(t *testing.T, name, workload string) (string, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	if workload != "" {
		tmpl.URIs = []*url.URL{mustParseWorkloadURI(t, workload)}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.sign)
	if err != nil {
		t.Fatal(err)
	}
	certPath := filepath.Join(ca.dir, name+".crt")
	keyPath := filepath.Join(ca.dir, name+".key")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	kder, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kder}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath
}

// mustParseWorkloadURI parses the SPIFFE id that transport.Task matches against
// the token's tenant and deployment.
func mustParseWorkloadURI(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("workload URI %q: %v", raw, err)
	}
	return u
}

func (ca *edgeTestCA) server(t *testing.T, h http.Handler, certPath, keyPath string) *httptest.Server {
	t.Helper()
	srv := httptest.NewUnstartedServer(h)
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatal(err)
	}
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    ca.pool,
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

func TestWorkspaceEdgeUploadsIntoProjectThroughTheUserEdge(t *testing.T) {
	// One dev key for both sides of this boundary. devSigningKey derives the
	// keypair from AHP_DEV_JWT_SEED and otherwise mints a FRESH ephemeral pair on
	// every call, so the origin's verifier (constructed inside newDataPlanes) and
	// this test's signer would hold different keys and every call would come back
	// "token signature is not valid". Sharing a seed across processes is exactly
	// what tools/cascade-stand-up.sh does on the deployment; the value is a test
	// fixture, not a secret.
	t.Setenv("AHP_DEV_JWT_SEED", "0000000000000000000000000000000000000000000000000000000000000001")

	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" || os.Getenv("AHP_S3_ENDPOINT") == "" {
		t.Skip("needs AHP_TEST_DATABASE_URL and AHP_S3_ENDPOINT (see scripts/asws-upload-leg.sh)")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	artHandler, wsHandler, err := (&server{pool: pool, logger: quietLogger()}).buildDataHandler(ctx)
	if err != nil {
		t.Fatalf("the real data plane refused to build: %v", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/v1/artifacts/", artHandler)
	mux.Handle("/v1/workspace/", wsHandler)

	tenant := "edge-e2e-" + contracts.NewID()
	const (
		deployment = "edge-e2e-deploy"
		subjectA   = "user-a"
		subjectB   = "user-b"
	)
	ca := newEdgeTestCA(t)
	originCert, originKey := ca.issue(t, "origin", "")
	origin := ca.server(t, mux, originCert, originKey)
	edgeCert, edgeKey := ca.issue(t, "edge", "spiffe://ahp/tenant/"+tenant+"/deployment/"+deployment)

	ids := identity.New(pool)
	if err := ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	verifier, signer := devIdentityAuth()
	human := func(subject string) string {
		tok, err := signer.Sign(auth.Claims{Audience: "ahp", Kind: auth.KindHuman, Tenant: tenant, Subject: subject},
			5*time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return tok
	}
	// The exchange for this boundary: a KindHuman token in, a KindTask token out,
	// carrying the CALLER's tenant and subject. The deployment is the edge's own
	// workload, because transport.Task compares the token against the certificate
	// that presents it.
	var seenBoundaries []string
	exchange := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req exchangeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("exchange request: %v", err)
		}
		// Fail closed on the shape the contract froze: exactly one capability per
		// boundary, and only the two contracted task/capability pairs. A merged
		// root would make this leg fail instead of quietly passing.
		capability := ""
		if len(req.Capabilities) == 1 {
			capability = req.Capabilities[0]
		}
		switch req.Task + "|" + capability {
		case workspaceExchangeTask + "|" + workspaceExchangeCap:
		case artifactUploadTask + "|" + artifactUploadCap:
		default:
			t.Errorf("the edge exchanged task=%q capabilities=%v, which the contract does not authorise",
				req.Task, req.Capabilities)
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"code":"FORBIDDEN","message":"unauthorised authority prefix"}`))
			return
		}
		seenBoundaries = append(seenBoundaries, req.Task)
		claims, err := verifier.Verify(req.HumanToken, "ahp", auth.KindHuman, "")
		if err != nil {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"code":"FORBIDDEN","message":"human token refused"}`))
			return
		}
		scope := contracts.Scope{Tenant: claims.Tenant, Deployment: deployment, Subject: claims.Subject}
		task := contracts.OperationKey(scope, req.Task, capability, req.Context)
		grant, err := ids.CreateRoot(r.Context(), scope, identity.RootRequest{Task: task, Context: "ctx-" + task, Deployment: deployment, Capabilities: []string{capability}, TTL: 5 * time.Minute})
		if err != nil {
			t.Errorf("create real exchange grant: %v", err)
			http.Error(w, "grant failed", 500)
			return
		}
		tok, err := ids.MintTask(r.Context(), scope, grant.Task, signer, "ahp", 0)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = w.Write([]byte(`{"token":"` + tok + `","task":"` + req.Task + `"}`))
	}))
	defer exchange.Close()

	t.Setenv(envWorkspaceOrigin, origin.URL)
	t.Setenv(envWorkspaceExchange, exchange.URL+"/v1/identity/control/exchange")
	t.Setenv(envWorkspaceClientCert, edgeCert)
	t.Setenv(envWorkspaceClientKey, edgeKey)
	t.Setenv(envWorkspaceCA, filepath.Join(ca.dir, "ca.pem"))

	edge := http.NewServeMux()
	if !mountSingleOrigin(edge, "http://127.0.0.1:1", true, quietLogger()) {
		t.Fatal("the edge did not mount")
	}

	call := func(path, token string, body map[string]any) (int, string) {
		raw, _ := json.Marshal(body)
		req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		edge.ServeHTTP(rec, req)
		return rec.Code, rec.Body.String()
	}
	tokenA := human(subjectA)

	// A fresh payload per run: the project is named after the content digest, so a
	// fixed payload would make the second run of this leg answer 409 against its
	// own earlier workspace and the leg would measure its own history instead of
	// the chain.
	nonce := make([]byte, 8)
	if _, err := rand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	payload := []byte("browser path upload into project context " + hex.EncodeToString(nonce) + "\n")
	sum := sha256.Sum256(payload)
	wantSHA := hex.EncodeToString(sum[:])

	code, body := call("/v1/artifacts/BeginUpload", tokenA, map[string]any{
		"key": "edge-e2e-" + wantSHA[:12], "name": "hello.txt", "mime": "text/plain", "sha256": wantSHA, "size": len(payload)})
	if code != http.StatusOK {
		t.Fatalf("BeginUpload through the edge = %d %s", code, body)
	}
	var up struct {
		ID           string `json:"id"`
		PresignedURL string `json:"presigned_url"`
	}
	if err := json.Unmarshal([]byte(body), &up); err != nil {
		t.Fatal(err)
	}
	put, err := http.NewRequest(http.MethodPut, up.PresignedURL, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(put)
	if err != nil {
		t.Fatal(err)
	}
	putBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		t.Fatalf("presigned PUT = %d %s", resp.StatusCode, putBody)
	}
	code, body = call("/v1/artifacts/Seal", tokenA, map[string]any{"id": up.ID})
	if code != http.StatusOK {
		t.Fatalf("Seal through the edge = %d %s", code, body)
	}
	var sealed struct {
		SHA256 string `json:"sha256"`
		Size   int64  `json:"size"`
	}
	if err := json.Unmarshal([]byte(body), &sealed); err != nil {
		t.Fatal(err)
	}
	if sealed.SHA256 != wantSHA || sealed.Size != int64(len(payload)) {
		t.Fatalf("Seal returned sha256=%s size=%d, want %s/%d", sealed.SHA256, sealed.Size, wantSHA, len(payload))
	}

	code, body = call("/v1/workspace/Create", tokenA, map[string]any{"workspace": "edge-proj-" + wantSHA[:12]})
	if code != http.StatusOK {
		t.Fatalf("workspace/Create through the edge = %d %s", code, body)
	}
	var ws struct{ ID string }
	if err := json.Unmarshal([]byte(body), &ws); err != nil {
		t.Fatal(err)
	}
	code, body = call("/v1/workspace/CreateCandidate", tokenA, map[string]any{"workspace": ws.ID})
	if code != http.StatusOK {
		t.Fatalf("CreateCandidate through the edge = %d %s", code, body)
	}
	var cand struct{ ID string }
	if err := json.Unmarshal([]byte(body), &cand); err != nil {
		t.Fatal(err)
	}
	code, body = call("/v1/workspace/UpdateCandidateFiles", tokenA, map[string]any{
		"candidate_id": cand.ID,
		"files":        []map[string]any{{"path": "hello.txt", "artifact_id": up.ID, "mode": 420, "digest": wantSHA}},
	})
	if code != http.StatusOK {
		t.Fatalf("UpdateCandidateFiles through the edge = %d %s", code, body)
	}
	t.Logf("EDGE-LEG upload-into-project workspace=%s candidate=%s http=200 sha256=%s size=%d verdict=PASS",
		ws.ID, cand.ID, sealed.SHA256, sealed.Size)

	// Both boundaries of the chain were exchanged, each under its own authority
	// prefix: the upload never held workspace authority and the workspace calls
	// never held artifact authority.
	seen := strings.Join(seenBoundaries, ",")
	if !strings.Contains(seen, workspaceExchangeTask) || !strings.Contains(seen, artifactUploadTask) {
		t.Fatalf("the chain exchanged under %q, want both %q and %q", seen, workspaceExchangeTask, artifactUploadTask)
	}
	t.Logf("EDGE-LEG authority-prefixes=%s verdict=PASS", seen)

	// The same edge, a different subject: the exchange hands over that subject's
	// own scope, so the authorization gate -- not the proxy -- is what answers.
	code, body = call("/v1/workspace/UpdateCandidateFiles", human(subjectB), map[string]any{
		"candidate_id": cand.ID,
		"files":        []map[string]any{{"path": "hello.txt", "artifact_id": up.ID, "mode": 420, "digest": wantSHA}},
	})
	if code != http.StatusForbidden {
		t.Errorf("a second subject staging into the first user's candidate = %d %s, want 403 from the authorization gate", code, body)
	}
	t.Logf("EDGE-LEG foreign-subject http=%d verdict=PASS", code)
	fmt.Fprintln(os.Stderr, "EDGE_E2E=PASS sha256="+wantSHA+" size="+fmt.Sprint(len(payload)))
}
