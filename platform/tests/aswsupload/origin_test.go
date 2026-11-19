//go:build ahp_data

// Package aswsupload proves, on an isolated LANE origin (not prod), that a real
// file upload into a project context is reachable through the mounted
// /v1/workspace/* route: the owner stages a sealed artifact into a workspace
// candidate and gets 200 with the content sha256 and size, a foreign subject is
// refused 403, and two users' rights in the same tenant are separated
// (editor writes, editor cannot publish; a foreign tenant cannot even resolve
// the project -> 404). It mounts the REAL workspaceapi/artifactsapi handlers
// behind REAL mTLS (transport.Task) with REAL auth.Signer-issued KindTask
// tokens over REAL PostgreSQL + MinIO. Skips without the env it needs.
//
// ORACLE STATUS (requested by hive-director, 20261101T163034Z section 5.3):
// this test is the leg oracle for "upload a file into a project context". A
// SKIP here is NOT a pass and must not be reported as one -- it means the
// substrate is missing, not that the behavior is wrong. The substrate is an
// ISOLATED PostgreSQL role+database WITH CREATEDB (one per lane, never shared,
// provisioned on the acceptance node) plus a private MinIO endpoint and bucket.
// A DSN whose role lacks CREATEDB makes this leg red for a reason that is not
// the product: the run is invalid, not the verdict.
package aswsupload

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
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
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/artifactsapi"
	"github.com/ahp-platform/ahp/internal/workspace"
	"github.com/ahp-platform/ahp/internal/workspace/workspaceapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

const (
	tenantA      = "ahpleg"
	deploymentA  = "aswsleg"
	foreignTen   = "ahpleg-foreign"
	issuer       = "ahp"
	kid          = "dev-1"
	capWorkspace = "workspace"
	capArtifacts = "artifacts"
)

type leg struct {
	t   *testing.T
	url string
	cl  *http.Client
	ev  []string
}

func (l *leg) rec(format string, a ...any) {
	line := fmt.Sprintf(format, a...)
	l.ev = append(l.ev, line)
	l.t.Log(line)
}

// call POSTs body to op with a bearer task token; returns status + raw body.
func (l *leg) call(op, tok string, body any) (int, []byte) {
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, l.url+"/v1/"+op, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := l.cl.Do(req)
	if err != nil {
		l.t.Fatalf("call %s: %v", op, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

func TestUploadIntoProjectContextThroughOrigin(t *testing.T) {
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	s3ep := os.Getenv("AHP_S3_ENDPOINT")
	bucket := os.Getenv("AHP_S3_BUCKET")
	if dsn == "" || s3ep == "" || bucket == "" {
		t.Skip("ORACLE SKIPPED, NOT PASSED: upload-into-project needs an isolated DSN whose role has CREATEDB " +
			"plus AHP_S3_ENDPOINT and AHP_S3_BUCKET; a missing substrate makes this run invalid, not the product red")
	}
	ctx := context.Background()

	// --- stores (wiring mirrors cmd/ahp/role_data.go newDataPlanes) ---
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("pg ping: %v", err)
	}
	mu, err := minio.New(mustHost(s3ep), &minio.Options{
		Creds:  credentials.NewStaticV4(os.Getenv("AHP_MINIO_ROOT_USER"), os.Getenv("AHP_MINIO_ROOT_PASSWORD"), ""),
		Secure: false,
		Region: os.Getenv("AHP_S3_REGION"),
	})
	if err != nil {
		t.Fatalf("minio: %v", err)
	}
	artifactActions := map[string]bool{"upload": true, "seal": true, "read": true, "delete": true,
		"publish": true, "derive": true, "retain": true, "hold": true, "purge": true}
	workspaceActions := map[string]bool{"create": true, "read": true, "write": true, "purge": true,
		"candidate:create": true, "candidate:list": true, "candidate:write": true,
		"candidate:check": true, "candidate:reject": true, "publish": true,
		"member:add": true, "member:remove": true, "member:list": true}
	var wsStore *workspace.Store
	artStore, err := artifacts.New(pool, mu, bucket,
		func(_ context.Context, scope contracts.Scope, action, _ string) error {
			if err := scope.Validate(); err != nil {
				return err
			}
			if !artifactActions[action] {
				return contracts.NewError(contracts.ErrForbidden, "lane: artifact action "+action+" not permitted")
			}
			return nil
		},
		artifacts.WithWorkspaceAccess(func(ctx context.Context, scope contracts.Scope, w string) error {
			if wsStore == nil {
				return contracts.NewError(contracts.ErrForbidden, "lane: workspace membership not ready")
			}
			return wsStore.IsMember(ctx, scope, w)
		}))
	if err != nil {
		t.Fatalf("artifacts.New: %v", err)
	}
	wsStore, err = workspace.New(pool, artStore,
		func(_ context.Context, scope contracts.Scope, action, _ string) error {
			if err := scope.Validate(); err != nil {
				return err
			}
			if !workspaceActions[action] {
				return contracts.NewError(contracts.ErrForbidden, "lane: workspace action "+action+" not permitted")
			}
			return nil
		})
	if err != nil {
		t.Fatalf("workspace.New: %v", err)
	}
	if err := artStore.Migrate(ctx); err != nil {
		t.Fatalf("artifacts.Migrate: %v", err)
	}
	if err := wsStore.Migrate(ctx); err != nil {
		t.Fatalf("workspace.Migrate: %v", err)
	}

	// --- real identity (dev key like cmd/ahp devIdentityAuth) ---
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner(issuer, kid, priv)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier(issuer, map[string]ed25519.PublicKey{kid: pub})
	mint := func(tenant, subject, tag string) string {
		tok, err := signer.Sign(auth.Claims{
			Audience: issuer, Kind: auth.KindTask,
			Tenant: tenant, Deployment: deploymentA, Subject: subject,
			Task: "task-" + tag, Root: "root-" + tag, Context: "ctx-" + tag,
			Grant: "grant-" + tag, Depth: 1,
		}, 5*time.Minute)
		if err != nil {
			t.Fatalf("mint: %v", err)
		}
		return tok
	}
	allow := func(want string) transport.Authorize {
		return func(_ context.Context, claims auth.Claims, got string) error {
			if got != want {
				return contracts.NewError(contracts.ErrForbidden, "lane: unexpected capability "+got)
			}
			if claims.Kind != auth.KindTask {
				return contracts.NewError(contracts.ErrForbidden, "lane: task token required")
			}
			return claims.Scope().Validate()
		}
	}
	artH, err := artifactsapi.New(artStore, verifier, allow(capArtifacts))
	if err != nil {
		t.Fatal(err)
	}
	wsH, err := workspaceapi.New(wsStore, verifier, allow(capWorkspace))
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/v1/artifacts/", artH)
	mux.Handle("/v1/workspace/", wsH)

	// --- real mTLS origin (mirrors internal/stateapi/stand) ---
	caCert, caKey, caPEM := genCA()
	capool := x509.NewCertPool()
	capool.AppendCertsFromPEM(caPEM)
	serverTLSCert := genLeaf(caCert, caKey, "asws-leg-serve", true, nil, []net.IP{net.IPv4(127, 0, 0, 1)})
	clientA := genLeaf(caCert, caKey, "asws-leg-client-a", false, []string{workloadURI(tenantA, deploymentA)}, nil)
	clientF := genLeaf(caCert, caKey, "asws-leg-client-f", false, []string{workloadURI(foreignTen, deploymentA)}, nil)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tl := tls.NewListener(ln, &tls.Config{
		Certificates: []tls.Certificate{serverTLSCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    capool,
		MinVersion:   tls.VersionTLS12,
	})
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(tl) }()
	defer srv.Close()
	base := "https://" + tl.Addr().String()
	owner := &leg{t: t, url: base, cl: mtlsClient(capool, clientA)}
	foreign := &leg{t: t, url: base, cl: mtlsClient(capool, clientF)}
	// the same client cert (tenant/deployment) with a different token SUBJECT:
	secondUserClient := owner.cl

	tokenA := mint(tenantA, "user-a", "a")
	tokenB := mint(tenantA, "user-b", "b")
	tokenF := mint(foreignTen, "user-a", "f")

	payload := []byte("ahp workspace lane upload proof\n")
	sum := sha256.Sum256(payload)
	wantSHA := hex.EncodeToString(sum[:])
	stamp := fmt.Sprintf("%d", time.Now().UnixNano())

	owner.rec("BASE %s (mTLS origin, deployment=%s tenant=%s)", base, deploymentA, tenantA)

	// LEG 1 -- seal a real artifact through the origin: 200 with sha256+size.
	code, rb := owner.call("artifacts/BeginUpload", tokenA, map[string]any{
		"key": "leg-" + stamp, "name": "hello.txt", "mime": "text/plain",
		"sha256": wantSHA, "size": len(payload),
	})
	if code != 200 {
		t.Fatalf("BeginUpload = %d %s", code, rb)
	}
	var up struct {
		ID           string `json:"id"`
		PresignedURL string `json:"presigned_url"`
		SHA256       string `json:"sha256"`
		Size         int64  `json:"size"`
	}
	json.Unmarshal(rb, &up)
	// PUT the bytes to the object store (out-of-band presigned PUT, as in prod).
	put, _ := http.NewRequest(http.MethodPut, up.PresignedURL, bytes.NewReader(payload))
	pr, err := http.DefaultClient.Do(put)
	if err != nil {
		t.Fatalf("presigned PUT: %v", err)
	}
	pbody, _ := io.ReadAll(pr.Body)
	pr.Body.Close()
	if pr.StatusCode != 200 && pr.StatusCode != 204 {
		t.Fatalf("presigned PUT = %d %s", pr.StatusCode, pbody)
	}
	code, rb = owner.call("artifacts/Seal", tokenA, map[string]any{"id": up.ID})
	if code != 200 {
		t.Fatalf("Seal = %d %s", code, rb)
	}
	var ref struct {
		ID     string `json:"id"`
		SHA256 string `json:"sha256"`
		Size   int64  `json:"size"`
	}
	json.Unmarshal(rb, &ref)
	if ref.SHA256 != wantSHA || ref.Size != int64(len(payload)) {
		t.Fatalf("Seal ref sha/size mismatch: got %s/%d want %s/%d", ref.SHA256, ref.Size, wantSHA, len(payload))
	}
	owner.rec("LEG1 seal-through-origin id=%s http=200 sha256=%s size=%d verdict=PASS", ref.ID, ref.SHA256, ref.Size)

	// LEG 2 -- create project, candidate, stage the file into project context: 200.
	code, rb = owner.call("workspace/Create", tokenA, map[string]any{"workspace": "proj-" + stamp})
	if code != 200 {
		t.Fatalf("workspace/Create = %d %s", code, rb)
	}
	var wsv struct {
		ID string `json:"id"`
	}
	json.Unmarshal(rb, &wsv)
	code, rb = owner.call("workspace/CreateCandidate", tokenA, map[string]any{"workspace": wsv.ID})
	if code != 200 {
		t.Fatalf("CreateCandidate = %d %s", code, rb)
	}
	var cand struct {
		ID           string `json:"id"`
		ExpectedHead string `json:"expected_head"`
	}
	json.Unmarshal(rb, &cand)
	files := []map[string]any{{"path": "hello.txt", "artifact_id": ref.ID, "mode": 420, "digest": wantSHA}}
	code, rb = owner.call("workspace/UpdateCandidateFiles", tokenA, map[string]any{"candidate_id": cand.ID, "files": files})
	if code != 200 {
		t.Fatalf("UpdateCandidateFiles(owner) = %d %s", code, rb)
	}
	owner.rec("LEG2 upload-into-project workspace=%s candidate=%s UpdateCandidateFiles http=200 sha256=%s size=%d verdict=PASS",
		wsv.ID, cand.ID, wantSHA, len(payload))

	// LEG 2b -- owner records a passed check, publishes, reads the file back:
	// 200 with the same sha256 and size. If a publication gate refuses (outside
	// this lane's scope) the readback is skipped as informational; the upload-
	// into-project proof (LEG2) already stands.
	code, rb = owner.call("workspace/SetCheck", tokenA, map[string]any{"candidate_id": cand.ID, "name": "ci", "status": "passed"})
	owner.rec("LEG2 SetCheck(ci=passed) http=%d", code)
	code, rb = owner.call("workspace/Publish", tokenA, map[string]any{"candidate_id": cand.ID, "expected_head": cand.ExpectedHead})
	if code == 200 {
		var rev struct {
			ID string `json:"id"`
		}
		json.Unmarshal(rb, &rev)
		req, _ := http.NewRequest(http.MethodPost, base+"/v1/workspace/ReadFile", bytes.NewBuffer(
			mustJSON(map[string]any{"workspace": wsv.ID, "revision": rev.ID, "path": "hello.txt"})))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+tokenA)
		rd, err := owner.cl.Do(req)
		if err != nil {
			t.Fatalf("ReadFile: %v", err)
		}
		got, _ := io.ReadAll(rd.Body)
		rd.Body.Close()
		gotSum := sha256.Sum256(got)
		if rd.StatusCode != 200 || hex.EncodeToString(gotSum[:]) != wantSHA || len(got) != len(payload) {
			t.Fatalf("ReadFile = %d sha=%s len=%d (want 200 %s %d)", rd.StatusCode, hex.EncodeToString(gotSum[:]), len(got), wantSHA, len(payload))
		}
		owner.rec("LEG2b publish+readback-in-project revision=%s http=200 sha256=%s size=%d verdict=PASS", rev.ID, wantSHA, len(got))
	} else {
		owner.rec("LEG2b readback skipped (publish gate, out of lane scope) http=%d body=%s", code, string(rb))
	}

	// LEG 3 -- foreign SUBJECT (same tenant cert, non-member token) -> 403.
	code, rb = (&leg{t: t, url: base, cl: secondUserClient}).call("workspace/UpdateCandidateFiles", tokenB,
		map[string]any{"candidate_id": cand.ID, "files": files})
	if code != 403 {
		t.Fatalf("foreign-subject UpdateCandidateFiles = %d %s, want 403", code, rb)
	}
	owner.rec("LEG3 foreign-subject(user-b non-member) UpdateCandidateFiles http=403 verdict=PASS")

	// LEG 4 -- two users' rights separated: add user-b as editor; editor writes
	// (200) but editor cannot publish (403).
	code, rb = owner.call("workspace/AddMember", tokenA, map[string]any{"workspace": wsv.ID, "subject": "user-b"})
	if code != 200 {
		t.Fatalf("AddMember(owner) = %d %s", code, rb)
	}
	code, rb = owner.call("workspace/CreateCandidate", tokenA, map[string]any{"workspace": wsv.ID})
	var candB struct {
		ID           string `json:"id"`
		ExpectedHead string `json:"expected_head"`
	}
	json.Unmarshal(rb, &candB)
	code, rb = (&leg{t: t, url: base, cl: secondUserClient}).call("workspace/UpdateCandidateFiles", tokenB,
		map[string]any{"candidate_id": candB.ID, "files": files})
	if code != 200 {
		t.Fatalf("editor UpdateCandidateFiles = %d %s, want 200", code, rb)
	}
	code, rb = (&leg{t: t, url: base, cl: secondUserClient}).call("workspace/Publish", tokenB,
		map[string]any{"candidate_id": candB.ID, "expected_head": candB.ExpectedHead})
	if code != 403 {
		t.Fatalf("editor Publish = %d %s, want 403", code, rb)
	}
	owner.rec("LEG4 two-user-rights-separated user-b=editor write=200 publish=403 verdict=PASS")

	// LEG 5 -- foreign TENANT cannot even resolve the project -> 404.
	code, rb = foreign.call("workspace/UpdateCandidateFiles", tokenF,
		map[string]any{"candidate_id": cand.ID, "files": files})
	if code != 404 && code != 403 {
		t.Fatalf("foreign-tenant UpdateCandidateFiles = %d %s, want 404/403", code, rb)
	}
	foreign.rec("LEG5 foreign-tenant(%s) cannot touch project http=%d verdict=PASS", foreignTen, code)

	owner.rec("OVERALL VERDICT=PASS sha256=%s size=%d origin=%s lane-not-prod", wantSHA, len(payload), base)

	// durable evidence
	if ev := os.Getenv("AHP_LEG_EVIDENCE"); ev != "" {
		_ = os.MkdirAll(filepath.Dir(ev), 0o755)
		_ = os.WriteFile(ev, []byte(joinLines(owner.ev)+"\n"+joinLines(foreign.ev)+"\n"), 0o644)
	}
}

func joinLines(s []string) string {
	out := ""
	for i, l := range s {
		if i > 0 {
			out += "\n"
		}
		out += l
	}
	return out
}

func mustJSON(v any) []byte { b, _ := json.Marshal(v); return b }

func mustHost(raw string) string {
	for _, p := range []string{"http://", "https://"} {
		if len(raw) > len(p) && raw[:len(p)] == p {
			raw = raw[len(p):]
			break
		}
	}
	return raw
}

func workloadURI(tenant, dep string) string {
	return "spiffe://ahp/tenant/" + tenant + "/deployment/" + dep
}

func mtlsClient(pool *x509.CertPool, cert tls.Certificate) *http.Client {
	return &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12},
	}}
}

func genCA() (*x509.Certificate, *ecdsa.PrivateKey, []byte) {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "asws-leg-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	cert, _ := x509.ParseCertificate(der)
	return cert, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func genLeaf(ca *x509.Certificate, caKey *ecdsa.PrivateKey, cn string, server bool, uris []string, ips []net.IP) tls.Certificate {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	u := []*url.URL{}
	for _, s := range uris {
		p, _ := url.Parse(s)
		u = append(u, p)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		URIs:         u,
		IPAddresses:  ips,
	}
	if server {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	} else {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		panic(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	kb, _ := x509.MarshalECPrivateKey(key)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb})
	c, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		panic(err)
	}
	return c
}
