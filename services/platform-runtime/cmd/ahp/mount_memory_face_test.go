//go:build ahp_data

package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/ahp-platform/ahp/internal/memory"
	"github.com/ahp-platform/ahp/internal/memory/memoryapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// This file is the legacy proposal/review contract leg for the Memory mount: it drives the
// MOUNTED /v1/memory face over real mTLS + real Ed25519 task tokens against
// real PostgreSQL (pgvector) and a real object store, and asserts the memory
// LIFECYCLE and the read-side ACL as a caller experiences them -- not the
// package's internal unit behaviour.
//
// Gate: AHP_MOUNT_MEMORY=1 plus the stand env (AHP_TEST_DATABASE_URL pointing at
// a DEDICATED database -- the artifact and memory migrations create schemas and
// the leg drops them again -- AHP_S3_ENDPOINT, MinIO credentials,
// AHP_DEV_JWT_SEED). Once the gate is on, a missing variable is a FAILURE, never
// a silent pass (reports/mac-director/pitfalls-product.md §2). With the gate off
// the leg skips: the default suite stays runnable without infrastructure.

// memFaceGate reads the gate and the stand env; it never prints a secret.
func memFaceGate(t *testing.T) string {
	t.Helper()
	if strings.TrimSpace(os.Getenv("AHP_MOUNT_MEMORY")) != "1" {
		t.Skip("SKIPPED by design: mounted /v1/memory face leg; set AHP_MOUNT_MEMORY=1 and source the stand env")
	}
	for _, name := range []string{"AHP_TEST_DATABASE_URL", "AHP_S3_ENDPOINT", "AHP_MINIO_ROOT_USER", "AHP_MINIO_ROOT_PASSWORD", "AHP_DEV_JWT_SEED"} {
		if strings.TrimSpace(os.Getenv(name)) == "" {
			t.Fatalf("AHP_MOUNT_MEMORY=1 but %s is empty: source /tmp/ahp-test-env.sh first", name)
		}
	}
	return strings.TrimSpace(os.Getenv("AHP_TEST_DATABASE_URL"))
}

func memFaceEnvOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

// --- test PKI: one throwaway CA, one server certificate, one client certificate
// per workload. The client certificates carry the SPIFFE workload URI the
// transport reads, which is what makes the workload binding real here.

type memFaceCA struct {
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
	pool    *x509.CertPool
	server  tls.Certificate
	signErr error
}

func newMemFaceCA(t *testing.T) *memFaceCA {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "h20-memory-face-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("ca cert: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse ca: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(caCert)

	srvKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("server key: %v", err)
	}
	srvTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.IPv6loopback},
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTmpl, caCert, &srvKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("server cert: %v", err)
	}
	return &memFaceCA{cert: caCert, key: caKey, pool: pool,
		server: tls.Certificate{Certificate: [][]byte{srvDER}, PrivateKey: srvKey}}
}

// client mints a client certificate bound to one (tenant, deployment) workload.
func (c *memFaceCA) client(t *testing.T, tenant, deployment string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("client key: %v", err)
	}
	uri, err := url.Parse(fmt.Sprintf("spiffe://ahp/tenant/%s/deployment/%s", tenant, deployment))
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: tenant + "/" + deployment},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		URIs:         []*url.URL{uri},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.cert, &key.PublicKey, c.key)
	if err != nil {
		t.Fatalf("client cert: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func (c *memFaceCA) httpClient(t *testing.T, cert tls.Certificate) *http.Client {
	t.Helper()
	return &http.Client{
		Timeout:   60 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: c.pool, ServerName: "localhost", Certificates: []tls.Certificate{cert}}},
	}
}

// memFace is the harness: a live HTTPS listener serving the mounted face.
type memFace struct {
	t        *testing.T
	base     string
	ca       *memFaceCA
	signer   *auth.Signer
	clients  map[string]*http.Client
	suffix   string
	tenantA  string
	tenantB  string
	deploy   string
	subject1 string
	subject2 string
	reviewer string
}

func (f *memFace) client(t *testing.T, tenant, deployment string) *http.Client {
	t.Helper()
	key := tenant + "/" + deployment
	if c, ok := f.clients[key]; ok {
		return c
	}
	c := f.ca.httpClient(t, f.ca.client(t, tenant, deployment))
	f.clients[key] = c
	return c
}

// token mints a task token for one subject.
func (f *memFace) token(t *testing.T, tenant, deployment, subject, task string) string {
	t.Helper()
	tok, err := f.signer.Sign(auth.Claims{
		Audience:   "ahp",
		Kind:       auth.KindTask,
		Tenant:     tenant,
		Deployment: deployment,
		Subject:    subject,
		Task:       task,
		Root:       task,
		Context:    "ctx-" + task,
		Grant:      "grant-" + task,
	}, 900*time.Second)
	if err != nil {
		t.Fatalf("mint token: %v", err)
	}
	return tok
}

// call drives POST /v1/memory/{op} with a bearer token over the mTLS client.
func (f *memFace) call(t *testing.T, tenant, deployment, subject, task, op string, body any) (int, []byte) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, f.base+"/v1/memory/"+op, bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+f.token(t, tenant, deployment, subject, task))
	resp, err := f.client(t, tenant, deployment).Do(req)
	if err != nil {
		t.Fatalf("%s: %v", op, err)
	}
	defer func() { _ = resp.Body.Close() }()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out
}

// callWithToken is call() with a caller-supplied token: the cross-scope leg
// needs to present a token from one workload on another workload's certificate.
func (f *memFace) callWithToken(t *testing.T, certTenant, certDeployment, token, op string, body any) (int, []byte) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, f.base+"/v1/memory/"+op, bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := f.client(t, certTenant, certDeployment).Do(req)
	if err != nil {
		t.Fatalf("%s: %v", op, err)
	}
	defer func() { _ = resp.Body.Close() }()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out
}

func memFaceSuffix(t *testing.T) string {
	t.Helper()
	var b [5]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b[:])
}

func mustStatus(t *testing.T, want, got int, body []byte, what string) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: status = %d, want %d (body=%s)", what, got, want, truncateBody(body))
	}
}

func truncateBody(b []byte) string {
	s := string(b)
	if len(s) > 400 {
		return s[:400] + "..."
	}
	return s
}

// TestMemoryFaceLifecycleAndACL plays the whole memory journey against the
// mounted face: propose -> read -> search -> review (capability-gated) ->
// supersede (update) -> delete, plus the two ACL properties the PDF cares about
// (live workspace membership decides visibility; another tenant sees nothing)
// and the transport property (a token cannot travel to another workload).
func TestMemoryFaceLifecycleAndACL(t *testing.T) {
	dsn := memFaceGate(t)
	suffix := memFaceSuffix(t)
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)

	bucket := "ahp-h20mem-" + suffix
	t.Setenv("AHP_S3_BUCKET", bucket)
	if v := memFaceEnvOr("AHP_S3_REGION", ""); v != "" {
		t.Setenv("AHP_S3_REGION", v)
	}

	s := &server{pool: pool}
	artStore, wsStore, _, err := s.newDataPlanes(ctx)
	if err != nil {
		t.Fatalf("newDataPlanes: %v", err)
	}
	if err := artStore.Migrate(ctx); err != nil {
		t.Fatalf("artifacts migrate: %v", err)
	}
	if err := wsStore.Migrate(ctx); err != nil {
		t.Fatalf("workspace migrate: %v", err)
	}
	// Teardown of everything this leg created: the three schemas it migrated and
	// the one bucket it made. Nothing shared is touched.
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		_, _ = pool.Exec(c, `DROP SCHEMA IF EXISTS ahp_memory CASCADE`)
		_, _ = pool.Exec(c, `DROP SCHEMA IF EXISTS ahp_workspace CASCADE`)
		_, _ = pool.Exec(c, `DROP SCHEMA IF EXISTS ahp_artifacts CASCADE`)
		if u, perr := url.Parse(memFaceEnvOr("AHP_S3_ENDPOINT", "")); perr == nil && u.Host != "" {
			mc, cerr := minio.New(u.Host, &minio.Options{
				Creds:  credentials.NewStaticV4(os.Getenv("AHP_MINIO_ROOT_USER"), os.Getenv("AHP_MINIO_ROOT_PASSWORD"), ""),
				Secure: u.Scheme == "https",
			})
			if cerr == nil {
				for obj := range mc.ListObjects(c, bucket, minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
					if obj.Err != nil {
						break
					}
					_ = mc.RemoveObject(c, bucket, obj.Key, minio.RemoveObjectOptions{VersionID: obj.VersionID})
				}
				_ = mc.RemoveBucket(c, bucket)
			}
		}
	})

	// The test authority is deliberately NOT permissive: the coarse plane
	// capability is granted, but memory:review belongs to ONE subject, so every
	// review that succeeds proves the mount actually carried the fine
	// capability through to the authority.
	tenantA := "h20mfa" + suffix
	tenantB := "h20mfb" + suffix
	deploy := "dmem"
	subject1 := "s1" + suffix
	subject2 := "s2" + suffix
	reviewer := "rev" + suffix
	authorize := func(_ context.Context, claims auth.Claims, capability string) error {
		switch capability {
		case "memory", "memory:label", "memory:delete", "memory:delete-subject":
			return nil
		case memory.CapabilityReview:
			if claims.Subject == reviewer || claims.Subject == subject1 {
				return nil
			}
			return contracts.NewError(contracts.ErrForbidden, "test authority: subject does not hold memory:review")
		}
		return contracts.NewError(contracts.ErrForbidden, "test authority: capability not granted")
	}

	memStore, err := newMemoryStore(pool, artStore, wsStore, authorize)
	if err != nil {
		t.Fatalf("newMemoryStore: %v", err)
	}
	if err := memStore.Migrate(ctx); err != nil {
		t.Fatalf("memory migrate: %v", err)
	}
	verifier, signer := devIdentityAuth()
	handler, err := memoryapi.New(memStore, verifier, authorize)
	if err != nil {
		t.Fatalf("memoryapi.New: %v", err)
	}
	mux := http.NewServeMux()
	mountMemoryHandlers(mux, handler)

	ca := newMemFaceCA(t)
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{ca.server},
		ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: ca.pool}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	hs := &http.Server{Handler: mux, TLSConfig: cfg, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = hs.Serve(tls.NewListener(ln, cfg)) }()
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = hs.Shutdown(c)
	})

	f := &memFace{t: t, base: "https://localhost:" + fmt.Sprint(ln.Addr().(*net.TCPAddr).Port),
		ca: ca, signer: signer, clients: map[string]*http.Client{}, suffix: suffix,
		tenantA: tenantA, tenantB: tenantB, deploy: deploy,
		subject1: subject1, subject2: subject2, reviewer: reviewer}

	// A workspace owned by subject1: the mount wires memory.WithWorkspaces to
	// THIS store's live membership query, so membership changes here change what
	// the memory face can read -- with no restart and no cached copy.
	wsName := "ws" + suffix
	if _, err := wsStore.Create(ctx, contracts.Scope{Tenant: tenantA, Deployment: deploy, Subject: subject1}, wsName); err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	var idUser, idUser2, idWS string

	t.Run("create_propose_then_read", func(t *testing.T) {
		st, body := f.call(t, tenantA, deploy, subject1, "mk-user", "Propose", map[string]any{
			"kind": "user", "text": "marker-user-" + suffix + " preferred build tool is bazel",
			"op_key": "op-user-" + suffix,
		})
		mustStatus(t, http.StatusOK, st, body, "Propose user")
		var res struct {
			Memory struct {
				ID    string `json:"id"`
				State string `json:"state"`
				Kind  string `json:"kind"`
			} `json:"memory"`
		}
		if err := json.Unmarshal(body, &res); err != nil {
			t.Fatalf("decode propose: %v (%s)", err, truncateBody(body))
		}
		idUser = res.Memory.ID
		if idUser == "" || res.Memory.State != "proposed" || res.Memory.Kind != "user" {
			t.Fatalf("unexpected proposal: %s", truncateBody(body))
		}
		st, body = f.call(t, tenantA, deploy, subject1, "rd-user", "Read", map[string]any{"id": idUser})
		mustStatus(t, http.StatusOK, st, body, "Read user")
		if !strings.Contains(string(body), `"state":"proposed"`) {
			t.Fatalf("read body = %s", truncateBody(body))
		}
		t.Logf("EVIDENCE lifecycle: propose->read ok id=%s state=proposed", idUser)
	})

	t.Run("search_excludes_proposed_but_owner_can_read", func(t *testing.T) {
		st, body := f.call(t, tenantA, deploy, subject1, "srch-user", "Search",
			map[string]any{"query": "marker-user-" + suffix})
		mustStatus(t, http.StatusOK, st, body, "Search user")
		if strings.Contains(string(body), idUser) {
			t.Fatalf("proposal crossed the retrieval review gate: %s", truncateBody(body))
		}
		// PDF rev3.2 section 7 distinguishes proposed/active and review_required.
		// Owner inspection remains available before publication into retrieval.
		st, body = f.call(t, tenantA, deploy, subject1, "inspect-proposed", "Read", map[string]any{"id": idUser})
		mustStatus(t, http.StatusOK, st, body, "Owner inspects proposal")
		if !strings.Contains(string(body), `"state":"proposed"`) {
			t.Fatalf("proposal disappeared from owner inspection: %s", truncateBody(body))
		}
	})

	t.Run("workspace_memory_requires_live_membership", func(t *testing.T) {
		st, body := f.call(t, tenantA, deploy, subject1, "mk-ws", "Propose", map[string]any{
			"kind": "workspace", "workspace": wsName,
			"text":     "marker-ws-" + suffix + " release train freezes on fridays",
			"op_key":   "op-ws-" + suffix,
			"evidence": []map[string]any{{"source_kind": "test-run", "detail": "mounted face leg"}},
		})
		mustStatus(t, http.StatusOK, st, body, "Propose workspace")
		var res struct {
			Memory struct {
				ID    string `json:"id"`
				State string `json:"state"`
			} `json:"memory"`
		}
		if err := json.Unmarshal(body, &res); err != nil {
			t.Fatalf("decode: %v (%s)", err, truncateBody(body))
		}
		idWS = res.Memory.ID
		if idWS == "" {
			t.Fatalf("no id: %s", truncateBody(body))
		}

		// A non-member cannot even review: the capability gate fires first.
		st, body = f.call(t, tenantA, deploy, subject2, "rev-denied", "Review",
			map[string]any{"op_key": "rev-denied-" + suffix, "id": idWS, "decision": "approve"})
		mustStatus(t, http.StatusForbidden, st, body, "Review without capability")
		if !strings.Contains(string(body), "memory:review") {
			t.Fatalf("refusal does not name the missing capability: %s", truncateBody(body))
		}

		// A review capability does not bypass the owner's source read ACL.
		st, body = f.call(t, tenantA, deploy, reviewer, "rev-foreign", "Review",
			map[string]any{"id": idWS, "decision": "approve", "op_key": "rev-foreign-" + suffix})
		mustStatus(t, http.StatusNotFound, st, body, "Foreign reviewer cannot inspect proposal")
		// The owner has both the source visibility and the review capability.
		st, body = f.call(t, tenantA, deploy, subject1, "rev-ok", "Review",
			map[string]any{"id": idWS, "decision": "approve", "op_key": "rev-ok-" + suffix})
		mustStatus(t, http.StatusOK, st, body, "Review by authorized owner")
		if !strings.Contains(string(body), `"state":"active"`) {
			t.Fatalf("review body = %s", truncateBody(body))
		}

		// Member sees it, non-member does not: neither by search nor by id.
		st, body = f.call(t, tenantA, deploy, subject1, "srch-member", "Search",
			map[string]any{"query": "marker-ws-" + suffix})
		mustStatus(t, http.StatusOK, st, body, "Search as member")
		if !strings.Contains(string(body), idWS) {
			t.Fatalf("member could not find the workspace memory: %s", truncateBody(body))
		}
		st, body = f.call(t, tenantA, deploy, subject2, "srch-nonmember", "Search",
			map[string]any{"query": "marker-ws-" + suffix})
		mustStatus(t, http.StatusOK, st, body, "Search as non-member")
		if strings.Contains(string(body), idWS) {
			t.Fatalf("a non-member read workspace memory: %s", truncateBody(body))
		}
		st, body = f.call(t, tenantA, deploy, subject2, "rd-nonmember", "Read", map[string]any{"id": idWS})
		mustStatus(t, http.StatusNotFound, st, body, "Read by non-member (no oracle)")

		// Grant membership -> the very next call reads it. This is the live
		// membership hook the mount wired, not a token-scope copy.
		if err := wsStore.AddMember(ctx, contracts.Scope{Tenant: tenantA, Deployment: deploy, Subject: subject1}, wsName, subject2); err != nil {
			t.Fatalf("AddMember: %v", err)
		}
		st, body = f.call(t, tenantA, deploy, subject2, "rd-member", "Read", map[string]any{"id": idWS})
		mustStatus(t, http.StatusOK, st, body, "Read after membership grant")
		t.Logf("EVIDENCE ACL: workspace memory invisible to non-member, visible immediately after AddMember")
	})

	t.Run("foreign_tenant_sees_nothing", func(t *testing.T) {
		// Same subject string, different tenant: the scope comes from the
		// certificate + token pair, never from the body.
		st, body := f.call(t, tenantB, deploy, subject1, "x-read", "Read", map[string]any{"id": idWS})
		mustStatus(t, http.StatusNotFound, st, body, "Foreign tenant Read")
		st, body = f.call(t, tenantB, deploy, subject1, "x-search", "Search",
			map[string]any{"query": "marker-ws-" + suffix})
		mustStatus(t, http.StatusOK, st, body, "Foreign tenant Search")
		if strings.Contains(string(body), idWS) || strings.Contains(string(body), idUser) {
			t.Fatalf("cross-tenant leak: %s", truncateBody(body))
		}
		t.Logf("EVIDENCE ACL: tenant %s could not read or find tenant %s records", tenantB, tenantA)
	})

	t.Run("token_cannot_travel_to_another_workload", func(t *testing.T) {
		tok := f.token(t, tenantA, deploy, subject1, "travel")
		st, body := f.callWithToken(t, tenantB, deploy, tok, "Search", map[string]any{"query": "x"})
		mustStatus(t, http.StatusForbidden, st, body, "Token from workload A on workload B")
		t.Logf("EVIDENCE transport: foreign-workload bearer refused")
	})

	t.Run("update_by_supersede", func(t *testing.T) {
		st, body := f.call(t, tenantA, deploy, subject1, "rev-u1", "Review",
			map[string]any{"op_key": "rev-u1-" + suffix, "id": idUser, "decision": "approve"})
		mustStatus(t, http.StatusOK, st, body, "Approve first user record")
		st, body = f.call(t, tenantA, deploy, subject1, "search-approved", "Search", map[string]any{"query": "marker-user-" + suffix})
		mustStatus(t, http.StatusOK, st, body, "Search approved user record")
		if !strings.Contains(string(body), idUser) {
			t.Fatalf("approved record missing from retrieval: %s", truncateBody(body))
		}

		st, body = f.call(t, tenantA, deploy, subject1, "mk-u2", "Propose", map[string]any{
			"kind": "user", "text": "marker-user2-" + suffix + " preferred build tool is nix",
			"op_key": "op-user2-" + suffix, "supersedes": idUser,
		})
		mustStatus(t, http.StatusOK, st, body, "Propose superseding record")
		var res struct {
			Memory struct {
				ID string `json:"id"`
			} `json:"memory"`
		}
		if err := json.Unmarshal(body, &res); err != nil {
			t.Fatalf("decode: %v (%s)", err, truncateBody(body))
		}
		idUser2 = res.Memory.ID
		st, body = f.call(t, tenantA, deploy, subject1, "rev-u2", "Review",
			map[string]any{"op_key": "rev-u2-" + suffix, "id": idUser2, "decision": "approve"})
		mustStatus(t, http.StatusOK, st, body, "Approve superseding record")

		st, body = f.call(t, tenantA, deploy, subject1, "rd-u1", "Read", map[string]any{"id": idUser})
		mustStatus(t, http.StatusOK, st, body, "Read superseded record")
		if !strings.Contains(string(body), `"state":"superseded"`) {
			t.Fatalf("prior version was not superseded: %s", truncateBody(body))
		}
		t.Logf("EVIDENCE lifecycle: supersede moved %s to superseded, %s is active", idUser, idUser2)
	})

	t.Run("delete_tombstones", func(t *testing.T) {
		st, body := f.call(t, tenantA, deploy, subject1, "del-u2", "Delete",
			map[string]any{"id": idUser2, "op_key": "del-user2-" + suffix})
		mustStatus(t, http.StatusOK, st, body, "Delete")
		st, body = f.call(t, tenantA, deploy, subject1, "rd-u2", "Read", map[string]any{"id": idUser2})
		mustStatus(t, http.StatusNotFound, st, body, "Read after delete")
		st, body = f.call(t, tenantA, deploy, subject1, "srch-u2", "Search",
			map[string]any{"query": "marker-user2-" + suffix})
		mustStatus(t, http.StatusOK, st, body, "Search after delete")
		if strings.Contains(string(body), idUser2) {
			t.Fatalf("deleted record still searchable: %s", truncateBody(body))
		}
		t.Logf("EVIDENCE lifecycle: delete tombstoned %s (read 404, search empty)", idUser2)
	})

	t.Run("review_twice_is_not_silent", func(t *testing.T) {
		st, body := f.call(t, tenantA, deploy, subject1, "rev-twice", "Review",
			map[string]any{"op_key": "rev-twice-" + suffix, "id": idUser2, "decision": "approve"})
		if st == http.StatusOK {
			t.Fatalf("reviewing a deleted record succeeded: %s", truncateBody(body))
		}
		t.Logf("EVIDENCE lifecycle: re-review of a non-proposed record refused with %d", st)
	})
}
