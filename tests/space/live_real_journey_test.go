//go:build live_real

// Package space_test drives ONE end-to-end, fail-closed LIVE journey against
// REAL services only — no httptest SUT for the model/MinIO, no in-process mock
// State, no fixed fixture bytes:
//
//	MANDATORY: native-Qwen (real LLM generation bytes) -> AHP's OWN product
//	save_artifact tool (core.NewSaveArtifactTool over a real artifacts.Store,
//	dispatched via ToolRegistry/MultiExecutor as the runner wires it) -> real
//	MinIO (:29000) versioned artifact sealed by AHP Artifacts (sha256 PUT==GET)
//	-> durable State over real PostgreSQL (:25432) -> process restart/restore ->
//	foreign-scope private-context isolation. The production model<->tool loop is
//	also run on real Qwen with the same tool registered + durable sink (iv #74).
//
//	OPTIONAL (NOT a gate): a real Emil provenance hop (A2A message/send via the
//	product adapter) enriches the State event only when EMIL_BASE_URL is set and
//	reachable. Emil's container name/port/immutability are NOT product
//	requirements (direct user clarification 2026-10-30,
//	decisions/2026-10-30-emil-image-scope-correction), so its absence never fails
//	the journey — but Emil is never faked either (no httptest/:19081/newMockState).
//
// Availability (HTTP 200) is never proof: every stage asserts real semantics.
// Missing/foreign MANDATORY services FAIL (t.Fatalf); there is no t.Skip and no
// httptest/newMockState for the proven path. The ONLY httptest.Server here is the
// State server-under-test — AHP product code (internal/stateapi) over real PG.
package space_test

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
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/ahp-platform/ahp/adapters/emil"
	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/stateapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

const (
	ljAudience = "ahp"
	ljIssuer   = "https://identity.test"
	ljKID      = "k1"
	ljSubject  = "user-ada"
)

type ljLive struct {
	dsn   string
	llmB  string
	llmM  string
	llmK  string
	s3URL string
	s3U   string
	s3P   string
	emilB string
	// emilReach is true only when EMIL_BASE_URL is set AND its agent-card is
	// reachable. Emil is OPTIONAL provenance, never a gate, so its absence only
	// skips the Emil hop (see decisions/2026-10-30-emil-image-scope-correction).
	emilReach bool
}

// ljRequire gathers and validates all required LIVE inputs. It FAILS (never
// skips) when anything is missing or non-native, so a green run is impossible
// without the real services present and real native-Qwen configured.
func ljRequire(t *testing.T) ljLive {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		t.Fatalf("LIVE journey requires a real PostgreSQL DSN (AHP_TEST_DATABASE_URL/DATABASE_URL): fail-closed, not skip")
	}
	l := ljLive{
		dsn:   dsn,
		llmB:  os.Getenv("AHP_LLM_BASE_URL"),
		llmM:  os.Getenv("AHP_LLM_MODEL"),
		llmK:  os.Getenv("AHP_LLM_API_KEY"),
		s3URL: os.Getenv("AHP_S3_ENDPOINT"),
		s3U:   os.Getenv("AHP_MINIO_ROOT_USER"),
		s3P:   os.Getenv("AHP_MINIO_ROOT_PASSWORD"),
		emilB: os.Getenv("EMIL_BASE_URL"),
	}
	host, err := classifyNativeQwen(l.llmB, l.llmM)
	if err != nil {
		t.Fatalf("native-Qwen gate: %v", err)
	}
	if l.llmK == "" {
		t.Fatalf("AHP_LLM_API_KEY unset: live journey must FAIL, not skip")
	}
	if l.s3URL == "" || l.s3U == "" || l.s3P == "" {
		t.Fatalf("MinIO endpoint/credentials unset: live journey must FAIL, not skip")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if !ljGetOK(ctx, t, strings.TrimRight(l.s3URL, "/")+"/minio/health/live") {
		t.Fatalf("MinIO health not reachable at %s: real MinIO required", l.s3URL)
	}
	// Emil is OPTIONAL provenance under the de-gated directive, NOT a gate: an
	// unset/unreachable Emil merely skips the Emil hop. The MANDATORY journey is
	// native-Qwen -> OUR save_artifact tool -> real MinIO seal -> durable PG ->
	// restart -> private-context, all fail-closed. Emil must NEVER be faked with
	// httptest/newMockState/:19081.
	if l.emilB == "" {
		t.Logf("EMIL_BASE_URL unset: Emil provenance hop SKIPPED by design (Emil is not a gate).")
	} else if !ljGetOK(ctx, t, l.emilB+"/.well-known/agent-card.json") {
		t.Logf("EMIL_BASE_URL=%s agent-card unreachable: Emil provenance hop SKIPPED (not a gate).", l.emilB)
	} else {
		l.emilReach = true
	}
	t.Logf("live infra OK (mandatory): native-Qwen host=%s model=%s; MinIO=%s; PG=127.0.0.1:25432; Emil(optional)=%s reachable=%v",
		host, l.llmM, l.s3URL, l.emilB, l.emilReach)
	return l
}

func ljGetOK(ctx context.Context, t *testing.T, u string) bool {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		t.Fatalf("bad url %s: %v", u, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Logf("GET %s err: %v", u, err)
		return false
	}
	defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()
	t.Logf("GET %s -> %d", u, resp.StatusCode)
	return resp.StatusCode == http.StatusOK
}

// ---- native-Qwen model -------------------------------------------------------

func ljRealQwen(t *testing.T, l ljLive) *core.OpenAICompatClient {
	t.Helper()
	cli, err := core.NewOpenAICompat(l.llmB, l.llmK, l.llmM)
	if err != nil {
		t.Fatalf("NewOpenAICompat: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	resp, err := cli.Generate(ctx, &core.ModelRequest{
		Messages:  []core.ChatMessage{{Role: "user", Content: json.RawMessage(`"Reply with exactly: READY"`)}},
		MaxTokens: 16, Step: 1,
	})
	if err != nil {
		t.Fatalf("native-Qwen Generate failed (real LLM required): %v", err)
	}
	if len(resp.Content) == 0 {
		t.Fatalf("native-Qwen returned empty content")
	}
	t.Logf("native-Qwen round-trip ok: stop=%q content_bytes=%d", resp.Stop, len(resp.Content))
	return cli
}

// ---- optional Emil provenance hop (NOT a gate) ------------------------------

// ljEmilProvenance performs an OPTIONAL REAL Emil A2A message/send against a
// real core-agent (EMIL_BASE_URL) via the product adapter, used only to enrich
// the durable State event with cross-system provenance. It is NOT a readiness
// gate: any absence or transient failure returns "" (logged) rather than failing
// the mandatory journey, because Emil's container name/port/immutability are no
// longer product requirements (direct user clarification 2026-10-30,
// decisions/2026-10-30-emil-image-scope-correction). When it DOES run it uses the
// real adapter (no httptest, no in-process stand-in, no :19081 fixture) and
// records Emil's real reply text plus whether a builtin tool / core_artifact_save
// ran, so a present Emil is exercised for real without becoming a dependency.
func ljEmilProvenance(t *testing.T, l ljLive) string {
	t.Helper()
	a, err := emil.New(emil.Config{BaseURL: l.emilB, CardCacheTTL: time.Minute})
	if err != nil {
		t.Logf("emil.New (provenance) failed: %v — skipping Emil hop (not a gate)", err)
		return ""
	}
	marker := "AHP-LIVE-" + strings.ToUpper(ljRand(t, 6))
	prompt := "Reply with exactly one line that begins with the literal token " + marker +
		" followed by a short factual sentence naming the built-in tool you executed this turn."
	const maxAttempts = 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
		res, prov, err := a.Delegate(ctx, prompt)
		cancel()
		if err != nil {
			t.Logf("Emil provenance attempt %d transport failure: %v", attempt, err)
			continue
		}
		if res.State != emil.StateCompleted || strings.TrimSpace(res.Text) == "" {
			t.Logf("Emil provenance attempt %d not completed/empty: state=%q text=%q", attempt, res.State, res.Text)
			continue
		}
		var ranBuiltin, sawArtifactSave bool
		for _, tc := range prov.ToolCalls {
			if strings.HasPrefix(tc, "core_") {
				ranBuiltin = true
			}
			if tc == "core_artifact_save" {
				sawArtifactSave = true
			}
		}
		t.Logf("real Emil provenance hop OK: task=%s context=%s state=%s tool_calls=%v builtin=%v core_artifact_save=%v reply=%q",
			prov.TaskID, prov.ContextID, res.State, prov.ToolCalls, ranBuiltin, sawArtifactSave, res.Text)
		return res.Text
	}
	t.Logf("Emil provenance hop SKIPPED after %d attempts against %s (transient real fault; Emil is not a gate)", maxAttempts, l.emilB)
	return ""
}

// ---- real MinIO artifact via AHP artifacts store ----------------------------

// ljStoreBundle ties a real PG pool + real MinIO client to one scoped bucket.
type ljStoreBundle struct {
	pool   *pgxpool.Pool
	store  *artifacts.Store
	bucket string
	scope  contracts.Scope
}

func ljBootStore(t *testing.T, ctx context.Context, l ljLive, bucket string, scope contracts.Scope, migrate bool) *ljStoreBundle {
	t.Helper()
	u, err := url.Parse(l.s3URL)
	if err != nil || u.Host == "" {
		t.Fatalf("bad AHP_S3_ENDPOINT: %v", err)
	}
	pool, err := pgxpool.New(ctx, l.dsn)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	cli, err := minio.New(u.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(l.s3U, l.s3P, ""),
		Secure: u.Scheme == "https",
	})
	if err != nil {
		_ = pool.Close
		t.Fatalf("minio.New: %v", err)
	}
	store, err := artifacts.New(pool, cli, bucket,
		func(_ context.Context, _ contracts.Scope, _, _ string) error { return nil })
	if err != nil {
		_ = pool.Close
		t.Fatalf("artifacts.New (host=%s bucket=%s): %v", u.Host, bucket, err)
	}
	if migrate {
		if err := store.Migrate(ctx); err != nil {
			_ = pool.Close
			t.Fatalf("artifacts.Migrate: %v", err)
		}
	}
	return &ljStoreBundle{pool: pool, store: store, bucket: bucket, scope: scope}
}

// ljStoreSink implements core.ArtifactSink over the real AHP Data/artifacts
// store (BeginUpload -> presigned PUT to real MinIO -> Seal). It mirrors
// cmd/as-runner's production wiring so the LIVE journey proves the same code
// path the runner uses, not a test-only shortcut. Seal happens in AHP Artifacts,
// NOT in Emil's private store.
type ljStoreSink struct{ b *ljStoreBundle }

func (s *ljStoreSink) SaveArtifact(ctx context.Context, name, mime string, data []byte) (core.ArtifactHandle, error) {
	if mime == "" {
		mime = "text/plain"
	}
	sum := sha256.Sum256(data)
	want := hex.EncodeToString(sum[:])
	up, err := s.b.store.BeginUpload(ctx, s.b.scope, name, artifacts.UploadRequest{
		Name: name, MIME: mime, SHA256: want, Size: int64(len(data)),
	})
	if err != nil {
		return core.ArtifactHandle{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, up.PresignedURL, bytes.NewReader(data))
	if err != nil {
		return core.ArtifactHandle{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return core.ArtifactHandle{}, err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return core.ArtifactHandle{}, fmt.Errorf("PUT real MinIO status=%d", resp.StatusCode)
	}
	ref, err := s.b.store.Seal(ctx, s.b.scope, up.ID)
	if err != nil {
		return core.ArtifactHandle{}, err
	}
	return core.ArtifactHandle{ID: ref.ID, Version: ref.Version, SHA256: ref.SHA256, Size: ref.Size}, nil
}

// ljRealQwenBytes returns REAL native-Qwen generation bytes for use as the
// mandatory artifact content. This is a genuine LLM call (no fixture, no mock,
// no stand-in); it FAILS if the real model yields empty content, so the sealed
// artifact is always authentic native-Qwen output.
func ljRealQwenBytes(t *testing.T, l ljLive) []byte {
	t.Helper()
	cli, err := core.NewOpenAICompat(l.llmB, l.llmK, l.llmM)
	if err != nil {
		t.Fatalf("ljRealQwenBytes NewOpenAICompat: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	// Raise the artifact output volume: the mandatory artifact must carry a
	// substantive real native-Qwen document, not a 2-byte trivial stub. Ask for a
	// multi-line plain-text note and give the model room to produce it (director
	// finding: MaxTokens=16 -> 2 bytes trivial). Retries absorb LLM nondeterminism;
	// the floor assertion is fail-closed, so the journey still cannot pass on a
	// trivial artifact.
	const minArtifactBytes = 128
	const attempts = 3
	var best []byte
	var lastStop string
	for i := 0; i < attempts; i++ {
		resp, err := cli.Generate(ctx, &core.ModelRequest{
			Messages: []core.ChatMessage{
				{Role: "system", Content: json.RawMessage(`"You write information-dense plain-text technical notes. Do not use markdown or bullet glyphs."`)},
				{Role: "user", Content: json.RawMessage(`"Write a plain-text note of 6 to 10 short lines about cryptographic hash functions. Each line states exactly one distinct factual property. Output only the lines."`)},
			},
			MaxTokens: 512, Step: 1 + i,
		})
		if err != nil {
			t.Fatalf("ljRealQwenBytes real native-Qwen Generate failed: %v", err)
		}
		lastStop = resp.Stop
		if n := len(resp.Content); n > len(best) {
			best = []byte(resp.Content)
		}
		if len(best) >= minArtifactBytes {
			break
		}
	}
	b := best
	if len(b) < minArtifactBytes {
		t.Fatalf("ljRealQwenBytes: real native-Qwen produced a trivial artifact (%d bytes < %d, stop=%q): substantive content required, fail-closed", len(b), minArtifactBytes, lastStop)
	}
	t.Logf("real native-Qwen artifact content: bytes=%d stop=%q sha=%s", len(b), lastStop, hex.EncodeToString(shaBytes(b))[:16])
	return b
}

// ljSealViaProductTool is the MANDATORY artifact proof: it runs AHP's OWN product
// save_artifact tool (core.NewSaveArtifactTool over a real artifacts.Store sink,
// dispatched through core.ToolRegistry/MultiExecutor exactly as the runner wires
// it) to write REAL bytes into REAL MinIO with an AHP Seal. It asserts the
// returned handle digest equals the input and that GET of the sealed object
// round-trips (sha256 PUT == GET). No Emil, no httptest, no fixture on this path.
func ljSealViaProductTool(t *testing.T, ctx context.Context, b *ljStoreBundle, content []byte) artifacts.ArtifactRef {
	t.Helper()
	wantSHA := hex.EncodeToString(shaBytes(content))
	sink := &ljStoreSink{b: b}
	mx := core.NewMultiExecutor()
	sa := core.NewSaveArtifactTool(sink)
	mx.Register(core.ToolSaveArtifact, sa)
	reg := core.NewToolRegistry(mx)
	reg.Register(sa.Def())
	toolCtx := core.WithRunScope(ctx, b.scope, "live-journey-task-token")
	args, err := json.Marshal(map[string]string{
		"name": "live-journey-qwen.txt", "mime": "text/plain", "content": string(content),
	})
	if err != nil {
		t.Fatalf("marshal save_artifact args: %v", err)
	}
	out, err := reg.Execute(toolCtx, core.ToolSaveArtifact, args)
	if err != nil {
		t.Fatalf("product save_artifact Execute over real MinIO failed: %v", err)
	}
	var h core.ArtifactHandle
	if err := json.Unmarshal(out, &h); err != nil {
		t.Fatalf("decode save_artifact result %q: %v", out, err)
	}
	if h.ID == "" || h.Version == "" {
		t.Fatalf("save_artifact returned empty handle: %+v", h)
	}
	if h.SHA256 != wantSHA {
		t.Fatalf("save_artifact handle sha=%q want=%q (digest not authentic)", h.SHA256, wantSHA)
	}
	rc, _, err := b.store.Open(ctx, b.scope, h.ID)
	if err != nil {
		t.Fatalf("GET sealed artifact: %v", err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("read artifact body: %v", err)
	}
	if hex.EncodeToString(shaBytes(got)) != wantSHA {
		t.Fatalf("sha256 PUT != GET via product tool (real MinIO corrupt)")
	}
	if int64(len(got)) != h.Size {
		t.Fatalf("artifact size mismatch: got=%d handle=%d", len(got), h.Size)
	}
	t.Logf("MANDATORY artifact via OUR save_artifact tool OK: bucket=%s id=%s version=%s sha=%s PUT==GET bytes=%d",
		b.bucket, h.ID, h.Version, h.SHA256[:16], len(got))
	return artifacts.ArtifactRef{ID: h.ID, Version: h.Version, SHA256: h.SHA256, Size: h.Size}
}

func shaBytes(b []byte) []byte { s := sha256.Sum256(b); return s[:] }

// ---- durable State over real PG (mTLS, Ed25519 task tokens) -----------------

type ljCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func ljNewCA(t *testing.T) *ljCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ahp-live-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &ljCA{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

func (ca *ljCA) clientCert(t *testing.T, uris ...string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	parsed := make([]*url.URL, 0, len(uris))
	for _, u := range uris {
		p, err := url.Parse(u)
		if err != nil {
			t.Fatalf("bad uri %q: %v", u, err)
		}
		parsed = append(parsed, p)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "ahp-live-client"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		URIs:        parsed,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

func ljWorkloadURI(tenant, deployment string) string {
	return "spiffe://ahp/tenant/" + tenant + "/deployment/" + deployment
}

// ljStateStack models the State service process. Reboot() closes the pool +
// server and reopens them against the SAME PG with the SAME identity material —
// the equivalent of a process restart that must not lose persisted rows.
type ljStateStack struct {
	t          *testing.T
	tctx       context.Context
	dsn        string
	signer     *auth.Signer
	verifier   *auth.Verifier
	ca         *ljCA
	tenant     string
	deployment string
	cert       tls.Certificate

	pool   *pgxpool.Pool
	srv    *httptest.Server
	client *space.StateClient
}

func ljNewStateStack(t *testing.T, ctx context.Context, dsn string) *ljStateStack {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner(ljIssuer, ljKID, priv)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier(ljIssuer, map[string]ed25519.PublicKey{ljKID: pub})
	ca := ljNewCA(t)
	tenant := "t-" + ljRand(t, 6)
	deployment := "d-" + ljRand(t, 6)
	return &ljStateStack{
		t: t, tctx: ctx, dsn: dsn, signer: signer, verifier: verifier, ca: ca,
		tenant: tenant, deployment: deployment,
		cert: ca.clientCert(t, ljWorkloadURI(tenant, deployment)),
	}
}

func (s *ljStateStack) boot() {
	s.t.Helper()
	pool, err := pgxpool.New(s.tctx, s.dsn)
	if err != nil {
		s.t.Fatalf("state pool: %v", err)
	}
	store := state.New(pool)
	if err := store.Migrate(s.tctx); err != nil {
		_ = pool.Close
		s.t.Fatalf("state Migrate: %v", err)
	}
	signer := s.signer
	issue := func(_ context.Context, sc contracts.Scope, taskID string) (string, error) {
		return signer.Sign(auth.Claims{
			Audience: ljAudience, Kind: auth.KindTask,
			Tenant: sc.Tenant, Deployment: sc.Deployment, Subject: sc.Subject,
			Task: taskID, Root: "root-" + taskID, Context: "ctx-" + taskID,
			Grant: "grant-" + taskID, Depth: 1,
		}, 5*time.Minute)
	}
	authorize := func(context.Context, auth.Claims, string) error { return nil }
	handler, err := stateapi.New(store, s.verifier, authorize, issue)
	if err != nil {
		_ = pool.Close
		s.t.Fatalf("stateapi.New: %v", err)
	}
	serverCAs := x509.NewCertPool()
	if !serverCAs.AppendCertsFromPEM(s.ca.pem) {
		s.t.Fatal("CA PEM rejected")
	}
	srv := httptest.NewUnstartedServer(handler)
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: serverCAs}
	srv.StartTLS()
	tr := srv.Client().Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{s.cert}
	srv.Client().Transport = tr
	s.pool = pool
	s.srv = srv
	s.client = space.NewStateClient(srv.URL, srv.Client())
}

func (s *ljStateStack) shutdown() {
	if s.srv != nil {
		s.srv.Close()
		s.srv = nil
	}
	if s.pool != nil {
		s.pool.Close()
		s.pool = nil
	}
	s.client = nil
}

// reboot tears the process down and brings it back with the SAME identity
// (signer/verifier/CA/tenant) against the SAME PG: a true process restart.
func (s *ljStateStack) reboot() {
	s.shutdown()
	s.boot()
}

func (s *ljStateStack) signHuman(t *testing.T) string {
	t.Helper()
	tok, err := s.signer.Sign(auth.Claims{
		Audience: ljAudience, Kind: auth.KindTask,
		Tenant: s.tenant, Deployment: s.deployment, Subject: ljSubject,
		Task: "task-" + ljRand(t, 6), Root: "run-1", Context: "ctx-1",
		Grant: "grant-1", Depth: 0,
	}, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func ljRand(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

// ---- the one journey ---------------------------------------------------------

func TestLiveRealEndToEnd(t *testing.T) {
	l := ljRequire(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	// (1) real native-Qwen reachable.
	qwen := ljRealQwen(t, l)

	// (2) MANDATORY real LLM content: REAL native-Qwen generation bytes used as
	//     the artifact payload (no fixture bytes, no stand-in).
	qwenBytes := ljRealQwenBytes(t, l)

	// (3) MANDATORY artifact via AHP's OWN product tool: core save_artifact over
	//     a real artifacts.Store -> real MinIO PUT -> AHP Seal -> GET sha PUT==GET.
	sfx := ljRand(t, 6)
	bucket := "ahp-space-live-" + sfx // valid S3 bucket name, no underscore
	scope := contracts.Scope{Tenant: "t-" + sfx, Deployment: "d-" + sfx, Subject: ljSubject}
	sb := ljBootStore(t, ctx, l, bucket, scope, true)
	ref := ljSealViaProductTool(t, ctx, sb, qwenBytes)

	// (4) real-Qwen Core loop with the SAME product tool registered + a durable
	//     sink: proves the production model<->tool loop runs on real Qwen and that
	//     step emission is durable on the production path (iv #74), not test-only.
	saTool := core.NewSaveArtifactTool(&ljStoreSink{b: sb})
	mxLoop := core.NewMultiExecutor()
	mxLoop.Register(core.ToolSaveArtifact, saTool)
	loopReg := core.NewToolRegistry(mxLoop)
	loopReg.Register(saTool.Def())
	dsink := space.NewMemDurabilitySink()
	rt := core.NewRuntime(core.RuntimeConfig{Model: qwen, Tools: loopReg, Durable: dsink})
	results := make(chan space.StepResult, 64)
	if err := rt.Run(ctx, &space.CoreRequest{
		TurnID: "turn-" + ljRand(t, 4), Input: json.RawMessage(`"Reply with the single word: OK"`), MaxSteps: 3,
	}, results); err != nil {
		t.Fatalf("core Run against real Qwen: %v", err)
	}
	var modelSteps int
drain:
	for {
		select {
		case s := <-results:
			if s.StepType == space.StepModel {
				modelSteps++
			}
		default:
			break drain
		}
	}
	if modelSteps < 1 {
		t.Fatalf("core loop produced no StepModel with real Qwen")
	}
	if dsink.Len() < 1 {
		t.Fatalf("durable sink empty after live loop: #74 durable emission not proven on prod path")
	}
	t.Logf("core loop (real Qwen + product save_artifact tool registered): model_steps=%d durable_emissions=%d", modelSteps, dsink.Len())

	// (4b) OPTIONAL Emil provenance hop (NOT a gate): a real A2A message/send via
	//     the product adapter, recorded only to enrich provenance. Absence never
	//     fails the journey; when present it is real (no httptest/:19081/newMockState).
	var emilText string
	if l.emilReach {
		emilText = ljEmilProvenance(t, l)
	} else {
		t.Logf("Emil provenance hop SKIPPED (Emil is not a readiness gate; mandatory proof is native-Qwen->product tool->MinIO->PG->restart).")
	}

	// (5) durable State on real PG: thread + run referencing the artifact.
	stack := ljNewStateStack(t, ctx, l.dsn)
	stack.boot()
	human := stack.signHuman(t)
	th, err := stack.client.CreateThread(ctx, human, contracts.Scope{}, "ctx-"+ljRand(t, 4), "lj-thread")
	if err != nil {
		stack.shutdown()
		t.Fatalf("CreateThread: %v", err)
	}
	runPayloadMap := map[string]any{"artifact_id": ref.ID, "sha256": ref.SHA256, "version": ref.Version, "origin": "native-qwen-product-save_artifact"}
	if emilText != "" {
		runPayloadMap["emil_provenance"] = map[string]any{
			"sha256": hex.EncodeToString(shaBytes([]byte(emilText))), "bytes": len(emilText),
		}
	} else {
		runPayloadMap["emil_provenance"] = "skipped (Emil is not a gate)"
	}
	runPayload, _ := json.Marshal(runPayloadMap)
	run, err := stack.client.CreateRun(ctx, human, th.ID, "lj-run", runPayload, "")
	if err != nil {
		stack.shutdown()
		t.Fatalf("CreateRun: %v", err)
	}
	if run.Status != state.RunPending {
		stack.shutdown()
		t.Fatalf("CreateRun status=%q want pending", run.Status)
	}
	claim, err := stack.client.ClaimRun(ctx, "", "w1", 60*time.Second)
	if err != nil || claim.Lease == nil {
		stack.shutdown()
		t.Fatalf("ClaimRun: %v", err)
	}
	fence := space.Fence{RunID: claim.Lease.Run.ID, Worker: "w1", Epoch: claim.Lease.Epoch}
	if _, err := stack.client.AppendStream(ctx, claim.TaskToken, th.ID, fence.RunID, fence.Worker, fence.Epoch,
		"lj-event", "artifact_sealed",
		json.RawMessage(`{"sha256":"`+ref.SHA256+`","version":"`+ref.Version+`"}`)); err != nil {
		stack.shutdown()
		t.Fatalf("AppendStream: %v", err)
	}
	if err := stack.client.Release(ctx, claim.TaskToken, fence.RunID, fence.Worker, fence.Epoch, state.RunDone); err != nil {
		stack.shutdown()
		t.Fatalf("Release: %v", err)
	}

	// (6) RESTART (same identity, same PG + same MinIO bucket): rows + object must
	//     survive; sha stays PUT==GET.
	stack.reboot()
	defer stack.shutdown()

	sb.pool.Close()
	sb2 := ljBootStore(t, ctx, l, bucket, scope, false)
	defer sb2.pool.Close()

	rr, err := stack.client.GetRun(ctx, stack.signHuman(t), run.ID)
	if err != nil {
		t.Fatalf("GetRun after restart (durability): %v", err)
	}
	if rr.Status != state.RunDone {
		t.Fatalf("post-restart run status=%q want done (state not durable)", rr.Status)
	}
	evs, err := stack.client.ReadStream(ctx, stack.signHuman(t), th.ID, 0, 50)
	if err != nil {
		t.Fatalf("ReadStream after restart: %v", err)
	}
	found := false
	for _, e := range evs {
		if e.Kind == "artifact_sealed" {
			found = true
		}
	}
	if !found {
		t.Fatalf("artifact_sealed stream event not durable after restart (events=%d)", len(evs))
	}
	rc2, ref2, err := sb2.store.Open(ctx, scope, ref.ID)
	if err != nil {
		t.Fatalf("Open artifact after restart (MinIO durability): %v", err)
	}
	got2, _ := io.ReadAll(rc2)
	_ = rc2.Close()
	if hex.EncodeToString(shaBytes(got2)) != ref.SHA256 {
		t.Fatalf("post-restart artifact sha mismatch (got %d bytes)", len(got2))
	}
	if ref2.Version != ref.Version {
		t.Fatalf("post-restart artifact version drift: %q vs %q", ref2.Version, ref.Version)
	}
	t.Logf("durable after restart: run=%s status=%s stream_events=%d artifact_version=%s sha=%s",
		rr.ID, rr.Status, len(evs), ref2.Version, ref.SHA256[:16])

	// (7) private context: a foreign scope cannot read the owner's artifact bytes.
	foreign := contracts.Scope{Tenant: "t-foreign-" + ljRand(t, 6), Deployment: "d-foreign-" + ljRand(t, 6), Subject: "mallory"}
	if _, _, err := sb2.store.Open(ctx, foreign, ref.ID); err == nil {
		t.Fatalf("private-context leak: foreign scope read owner artifact")
	} else if contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign artifact Open want NOT_FOUND, got %v", err)
	}
	t.Logf("private-context OK: foreign scope -> NOT_FOUND on owner artifact (bytes do not leak)")
}
