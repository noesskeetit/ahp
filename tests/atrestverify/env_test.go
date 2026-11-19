package atrestverify_test

// Independent verification of the MOUNTED at-rest object path (P1 Crypto/KMS, R6 remainder).
//
// Why this file exists at all: commit 4075d47f ("feat(atrest,R6): mount objectcipher into
// Artifacts write/read path") is in the integration tree, but no independent review receipt
// names it, and the existing tests of that mount live INSIDE package artifacts and reach into
// unexported fields (atrest_mount_test.go sets e.store.cipher directly) using the author's own
// fixture. That proves the author's wiring under the author's bootstrap; it does not prove the
// claim to a reader. This harness therefore:
//   - lives outside the package (package atrestverify_test) and touches the mount ONLY through
//     exported API: artifacts.New + artifacts.WithObjectCipher + BeginUpload/Seal/Open/OpenRange;
//   - reads bytes out of MinIO directly, bypassing every code path under test;
//   - runs against its own PostgreSQL database and its own versioned bucket, never a shared one;
//   - states KMS reality: the key authority here is the SYNTHETIC in-repo authority. A vendor KMS
//     is an external prerequisite this Mac cannot exercise, and nothing below claims otherwise.
//
// Product code is not modified by this file, and no expectation of the product is relaxed to make
// a leg pass: every leg states the fact it demands and fails if the fact is missing.
//
// Secrets: no DSN, credential or key material is ever printed or written to evidence. Only
// database/bucket NAMES and content digests appear there.

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/atrest"
	"github.com/ahp-platform/ahp/internal/atrest/objectcipher"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// --- evidence collection -------------------------------------------------------------------

type legRecord struct {
	Name      string         `json:"leg"`
	Status    string         `json:"status"` // PASS | FAIL | SKIP
	Started   string         `json:"started_utc"`
	DurMS     float64        `json:"dur_ms"`
	Detail    map[string]any `json:"detail,omitempty"`
	FailText  string         `json:"fail_text,omitempty"`
	KMSKind   string         `json:"kms"`
	Synthetic bool           `json:"synthetic_kms"`
}

type environment struct {
	ProductRevision string    `json:"product_revision"`
	VCSModified     bool      `json:"vcs_modified"`
	HarnessRevision string    `json:"harness_revision"`
	GoVersion       string    `json:"go_version"`
	GOOS            string    `json:"goos"`
	GOARCH          string    `json:"goarch"`
	NumCPU          int       `json:"num_cpu"`
	Hostname        string    `json:"hostname"`
	MemoryBytes     uint64    `json:"memory_bytes,omitempty"`
	LoadBefore      []float64 `json:"load_before,omitempty"`
	LoadAfter       []float64 `json:"load_after,omitempty"`
	DatabaseName    string    `json:"database_name"`
	BucketPrefix    string    `json:"bucket_prefix"`
	KMSKind         string    `json:"kms"`
	KMSNote         string    `json:"kms_note"`
}

type evidenceFile struct {
	Kind        string         `json:"kind"`
	Generated   string         `json:"generated_utc"`
	Environment environment    `json:"environment"`
	Legs        []legRecord    `json:"legs"`
	Counters    map[string]int `json:"counters"`
	Verdict     string         `json:"verdict"`
	Notes       []string       `json:"notes"`
}

var (
	evMu     sync.Mutex
	evLegs   []legRecord
	evNotes  []string
	evEnv    environment
	runIdent string
)

func TestMain(m *testing.M) {
	code := m.Run()
	dropRunDatabase()
	writeEvidence(code)
	os.Exit(code)
}

func writeEvidence(exitCode int) {
	out := os.Getenv("AHP_ATREST_VERIFY_EVIDENCE")
	if out == "" {
		return
	}
	evMu.Lock()
	defer evMu.Unlock()
	counters := map[string]int{"PASS": 0, "FAIL": 0, "SKIP": 0}
	for _, l := range evLegs {
		counters[l.Status]++
	}
	verdict := "PASS"
	if counters["FAIL"] > 0 {
		verdict = "FAIL"
	} else if counters["SKIP"] > 0 {
		verdict = "BLOCKED" // an unrun leg is not a verified fact
	}
	if exitCode != 0 && verdict == "PASS" {
		verdict = "FAIL" // the runner saw something this recorder did not name
	}
	evEnv.LoadAfter = loadAverage()
	doc := evidenceFile{
		Kind:        "atrest-mounted-path-verification",
		Generated:   utcNow(),
		Environment: evEnv, Legs: evLegs, Counters: counters,
		Verdict: verdict, Notes: evNotes,
	}
	raw, err := json.MarshalIndent(&doc, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, "atrestverify: evidence marshal failed", err)
		return
	}
	if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "atrestverify: evidence dir failed", err)
		return
	}
	if err := os.WriteFile(out, raw, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "atrestverify: evidence write failed", err)
		return
	}
	fmt.Printf("atrestverify evidence=%s verdict=%s PASS=%d FAIL=%d SKIP=%d\n",
		out, verdict, counters["PASS"], counters["FAIL"], counters["SKIP"])
}

func note(f string, args ...any) {
	evMu.Lock()
	defer evMu.Unlock()
	evNotes = append(evNotes, fmt.Sprintf(f, args...))
}

type legRec struct {
	rec legRecord
	t   *testing.T
}

func startLeg(t *testing.T, name string) *legRec {
	runIdentOnce()
	l := &legRec{t: t, rec: legRecord{Name: name, Started: utcNow(), Status: "PASS",
		KMSKind: "SYNTHETIC (in-repo DynamicAuthority)", Synthetic: true,
		Detail: map[string]any{}}}
	t.Cleanup(func() {
		l.rec.DurMS = float64(time.Since(mustTime(l.rec.Started)).Microseconds()) / 1000.0
		switch {
		case t.Skipped():
			l.rec.Status = "SKIP"
		case t.Failed():
			l.rec.Status = "FAIL"
			l.rec.FailText = "test failed; see the runner log for this leg name"
		default:
			l.rec.Status = "PASS"
		}
		evMu.Lock()
		evLegs = append(evLegs, l.rec)
		evMu.Unlock()
	})
	return l
}

func (l *legRec) set(key string, value any) { l.rec.Detail[key] = value }

func mustTime(iso string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, iso)
	if err != nil {
		return time.Now()
	}
	return t
}

func utcNow() string { return time.Now().UTC().Format(time.RFC3339Nano) }

var digitsOnly = regexp.MustCompile(`[^a-z0-9]+`)

func ident() string {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

func runIdentOnce() {
	if runIdent == "" {
		runIdent = ident()
	}
}

func loadAverage() []float64 {
	if runtime.GOOS != "darwin" {
		return nil
	}
	var load [3]float64
	if err := sysLoadAvg(load[:]); err != nil {
		return nil
	}
	return []float64{load[0], load[1], load[2]}
}

func runIdentValue() string { runIdentOnce(); return runIdent }

// --- live fixture --------------------------------------------------------------------------

type venv struct {
	// store is the handle whose cipher state the leg chose; onStore/offStore let one leg
	// demonstrate dual-read over the SAME objects with the mount flipped.
	store    *artifacts.Store
	onStore  *artifacts.Store
	offStore *artifacts.Store
	client   *minio.Client
	pool     *pgxpool.Pool
	bucket   string
	scope    contracts.Scope
}

func productRevisionFromBuild() (string, bool) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "", false
	}
	rev, mod := "", false
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			mod = s.Value == "true"
		}
	}
	return rev, mod
}

var envOnce sync.Once
var envPool *pgxpool.Pool
var envDB string
var envErr error

func requireLive(t *testing.T) {
	t.Helper()
	s3 := os.Getenv("AHP_S3_ENDPOINT")
	if s3 == "" {
		t.Skip("AHP_S3_ENDPOINT not set: no object store for this verification")
	}
	if os.Getenv("AHP_MINIO_ROOT_USER") == "" || os.Getenv("AHP_MINIO_ROOT_PASSWORD") == "" {
		t.Skip("MinIO credentials not configured")
	}
	dsn := databaseDSN()
	if dsn == "" {
		t.Skip("AHP_TEST_DATABASE_URL not set: no PostgreSQL for this verification")
	}
	envOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
		defer cancel()
		cfg, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			envErr = err
			return
		}
		admin, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			envErr = err
			return
		}
		defer admin.Close()
		if err := admin.Ping(ctx); err != nil {
			envErr = err
			return
		}
		// OWN database: this suite writes artifact rows and runs Migrate DDL; on a shared
		// database that would touch other lanes' rows. Never printed: only the name is kept.
		// PostgreSQL folds identifiers longer than 63 bytes, and a name that is truncated to the
		// same 63 bytes as a sibling leg database would collide; the name is kept short on purpose.
		envDB = "ahp_atverify_" + ident() + ident()
		if _, err := admin.Exec(ctx, `CREATE DATABASE "`+envDB+`"`); err != nil {
			envErr = fmt.Errorf("create own database failed: %w", err)
			return
		}
		runCfg := cfg.Copy()
		runCfg.ConnConfig.Database = envDB
		runCfg.MaxConns = 8
		pool, err := pgxpool.NewWithConfig(ctx, runCfg)
		if err != nil {
			envErr = err
			return
		}
		if err := pool.Ping(ctx); err != nil {
			envErr = err
			return
		}
		envPool = pool
		// Provenance must be stated, not assumed. debug.ReadBuildInfo carries vcs settings only for
		// main packages, so a test binary reports no revision at all: reading it from the build
		// info alone would have published an empty field silently, which is the same class of
		// defect as a binary whose VCS stamp does not match the source it was built from. The
		// revision therefore comes from the working tree, and the product binary's own stamp is
		// read separately in the start-up leg (go version -m).
		rev, mod := productRevisionFromBuild()
		if gitRev, gitDirty, err := gitProvenance(context.Background()); err == nil {
			rev, mod = gitRev, gitDirty
		}
		host, _ := os.Hostname()
		evEnv = environment{ProductRevision: rev, VCSModified: mod, HarnessRevision: rev,
			GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH,
			NumCPU: runtime.NumCPU(), Hostname: host, LoadBefore: loadAverage(),
			DatabaseName: envDB, BucketPrefix: "go-atverify-",
			KMSKind: "SYNTHETIC (in-repo DynamicAuthority)",
			KMSNote: "keys come from atrest.NewDynamicAuthority(root) inside this process; a vendor KMS is an external prerequisite and is NOT exercised here"}
	})
	if envErr != nil {
		t.Fatalf("live fixture unavailable: %v", envErr)
	}
}

// dropRunDatabase removes only the database this run created.
func dropRunDatabase() {
	dsn := databaseDSN()
	if dsn == "" || envDB == "" || envPool == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	envPool.Close()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return
	}
	defer admin.Close()
	_, _ = admin.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`, envDB)
	if _, err := admin.Exec(ctx, `DROP DATABASE IF EXISTS "`+envDB+`"`); err != nil {
		note("run database %s could not be dropped: %v", envDB, err)
	}
}

func databaseDSN() string {
	if v := os.Getenv("AHP_TEST_DATABASE_URL"); v != "" {
		return v
	}
	return os.Getenv("DATABASE_URL")
}

func minioClient(t *testing.T) *minio.Client {
	t.Helper()
	u, err := url.Parse(os.Getenv("AHP_S3_ENDPOINT"))
	if err != nil || u.Host == "" {
		t.Fatalf("AHP_S3_ENDPOINT unparsable")
	}
	client, err := minio.New(u.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(os.Getenv("AHP_MINIO_ROOT_USER"), os.Getenv("AHP_MINIO_ROOT_PASSWORD"), ""),
		Secure: u.Scheme == "https",
	})
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	return client
}

// newEnv gives one leg its own versioned bucket, tenant and store.
// segmentSize <= 0 leaves the cipher on the product default (1 MiB).
func newEnv(t *testing.T, cipherOn bool, segmentSize int64) *venv {
	t.Helper()
	requireLive(t)
	client := minioClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	suffix := runIdentValue() + ident()[:4]
	bucket := "go-atverify-" + suffix
	if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
		t.Fatalf("make bucket: %v", err)
	}
	// Versioning is ON because the product's Seal binds an exact version id; an
	// unversioned bucket would let a leg pass on "latest object" semantics that the
	// product does not rely on.
	if err := client.SetBucketVersioning(ctx, bucket, minio.BucketVersioningConfiguration{Status: "Enabled"}); err != nil {
		t.Fatalf("enable versioning: %v", err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		for obj := range client.ListObjects(c, bucket, minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
			if obj.Err != nil {
				break
			}
			_ = client.RemoveObject(c, bucket, obj.Key, minio.RemoveObjectOptions{VersionID: obj.VersionID})
		}
		_ = client.RemoveBucket(c, bucket)
	})
	scope := contracts.Scope{Tenant: "v-" + suffix, Deployment: "d-verify", Subject: "owner"}
	allow := func(_ context.Context, _ contracts.Scope, _, _ string) error { return nil }
	build := func(withCipher bool) *artifacts.Store {
		opts := []artifacts.Option{}
		if withCipher {
			ciph := objectcipher.New(mustCodec(t), client, true)
			if segmentSize > 0 {
				ciph = ciph.WithSegmentSize(segmentSize)
			}
			opts = append(opts, artifacts.WithObjectCipher(ciph))
		}
		st, err := artifacts.New(envPool, client, bucket, allow, opts...)
		if err != nil {
			t.Fatalf("artifacts.New: %v", err)
		}
		return st
	}
	first := build(cipherOn)
	if err := first.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	v := &venv{store: first, client: client, pool: envPool, bucket: bucket, scope: scope}
	if cipherOn {
		v.onStore = first
		v.offStore = build(false)
	} else {
		v.offStore = first
		v.onStore = build(true)
	}
	return v
}

func mustCodec(t *testing.T) *atrest.Codec {
	t.Helper()
	root := make([]byte, 32)
	if _, err := rand.Read(root); err != nil {
		t.Fatalf("root material: %v", err)
	}
	codec, err := atrest.NewCodec(atrest.NewDynamicAuthority(root))
	if err != nil {
		t.Fatalf("codec: %v", err)
	}
	return codec
}

// uploadSeal drives the real client contract: BeginUpload -> presigned PUT -> Seal.
func (v *venv) uploadSeal(t *testing.T, st *artifacts.Store, key, body string) (artifacts.ArtifactRef, artifacts.Upload) {
	t.Helper()
	sum := sha256.Sum256([]byte(body))
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	up, err := st.BeginUpload(ctx, v.scope, key, artifacts.UploadRequest{
		Name: "verify.bin", MIME: "application/octet-stream",
		SHA256: hex.EncodeToString(sum[:]), Size: int64(len(body)),
	})
	if err != nil {
		t.Fatalf("BeginUpload: %v", err)
	}
	if err := presignedPut(ctx, up.PresignedURL, []byte(body)); err != nil {
		t.Fatalf("presigned PUT: %v", err)
	}
	ref, err := st.Seal(ctx, v.scope, up.ID)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	return ref, up
}

// presignedPut sends bytes exactly as a client would; the URL never appears in output.
func presignedPut(ctx context.Context, signed string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, signed, bytes.NewReader(body))
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("storage status=%d", resp.StatusCode)
	}
	return nil
}

// rawGetVersion reads stored bytes directly from the object store, bypassing the product.
func (v *venv) rawGetVersion(t *testing.T, object, version string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	rc, err := v.client.GetObject(ctx, v.bucket, object, minio.GetObjectOptions{VersionID: version})
	if err != nil {
		t.Fatalf("raw get: %v", err)
	}
	defer func() { _ = rc.Close() }()
	raw, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("raw read: %v", err)
	}
	return raw
}

func (v *venv) rawPut(t *testing.T, object string, body []byte) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	info, err := v.client.PutObject(ctx, v.bucket, object, bytes.NewReader(body), int64(len(body)), minio.PutObjectOptions{})
	if err != nil {
		t.Fatalf("raw put: %v", err)
	}
	return info.VersionID
}

func shaHex(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

// bodyOf builds deterministic bytes of length n (not constant, so digests are content-specific).
func bodyOf(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + i%26)
		if i%4096 == 0 {
			b[i] = 'X'
		}
	}
	return b
}

func readAllBytes(t *testing.T, rc io.ReadCloser) []byte {
	t.Helper()
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return b
}

func envFlag(name string) bool { return os.Getenv(name) == "1" }

var unsupportedLoadAvg = errors.New("load average is only collected on darwin")

// createScratchDatabase makes a second private database for a leg that must hand a DSN to a child
// process (the product binary runs its own migrations there). Only the name is ever reported.
func createScratchDatabase(t *testing.T, suffix string) string {
	t.Helper()
	dsn := databaseDSN()
	if dsn == "" {
		t.Fatal("no database for a scratch database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("scratch admin pool: %v", err)
	}
	defer admin.Close()
	name := envDB + "_" + digitsOnly.ReplaceAllString(suffix, "")
	if _, err := admin.Exec(ctx, `CREATE DATABASE "`+name+`"`); err != nil {
		t.Fatalf("create scratch database: %v", err)
	}
	t.Cleanup(func() {
		c, cn := context.WithTimeout(context.Background(), 60*time.Second)
		defer cn()
		pool, err := pgxpool.New(c, dsn)
		if err != nil {
			return
		}
		defer pool.Close()
		_, _ = pool.Exec(c, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`, name)
		if _, err := pool.Exec(c, `DROP DATABASE IF EXISTS "`+name+`"`); err != nil {
			note("scratch database %s could not be dropped: %v", name, err)
		}
	})
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("scratch dsn: %v", err)
	}
	cfg.ConnConfig.Database = name
	return cfg.ConnString()
}

// gitProvenance reports HEAD and whether the working tree carries uncommitted changes. It reads
// nothing outside the repository metadata and prints nothing.
func gitProvenance(ctx context.Context) (revision string, dirty bool, err error) {
	raw, err := exec.CommandContext(ctx, "git", "rev-parse", "HEAD").Output()
	if err != nil {
		return "", false, err
	}
	revision = strings.TrimSpace(string(raw))
	if len(revision) < 40 {
		return "", false, fmt.Errorf("unexpected HEAD output length %d", len(revision))
	}
	st, err := exec.CommandContext(ctx, "git", "status", "--porcelain").Output()
	if err != nil {
		return revision, false, err
	}
	return revision, len(strings.TrimSpace(string(st))) > 0, nil
}
