package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/artifactsapi"
	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func TestRunnerArtifactOwnerWithoutStorageCredentials(t *testing.T) {
	ctx := context.Background()
	// This scenario has to hand a text artifact back through the owner, and the
	// owner only delivers supported text once the trusted parser has extracted
	// it: an unextracted artifact is refused fail-closed by design
	// (internal/artifacts/ingestion.go, internal/artifacts/parser.go). Without a
	// configured parser that refusal is what the caller would see -- a correct
	// product answer that says nothing about the runner path under test. Name the
	// missing tooling up front instead of letting it surface as a byte mismatch,
	// and treat a configured-but-unusable parser as the real fault it is.
	parser := os.Getenv("AHP_ARTIFACT_PARSER")
	if parser == "" {
		t.Skip("requires a built artifact parser: set AHP_ARTIFACT_PARSER to the absolute path of the binary built from ./cmd/artifact-parser")
	}
	if !filepath.IsAbs(parser) {
		t.Fatalf("AHP_ARTIFACT_PARSER must be an absolute path, got %q", parser)
	}
	if _, err := os.Stat(parser); err != nil {
		t.Fatalf("AHP_ARTIFACT_PARSER is not usable: %v", err)
	}
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("real PostgreSQL required")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal("invalid PG configuration")
	}
	defer pool.Close()
	endpoint, err := url.Parse(os.Getenv("AHP_S3_ENDPOINT"))
	if err != nil || endpoint.Host == "" {
		t.Fatal("real MinIO required")
	}
	s3, err := minio.New(endpoint.Host, &minio.Options{Creds: credentials.NewStaticV4(os.Getenv("AHP_MINIO_ROOT_USER"), os.Getenv("AHP_MINIO_ROOT_PASSWORD"), ""), Secure: endpoint.Scheme == "https"})
	if err != nil {
		t.Fatal("invalid S3 configuration")
	}
	bucket := fmt.Sprintf("ahp-runner-api-%d", time.Now().UnixNano())
	store, err := artifacts.New(pool, s3, bucket, func(context.Context, contracts.Scope, string, string) error { return nil }, artifacts.WithParserCommand(artifacts.ParserCommand{Path: parser}))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		for obj := range s3.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
			if obj.Err == nil {
				_ = s3.RemoveObject(ctx, bucket, obj.Key, minio.RemoveObjectOptions{VersionID: obj.VersionID})
			}
		}
		_ = s3.RemoveBucket(ctx, bucket)
	}()
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := auth.NewSigner("runner-owner", "test", priv)
	verifier := auth.NewVerifier("runner-owner", map[string]ed25519.PublicKey{"test": pub})
	var revoked atomic.Bool
	handler, err := artifactsapi.New(store, verifier, func(context.Context, auth.Claims, string) error {
		if revoked.Load() {
			return contracts.NewError(contracts.ErrForbidden, "revoked")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	scope := contracts.Scope{Tenant: fmt.Sprintf("runner-%d", time.Now().UnixNano()), Subject: "alice", Deployment: "runner"}
	ca := newOwnerCA(t)
	clientCert := ca.clientCert(t, auth.WorkloadID(scope))
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	srv := httptest.NewUnstartedServer(handler)
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	srv.StartTLS()
	defer srv.Close()
	key, err := x509.MarshalPKCS8PrivateKey(clientCert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for name, data := range map[string][]byte{"cert": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientCert.Certificate[0]}), "key": pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), "ca": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})} {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"AS_DATA_DATABASE_URL", "DATABASE_URL", "AHP_DB_URL", "AHP_S3_ENDPOINT", "AHP_MINIO_ROOT_USER", "AHP_MINIO_ROOT_PASSWORD", "AS_ARTIFACT_BUCKET"} {
		t.Setenv(name, "")
	}
	t.Setenv("AS_ARTIFACTS_URL", srv.URL)
	t.Setenv("AS_WORKLOAD_CERT", filepath.Join(dir, "cert"))
	t.Setenv("AS_WORKLOAD_KEY", filepath.Join(dir, "key"))
	t.Setenv("AS_WORKLOAD_CA", filepath.Join(dir, "ca"))
	t.Setenv("AS_AUTH_ISSUER", "runner-owner")
	t.Setenv("AS_AUTH_KEYS", "test="+base64.StdEncoding.EncodeToString(pub))
	token, err := signer.Sign(auth.Claims{Audience: "ahp", Kind: auth.KindTask, Tenant: scope.Tenant, Deployment: scope.Deployment, Subject: scope.Subject, Task: "grant-task", Root: "root", Context: "thread", Grant: "grant"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	sink, enabled, err := buildArtifactSink()
	if err != nil || !enabled {
		t.Fatalf("owner-only runner sink: enabled=%v err=%v", enabled, err)
	}
	runCtx := core.WithRunBinding(core.WithRunScope(ctx, contracts.Scope{Tenant: scope.Tenant, Subject: scope.Subject}, token), core.RunBinding{TaskID: "durable-run", ContextID: "thread"})
	data := []byte("runner -> owner API -> versioned MinIO\n")
	ref, err := sink.SaveArtifact(runCtx, "runner.txt", "text/plain", data)
	if err != nil {
		t.Fatal(err)
	}
	client, err := buildArtifactClient()
	if err != nil {
		t.Fatal(err)
	}
	client = client.WithTokenSource(func(context.Context) (string, error) { return token, nil })
	got, err := client.Download(ctx, ref.ID)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("byte mismatch, err=%v", err)
	}
	metadata, err := client.Get(ctx, ref.ID)
	if err != nil || metadata.ContextID != "thread" || metadata.TaskID != "grant-task" || metadata.Subject != "alice" {
		t.Fatalf("owner binding lost: %+v err=%v", metadata, err)
	}
	revoked.Store(true)
	if _, err := sink.SaveArtifact(runCtx, "revoked.txt", "text/plain", data); err == nil {
		t.Fatal("revoked run wrote artifact")
	}
}
