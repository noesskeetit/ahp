package artifacts_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	store "github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/artifactsapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	sdk "github.com/ahp-platform/ahp/sdk/go/artifacts"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func TestOwnerAPIRoundTripAndAuthority(t *testing.T) {
	ctx := context.Background()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("AHP_TEST_DATABASE_URL required for real owner API test")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal("PG configuration invalid")
	}
	defer pool.Close()
	endpoint, err := url.Parse(os.Getenv("AHP_S3_ENDPOINT"))
	if err != nil || endpoint.Host == "" {
		t.Fatal("AHP_S3_ENDPOINT required")
	}
	s3, err := minio.New(endpoint.Host, &minio.Options{Creds: credentials.NewStaticV4(os.Getenv("AHP_MINIO_ROOT_USER"), os.Getenv("AHP_MINIO_ROOT_PASSWORD"), ""), Secure: endpoint.Scheme == "https"})
	if err != nil {
		t.Fatal("S3 configuration invalid")
	}
	bucket := fmt.Sprintf("ahp-sdk-%d", time.Now().UnixNano())
	st, err := store.New(pool, s3, bucket, func(context.Context, contracts.Scope, string, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err = st.Migrate(ctx); err != nil {
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
	signer, _ := auth.NewSigner("sdk-test", "k", priv)
	verifier := auth.NewVerifier("sdk-test", map[string]ed25519.PublicKey{"k": pub})
	var revoked atomic.Bool
	h, err := artifactsapi.New(st, verifier, func(context.Context, auth.Claims, string) error {
		if revoked.Load() {
			return contracts.NewError(contracts.ErrForbidden, "revoked")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	tenant := fmt.Sprintf("sdk-%d", time.Now().UnixNano())
	ca := newCA(t)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	srv := httptest.NewUnstartedServer(h)
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	srv.StartTLS()
	defer srv.Close()
	hc := srv.Client()
	tr := hc.Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{ca.clientCert(t, "spiffe://ahp/tenant/"+tenant+"/deployment/runner")}
	hc.Transport = tr
	mint := func(subject, contextID string) sdk.TokenSource {
		tok, e := signer.Sign(auth.Claims{Audience: "ahp", Kind: auth.KindTask, Tenant: tenant, Deployment: "runner", Subject: subject, Task: "grant-task", Root: "root", Context: contextID, Grant: "grant"}, time.Minute)
		if e != nil {
			t.Fatal(e)
		}
		return func(context.Context) (string, error) { return tok, nil }
	}
	c, err := sdk.New(sdk.Config{BaseURL: srv.URL, HTTPClient: hc, TokenSource: mint("alice", "ctx-a")})
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("\x00real owner API content\xff\n"), 4096)
	ref, err := c.Put(ctx, sdk.PutRequest{Key: "same-logical-file", Name: "a.bin", MIME: "application/octet-stream", Data: data})
	if err != nil {
		t.Fatal(err)
	}
	if ref.ContextID != "ctx-a" || ref.TaskID != "grant-task" || ref.Subject != "alice" {
		t.Fatalf("server binding lost: %+v", ref)
	}
	got, err := c.Download(ctx, ref.ID)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("download differs: len=%d err=%v", len(got), err)
	}
	chunk, err := c.ReadRange(ctx, ref.ID, 7, 100)
	if err != nil || !bytes.Equal(chunk, data[7:107]) {
		t.Fatalf("chunk differs: %v", err)
	}
	again, err := c.Put(ctx, sdk.PutRequest{Key: "same-logical-file", Name: "a.bin", MIME: "application/octet-stream", Data: data})
	if err != nil || again.ID != ref.ID {
		t.Fatalf("replay changed artifact: %v", err)
	}
	for _, source := range []sdk.TokenSource{mint("mallory", "ctx-a"), mint("alice", "ctx-b"), nil} {
		if _, err := c.WithTokenSource(source).Download(ctx, ref.ID); err == nil {
			t.Fatal("foreign or absent authority read artifact")
		}
	}
	revoked.Store(true)
	if _, err := c.Download(ctx, ref.ID); err == nil {
		t.Fatal("revoked authority read artifact")
	}
}

func TestClientRejectsUnsafeConfiguration(t *testing.T) {
	for _, base := range []string{"http://example.com", "https://example.com/a", "https://user:secret@example.com", "https://example.com?x=y"} {
		if _, err := sdk.New(sdk.Config{BaseURL: base, HTTPClient: &http.Client{}}); err == nil {
			t.Fatalf("accepted %s", base)
		}
	}
	if _, err := sdk.New(sdk.Config{BaseURL: "https://example.com", HTTPClient: &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}}); err == nil {
		t.Fatal("insecure TLS accepted")
	}
}
