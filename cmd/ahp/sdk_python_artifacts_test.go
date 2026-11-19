//go:build ahp_data

package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/artifactsapi"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	sdk "github.com/ahp-platform/ahp/sdk/go/artifacts"
)

// Actual PostgreSQL, versioned MinIO, Identity and mTLS owner HTTP. Extraction
// runs the repository's Linux seccomp parser binary, never a SQL readiness edit.
func TestPythonArtifactsRealOwners(t *testing.T) {
	for _, key := range []string{"AHP_TEST_DATABASE_URL", "AHP_S3_ENDPOINT", "AHP_MINIO_ROOT_USER", "AHP_MINIO_ROOT_PASSWORD"} {
		if os.Getenv(key) == "" {
			t.Fatalf("%s required; no skip", key)
		}
	}
	if runtime.GOOS != "linux" {
		t.Fatal("real sandbox parser requires Linux; run this suite in the Linux lane")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, store, scope := cascadeEnv(t, ctx, func(_ context.Context, s contracts.Scope, action, _ string) error {
		if err := s.Validate(); err != nil {
			return err
		}
		switch action {
		case "read", "upload", "seal", "derive":
			return nil
		}
		return contracts.NewError(contracts.ErrForbidden, "test task action refused")
	})
	ids := identity.New(pool)
	if err := ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := configureArtifactTransfer(store, ids); err != nil {
		t.Fatal(err)
	}
	artifacts.WithGrantLabels(dataArtifactGrantLabels(ids))(store)
	parser := filepath.Join(t.TempDir(), "artifact-parser")
	build := exec.CommandContext(ctx, "go", "build", "-o", parser, "../../cmd/artifact-parser")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("real parser build: %v %s", err, out)
	}
	artifacts.WithParserCommand(artifacts.ParserCommand{Path: parser})(store)
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := auth.NewSigner("python-artifacts", "k", priv)
	verifier := auth.NewVerifier("python-artifacts", map[string]ed25519.PublicKey{"k": pub})
	task := func(s contracts.Scope, id, contextID string) string {
		t.Helper()
		_, err := ids.CreateRoot(ctx, s, identity.RootRequest{Task: id, Context: contextID, Deployment: s.Deployment, Capabilities: []string{"artifacts"}, TTL: 10 * time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		token, err := ids.MintTask(ctx, s, id, signer, "ahp", 10*time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	sourceContext := scope.Tenant + "-source"
	targetContext := scope.Tenant + "-target"
	sourceToken := task(scope, "source", sourceContext)
	targetToken := task(scope, "target", targetContext)
	revoked := task(scope, "revoked", sourceContext)
	raceToken := task(scope, "race", sourceContext)
	if err := ids.CancelSubtree(ctx, scope, "revoked"); err != nil {
		t.Fatal(err)
	}
	ah, err := artifactsapi.New(store, verifier, ids.Authorize)
	if err != nil {
		t.Fatal(err)
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Test-Lose-ACK") == "1" {
			record := httptest.NewRecorder()
			ah.ServeHTTP(record, r)
			if record.Code != 200 {
				t.Errorf("lost ACK owner status %d", record.Code)
			}
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				conn.Close()
			}
			return
		}
		if r.Header.Get("X-Test-Revoke-After-Get") == "1" && strings.HasSuffix(r.URL.Path, "/Get") {
			record := httptest.NewRecorder()
			ah.ServeHTTP(record, r)
			if err := ids.CancelSubtree(ctx, scope, "race"); err != nil {
				t.Error(err)
			}
			for k, v := range record.Header() {
				w.Header()[k] = v
			}
			w.WriteHeader(record.Code)
			w.Write(record.Body.Bytes())
			return
		}
		ah.ServeHTTP(w, r)
	})
	ca := newTestCA(t)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	srv := httptest.NewUnstartedServer(handler)
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	srv.StartTLS()
	defer srv.Close()
	cert := ca.clientCert(t, auth.WorkloadID(scope))
	httpClient := srv.Client()
	tr := httpClient.Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{cert}
	httpClient.Transport = tr
	client := func(token string) *sdk.Client {
		t.Helper()
		c, err := sdk.New(sdk.Config{BaseURL: srv.URL, HTTPClient: httpClient, TokenSource: func(context.Context) (string, error) { return token, nil }})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	goData := []byte("Go immutable bytes\x00\xff")
	var goRef sdk.ArtifactRef
	t.Run("GoUploadSource", func(t *testing.T) {
		var err error
		goRef, err = client(sourceToken).Put(ctx, sdk.PutRequest{Key: "go-source", Name: "go.bin", MIME: "application/octet-stream", Data: goData})
		if err != nil {
			t.Fatal(err)
		}
	})
	if goRef.ID == "" {
		t.Fatal("source fixture failed")
	}
	dir := t.TempDir()
	write := func(name string, data []byte) string {
		t.Helper()
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, data, 0600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	hand := map[string]any{"base": srv.URL, "source": sourceToken, "subject_id": scope.Subject, "target": targetToken, "revoked": revoked, "race": raceToken, "owner_deployment": scope.Deployment, "target_context": targetContext, "target_task": "target", "go_ref": goRef, "go_data": goData, "output": filepath.Join(dir, "output.json"), "ca": write("ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.TLS.Certificates[0].Certificate[0]}))}
	certFiles := func(name string, cert tls.Certificate) {
		der, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
		if err != nil {
			t.Fatal(err)
		}
		hand[name+"_cert"] = write(name+".pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}))
		hand[name+"_key"] = write(name+".key", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	}
	var blobCalls atomic.Int32
	blobTrap := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		blobCalls.Add(1)
		if len(r.TLS.PeerCertificates) != 0 || r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			t.Error("presigned PUT leaked task/workload credentials")
		}
		if r.Method != "PUT" {
			t.Error("unexpected blob method")
		}
		w.WriteHeader(http.StatusOK)
	}))
	blobTrap.TLS = &tls.Config{ClientAuth: tls.RequestClientCert, ClientCAs: roots}
	blobTrap.StartTLS()
	defer blobTrap.Close()
	hand["blob_url"] = blobTrap.URL
	hand["blob_ca"] = write("blob-ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: blobTrap.TLS.Certificates[0].Certificate[0]}))
	certFiles("client", cert)
	for _, dimension := range []string{"tenant", "deployment", "subject"} {
		s := scope
		switch dimension {
		case "tenant":
			s.Tenant += "-foreign"
		case "deployment":
			s.Deployment += "-foreign"
		case "subject":
			s.Subject += "-foreign"
		}
		hand[dimension] = task(s, "foreign-"+dimension, sourceContext+"-"+dimension)
		certFiles(dimension, ca.clientCert(t, auth.WorkloadID(s)))
	}
	hand["other_context"] = task(scope, "other-context", scope.Tenant+"-other")
	raw, _ := json.Marshal(hand)
	config := write("config.json", raw)
	t.Run("PythonSyncAsyncRealHTTP", func(t *testing.T) {
		command := exec.CommandContext(ctx, "python3", "../../sdk/python/ahp_clients/tests/artifacts_live.py", config)
		command.Env = append(os.Environ(), "PYTHONPATH=../../sdk/python:"+os.Getenv("PYTHONPATH"))
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("Python artifact contract: %v\n%s", err, output)
		} else {
			t.Log(string(output))
		}
	})
	if blobCalls.Load() != 2 {
		t.Fatalf("credential trap calls %d", blobCalls.Load())
	}
	var output struct{ Sync, Async, Transfer sdk.ArtifactRef }
	raw, err = os.ReadFile(hand["output"].(string))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &output); err != nil {
		t.Fatal(err)
	}
	t.Run("GoCrossReadsPythonBytesAndTransferLabels", func(t *testing.T) {
		for _, row := range []struct {
			ref   sdk.ArtifactRef
			want  string
			token string
		}{{output.Sync, "Python sync alpha searchable привет", sourceToken}, {output.Async, "Python async exact bytes", sourceToken}, {output.Transfer, "Python sync alpha searchable привет", targetToken}} {
			got, err := client(row.token).Download(ctx, row.ref.ID)
			if err != nil || !bytes.Equal(got, []byte(row.want)) {
				t.Fatalf("cross read: %v", err)
			}
		}
		g, err := ids.Evaluate(ctx, scope, "target", "artifacts")
		if err != nil {
			t.Fatal(err)
		}
		if len(g.Labels.Subjects) != 1 || g.Labels.Subjects[0] != scope.Subject {
			t.Fatalf("transfer failed to taint target: %+v", g.Labels)
		}
	})
}
