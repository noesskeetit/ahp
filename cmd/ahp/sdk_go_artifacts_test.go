//go:build ahp_data

package main

import (
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
	"testing"
	"time"
	"unicode/utf8"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/artifactsapi"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	sdk "github.com/ahp-platform/ahp/sdk/go/artifacts"
)

// New Go read/derive helpers operate on bytes created by the public Python
// client. Storage, Identity and the Linux sandbox parser are actual owners.
func TestGoArtifactsPythonCreatedTextRealOwners(t *testing.T) {
	for _, key := range []string{"AHP_TEST_DATABASE_URL", "AHP_S3_ENDPOINT", "AHP_MINIO_ROOT_USER", "AHP_MINIO_ROOT_PASSWORD"} {
		if os.Getenv(key) == "" {
			t.Fatalf("%s required; no skip", key)
		}
	}
	if runtime.GOOS != "linux" {
		t.Fatal("real sandbox parser requires Linux")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, store, scope := cascadeEnv(t, ctx)
	ids := identity.New(pool)
	if err := ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	artifacts.WithGrantLabels(dataArtifactGrantLabels(ids))(store)
	parser := filepath.Join(t.TempDir(), "artifact-parser")
	if output, err := exec.CommandContext(ctx, "go", "build", "-o", parser, "../../cmd/artifact-parser").CombinedOutput(); err != nil {
		t.Fatalf("build parser %v: %s", err, output)
	}
	artifacts.WithParserCommand(artifacts.ParserCommand{Path: parser})(store)
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := auth.NewSigner("go-artifacts", "k", priv)
	verifier := auth.NewVerifier("go-artifacts", map[string]ed25519.PublicKey{"k": pub})
	mint := func(s contracts.Scope, task, contextID string) string {
		t.Helper()
		_, err := ids.CreateRoot(ctx, s, identity.RootRequest{Task: task, Context: contextID, Deployment: s.Deployment, Capabilities: []string{"artifacts"}, TTL: 10 * time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		token, err := ids.MintTask(ctx, s, task, signer, "ahp", 10*time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	token := mint(scope, "source", scope.Tenant+"-source")
	handler, err := artifactsapi.New(store, verifier, ids.Authorize)
	if err != nil {
		t.Fatal(err)
	}
	ca := newTestCA(t)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	srv := httptest.NewUnstartedServer(handler)
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	srv.StartTLS()
	defer srv.Close()
	newHTTP := func(s contracts.Scope) *http.Client {
		hc := srv.Client()
		tr := hc.Transport.(*http.Transport).Clone()
		tr.TLSClientConfig.Certificates = []tls.Certificate{ca.clientCert(t, auth.WorkloadID(s))}
		hc.Transport = tr
		t.Cleanup(hc.CloseIdleConnections)
		return hc
	}
	sourceHTTP := newHTTP(scope)
	source, err := sdk.New(sdk.Config{BaseURL: srv.URL, HTTPClient: sourceHTTP, TokenSource: func(context.Context) (string, error) { return token, nil }})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	write := func(name string, data []byte) string {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	cert := sourceHTTP.Transport.(*http.Transport).TLSClientConfig.Certificates[0]
	key, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	text := "First parity привет\nSecond indexed line."
	cfg := map[string]any{"base": srv.URL, "token": token, "text": text, "output": filepath.Join(dir, "refs.json"), "ca": write("ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.TLS.Certificates[0].Certificate[0]})), "cert": write("client.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]})), "key": write("client.key", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}))}
	raw, _ := json.Marshal(cfg)
	path := write("config.json", raw)
	const python = `import json,os,ssl,sys,httpx
from ahp_clients import ArtifactsClient
with open(sys.argv[1]) as f:c=json.load(f)
tls=ssl.create_default_context(cafile=c['ca']);tls.load_cert_chain(c['cert'],c['key'])
with httpx.Client(verify=tls,trust_env=False,timeout=30) as http:
 client=ArtifactsClient(c['base'],lambda:c['token'],http_client=http)
 refs={name:client.put(name,name,mime,data) for name,mime,data in [('text','text/plain',c['text'].encode()),('binary','application/octet-stream',b'opaque'),('malformed','application/json',b'{broken')]}
 assert client.readiness(refs['text']['id'])['ready']
fd=os.open(c['output'],os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
with os.fdopen(fd,'w') as f:json.dump(refs,f)
print('Python public client created and sealed text/binary/quarantined JSON via real owner API')
`
	t.Run("PythonCreatesThroughOwner", func(t *testing.T) {
		command := exec.CommandContext(ctx, "python3", "-c", python, path)
		command.Env = append(os.Environ(), "PYTHONPATH=../../sdk/python:"+os.Getenv("PYTHONPATH"))
		if out, err := command.CombinedOutput(); err != nil {
			t.Fatalf("Python creation %v: %s", err, out)
		} else {
			t.Log(string(out))
		}
	})
	var refs map[string]sdk.ArtifactRef
	raw, err = os.ReadFile(cfg["output"].(string))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &refs); err != nil {
		t.Fatal(err)
	}
	ref := refs["text"]
	t.Run("TypedReadinessAndProcess", func(t *testing.T) {
		for _, call := range []func(context.Context, string) (sdk.Readiness, error){source.Readiness, source.Process} {
			rd, err := call(ctx, ref.ID)
			if err != nil || !rd.ContentReady || !rd.ExtractionReady || !rd.SearchReady || !rd.Ready || rd.Quarantined {
				t.Fatalf("text readiness %+v %v", rd, err)
			}
		}
		rd, err := source.Readiness(ctx, refs["binary"].ID)
		if err != nil || !rd.ContentReady || rd.ExtractionReady || rd.Ready {
			t.Fatalf("binary readiness %+v %v", rd, err)
		}
		rd, err = source.Readiness(ctx, refs["malformed"].ID)
		if err != nil || !rd.Quarantined || rd.Ready {
			t.Fatalf("quarantine readiness %+v %v", rd, err)
		}
	})
	t.Run("VersionPinnedUTF8ChunksSearchAndResolve", func(t *testing.T) {
		var got strings.Builder
		var offset int64
		for i := 0; i < 30; i++ {
			chunk, err := source.ReadTextChunk(ctx, ref.ID, ref.Version, offset, 32768, 9)
			if err != nil {
				t.Fatal(err)
			}
			if chunk.ID != ref.ID || chunk.Version != ref.Version || chunk.SHA256 != ref.SHA256 || chunk.Offset != offset || chunk.TotalBytes != int64(len([]byte(text))) || !utf8.ValidString(chunk.Text) || len(chunk.Text) > 9 {
				t.Fatalf("chunk bounds %+v", chunk)
			}
			got.WriteString(chunk.Text)
			if chunk.EOF {
				break
			}
			if chunk.NextOffset <= offset {
				t.Fatal("no chunk progress")
			}
			offset = chunk.NextOffset
		}
		if got.String() != text {
			t.Fatalf("text changed: %q", got.String())
		}
		for _, term := range []string{"parity", "absentword"} {
			match, err := source.SearchText(ctx, ref.ID, ref.Version, term)
			if err != nil || match.ID != ref.ID || match.Version != ref.Version || match.Matched != (term == "parity") {
				t.Fatalf("search %+v %v", match, err)
			}
		}
		resolved, err := source.Resolve(ctx, ref.SHA256)
		if err != nil || resolved.SHA256 != ref.SHA256 || len(resolved.Versions) != 1 {
			t.Fatalf("resolve %+v %v", resolved, err)
		}
		v := resolved.Versions[0]
		if v.ArtifactID != ref.ID || v.Version != ref.Version || v.SHA256 != ref.SHA256 || v.Size != ref.Size {
			t.Fatalf("resolve identity %+v", v)
		}
		body, err := source.Download(ctx, v.ArtifactID)
		if err != nil || string(body) != text {
			t.Fatalf("download resolved version %v", err)
		}
	})
	t.Run("VersionAndBudgetRefusals", func(t *testing.T) {
		if _, err := source.ReadTextChunk(ctx, ref.ID, "tampered", 0, 32, 32); contracts.Code(err) != contracts.ErrConflict {
			t.Fatal("wrong text version", err)
		}
		if _, err := source.SearchText(ctx, ref.ID, "tampered", "parity"); contracts.Code(err) != contracts.ErrConflict {
			t.Fatal("wrong search version", err)
		}
		if _, err := source.ReadTextChunk(ctx, ref.ID, ref.Version, 0, 32, 0); contracts.Code(err) != contracts.ErrInvalid {
			t.Fatal("missing context budget", err)
		}
		if _, err := source.ReadTextChunk(ctx, refs["malformed"].ID, refs["malformed"].Version, 0, 32, 32); err == nil {
			t.Fatal("quarantined text exposed")
		}
		if _, err := source.Resolve(ctx, strings.Repeat("0", 64)); contracts.Code(err) != contracts.ErrNotFound {
			t.Fatal("missing digest", err)
		}
	})
	assertDenied := func(t *testing.T, c *sdk.Client, want, resolveWant string) {
		t.Helper()
		calls := []func() error{func() error { _, e := c.Readiness(ctx, ref.ID); return e }, func() error { _, e := c.Process(ctx, ref.ID); return e }, func() error { _, e := c.ReadTextChunk(ctx, ref.ID, ref.Version, 0, 32, 32); return e }, func() error { _, e := c.SearchText(ctx, ref.ID, ref.Version, "parity"); return e }, func() error { _, e := c.Resolve(ctx, ref.SHA256); return e }}
		for i, call := range calls {
			err := call()
			code := contracts.Code(err)
			expected := want
			if i == 4 {
				expected = resolveWant
			}
			if code != expected {
				t.Fatalf("operation%d expected%s got%v", i, expected, err)
			}
		}
	}
	for _, dimension := range []string{"tenant", "subject", "deployment"} {
		t.Run("Foreign_"+dimension, func(t *testing.T) {
			foreign := scope
			switch dimension {
			case "tenant":
				foreign.Tenant += "-other"
			case "subject":
				foreign.Subject += "-other"
			default:
				foreign.Deployment += "-other"
			}
			foreignToken := mint(foreign, "foreign", scope.Tenant+"-foreign-"+dimension)
			c, e := sdk.New(sdk.Config{BaseURL: srv.URL, HTTPClient: newHTTP(foreign), TokenSource: func(context.Context) (string, error) { return foreignToken, nil }})
			if e != nil {
				t.Fatal(e)
			}
			expected := contracts.ErrForbidden
			if dimension == "tenant" || dimension == "subject" {
				expected = contracts.ErrNotFound
			}
			assertDenied(t, c, expected, contracts.ErrNotFound)
		})
	}
	t.Run("CurrentRevokeAllHelpers", func(t *testing.T) {
		if err := ids.CancelSubtree(ctx, scope, "source"); err != nil {
			t.Fatal(err)
		}
		assertDenied(t, source, contracts.ErrForbidden, contracts.ErrForbidden)
	})
}
