package artifacts_test

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	sdk "github.com/ahp-platform/ahp/sdk/go/artifacts"
)

func mtlsFixture(t *testing.T, h http.Handler) (*httptest.Server, *http.Client) {
	t.Helper()
	ca := newCA(t)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	srv := httptest.NewUnstartedServer(h)
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	client := srv.Client()
	tr := client.Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{ca.clientCert(t, "spiffe://ahp/tenant/sdk/deployment/runner")}
	client.Transport = tr
	return srv, client
}

func TestDownloadRejectsCorruptBodyAndCanceledContext(t *testing.T) {
	data := []byte("correct bytes")
	sum := sha256.Sum256(data)
	var calls atomic.Int32
	srv, hc := mtlsFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path == "/v1/artifacts/Get" {
			_ = json.NewEncoder(w).Encode(sdk.ArtifactRef{ID: "artifact", Version: "v1", SHA256: hex.EncodeToString(sum[:]), Size: int64(len(data))})
			return
		}
		_, _ = w.Write([]byte("wrong!! bytes"))
	}))
	c, err := sdk.New(sdk.Config{BaseURL: srv.URL, HTTPClient: hc, TokenSource: func(context.Context) (string, error) { return "task-token", nil }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Download(context.Background(), "artifact"); err == nil {
		t.Fatal("corrupt bytes accepted")
	}
	before := calls.Load()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Download(ctx, "artifact"); err == nil {
		t.Fatal("canceled request accepted")
	}
	if calls.Load() != before {
		t.Fatal("canceled context made network request")
	}
}

func TestUploadDoesNotSendTaskOrWorkloadCredentials(t *testing.T) {
	var bearerSeen, certSeen atomic.Bool
	blob := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bearerSeen.Store(r.Header.Get("Authorization") != "")
		certSeen.Store(len(r.TLS.PeerCertificates) > 0)
		w.WriteHeader(http.StatusOK)
	}))
	blob.TLS = &tls.Config{ClientAuth: tls.RequestClientCert}
	blob.StartTLS()
	defer blob.Close()
	owner, hc := mtlsFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "unused", 500) }))
	upload := blob.Client()
	uploadTr := upload.Transport.(*http.Transport).Clone()
	uploadTr.TLSClientConfig.Certificates = hc.Transport.(*http.Transport).TLSClientConfig.Certificates
	upload.Transport = uploadTr
	c, err := sdk.New(sdk.Config{BaseURL: owner.URL, HTTPClient: hc, UploadHTTPClient: upload, TokenSource: func(context.Context) (string, error) { return "must-not-leak", nil }})
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("bytes")
	sum := sha256.Sum256(data)
	if err := c.Upload(context.Background(), sdk.Upload{ID: "id", PresignedURL: blob.URL, ExpectedSHA: hex.EncodeToString(sum[:]), ExpectedSize: int64(len(data))}, data); err != nil {
		t.Fatal(err)
	}
	if bearerSeen.Load() || certSeen.Load() {
		t.Fatal("presigned upload disclosed task or workload credentials")
	}
}
