package main

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/statejournal"
	"github.com/ahp-platform/ahp/pkg/auth"
)

func TestCompactStateRoutesShareOriginWithLegacyState(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/state/", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("State-Owner", "legacy")
	})
	mountCompactStateRoutes(mux, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("State-Owner", "compact")
	}))

	for _, route := range []struct {
		path, owner string
	}{
		{"/v1/state/sessions", "compact"},
		{"/v1/state/sessions:acquire", "compact"},
		{"/v1/state/sessions:enqueue", "compact"},
		{"/v1/state/sessions:claim-turn", "compact"},
		{"/v1/state/sessions/a/events:append", "compact"},
		{"/v1/state/sessions/a:heartbeat", "compact"},
		{"/v1/state/sessions/a/checkpoints", "compact"},
		{"/v1/state/sessions/a/vm:claim", "compact"},
		{"/internal/v1/state/vm-reconcile", "compact"},
		{"/v1/state/kv", "compact"},
		{"/v1/state/kv:mutate", "compact"},
		{"/v1/state/aux:mutate", "compact"},
		{"/v1/state/intents:mutate", "compact"},
		{"/v1/state/ClaimRun", "legacy"},
		{"/v1/state/GetKV", "legacy"},
		{"/v1/state/ReadStream", "legacy"},
	} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, route.path, nil))
		if actual := rec.Header().Get("State-Owner"); actual != route.owner {
			t.Errorf("%s owner = %q, want %q", route.path, actual, route.owner)
		}
	}
}

func TestCompactStateChecksSignedCapabilitiesWithWorkloadCertificate(t *testing.T) {
	public, private, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner("ahp", "compact-test", private)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier("ahp", map[string]ed25519.PublicKey{"compact-test": public})
	handler, err := statejournal.NewHTTP(statejournal.New(nil), verifier, compactStateClaimsAuthorize)
	if err != nil {
		t.Fatal(err)
	}

	ca := newMountCA(t)
	roots := x509.NewCertPool()
	roots.AddCert(ca.cert)
	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	server.StartTLS()
	t.Cleanup(server.Close)
	client := server.Client()
	httpTransport := client.Transport.(*http.Transport)
	httpTransport.TLSClientConfig.Certificates = []tls.Certificate{
		ca.clientCert(t, "spiffe://ahp/tenant/test/deployment/compact"),
	}
	for _, tc := range []struct {
		name string
		caps []string
		want int
	}{
		{"missing", nil, http.StatusForbidden},
		{"wildcard", []string{"state.*"}, http.StatusForbidden},
		{"only retention", []string{"state.retention"}, http.StatusForbidden},
		{"authorized", []string{"state"}, http.StatusNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			token, err := signer.Sign(auth.Claims{
				Kind: auth.KindTask, Audience: "ahp", Tenant: "test", Deployment: "compact",
				Subject: "user", Task: "task", Root: "task", Context: "session", Grant: "grant",
				Capabilities: tc.caps,
			}, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			// A nonexistent subroute only answers 404 once transport authentication
			// and authorization succeeded. This path never requires a database.
			request, err := http.NewRequestWithContext(context.Background(), "GET", server.URL+"/v1/state/sessions/session/unknown", nil)
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Authorization", "Bearer "+token)
			response, err := client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != tc.want {
				t.Errorf("status = %d, want %d", response.StatusCode, tc.want)
			}
		})
	}
}

func TestCompactStateMountRequiresVerifiedTLS(t *testing.T) {
	t.Setenv("AHP_ENABLE_COMPACT_STATE", "1")
	s := &server{}
	_, err := s.mountCompactState(context.Background(), http.NewServeMux(), nil, compactStateClaimsAuthorize)
	if err == nil {
		t.Fatal("compact state mounted with no verified workload transport")
	}
}

func TestCompactStateS3PlatformConfig(t *testing.T) {
	valid := map[string]string{
		"AHP_S3_ENDPOINT": "http://127.0.0.1:9000", "AHP_S3_BUCKET": "test-bucket",
		"AHP_MINIO_ROOT_USER": "test-user", "AHP_MINIO_ROOT_PASSWORD": "test-pass",
		"AHP_STATE_S3_BUCKET": "dedicated-bucket", "AHP_S3_REGION": "us-east-1",
	}
	config, err := compactStateS3Settings(func(name string) string { return valid[name] })
	if err != nil || config.endpoint != "127.0.0.1:9000" || config.secure || config.bucket != "dedicated-bucket" {
		t.Fatalf("unexpected platform object storage settings: endpoint=%q secure=%t bucket=%q err=%v",
			config.endpoint, config.secure, config.bucket, err)
	}

	delete(valid, "AHP_MINIO_ROOT_PASSWORD")
	if _, err = compactStateS3Settings(func(name string) string { return valid[name] }); err == nil {
		t.Fatal("partial object storage settings silently left the journal in inline-only mode")
	}
}
