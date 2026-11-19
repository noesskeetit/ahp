//go:build ahp_data

package main

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/workspace"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// The browser credential the user edge forwards. Identity puts the browser
// audience on it and no deployment at all, which is why the plane has to be
// told -- in the deployment, not in the token -- that user credentials arrive.
const (
	userTokenAudience  = "ahp-browser"
	userEdgeDeployment = "asws-edge-lane"
)

// userDataPlane builds the real data-plane faces against an unroutable database
// and an unreachable object store: pgx connects lazily and minio dials only on
// object I/O, so the mount, the credential rules and the rights gate are all
// exercised offline, and any request that survives them fails on I/O instead.
func userDataPlane(t *testing.T) (*artifacts.Store, *workspace.Store, *auth.Signer, contracts.Scope) {
	t.Helper()
	t.Setenv("AHP_DEV_JWT_SEED", hex.EncodeToString(make([]byte, ed25519.SeedSize)))
	t.Setenv("AHP_S3_ENDPOINT", "http://127.0.0.1:9000")
	t.Setenv("AHP_S3_BUCKET", "ws1-bucket")
	t.Setenv("AHP_S3_REGION", "us-east-1")

	pool, err := pgxpool.New(context.Background(),
		"postgres://ws1:ws1@127.0.0.1:59999/ws1?sslmode=disable")
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)

	s := &server{pool: pool}
	artStore, wsStore, _, err := s.newDataPlanes(context.Background())
	if err != nil {
		t.Fatalf("newDataPlanes: %v", err)
	}
	_, signer := devIdentityAuth()
	scope := contracts.Scope{Tenant: "t-usertok", Deployment: userEdgeDeployment,
		Subject: "human:owner-usertok"}
	return artStore, wsStore, signer, scope
}

func mountOverMTLS(t *testing.T, artifactsHandler, workspaceHandler http.Handler,
	ca *testCA, client tls.Certificate) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mountDataHandlers(mux, artifactsHandler, workspaceHandler)
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca.pem) {
		t.Fatal("ca pem")
	}
	srv := httptest.NewUnstartedServer(mux)
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
	srv.StartTLS()
	srv.Client().Transport.(*http.Transport).TLSClientConfig = &tls.Config{
		Certificates: []tls.Certificate{client}, InsecureSkipVerify: true}
	t.Cleanup(srv.Close)
	return srv
}

func postFace(t *testing.T, srv *httptest.Server, path, token string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+path, strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func userToken(t *testing.T, signer *auth.Signer, audience string, scope contracts.Scope) string {
	t.Helper()
	tok, err := signer.Sign(auth.Claims{Audience: audience, Kind: auth.KindHuman,
		Tenant: scope.Tenant, Subject: scope.Subject, JTI: "jti-usertok"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// TestWorkspaceFaceStaysTaskOnlyWhenNoEdgeIsSanctioned is the default-off
// guarantee: an unconfigured plane answers a forwarded browser bearer with the
// refusal every existing leg measures today, byte for byte.
func TestWorkspaceFaceStaysTaskOnlyWhenNoEdgeIsSanctioned(t *testing.T) {
	art, ws, signer, scope := userDataPlane(t)
	verifier, _ := devIdentityAuth()
	artifactsHandler, workspaceHandler, err := dataHandlers(art, ws, verifier)
	if err != nil {
		t.Fatalf("dataHandlers: %v", err)
	}
	ca := newTestCA(t)
	srv := mountOverMTLS(t, artifactsHandler, workspaceHandler, ca,
		ca.clientCert(t, auth.WorkloadID(scope)))
	code, body := postFace(t, srv, "/v1/workspace/GetWorkspace", userToken(t, signer, userTokenAudience, scope))
	t.Logf("no sanctioned edge: http=%d body=%s", code, strings.TrimSpace(body))
	if code != http.StatusForbidden {
		t.Fatalf("status = %d body=%s, want 403 while no edge is sanctioned", code, body)
	}
	if !strings.Contains(body, "audience") {
		t.Fatalf("body = %s, want the audience refusal the lane measured", body)
	}
}

// TestWorkspaceFaceVerifiesTheUserCredentialWhenAnEdgeIsSanctioned is the ruling
// made measurable: with this deployment named, the same credential on the same
// face clears authentication and authentication only -- the request then dies on
// the unreachable store, never on the credential.
func TestWorkspaceFaceVerifiesTheUserCredentialWhenAnEdgeIsSanctioned(t *testing.T) {
	art, ws, signer, scope := userDataPlane(t)
	verifier, _ := devIdentityAuth()
	artifactsHandler, workspaceHandler, err := dataHandlersWithUserEdges(art, ws, verifier,
		[]string{userEdgeDeployment}, userTokenAudience)
	if err != nil {
		t.Fatalf("dataHandlersWithUserEdges: %v", err)
	}
	ca := newTestCA(t)
	srv := mountOverMTLS(t, artifactsHandler, workspaceHandler, ca,
		ca.clientCert(t, auth.WorkloadID(scope)))
	code, body := postFace(t, srv, "/v1/workspace/GetWorkspace", userToken(t, signer, userTokenAudience, scope))
	t.Logf("sanctioned edge: http=%d body=%s", code, strings.TrimSpace(body))
	if code == http.StatusForbidden {
		t.Fatalf("status = 403 body=%s, want the credential accepted and the request to reach the store", body)
	}
	for _, refuse := range []string{"audience", "sanctioned", "task token"} {
		if strings.Contains(body, refuse) {
			t.Fatalf("body = %s, must not refuse on %q once the edge is sanctioned", body, refuse)
		}
	}
}

// TestWorkspaceFaceRefusesAUserCredentialFromAnUnsanctionedWorkload: naming one
// edge must not mean naming every workload that holds a client certificate, or
// any deployment in the tenant could replay a stolen browser bearer.
func TestWorkspaceFaceRefusesAUserCredentialFromAnUnsanctionedWorkload(t *testing.T) {
	art, ws, signer, scope := userDataPlane(t)
	verifier, _ := devIdentityAuth()
	artifactsHandler, workspaceHandler, err := dataHandlersWithUserEdges(art, ws, verifier,
		[]string{"some-other-edge"}, userTokenAudience)
	if err != nil {
		t.Fatalf("dataHandlersWithUserEdges: %v", err)
	}
	ca := newTestCA(t)
	srv := mountOverMTLS(t, artifactsHandler, workspaceHandler, ca,
		ca.clientCert(t, auth.WorkloadID(scope)))
	code, body := postFace(t, srv, "/v1/workspace/GetWorkspace", userToken(t, signer, userTokenAudience, scope))
	if code != http.StatusForbidden {
		t.Fatalf("status = %d body=%s, want 403 from an unsanctioned workload", code, body)
	}
	if !strings.Contains(body, "sanctioned") {
		t.Fatalf("body = %s, want the policy's own refusal", body)
	}
}

// TestArtifactFaceNeverAcceptsAUserCredential: the browser route was ruled for
// the workspace face. The artifact boundary keeps demanding a task token, so
// mounting a user path on one plane cannot widen the other.
func TestArtifactFaceNeverAcceptsAUserCredential(t *testing.T) {
	art, ws, signer, scope := userDataPlane(t)
	verifier, _ := devIdentityAuth()
	artifactsHandler, workspaceHandler, err := dataHandlersWithUserEdges(art, ws, verifier,
		[]string{userEdgeDeployment}, userTokenAudience)
	if err != nil {
		t.Fatalf("dataHandlersWithUserEdges: %v", err)
	}
	ca := newTestCA(t)
	srv := mountOverMTLS(t, artifactsHandler, workspaceHandler, ca,
		ca.clientCert(t, auth.WorkloadID(scope)))
	code, body := postFace(t, srv, "/v1/artifacts/BeginUpload", userToken(t, signer, userTokenAudience, scope))
	if code != http.StatusForbidden {
		t.Fatalf("status = %d body=%s, want 403 on the artifact face", code, body)
	}
}

// TestWorkspaceUserEdgeConfigFailsClosed: the switch is a named deployment, and
// a value that names none -- empty, blanks, commas only -- must leave the face
// shut rather than open it to "everyone".
func TestWorkspaceUserEdgeConfigFailsClosed(t *testing.T) {
	t.Setenv("AHP_WORKSPACE_USER_EDGE_DEPLOYMENTS", "")
	t.Setenv("AHP_WORKSPACE_USER_AUDIENCE", "")
	if edges, audience := workspaceUserEdgeConfig(); edges != nil || audience != "" {
		t.Fatalf("edges=%v audience=%q, want the face closed when nothing is named", edges, audience)
	}
	t.Setenv("AHP_WORKSPACE_USER_EDGE_DEPLOYMENTS", " , ,")
	if edges, audience := workspaceUserEdgeConfig(); edges != nil || audience != "" {
		t.Fatalf("edges=%v audience=%q, want blanks to keep the face closed", edges, audience)
	}
	t.Setenv("AHP_WORKSPACE_USER_EDGE_DEPLOYMENTS", " asws-edge-lane , other-edge ")
	edges, audience := workspaceUserEdgeConfig()
	if len(edges) != 2 || edges[0] != "asws-edge-lane" || edges[1] != "other-edge" {
		t.Fatalf("edges = %v, want both named deployments, trimmed", edges)
	}
	if audience != "ahp-browser" {
		t.Fatalf("audience = %q, want the browser audience identity issues", audience)
	}
	t.Setenv("AHP_WORKSPACE_USER_AUDIENCE", "ahp-embedded-client")
	if _, audience := workspaceUserEdgeConfig(); audience != "ahp-embedded-client" {
		t.Fatalf("audience = %q, want the configured audience to win", audience)
	}
}
