package toolsapi

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/model/secretstore"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/internal/tools"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/netpolicy"
	"github.com/ahp-platform/ahp/pkg/transport"
	"github.com/jackc/pgx/v5/pgxpool"
)

type fixtureResolver struct{}

func (fixtureResolver) LookupHost(context.Context, string) ([]string, error) {
	return []string{"10.10.10.10"}, nil
}

type fixtureDialer struct{ address string }

func (d fixtureDialer) DialContext(ctx context.Context, network, _ string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, d.address)
}

func TestOAuthHumanHTTPProxyRealPostgresVault(t *testing.T) {
	if os.Getenv("AHP_TEST_DATABASE_URL") == "" || os.Getenv("AHP_VAULT_ADDR") == "" {
		t.Skip("real PostgreSQL and Vault required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, os.Getenv("AHP_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	vault, err := secretstore.NewFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	var tokenCalls atomic.Int32
	var challenge string
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/authorize" {
			q := r.URL.Query()
			challenge = q.Get("code_challenge")
			u, _ := url.Parse(q.Get("redirect_uri"))
			q2 := u.Query()
			q2.Set("state", q.Get("state"))
			q2.Set("code", "fixture-single-use-code")
			u.RawQuery = q2.Encode()
			http.Redirect(w, r, u.String(), 302)
			return
		}
		tokenCalls.Add(1)
		_ = r.ParseForm()
		sum := sha256.Sum256([]byte(r.Form.Get("code_verifier")))
		if base64.RawURLEncoding.EncodeToString(sum[:]) != challenge {
			http.Error(w, "PKCE", 400)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "fixture-access-secret", "refresh_token": "fixture-refresh-secret", "token_type": "Bearer", "expires_in": 3600, "scope": "read"})
	}))
	defer provider.Close()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner("oauth-http", "key", key)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier("oauth-http", map[string]ed25519.PublicKey{"key": signer.Public()})
	human := auth.Claims{Kind: auth.KindHuman, Tenant: "tenant", Subject: "owner", Audience: "browser"}
	token, err := signer.Sign(human, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	schema := "ahp_oauth_http_" + strings.ToLower(strings.ReplaceAll(contracts.NewID()[30:], "_", ""))[:12]
	front := httptest.NewServer(http.NotFoundHandler())
	defer front.Close()
	policy := netpolicy.Policy{PrivateHosts: []string{"provider.fixture"}, AllowedMethods: []string{"GET", "POST"}, AllowedPathPrefixes: []string{"/authorize", "/token"}}
	m, err := tools.NewOAuthManager(ctx, pool, schema, tools.OAuthConfig{Secrets: vault, SecretPrefix: "secret/data/oauth-http/" + schema, Providers: map[string]tools.OAuthProvider{"personal": {AuthorizeURL: "http://provider.fixture/authorize", TokenURL: "http://provider.fixture/token", RedirectURI: front.URL + "/", ClientID: "fixture", Scopes: []string{"read"}, Policy: policy}}, Authorize: func(context.Context, contracts.Scope) error { return nil }, LabelPrivate: func(context.Context, contracts.Scope) error { return nil }}, netpolicy.WithResolver(fixtureResolver{}), netpolicy.WithDialer(fixtureDialer{strings.TrimPrefix(provider.URL, "http://")}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = m.DeleteTenant(context.Background(), "tenant")
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
	}()
	mcpServer := newMCPFixture(t)
	svc, err := tools.New(ctx, pool, schema, tools.Config{Guardian: tools.NewPolicyGuardian("test"), OAuth: m, Credentials: tools.OAuthResolver{OAuth: m}, Connections: map[string]tools.Connector{"personal": mcpServer.connector()}, Tools: map[string]tools.ToolSpec{"echo": {Connection: "personal", Name: "echo"}}})
	if err != nil {
		t.Fatal(err)
	}
	authorize := func(_ context.Context, a auth.Claims, capability string) error {
		if a.Kind == auth.KindTask && capability == "tools" {
			return nil
		}
		if a.Kind != auth.KindHuman || capability != ConnectCapability {
			return contracts.NewError(contracts.ErrForbidden, "human connect required")
		}
		return nil
	}
	handler, err := NewHandlerWithUsers(svc, verifier, authorize, "browser", authorize)
	if err != nil {
		t.Fatal(err)
	}
	ca := newCA(t)
	clientCert := ca.clientCert(t, workloadURI("tenant", "space"))
	roots := x509.NewCertPool()
	roots.AddCert(ca.cert)
	backend := httptest.NewUnstartedServer(handler)
	backend.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots, MinVersion: tls.VersionTLS12}
	backend.StartTLS()
	defer backend.Close()
	proxyClient := backend.Client()
	tr := proxyClient.Transport.(*http.Transport).Clone()
	tr.TLSClientConfig = tr.TLSClientConfig.Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{clientCert}
	proxyClient.Transport = tr
	proxy, err := space.NewHumanToolsProxy(backend.URL, proxyClient)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle(space.UIToolsPath, transport.Human(verifier, "browser")(proxy))
	// Explicit browser fixture session, not production Identity/login acceptance.
	mux.HandleFunc("/u-ahp/v1/auth/token", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": token, "expires_in": 3600, "subject": human.Subject, "tenant": human.Tenant})
	})
	mux.Handle("/", http.FileServer(http.Dir("../../../web/aifactory")))
	front.Config.Handler = mux
	jar, _ := cookiejar.New(nil)
	browser := &http.Client{Jar: jar}
	call := func(client *http.Client, bearer, op string, body any) (int, []byte) {
		b, _ := json.Marshal(body)
		req, _ := http.NewRequest(http.MethodPost, front.URL+space.UIToolsPath+op, bytes.NewReader(b))
		req.Header.Set("Authorization", "Bearer "+bearer)
		req.Header.Set("Content-Type", "application/json")
		resp, e := client.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		defer resp.Body.Close()
		out, e := io.ReadAll(resp.Body)
		if e != nil {
			t.Fatal(e)
		}
		return resp.StatusCode, out
	}

	machine := auth.Claims{Kind: auth.KindTask, Audience: "ahp", Tenant: "tenant", Subject: "owner", Deployment: "space", Task: "task", Root: "root", Context: "ctx", Grant: "grant"}
	machineToken, err := signer.Sign(machine, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	invoke := func() (int, []byte) {
		req, _ := http.NewRequest(http.MethodPost, backend.URL+"/v1/tools/Call", strings.NewReader(`{"key":"personal-call","tool":"echo","arguments":{}}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+machineToken)
		resp, err := proxyClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, b
	}
	status, body := invoke()
	var authRequired struct {
		Code   string `json:"code"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(body, &authRequired); err != nil || status != http.StatusConflict ||
		authRequired.Code != "AUTH_REQUIRED" || authRequired.Reason != "AUTH_REQUIRED" {
		t.Fatalf("missing connection: %d %s (decode error: %v)", status, body, err)
	}

	status, body = call(browser, token, "OAuthBegin", map[string]string{"connection": "personal"})
	if status != 200 {
		t.Fatalf("begin status %d: %s", status, body)
	}
	var consent struct {
		URL string `json:"authorize_url"`
	}
	if err = json.Unmarshal(body, &consent); err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(consent.URL)
	u.Host = strings.TrimPrefix(provider.URL, "http://")
	noRedirect := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := noRedirect.Get(u.String())
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	redirect, _ := url.Parse(resp.Header.Get("Location"))
	callback := map[string]string{"state": redirect.Query().Get("state"), "code": redirect.Query().Get("code")}
	foreign := human
	foreign.Subject = "other"
	foreignToken, _ := signer.Sign(foreign, time.Hour)
	for _, tc := range []struct {
		client *http.Client
		bearer string
	}{{http.DefaultClient, token}, {browser, foreignToken}} {
		status, _ = call(tc.client, tc.bearer, "OAuthCallback", callback)
		if status != 403 {
			t.Fatalf("nonce/owner refusal %d", status)
		}
	}
	status, body = call(browser, token, "OAuthCallback", callback)
	if status != 200 {
		t.Fatalf("callback %d %s", status, body)
	}
	if status, body := invoke(); status != 200 || !bytes.Contains(body, []byte("SUCCEEDED")) {
		t.Fatalf("personal tool: %d %s", status, body)
	}
	if got := mcpServer.lastBearer.Load(); got != "Bearer fixture-access-secret" {
		t.Fatal("MCP did not receive its resolved personal provider token")
	}
	status, body = call(browser, token, "OAuthConnections", map[string]string{})
	if status != 200 || !bytes.Contains(body, []byte("active")) {
		t.Fatalf("connections %d %s", status, body)
	}
	for _, secret := range []string{"fixture-access-secret", "fixture-refresh-secret", "code_verifier", "secret_path"} {
		if bytes.Contains(body, []byte(secret)) {
			t.Fatal("secret leaked to human response")
		}
	}
	status, _ = call(browser, token, "OAuthCallback", callback)
	if status != 403 || tokenCalls.Load() != 1 {
		t.Fatal("callback replay reached token endpoint")
	}
	task := human
	task.Kind = auth.KindTask
	task.Deployment = "agent"
	task.Task = "task"
	task.Root = "root"
	task.Context = "context"
	task.Grant = "grant"
	taskToken, err := signer.Sign(task, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{"OAuthBegin", "OAuthConnections", "OAuthRevoke", "OAuthCallback"} {
		status, _ = call(browser, taskToken, op, map[string]string{"connection": "personal"})
		if status != 403 {
			t.Fatalf("task reached human %s: %d", op, status)
		}
	}
	if script := os.Getenv("AHP_OAUTH_BROWSER_SCRIPT"); script != "" {
		// Browser requests use the same fixture's loopback URL; token POST still goes
		// through the netpolicy resolver/dial test seam to that real HTTP endpoint.
		cmd := exec.CommandContext(ctx, "node", script, front.URL, provider.URL)
		cmd.Env = os.Environ()
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("browser fixture: %v: %s", err, out)
		}
		t.Log(string(out))
	}

}
