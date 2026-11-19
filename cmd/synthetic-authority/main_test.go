package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/tools"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/netpolicy"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// This builds and starts the actual CLI as a separate process. Neither HTTP
// fixtures nor a direct handler call substitutes for the standalone smoke.
func TestStandaloneAuthorityCLIAndNegativeHTTPS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("real PostgreSQL required")
	}
	tmp := t.TempDir()
	binary := filepath.Join(tmp, "authority")
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, ".")
	if out, e := build.CombinedOutput(); e != nil {
		t.Fatalf("standalone build: %v %s", e, out)
	}
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	pubDER, _ := x509.MarshalPKIXPublicKey(pub)
	pubfile := filepath.Join(tmp, "ahp-public.pem")
	if e := os.WriteFile(pubfile, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER}), 0600); e != nil {
		t.Fatal(e)
	}
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	issuer := "https://" + l.Addr().String()
	l.Close()
	dir := filepath.Join(tmp, "private")
	command := func(args ...string) ([]byte, error) {
		c := exec.CommandContext(ctx, binary, args...)
		c.Env = append(os.Environ(), "AHP_DATABASE_URL="+dsn)
		return c.CombinedOutput()
	}
	if _, e = command("init", "--dir", dir, "--issuer", issuer, "--ahp-public-key", pubfile); e != nil {
		t.Fatalf("init failed: %v", e)
	}
	cfg, issuerKey, _, e := load(dir)
	if e != nil {
		t.Fatal(e)
	}
	for _, badIssuer := range []string{"https://0.0.0.0:29443", "https://[::]:29443", "https://203.0.113.8:29443"} {
		if _, err := command("init", "--dir", filepath.Join(tmp, "bad-init"), "--issuer", badIssuer, "--ahp-public-key", pubfile); err == nil {
			t.Fatal("authority initialized outside loopback")
		}
		bad := cfg
		bad.Issuer = badIssuer
		raw, _ := json.Marshal(bad)
		if err := os.WriteFile(filepath.Join(dir, "config.json"), raw, 0600); err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := load(dir); err == nil {
			t.Fatal("edited configuration exposed authority outside loopback")
		}
	}
	cfg.Connections["trusted"] = destination{Audience: "mcp-trusted", Resource: "urn:mcp:trusted", Scope: "read"}
	raw, _ := json.Marshal(cfg)
	if e = os.WriteFile(filepath.Join(dir, "config.json"), raw, 0600); e != nil {
		t.Fatal(e)
	}
	pool, e := pgxpool.New(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer pool.Close()
	policy := identity.NewSyntheticResourcePolicy(pool)
	ids := identity.New(pool, identity.WithResourceEvaluator(policy))
	if e = ids.Migrate(ctx); e != nil {
		t.Fatal(e)
	}
	sc := contracts.Scope{Tenant: "standalone-" + contracts.NewID(), Subject: "alice", Deployment: "agent"}
	for _, r := range []identity.ResourceRule{{Tenant: sc.Tenant, Subject: sc.Subject, Resource: identity.Resource{Kind: "tenant", ID: sc.Tenant}, Action: "login", Allowed: true}, {Tenant: sc.Tenant, Subject: sc.Subject, Resource: identity.Resource{Kind: "tenant", ID: sc.Tenant}, Action: "tools", Allowed: true}, {Tenant: sc.Tenant, Subject: sc.Subject, Resource: identity.Resource{Kind: "deployment", ID: sc.Deployment}, Action: "tools", Allowed: true}, {Tenant: sc.Tenant, Subject: sc.Subject, Resource: identity.Resource{Kind: "connection", ID: "trusted"}, Action: "use", Allowed: true}} {
		if _, e = policy.PutRule(ctx, r, 0); e != nil {
			t.Fatal(e)
		}
	}
	task := "task-" + contracts.NewID()
	g, e := ids.CreateRoot(ctx, sc, identity.RootRequest{Task: task, Context: task, Deployment: sc.Deployment, Capabilities: []string{"tools"}, TTL: time.Minute})
	if e != nil {
		t.Fatal(e)
	}
	signer, _ := auth.NewSigner("ahp", "dev-1", key)
	taskToken, e := ids.MintTask(ctx, sc, task, signer, "ahp", time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	verifier := auth.NewVerifier("ahp", map[string]ed25519.PublicKey{"dev-1": pub})
	claims, e := verifier.Verify(taskToken, "ahp", auth.KindTask, auth.WorkloadID(sc))
	if e != nil {
		t.Fatal(e)
	}
	var logs bytes.Buffer
	proc := exec.CommandContext(ctx, binary, "serve", "--dir", dir)
	proc.Env = append(os.Environ(), "AHP_DATABASE_URL="+dsn)
	proc.Stdout = &logs
	proc.Stderr = &logs
	if e = proc.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() {
		_ = proc.Process.Signal(syscall.SIGTERM)
		_ = proc.Wait()
		if strings.Contains(logs.String(), taskToken) {
			t.Error("task credential in authority logs")
		}
	}()
	caFile := filepath.Join(dir, "tls.pem")
	ca, _ := os.ReadFile(caFile)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca)
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}}}
	ready := false
	for i := 0; i < 60; i++ {
		resp, err := client.Get(issuer + "/.well-known/openid-configuration")
		if err == nil {
			resp.Body.Close()
			ready = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ready {
		t.Fatal("standalone HTTPS authority did not become ready")
	}
	outFile := filepath.Join(tmp, "human.jwt")
	mintLog, e := command("mint", "--dir", dir, "--tenant", sc.Tenant, "--subject", sc.Subject, "--out", outFile)
	if e != nil {
		t.Fatalf("operator mint failed: %v", e)
	}
	humanRaw, e := readPrivate(outFile)
	if e != nil {
		t.Fatal(e)
	}
	human := strings.TrimSpace(string(humanRaw))
	if bytes.Contains(mintLog, []byte(human)) {
		t.Fatal("mint printed credential")
	}
	oidc, e := identity.NewOIDCAuthenticator(identity.OIDCConfig{Issuer: issuer, DiscoveryURL: issuer + "/.well-known/openid-configuration", ClientID: cfg.Audience, HTTPClient: client, Synthetic: true})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = oidc.Login(ctx, identity.LoginInput{Assertion: human}); e != nil {
		t.Fatal("standalone operator assertion was not verifiable")
	}
	if _, e = command("mint", "--dir", dir, "--tenant", sc.Tenant, "--subject", "foreign", "--out", filepath.Join(tmp, "forbidden.jwt")); e == nil {
		t.Fatal("operator minted an unprovisioned principal")
	}
	registration := tools.ExchangeRegistration{TokenURL: issuer + "/token", Audience: "mcp-trusted", Resource: "urn:mcp:trusted", Scope: "read", SyntheticLoopbackCAFile: caFile, Policy: netpolicy.Policy{AllowedPathPrefixes: []string{"/token"}}}
	target := identity.ConnectionTarget{Connection: "trusted", TokenURL: registration.TokenURL, Audience: registration.Audience, Resource: registration.Resource, Scope: registration.Scope}
	issue := func(ctx context.Context, _ contracts.Scope, _ string, cfg tools.ExchangeRegistration) (string, error) {
		x := target
		x.Audience = cfg.Audience
		return ids.MintConnectionAssertion(ctx, claims, x, signer)
	}
	check := func(ctx context.Context, _ contracts.Scope, c string) error {
		return ids.RequireConnection(ctx, claims, c)
	}
	resolver, e := tools.NewSyntheticExchangeResolver("synthetic", map[string]tools.ExchangeRegistration{"trusted": registration}, issue, check, nil)
	if e != nil {
		t.Fatal(e)
	}
	cred, e := resolver.Resolve(ctx, sc, "trusted")
	if e != nil {
		t.Fatal(e)
	}
	parts := strings.Split(cred.Bearer, ".")
	if len(parts) != 3 {
		t.Fatal("downstream token not signed")
	}
	signature, _ := base64.RawURLEncoding.DecodeString(parts[2])
	if !ed25519.Verify(issuerKey.Public().(ed25519.PublicKey), []byte(parts[0]+"."+parts[1]), signature) {
		t.Fatal("invalid downstream credential signature")
	}
	payload, _ := base64.RawURLEncoding.DecodeString(parts[1])
	var access map[string]any
	_ = json.Unmarshal(payload, &access)
	if access["aud"] != "mcp-trusted" || access["token_use"] != "mcp_access" || access["resource"] != "urn:mcp:trusted" {
		t.Fatal("downstream access constraints lost")
	}
	if _, e = oidc.Login(ctx, identity.LoginInput{Assertion: cred.Bearer}); e == nil {
		t.Fatal("MCP token accepted as public login")
	}
	post := func(proof, audience string) int {
		form := url.Values{"grant_type": {tools.TokenExchangeGrantType}, "subject_token": {proof}, "subject_token_type": {"urn:ietf:params:oauth:token-type:jwt"}, "audience": {audience}, "resource": {target.Resource}, "scope": {target.Scope}}
		resp, err := client.PostForm(issuer+"/token", form)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode
	}
	if post(human, target.Audience) != 403 || post(taskToken, target.Audience) != 403 {
		t.Fatal("raw human/task accepted for external exchange")
	}
	assertion, e := ids.MintConnectionAssertion(ctx, claims, target, signer)
	if e != nil {
		t.Fatal(e)
	}
	if post(assertion, "foreign") != 403 {
		t.Fatal("cross-audience accepted")
	}
	if post(assertion, target.Audience) != 200 || post(assertion, target.Audience) != 403 {
		t.Fatal("assertion replay boundary failed")
	}
	stale, e := ids.MintConnectionAssertion(ctx, claims, target, signer)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = policy.PutRule(ctx, identity.ResourceRule{Tenant: sc.Tenant, Subject: sc.Subject, Resource: identity.Resource{Kind: "connection", ID: "trusted"}, Action: "use", Allowed: false}, 1); e != nil {
		t.Fatal(e)
	}
	if post(stale, target.Audience) != 403 {
		t.Fatal("old assertion survived live resource revoke")
	}
	for _, f := range []string{"issuer-key.pem", "tls-key.pem", "config.json"} {
		if _, e = readPrivate(filepath.Join(dir, f)); e != nil {
			t.Fatal(e)
		}
	}
	t.Logf("standalone SYNTHETIC HTTPS/CLI verified; grant epoch %d; secrets written only to temporary 0600 files", g.Epoch)
}
