package tools

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/model/secretstore"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/netpolicy"
	"github.com/jackc/pgx/v5/pgxpool"
)

type oauthFixtureResolver struct{}

func (oauthFixtureResolver) LookupHost(context.Context, string) ([]string, error) {
	return []string{"10.80.20.10"}, nil
}

type oauthFixtureDialer struct{ addr string }

func (d oauthFixtureDialer) DialContext(ctx context.Context, network, _ string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, d.addr)
}

type oauthFixture struct {
	server                     *httptest.Server
	mu                         sync.Mutex
	codes                      map[string]string
	refresh                    map[string]bool
	tokens, refreshes, revokes atomic.Int32
	failRefresh                atomic.Bool
}

func newOAuthFixture(t *testing.T) *oauthFixture {
	f := &oauthFixture{codes: map[string]string{}, refresh: map[string]bool{}}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/authorize" {
			q := r.URL.Query()
			if q.Get("code_challenge_method") != "S256" || q.Get("response_type") != "code" || q.Get("client_id") != "fixture-client" {
				http.Error(w, "invalid authorization", 400)
				return
			}
			code := oauthRandom()
			f.mu.Lock()
			f.codes[code] = q.Get("code_challenge")
			f.mu.Unlock()
			target, _ := url.Parse(q.Get("redirect_uri"))
			v := target.Query()
			v.Set("code", code)
			v.Set("state", q.Get("state"))
			target.RawQuery = v.Encode()
			http.Redirect(w, r, target.String(), 302)
			return
		}
		if r.Method != "POST" {
			http.Error(w, "method", 405)
			return
		}
		_ = r.ParseForm()
		if r.URL.Path == "/revoke" {
			f.revokes.Add(1)
			w.WriteHeader(200)
			return
		}
		if r.URL.Path != "/token" {
			http.NotFound(w, r)
			return
		}
		f.tokens.Add(1)
		f.mu.Lock()
		defer f.mu.Unlock()
		switch r.Form.Get("grant_type") {
		case "authorization_code":
			challenge, ok := f.codes[r.Form.Get("code")]
			sum := sha256.Sum256([]byte(r.Form.Get("code_verifier")))
			if !ok || base64.RawURLEncoding.EncodeToString(sum[:]) != challenge {
				http.Error(w, "invalid code or verifier", 400)
				return
			}
			delete(f.codes, r.Form.Get("code"))
		case "refresh_token":
			f.refreshes.Add(1)
			token := r.Form.Get("refresh_token")
			if !f.refresh[token] {
				http.Error(w, "invalid refresh", 400)
				return
			}
			delete(f.refresh, token)
			if f.failRefresh.Load() {
				http.Error(w, "simulated lost outcome", 503)
				return
			}
		default:
			http.Error(w, "grant", 400)
			return
		}
		refresh := oauthRandom()
		f.refresh[refresh] = true
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(oauthTokens{AccessToken: oauthRandom(), RefreshToken: refresh, TokenType: "Bearer", ExpiresIn: 3600, Scope: "read"})
	}))
	t.Cleanup(f.server.Close)
	return f
}

type oauthTestEnv struct {
	m                   *OAuthManager
	pool                *pgxpool.Pool
	vault               *secretstore.Client
	f                   *oauthFixture
	scope               contracts.Scope
	actor               auth.Claims
	cfg                 OAuthConfig
	opts                []netpolicy.Option
	ctx                 context.Context
	revoked, unlabelled atomic.Bool
}

func newOAuthTest(t *testing.T) *oauthTestEnv {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" || os.Getenv("AHP_VAULT_ADDR") == "" {
		t.Skip("real PostgreSQL AND Vault required: set AHP_TEST_DATABASE_URL and AHP_VAULT_ADDR")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	pool, e := pgxpool.New(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	if e = pool.Ping(ctx); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(pool.Close)
	vault, e := secretstore.NewFromEnv()
	if e != nil {
		t.Fatal(e)
	}
	env := &oauthTestEnv{pool: pool, vault: vault, f: newOAuthFixture(t), scope: contracts.Scope{Tenant: "oauth-tenant", Subject: "owner", Deployment: "agent"}, actor: auth.Claims{Kind: auth.KindHuman, Tenant: "oauth-tenant", Subject: "owner"}, ctx: ctx}
	env.cfg = OAuthConfig{Secrets: vault, SecretPrefix: "secret/data/ahp-oauth-tests/" + randHex(8), Providers: map[string]OAuthProvider{"personal": {AuthorizeURL: "http://oauth.fixture/authorize", TokenURL: "http://oauth.fixture/token", RevokeURL: "http://oauth.fixture/revoke", ClientID: "fixture-client", RedirectURI: "http://127.0.0.1:9999/", Scopes: []string{"read"}, Policy: netpolicy.Policy{PrivateHosts: []string{"oauth.fixture"}, AllowedMethods: []string{"GET", "POST"}, AllowedPathPrefixes: []string{"/authorize", "/token", "/revoke"}}}}, Authorize: func(context.Context, contracts.Scope) error {
		if env.revoked.Load() {
			return oauthDenied("grant revoked")
		}
		return nil
	}, LabelPrivate: func(context.Context, contracts.Scope) error {
		if env.unlabelled.Load() {
			return oauthDenied("label sink unavailable")
		}
		return nil
	}}
	env.opts = []netpolicy.Option{netpolicy.WithResolver(oauthFixtureResolver{}), netpolicy.WithDialer(oauthFixtureDialer{addr: strings.TrimPrefix(env.f.server.URL, "http://")})}
	env.m, e = NewOAuthManager(ctx, pool, "ahp_oauth_"+randHex(6), env.cfg, env.opts...)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		clean, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = env.m.DeleteTenant(clean, env.scope.Tenant)
		_ = env.m.DeleteTenant(clean, "foreign-tenant")
		_, _ = pool.Exec(clean, "DROP SCHEMA "+env.m.schema+" CASCADE")
	})
	return env
}
func (e *oauthTestEnv) code(t *testing.T, consent OAuthConsent) OAuthCallback {
	t.Helper()
	u, _ := url.Parse(consent.URL)
	u.Scheme = "http"
	u.Host = strings.TrimPrefix(e.f.server.URL, "http://")
	client := http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	r, err := client.Get(u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != 302 {
		t.Fatalf("authorize status %d", r.StatusCode)
	}
	target, err := url.Parse(r.Header.Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	return OAuthCallback{State: target.Query().Get("state"), Code: target.Query().Get("code"), Nonce: consent.Nonce}
}
func (e *oauthTestEnv) connect(t *testing.T) {
	t.Helper()
	c, err := e.m.Begin(e.ctx, e.actor, "personal")
	if err != nil {
		t.Fatal(err)
	}
	if err = e.m.Complete(e.ctx, e.actor, e.code(t, c)); err != nil {
		t.Fatal(err)
	}
}

func TestOAuthLifecycleRealPostgresVault(t *testing.T) {
	e := newOAuthTest(t)
	if err := e.m.Require(e.ctx, e.scope, "personal"); !errors.Is(err, ErrAuthRequired) {
		t.Fatalf("need AUTH_REQUIRED, got %v", err)
	}
	consent, err := e.m.Begin(e.ctx, e.actor, "personal")
	if err != nil {
		t.Fatal(err)
	}
	callback := e.code(t, consent)
	// Consent is human-only, callback owner and same-browser nonce are independently bound.
	task := e.actor
	task.Kind = auth.KindTask
	if _, err = e.m.Begin(e.ctx, task, "personal"); err == nil {
		t.Fatal("task gave consent")
	}
	for _, actor := range []auth.Claims{{Kind: auth.KindHuman, Tenant: e.actor.Tenant, Subject: "other"}, {Kind: auth.KindHuman, Tenant: "foreign-tenant", Subject: e.actor.Subject}} {
		if err = e.m.Complete(e.ctx, actor, callback); err == nil {
			t.Fatal("foreign callback accepted")
		}
	}
	bad := callback
	bad.Nonce = oauthRandom()
	if err = e.m.Complete(e.ctx, e.actor, bad); err == nil {
		t.Fatal("foreign nonce accepted")
	}
	// Many replicas race the same code. Exactly one durable consume reaches the provider.
	var successes atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if e.m.Complete(e.ctx, e.actor, callback) == nil {
				successes.Add(1)
			}
		}()
	}
	wg.Wait()
	if successes.Load() != 1 || e.f.tokens.Load() != 1 {
		t.Fatalf("callback winners %d provider exchanges %d", successes.Load(), e.f.tokens.Load())
	}
	if err = e.m.Complete(e.ctx, e.actor, callback); err == nil {
		t.Fatal("replay accepted")
	}
	cred, err := e.m.Resolve(e.ctx, e.scope, "personal")
	if err != nil || cred.Bearer == "" {
		t.Fatalf("resolve: %v", err)
	}
	// Database metadata and human view cannot contain raw token or PKCE values.
	var dump string
	if err = e.pool.QueryRow(e.ctx, "SELECT string_agg(row_to_json(t)::text,'') FROM (SELECT * FROM "+e.m.schema+".oauth_requests) t").Scan(&dump); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(dump, cred.Bearer) || strings.Contains(dump, callback.Code) || strings.Contains(dump, consent.Nonce) || strings.Contains(dump, callback.State) {
		t.Fatal("secret in SQL metadata")
	}
	// A second process sees the same durable grant and single-flights refresh.
	second, err := NewOAuthManager(e.ctx, e.pool, e.m.schema, e.cfg, e.opts...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = e.pool.Exec(e.ctx, "UPDATE "+e.m.schema+".oauth_connections SET expires_at=now()-interval '1 second'"); err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 12)
	tokens := make(chan string, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m := e.m
			if i%2 == 0 {
				m = second
			}
			c, err := m.Resolve(e.ctx, e.scope, "personal")
			errs <- err
			tokens <- c.Bearer
		}(i)
	}
	wg.Wait()
	close(errs)
	close(tokens)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	for token := range tokens {
		seen[token] = true
	}
	if len(seen) != 1 || e.f.refreshes.Load() != 1 {
		t.Fatalf("rotation was not single-flight: tokens=%d refreshes=%d", len(seen), e.f.refreshes.Load())
	}
	e.revoked.Store(true)
	if _, err = e.m.Resolve(e.ctx, e.scope, "personal"); err == nil {
		t.Fatal("revoked task resolved credential")
	}
	e.revoked.Store(false)
	e.unlabelled.Store(true)
	if _, err = e.m.Resolve(e.ctx, e.scope, "personal"); err == nil {
		t.Fatal("credential escaped without private label")
	}
	e.unlabelled.Store(false)
	if err = e.m.Revoke(e.ctx, e.actor, "personal"); err != nil {
		t.Fatal(err)
	}
	if _, err = e.m.Resolve(e.ctx, e.scope, "personal"); err == nil {
		t.Fatal("revoked connection usable")
	}
	if e.f.revokes.Load() != 2 {
		t.Fatalf("provider revoke count %d", e.f.revokes.Load())
	}
	if err = e.m.Revoke(e.ctx, e.actor, "personal"); err != nil {
		t.Fatal("revoke retry", err)
	}
}

func TestOAuthRefreshUnknownAndCASFailClosedRealPostgresVault(t *testing.T) {
	t.Run("unknown-refresh-never-retries-after-restart", func(t *testing.T) {
		e := newOAuthTest(t)
		e.connect(t)
		e.f.failRefresh.Store(true)
		_, _ = e.pool.Exec(e.ctx, "UPDATE "+e.m.schema+".oauth_connections SET expires_at=now()-interval '1 second'")
		if _, err := e.m.Resolve(e.ctx, e.scope, "personal"); err == nil {
			t.Fatal("ambiguous refresh accepted")
		}
		second, err := NewOAuthManager(e.ctx, e.pool, e.m.schema, e.cfg, e.opts...)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = second.Resolve(e.ctx, e.scope, "personal"); err == nil {
			t.Fatal("unknown refresh usable")
		}
		if e.f.refreshes.Load() != 1 {
			t.Fatal("refresh was replayed")
		}
	})
	t.Run("Vault-CAS-rejects-stale-generation", func(t *testing.T) {
		e := newOAuthTest(t)
		e.connect(t)
		var path string
		_ = e.pool.QueryRow(e.ctx, "SELECT secret_path FROM "+e.m.schema+".oauth_connections").Scan(&path)
		data, version, err := e.vault.ReadVersion(e.ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = e.vault.WriteCAS(e.ctx, path, data, version); err != nil {
			t.Fatal(err)
		}
		if _, err = e.vault.WriteCAS(e.ctx, path, data, version); !errors.Is(err, secretstore.ErrCAS) {
			t.Fatalf("stale CAS accepted %v", err)
		}
		if _, err = e.m.Resolve(e.ctx, e.scope, "personal"); err == nil {
			t.Fatal("stale stored version usable")
		}
	})
}

func TestOAuthTenantDeletionAndExpiredCallbackRealPostgresVault(t *testing.T) {
	e := newOAuthTest(t)
	e.connect(t)
	c, err := e.m.Begin(e.ctx, e.actor, "personal")
	if err != nil {
		t.Fatal(err)
	}
	callback := e.code(t, c)
	_, err = e.pool.Exec(e.ctx, "UPDATE "+e.m.schema+".oauth_requests SET expires_at=now()-interval '1 second' WHERE state_hash=$1", oauthHash(c.State))
	if err != nil {
		t.Fatal(err)
	}
	if err = e.m.Complete(e.ctx, e.actor, callback); err == nil {
		t.Fatal("expired callback")
	}
	c, err = e.m.Begin(e.ctx, e.actor, "personal")
	if err != nil {
		t.Fatal(err)
	}
	callback = e.code(t, c)
	var paths []string
	rows, err := e.pool.Query(e.ctx, "SELECT path FROM "+e.m.schema+".oauth_secrets")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var p string
		_ = rows.Scan(&p)
		paths = append(paths, p)
	}
	rows.Close()
	if err = e.m.DeleteTenant(e.ctx, e.scope.Tenant); err != nil {
		t.Fatal(err)
	}
	if err = e.m.Complete(e.ctx, e.actor, callback); err == nil {
		t.Fatal("late callback recreated tenant")
	}
	if _, err = e.m.Begin(e.ctx, e.actor, "personal"); err == nil {
		t.Fatal("late consent recreated tenant")
	}
	for _, p := range paths {
		if _, _, err = e.vault.ReadVersion(e.ctx, p); !errors.Is(err, secretstore.ErrSecretNotFound) {
			t.Fatalf("Vault metadata remains: %v", err)
		}
	}
	if err = e.m.DeleteTenant(e.ctx, e.scope.Tenant); err != nil {
		t.Fatal("delete retry", err)
	}
}

func TestOAuthProviderSSRF(t *testing.T) {
	base := OAuthProvider{AuthorizeURL: "https://oauth.example/authorize", TokenURL: "https://oauth.example/token", ClientID: "client", RedirectURI: "https://portal.example/", Policy: netpolicy.Policy{AllowedHosts: []string{"oauth.example"}, AllowedMethods: []string{"GET", "POST"}, AllowedPathPrefixes: []string{"/authorize", "/token"}}}
	for _, endpoint := range []string{"http://169.254.169.254/token", "https://127.0.0.1/token", "https://evil.example/token", "https://oauth.example/other", "https://user:secret@oauth.example/token"} {
		t.Run(fmt.Sprint(endpoint), func(t *testing.T) {
			p := base
			p.TokenURL = endpoint
			if _, err := newOAuthProvider(p); err == nil {
				t.Fatal("unsafe endpoint accepted")
			}
		})
	}
}

func TestOAuthTwoOwnersAndCancelledConsentRealPostgresVault(t *testing.T) {
	e := newOAuthTest(t)
	e.connect(t)
	ownerCredential, err := e.m.Resolve(e.ctx, e.scope, "personal")
	if err != nil {
		t.Fatal(err)
	}
	other := e.actor
	other.Subject = "second-owner"
	otherScope := e.scope
	otherScope.Subject = other.Subject
	pending, err := e.m.Begin(e.ctx, other, "personal")
	if err != nil {
		t.Fatal(err)
	}
	if err = e.m.Complete(e.ctx, other, OAuthCallback{State: pending.State, Nonce: pending.Nonce, Error: "access_denied"}); err != nil {
		t.Fatal(err)
	}
	if _, err = e.m.Resolve(e.ctx, otherScope, "personal"); err == nil {
		t.Fatal("cancelled consent produced credentials")
	}
	pending, err = e.m.Begin(e.ctx, other, "personal")
	if err != nil {
		t.Fatal(err)
	}
	if err = e.m.Complete(e.ctx, other, e.code(t, pending)); err != nil {
		t.Fatal(err)
	}
	otherCredential, err := e.m.Resolve(e.ctx, otherScope, "personal")
	if err != nil {
		t.Fatal(err)
	}
	if ownerCredential.Bearer == otherCredential.Bearer {
		t.Fatal("two owners share a provider token")
	}
	if err = e.m.Revoke(e.ctx, other, "personal"); err != nil {
		t.Fatal(err)
	}
	after, err := e.m.Resolve(e.ctx, e.scope, "personal")
	if err != nil || after.Bearer != ownerCredential.Bearer {
		t.Fatal("revoking another owner changed first grant")
	}
}

func TestOAuthProviderRedirectNeverCarriesToken(t *testing.T) {
	var foreignCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { foreignCalls.Add(1) }))
	defer target.Close()
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/steal", http.StatusTemporaryRedirect)
	}))
	defer provider.Close()
	cfg := OAuthProvider{AuthorizeURL: "http://oauth.fixture/authorize", TokenURL: "http://oauth.fixture/token", ClientID: "fixture", RedirectURI: "http://127.0.0.1/", Policy: netpolicy.Policy{PrivateHosts: []string{"oauth.fixture"}, AllowedMethods: []string{"GET", "POST"}, AllowedPathPrefixes: []string{"/authorize", "/token"}}}
	p, err := newOAuthProvider(cfg, netpolicy.WithResolver(oauthFixtureResolver{}), netpolicy.WithDialer(oauthFixtureDialer{strings.TrimPrefix(provider.URL, "http://")}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.request(context.Background(), p.config.TokenURL, url.Values{"refresh_token": {"secret-fixture"}}, "", true); err == nil {
		t.Fatal("redirect accepted")
	}
	if foreignCalls.Load() != 0 {
		t.Fatal("redirect leaked token body")
	}
}

func TestOAuthCallbackRaceWithRevokeAndDeleteRealPostgresVault(t *testing.T) {
	for _, deleting := range []bool{false, true} {
		t.Run(fmt.Sprintf("delete-%t", deleting), func(t *testing.T) {
			e := newOAuthTest(t)
			consent, err := e.m.Begin(e.ctx, e.actor, "personal")
			if err != nil {
				t.Fatal(err)
			}
			callback := e.code(t, consent)
			start := make(chan struct{})
			completed := make(chan error, 1)
			revoked := make(chan error, 1)
			go func() { <-start; completed <- e.m.Complete(e.ctx, e.actor, callback) }()
			go func() {
				<-start
				if deleting {
					revoked <- e.m.DeleteTenant(e.ctx, e.scope.Tenant)
				} else {
					revoked <- e.m.Revoke(e.ctx, e.actor, "personal")
				}
			}()
			close(start)
			<-completed
			if err = <-revoked; err != nil {
				t.Fatal(err)
			}
			if _, err = e.m.Resolve(e.ctx, e.scope, "personal"); err == nil {
				t.Fatal("credential survived revoke/delete race")
			}
			if err = e.m.Complete(e.ctx, e.actor, callback); err == nil {
				t.Fatal("callback resurrected revoked grant")
			}
			var count int
			err = e.pool.QueryRow(e.ctx, "SELECT count(*) FROM "+e.m.schema+".oauth_secrets WHERE tenant=$1", e.scope.Tenant).Scan(&count)
			if err != nil || count != 0 {
				t.Fatalf("secret inventory remains: %d %v", count, err)
			}
		})
	}
}
