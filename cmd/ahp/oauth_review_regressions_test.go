package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/model/secretstore"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/internal/tools"
	"github.com/ahp-platform/ahp/internal/tools/toolsapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
	"github.com/ahp-platform/ahp/pkg/netpolicy"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Regression fixtures use an isolated real PostgreSQL database and real Vault.
// Provider traffic and the verified mTLS request object are explicit test seams.
func oauthRegressionPool(t *testing.T, max int32) *pgxpool.Pool {
	t.Helper()
	if os.Getenv("AHP_TEST_DATABASE_URL") == "" || os.Getenv("AHP_VAULT_ADDR") == "" {
		t.Skip("real PostgreSQL and Vault required")
	}
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(os.Getenv("AHP_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal("parse substrate")
	}
	admin, err := pgxpool.NewWithConfig(ctx, cfg.Copy())
	if err != nil {
		t.Fatal("admin pool")
	}
	db := fmt.Sprintf("oauth_review_%d", time.Now().UnixNano())
	if _, err = admin.Exec(ctx, "CREATE DATABASE "+db); err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Database = db
	cfg.MaxConns = max
	cfg.ConnConfig.RuntimeParams["application_name"] = "ahp-shared-pool"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, err := admin.Exec(ctx, "DROP DATABASE "+db)
		if err != nil {
			t.Error("own DB cleanup", err)
		}
		admin.Close()
	})
	t.Log("isolated real PostgreSQL database", db)
	return pool
}
func oauthRegressionConfig(t *testing.T, secrets tools.OAuthSecrets) tools.OAuthConfig {
	return tools.OAuthConfig{Providers: map[string]tools.OAuthProvider{"personal": {AuthorizeURL: "https://provider.example/authorize", TokenURL: "https://provider.example/token", ClientID: "review", RedirectURI: "http://127.0.0.1:9999/", Scopes: []string{"read"}, Policy: netpolicy.Policy{AllowedHosts: []string{"provider.example"}, AllowedMethods: []string{"GET", "POST"}, AllowedPathPrefixes: []string{"/authorize", "/token"}}}}, Secrets: secrets, SecretPrefix: fmt.Sprintf("secret/data/oauth-review/%d", time.Now().UnixNano()), Authorize: func(context.Context, contracts.Scope) error { return nil }, LabelPrivate: func(context.Context, contracts.Scope) error { return nil }}
}

type oauthRegressionHTTP func(*http.Request) (*http.Response, error)

func (f oauthRegressionHTTP) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type oauthRegressionResolver struct{}

func (oauthRegressionResolver) LookupHost(context.Context, string) ([]string, error) {
	return []string{"10.90.0.1"}, nil
}

type oauthRegressionDialer struct{ address string }

func (d oauthRegressionDialer) DialContext(ctx context.Context, network, _ string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, d.address)
}

func TestOAuthRevokedHumanSessionRealIdentity(t *testing.T) {
	ctx := context.Background()
	pool := oauthRegressionPool(t, 4)
	ids := identity.New(pool)
	if err := ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	user := identity.BrowserUser{UserID: "review-user", Tenant: "review-tenant", Subject: "review-owner"}
	if err := ids.SeedBrowserUser(ctx, user, "review-user", "review-password-is-synthetic", nil, []string{"tools:connect"}); err != nil {
		t.Fatal(err)
	}
	vault, err := secretstore.NewFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	cfg := oauthRegressionConfig(t, vault)
	m, err := tools.NewOAuthManager(ctx, pool, "ahp_review_oauth", cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		clean, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := m.DeleteTenant(clean, user.Tenant); err != nil {
			t.Error("own Vault cleanup", err)
		}
	})
	svc, err := tools.New(ctx, pool, "ahp_review_oauth", tools.Config{OAuth: m, Guardian: tools.NewPolicyGuardian("review"), Credentials: tools.OAuthResolver{OAuth: m}, Connections: map[string]tools.Connector{"personal": nil}, Tools: map[string]tools.ToolSpec{"read": {Connection: "personal", Name: "read"}}})
	if err != nil {
		t.Fatal(err)
	}
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner("review", "review", key)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier("review", map[string]ed25519.PublicKey{"review": signer.Public()})
	handler, err := toolsapi.NewHandlerWithUsers(svc, verifier, func(context.Context, auth.Claims, string) error { return nil }, "browser", toolsHumanAuthorize(ids, nil))
	if err != nil {
		t.Fatal(err)
	}
	uri, _ := url.Parse("spiffe://ahp/tenant/review-tenant/deployment/space")
	client := &http.Client{Transport: oauthRegressionHTTP(func(r *http.Request) (*http.Response, error) {
		r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{URIs: []*url.URL{uri}}}}}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w.Result(), nil
	})}
	proxy, err := space.NewHumanToolsProxy("https://gateway.fixture", client)
	if err != nil {
		t.Fatal(err)
	}
	mw, err := space.HumanAuthMiddleware(space.HumanAuthConfig{Issuer: "review", Audience: "browser", Keys: map[string]ed25519.PublicKey{"review": signer.Public()}})
	if err != nil {
		t.Fatal(err)
	}
	edge := mw(proxy)
	var lastBody string
	call := func(token, operation string) int {
		r := httptest.NewRequest("POST", space.UIToolsPath+operation, bytes.NewBufferString(`{"connection":"personal"}`))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		edge.ServeHTTP(w, r)
		lastBody = w.Body.String()
		return w.Code
	}
	for _, mode := range []string{"logout", "refresh", "subject-revoked"} {
		ses, err := ids.StartBrowserSession(ctx, user.UserID)
		if err != nil {
			t.Fatal(err)
		}
		jti := "review-" + mode
		if err = ids.BindBrowserToken(ctx, jti, ses); err != nil {
			t.Fatal(err)
		}
		token, err := signer.Sign(auth.Claims{Kind: auth.KindHuman, Tenant: user.Tenant, Subject: user.Subject, Audience: "browser", JTI: jti}, 10*time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if got := call(token, "OAuthBegin"); got != 200 {
			t.Fatalf("live session begin=%d", got)
		}
		switch mode {
		case "logout":
			err = ids.RevokeBrowserSession(ctx, ses.ID)
		case "refresh":
			_, err = ids.RefreshBrowserSession(ctx, ses.ID)
		case "subject-revoked":
			err = ids.RevokeSubject(ctx, user.Tenant, user.Subject)
		}
		if err != nil {
			t.Fatal(err)
		}
		if _, err = ids.BrowserTokenActive(ctx, jti); err == nil {
			t.Fatal("Identity did not retire browser token")
		}
		var denialBody string
		for _, operation := range []string{"OAuthBegin", "OAuthConnections", "OAuthCallback", "OAuthRevoke"} {
			got := call(token, operation)
			t.Logf("%s: inactive session %s status=%d", mode, operation, got)
			if denialBody == "" {
				denialBody = lastBody
			} else if lastBody != denialBody {
				t.Errorf("%s bypassed uniform live-session denial", operation)
			}
			if got != http.StatusForbidden {
				t.Errorf("inactive %s credential reached %s: %d", mode, operation, got)
			}
		}
	}
	var count int
	if err = pool.QueryRow(ctx, "SELECT count(*) FROM ahp_review_oauth.oauth_requests").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Errorf("only live sessions may create requests, got %d", count)
	}
}

type oauthRegressionBlockingVault struct {
	*secretstore.Client
	entered chan struct{}
	resume  chan struct{}
	once    atomic.Bool
}

func (v *oauthRegressionBlockingVault) ReadVersion(ctx context.Context, path string) (map[string]any, int, error) {
	data, version, err := v.Client.ReadVersion(ctx, path)
	if strings.HasSuffix(path, "/fixture-tokens") && v.once.CompareAndSwap(false, true) {
		close(v.entered)
		select {
		case <-v.resume:
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		}
	}
	return data, version, err
}

// admissionCountSQL samples, in one statement over one snapshot, the sessions on
// this database, every backend attached to it, the connections the shared pool
// owns (in-pool and detached), and the type of anything that is not a session.
const admissionCountSQL = `SELECT count(*) FILTER (WHERE backend_type='client backend'),
	count(*),
	count(*) FILTER (WHERE application_name='ahp-shared-pool'),
	coalesce(string_agg(DISTINCT coalesce(backend_type,'NULL'), ',') FILTER (WHERE backend_type IS DISTINCT FROM 'client backend'),'')
	FROM pg_stat_activity WHERE datname=current_database()`

// oauthAdmissionDetail runs only on the violation path, on the observer connection
// that is already open, so it costs a healthy run nothing. It re-samples the
// session/non-session split to catch the backend the outer count saw, then lists
// every backend together with the statistics of the pool that constructed it.
func oauthAdmissionDetail(t *testing.T, tag string, bound, sessions, allBackends int, nonClient string, main, observer *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	peak, peakNonClient := allBackends, nonClient
	for i := 0; i < 200; i++ {
		var all int
		var types string
		if err := observer.QueryRow(ctx, `SELECT count(*),
				coalesce(string_agg(DISTINCT coalesce(backend_type,'NULL'), ',') FILTER (WHERE backend_type IS DISTINCT FROM 'client backend'),'')
				FROM pg_stat_activity WHERE datname=current_database()`).Scan(&all, &types); err != nil {
			t.Errorf("admission detail: %v", err)
			return
		}
		if all > peak {
			peak, peakNonClient = all, types
		}
		if peakNonClient != "" {
			break
		}
	}
	rows, err := observer.Query(ctx, `SELECT pid, coalesce(backend_type,'NULL'), coalesce(usename,'NULL'),
		coalesce(application_name,'NULL'), coalesce(state,'NULL'),
		left(regexp_replace(coalesce(query,'NULL'), E'\\\\s+', ' ', 'g'), 64)
		FROM pg_stat_activity WHERE datname=current_database() ORDER BY pid`)
	if err != nil {
		t.Errorf("admission detail: %v", err)
		return
	}
	var b strings.Builder
	fmt.Fprintf(&b, "admission detail %s: sessions=%d bound=%d all_backends=%d peak_all=%d non_client_types=%q\n",
		tag, sessions, bound, allBackends, peak, peakNonClient)
	for rows.Next() {
		var pid int
		var bt, usr, app, st, q string
		if err := rows.Scan(&pid, &bt, &usr, &app, &st, &q); err != nil {
			t.Errorf("admission detail: %v", err)
			break
		}
		fmt.Fprintf(&b, "admission detail   pid=%d type=%s user=%s app=%s state=%s | %s\n", pid, bt, usr, app, st, q)
	}
	rows.Close()
	ms := main.Stat()
	fmt.Fprintf(&b, "admission detail   shared pool: held=%d acquired=%d idle=%d constructing=%d constructed=%d acquires=%d",
		ms.TotalConns(), ms.AcquiredConns(), ms.IdleConns(), ms.ConstructingConns(), ms.NewConnsCount(), ms.AcquireCount())
	t.Log(b.String())
}

func TestOAuthSharedIdentityPoolRealIdentity(t *testing.T) {
	for _, max := range []int32{1, 2} {
		for _, mode := range []string{"same-owner", "different-owner", "cancel-waiter", "cancel-holder", "revoked-before-post", "storm"} {
			t.Run(fmt.Sprintf("pool-%d/%s", max, mode), func(t *testing.T) { oauthSharedIdentityPool(t, max, mode) })
		}
	}
}

func oauthSharedIdentityPool(t *testing.T, max int32, mode string) {
	ctx, cancelAll := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelAll()
	pool := oauthRegressionPool(t, max)
	ids := identity.New(pool)
	if err := ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	scope := contracts.Scope{Tenant: "review-tenant", Subject: "review-owner", Deployment: "agent"}
	grant, err := ids.CreateRoot(ctx, scope, identity.RootRequest{Task: "review-task", Context: "review-context", Deployment: scope.Deployment, Capabilities: []string{"tools"}, TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	claims := auth.Claims{Kind: auth.KindTask, TokenUse: auth.TokenUse, Tenant: scope.Tenant, Subject: scope.Subject, Deployment: scope.Deployment, Task: grant.Task, Root: grant.Root, Grant: grant.ID, Context: grant.Context, Depth: grant.Depth, Actors: grant.Actors}
	waiterScope := scope
	authority := map[string]auth.Claims{scope.Subject: claims}
	if mode == "different-owner" {
		waiterScope.Subject = "second-owner"
		other, err := ids.CreateRoot(ctx, waiterScope, identity.RootRequest{Task: "second-task", Context: "second-context", Deployment: scope.Deployment, Capabilities: []string{"tools"}, TTL: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		otherClaims := claims
		otherClaims.Subject = waiterScope.Subject
		otherClaims.Task = other.Task
		otherClaims.Root = other.Root
		otherClaims.Context = other.Context
		otherClaims.Grant = other.ID
		otherClaims.Actors = other.Actors
		authority[waiterScope.Subject] = otherClaims
	}
	vault, err := secretstore.NewFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	blocking := &oauthRegressionBlockingVault{Client: vault, entered: make(chan struct{}), resume: make(chan struct{})}
	cfg := oauthRegressionConfig(t, blocking)
	var tokenCalls atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenCalls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "synthetic-review-new-access", "refresh_token": "synthetic-review-new-refresh", "token_type": "Bearer", "expires_in": 3600})
	}))
	defer provider.Close()
	registration := cfg.Providers["personal"]
	registration.AuthorizeURL = "http://review.fixture/authorize"
	registration.TokenURL = "http://review.fixture/token"
	registration.Policy = netpolicy.Policy{PrivateHosts: []string{"review.fixture"}, AllowedMethods: []string{"GET", "POST"}, AllowedPathPrefixes: []string{"/authorize", "/token"}}
	cfg.Providers["personal"] = registration
	var authorized atomic.Int32
	cfg.Authorize = func(ctx context.Context, s contracts.Scope) error {
		err := ids.Authorize(ctx, authority[s.Subject], "tools")
		if err == nil {
			authorized.Add(1)
		}
		return err
	}
	cfg.LabelPrivate = func(ctx context.Context, s contracts.Scope) error {
		_, err := ids.AddLabels(ctx, s, authority[s.Subject].Task, labels.Set{Subjects: []string{s.Subject}})
		return err
	}
	m, err := tools.NewOAuthManager(ctx, pool, "ahp_review_oauth", cfg, netpolicy.WithResolver(oauthRegressionResolver{}), netpolicy.WithDialer(oauthRegressionDialer{strings.TrimPrefix(provider.URL, "http://")}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		clean, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := m.DeleteTenant(clean, scope.Tenant); err != nil {
			t.Error("own Vault cleanup", err)
		}
	})
	seed := func(owner contracts.Scope, suffix string) {
		path := filepath.ToSlash(cfg.SecretPrefix + suffix)
		if _, err := pool.Exec(ctx, "INSERT INTO ahp_review_oauth.oauth_secrets(tenant,subject,connection,path)VALUES($1,$2,'personal',$3)", owner.Tenant, owner.Subject, path); err != nil {
			t.Fatal(err)
		}
		version, err := vault.WriteCAS(ctx, path, map[string]any{"access_token": "synthetic-review-access", "refresh_token": "synthetic-review-refresh"}, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = pool.Exec(ctx, "INSERT INTO ahp_review_oauth.oauth_connections(tenant,subject,connection,status,secret_path,vault_version,expires_at)VALUES($1,$2,'personal','active',$3,$4,now()+interval '10 seconds')", owner.Tenant, owner.Subject, path, version); err != nil {
			t.Fatal(err)
		}
	}
	seed(scope, "/fixture-tokens")
	if mode == "different-owner" {
		seed(waiterScope, "/other-tokens")
	}

	ownerCtx, cancelOwner := context.WithTimeout(ctx, 3*time.Second)
	defer cancelOwner()
	observerCfg := pool.Config()
	observerCfg.ConnConfig.RuntimeParams["application_name"] = "ahp-observer"
	observer, err := pgxpool.NewWithConfig(ctx, observerCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	ownerDone := make(chan error, 1)
	go func() { _, err := m.Resolve(ownerCtx, scope, "personal"); ownerDone <- err }()
	select {
	case <-blocking.entered:
	case <-ctx.Done():
		t.Fatal("owner did not reach Vault read")
	}
	if mode == "cancel-holder" || mode == "revoked-before-post" {
		if mode == "cancel-holder" {
			cancelOwner()
		} else {
			if err := ids.RevokeSubject(ctx, scope.Tenant, scope.Subject); err != nil {
				t.Fatal(err)
			}
			close(blocking.resume)
		}
		if err := <-ownerDone; err == nil {
			t.Fatal("cancelled/revoked owner returned credential")
		}
		var status string
		if err := observer.QueryRow(ctx, "SELECT status FROM ahp_review_oauth.oauth_connections").Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status != "active" || tokenCalls.Load() != 0 {
			t.Fatalf("local failure spent refresh: %s POST=%d", status, tokenCalls.Load())
		}
		if mode == "cancel-holder" {
			if _, err = m.Resolve(ctx, scope, "personal"); err != nil {
				t.Fatal("retry after holder cancellation", err)
			}
			if tokenCalls.Load() != 1 {
				t.Fatal("retry refresh count")
			}
		}
		assertOAuthLocksReleased(t, observer)
		return
	}
	waiterCtx, cancelWait := context.WithTimeout(ctx, 5*time.Second)
	defer cancelWait()
	waiterDone := make(chan error, 24)
	contenders := 1
	if mode == "storm" {
		contenders = 20
	}
	for i := 0; i < contenders; i++ {
		go func() { _, err := m.Resolve(waiterCtx, waiterScope, "personal"); waiterDone <- err }()
	}
	deadline := time.Now().Add(500 * time.Millisecond)
	for authorized.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if authorized.Load() < 2 {
		close(blocking.resume)
		t.Fatal("Identity authorization blocked behind OAuth pool lease")
	}
	if max == 2 && mode != "different-owner" {
		var waiting int
		for time.Now().Before(deadline) {
			if err = observer.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory'").Scan(&waiting); err != nil {
				t.Fatal(err)
			}
			if waiting > 0 {
				break
			}
			time.Sleep(time.Millisecond)
		}
		if waiting == 0 {
			close(blocking.resume)
			t.Fatal("owner-lock contention not arranged")
		}
	}
	var sessions, allBackends, sharedBackends int
	var nonClient string
	// A bare pg_stat_activity count charges every row attached to the database to
	// this assertion, including cluster backends PostgreSQL attaches transiently
	// under load (they carry no session user and no client backend type). The
	// resource under test is a session, so it is counted as a client backend.
	if err = observer.QueryRow(ctx, admissionCountSQL).Scan(&sessions, &allBackends, &sharedBackends, &nonClient); err != nil {
		t.Fatal(err)
	}
	// Shared pool + at most MaxConns detached locks + the independent observer.
	if sessions > int(2*max+1) {
		oauthAdmissionDetail(t, fmt.Sprintf("pool-%d/%s", max, mode), int(2*max+1), sessions, allBackends, nonClient, pool, observer)
		t.Errorf("unbounded lock admission: %d sessions, max=%d", sessions, 2*max+1)
	}
	// The same budget measured from both sides, so a violation names the culprit
	// instead of only counting rows: the pool itself may never exceed MaxConns,
	// and everything it handed out (in-pool plus detached lock connections) may
	// never exceed MaxConns in-pool plus the MaxConns lock slots.
	if held := pool.Stat().TotalConns(); held > max {
		t.Errorf("shared pool exceeds MaxConns: %d connections, max=%d", held, max)
	}
	if sharedBackends > 2*int(max) {
		t.Errorf("lock admission over budget: %d shared-pool connections (in-pool+detached), max=%d", sharedBackends, 2*int(max))
	}
	t.Logf("admission: client_backends=%d all_backends=%d non_client=%q shared_connections=%d pool_held=%d detached_locks=%d",
		sessions, allBackends, nonClient, sharedBackends, pool.Stat().TotalConns(), sharedBackends-int(pool.Stat().TotalConns()))
	if mode == "cancel-waiter" {
		cancelWait()
		if err := <-waiterDone; err == nil {
			t.Error("cancelled waiter returned a credential")
		}
	}
	close(blocking.resume)
	ownerErr := <-ownerDone
	if ownerErr != nil {
		t.Fatalf("owner failed: %v", ownerErr)
	}
	if mode != "cancel-waiter" {
		for i := 0; i < contenders; i++ {
			if err := <-waiterDone; err != nil {
				t.Fatalf("waiter failed: %v", err)
			}
		}
	}
	for i := 0; i < 2; i++ {
		if _, err = m.Resolve(ctx, scope, "personal"); err != nil {
			t.Fatal("subsequent resolve", err)
		}
	}
	wantCalls := int32(1)
	if mode == "different-owner" {
		wantCalls = 2
	}
	if tokenCalls.Load() != wantCalls {
		t.Fatalf("provider POSTs=%d want=%d", tokenCalls.Load(), wantCalls)
	}
	for subject, claims := range authority {
		owner := scope
		owner.Subject = subject
		grant, err := ids.Get(ctx, owner, claims.Task)
		if err != nil {
			t.Fatal(err)
		}
		if len(grant.Labels.Subjects) != 1 || grant.Labels.Subjects[0] != subject {
			t.Fatalf("private labels not committed for %s", subject)
		}
	}
	assertOAuthLocksReleased(t, observer)
	t.Logf("shared pool=%d mode=%s bounded sessions=%d provider_POSTs=%d", max, mode, sessions, tokenCalls.Load())
}

func assertOAuthLocksReleased(t *testing.T, observer *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var n int
	for ctx.Err() == nil {
		err := observer.QueryRow(ctx, "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a ON l.pid=a.pid WHERE a.datname=current_database() AND l.locktype='advisory'").Scan(&n)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("detached sessions retained %d advisory locks", n)
}
