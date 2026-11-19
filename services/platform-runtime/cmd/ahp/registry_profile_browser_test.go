package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
	"github.com/ahp-platform/ahp/web"
	"github.com/jackc/pgx/v5/pgxpool"
)

type browserProfileExchange struct {
	tokens map[string]string
	tasks  map[string]string
}

func (e browserProfileExchange) Exchange(_ context.Context, h space.HumanPrincipal, _ string) (string, string, error) {
	token := e.tokens[h.Subject]
	if token == "" {
		return "", "", contracts.NewError(contracts.ErrForbidden, "unknown fixture human")
	}
	return token, e.tasks[h.Subject], nil
}

// Real Chromium UI writes -> production AS proxy -> real mTLS Registry and PG.
// Login transport and unrelated initial-data endpoints are explicitly fixtures;
// signed identities, sessions, administrative authority and all settings are real.
func TestProfileBrowserRealPG(t *testing.T) {
	module := os.Getenv("AHP_PLAYWRIGHT_MODULE")
	if module == "" {
		t.Skip("explicit Playwright module required")
	}
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("real PostgreSQL required")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ids, store := identity.New(pool), registry.New(pool)
	for _, m := range []func(context.Context) error{ids.Migrate, store.Migrate} {
		if err = m(ctx); err != nil {
			t.Fatal(err)
		}
	}
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	signer, _ := auth.NewSigner("profile-browser", "key", priv)
	v := auth.NewVerifier("profile-browser", map[string]ed25519.PublicKey{"key": pub})
	tenant := "browser-" + strings.ToLower(contracts.NewID())
	human := map[string]string{}
	exchange := browserProfileExchange{map[string]string{}, map[string]string{}}
	for _, name := range []string{"admin", "alice", "bob"} {
		user := identity.BrowserUser{UserID: "usr-" + contracts.NewID(), Tenant: tenant, Subject: name}
		role := "user"
		if name == "admin" {
			role = "admin"
		}
		if err := ids.SeedBrowserUser(ctx, user, user.UserID, "synthetic-browser-password", []string{role}, []string{"registry"}); err != nil {
			t.Fatal(err)
		}
		sess, err := ids.StartBrowserSession(ctx, user.UserID)
		if err != nil {
			t.Fatal(err)
		}
		jti := contracts.NewID()
		if err = ids.BindBrowserToken(ctx, jti, sess); err != nil {
			t.Fatal(err)
		}
		human[name], err = signer.Sign(auth.Claims{Kind: auth.KindHuman, Audience: "ahp-browser", Tenant: tenant, Subject: name, JTI: jti}, 5*time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		scope := contracts.Scope{Tenant: tenant, Subject: name, Deployment: "space"}
		task := "browser-task-" + contracts.NewID()
		if _, err = ids.CreateRoot(ctx, scope, identity.RootRequest{Task: task, Context: task, Deployment: "space", Capabilities: []string{"registry"}, TTL: 5 * time.Minute}); err != nil {
			t.Fatal(err)
		}
		exchange.tokens[name], err = ids.MintTask(ctx, scope, task, signer, "ahp", 0)
		if err != nil {
			t.Fatal(err)
		}
		exchange.tasks[name] = task
	}
	h, err := registry.NewHTTPHandler(store, v, ids.Authorize)
	if err != nil {
		t.Fatal(err)
	}
	if err = attachRegistryManagement(h, v, ids, []string{"space"}, "ahp-browser"); err != nil {
		t.Fatal(err)
	}
	ca := newMountCA(t)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	owner := httptest.NewUnstartedServer(h)
	owner.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	owner.StartTLS()
	defer owner.Close()
	tr := owner.Client().Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{ca.clientCert(t, auth.WorkloadID(contracts.Scope{Tenant: tenant, Deployment: "space"}))}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr}
	humanAuth, err := space.HumanAuthMiddleware(space.HumanAuthConfig{Issuer: "profile-browser", Audience: "ahp-browser", Keys: map[string]ed25519.PublicKey{"key": pub}})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, spec := range []space.PlaneSpec{space.RegistryPlane, space.ProfilePlane} {
		p, err := space.NewPlaneProxy(space.PlaneProxyConfig{Spec: spec, Origin: owner.URL, Client: client})
		if err != nil {
			t.Fatal(err)
		}
		mux.Handle(spec.RoutePrefix, space.ExchangeMiddleware(exchange, humanAuth)(p))
	}
	mux.HandleFunc("/u-ahp/v1/auth/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/login") {
			var in struct {
				Username string `json:"username"`
				Password string `json:"password"`
			}
			_ = json.NewDecoder(r.Body).Decode(&in)
			if human[in.Username] == "" || in.Password != "fixture" {
				w.WriteHeader(403)
				return
			}
			http.SetCookie(w, &http.Cookie{Name: "fixture-user", Value: in.Username, Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode})
			transport.WriteJSON(w, 200, map[string]string{"subject": in.Username})
			return
		}
		if strings.HasSuffix(r.URL.Path, "/logout") {
			http.SetCookie(w, &http.Cookie{Name: "fixture-user", Path: "/", MaxAge: -1, HttpOnly: true})
			transport.WriteJSON(w, 200, map[string]bool{"ok": true})
			return
		}
		cookie, err := r.Cookie("fixture-user")
		if err != nil || human[cookie.Value] == "" {
			w.WriteHeader(401)
			return
		}
		transport.WriteJSON(w, 200, map[string]any{"access_token": human[cookie.Value], "subject": cookie.Value, "tenant": tenant, "expires_in": 300})
	})
	for _, path := range []string{"/v1/space/ListChats", "/u-ahp/v1/memory/Search", "/u-ahp/v1/triggers/List"} {
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			transport.WriteJSON(w, 200, map[string]any{"threads": []any{}, "hits": []any{}, "triggers": []any{}})
		})
	}
	t.Setenv(web.UIAIFactoryEnv, "1")
	mux.Handle("/", web.Handler())
	ui := httptest.NewServer(mux)
	defer ui.Close()
	command := exec.Command("node", "testdata/profile-browser.mjs", ui.URL, module, os.Getenv("AHP_BROWSER_EVIDENCE_DIR"))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("browser operations: %v\n%s", err, output)
	}
	t.Log(string(output))
	// Independent persistence check after reload/browser completion.
	p, err := store.GetProfile(ctx, contracts.Scope{Tenant: tenant, Subject: "alice", Deployment: "space"})
	if err != nil || p.Revision != 2 {
		t.Fatalf("persisted profile revision=%d err=%v", p.Revision, err)
	}
}
