package tools

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/internal/model/secretstore"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/netpolicy"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrAuthRequired is safe to return to agents: it contains no consent URL or code.
var ErrAuthRequired = contracts.NewError("AUTH_REQUIRED", "oauth: owner must connect in the human portal")

// OAuthSecrets is the Vault KV-v2 boundary. Neither PostgreSQL nor clients receive its data.
type OAuthSecrets interface {
	ReadVersion(context.Context, string) (map[string]any, int, error)
	WriteCAS(context.Context, string, map[string]any, int) (int, error)
	DeleteMetadata(context.Context, string) error
}

// OAuthConfig registers personal connectors and trusted task authority callbacks.
type OAuthConfig struct {
	Providers    map[string]OAuthProvider
	Secrets      OAuthSecrets
	SecretPrefix string
	// Authorize rechecks the live grant before refreshing or returning credentials.
	Authorize func(context.Context, contracts.Scope) error
	// LabelPrivate must durably label the task before personal data can be read.
	LabelPrivate func(context.Context, contracts.Scope) error
}

// OAuthManager owns durable personal authorization, token rotation and revocation.
type OAuthManager struct {
	pool      *pgxpool.Pool
	schema    string
	cfg       OAuthConfig
	providers map[string]*oauthProvider
	// Session-lock connections have separate ownership from the shared SQL pool.
	// Admission is bounded before borrowing a connection; waiting callers hold none.
	lockSlots chan struct{}
}

// NewOAuthManager validates registrations and creates only service-owned metadata tables.
func NewOAuthManager(ctx context.Context, pool *pgxpool.Pool, schema string, cfg OAuthConfig, opts ...netpolicy.Option) (*OAuthManager, error) {
	if pool == nil || !schemaRe.MatchString(schema) || cfg.Secrets == nil || cfg.Authorize == nil || cfg.LabelPrivate == nil || len(cfg.Providers) == 0 {
		return nil, oauthInvalid("durable store, Vault, authority and private-label sink are required")
	}
	if cfg.SecretPrefix == "" || !strings.Contains(cfg.SecretPrefix, "/data/") || strings.ContainsAny(cfg.SecretPrefix, "?#\\%") || strings.HasPrefix(cfg.SecretPrefix, "/") {
		return nil, oauthInvalid("invalid Vault prefix")
	}
	for _, part := range strings.Split(cfg.SecretPrefix, "/") {
		if part == "" || part == "." || part == ".." {
			return nil, oauthInvalid("invalid Vault prefix")
		}
	}
	m := &OAuthManager{pool: pool, schema: schema, cfg: cfg, providers: map[string]*oauthProvider{}, lockSlots: make(chan struct{}, pool.Config().MaxConns)}
	for name, p := range cfg.Providers {
		if name == "" || len(name) > 128 {
			return nil, oauthInvalid("invalid connection")
		}
		provider, err := newOAuthProvider(p, opts...)
		if err != nil {
			return nil, err
		}
		m.providers[name] = provider
	}
	ddl := []string{
		`CREATE SCHEMA IF NOT EXISTS ` + schema,
		`CREATE TABLE IF NOT EXISTS ` + schema + `.oauth_subjects(tenant text NOT NULL, subject text NOT NULL, PRIMARY KEY(tenant,subject))`,
		`CREATE TABLE IF NOT EXISTS ` + schema + `.oauth_tenants(tenant text PRIMARY KEY, fenced_at timestamptz NOT NULL DEFAULT now())`,
		`CREATE TABLE IF NOT EXISTS ` + schema + `.oauth_connections(tenant text NOT NULL,subject text NOT NULL,connection text NOT NULL,status text NOT NULL,secret_path text NOT NULL DEFAULT '',vault_version integer NOT NULL DEFAULT 0,expires_at timestamptz NOT NULL DEFAULT now(),revision bigint NOT NULL DEFAULT 0,PRIMARY KEY(tenant,subject,connection))`,
		`CREATE TABLE IF NOT EXISTS ` + schema + `.oauth_requests(id text PRIMARY KEY,tenant text NOT NULL,subject text NOT NULL,connection text NOT NULL,status text NOT NULL,state_hash text UNIQUE,nonce_hash text,secret_path text NOT NULL DEFAULT '',provider_hash text NOT NULL DEFAULT '',expires_at timestamptz NOT NULL DEFAULT now()+interval '15 minutes',created_at timestamptz NOT NULL DEFAULT now())`,
		`CREATE INDEX IF NOT EXISTS oauth_requests_owner ON ` + schema + `.oauth_requests(tenant,subject,connection)`,
		// The durable inventory is inserted BEFORE a Vault mutation. A crash can leave
		// a harmless absent path, never a secret deletion cannot discover.
		`CREATE TABLE IF NOT EXISTS ` + schema + `.oauth_secrets(tenant text NOT NULL,subject text NOT NULL,connection text NOT NULL,path text PRIMARY KEY)`,
	}
	for _, q := range ddl {
		if _, err := pool.Exec(ctx, q); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func oauthHash(v string) string { s := sha256.Sum256([]byte(v)); return hex.EncodeToString(s[:]) }
func oauthRandom() string {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("oauth: random source unavailable")
	}
	return base64.RawURLEncoding.EncodeToString(b[:])
}
func oauthActor(a auth.Claims) (contracts.Scope, error) {
	if a.Kind != auth.KindHuman {
		return contracts.Scope{}, oauthDenied("human session required")
	}
	s := contracts.Scope{Tenant: a.Tenant, Subject: a.Subject, Deployment: "oauth-human"}
	return s, s.Validate()
}
func (m *OAuthManager) has(connection string) bool { _, ok := m.providers[connection]; return ok }

// lock uses an exclusively owned connection for PostgreSQL session locks.
// Identity callbacks use m.pool, so retaining any pool lease here (including a
// waiting lock contender) can deadlock even with a healthy database. Hijack keeps
// the pool's connection/auth hooks but relinquishes its lease before any lock or
// callback. Every outcome closes the detached connection, releasing session locks.
func (m *OAuthManager) lock(ctx context.Context, tenant, subject, owner string, exclusive bool) (*pgx.Conn, func(), error) {
	select {
	case m.lockSlots <- struct{}{}:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
	pooled, err := m.pool.Acquire(ctx)
	if err != nil {
		<-m.lockSlots
		return nil, nil, err
	}
	c := pooled.Hijack()
	release := func() {
		clean, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = c.Close(clean)
		<-m.lockSlots
	}
	lockFn := "pg_advisory_lock_shared"
	if exclusive {
		lockFn = "pg_advisory_lock"
	}
	tenantKey := m.schema + ":oauth:tenant:" + tenant
	if _, err = c.Exec(ctx, "SELECT "+lockFn+"(hashtextextended($1,0))", tenantKey); err != nil {
		release()
		return nil, nil, err
	}
	if owner != "" {
		if _, err = c.Exec(ctx, "SELECT pg_advisory_lock(hashtextextended($1,0))", m.schema+":oauth:owner:"+tenant+":"+owner); err != nil {
			release()
			return nil, nil, err
		}
	}
	if !exclusive {
		var fenced bool
		err = c.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM "+m.schema+".oauth_tenants WHERE tenant=$1) OR EXISTS(SELECT 1 FROM "+m.schema+".oauth_subjects WHERE tenant=$1 AND subject=$2)", tenant, subject).Scan(&fenced)
		if err != nil || fenced {
			release()
			if err == nil {
				err = oauthDenied("tenant unavailable")
			}
			return nil, nil, err
		}
	}
	return c, release, nil
}

func ownerKey(s contracts.Scope, connection string) string {
	return oauthHash(s.Subject + "\x00" + connection)
}

// Require registers auth-required without ever returning a consent link to a task.
func (m *OAuthManager) Require(ctx context.Context, s contracts.Scope, connection string) error {
	if !m.has(connection) {
		return nil
	}
	if e := s.Validate(); e != nil {
		return e
	}
	if e := m.cfg.Authorize(ctx, s); e != nil {
		return e
	}
	c, release, e := m.lock(ctx, s.Tenant, s.Subject, ownerKey(s, connection), false)
	if e != nil {
		return e
	}
	defer release()
	var status string
	e = c.QueryRow(ctx, "SELECT status FROM "+m.schema+".oauth_connections WHERE tenant=$1 AND subject=$2 AND connection=$3", s.Tenant, s.Subject, connection).Scan(&status)
	if e != nil && !errors.Is(e, pgx.ErrNoRows) {
		return e
	}
	if status == "active" {
		return nil
	}
	_, e = c.Exec(ctx, "INSERT INTO "+m.schema+".oauth_connections(tenant,subject,connection,status) VALUES($1,$2,$3,'auth_required') ON CONFLICT DO NOTHING", s.Tenant, s.Subject, connection)
	if e != nil {
		return e
	}
	_, e = c.Exec(ctx, "INSERT INTO "+m.schema+".oauth_requests(id,tenant,subject,connection,status) SELECT $4,$1,$2,$3,'pending' WHERE NOT EXISTS(SELECT 1 FROM "+m.schema+".oauth_requests WHERE tenant=$1 AND subject=$2 AND connection=$3 AND status IN ('pending','authorizing') AND expires_at>now())", s.Tenant, s.Subject, connection, contracts.NewID())
	if e != nil {
		return e
	}
	return ErrAuthRequired
}

// ConsentActive reports whether the owner's personal grant for connection is
// active right now. It is read-only by construction: no consent request is
// inserted, no token is refreshed and the external provider is never contacted,
// because reading a durable result must never resurrect a credential
// (TOOLS-RESULT contract, 2026-11-09). A connection this manager does not
// govern reports true: there is no personal grant to check.
func (m *OAuthManager) ConsentActive(ctx context.Context, s contracts.Scope, connection string) (bool, error) {
	if !m.has(connection) {
		return true, nil
	}
	if e := s.Validate(); e != nil {
		return false, e
	}
	c, release, e := m.lock(ctx, s.Tenant, s.Subject, ownerKey(s, connection), false)
	if e != nil {
		return false, e
	}
	defer release()
	var status string
	e = c.QueryRow(ctx, "SELECT status FROM "+m.schema+".oauth_connections WHERE tenant=$1 AND subject=$2 AND connection=$3", s.Tenant, s.Subject, connection).Scan(&status)
	if errors.Is(e, pgx.ErrNoRows) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	return status == "active", nil
}

// OAuthConnection is the non-secret personal connection view shown to its owner.
type OAuthConnection struct {
	Connection string     `json:"connection"`
	Status     string     `json:"status"`
	ExpiresAt  *time.Time `json:"expires_at,omitempty"`
}

// Connections lists only the verified human's metadata, including registered disconnected providers.
func (m *OAuthManager) Connections(ctx context.Context, a auth.Claims) ([]OAuthConnection, error) {
	s, e := oauthActor(a)
	if e != nil {
		return nil, e
	}
	c, release, e := m.lock(ctx, s.Tenant, s.Subject, "", false)
	if e != nil {
		return nil, e
	}
	defer release()
	out := make([]OAuthConnection, 0, len(m.providers))
	for name := range m.providers {
		v := OAuthConnection{Connection: name, Status: "disconnected"}
		var expires time.Time
		e = c.QueryRow(ctx, "SELECT status,expires_at FROM "+m.schema+".oauth_connections WHERE tenant=$1 AND subject=$2 AND connection=$3", s.Tenant, s.Subject, name).Scan(&v.Status, &expires)
		if e != nil && !errors.Is(e, pgx.ErrNoRows) {
			return nil, e
		}
		if v.Status == "active" {
			v.ExpiresAt = &expires
		}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Connection < out[j].Connection })
	return out, nil
}

// OAuthConsent contains only the browser redirect and its cookie binding, never tokens or PKCE verifier.
type OAuthConsent struct {
	URL          string `json:"authorize_url"`
	State        string `json:"-"`
	Nonce        string `json:"-"`
	SecureCookie bool   `json:"-"`
}

// Begin accepts consent only from the verified owner; previous attempts become unusable.
func (m *OAuthManager) Begin(ctx context.Context, a auth.Claims, connection string) (OAuthConsent, error) {
	s, e := oauthActor(a)
	if e != nil {
		return OAuthConsent{}, e
	}
	p, ok := m.providers[connection]
	if !ok {
		return OAuthConsent{}, oauthDenied("connection unavailable")
	}
	c, release, e := m.lock(ctx, s.Tenant, s.Subject, ownerKey(s, connection), false)
	if e != nil {
		return OAuthConsent{}, e
	}
	defer release()
	state, nonce, verifier := oauthRandom(), oauthRandom(), oauthRandom()
	id := contracts.NewID()
	path := m.cfg.SecretPrefix + "/" + oauthHash(s.Tenant) + "/" + id + "-pkce"
	tx, e := c.Begin(ctx)
	if e != nil {
		return OAuthConsent{}, e
	}
	defer tx.Rollback(ctx)
	_, e = tx.Exec(ctx, "UPDATE "+m.schema+".oauth_requests SET status='superseded' WHERE tenant=$1 AND subject=$2 AND connection=$3 AND status IN ('pending','authorizing','exchanging')", s.Tenant, s.Subject, connection)
	if e != nil {
		return OAuthConsent{}, e
	}
	_, e = tx.Exec(ctx, "INSERT INTO "+m.schema+".oauth_secrets(tenant,subject,connection,path) VALUES($1,$2,$3,$4)", s.Tenant, s.Subject, connection, path)
	if e != nil {
		return OAuthConsent{}, e
	}
	digest, _ := contracts.Digest(p.config)
	_, e = tx.Exec(ctx, "INSERT INTO "+m.schema+".oauth_requests(id,tenant,subject,connection,status,state_hash,nonce_hash,secret_path,provider_hash) VALUES($1,$2,$3,$4,'preparing',$5,$6,$7,$8)", id, s.Tenant, s.Subject, connection, oauthHash(state), oauthHash(nonce), path, digest)
	if e != nil {
		return OAuthConsent{}, e
	}
	if e = tx.Commit(ctx); e != nil {
		return OAuthConsent{}, e
	}
	if _, e = m.cfg.Secrets.WriteCAS(ctx, path, map[string]any{"verifier": verifier}, 0); e != nil {
		return OAuthConsent{}, e
	}
	_, e = c.Exec(ctx, "UPDATE "+m.schema+".oauth_requests SET status='authorizing' WHERE id=$1 AND status='preparing'", id)
	if e != nil {
		return OAuthConsent{}, e
	}
	u, _ := url.Parse(p.config.AuthorizeURL)
	q := u.Query()
	q.Set("client_id", p.config.ClientID)
	q.Set("redirect_uri", p.config.RedirectURI)
	q.Set("response_type", "code")
	q.Set("scope", strings.Join(p.config.Scopes, " "))
	q.Set("state", state)
	challenge := sha256.Sum256([]byte(verifier))
	q.Set("code_challenge", base64.RawURLEncoding.EncodeToString(challenge[:]))
	q.Set("code_challenge_method", "S256")
	u.RawQuery = q.Encode()
	return OAuthConsent{URL: u.String(), State: state, Nonce: nonce, SecureCookie: strings.HasPrefix(p.config.RedirectURI, "https:")}, nil
}

// OAuthCallback is the code response and same-browser binding; it is never logged or stored.
type OAuthCallback struct {
	State string `json:"state"`
	Code  string `json:"code"`
	Error string `json:"error,omitempty"`
	Nonce string `json:"-"`
}

// Complete durably consumes state BEFORE the token POST. A lost response is never retried.
func (m *OAuthManager) Complete(ctx context.Context, a auth.Claims, in OAuthCallback) error {
	s, e := oauthActor(a)
	if e != nil {
		return e
	}
	if len(in.State) != 43 || len(in.Nonce) != 43 || len(in.Code) > 8192 || len(in.Error) > 256 {
		return oauthDenied("invalid callback")
	}
	// Owner filtering happens before acquiring a connector lock; no foreign request can be consumed.
	var connection string
	e = m.pool.QueryRow(ctx, "SELECT connection FROM "+m.schema+".oauth_requests WHERE tenant=$1 AND subject=$2 AND state_hash=$3", s.Tenant, s.Subject, oauthHash(in.State)).Scan(&connection)
	if e != nil {
		return oauthDenied("callback unavailable")
	}
	p, ok := m.providers[connection]
	if !ok {
		return oauthDenied("connection unavailable")
	}
	c, release, e := m.lock(ctx, s.Tenant, s.Subject, ownerKey(s, connection), false)
	if e != nil {
		return e
	}
	defer release()
	var id, path string
	digest, _ := contracts.Digest(p.config)
	e = c.QueryRow(ctx, "UPDATE "+m.schema+".oauth_requests SET status='exchanging' WHERE tenant=$1 AND subject=$2 AND state_hash=$3 AND nonce_hash=$4 AND status='authorizing' AND expires_at>now() AND provider_hash=$5 RETURNING id,secret_path", s.Tenant, s.Subject, oauthHash(in.State), oauthHash(in.Nonce), digest).Scan(&id, &path)
	if e != nil {
		return oauthDenied("callback expired or consumed")
	}
	if in.Error != "" || in.Code == "" {
		_, e = c.Exec(ctx, "UPDATE "+m.schema+".oauth_requests SET status='denied' WHERE id=$1", id)
		return e
	}
	data, _, e := m.cfg.Secrets.ReadVersion(ctx, path)
	if e != nil {
		return e
	}
	verifier, _ := data["verifier"].(string)
	if verifier == "" {
		return oauthDenied("PKCE unavailable")
	}
	secret, e := m.clientSecret(ctx, p)
	if e != nil {
		return e
	}
	token, e := p.request(ctx, p.config.TokenURL, url.Values{"grant_type": {"authorization_code"}, "code": {in.Code}, "redirect_uri": {p.config.RedirectURI}, "code_verifier": {verifier}}, secret, true)
	if e != nil {
		return e
	}
	tokenPath := m.cfg.SecretPrefix + "/" + oauthHash(s.Tenant) + "/" + id + "-tokens"
	_, e = c.Exec(ctx, "INSERT INTO "+m.schema+".oauth_secrets(tenant,subject,connection,path) VALUES($1,$2,$3,$4)", s.Tenant, s.Subject, connection, tokenPath)
	if e != nil {
		return e
	}
	expires := time.Now().Add(time.Duration(token.ExpiresIn) * time.Second)
	version, e := m.cfg.Secrets.WriteCAS(ctx, tokenPath, tokenData(token, expires), 0)
	if e != nil {
		return e
	}
	tx, e := c.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	_, e = tx.Exec(ctx, "INSERT INTO "+m.schema+".oauth_connections(tenant,subject,connection,status,secret_path,vault_version,expires_at,revision) VALUES($1,$2,$3,'active',$4,$5,$6,1) ON CONFLICT(tenant,subject,connection) DO UPDATE SET status='active',secret_path=$4,vault_version=$5,expires_at=$6,revision="+m.schema+".oauth_connections.revision+1", s.Tenant, s.Subject, connection, tokenPath, version, expires)
	if e != nil {
		return e
	}
	_, e = tx.Exec(ctx, "UPDATE "+m.schema+".oauth_requests SET status='complete' WHERE id=$1", id)
	if e != nil {
		return e
	}
	if e = tx.Commit(ctx); e != nil {
		return e
	}
	// Cleanup failure does not undo the durable grant; inventory keeps the path retryable.
	_ = m.cfg.Secrets.DeleteMetadata(ctx, path)
	return nil
}

func (m *OAuthManager) clientSecret(ctx context.Context, p *oauthProvider) (string, error) {
	if p.config.ClientSecretPath == "" {
		return "", nil
	}
	d, _, e := m.cfg.Secrets.ReadVersion(ctx, p.config.ClientSecretPath)
	if e != nil {
		return "", e
	}
	s, _ := d["client_secret"].(string)
	if s == "" {
		return "", oauthDenied("client credential unavailable")
	}
	return s, nil
}
func tokenData(t oauthTokens, expiry time.Time) map[string]any {
	return map[string]any{"access_token": t.AccessToken, "refresh_token": t.RefreshToken, "expires_at": expiry.UTC().Format(time.RFC3339Nano)}
}

// Resolve serializes rotation across processes and uses Vault CAS. Ambiguous refresh
// remains refresh_unknown across restart; reconnect is required, never another POST.
func (m *OAuthManager) Resolve(ctx context.Context, s contracts.Scope, connection string) (Credential, error) {
	if e := s.Validate(); e != nil {
		return Credential{}, e
	}
	p, ok := m.providers[connection]
	if !ok {
		return Credential{}, oauthDenied("connection unavailable")
	}
	if e := m.cfg.Authorize(ctx, s); e != nil {
		return Credential{}, e
	}
	c, release, e := m.lock(ctx, s.Tenant, s.Subject, ownerKey(s, connection), false)
	if e != nil {
		return Credential{}, e
	}
	defer release()
	var status, path string
	var version int
	var revision int64
	var expires time.Time
	e = c.QueryRow(ctx, "SELECT status,secret_path,vault_version,expires_at,revision FROM "+m.schema+".oauth_connections WHERE tenant=$1 AND subject=$2 AND connection=$3", s.Tenant, s.Subject, connection).Scan(&status, &path, &version, &expires, &revision)
	if e != nil || status != "active" {
		return Credential{}, ErrAuthRequired
	}
	data, actual, e := m.cfg.Secrets.ReadVersion(ctx, path)
	if e != nil {
		return Credential{}, e
	}
	if actual != version {
		return Credential{}, oauthDenied("credential version conflict")
	}
	access, _ := data["access_token"].(string)
	if time.Until(expires) <= 30*time.Second {
		refresh, _ := data["refresh_token"].(string)
		if refresh == "" {
			if _, err := c.Exec(ctx, "UPDATE "+m.schema+".oauth_connections SET status='auth_required' WHERE tenant=$1 AND subject=$2 AND connection=$3", s.Tenant, s.Subject, connection); err != nil {
				return Credential{}, err
			}
			return Credential{}, ErrAuthRequired
		}
		if e = m.cfg.Authorize(ctx, s); e != nil {
			return Credential{}, e
		}
		secret, e := m.clientSecret(ctx, p)
		if e != nil {
			return Credential{}, e
		}
		// All fallible local/Identity preparation precedes UNKNOWN. Commit the
		// marker immediately before the external POST; a process crash from here
		// is still ambiguous and must never cause automatic token replay.
		if err := ctx.Err(); err != nil {
			return Credential{}, err
		}
		_, e = c.Exec(ctx, "UPDATE "+m.schema+".oauth_connections SET status='refresh_unknown' WHERE tenant=$1 AND subject=$2 AND connection=$3 AND revision=$4", s.Tenant, s.Subject, connection, revision)
		if e != nil {
			return Credential{}, e
		}
		token, e := p.request(ctx, p.config.TokenURL, url.Values{"grant_type": {"refresh_token"}, "refresh_token": {refresh}}, secret, true)
		if e != nil {
			return Credential{}, e
		}
		if token.RefreshToken == "" {
			token.RefreshToken = refresh
		}
		expires = time.Now().Add(time.Duration(token.ExpiresIn) * time.Second)
		next, e := m.cfg.Secrets.WriteCAS(ctx, path, tokenData(token, expires), version)
		if e != nil {
			return Credential{}, e
		}
		tag, e := c.Exec(ctx, "UPDATE "+m.schema+".oauth_connections SET status='active',vault_version=$4,expires_at=$5,revision=revision+1 WHERE tenant=$1 AND subject=$2 AND connection=$3 AND revision=$6 AND status='refresh_unknown'", s.Tenant, s.Subject, connection, next, expires, revision)
		if e != nil {
			return Credential{}, e
		}
		if tag.RowsAffected() != 1 {
			return Credential{}, oauthDenied("credential changed during refresh")
		}
		access = token.AccessToken
	}
	if access == "" {
		return Credential{}, ErrAuthRequired
	}
	if e = m.cfg.Authorize(ctx, s); e != nil {
		return Credential{}, e
	}
	if e = m.cfg.LabelPrivate(ctx, s); e != nil {
		return Credential{}, e
	}
	return Credential{Bearer: access}, nil
}

// Revoke disables local use BEFORE provider I/O; repeated calls retry remote revocation and Vault erasure.
func (m *OAuthManager) Revoke(ctx context.Context, a auth.Claims, connection string) error {
	s, e := oauthActor(a)
	if e != nil {
		return e
	}
	p, ok := m.providers[connection]
	if !ok {
		return oauthDenied("connection unavailable")
	}
	c, release, e := m.lock(ctx, s.Tenant, s.Subject, ownerKey(s, connection), false)
	if e != nil {
		return e
	}
	defer release()
	_, e = c.Exec(ctx, "INSERT INTO "+m.schema+".oauth_connections(tenant,subject,connection,status) VALUES($1,$2,$3,'revoked') ON CONFLICT(tenant,subject,connection) DO UPDATE SET status='revoked',revision="+m.schema+".oauth_connections.revision+1", s.Tenant, s.Subject, connection)
	if e != nil {
		return e
	}
	_, e = c.Exec(ctx, "UPDATE "+m.schema+".oauth_requests SET status='revoked' WHERE tenant=$1 AND subject=$2 AND connection=$3", s.Tenant, s.Subject, connection)
	if e != nil {
		return e
	}
	// Revoke every historical token path too: reconnect can leave prior grants alive.
	rows, e := c.Query(ctx, "SELECT path FROM "+m.schema+".oauth_secrets WHERE tenant=$1 AND subject=$2 AND connection=$3", s.Tenant, s.Subject, connection)
	if e != nil {
		return e
	}
	paths := []string{}
	for rows.Next() {
		var p string
		if e = rows.Scan(&p); e != nil {
			rows.Close()
			return e
		}
		paths = append(paths, p)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, path := range paths {
		if p.config.RevokeURL != "" && strings.HasSuffix(path, "-tokens") {
			data, _, readErr := m.cfg.Secrets.ReadVersion(ctx, path)
			if readErr == nil {
				secret, e := m.clientSecret(ctx, p)
				if e != nil {
					return e
				}
				for _, kind := range []string{"refresh_token", "access_token"} {
					token, _ := data[kind].(string)
					if token != "" {
						if _, e = p.request(ctx, p.config.RevokeURL, url.Values{"token": {token}, "token_type_hint": {kind}}, secret, false); e != nil {
							return e
						}
					}
				}
			} else if !errors.Is(readErr, secretstore.ErrSecretNotFound) {
				return readErr
			}
		}
		if e = m.cfg.Secrets.DeleteMetadata(ctx, path); e != nil {
			return e
		}
		if _, e = c.Exec(ctx, "DELETE FROM "+m.schema+".oauth_secrets WHERE path=$1", path); e != nil {
			return e
		}
	}
	return nil
}

// FenceTenant permanently rejects late callbacks, refreshes and reconnections.
func (m *OAuthManager) FenceTenant(ctx context.Context, tenant string) error {
	c, release, e := m.lock(ctx, tenant, "", "", true)
	if e != nil {
		return e
	}
	defer release()
	_, e = c.Exec(ctx, "INSERT INTO "+m.schema+".oauth_tenants(tenant) VALUES($1) ON CONFLICT DO NOTHING", tenant)
	return e
}

// DeleteTenant erases all Vault versions before metadata; the durable tombstone remains.
func (m *OAuthManager) DeleteTenant(ctx context.Context, tenant string) error {
	if e := m.FenceTenant(ctx, tenant); e != nil {
		return e
	}
	c, release, e := m.lock(ctx, tenant, "", "", true)
	if e != nil {
		return e
	}
	defer release()
	rows, e := c.Query(ctx, "SELECT path FROM "+m.schema+".oauth_secrets WHERE tenant=$1", tenant)
	if e != nil {
		return e
	}
	paths := []string{}
	for rows.Next() {
		var p string
		if e = rows.Scan(&p); e != nil {
			rows.Close()
			return e
		}
		paths = append(paths, p)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, p := range paths {
		if e = m.cfg.Secrets.DeleteMetadata(ctx, p); e != nil {
			return e
		}
	}
	tx, e := c.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	for _, table := range []string{"oauth_secrets", "oauth_requests", "oauth_connections"} {
		if _, e = tx.Exec(ctx, "DELETE FROM "+m.schema+"."+table+" WHERE tenant=$1", tenant); e != nil {
			return e
		}
	}
	return tx.Commit(ctx)
}

// String deliberately avoids configuration and Vault paths in diagnostics.
func (m *OAuthManager) String() string {
	return fmt.Sprintf("OAuthManager{%d registered providers}", len(m.providers))
}
