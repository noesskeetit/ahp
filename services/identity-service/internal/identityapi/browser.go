package identityapi

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// The browser-facing surface: the Agent Space UI's login, session lifecycle and
// "who am I" endpoints.
//
// Shape of the trust, restated where the code lives:
//
//   - The browser is untrusted. It receives no signing key and no platform
//     secret; it gets an opaque session id in an HttpOnly cookie and a short
//     lived human JWT it keeps in memory.
//   - Subject and tenant come from the authenticated account row. The login
//     body has exactly two fields, decoded strictly, so a caller cannot ask to
//     be somebody else or pick a tenant. There is no endpoint that mints a
//     token for a caller-named subject, and no user directory to pick from.
//   - These routes accept a human JWT and refuse a task token; the agent-facing
//     routes in handlers.go accept a task token bound to an mTLS workload and
//     refuse a human JWT. The two never substitute for each other.
//   - The credential store behind this is the local prototype replacement for
//     the corporate IdP described in the architecture (public entry "login via
//     IdP", JWT staff, IAM ExtAuthz). It is a real argon2id store with real
//     server-side sessions, and it is not a production identity provider.
const (
	// defaultBrowserPrefix is where the UI-facing routes are mounted. The
	// external prefix behind the gateway is the delivery layer's decision; this
	// is the service's own mount.
	defaultBrowserPrefix = "/u-ahp/v1"
	// defaultBrowserCookie is the session cookie name.
	defaultBrowserCookie = "ahp_session"
	// browserSessionHeader must accompany every cookie-bearing state change.
	// SameSite=Strict is the primary CSRF defence; this header is the second,
	// because a cross-site request cannot set a custom header without a
	// preflight the browser will not perform for this simple shape.
	browserSessionHeader = "X-AHP-Session"
	// codeUnauthenticated is the wire code for "you are not signed in", kept
	// distinct from FORBIDDEN so the UI can tell "log in again" from "no
	// permission".
	codeUnauthenticated = "UNAUTHENTICATED"
	// maxBrowserBody bounds a credential POST; credentials are small.
	maxBrowserBody = 8 * 1024
	// defaultBrowserAudience is the audience of a browser access token. It is
	// deliberately NOT the control-plane human audience: signing in to the UI
	// hands out a credential for the UI-facing APIs, not the general human
	// authority that may open a task tree. A service accepts browser tokens by
	// listing this audience explicitly, which is how the Human -> authorized
	// Space -> scoped task transition keeps its edges.
	defaultBrowserAudience = "ahp-web"
)

// BrowserConfig mounts the browser surface. A nil BrowserConfig on Config
// leaves the surface entirely absent, which keeps the accepted control and
// agent planes byte-identical for callers that do not serve a UI.
type BrowserConfig struct {
	// Prefix mounts the UI routes; empty selects defaultBrowserPrefix.
	Prefix string
	// CookieName overrides defaultBrowserCookie.
	CookieName string
	// AccessAudience is the audience browser access tokens are minted and
	// verified with; empty selects defaultBrowserAudience. Downstream services
	// that accept browser credentials (the Space facade) must be configured with
	// this audience and must never accept the control-plane human audience on
	// a browser-reachable path.
	AccessAudience string
	// FeatureFlags are surfaced verbatim on /me/flags for UI gating.
	FeatureFlags map[string]bool
}

func (c BrowserConfig) prefix() string {
	if strings.TrimSpace(c.Prefix) == "" {
		return defaultBrowserPrefix
	}
	return "/" + strings.Trim(c.Prefix, "/")
}

func (c BrowserConfig) cookieName() string {
	if strings.TrimSpace(c.CookieName) == "" {
		return defaultBrowserCookie
	}
	return c.CookieName
}

func (c *BrowserConfig) accessAudience(controlAudience string) string {
	if c != nil {
		if a := strings.TrimSpace(c.AccessAudience); a != "" {
			return a
		}
	}
	if strings.TrimSpace(controlAudience) == "" {
		return defaultBrowserAudience
	}
	return controlAudience + "/web"
}

// mountBrowser registers the UI-facing routes on mux.
func (h *api) mountBrowser(mux *http.ServeMux) {
	p := h.cfg.Browser.prefix()
	mux.HandleFunc("POST "+p+"/auth/login", h.browserLogin)
	mux.HandleFunc("POST "+p+"/auth/token", h.browserToken)
	mux.HandleFunc("POST "+p+"/auth/refresh", h.browserRefresh)
	mux.HandleFunc("POST "+p+"/auth/logout", h.browserLogout)
	mux.HandleFunc("GET "+p+"/auth/config", h.browserAuthConfig)
	mux.Handle("GET "+p+"/me/user", h.requireBrowserPrincipal(http.HandlerFunc(h.browserMeUser)))
	mux.Handle("GET "+p+"/me/flags", h.requireBrowserPrincipal(http.HandlerFunc(h.browserMeFlags)))

	h.mountControl(mux)
}

// mountControl registers the service-facing control routes. They travel with
// the browser surface, and since AHP_ENABLE_CONTROL_EXCHANGE they can also
// stand alone on a deployment that serves no UI at all: PDF rev 3.2 requirement
// 6 makes the Human -> authorized Space -> scoped task transition something
// every front can perform, not a side effect of serving a browser. Mounting it
// stays opt-in so every accepted plane is byte-identical by default.
func (h *api) mountControl(mux *http.ServeMux) {
	// Introspection is for services, not browsers: it is reachable only over a
	// verified workload certificate, and it is how a downstream service turns a
	// stateless human JWT into a revocation-aware decision.
	mux.Handle("POST /v1/identity/control/introspect",
		transport.Workload()(http.HandlerFunc(h.browserIntrospect)))

	// The Human -> authorized Space -> scoped task boundary. A trusted
	// service (its deployment is allowlisted) converts a live browser human
	// session into a scoped task token it presents to State with its OWN
	// workload certificate. No browser or task signing credential is ever
	// handed to a client, and the human token never travels to State.
	mux.Handle("POST /v1/identity/control/exchange",
		transport.Workload()(http.HandlerFunc(h.exchangeScopedTask)))
}

// --- request/response shapes ---------------------------------------------

type browserLoginBody struct {
	Username string `json:"username"`
	Password string `json:"password"`
	// IDToken is the OIDC assertion an external IdP issued. The local profile
	// rejects it; the OIDC profile requires it. Neither profile reads a subject,
	// tenant or deployment from the body.
	IDToken     string `json:"id_token,omitempty"`
	AccessToken string `json:"access_token,omitempty"`
}

type browserPrincipalView struct {
	ID          string    `json:"id"`
	Subject     string    `json:"subject"`
	Tenant      string    `json:"tenant"`
	DisplayName string    `json:"displayName"`
	Email       string    `json:"email,omitempty"`
	ExpiresAt   time.Time `json:"expiresAt"`
}

type browserTokenView struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int64  `json:"expires_in"`
	Subject     string `json:"subject"`
	Tenant      string `json:"tenant"`
}

type browserFlagsView struct {
	Roles        []string        `json:"roles"`
	Permissions  []string        `json:"permissions"`
	FeatureFlags map[string]bool `json:"featureFlags,omitempty"`
	// AuthProfile names how the human authenticated ("local" or "oidc"). It is
	// surfaced so a UI or operator can never mistake the local development
	// credential store for corporate IAM.
	AuthProfile string `json:"authProfile"`
}

// browserAuthConfigView answers the pre-login question "how do I sign in?", so
// the UI can adapt to the OIDC seam or the local profile without guessing. It
// carries no secret; an OIDC discovery document is public by design.
type browserAuthConfigView struct {
	Profile         string `json:"profile"`
	BrowserAudience string `json:"browserAudience"`
	// LocalProfile is true exactly when authentication is this repository's
	// development credential store, never a corporate IdP.
	LocalProfile bool `json:"localProfile"`
}

type browserIntrospectBody struct {
	Token string `json:"token"`
}

type browserIntrospectView struct {
	Active    bool      `json:"active"`
	Subject   string    `json:"subject,omitempty"`
	Tenant    string    `json:"tenant,omitempty"`
	SessionID string    `json:"session_id,omitempty"`
	ExpiresAt time.Time `json:"expires_at,omitempty"`
}

// --- handlers -------------------------------------------------------------

func (h *api) browserLogin(w http.ResponseWriter, r *http.Request) {
	if !browserCSRFOK(r) {
		h.browserError(w, r, http.StatusForbidden, contracts.ErrForbidden, "identityapi: session header missing")
		return
	}
	authn := h.authenticator()
	var body browserLoginBody
	if !h.readStrictJSON(w, r, &body) {
		return
	}
	if body.AccessToken != "" {
		if body.IDToken != "" {
			h.browserError(w, r, http.StatusBadRequest, contracts.ErrInvalid, "provide one access assertion")
			return
		}
		body.IDToken = body.AccessToken
	}
	// Which body shape the profile accepts; subject/tenant/deployment stay
	// unnameable on every profile.
	var in identity.LoginInput
	switch authn.Profile() {
	case "local":
		if body.IDToken != "" {
			h.browserError(w, r, http.StatusBadRequest, contracts.ErrInvalid,
				"identityapi: this deployment authenticates by credential, not an access assertion")
			return
		}
		if strings.TrimSpace(body.Username) == "" || body.Password == "" {
			h.browserError(w, r, http.StatusBadRequest, contracts.ErrInvalid,
				"identityapi: username and password are required")
			return
		}
		in = identity.LoginInput{Username: body.Username, Password: body.Password}
	default: // oidc
		if body.Username != "" || body.Password != "" {
			h.browserError(w, r, http.StatusBadRequest, contracts.ErrInvalid,
				"identityapi: this deployment authenticates by an access assertion, not credential")
			return
		}
		if strings.TrimSpace(body.IDToken) == "" {
			h.browserError(w, r, http.StatusBadRequest, contracts.ErrInvalid,
				"identityapi: access_token is required")
			return
		}
		in = identity.LoginInput{Assertion: body.IDToken}
	}
	user, err := authn.Login(r.Context(), in)
	if err != nil {
		if errors.Is(err, identity.ErrAuthentication) {
			// One answer for "no such account", "wrong credential" and "bad
			// assertion": login cannot enumerate accounts on either profile.
			h.browserError(w, r, http.StatusUnauthorized, codeUnauthenticated,
				"identityapi: invalid credentials")
			return
		}
		transport.WriteError(w, r, err)
		return
	}
	if authn.Profile() != "local" {
		// Mirror a federated identity from its verified subject/tenant so the
		// session, token-binding and revocation machinery applies unchanged.
		user, err = h.cfg.Store.EnsureBrowserUserBySubject(r.Context(), user.Tenant, user.Subject,
			user.DisplayName, user.Email)
		if err != nil {
			transport.WriteError(w, r, err)
			return
		}
	}
	session, err := h.cfg.Store.StartBrowserSession(r.Context(), user.UserID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	h.setSessionCookie(w, r, session.ID)
	transport.WriteJSON(w, http.StatusOK, browserPrincipalView{
		ID: user.UserID, Subject: user.Subject, Tenant: user.Tenant,
		DisplayName: user.DisplayName, Email: user.Email, ExpiresAt: session.AbsoluteAt,
	})
}

func (h *api) browserToken(w http.ResponseWriter, r *http.Request) {
	h.issueBrowserToken(w, r, false)
}

func (h *api) browserRefresh(w http.ResponseWriter, r *http.Request) {
	h.issueBrowserToken(w, r, true)
}

// issueBrowserToken exchanges a live session for a human access token. With
// rotate the session epoch advances first, which retires every token minted
// under the previous epoch; the session's absolute deadline is never moved.
func (h *api) issueBrowserToken(w http.ResponseWriter, r *http.Request, rotate bool) {
	if !browserCSRFOK(r) {
		h.browserError(w, r, http.StatusForbidden, contracts.ErrForbidden, "identityapi: session header missing")
		return
	}
	sid, ok := h.sessionID(r)
	if !ok {
		h.clearSessionCookie(w, r)
		h.browserError(w, r, http.StatusUnauthorized, codeUnauthenticated, "identityapi: no session")
		return
	}
	var (
		session identity.BrowserSession
		err     error
	)
	if rotate {
		session, err = h.cfg.Store.RefreshBrowserSession(r.Context(), sid)
	} else {
		session, err = h.cfg.Store.BrowserSession(r.Context(), sid)
	}
	if err != nil {
		// A dead session is NOT_FOUND in the store and UNAUTHENTICATED here: the
		// UI's only correct reaction is to log in again.
		h.clearSessionCookie(w, r)
		h.browserError(w, r, http.StatusUnauthorized, codeUnauthenticated,
			"identityapi: session is not valid")
		return
	}
	// Each mint gets its own jti. Without it two tokens for the same principal
	// minted in the same second are byte-identical, and revoking one browser's
	// credential would reach into another's.
	jti, err := newTokenID()
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	token, err := h.cfg.Signer.Sign(auth.Claims{
		Subject:  session.Subject,
		Tenant:   session.Tenant,
		Audience: h.cfg.Browser.accessAudience(h.cfg.Audience),
		JTI:      jti,
		Kind:     auth.KindHuman,
	}, identity.BrowserAccessTTL)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	// Binding after signing: the token exists only if it was signed, and a
	// binding failure must not hand out an untracked credential.
	if err := h.cfg.Store.BindBrowserToken(r.Context(), jti, session); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, browserTokenView{
		AccessToken: token,
		TokenType:   "Bearer",
		ExpiresIn:   int64(identity.BrowserAccessTTL / time.Second),
		Subject:     session.Subject,
		Tenant:      session.Tenant,
	})
}

func (h *api) browserLogout(w http.ResponseWriter, r *http.Request) {
	if !browserCSRFOK(r) {
		h.browserError(w, r, http.StatusForbidden, contracts.ErrForbidden, "identityapi: session header missing")
		return
	}
	if sid, ok := h.sessionID(r); ok {
		if err := h.cfg.Store.RevokeBrowserSession(r.Context(), sid); err != nil &&
			contracts.Code(err) != contracts.ErrNotFound {
			transport.WriteError(w, r, err)
			return
		}
	}
	// The cookie goes away whatever the store said: an already-dead session is
	// a successful logout from the browser's point of view.
	h.clearSessionCookie(w, r)
	transport.WriteJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (h *api) browserMeUser(w http.ResponseWriter, r *http.Request) {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		h.browserError(w, r, http.StatusUnauthorized, codeUnauthenticated, "identityapi: no principal")
		return
	}
	user, err := h.cfg.Store.BrowserUserBySubject(r.Context(), p.Claims.Tenant, p.Claims.Subject)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, browserPrincipalView{
		ID: user.UserID, Subject: user.Subject, Tenant: user.Tenant,
		DisplayName: user.DisplayName, Email: user.Email,
		ExpiresAt: time.Unix(p.Claims.ExpiresAt, 0).UTC(),
	})
}

func (h *api) browserMeFlags(w http.ResponseWriter, r *http.Request) {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		h.browserError(w, r, http.StatusUnauthorized, codeUnauthenticated, "identityapi: no principal")
		return
	}
	user, err := h.cfg.Store.BrowserUserBySubject(r.Context(), p.Claims.Tenant, p.Claims.Subject)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, browserFlagsView{
		Roles:        user.Roles,
		Permissions:  user.Permissions,
		FeatureFlags: h.cfg.Browser.FeatureFlags,
		AuthProfile:  h.authenticator().Profile(),
	})
}

// browserAuthConfig reports the authentication profile before login so the UI
// can adapt its seam. It is intentionally reachable without a credential and
// exposes no secret.
func (h *api) browserAuthConfig(w http.ResponseWriter, r *http.Request) {
	authn := h.authenticator()
	transport.WriteJSON(w, http.StatusOK, browserAuthConfigView{
		Profile:         authn.Profile(),
		BrowserAudience: h.cfg.Browser.accessAudience(h.cfg.Audience),
		LocalProfile:    authn.Profile() == "local",
	})
}

// authenticator resolves the configured authenticator, defaulting to the local
// development credential profile so callers that only set BrowserConfig keep
// working unchanged.
func (h *api) authenticator() identity.Authenticator {
	if h.cfg.Auth != nil {
		return h.cfg.Auth
	}
	return identity.NewLocalAuthenticator(h.cfg.Store)
}

// exchangeScopedTaskBody is what a trusted service sends to turn a browser
// session into a scoped task. It carries NO subject, tenant or deployment: the
// subject and tenant come only from the verified human token, and the
// deployment is the caller's own workload certificate. task is the caller's
// thread/task identifier (a server service may name a task; a browser may not
// choose a subject or a deployment).
type exchangeScopedTaskBody struct {
	HumanToken   string   `json:"human_token"`
	Task         string   `json:"task"`
	Context      string   `json:"context,omitempty"`
	Capabilities []string `json:"capabilities,omitempty"`
}

type exchangeScopedTaskView struct {
	Token string `json:"token"`
	Task  string `json:"task"`
}

// exchangeScopedTask implements the Human -> authorized Space -> scoped task
// transition on the SERVER side (PDF 3.5: the task token is obtained by a
// server via a grant, never carried by the client). Order of checks, all fail
// closed:
//
//  1. the caller is an allowlisted exchanger deployment (trusted service);
//  2. the presented human token verifies for the browser audience and is a
//     live, unrevoked session (logout / refresh rotation / subject revocation
//     reach this immediately, not at token expiry);
//  3. the human's tenant equals the caller's certificate tenant, so a trusted
//     service in one tenant can never mint authority in another;
//  4. the human personally holds the exchange permission;
//  5. identity roots a per-session ingress authority under the verified subject
//     and the CALLER's deployment. The durable root ceiling is the human's own
//     permissions at creation, so the authority is scoped to (subject, session)
//     and a fresh login mints a fresh root: revoking or expiring one session
//     never breaks another session or unrelated chats; only a subject
//     revocation blocks future logins.
//  6. EVERY exchange re-derives the effective capability set from the human's
//     CURRENT permissions intersected with what the caller asked for, and mints
//     an ATTENUATED grant. A reused authority therefore never replays an older,
//     wider grant: if the caller narrows its request or the human lost a
//     permission since the root was created, the minted token (and its live
//     grant, which is what Evaluate and Authorize enforce) carries only the
//     remaining capabilities. Widening beyond the root ceiling is refused; a
//     re-login (new session, new root) is required to gain genuinely new rights.
//
// The task token is bound to the caller's workload, which presents it to State
// with its own mTLS and its own server-derived subject. The human JWT is never
// forwarded to State and no signing or task credential is returned to a browser.
func (h *api) exchangeScopedTask(w http.ResponseWriter, r *http.Request) {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		h.exchangeRefuse(w, r, ReasonAzpMismatch,
			"identityapi: exchange refused: no workload principal", "", "", "", "")
		return
	}
	if !h.cfg.workloadAllowed(p.Scope, h.cfg.ExchangerWorkloadIDs, h.cfg.ExchangerDeployments) {
		h.exchangeRefuse(w, r, ReasonAzpMismatch,
			"identityapi: exchange refused: deployment is not an authorized exchanger", "", "", p.Scope.Deployment, "")
		return
	}
	var body exchangeScopedTaskBody
	if !h.readStrictJSON(w, r, &body) {
		return
	}
	if strings.TrimSpace(body.HumanToken) == "" || strings.TrimSpace(body.Task) == "" {
		h.browserError(w, r, http.StatusBadRequest, contracts.ErrInvalid,
			"identityapi: human_token and task are required")
		return
	}
	context := body.Context
	if context == "" {
		context = body.Task
	}
	browserAud := h.cfg.Browser.accessAudience(h.cfg.Audience)
	claims, err := h.cfg.Verifier.Verify(body.HumanToken, browserAud, auth.KindHuman, "")
	if err != nil || claims.JTI == "" {
		// Not a browser human token this service signed (or it has no session
		// binding). A control human token, a task token and a forged token all
		// land here identically.
		h.exchangeRefuse(w, r, ReasonTokenUseInvalid,
			"identityapi: exchange refused: a live browser human token is required", body.Task, context, p.Scope.Deployment, "")
		return
	}
	// The caller's tenant comes from its certificate and the human's tenant
	// from the verified token; they must agree. A mismatch is denied without
	// distinguishing, so one tenant cannot probe another.
	if claims.Tenant != p.Scope.Tenant {
		// Opaque on the wire (no tenant oracle); the journal records the true
		// cause so an operator can still diagnose a cross-tenant refusal.
		h.exchangeRefuseOpaque(w, r, reasonTenantMismatchInternal, body.Task, context, p.Scope.Deployment, claims.Subject)
		return
	}
	// Live-session gate: this is what makes logout, refresh rotation and subject
	// revocation stop the next exchange immediately.
	if _, err := h.cfg.Store.BrowserTokenActive(r.Context(), claims.JTI); err != nil {
		h.exchangeRefuse(w, r, ReasonGrantExpired,
			"identityapi: exchange refused: browser session is not live", body.Task, context, p.Scope.Deployment, claims.Subject)
		return
	}
	// The human must personally be entitled, and the task can only carry
	// capabilities the human holds.
	human, err := h.cfg.Store.BrowserUserBySubject(r.Context(), claims.Tenant, claims.Subject)
	if err != nil {
		// A subject that cannot be resolved inside the already-matched tenant
		// stays indistinguishable on the wire (no subject-existence oracle);
		// only the journal records it.
		h.exchangeRefuseOpaque(w, r, "subject_unresolvable", body.Task, context, p.Scope.Deployment, claims.Subject)
		return
	}
	required := h.cfg.ExchangeRequiredPermission()
	if !contains(human.Permissions, required) {
		h.exchangeRefuse(w, r, ReasonExchangePermissionMissing,
			"identityapi: exchange refused: subject lacks the exchange entitlement", body.Task, context, p.Scope.Deployment, claims.Subject)
		return
	}
	// effective is recomputed from the human's CURRENT permissions on every
	// exchange, so a dropped permission can never ride along from an earlier
	// mint. With no requested list it is the whole held set; it can never add a
	// capability the human lacks.
	effective := intersect(human.Permissions, body.Capabilities)
	if len(effective) == 0 {
		h.exchangeRefuse(w, r, ReasonCapabilityWidening,
			"identityapi: exchange refused: no requested capability is held by the subject", body.Task, context, p.Scope.Deployment, claims.Subject)
		return
	}
	scope := contracts.Scope{Tenant: claims.Tenant, Deployment: p.Scope.Deployment, Subject: claims.Subject}
	// The durable per-session authority ceiling is the human's permissions at
	// first use. It is created once; reuse never re-creates it (a differing body
	// on the same task would be a key reuse), which is why attenuation is done
	// by minting a child rather than by mutating the root.
	root, err := h.cfg.Store.Get(r.Context(), scope, body.Task)
	if err != nil {
		if contracts.Code(err) != contracts.ErrNotFound {
			h.exchangeStoreError(w, r, err, body.Task, context, p.Scope.Deployment, claims.Subject)
			return
		}
		root, err = h.cfg.Store.CreateRoot(r.Context(), scope, identity.RootRequest{
			Task:         body.Task,
			Context:      context,
			Deployment:   p.Scope.Deployment,
			Capabilities: human.Permissions,
			TTL:          identity.ExchangeAuthorityTTL,
			AllowSelf:    true,
		})
		if err != nil {
			h.exchangeStoreError(w, r, err, body.Task, context, p.Scope.Deployment, claims.Subject)
			return
		}
	}
	// Mint an attenuated grant when the effective set is narrower than the root
	// ceiling; otherwise the effective set equals the ceiling and the root
	// itself is minted. The child id depends only on the (root, effective set)
	// pair, so repeating the same narrowed exchange is idempotent and creates no
	// per-request proliferation.
	mintTask := body.Task
	if !sameCapabilitySet(root.Capabilities, effective) {
		mintTask = attenuatedTaskID(body.Task, effective)
		if _, err := h.cfg.Store.Delegate(r.Context(), scope, body.Task, identity.DelegateRequest{
			Task:         mintTask,
			Context:      context,
			Deployment:   p.Scope.Deployment,
			Capabilities: effective,
			TTL:          identity.ExchangeAuthorityTTL,
		}); err != nil {
			h.exchangeStoreError(w, r, err, body.Task, context, p.Scope.Deployment, claims.Subject)
			return
		}
	}
	token, err := h.cfg.Store.MintAuthorityTask(r.Context(), scope, mintTask, h.cfg.Signer, h.cfg.Audience, 0)
	if err != nil {
		h.exchangeStoreError(w, r, err, body.Task, context, p.Scope.Deployment, claims.Subject)
		return
	}
	// The response names the task the returned token is ACTUALLY bound to. When
	// the grant was attenuated that is the narrowed child task, not the broad
	// authority task the caller sent: the caller must delegate, mint and cancel
	// against this value (frozen Tier-2 contract), because a token can only act
	// under its own task. It equals the request task only when the token carries
	// the full ceiling.
	transport.WriteJSON(w, http.StatusOK, exchangeScopedTaskView{Token: token, Task: mintTask})
}

// exchangeStoreError writes a Store error the exchange path hit on a
// Get/CreateRoot/Delegate/MintTask. When the error is one of the tree/quota/
// expiry denials the identity package reports as FORBIDDEN, it is surfaced with
// the stable exchange reason code (and WARN), so a caller can machine-
// distinguish depth vs quota vs widening vs expiry. A revoked-subject denial is
// intentionally NOT mapped and stays the generic opaque FORBIDDEN.
// Any other error keeps its own contract code via transport.WriteError, so
// classification never turns a DENY into an ALLOW nor invents a code for an
// unknown failure.
func (h *api) exchangeStoreError(w http.ResponseWriter, r *http.Request, err error, task, context, azp, subject string) {
	if reason, ok := classifyExchangeStoreError(err); ok {
		h.exchangeRefuse(w, r, reason, "identityapi: exchange refused: authority tree denied the mint", task, context, azp, subject)
		return
	}
	transport.WriteError(w, r, err)
}

// sameCapabilitySet reports whether two capability slices hold the same set,
// ignoring order. Both sides are normalised to sorted copies so it is correct
// whether or not the caller pre-sorted them.
func sameCapabilitySet(a, b []string) bool {
	xs := append([]string(nil), a...)
	ys := append([]string(nil), b...)
	sort.Strings(xs)
	sort.Strings(ys)
	if len(xs) != len(ys) {
		return false
	}
	for i := range xs {
		if xs[i] != ys[i] {
			return false
		}
	}
	return true
}

// attenuatedTaskID names the child grant that carries exactly the effective
// capability set under an authority. It is a deterministic function of the
// authority task and the sorted effective set, so the same narrowed exchange
// repeats idempotently while a different set yields a different child.
func attenuatedTaskID(authority string, effective []string) string {
	sorted := append([]string(nil), effective...)
	sort.Strings(sorted)
	h := sha256.New()
	h.Write([]byte(authority))
	h.Write([]byte{0})
	h.Write([]byte(strings.Join(sorted, "\x00")))
	sum := h.Sum(nil)
	return "spauth:" + hex.EncodeToString(sum[:])[:40]
}

func contains(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}

// intersect keeps, in human order, the permissions the human holds that the
// caller also asked for. With no caller list it returns the human's set. The
// result can never contain a capability the human lacks.
func intersect(held, want []string) []string {
	out := make([]string, 0, len(want))
	seen := make(map[string]bool, len(want))
	for _, h := range held {
		if len(want) > 0 && !contains(want, h) {
			continue
		}
		if seen[h] {
			continue
		}
		seen[h] = true
		out = append(out, h)
	}
	return out
}

// browserIntrospect answers a workload service: is this human token still
// backed by a live session? An inactive answer is a 200 with active=false, the
// way introspection works; the token itself is never echoed back.
func (h *api) browserIntrospect(w http.ResponseWriter, r *http.Request) {
	var body browserIntrospectBody
	if !h.readStrictJSON(w, r, &body) {
		return
	}
	if strings.TrimSpace(body.Token) == "" {
		h.browserError(w, r, http.StatusBadRequest, contracts.ErrInvalid, "identityapi: token is required")
		return
	}
	// Introspection is revocation-aware, so it has to look inside the
	// credential to find its identifier. Anything that is not a browser token
	// this service signed is simply inactive; the caller is a workload, not an
	// auditor, and is not told why.
	claims, err := h.cfg.Verifier.Verify(body.Token, h.cfg.Browser.accessAudience(h.cfg.Audience),
		auth.KindHuman, "")
	if err != nil || claims.JTI == "" {
		transport.WriteJSON(w, http.StatusOK, browserIntrospectView{Active: false})
		return
	}
	session, err := h.cfg.Store.BrowserTokenActive(r.Context(), claims.JTI)
	if err != nil {
		if contracts.Code(err) == contracts.ErrNotFound {
			transport.WriteJSON(w, http.StatusOK, browserIntrospectView{Active: false})
			return
		}
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, browserIntrospectView{
		Active: true, Subject: session.Subject, Tenant: session.Tenant,
		SessionID: session.ID, ExpiresAt: session.ExpiresAt,
	})
}

// requireBrowserPrincipal verifies a human JWT for the configured audience and
// then demands that the credential is still backed by a live session, so
// logout, refresh rotation and subject revocation reach the UI endpoints
// immediately instead of at token expiry.
func (h *api) requireBrowserPrincipal(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if q := r.URL.Query(); len(q) > 0 {
			// The caller's scope is whatever its token says. A query parameter
			// that names a subject or tenant is not a filter to honour, it is a
			// caller trying to choose an identity.
			h.browserError(w, r, http.StatusBadRequest, contracts.ErrInvalid,
				"identityapi: this endpoint takes no query parameters; identity comes from the token")
			return
		}
		raw, err := browserBearer(r)
		if err != nil {
			h.browserError(w, r, http.StatusUnauthorized, codeUnauthenticated, err.Error())
			return
		}
		claims, err := h.cfg.Verifier.Verify(raw, h.cfg.Browser.accessAudience(h.cfg.Audience), auth.KindHuman, "")
		if err != nil {
			h.browserError(w, r, http.StatusUnauthorized, codeUnauthenticated,
				"identityapi: bearer is not a valid browser token")
			return
		}
		if claims.JTI == "" {
			// A token of this audience with no jti was not minted here, so it
			// cannot be checked for revocation. Refuse rather than trust exp.
			h.browserError(w, r, http.StatusUnauthorized, codeUnauthenticated,
				"identityapi: bearer carries no token identifier")
			return
		}
		if _, err := h.cfg.Store.BrowserTokenActive(r.Context(), claims.JTI); err != nil {
			if contracts.Code(err) == contracts.ErrNotFound {
				h.browserError(w, r, http.StatusUnauthorized, codeUnauthenticated,
					"identityapi: session is no longer active")
				return
			}
			// Anything else is this service failing, not the caller being
			// unauthenticated. It stays fail-closed (no request proceeds) but
			// is reported as a server fault so a storage problem cannot be
			// mistaken for a revoked session.
			transport.WriteError(w, r, err)
			return
		}
		scope := claims.Scope()
		scope.Deployment = "" // a human principal has no deployment; see pkg/auth
		next.ServeHTTP(w, r.WithContext(transport.WithPrincipal(r.Context(),
			transport.Principal{Claims: claims, Scope: claims.Scope()})))
	})
}

// --- helpers --------------------------------------------------------------

// newTokenID returns 256 bits of CSPRNG output as a base64url jti.
func newTokenID() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("identityapi: token identifier: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func browserCSRFOK(r *http.Request) bool {
	return r.Header.Get(browserSessionHeader) == "1"
}

func browserBearer(r *http.Request) (string, error) {
	const prefix = "Bearer "
	got := r.Header.Get("Authorization")
	if len(got) <= len(prefix) || !strings.EqualFold(got[:len(prefix)], prefix) {
		return "", errors.New("identityapi: a bearer token is required")
	}
	token := strings.TrimSpace(got[len(prefix):])
	if token == "" || strings.ContainsAny(token, " \t\r\n") {
		return "", errors.New("identityapi: malformed bearer token")
	}
	return token, nil
}

// readStrictJSON decodes a body that may name only the fields of out. It is
// deliberately stricter than transport.ReadJSON here: on this surface an extra
// field such as "tenant" or "subject" is an attempt to choose an identity, not
// a harmless unknown key.
func (h *api) readStrictJSON(w http.ResponseWriter, r *http.Request, out any) bool {
	if r.Body == nil {
		h.browserError(w, r, http.StatusBadRequest, contracts.ErrInvalid, "identityapi: request has no body")
		return false
	}
	defer func() { _ = r.Body.Close() }()
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBrowserBody))
	if err != nil {
		h.browserError(w, r, http.StatusBadRequest, contracts.ErrInvalid,
			"identityapi: request body is missing or too large")
		return false
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(out); err != nil {
		h.browserError(w, r, http.StatusBadRequest, contracts.ErrInvalid,
			"identityapi: request body is not valid JSON of the expected shape")
		return false
	}
	if dec.More() {
		h.browserError(w, r, http.StatusBadRequest, contracts.ErrInvalid,
			"identityapi: request body carries trailing data")
		return false
	}
	return true
}
func (h *api) sessionID(r *http.Request) (string, bool) {
	c, err := r.Cookie(h.cfg.Browser.cookieName())
	if err != nil || c.Value == "" {
		return "", false
	}
	return c.Value, true
}

func (h *api) setSessionCookie(w http.ResponseWriter, r *http.Request, sid string) {
	http.SetCookie(w, &http.Cookie{
		Name:     h.cfg.Browser.cookieName(),
		Value:    sid,
		Path:     "/",
		HttpOnly: true,
		Secure:   browserCookieSecure(r),
		SameSite: http.SameSiteStrictMode,
		MaxAge:   int(identity.BrowserSessionIdleTTL / time.Second),
	})
}

func (h *api) clearSessionCookie(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     h.cfg.Browser.cookieName(),
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		Secure:   browserCookieSecure(r),
		SameSite: http.SameSiteStrictMode,
		MaxAge:   -1,
		Expires:  time.Unix(0, 0).UTC(),
	})
}

// browserCookieSecure marks the cookie Secure whenever the request arrived over
// TLS, including through a proxy that terminated it.
func browserCookieSecure(r *http.Request) bool {
	if r.TLS != nil {
		return true
	}
	return strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https")
}

// browserError writes an authentication-shaped failure. It exists because the
// shared transport mapper has no 401 class, and 401 is what a UI keys its
// "sign in again" path on.
func (h *api) browserError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	_ = r
	transport.WriteJSON(w, status, &contracts.Error{Code: code, Message: message})
}
