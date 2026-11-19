package space

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// IdentityExchangePath is the authoritative Identity control-plane route a trusted
// service (Space) calls to turn a verified browser human JWT into a scoped Task
// token (decision 2026-10-30-human-task-exchange-wire-contract). Space posts the
// request there over its OWN workload mTLS (transport.Workload()).
const IdentityExchangePath = "/v1/identity/control/exchange"

// HumanPrincipal is a browser caller whose human JWT (aud "ahp-web") has been
// verified by the Space auth middleware (signature + expiry + live session). It
// carries the VERIFIED subject and the human bearer itself: the human JWT exists
// OUTSIDE the auth middleware ONLY on this single server->Identity exchange hop,
// and is NEVER forwarded to State or to any external MCP surface. The browser never
// chooses tenant/deployment and never receives a Task or a signing credential.
type HumanPrincipal struct {
	Subject    string
	HumanToken string
}

// TaskExchanger performs the server-side human->scoped-task exchange (PDF §3.5): it
// turns a verified human principal + a server-derived task id into a short-lived,
// scope-bound State Task token via an explicit trusted operation against Identity.
// The Space edge uses the result for every State call so the browser's human JWT is
// NEVER sent to State and the browser is NEVER handed a Task or signing credential.
// Implementations MUST be fail-closed: any verification or mint failure is returned
// as an error and the request is refused, never silently downgraded to forwarding
// the human JWT.
type TaskExchanger interface {
	// Exchange returns the scoped task TOKEN and the task it is actually bound to
	// (issuedTask). Per the frozen contract the issued task is the ATTENUATED task the
	// token carries (Claims.Task), which Space uses as the Tier-2 delegate PARENT - never
	// the broad request task, and never a browser value.
	Exchange(ctx context.Context, human HumanPrincipal, requestTask string) (token string, issuedTask string, err error)
}

// scopedTaskKey carries an already-exchanged (or, for trusted service callers,
// already-valid) State task token through the request context so handlers never
// touch the raw Authorization header.
type scopedTaskKey struct{}

// WithScopedTask returns ctx carrying a task token resolved by the auth layer.
func WithScopedTask(ctx context.Context, token string) context.Context {
	return context.WithValue(ctx, scopedTaskKey{}, token)
}

// sessionKey carries the verified browser session id (the human token's JTI). Base
// pkg/auth Claims has no JTI; once the AUTHORITATIVE identity pkg/auth (canonical jti)
// is composed, the auth layer injects the verified JTI here and the exchange authority
// becomes session-scoped. At base it is empty and the authority falls back to a
// deterministic per-subject label (never empty, so body-less reads still succeed).
type sessionKey struct{}

func WithSession(ctx context.Context, jti string) context.Context {
	return context.WithValue(ctx, sessionKey{}, jti)
}
func SessionFromContext(ctx context.Context) string {
	v, _ := ctx.Value(sessionKey{}).(string)
	return v
}

// authorityTaskKey carries the SERVER-OWNED Tier-1 ingress authority task id produced by
// the human->task exchange. Tier-2 Delegate uses it as the child root's PARENT (a run grant
// is a child of the session authority root, never a sibling), so it is a resource id, not a
// credential, and is safe to thread through the request context (never serialized).
type authorityTaskKey struct{}

func WithAuthorityTask(ctx context.Context, task string) context.Context {
	return context.WithValue(ctx, authorityTaskKey{}, task)
}
func AuthorityTaskFromContext(ctx context.Context) string {
	v, _ := ctx.Value(authorityTaskKey{}).(string)
	return v
}

// ScopedTaskFromContext extracts a task token previously resolved by the auth layer.
func ScopedTaskFromContext(ctx context.Context) (string, bool) {
	t, ok := ctx.Value(scopedTaskKey{}).(string)
	return t, ok && t != ""
}

// ExchangeMiddleware wraps an upstream auth middleware (which verifies the browser
// human JWT and injects the principal) with the server-side human->task exchange:
// it extracts the human bearer, derives the server-visible task id for the request,
// exchanges the verified human principal for a scoped task token, and re-injects the
// request carrying WithScopedTask. If no principal was verified the request is passed
// through so the Space handler still answers 403 (fail-closed).
func ExchangeMiddleware(exchanger TaskExchanger, auth func(http.Handler) http.Handler) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return auth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if _, ok := ScopedTaskFromContext(r.Context()); ok {
				next.ServeHTTP(w, r) // already resolved by an upstream stage
				return
			}
			p, ok := transport.FromContext(r.Context())
			if !ok {
				next.ServeHTTP(w, r) // no principal -> handler returns 403
				return
			}
			if exchanger == nil {
				// A human browser request reached Space but no exchanger is wired.
				// Fail closed: never forward the human JWT to State. (Production
				// as-runner refuses to start serving human traffic without one.)
				transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden,
					"space: no task exchange configured (human JWT must not reach State)"))
				return
			}
			subj := p.Claims.Subject
			if subj == "" {
				subj = p.Scope.Subject
			}
			// The single hop where the human JWT exists outside the auth middleware.
			// The task scope is SERVER-OWNED (from the verified principal + the
			// server-fixed deployment), NEVER read from the browser body/path, so a
			// caller cannot choose a thread/task/context and a body-less read/list op
			// still gets a valid subject-scoped authority. The middleware does not
			// consume the request body.
			bearer := bearerToken(r)
			requestTask := serverAuthorityTask(r.Context(), p)
			token, issuedTask, err := exchanger.Exchange(r.Context(),
				HumanPrincipal{Subject: subj, HumanToken: bearer}, requestTask)
			if err != nil {
				transport.WriteError(w, r, err)
				return
			}
			// The Tier-2 delegate PARENT is the task the issued token is ACTUALLY bound to
			// (Identity's attenuated mintTask), never the broad request task and never a
			// browser value. The exchanger fails closed if Identity returns no task binding.
			if issuedTask == "" {
				transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "space: exchange issued an unbound task"))
				return
			}
			next.ServeHTTP(w, r.WithContext(WithScopedTask(WithAuthorityTask(r.Context(), issuedTask), token)))
		}))
	}
}

// serverAuthorityTask returns the SERVER-OWNED authority/request id the human->task
// exchange is scoped to. It is derived ONLY from the VERIFIED principal + the server-fixed
// deployment + the verified browser session (JTI, when the authoritative identity pkg/auth
// is composed; see SessionFromContext). It is NEVER read from a browser body/path value, so
// a caller cannot choose its identity and a body-less read/list op still gets a valid,
// non-empty authority. Distinct sessions => distinct authority (revoke/expiry of one login
// does not brick the next). Capability attenuation + per-(subject,session) rooting are
// Identity Tier-1 duties (decision 2026-10-30-exchange-scope-authority-model).
func serverAuthorityTask(ctx context.Context, p transport.Principal) string {
	subj := p.Claims.Subject
	if subj == "" {
		subj = p.Scope.Subject
	}
	dep := p.Scope.Deployment
	if dep == "" {
		dep = p.Claims.Deployment
	}
	// "space-authority:<subject>:<deployment>:<session>" (session rides in the token).
	return "space-authority:" + subj + ":" + dep + ":" + SessionFromContext(ctx)
}

// GrantExchangerConfig configures the Identity-backed task exchanger. Per the
// authoritative wire contract the request carries NO tenant/subject/deployment/
// grant/session/ttl — subject+tenant are derived by Identity from the verified
// human_token, and deployment from the calling workload certificate. So the config
// holds only the endpoint + the mTLS client + optional capabilities + a hop timeout.
type GrantExchangerConfig struct {
	// IdentityExchangeURL is the Identity exchange endpoint: either the full URL
	// (…/v1/identity/control/exchange) or a base to which IdentityExchangePath is
	// appended. Never a browser-reachable value.
	IdentityExchangeURL string
	// Client is the mTLS http client presenting Space's OWN workload certificate.
	// Space authenticates to Identity as a trusted service; the browser's credential
	// is never reused here.
	Client *http.Client
	// AuthorityNamespace is the server's own workload identity. Human JWTs have
	// no deployment before the exchange; without a namespace two deployments
	// serving one browser session derive the same Identity context. This affects
	// request identity only, never authorization or the verified returned scope.
	AuthorityNamespace string
	// Capabilities is an optional subset of capabilities to request on the task;
	// Identity intersects it with the human's held permissions.
	Capabilities []string
	// RequestTimeout bounds the server->server hop.
	RequestTimeout time.Duration
}

// IdentityGrantExchanger is the production TaskExchanger: an explicit, trusted
// Space->Identity operation that mints a scoped Task token for a verified human
// subject. Space does NOT hold a signing key (Identity holds it) and the browser
// never sees the result beyond its own human session.
type IdentityGrantExchanger struct {
	cfg GrantExchangerConfig
	url string
}

func NewIdentityGrantExchanger(cfg GrantExchangerConfig) (*IdentityGrantExchanger, error) {
	if cfg.IdentityExchangeURL == "" {
		return nil, contracts.NewError(contracts.ErrInvalid, "space: exchange needs the identity exchange URL")
	}
	if cfg.Client == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "space: exchange needs an mTLS client (Space workload identity)")
	}
	url := cfg.IdentityExchangeURL
	if !strings.Contains(url, IdentityExchangePath) {
		url = strings.TrimRight(url, "/") + IdentityExchangePath
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 5 * time.Minute
	}
	client := *cfg.Client
	client.Timeout = cfg.RequestTimeout
	e := &IdentityGrantExchanger{cfg: cfg, url: url}
	e.cfg.Client = &client
	return e, nil
}

// exchangeRequest is the AUTHORITATIVE Identity exchange body (decision
// 2026-10-30-human-task-exchange-wire-contract). Strict: no grant/subject/tenant/
// deployment/session_id/ttl_seconds. subject+tenant come from the verified
// human_token; deployment comes from the workload certificate.
type exchangeRequest struct {
	HumanToken   string   `json:"human_token"`
	Task         string   `json:"task"`
	Context      string   `json:"context,omitempty"`
	Capabilities []string `json:"capabilities,omitempty"`
}

// exchangeResponse is the authoritative response: a scoped task token under "token"
// (NOT "task_token") plus the echoed task.
type exchangeResponse struct {
	Token string `json:"token"`
	Task  string `json:"task"`
}

func (e *IdentityGrantExchanger) Exchange(ctx context.Context, human HumanPrincipal, task string) (string, string, error) {
	if human.HumanToken == "" {
		return "", "", contracts.NewError(contracts.ErrForbidden, "space: exchange needs the verified human token")
	}
	if task == "" {
		return "", "", contracts.NewError(contracts.ErrForbidden, "space: exchange needs a server-derived task id")
	}
	if e.cfg.AuthorityNamespace != "" {
		digest := sha256.Sum256([]byte("AHP/Space/ingress/v1\x00" + e.cfg.AuthorityNamespace + "\x00" + task))
		task = fmt.Sprintf("space-authority:%x", digest)
	}
	req := exchangeRequest{
		HumanToken:   human.HumanToken,
		Task:         task,
		Capabilities: e.cfg.Capabilities,
	}
	body, _ := json.Marshal(req)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url, bytes.NewReader(body))
	if err != nil {
		return "", "", err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := e.cfg.Client.Do(httpReq)
	if err != nil {
		return "", "", contracts.NewError(contracts.ErrForbidden, "space: task exchange transport failed")
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		// Do not echo the body (may carry identifiers); fail closed.
		return "", "", contracts.NewError(contracts.ErrForbidden, fmt.Sprintf("space: task exchange rejected (HTTP %d)", resp.StatusCode))
	}
	var out exchangeResponse
	if err := json.Unmarshal(data, &out); err != nil || out.Token == "" {
		return "", "", contracts.NewError(contracts.ErrForbidden, "space: task exchange returned no token")
	}
	// Fail closed if Identity issued a token with no task binding: the Tier-2 delegate
	// parent is derived from this task, so an unbound token cannot authorize a child.
	if out.Task == "" {
		return "", "", contracts.NewError(contracts.ErrForbidden, "space: task exchange returned a token with no task binding")
	}
	return out.Token, out.Task, nil
}

// NewMTLSClient builds an http.Client presenting a workload client certificate and
// trusting a private CA — used for the Space->Identity and Space->State server->
// server hops. It never carries any browser credential.
func NewMTLSClient(certFile, keyFile, caFile string) (*http.Client, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	caPem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPem) {
		return nil, contracts.NewError(contracts.ErrInvalid, "space: bad CA bundle")
	}
	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig:     &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}, RootCAs: pool},
			MaxIdleConnsPerHost: 1024,
			MaxIdleConns:        2048,
			IdleConnTimeout:     90 * time.Second,
		},
	}, nil
}
