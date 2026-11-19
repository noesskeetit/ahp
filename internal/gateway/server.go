package gateway

import (
	"context"
	"net/http"
	"strings"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// Route defaults of the execution-plane ingress.
const (
	DefaultPublicPath   = "/a2a/public"
	DefaultInternalPath = "/a2a/internal"
	DefaultHealthPath   = "/healthz"
	// DefaultMaxRequestBytes bounds one A2A request body.
	DefaultMaxRequestBytes = 4 << 20
)

// AuthConfig declares how one ingress face authenticates callers. Production
// uses Verifier plus Audience; Middleware is an explicit override seam for
// deployments whose identity arrives over a different channel (a test harness,
// a trusted sidecar). Setting neither fails closed.
type AuthConfig struct {
	Verifier   *auth.Verifier
	Audience   string
	Capability string
	Authorize  transport.Authorize
	Middleware func(http.Handler) http.Handler
}

func (c AuthConfig) middleware() (func(http.Handler) http.Handler, error) {
	if c.Middleware != nil {
		return c.Middleware, nil
	}
	if c.Verifier == nil || c.Audience == "" {
		return nil, contracts.NewError(contracts.ErrInvalid, "gateway: ingress needs a verifier and audience, or an explicit middleware")
	}
	return transport.Human(c.Verifier, c.Audience), nil
}

// ServerConfig wires the public and internal A2A faces of one execution plane.
type ServerConfig struct {
	AgentCard *a2a.AgentCard
	// Public is the human-facing face (end-user clients of an agent).
	Public AuthConfig
	// Internal is the service-to-service face used by delegating tasks and
	// sibling deployments. It defaults to task-token authentication.
	Internal AuthConfig
	// TaskAuthorize gates the internal face; required unless Internal supplies
	// its own Middleware.
	TaskAuthorize transport.Authorize
	PublicPath    string
	InternalPath  string
	HealthPath    string
	// MaxRequestBytes bounds a request body; zero uses DefaultMaxRequestBytes.
	MaxRequestBytes int64
}

// NewServer builds the ingress. Both faces mount the official a2a-go
// JSON-RPC transport, so streaming (SSE) and every protocol method behave
// exactly as the specification requires; AHP only chooses who may reach them.
func NewServer(h *Handler, cfg ServerConfig) (http.Handler, error) {
	if h == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "gateway: handler is required")
	}
	if cfg.PublicPath == "" {
		cfg.PublicPath = DefaultPublicPath
	}
	if cfg.InternalPath == "" {
		cfg.InternalPath = DefaultInternalPath
	}
	if cfg.HealthPath == "" {
		cfg.HealthPath = DefaultHealthPath
	}
	if cfg.MaxRequestBytes <= 0 {
		cfg.MaxRequestBytes = DefaultMaxRequestBytes
	}
	if cfg.PublicPath == cfg.InternalPath {
		return nil, contracts.NewError(contracts.ErrInvalid, "gateway: public and internal paths must differ")
	}

	public, err := cfg.Public.middleware()
	if err != nil {
		return nil, err
	}
	internal := cfg.Internal.Middleware
	if internal == nil {
		if cfg.Internal.Verifier == nil || cfg.Internal.Audience == "" || cfg.TaskAuthorize == nil {
			return nil, contracts.NewError(contracts.ErrInvalid,
				"gateway: internal ingress needs a verifier, audience and authorizer, or an explicit middleware")
		}
		internal = transport.Task(cfg.Internal.Verifier, cfg.Internal.Audience, cfg.Internal.Capability, cfg.TaskAuthorize)
	}

	rpc := a2asrv.NewJSONRPCHandler(h)
	limit := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Body = http.MaxBytesReader(w, r.Body, cfg.MaxRequestBytes)
			next.ServeHTTP(w, r.WithContext(withBearer(r.Context(), r)))
		})
	}

	mux := http.NewServeMux()
	mux.Handle(cfg.HealthPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		transport.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}))
	if cfg.AgentCard != nil {
		mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(cfg.AgentCard))
	}
	mux.Handle(cfg.PublicPath, limit(public(rpc)))
	mux.Handle(cfg.InternalPath, limit(internal(rpc)))
	return mux, nil
}

// withBearer keeps the request's own bearer available to the acceptance point,
// which verifies a delegated child's parent against the credential that is
// presenting itself rather than against any field of the frame. It lives only in
// this request's context and is never logged or journalled (refusal details are
// sanitised, and no refusal path prints a credential).
func withBearer(ctx context.Context, r *http.Request) context.Context {
	const prefix = "bearer "
	authz := r.Header.Get("Authorization")
	if len(authz) < len(prefix) || !strings.EqualFold(authz[:len(prefix)], prefix) {
		return ctx
	}
	token := strings.TrimSpace(authz[len(prefix):])
	if token == "" {
		return ctx
	}
	return context.WithValue(ctx, bearerContextKey{}, token)
}
