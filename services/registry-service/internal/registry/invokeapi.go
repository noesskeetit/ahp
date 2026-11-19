package registry

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// InvokeRequest is one execution of an installed agent version. The tenant, agent
// and version are NOT supplied by the client: the handler resolves them from the
// installation row the caller names, so a caller can never invoke an arbitrary or
// foreign version. Token is used only by the trusted issuer to derive a scoped
// target grant; it is never forwarded to the runtime, serialized, or logged.
type InvokeRequest struct {
	// Set only by the authenticated conversation controller, never by JSON.
	ConversationContext string          `json:"-"`
	Observe             bool            `json:"-"`
	Capabilities        Capabilities    `json:"capabilities"`
	Scope               contracts.Scope `json:"-"`
	TaskID              string          `json:"task_id"`
	Config              json.RawMessage `json:"config"`
	ConfigRevision      int64           `json:"config_revision"`
	Tenant              string          `json:"tenant"`
	Deployment          string          `json:"deployment"`
	AgentID             string          `json:"agent_id"`
	VersionID           string          `json:"version_id"`
	VersionDigest       string          `json:"version_digest"`
	InstallationID      string          `json:"installation_id"`
	Environment         string          `json:"environment"`
	Input               json.RawMessage `json:"input"`
	Token               string          `json:"-"`
}

// RunDispatcher starts the actual work for an invoke and returns the new task id.
// The dispatcher resolves an immutable version to an operator-registered runtime
// and must fail closed when that target is unavailable.
type RunDispatcher interface {
	Invoke(ctx context.Context, req InvokeRequest) (taskID string, err error)
}

// invoke is a thin route adapter over the accepted pieces: the installation row is
// the authoritative source of tenant/agent/version, the run goes to the injected
// dispatcher (the trigger->run executor), and the result is the accepted task id.
func (h *HTTPHandler) invoke(w http.ResponseWriter, r *http.Request) {
	scope, err := h.registryPrincipal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if h.dispatcher == nil {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrConflict,
			"registry: invoke is not wired on this front (no run dispatcher configured)"))
		return
	}
	var in struct {
		InstallationID string          `json:"installation_id"`
		Input          json.RawMessage `json:"input"`
	}
	if err := transport.ReadJSON(w, r, &in, registryMaxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	// The pin is read from the installation the CALLER's tenant owns; a foreign or
	// unknown installation is reported NOT_FOUND by the Store (tenant from cert).
	inst, err := h.store.GetInstallation(r.Context(), scope.Tenant, in.InstallationID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	reservation := contracts.NewID()
	if _, err = h.store.PinForSubject(r.Context(), scope, inst.ID, reservation); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	effective, err := h.store.ResolveConfig(r.Context(), scope.Tenant, inst.ID, reservation)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}

	req := InvokeRequest{
		Scope: scope, Tenant: scope.Tenant, Deployment: scope.Deployment,
		Capabilities: effective.Capabilities, TaskID: reservation, Config: effective.Config, ConfigRevision: effective.ConfigRevision,
		AgentID: effective.AgentID, VersionID: effective.VersionID, VersionDigest: effective.Digest,
		InstallationID: inst.ID, Environment: inst.Environment, Input: in.Input,
		Token: bearerOf(r),
	}
	taskID, err := h.dispatcher.Invoke(r.Context(), req)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.store.AliasTaskPin(r.Context(), scope.Tenant, inst.ID, reservation, taskID); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"task_id": taskID})
}

func bearerOf(r *http.Request) string {
	h := r.Header.Get("authorization")
	if v, ok := strings.CutPrefix(strings.TrimSpace(h), "Bearer "); ok {
		return strings.TrimSpace(v)
	}
	return ""
}

// -- immutable runtime dispatcher --

// A2AInvokeDispatcher invokes the selected immutable runtime with a gateway mTLS
// certificate and a separately issued target task token. GetInvokedTask observes
// the same pinned runtime after promotion or rollback.
type A2AInvokeDispatcher struct {
	url    string
	store  *Store
	issuer RuntimeTokenIssuer
	client *http.Client
}

// NewA2AInvokeDispatcher retains the legacy TLS constructor for compatibility.
// Its result cannot invoke until an immutable runtime store is supplied by the
// routed constructor; a common URL alone never authorizes execution.
func NewA2AInvokeDispatcher(url, certFile, keyFile, caFile string) (*A2AInvokeDispatcher, error) {
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "invoke: ca: "+err.Error())
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, contracts.NewError(contracts.ErrInvalid, "invoke: ca: no certificate parsed")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "invoke: client cert: "+err.Error())
	}
	host := url
	if i := strings.Index(host, "://"); i >= 0 {
		host = host[i+3:]
	}
	if j := strings.Index(host, "/"); j >= 0 {
		host = host[:j]
	}
	sni := host
	if k := strings.LastIndex(sni, ":"); k >= 0 {
		sni = sni[:k]
	}
	if sni == "127.0.0.1" || sni == "localhost" {
		sni = "localhost"
	}
	return &A2AInvokeDispatcher{url: url, client: &http.Client{
		Timeout:   60 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}},
	}}, nil
}

// Invoke submits one native A2A task without waiting for its execution to finish.
// The task's opaque runtime ID is separate from req.TaskID's Identity grant.
func (d *A2AInvokeDispatcher) Invoke(ctx context.Context, req InvokeRequest) (string, error) {
	binding, targetToken, err := d.authorizeRuntime(ctx, req, true)
	if err != nil {
		return "", err
	}
	if err := requireID("invoke grant", req.TaskID); err != nil {
		return "", err
	}
	conversation := invocationContext(req, binding)
	// Match the conversation transport's native user role and retain the kind
	// hint required by already-published reference images. The pinned upstream
	// A2A1.0 receiver accepts this same text part; both receivers are tested.
	message := map[string]any{
		"role": "ROLE_USER", "messageId": contracts.OperationKey(req.Scope, req.TaskID, "registry.invoke", "v1"),
		"contextId": conversation, "parts": []any{map[string]string{"kind": "text", "text": extractText(req.Input)}},
	}
	params := map[string]any{
		"message":       message,
		"configuration": map[string]any{"returnImmediately": true},
		"metadata":      map[string]any{"ahp": map[string]any{"task_id": req.TaskID, "tenant": req.Tenant, "agent_id": req.AgentID, "version_id": req.VersionID, "version_digest": req.VersionDigest, "installation_id": req.InstallationID, "config_revision": req.ConfigRevision, "config": req.Config}},
	}
	raw, err := d.agentRPC(ctx, binding, targetToken, "SendMessage", params, "", conversation)
	if err != nil {
		return "", err
	}
	var task struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &task); err != nil || task.ID == "" {
		return "", contracts.NewError(contracts.ErrConflict, "invoke: runtime did not return a task")
	}
	return task.ID, nil
}

// Keep the default context identical to runtimeAuthority.issueWithContext. A
// conversation controller may supply its already-authorized stable context.
func invocationContext(req InvokeRequest, binding RuntimeBinding) string {
	if req.ConversationContext != "" {
		return req.ConversationContext
	}
	scope := req.Scope
	scope.Tenant, scope.Deployment = req.Tenant, binding.Deployment
	return contracts.OperationKey(scope, req.TaskID, "runtime.context", "v1")
}

// extractText pulls a text part from the invoke input without ever treating it as
// an identity claim. It tolerates a bare string or an {input|text|parts:[{text}]}
// envelope and falls back to the raw JSON so the run always carries something.
func extractText(input json.RawMessage) string {
	if len(input) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(input, &s) == nil {
		return s
	}
	var obj map[string]any
	if json.Unmarshal(input, &obj) == nil {
		for _, k := range []string{"text", "input", "message"} {
			if v, ok := obj[k].(string); ok {
				return v
			}
		}
		if msg, ok := obj["message"].(map[string]any); ok {
			if parts, ok := msg["parts"].([]any); ok && len(parts) > 0 {
				if p, ok := parts[0].(map[string]any); ok {
					if t, ok := p["text"].(string); ok {
						return t
					}
				}
			}
		}
	}
	return string(input)
}

// RunObserver reads the actual task from its immutable runtime. It is separate
// from acceptance: a submitted task is never reported completed by the registry.
type RunObserver interface {
	Observe(context.Context, InvokeRequest) (json.RawMessage, error)
}

// OwnerRunObserver reads durable history for an independently authenticated human.
// It never reissues an execution grant or relaxes the machine parent relationship.
type OwnerRunObserver interface {
	ObserveOwned(context.Context, contracts.Scope, EffectiveConfig) (json.RawMessage, error)
}

func AttachOwnerObserver(handler http.Handler, observer OwnerRunObserver, verifier *auth.Verifier, humanAudience string, authorize transport.Authorize) error {
	h, ok := handler.(*HTTPHandler)
	if !ok || observer == nil || verifier == nil || humanAudience == "" || authorize == nil {
		return contracts.NewError(contracts.ErrInvalid, "registry: owner observation requires verified human policy")
	}
	h.ownerObserver = observer
	h.observationAuth = transport.TaskOrUser(verifier, registryAudience, humanAudience, registryCapability, authorize)
	return nil
}

func (h *HTTPHandler) getInvokedTask(w http.ResponseWriter, r *http.Request) {
	scope, err := h.registryPrincipal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in struct {
		InstallationID string `json:"installation_id"`
		TaskID         string `json:"task_id"`
	}
	if err := transport.ReadJSON(w, r, &in, registryMaxBody); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if _, err := h.store.PinnedVersion(r.Context(), scope.Tenant, in.TaskID); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	config, err := h.store.ResolveConfig(r.Context(), scope.Tenant, in.InstallationID, in.TaskID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	principal, _ := transport.FromContext(r.Context())
	if principal.Claims.Kind == auth.KindHuman {
		if h.ownerObserver == nil {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrUnavailable, "owner history reader is not configured"))
			return
		}
		task, err := h.ownerObserver.ObserveOwned(r.Context(), scope, config)
		if err != nil {
			transport.WriteError(w, r, err)
			return
		}
		transport.WriteJSON(w, http.StatusOK, map[string]any{"task": task})
		return
	}
	observer, ok := h.dispatcher.(RunObserver)
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrUnavailable, "runtime observation is not configured"))
		return
	}
	req := InvokeRequest{Observe: true, Scope: scope, TaskID: in.TaskID, Tenant: scope.Tenant, Deployment: scope.Deployment, AgentID: config.AgentID, VersionID: config.VersionID, VersionDigest: config.Digest, InstallationID: in.InstallationID, Environment: config.Environment, Capabilities: config.Capabilities, Config: config.Config, ConfigRevision: config.ConfigRevision, Token: bearerOf(r)}
	task, err := observer.Observe(r.Context(), req)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"task": task})
}

func (d *A2AInvokeDispatcher) Observe(ctx context.Context, req InvokeRequest) (json.RawMessage, error) {
	if d.store == nil || d.issuer == nil {
		return nil, contracts.NewError(contracts.ErrUnavailable, "runtime observation is not configured")
	}
	if !req.Observe || req.Scope.Tenant != req.Tenant {
		return nil, contracts.NewError(contracts.ErrForbidden, "runtime observation requires its caller authority")
	}
	remote := req.TaskID
	grant, err := d.store.invocationGrant(ctx, req.Scope, req.InstallationID, remote)
	if err != nil {
		return nil, err
	}
	req.TaskID = grant
	binding, token, err := d.authorizeRuntime(ctx, req, false)
	if err != nil {
		return nil, err
	}
	return d.agentRPC(ctx, binding, token, "GetTask", map[string]any{"id": remote}, remote, invocationContext(req, binding))
}
