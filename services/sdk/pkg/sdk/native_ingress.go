package sdk

import (
	"context"
	"net/http"
	"strings"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// NativeIngressConfig describes the receiving workload, its trusted gateway
// peer and the policy dependency. Authorize must check current grant authority;
// a valid signature alone does not establish that a task remains active.
type NativeIngressConfig struct {
	Tenant, Deployment, GatewayDeployment string
	Audience                              string
	Verifier                              *auth.Verifier
	Authorize                             func(context.Context, auth.Claims) error
}

type nativeCredentialKey struct{}
type nativeCredential struct {
	token     string
	principal transport.Principal
}

// NewNativeIngress guards a custom agent's A2A entry. The TLS server must use
// RequireAndVerifyClientCert with the platform CA. The gateway peer's workload
// and the task JWT's receiving workload are deliberately checked separately.
// Mount peer-only readiness separately; this handler requires a task on every
// request. It never trusts forwarded identity headers or JSON metadata.
func NewNativeIngress(cfg NativeIngressConfig, next http.Handler) (http.Handler, error) {
	target := contracts.Scope{Tenant: cfg.Tenant, Deployment: cfg.Deployment, Subject: "native-ingress"}
	gateway := contracts.Scope{Tenant: cfg.Tenant, Deployment: cfg.GatewayDeployment, Subject: "native-ingress"}
	if target.Validate() != nil || gateway.Validate() != nil || cfg.Audience == "" || strings.TrimSpace(cfg.Audience) != cfg.Audience || len(cfg.Audience) > 256 || cfg.Verifier == nil || cfg.Authorize == nil || next == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "sdk: native ingress requires workload, gateway, verifier, audience, authorizer and handler")
	}
	gatewayID, targetID := auth.WorkloadID(gateway), auth.WorkloadID(target)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		refuse := func(status int) { http.Error(w, "native ingress refused", status) }
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.VerifiedChains[0]) == 0 {
			refuse(http.StatusForbidden)
			return
		}
		leaf := r.TLS.VerifiedChains[0][0]
		if leaf == nil || len(leaf.URIs) != 1 || leaf.URIs[0] == nil || leaf.URIs[0].String() != gatewayID {
			refuse(http.StatusForbidden)
			return
		}
		for _, chain := range r.TLS.VerifiedChains {
			if len(chain) == 0 || chain[0] == nil || !leaf.Equal(chain[0]) {
				refuse(http.StatusForbidden)
				return
			}
		}
		headers := r.Header.Values("Authorization")
		if len(headers) != 1 {
			refuse(http.StatusForbidden)
			return
		}
		token, ok := strings.CutPrefix(headers[0], "Bearer ")
		if !ok || token == "" || len(token) > 32<<10 || strings.ContainsAny(token, " \t\r\n") {
			refuse(http.StatusForbidden)
			return
		}
		claims, err := cfg.Verifier.Verify(token, cfg.Audience, auth.KindTask, targetID)
		if err != nil || claims.Tenant != cfg.Tenant || claims.Deployment != cfg.Deployment {
			refuse(http.StatusForbidden)
			return
		}
		if err := cfg.Authorize(r.Context(), claims); err != nil {
			status := http.StatusForbidden
			if contracts.Code(err) == contracts.ErrUnavailable {
				status = http.StatusServiceUnavailable
			}
			refuse(status)
			return
		}
		principal := transport.Principal{Claims: claims, Scope: claims.Scope()}
		ctx := transport.WithPrincipal(r.Context(), principal)
		ctx = context.WithValue(ctx, nativeCredentialKey{}, nativeCredential{token: token, principal: principal})
		next.ServeHTTP(w, r.WithContext(ctx))
	}), nil
}

// NativeTaskTokenSource supplies the verified credential of this request to a
// shared State client. It cannot fall back to a bootstrap/global task token.
// ClaimTaskRun subsequently binds the newly issued lease credential as usual.
// This source does not renew an expired JWT or restore lost lease ownership.
func NativeTaskTokenSource(ctx context.Context) (string, error) {
	credential, ok := ctx.Value(nativeCredentialKey{}).(nativeCredential)
	principal, present := transport.FromContext(ctx)
	if !ok || !present || credential.token == "" || principal.Claims.Kind != auth.KindTask || principal.Scope != credential.principal.Scope || principal.Claims.Task != credential.principal.Claims.Task || principal.Claims.Context != credential.principal.Claims.Context || principal.Claims.Grant != credential.principal.Claims.Grant {
		return "", contracts.NewError(contracts.ErrForbidden, "sdk: verified native request credential required")
	}
	return credential.token, nil
}
