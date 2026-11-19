package sdk

import (
	"context"
	"fmt"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// NewA2AHandler binds native task ownership to the verified AHP principal before
// the upstream handler reads a task, appends history, invokes or cancels it.
// The host must verify its gateway mTLS peer and target task JWT, then install
// transport.Principal. Message metadata and headers cannot replace that step.
//
// The default upstream taskstore enforces the resulting authenticated owner.
// A supplied custom taskstore must preserve that ownership contract. This
// constructor does not make the default in-memory taskstore durable.
func NewA2AHandler(executor a2asrv.AgentExecutor, options ...a2asrv.RequestHandlerOption) a2asrv.RequestHandler {
	guarded := make([]a2asrv.RequestHandlerOption, 0, len(options)+1)
	guarded = append(guarded, a2asrv.WithCallInterceptors(nativePrincipalInterceptor{}))
	guarded = append(guarded, options...)
	return a2asrv.NewHandler(executor, guarded...)
}

type nativePrincipalInterceptor struct {
	a2asrv.PassthroughCallInterceptor
}

func (nativePrincipalInterceptor) Before(ctx context.Context, call *a2asrv.CallContext, request *a2asrv.Request) (context.Context, any, error) {
	principal, ok := transport.FromContext(ctx)
	if !ok {
		return ctx, nil, a2a.ErrUnauthenticated
	}
	if principal.Claims.Kind != auth.KindTask || principal.Scope != principal.Claims.Scope() || principal.Scope.Validate() != nil || !validNativeInputID(principal.Claims.Task) || !validNativeInputID(principal.Claims.Context) {
		return ctx, nil, a2a.ErrUnauthorized
	}
	if call.Tenant() != "" && call.Tenant() != principal.Scope.Tenant {
		return ctx, nil, a2a.ErrUnauthorized
	}
	if params, ok := request.Payload.(*a2a.SendMessageRequest); ok {
		message := params.Message
		if message == nil || !validNativeInputID(message.ID) || message.Role != a2a.MessageRoleUser || message.TaskID != "" && !validNativeInputID(string(message.TaskID)) {
			return ctx, nil, fmt.Errorf("%w: native send requires a valid user message", a2a.ErrInvalidParams)
		}
		if message.ContextID != principal.Claims.Context {
			return ctx, nil, a2a.ErrUnauthorized
		}
		for _, part := range message.Parts {
			if part == nil {
				return ctx, nil, fmt.Errorf("%w: native message contains a nil part", a2a.ErrInvalidParams)
			}
		}
	}
	owner := contracts.OperationKey(principal.Scope, principal.Claims.Task, "sdk.native.owner", principal.Claims.Context)
	call.User = a2asrv.NewAuthenticatedUser(owner, nil)
	return ctx, nil, nil
}
