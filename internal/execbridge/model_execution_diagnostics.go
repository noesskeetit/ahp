package execbridge

import (
	"context"
	"log/slog"

	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// runObservedModelExecution records failures in the owner, even if its reader left.
// The buffered error channel still receives the original error for the same wire response.
func runObservedModelExecution(ctx context.Context, scope contracts.Scope, id, streamAttempt string,
	execute func(context.Context, contracts.Scope, string) (model.Generation, error), failures chan<- error) {
	_, err := execute(ctx, scope, id)
	if err == nil {
		return
	}
	attrs := model.InspectExecutionError(ctx, "execute", "owner", err).LogAttrs()
	owner := model.InspectExecutionError(ctx, "owner", "owner", nil)
	attrs = append(attrs, slog.String("generation_id", id), slog.String("stream_attempt_id", streamAttempt),
		slog.String("tenant", scope.Tenant), slog.String("subject", scope.Subject), slog.String("deployment", scope.Deployment),
		slog.String("owner_ctx_state", owner.ContextState), slog.Bool("owner_has_deadline", owner.HasDeadline),
		slog.Int64("owner_deadline_unix_ms", owner.DeadlineUnixMS), slog.Int64("owner_deadline_remaining_ms", owner.DeadlineRemainingMS))
	slog.LogAttrs(ctx, slog.LevelError, "model.execute_failed", attrs...)
	failures <- err
}
