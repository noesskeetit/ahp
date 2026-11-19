package executor

import (
	"context"

	"github.com/ahp-platform/ahp/internal/gateway"
)

// StartWorker activates the execution plane's durable drain worker.
//
// The durable A2A gateway is accept/execute split: when a task is accepted on
// the public face it is persisted as SUBMITTED, and NOTHING runs it until a
// worker claims it from the store via the fenced claim loop (Service.Run). The
// HTTP face alone therefore never advances a task -- without a running worker a
// correctly authenticated run is stranded at SUBMITTED forever (this was the
// composed role=all defect reported as iv-119).
//
// StartWorker starts that single fenced worker loop for the lifetime of ctx, so
// an accepted task is drained SUBMITTED -> WORKING -> terminal by the injected
// canonical executor (native Qwen + tool round-trip -> sealed MinIO artifact ->
// durable effect ledger). It is the execution role's responsibility to start it
// exactly once per mounted execution plane; the worker leases and fences each
// task, so cancelling ctx (role shutdown) releases held leases and a restart
// resumes without double-running an ambiguous side effect. internal/gateway is
// unchanged: this only drives the gateway's existing, already-accepted Service.
func StartWorker(ctx context.Context, svc *gateway.Service) context.CancelFunc {
	if svc == nil {
		return func() {}
	}
	runCtx, cancel := context.WithCancel(ctx)
	go svc.Run(runCtx)
	return cancel
}
