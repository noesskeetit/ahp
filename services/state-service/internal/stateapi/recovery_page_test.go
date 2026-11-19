package stateapi

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestRecoveryPageNeverTreatsVolatileFragmentsAsConfirmed(t *testing.T) {
	for _, mode := range []string{"step", "turn"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv(contracts.EnvCheckpointBoundary, mode)
			t.Setenv(contracts.EnvStreamRetention, "results-only")
			e := newEnv(t)
			scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: testSubject}
			thread, err := e.store.CreateThread(e.ctx, scope, "ctx-recovery", "thread")
			if err != nil {
				t.Fatal(err)
			}
			run, err := e.store.CreateRun(e.ctx, scope, thread.ID, "task-recovery", "run", json.RawMessage(`{}`))
			if err != nil {
				t.Fatal(err)
			}
			lease, err := e.store.ClaimRunFor(e.ctx, scope.Tenant, scope.Deployment, "worker", run.ID, time.Minute)
			if err != nil || lease == nil {
				t.Fatal("claim", err)
			}
			if _, err = e.store.AppendStream(e.ctx, scope, run.ID, lease.Worker, lease.Epoch, "fragment", "model_delta", json.RawMessage(`"volatile"`)); err != nil {
				t.Fatal(err)
			}
			token := e.signTask(t, "task-recovery")
			body := map[string]any{"thread_id": thread.ID, "after": 0, "limit": 128, "max_response_bytes": 16 << 20}
			read := func() map[string]any {
				raw, _ := json.Marshal(body)
				code, page := e.call(t, "ReadStream", token, string(raw))
				mustStatus(t, code, 200, page)
				return page
			}
			live := read()
			if len(live["events"].([]any)) != 1 {
				t.Fatal("UI lost the live fragment", live)
			}
			body["durable_only"] = true
			durable := read()
			if len(durable["events"].([]any)) != 0 || durable["has_more"] != false || durable["next_after"] != float64(0) {
				t.Fatal("volatile fragment became a confirmed recovery cursor", durable)
			}
			if _, err = e.store.AppendStream(e.ctx, scope, run.ID, lease.Worker, lease.Epoch, "terminal", "status", json.RawMessage(`{"status":"done"}`)); err != nil {
				t.Fatal(err)
			}
			if _, err = e.store.FlushStream(e.ctx, scope, run.ID, lease.Worker, lease.Epoch); err != nil {
				t.Fatal(err)
			}
			durable = read()
			rows := durable["events"].([]any)
			if len(rows) != 1 || rows[0].(map[string]any)["kind"] != "status" {
				t.Fatal("durable-only page lost the committed terminal or retained volatile data", durable)
			}
		})
	}
}
