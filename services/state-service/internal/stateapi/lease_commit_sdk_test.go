package stateapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	host "github.com/ahp-platform/ahp/pkg/sdk"
	sdk "github.com/ahp-platform/ahp/sdk/go/state"
)

type leaseCommitter interface {
	CommitAndEnd(context.Context, sdk.CommonCommit) (sdk.CommonReceipt, error)
}

// This tests the host lifecycle, not merely State's atomic commit. Losing the
// successful HTTP reply must leave execution stopped and permit the SAME commit
// to recover its receipt, with no second stream event or new lease authority.
func TestActiveLeaseAtomicCommitActualMTLSPostgres(t *testing.T) {
	if _, ok := any((*host.ActiveLease)(nil)).(leaseCommitter); !ok {
		t.Fatal("ActiveLease cannot atomically publish state and end its heartbeat/lease")
	}
	for _, status := range []string{sdk.RunWaiting, sdk.RunDone, sdk.RunFailed, sdk.RunCanceled} {
		t.Run(status, func(t *testing.T) {
			e := rev7APIEnv(t)
			state.WithCommonRecords()(e.store)
			if err := e.store.Migrate(e.ctx); err != nil {
				t.Fatal(err)
			}
			issue := func(_ context.Context, scope contracts.Scope, task string) (string, error) {
				return e.signer.Sign(auth.Claims{Audience: testAudience, Kind: auth.KindTask, Tenant: scope.Tenant, Deployment: scope.Deployment, Subject: scope.Subject, Task: task, Root: "run-1", Context: "ctx-1", Grant: "grant-1"}, time.Minute)
			}
			handler, err := New(e.store, e.verifier, func(context.Context, auth.Claims, string) error { return nil }, issue)
			if err != nil {
				t.Fatal(err)
			}
			var commits, heartbeats atomic.Int32
			// Execute the real HTTP handler against PostgreSQL, then lose exactly
			// its first committed reply. No backend operation is mocked here.
			e.srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/Heartbeat") {
					heartbeats.Add(1)
				}
				if strings.HasSuffix(r.URL.Path, "/CommitRecords") && commits.Add(1) == 1 {
					recorded := httptest.NewRecorder()
					handler.ServeHTTP(recorded, r)
					if recorded.Code != http.StatusOK {
						t.Errorf("commit before lost reply: %d %s", recorded.Code, recorded.Body.String())
					}
					connection, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					_ = connection.Close()
					return
				}
				handler.ServeHTTP(w, r)
			})
			token := e.signTask(t, "host-atomic-task")
			client, err := sdk.New(sdk.Config{BaseURL: e.srv.URL, HTTPClient: e.srv.Client(), TokenSource: func(context.Context) (string, error) { return token, nil }})
			if err != nil {
				t.Fatal(err)
			}
			thread, err := client.CreateThread(e.ctx, "ctx-1", "host-atomic-thread")
			if err != nil {
				t.Fatal(err)
			}
			run, err := client.CreateRun(e.ctx, thread.ID, "host-atomic-run", json.RawMessage(`{"type":"native"}`))
			if err != nil {
				t.Fatal(err)
			}
			cfg := &host.Config{StateClient: client, Worker: "atomic-host", LeaseTTL: 30 * time.Second, HeartbeatInterval: 10 * time.Millisecond}
			lease, err := host.ClaimTaskRun(e.ctx, cfg, run.ID)
			if err != nil || lease == nil {
				t.Fatal("claim", err)
			}
			finish := any(lease).(leaseCommitter)
			// Cleanup also protects the test's early assertion-failure paths.
			t.Cleanup(func() { _ = lease.Release(context.Background(), sdk.RunDone) })
			deadline := time.After(2 * time.Second)
			for heartbeats.Load() == 0 {
				select {
				case <-deadline:
					t.Fatal("host heartbeat never started")
				case <-time.After(time.Millisecond):
				}
			}
			op := &sdk.RunStatusOp{Status: status}
			switch status {
			case sdk.RunWaiting:
				op.WaitFor = "hitl_response"
			case sdk.RunDone:
				// Atomic State transitions require a reason even for done;
				// the older standalone Release door has a different policy.
				op.CloseReason = "graph_completed"
			case sdk.RunFailed:
				op.CloseReason = "error"
			case sdk.RunCanceled:
				op.CloseReason = "canceled"
			}
			req := sdk.CommonCommit{Namespace: "native", Key: "host-final", RunStatus: op, Stream: &sdk.StreamOp{Key: "host-final", Kind: "native-a2a", Payload: json.RawMessage(`{"question":"continue?"}`)}, Document: &sdk.CommonDocument{Format: sdk.CommonFormat, Profile: "native-test/1", Root: "task", Records: map[string]sdk.CommonRecord{"task": {Kind: "task", Attributes: json.RawMessage(`{"question":"continue?"}`), Relations: map[string][]string{}}}}}
			if _, err := finish.CommitAndEnd(e.ctx, req); err == nil {
				t.Fatal("lost reply incorrectly reported as confirmed commit")
			}
			if lease.Context().Err() == nil {
				t.Fatal("execution remained active after final commit attempt")
			}
			beatsAtEnd := heartbeats.Load()
			// A fresh read, independent of the lost response, proves commit happened.
			stored, err := client.GetRun(e.ctx, run.ID)
			if err != nil || stored.Status != status {
				t.Fatal("real commit did not persist run transition", stored, err)
			}
			snapshot, err := client.ReadRecords(e.ctx, thread.ID, "native", 0)
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := finish.CommitAndEnd(e.ctx, req)
			if err != nil || receipt.CutID != snapshot.Receipt.CutID || receipt.Revision != 1 {
				t.Fatal("unknown result did not recover original receipt", receipt, err)
			}
			if commits.Load() != 2 {
				t.Fatal("unexpected retry/request count", commits.Load())
			}
			if heartbeats.Load() != beatsAtEnd || lease.IsLost() {
				t.Fatal("heartbeat continued after the final commit or falsely marked lease lost")
			}
			events, err := client.ReadStream(e.ctx, thread.ID, 0, 10)
			if err != nil || len(events) != 1 {
				t.Fatal("lost reply replay duplicated stream", events, err)
			}
			req.Key = "old-worker-new-write"
			if _, err := finish.CommitAndEnd(e.ctx, req); contracts.Code(err) != contracts.ErrStaleEpoch {
				t.Fatal("ended lease regained write authority", err)
			}
		})
	}
}
