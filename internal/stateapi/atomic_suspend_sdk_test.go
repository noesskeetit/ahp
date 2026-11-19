package stateapi

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	sdk "github.com/ahp-platform/ahp/sdk/go/state"
)

// Actual Go SDK -> strict HTTP decoder -> workload mTLS/task JWT -> isolated
// PostgreSQL. Identity's authorizer/issuer are fixtures; no LLM is exercised.
func TestAtomicSuspendGoSDKActualMTLSPostgres(t *testing.T) {
	for _, common := range []bool{false, true} {
		for _, early := range []bool{false, true} {
			t.Run(fmt.Sprintf("common=%t/early=%t", common, early), func(t *testing.T) {
				e := rev7APIEnv(t)
				state.WithCommonRecords()(e.store)
				if err := e.store.Migrate(e.ctx); err != nil {
					t.Fatal(err)
				}
				// The generic fixture issuer uses ctx-<task>; this flow admits
				// ctx-1, so the issued claim credential must retain that context.
				issue := func(_ context.Context, scope contracts.Scope, task string) (string, error) {
					return e.signer.Sign(auth.Claims{Audience: testAudience, Kind: auth.KindTask, Tenant: scope.Tenant, Deployment: scope.Deployment, Subject: scope.Subject, Task: task, Root: "run-1", Context: "ctx-1", Grant: "grant-1"}, time.Minute)
				}
				handler, err := New(e.store, e.verifier, func(context.Context, auth.Claims, string) error { return nil }, issue)
				if err != nil {
					t.Fatal(err)
				}
				// No requests have been made to this fresh server yet.
				e.srv.Config.Handler = handler
				token := e.signTask(t, "atomic-sdk-task")
				newClient := func(token string) *sdk.Client {
					t.Helper()
					client, err := sdk.New(sdk.Config{BaseURL: e.srv.URL, HTTPClient: e.srv.Client(), TokenSource: func(context.Context) (string, error) { return token, nil }})
					if err != nil {
						t.Fatal(err)
					}
					return client
				}
				client := newClient(token)
				thread, err := client.CreateThread(e.ctx, "ctx-1", "atomic-sdk-thread")
				if err != nil {
					t.Fatal(err)
				}
				run, err := client.CreateRun(e.ctx, thread.ID, "atomic-sdk-run", json.RawMessage(`{"type":"native"}`))
				if err != nil {
					t.Fatal(err)
				}
				claim, err := client.ClaimRunFor(e.ctx, "atomic-sdk-worker", 30000, run.ID)
				if err != nil || claim == nil {
					t.Fatal("claim", err)
				}
				writer := newClient(claim.TaskToken)
				fence := sdk.Fence{RunID: run.ID, Worker: claim.Lease.Worker, Epoch: claim.Lease.Epoch}
				deadline := time.Now().UTC().Add(time.Minute).Truncate(time.Microsecond)
				op := &sdk.RunStatusOp{Status: sdk.RunWaiting, WaitFor: "hitl_response", Deadline: &deadline}
				if early {
					if _, err := client.Enqueue(e.ctx, thread.ID, "early-answer", "hitl_response", json.RawMessage(`{"approved":true}`)); err != nil {
						t.Fatal(err)
					}
				}
				stream := &sdk.StreamOp{Key: "question", Kind: "native-a2a", Payload: json.RawMessage(`{"question":"continue?"}`)}
				if common {
					req := sdk.CommonCommit{Fence: fence, Namespace: "native", Key: "question", RunStatus: op, Stream: stream, Document: &sdk.CommonDocument{Format: sdk.CommonFormat, Profile: "native-test/1", Root: "task", Records: map[string]sdk.CommonRecord{"task": {Kind: "task", Attributes: json.RawMessage(`{"question":"continue?"}`), Relations: map[string][]string{}}}}}
					first, err := writer.CommitRecords(e.ctx, req)
					if err != nil {
						t.Fatal("atomic common suspend over mTLS", err)
					}
					replay, err := writer.CommitRecords(e.ctx, req)
					if err != nil || replay.CutID != first.CutID {
						t.Fatal("receipt replay after lease release", err)
					}
					snapshot, err := newClient(token).ReadRecords(e.ctx, thread.ID, "native", 0)
					if err != nil || snapshot.Receipt.CutID != first.CutID || len(snapshot.Document.Records) != 1 {
						t.Fatal("fresh client could not restore native question cut", err)
					}
				} else {
					_, err := writer.PutBatch(e.ctx, fence, sdk.BatchRequest{Key: "question", RunStatus: op, Stream: stream, Checkpoint: &sdk.Checkpoint{ID: "question", Namespace: "native", Version: 1, Parts: []sdk.CheckpointPart{{Channel: "question", Version: "1", Data: []byte("continue?")}}}})
					if err != nil {
						t.Fatal("atomic checkpoint suspend over mTLS", err)
					}
					reader := newClient(token)
					snapshot, err := reader.GetCheckpoint(e.ctx, thread.ID, "native", "question")
					if err != nil || len(snapshot.Parts) != 1 || snapshot.Parts[0].Channel != "question" {
						t.Fatal("fresh client could not restore checkpoint question", err)
					}
					data, err := reader.GetPart(e.ctx, thread.ID, "native", snapshot.Parts[0].Channel, snapshot.Parts[0].Version)
					if err != nil || string(data) != "continue?" {
						t.Fatal("fresh client could not restore checkpoint bytes", err)
					}
				}
				stored, err := client.GetRun(e.ctx, run.ID)
				if err != nil {
					t.Fatal(err)
				}
				if early {
					if stored.Status != sdk.RunPending || stored.WaitFor != "" || stored.Deadline != nil {
						t.Fatal("early answer lost on HTTP path", stored)
					}
				} else if stored.Status != sdk.RunWaiting || stored.WaitFor != op.WaitFor || stored.Deadline == nil || !stored.Deadline.Equal(deadline) {
					t.Fatal("HTTP path lost wait target/deadline", stored)
				}
				events, err := client.ReadStream(e.ctx, thread.ID, 0, 10)
				if err != nil || len(events) != 1 || events[0].Key != stream.Key {
					t.Fatal("question stream missing/duplicated", events, err)
				}
				if _, err := writer.PutBatch(e.ctx, fence, sdk.BatchRequest{Key: "old-worker", RunStatus: &sdk.RunStatusOp{Status: sdk.RunRunning}}); contracts.Code(err) != contracts.ErrStaleEpoch {
					t.Fatal("old writer retained authority through HTTP", err)
				}
				if !early {
					if _, err := client.Enqueue(e.ctx, thread.ID, "answer", "hitl_response", json.RawMessage(`{"approved":true}`)); err != nil {
						t.Fatal(err)
					}
				}
				next, err := client.ClaimRunFor(e.ctx, "replacement-worker", 30000, run.ID)
				if err != nil || next == nil || next.Lease.Epoch <= claim.Lease.Epoch {
					t.Fatal("replacement worker could not resume original run", next, err)
				}
			})
		}
	}
}
