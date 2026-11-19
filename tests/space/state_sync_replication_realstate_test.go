//go:build state_integration

package space_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	sdkstate "github.com/ahp-platform/ahp/sdk/go/state"
	"github.com/jackc/pgx/v5/pgxpool"
)

// An opt-in, operator-controlled destructive drill on an isolated synchronous
// PostgreSQL pair. The State API, TLS, JWTs, pool and persistence are real;
// Identity policy remains the named cEnv fixture. Same physical Mac only.
// Missing opt-in is a SKIP, not evidence of PDF durability guarantee6.
func TestStateAPIConfirmedCutSurvivesPrimaryLossRealState(t *testing.T) {
	dsn := os.Getenv("AHP_PG_SYNC_DATABASE_URL")
	if dsn == "" {
		t.Skip("set AHP_PG_SYNC_DATABASE_URL and AHP_PG_SYNC_CONTROL_DIR for isolated primary-loss drill")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal("HA database configuration refused")
	}
	defer pool.Close()
	var syncCommit, syncNames string
	var replicas int
	if err := pool.QueryRow(ctx, `SELECT current_setting('synchronous_commit'), current_setting('synchronous_standby_names'), (SELECT count(*) FROM pg_stat_replication WHERE sync_state='sync' AND state='streaming')`).Scan(&syncCommit, &syncNames, &replicas); err != nil {
		t.Fatal("cannot inspect HA replication")
	}
	if syncCommit != "remote_apply" || syncNames == "" || replicas != 1 {
		t.Fatalf("confirmed State writes require remote_apply and one sync replica: commit=%s named=%t sync_replicas=%d", syncCommit, syncNames != "", replicas)
	}
	dir := os.Getenv("AHP_PG_SYNC_CONTROL_DIR")
	if !filepath.IsAbs(dir) {
		t.Fatal("explicit absolute operator handshake directory required")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal("cannot create private operator handshake")
	}
	sequence := 0
	action := func(name string) {
		t.Helper()
		sequence++
		id := fmt.Sprintf("phase-%02d", sequence)
		body, _ := json.Marshal(map[string]string{"id": id, "action": name})
		if os.WriteFile(filepath.Join(dir, "request.tmp"), body, 0600) != nil || os.Rename(filepath.Join(dir, "request.tmp"), filepath.Join(dir, "request.json")) != nil {
			t.Fatal("cannot signal operator phase")
		}
		deadline := time.Now().Add(45 * time.Second)
		for time.Now().Before(deadline) {
			data, err := os.ReadFile(filepath.Join(dir, "response.json"))
			var reply struct{ ID, Action, Status string }
			if err == nil && json.Unmarshal(data, &reply) == nil && reply.ID == id {
				if reply.Action != name || reply.Status != "passed" {
					t.Fatal("operator phase refused: " + name)
				}
				return
			}
			select {
			case <-ctx.Done():
				t.Fatal("operator drill deadline")
			case <-time.After(50 * time.Millisecond):
			}
		}
		t.Fatal("operator phase timeout: " + name)
	}
	t.Setenv("AHP_TEST_DATABASE_URL", dsn)
	e := cNewEnv(t)
	token := e.signTask(t, "ha-cut-task")
	client, err := sdkstate.New(sdkstate.Config{BaseURL: e.srv.URL, HTTPClient: e.srv.Client(), TokenSource: func(context.Context) (string, error) { return token, nil }})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := client.CreateThread(ctx, "ctx-1", "ha-context")
	if err != nil {
		t.Fatal(err)
	}
	run, err := client.CreateRun(ctx, thread.ID, "ha-run", json.RawMessage(`{"operation":"durability-cut"}`))
	if err != nil {
		t.Fatal(err)
	}
	claim, err := client.ClaimRunFor(ctx, "ha-first-worker", 120000, run.ID)
	if err != nil || claim == nil {
		t.Fatalf("cannot claim HA run: %v", err)
	}
	fence := sdkstate.Fence{RunID: run.ID, Worker: claim.Lease.Worker, Epoch: claim.Lease.Epoch}
	bound := client.WithTokenSource(func(context.Context) (string, error) { return claim.TaskToken, nil })
	cp := sdkstate.Checkpoint{ID: "ha-before-loss", Namespace: "ha", Version: 1, Parts: []sdkstate.CheckpointPart{{Channel: "result", Version: "1", Data: []byte("confirmed-checkpoint-before-primary-loss")}}}
	if _, err := bound.PutCheckpoint(ctx, fence, cp, "ha-checkpoint"); err != nil {
		t.Fatal(err)
	}
	before, err := bound.AppendStream(ctx, fence, "ha-stream", "confirmed", map[string]string{"marker": "durable-before-loss"})
	if err != nil {
		t.Fatal(err)
	}
	// Stopping replay must withhold success. A timed-out write may be committed
	// locally: only repeat its identical idempotency key/payload afterwards.
	action("pause-standby")
	limited, stop := context.WithTimeout(ctx, 350*time.Millisecond)
	_, writeErr := bound.PutKV(limited, fence, []string{"ha"}, "unknown-ack", "ha-unknown-ack", 0, json.RawMessage(`{"value":"same-on-retry"}`))
	stop()
	if writeErr == nil {
		t.Fatal("State returned durable ACK while synchronous replica was paused")
	}
	// COMMIT may have reached the primary's WAL even though its synchronous
	// wait was canceled. A receipt replay is still a mutation ACK: it cannot
	// confirm that locally committed result before the standby has received it.
	replayContext, stopReplay := context.WithTimeout(ctx, 350*time.Millisecond)
	_, replayErr := bound.PutKV(replayContext, fence, []string{"ha"}, "unknown-ack", "ha-unknown-ack", 0, json.RawMessage(`{"value":"same-on-retry"}`))
	stopReplay()
	if replayErr == nil {
		t.Fatal("receipt replay returned durable ACK before synchronous standby recovered")
	}
	action("resume-standby")
	var acknowledged sdkstate.KVItem
	for deadline := time.Now().Add(8 * time.Second); time.Now().Before(deadline); {
		acknowledged, err = bound.PutKV(ctx, fence, []string{"ha"}, "unknown-ack", "ha-unknown-ack", 0, json.RawMessage(`{"value":"same-on-retry"}`))
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil || acknowledged.Version != 1 {
		t.Fatalf("identical unknown-ACK replay failed: version=%d error=%v", acknowledged.Version, err)
	}
	// The original writer must be confirmed stopped before promotion. The
	// promoted node remains fail-closed for writes until rejoin restores sync.
	started := time.Now()
	action("crash-and-promote")
	var part []byte
	for deadline := time.Now().Add(12 * time.Second); time.Now().Before(deadline); {
		part, err = bound.GetPart(ctx, thread.ID, "ha", "result", "1")
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil || string(part) != "confirmed-checkpoint-before-primary-loss" {
		t.Fatalf("confirmed checkpoint lost after primary failure: %v", err)
	}
	events, err := bound.ReadStream(ctx, thread.ID, 0, 10)
	if err != nil || len(events) != 1 || events[0].ID != before.ID || events[0].Sequence != before.Sequence {
		t.Fatalf("confirmed stream cut lost: count=%d error=%v", len(events), err)
	}
	item, err := bound.GetKV(ctx, []string{"ha"}, "unknown-ack")
	if err != nil || item.Version != acknowledged.Version || string(item.Value) != string(acknowledged.Value) {
		t.Fatal("acknowledged KV receipt changed after primary failure")
	}
	readRecovery := time.Since(started)
	action("rejoin-old-primary")
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_replication WHERE sync_state='sync' AND state='streaming'`).Scan(&replicas); err != nil || replicas != 1 {
		t.Fatal("synchronous redundancy was not restored")
	}
	if err := bound.Suspend(ctx, fence, "ha-resume", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := bound.Enqueue(ctx, thread.ID, "ha-wake", "ha-resume", json.RawMessage(`{"resume":true}`)); err != nil {
		t.Fatal(err)
	}
	second, err := client.ClaimRunFor(ctx, "ha-second-worker", 120000, run.ID)
	if err != nil || second == nil || second.Lease.Epoch <= fence.Epoch {
		t.Fatal("new lease/fence was not recovered after failover")
	}
	_, err = bound.PutKV(ctx, fence, []string{"ha"}, "zombie", "ha-zombie", 0, json.RawMessage(`{"old_worker":true}`))
	if contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("old epoch wrote after failover/reclaim: %v", err)
	}
	newClient := client.WithTokenSource(func(context.Context) (string, error) { return second.TaskToken, nil })
	newFence := sdkstate.Fence{RunID: run.ID, Worker: second.Lease.Worker, Epoch: second.Lease.Epoch}
	if _, err := newClient.AppendStream(ctx, newFence, "ha-after-loss", "confirmed", map[string]bool{"after_failover": true}); err != nil {
		t.Fatal(err)
	}
	if err := newClient.Release(ctx, newFence, sdkstate.RunDone); err != nil {
		t.Fatal(err)
	}
	t.Logf("State API + mTLS + PG synchronous pair: no durable ACK while standby paused; exact checkpoint/stream/KV after primary kill; read recovery=%s; sync restored, epoch %d->%d, zombie refused, new write done. Same Mac, manual promotion; no host/disk/automatic-HA claim", readRecovery, fence.Epoch, second.Lease.Epoch)
}
