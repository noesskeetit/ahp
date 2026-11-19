//go:build state_integration

package space_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/ahp-platform/ahp/pkg/sdk"
	sdkstate "github.com/ahp-platform/ahp/sdk/go/state"
)

func TestSDKExecutorClaimBindingAndHeartbeatRealState(t *testing.T) {
	e := cNewEnv(t)
	bootstrap := e.signTask(t, "bootstrap")
	client, err := sdkstate.New(sdkstate.Config{BaseURL: e.srv.URL, HTTPClient: e.srv.Client(), TokenSource: func(context.Context) (string, error) { return bootstrap, nil }})
	if err != nil {
		t.Fatal(err)
	}
	oldThread, err := client.CreateThread(e.ctx, "backlog-context", "old")
	if err != nil {
		t.Fatal(err)
	}
	old, err := client.CreateRun(e.ctx, oldThread.ID, "old", json.RawMessage(`{"text":"OLD"}`))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, id := range []string{"first", "second"} {
		id := id
		tok := e.signTask(t, id)
		wg.Add(1)
		go func() {
			defer wg.Done()
			bound := client.WithTokenSource(func(context.Context) (string, error) { return tok, nil })
			cfg := &sdk.Config{StateClient: bound, Worker: "worker-" + id, LeaseTTL: 150 * time.Millisecond, HeartbeatInterval: 15 * time.Millisecond}
			exec, err := sdk.NewLangGraphExecutor(cfg, func(ctx context.Context, rc *sdk.RunContext) (json.RawMessage, error) {
				if rc.Task.TaskID != id || rc.Task.ContextID != "ctx-"+id || rc.Lease == nil {
					return nil, fmt.Errorf("mixed graph context: %+v", rc.Task)
				}
				var payload struct {
					Text string `json:"text"`
				}
				json.Unmarshal(rc.Task.Payload, &payload)
				if payload.Text != id {
					return nil, fmt.Errorf("payload mixed: %s", rc.Task.Payload)
				}
				// Run longer than two lease TTLs: a missing/unauthorized heartbeat makes
				// the real State reject the closing Release as stale.
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(350 * time.Millisecond):
				}
				return json.RawMessage(`{"ok":true}`), nil
			})
			if err != nil {
				errs <- err
				return
			}
			completed := false
			ec := &a2asrv.ExecutorContext{TaskID: a2a.TaskID(id), ContextID: "ctx-" + id, Message: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart(id))}
			for ev, err := range exec.Execute(e.ctx, ec) {
				if err != nil {
					errs <- err
					return
				}
				raw, _ := json.Marshal(ev)
				if strings.Contains(string(raw), "TASK_STATE_COMPLETED") {
					completed = true
				}
				if strings.Contains(string(raw), "TASK_STATE_FAILED") {
					errs <- fmt.Errorf("executor failed: %s", raw)
					return
				}
			}
			if !completed {
				errs <- fmt.Errorf("%s did not durably complete", id)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	stored, err := client.GetRun(e.ctx, old.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != "pending" {
		t.Fatalf("executor consumed unrelated backlog: %+v", stored)
	}
}

func TestExactClaimCannotOvertakeWaitingFIFORealState(t *testing.T) {
	e := cNewEnv(t)
	tok := e.signTask(t, "task")
	client, err := sdkstate.New(sdkstate.Config{BaseURL: e.srv.URL, HTTPClient: e.srv.Client(), TokenSource: func(context.Context) (string, error) { return tok, nil }})
	if err != nil {
		t.Fatal(err)
	}
	thread, err := client.CreateThread(e.ctx, "ctx-1", "fifo")
	if err != nil {
		t.Fatal(err)
	}
	first, err := client.CreateRun(e.ctx, thread.ID, "first", json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	second, err := client.CreateRun(e.ctx, thread.ID, "second", json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if claim, err := client.ClaimRunFor(e.ctx, "worker", 30000, second.ID); err != nil || claim != nil {
		t.Fatalf("second overtook pending head: %+v %v", claim, err)
	}
	claim, err := client.ClaimRunFor(e.ctx, "worker", 30000, first.ID)
	if err != nil || claim == nil {
		t.Fatalf("head claim: %+v %v", claim, err)
	}
	bound := client.WithTokenSource(func(context.Context) (string, error) { return claim.TaskToken, nil })
	if err := bound.Suspend(e.ctx, sdkstate.Fence{RunID: first.ID, Worker: claim.Lease.Worker, Epoch: claim.Lease.Epoch}, "human", nil); err != nil {
		t.Fatal(err)
	}
	if claim, err := client.ClaimRunFor(e.ctx, "worker", 30000, second.ID); err != nil || claim != nil {
		t.Fatalf("second overtook waiting head: %+v %v", claim, err)
	}
}

func TestSDKExecutorCancelAcknowledgesDurableState(t *testing.T) {
	e := cNewEnv(t)
	tok := e.signTask(t, "sdk-cancel")
	client, err := sdkstate.New(sdkstate.Config{BaseURL: e.srv.URL, HTTPClient: e.srv.Client(), TokenSource: func(context.Context) (string, error) { return tok, nil }})
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan string, 1)
	executor, err := sdk.NewLangGraphExecutor(&sdk.Config{StateClient: client, Worker: "cancel-worker", LeaseTTL: time.Second, HeartbeatInterval: 15 * time.Millisecond}, func(ctx context.Context, rc *sdk.RunContext) (json.RawMessage, error) {
		started <- rc.Task.RunID
		<-ctx.Done()
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ec := &a2asrv.ExecutorContext{TaskID: "sdk-cancel", ContextID: "sdk-cancel-context"}
	done := make(chan error, 1)
	go func() {
		canceled := false
		for ev, err := range executor.Execute(e.ctx, ec) {
			if err != nil {
				done <- err
				return
			}
			raw, _ := json.Marshal(ev)
			if strings.Contains(string(raw), "TASK_STATE_CANCELED") {
				canceled = true
			}
			if strings.Contains(string(raw), "TASK_STATE_COMPLETED") || strings.Contains(string(raw), "TASK_STATE_FAILED") {
				done <- fmt.Errorf("unexpected execution terminal event: %s", raw)
				return
			}
		}
		if !canceled {
			done <- fmt.Errorf("missing execution cancellation")
			return
		}
		done <- nil
	}()
	runID := <-started
	canceled := false
	for ev, err := range executor.Cancel(e.ctx, ec) {
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(ev)
		if !strings.Contains(string(raw), "TASK_STATE_CANCELED") {
			continue
		}
		canceled = true
		stored, err := client.GetRun(e.ctx, runID)
		if err != nil || stored.Status != "canceled" {
			t.Fatalf("ack precedes durable cancellation: %+v %v", stored, err)
		}
	}
	if !canceled {
		t.Fatal("missing cancellation acknowledgement")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if claim, err := client.ClaimRunFor(e.ctx, "other-worker", 30000, runID); err != nil || claim != nil {
		t.Fatalf("canceled task became claimable: %+v %v", claim, err)
	}
}
