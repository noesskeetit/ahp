package stateapi

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestClaimPayloadTypeKeepsOtherRunsAndFIFO(t *testing.T) {
	for _, sameThread := range []bool{false, true} {
		t.Run(map[bool]string{false: "separate_threads", true: "same_thread"}[sameThread], func(t *testing.T) {
			e := newEnv(t)
			scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: testSubject}
			th, err := e.store.CreateThread(e.ctx, scope, "generic-context", "generic-thread")
			if err != nil {
				t.Fatal(err)
			}
			generic, err := e.store.CreateRun(e.ctx, scope, th.ID, "generic-task", "generic-run", json.RawMessage(`{"type":"maintenance"}`))
			if err != nil {
				t.Fatal(err)
			}
			turnThread := th
			if !sameThread {
				turnThread, err = e.store.CreateThread(e.ctx, scope, "turn-context", "turn-thread")
				if err != nil {
					t.Fatal(err)
				}
			}
			turn, err := e.store.CreateRun(e.ctx, scope, turnThread.ID, "turn-task", "turn-run", json.RawMessage(`{"type":"turn"}`))
			if err != nil {
				t.Fatal(err)
			}
			status, claimed := e.call(t, "ClaimRun", "", `{"worker":"dot-worker","ttl_ms":60000,"payload_type":"turn"}`)
			mustStatus(t, status, http.StatusOK, claimed)
			if sameThread {
				if claimed["lease"] != nil {
					t.Fatal("type filter skipped the earlier unfinished run", claimed)
				}
			} else {
				lease, ok := claimed["lease"].(map[string]any)
				if !ok {
					t.Fatal("turn was not claimable", claimed)
				}
				if run, ok := lease["run"].(map[string]any); !ok || run["id"] != turn.ID {
					t.Fatal("worker claimed a different payload type", claimed)
				}
			}
			// A generic SDK still owns its exact run, under the original API.
			lease, err := e.store.ClaimRunFor(e.ctx, e.tenant, e.deployment, "sdk-worker", generic.ID, time.Minute)
			if err != nil || lease == nil || lease.Run.ID != generic.ID {
				t.Fatalf("generic run consumed by another worker: lease=%v err=%v", lease, err)
			}
			if err := e.store.ReleaseWithReason(e.ctx, scope, generic.ID, lease.Worker, lease.Epoch, "done", "caller_finished"); err != nil {
				t.Fatal(err)
			}
			if sameThread {
				status, claimed = e.call(t, "ClaimRun", "", `{"worker":"dot-worker","ttl_ms":60000,"payload_type":"turn"}`)
				mustStatus(t, status, http.StatusOK, claimed)
				if claimed["lease"] == nil {
					t.Fatal("turn remained blocked after FIFO predecessor completed")
				}
			}
		})
	}
}

// TestHappyPath walks the full core lifecycle over real mTLS with signed
// Ed25519 task tokens: create/get thread, create/get/list runs, claim with a
// real issued task token, heartbeat, suspend, release.
func TestHappyPath(t *testing.T) {
	e := newEnv(t)
	task := "task-" + contracts.NewID()
	tok := e.signTask(t, task)

	status, th := e.call(t, "CreateThread", tok,
		`{"context_id":"ctx-`+contracts.NewID()+`","key":"k-`+contracts.NewID()+`"}`)
	mustStatus(t, status, http.StatusOK, th)
	threadID, _ := th["id"].(string)
	if threadID == "" {
		t.Fatalf("no thread id: %v", th)
	}
	if got := th["scope"].(map[string]any); got == nil || got["tenant"] != e.tenant ||
		got["deployment"] != e.deployment || got["subject"] != testSubject {
		t.Fatalf("scope not derived from identity: %v", th["scope"])
	}

	status, got := e.call(t, "GetThread", tok, `{"thread_id":"`+threadID+`"}`)
	mustStatus(t, status, http.StatusOK, got)
	if got["id"] != threadID {
		t.Fatalf("GetThread returned %v", got)
	}

	status, got = e.call(t, "GetThreadByContext", tok, `{"context_id":"`+th["context_id"].(string)+`"}`)
	mustStatus(t, status, http.StatusOK, got)
	if got["id"] != threadID {
		t.Fatalf("GetThreadByContext returned %v", got)
	}

	// task_id omitted: bound to Claims.Task.
	status, run := e.call(t, "CreateRun", tok, `{"thread_id":"`+threadID+
		`","key":"k-`+contracts.NewID()+`","payload":{"a":1}}`)
	mustStatus(t, status, http.StatusOK, run)
	runID, _ := run["id"].(string)
	if run["task_id"] != task {
		t.Fatalf("run task_id = %v, want %v", run["task_id"], task)
	}

	// task_id mismatching the presenting token is denied.
	status, body := e.call(t, "CreateRun", tok, `{"thread_id":"`+threadID+
		`","key":"k-`+contracts.NewID()+`","payload":{"a":2},"task_id":"other-task"}`)
	mustStatus(t, status, http.StatusForbidden, body)

	status, got = e.call(t, "GetRun", tok, `{"run_id":"`+runID+`"}`)
	mustStatus(t, status, http.StatusOK, got)
	if got["id"] != runID {
		t.Fatalf("GetRun returned %v", got)
	}

	status, listed := e.call(t, "ListRuns", tok, `{"thread_id":"`+threadID+`"}`)
	mustStatus(t, status, http.StatusOK, listed)
	if runs, ok := listed["runs"].([]any); !ok || len(runs) != 1 {
		t.Fatalf("ListRuns = %v", listed)
	}

	// Claim is workload-only: no bearer token at all.
	status, claimed := e.call(t, "ClaimRun", "", `{"worker":"w1","ttl_ms":60000}`)
	mustStatus(t, status, http.StatusOK, claimed)
	leaseRaw, ok := claimed["lease"].(map[string]any)
	if !ok || leaseRaw == nil {
		t.Fatalf("expected a granted lease: %v", claimed)
	}
	leaseTok, _ := claimed["task_token"].(string)
	if leaseTok == "" {
		t.Fatalf("claim returned no task token: %v", claimed)
	}
	// The issued token must be a real signed task token accepted by the
	// verifier and bound to the lease scope.
	claims, err := e.verifier.Verify(leaseTok, "ahp", "task",
		"spiffe://ahp/tenant/"+e.tenant+"/deployment/"+e.deployment)
	if err != nil {
		t.Fatalf("issued task token rejected: %v", err)
	}
	if claims.Task != task {
		t.Fatalf("issued token task = %q, want %q", claims.Task, task)
	}
	leaseScope, _ := leaseRaw["scope"].(map[string]any)
	if leaseScope["tenant"] != e.tenant || leaseScope["subject"] != testSubject {
		t.Fatalf("lease scope = %v", leaseScope)
	}
	epoch := int64(leaseRaw["epoch"].(float64))

	// Heartbeat with the renewed lease token.
	status, hb := e.call(t, "Heartbeat", leaseTok,
		`{"run_id":"`+runID+`","worker":"w1","epoch":`+itoa(epoch)+`,"ttl_ms":60000}`)
	mustStatus(t, status, http.StatusOK, hb)
	if _, ok := hb["expires_at"].(string); !ok {
		t.Fatalf("heartbeat response = %v", hb)
	}

	// Suspend parks the run; it must not be claimable while waiting.
	past := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	suspendBody := `{"run_id":"` + runID + `","worker":"w1","epoch":` + itoa(epoch) +
		`,"wait_for":"inbox-1","deadline":"` + past + `"}`
	status, body = e.call(t, "Suspend", leaseTok, suspendBody)
	mustStatus(t, status, http.StatusOK, body)
	if body["ok"] != true {
		t.Fatalf("suspend body = %v", body)
	}

	// The same suspend with a stale epoch changes nothing.
	status, body = e.call(t, "Suspend", leaseTok, suspendBody)
	mustStatus(t, status, http.StatusConflict, body)

	// The waiting deadline already passed in database time, so the run is
	// claimable again with a grown epoch and a fresh token.
	status, claimed = e.call(t, "ClaimRun", "", `{"worker":"w2"}`)
	mustStatus(t, status, http.StatusOK, claimed)
	leaseRaw, _ = claimed["lease"].(map[string]any)
	if leaseRaw == nil {
		t.Fatalf("run with passed deadline not claimable: %v", claimed)
	}
	leaseTok, _ = claimed["task_token"].(string)
	newEpoch := int64(leaseRaw["epoch"].(float64))
	if newEpoch <= epoch {
		t.Fatalf("epoch did not grow: %d -> %d", epoch, newEpoch)
	}

	status, body = e.call(t, "Release", leaseTok, `{"run_id":"`+runID+`","worker":"w2","epoch":`+
		itoa(newEpoch)+`,"status":"done"}`)
	mustStatus(t, status, http.StatusOK, body)

	status, got = e.call(t, "GetRun", leaseTok, `{"run_id":"`+runID+`"}`)
	mustStatus(t, status, http.StatusOK, got)
	if got["status"] != "done" {
		t.Fatalf("run status = %v, want done", got["status"])
	}
}

func itoa(v int64) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}
