package stateapi

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// putbatch_run_status_test.go — DEF-BATCH-1.
//
// The finding (Heisenberg, 2026-11-09, high): the batch is an ATOMIC package of
// thread operations (PDF rev3.2 §2.5: "снимок, запись в поток транскрипта и смена
// статуса run"), and the status half of it was a side door around the plane's own
// explanation contract. Release refuses to end a run without a reason, but a run
// closed by PutBatch{run_status:"failed"} lands terminal with no close_reason, no
// failure_cause and no status frame - a `failed` NOBODY explained, which is exactly
// the shape the live journey's j10 leg could only report as "durable transcript
// carries no cause frame". A layer that ends a run is a layer that owes the reason
// for ending it; the dictionary and the two journal keys exist precisely so an
// unexplained terminal cannot be produced, and the batch must not produce one either.
//
// The vocabulary guard (pending/running/done/failed/canceled) already stood here and
// stays; what this file pins is the OTHER half of the release contract on the batch
// path: terminal only with a stated reason, journaled as durably as Release journals
// it, under the same fence and the same idempotent replay - while a non-terminal
// transition stays exactly as reasonless and cheap as it was.

// leaseFixture creates one claimed run and returns the token, thread, run, fence and
// the env (the env is the caller's, so every later call rides the same mTLS identity).
func leaseFixture(t *testing.T, payload string) (tok, threadID, runID string, fence map[string]any, e *env) {
	t.Helper()
	e = newEnv(t)
	task := "task-" + contracts.NewID()
	tok = e.signTask(t, task)
	status, th := e.call(t, "CreateThread", tok,
		`{"context_id":"ctx-`+contracts.NewID()+`","key":"k-`+contracts.NewID()+`"}`)
	mustStatus(t, status, http.StatusOK, th)
	threadID, _ = th["id"].(string)
	status, run := e.call(t, "CreateRun", tok, `{"thread_id":"`+threadID+
		`","key":"k-`+contracts.NewID()+`","payload":`+payload+`}`)
	mustStatus(t, status, http.StatusOK, run)
	runID, _ = run["id"].(string)
	// Claim is workload-only: no bearer (same shape the happy path pins).
	status, claimed := e.call(t, "ClaimRun", "", `{"run_id":"`+runID+`","worker":"w1","ttl_ms":60000}`)
	mustStatus(t, status, http.StatusOK, claimed)
	lease, _ := claimed["lease"].(map[string]any)
	if lease == nil {
		t.Fatalf("no lease: %v", claimed)
	}
	fence = map[string]any{"run_id": runID, "worker": lease["worker"], "epoch": lease["epoch"]}
	if st, body := e.call(t, "GetRun", tok, `{"run_id":"`+runID+`"}`); st != http.StatusOK {
		t.Fatalf("fixture: GetRun right after claim = %d: %v", st, body)
	}
	return
}

// TestBatchCannotEndARunWithoutAnExplanation is the DEF-BATCH-1 repro. Pre-fix this is
// the RED: the batch returns 200, the run lands `failed`, and NOTHING in the durable
// record says why - no close_reason, no failure_cause, no frame. Post-fix the same
// call must be refused (INVALID): a terminal transition through the batch owes a
// reason exactly like Release.
func TestBatchCannotEndARunWithoutAnExplanation(t *testing.T) {
	tok, _, runID, fence, e := leaseFixture(t, `{"type":"turn","input":"write a haiku","max_steps":4}`)

	fenceJSON, _ := json.Marshal(fence)
	status, body := e.call(t, "PutBatch", tok, `{"fence":`+string(fenceJSON)+
		`,"batch":{"key":"k-`+contracts.NewID()+`","run_status":{"status":"failed"}}}`)
	if status != http.StatusOK {
		// GREEN shape: the plane refuses the unexplained terminal. The run must then
		// still be alive (the refusal changed nothing).
		if status != http.StatusBadRequest {
			t.Fatalf("refusal of an unexplained terminal must be INVALID(400), got %d: %v", status, body)
		}
		_, got := e.call(t, "GetRun", tok, `{"run_id":"`+runID+`"}`)
		if got["status"] == "failed" {
			t.Fatalf("the refused batch still ended the run: %v", got)
		}
		return
	}

	// Pre-fix: prove the exact durable fingerprint of the side door - a dead run and
	// zero explanation anywhere in the record a reader can reach.
	_, got := e.call(t, "GetRun", tok, `{"run_id":"`+runID+`"}`)
	if got["status"] != "failed" {
		t.Fatalf("repro setup: status = %v, want failed", got["status"])
	}
	var payload map[string]any
	if m, ok := got["payload"].(map[string]any); ok {
		payload = m
	} else if raw, ok := got["payload"].(string); ok {
		_ = json.Unmarshal([]byte(raw), &payload)
	}
	if payload == nil {
		t.Fatalf("repro setup: no payload readable on %v", got)
	}
	_, hasReason := payload["close_reason"]
	_, hasCause := payload["failure_cause"]
	if !hasReason && !hasCause {
		t.Fatalf("DEF-BATCH-1: the batch drove the run to terminal failed with NO close_reason and NO "+
			"failure_cause in the run-row (payload %v) - an unexplained dead run is producible by the "+
			"state layer itself", payload)
	}
}

// TestBatchTerminalWithCauseIsDurableAndDictionaryBound pins the GREEN shape: a batch
// that states the reason ends the run and journals it exactly where Release journals it
// (separate close_reason and failure_cause keys inside the existing payload), and a
// status outside the dictionary stays refused on this path too.
func TestBatchTerminalWithCauseIsDurableAndDictionaryBound(t *testing.T) {
	tok, _, runID, fence, e := leaseFixture(t, `{"type":"turn","input":"write a haiku","max_steps":4}`)
	fenceJSON, _ := json.Marshal(fence)
	const cause = "space: batch writer ended this run"

	// Dictionary bound stays: an invented status is refused on the batch path too.
	status, body := e.call(t, "PutBatch", tok, `{"fence":`+string(fenceJSON)+
		`,"batch":{"key":"k-`+contracts.NewID()+`","run_status":{"status":"exploded"}}}`)
	if status == http.StatusOK {
		t.Fatalf("an invented status on the batch path must be refused, got 200: %v", body)
	}

	status, body = e.call(t, "PutBatch", tok, `{"fence":`+string(fenceJSON)+
		`,"batch":{"key":"k-`+contracts.NewID()+`","run_status":{"status":"failed","close_reason":"error","failure_cause":"`+cause+`"}}}`)
	if status != http.StatusOK {
		t.Fatalf("a batch terminal WITH a reason must be accepted, got %d: %v", status, body)
	}

	_, got := e.call(t, "GetRun", tok, `{"run_id":"`+runID+`"}`)
	if got["status"] != "failed" {
		t.Fatalf("status = %v, want failed", got["status"])
	}
	var payload map[string]any
	if m, ok := got["payload"].(map[string]any); ok {
		payload = m
	} else if raw, ok := got["payload"].(string); ok {
		_ = json.Unmarshal([]byte(raw), &payload)
	}
	if payload["close_reason"] != "error" {
		t.Fatalf("close_reason not journaled by the batch: %v", payload)
	}
	if payload["failure_cause"] != cause {
		t.Fatalf("failure_cause not journaled by the batch: %v", payload)
	}
	// The run's own payload keys survive the journal write, byte for byte in meaning.
	if payload["type"] != "turn" || payload["input"] != "write a haiku" {
		t.Fatalf("the run's own payload keys were not preserved: %v", payload)
	}
}

// TestBatchNonTerminalStatusStaysReasonless pins what must NOT tighten: a batch that
// parks the run back to pending (or keeps it running) states no verdict and must keep
// working exactly as before - a reason belongs to an END, not to a mid-flight update.
func TestBatchNonTerminalStatusStaysReasonless(t *testing.T) {
	tok, _, runID, fence, e := leaseFixture(t, `{"type":"turn","input":"keep going","max_steps":4}`)
	fenceJSON, _ := json.Marshal(fence)

	// running -> back to pending through the batch: no reason, no refusal.
	status, body := e.call(t, "PutBatch", tok, `{"fence":`+string(fenceJSON)+
		`,"batch":{"key":"k-`+contracts.NewID()+`","run_status":{"status":"pending"}}}`)
	if status != http.StatusOK {
		t.Fatalf("a non-terminal batch status must stay reasonless-accepted, got %d: %v", status, body)
	}
	_, got := e.call(t, "GetRun", tok, `{"run_id":"`+runID+`"}`)
	if got["status"] != "pending" {
		t.Fatalf("status = %v, want pending", got["status"])
	}
	var payload map[string]any
	if m, ok := got["payload"].(map[string]any); ok {
		payload = m
	} else if raw, ok := got["payload"].(string); ok {
		_ = json.Unmarshal([]byte(raw), &payload)
	}
	if _, present := payload["close_reason"]; present {
		t.Fatalf("a reasonless non-terminal update wrote a verdict: %v", payload)
	}
	if _, present := payload["failure_cause"]; present {
		t.Fatalf("a reasonless non-terminal update wrote a cause: %v", payload)
	}
}
