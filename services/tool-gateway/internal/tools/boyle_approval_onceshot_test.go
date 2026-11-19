package tools

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestApprovalOneShotSameScopeNewKey: a human signature funds EXACTLY ONE
// effect. A second operation with a new idempotency key inside the same scope
// must be refused and must not run the effect.
func TestApprovalOneShotSameScopeNewKey(t *testing.T) {
	e, ctx := newTestService(t)
	spec := e.svc.cfg.Tools["write"]
	spec.RequiresApproval = true
	e.svc.cfg.Tools["write"] = spec
	args := json.RawMessage(`{"amount":1,"recipient":"once-sink"}`)
	digest, err := ApprovalDigest("write", args)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.svc.Call(ctx, e.scope, "op-1", "write", args); !errors.Is(err, ErrApprovalRequired) {
		t.Fatalf("pre-approval call: err=%v, want ErrApprovalRequired", err)
	}
	if _, err := e.svc.Approve(ctx, e.scope, digest, "write", e.humanTok); err != nil {
		t.Fatalf("approve: %v", err)
	}
	before := atomic.LoadInt32(e.effCount)
	if op, err := e.svc.Call(ctx, e.scope, "op-1", "write", args); err != nil || op.Status != "SUCCEEDED" {
		t.Fatalf("approved call: op=%+v err=%v, want SUCCEEDED", op, err)
	}
	if got := atomic.LoadInt32(e.effCount); got != before+1 {
		t.Fatalf("approved effects=%d, want %d", got, before+1)
	}
	// New key, same tenant/subject/deployment/digest: refused, no second effect.
	if _, err := e.svc.Call(ctx, e.scope, "op-2", "write", args); err == nil {
		t.Fatalf("new-key reuse of a consumed approval must be refused; effects=%d", atomic.LoadInt32(e.effCount))
	}
	if _, err := e.svc.Call(ctx, e.scope, "op-1", "write", args); err != nil {
		t.Fatalf("same-key replay must return the sealed terminal record: %v", err)
	}
	if got := atomic.LoadInt32(e.effCount); got != before+1 {
		t.Fatalf("one-shot violated: effects=%d, want %d", got, before+1)
	}
}

// TestApprovalForeignScopeRefused: within one tenant, another subject AND
// deployment cannot spend an approval bound to the original operation.
func TestApprovalForeignScopeRefused(t *testing.T) {
	e, ctx := newTestService(t)
	spec := e.svc.cfg.Tools["write"]
	spec.RequiresApproval = true
	e.svc.cfg.Tools["write"] = spec
	args := json.RawMessage(`{"amount":2,"recipient":"scope-sink"}`)
	digest, _ := ApprovalDigest("write", args)
	if _, err := e.svc.Call(ctx, e.scope, "op-s", "write", args); !errors.Is(err, ErrApprovalRequired) {
		t.Fatalf("pre-approval: %v", err)
	}
	if _, err := e.svc.Approve(ctx, e.scope, digest, "write", e.humanTok); err != nil {
		t.Fatal(err)
	}
	before := atomic.LoadInt32(e.effCount)
	foreign := e.scope
	foreign.Subject = "another-subject"
	foreign.Deployment = "another-deployment"
	if _, err := e.svc.Call(ctx, foreign, "op-f", "write", args); err == nil {
		t.Fatalf("foreign scope consumed a bound approval; effects=%d", atomic.LoadInt32(e.effCount))
	}
	if _, err := e.svc.Call(ctx, e.scope, "op-s", "write", args); err != nil {
		t.Fatalf("legitimate call: %v", err)
	}
	if got := atomic.LoadInt32(e.effCount); got != before+1 {
		t.Fatalf("effects=%d, want %d (foreign scope must not execute)", got, before+1)
	}
}

// TestApprovalTTLExpiryRefused: an expired approval is refused at the gate with
// an explicit reason and cannot be consumed.
func TestApprovalTTLExpiryRefused(t *testing.T) {
	e, ctx := newTestService(t)
	spec := e.svc.cfg.Tools["write"]
	spec.RequiresApproval = true
	e.svc.cfg.Tools["write"] = spec
	args := json.RawMessage(`{"amount":3,"recipient":"ttl-sink"}`)
	digest, _ := ApprovalDigest("write", args)
	if _, err := e.svc.Call(ctx, e.scope, "op-t", "write", args); !errors.Is(err, ErrApprovalRequired) {
		t.Fatalf("pre-approval: %v", err)
	}
	if _, err := e.svc.Approve(ctx, e.scope, digest, "write", e.humanTok); err != nil {
		t.Fatal(err)
	}
	// Age the row past its TTL (a clock would do the same; the check is on the
	// consume path, not a janitor).
	if _, err := e.svc.pool.Exec(ctx, "UPDATE "+e.schema+".approvals SET expires_at = now() - interval '1 second'"); err != nil {
		t.Fatal(err)
	}
	before := atomic.LoadInt32(e.effCount)
	_, err := e.svc.Call(ctx, e.scope, "op-t", "write", args)
	if !errors.Is(err, ErrApprovalRequired) {
		t.Fatalf("expired approval call: err=%v, want ErrApprovalRequired", err)
	}
	if !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expired refusal must name the reason: %v", err)
	}
	if cerr := e.svc.TryConsumeApproval(ctx, e.scope, "op-t", digest, "write"); !errors.Is(cerr, ErrApprovalSpent) ||
		!strings.Contains(cerr.Error(), "expired") {
		t.Fatalf("TryConApproval on expired row: %v, want ErrApprovalSpent(expired)", cerr)
	}
	if got := atomic.LoadInt32(e.effCount); got != before {
		t.Fatalf("expired approval produced an effect: %d->%d", before, got)
	}
}

// TestApprovalConcurrentSingleEffect: the atomic consume lets exactly ONE
// concurrent attempt spend an approval, and a concurrent Call storm with one
// approval produces exactly one effect.
func TestApprovalConcurrentSingleEffect(t *testing.T) {
	e, ctx := newTestService(t)
	spec := e.svc.cfg.Tools["write"]
	spec.RequiresApproval = true
	e.svc.cfg.Tools["write"] = spec
	args := json.RawMessage(`{"amount":4,"recipient":"race-sink"}`)
	digest, _ := ApprovalDigest("write", args)
	if _, err := e.svc.Call(ctx, e.scope, "op-c", "write", args); !errors.Is(err, ErrApprovalRequired) {
		t.Fatalf("pre-approval: %v", err)
	}
	if _, err := e.svc.Approve(ctx, e.scope, digest, "write", e.humanTok); err != nil {
		t.Fatal(err)
	}

	// Primitive level: N concurrent consumes of one row -> exactly one wins.
	const n = 10
	var won int32
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := e.svc.TryConsumeApproval(ctx, e.scope, "op-c", digest, "write"); err == nil {
				atomic.AddInt32(&won, 1)
			}
		}()
	}
	wg.Wait()
	if got := atomic.LoadInt32(&won); got != 1 {
		t.Fatalf("concurrent consumes won=%d, want exactly 1", got)
	}

	// Call level: a storm on the same operation key yields one effect total
	// (the consume race is already resolved; the ledger pins the key).
	before := atomic.LoadInt32(e.effCount)
	var ok int32
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			if _, err := e.svc.Call(cctx, e.scope, "op-c", "write", args); err == nil {
				atomic.AddInt32(&ok, 1)
			}
		}()
	}
	wg.Wait()
	if got := atomic.LoadInt32(e.effCount); got != before {
		t.Fatalf("consumed approval executed again under concurrency: %d->%d", before, got)
	}
	_ = ok
}

// TestApprovalLegitSingleFlowStaysGreen: the legitimate human flow (refusal ->
// register -> approve -> exactly one execution -> sealed replay) stays green;
// no checks are weakened.
func TestApprovalLegitSingleFlowStaysGreen(t *testing.T) {
	e, ctx := newTestService(t)
	args := json.RawMessage(`{"v":"legit"}`)
	dig, _ := ApprovalDigest("gated", args)
	if _, err := e.svc.Call(ctx, e.scope, "k-legit", "gated", args); !errors.Is(err, ErrApprovalRequired) {
		t.Fatalf("first call: %v", err)
	}
	if _, err := e.svc.Approve(ctx, e.scope, dig, "gated", e.humanTok); err != nil {
		t.Fatalf("approve: %v", err)
	}
	if op, err := e.svc.Call(ctx, e.scope, "k-legit", "gated", args); err != nil || op.Status != "SUCCEEDED" {
		t.Fatalf("approved call: op=%+v err=%v", op, err)
	}
	if op, err := e.svc.Call(ctx, e.scope, "k-legit", "gated", args); err != nil || op.Status != "SUCCEEDED" {
		t.Fatalf("replay: op=%+v err=%v", op, err)
	}
	// A second Approve of the same digest finds nothing pending.
	if _, err := e.svc.Approve(ctx, e.scope, dig, "gated", e.humanTok); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("second approve: err=%v, want NOT_FOUND", err)
	}
}
