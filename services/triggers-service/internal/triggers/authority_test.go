package triggers

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// mkWebhook creates a webhook trigger in the test scope with a chosen
// delegation reference.
func (e *env) mkWebhook(t *testing.T, ctx context.Context, delegationRef string) Trigger {
	t.Helper()
	tr, err := e.svc.CreateWebhook(ctx, e.scope, CreateWebhookSpec{
		ParentTask:    "parent-" + randHex(4),
		DelegationRef: delegationRef,
		Target:        "agent:echo",
		SecretRef:     "sec-1",
		Delegated:     true,
	})
	if err != nil {
		t.Fatalf("CreateWebhook: %v", err)
	}
	return tr
}

// TestRevokedDelegationStopsTheSchedule proves the product rule of PDF rev 3.2,
// c. 48 and c. 54: when Identity & Policy can no longer honour a delegation,
// FUTURE runs are closed ("увольнение сотрудника закрывает будущие запуски",
// "Triggers остановлены"), instead of the trigger knocking forever.
func TestRevokedDelegationStopsTheSchedule(t *testing.T) {
	e, ctx := newTestService(t)
	tr := e.mkSchedule(t, ctx, baseTime, time.Hour, 5, true)

	e.iss.err = contracts.NewError(contracts.ErrNotFound, "identity: delegation is gone")
	n, err := e.svc.ProcessDue(ctx, baseTime, 0)
	if err != nil {
		t.Fatalf("ProcessDue must not abort the batch on an authority verdict: %v", err)
	}
	if n != 0 || e.disp.n != 0 {
		t.Fatalf("a revoked delegation must not start work: n=%d disp=%d", n, e.disp.n)
	}
	got, err := e.svc.Get(ctx, e.scope, tr.TriggerID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != StatusRevoked {
		t.Fatalf("trigger status = %s, want %s after a revoked delegation", got.Status, StatusRevoked)
	}
	if got.NextFireAt != nil {
		t.Fatalf("a stopped trigger still has a next fire: %v", got.NextFireAt)
	}
	// Even after the issuer "recovers", the stopped trigger never fires again.
	e.iss.err = nil
	if n, err := e.svc.ProcessDue(ctx, baseTime.Add(time.Hour), 0); err != nil || n != 0 || e.disp.n != 0 {
		t.Fatalf("revoked trigger fired later: n=%d disp=%d err=%v", n, e.disp.n, err)
	}
	// The refused fire is journalled as a failure carrying the reason.
	op, err := e.svc.ledger.Get(ctx, e.scope, fmt.Sprintf("trigger:%s:fire:1", tr.TriggerID))
	if err != nil {
		t.Fatalf("ledger Get: %v", err)
	}
	if op.Status != operations.StatusFailed {
		t.Fatalf("ledger status = %s, want %s", op.Status, operations.StatusFailed)
	}
}

// TestTransientIssuerFailureKeepsTheScheduleAlive is the other half of the same
// rule: an unreachable identity service is not a verdict about the delegation.
func TestTransientIssuerFailureKeepsTheScheduleAlive(t *testing.T) {
	e, ctx := newTestService(t)
	tr := e.mkSchedule(t, ctx, baseTime, time.Hour, 5, true)

	e.iss.err = errors.New("identity: connection reset")
	if _, err := e.svc.ProcessDue(ctx, baseTime, 0); err == nil {
		t.Fatal("a transient issuer failure must be reported")
	}
	got, err := e.svc.Get(ctx, e.scope, tr.TriggerID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != StatusActive {
		t.Fatalf("a transient failure stopped the trigger: %s", got.Status)
	}
	if got.NextFireAt == nil {
		t.Fatal("a transient failure must not end the schedule")
	}
	// No work started, and the attempt is journalled as a closed failure (the
	// effect never happened), never as an unknown outcome.
	if e.disp.n != 0 {
		t.Fatalf("a failed token issue must not dispatch work: %d", e.disp.n)
	}
	op, err := e.svc.ledger.Get(ctx, e.scope, fmt.Sprintf("trigger:%s:fire:1", tr.TriggerID))
	if err != nil {
		t.Fatalf("ledger Get: %v", err)
	}
	if op.Status != operations.StatusFailed {
		t.Fatalf("issuer failure sealed as %s, want %s", op.Status, operations.StatusFailed)
	}
	// The schedule lives on. The first tick after the outage only moves the
	// closed failure forward; the tick after that fires normally.
	e.iss.err = nil
	if n, err := e.svc.ProcessDue(ctx, baseTime.Add(time.Hour), 0); err != nil || n != 0 {
		t.Fatalf("recovery tick = %d, %v", n, err)
	}
	if n, err := e.svc.ProcessDue(ctx, baseTime.Add(2*time.Hour), 0); err != nil || n != 1 || e.disp.n != 1 {
		t.Fatalf("fire after the outage: n=%d disp=%d err=%v", n, e.disp.n, err)
	}
}

// TestRefusedFireIsSealedAndBurnsSlot proves the distinction between a refusal
// and an unknown outcome (PDF rev 3.2, c. 13: "неотправленное закрыто" vs
// "отправленное ... становится UNKNOWN"): a provable refusal is settled and the
// bounded budget moves on, while an ambiguous outcome stays unknown.
func TestRefusedFireIsSealedAndBurnsSlot(t *testing.T) {
	e, ctx := newTestService(t)
	tr := e.mkSchedule(t, ctx, baseTime, time.Hour, 3, true)
	e.disp.err = fmt.Errorf("gateway said no: %w", ErrDispatchRefused)

	if n, err := e.svc.ProcessDue(ctx, baseTime, 0); err != nil || n != 0 {
		t.Fatalf("ProcessDue = %d, %v", n, err)
	}
	op, err := e.svc.ledger.Get(ctx, e.scope, fmt.Sprintf("trigger:%s:fire:1", tr.TriggerID))
	if err != nil {
		t.Fatalf("ledger Get: %v", err)
	}
	if op.Status != operations.StatusFailed {
		t.Fatalf("refused fire status = %s, want %s (not an unknown outcome)", op.Status, operations.StatusFailed)
	}
	got, err := e.svc.Get(ctx, e.scope, tr.TriggerID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.FiredCount != 1 || got.NextFireAt == nil || got.Status != StatusActive {
		t.Fatalf("refused fire did not burn its slot cleanly: %+v", got)
	}

	// The contrast: an ambiguous dispatcher outcome must stay unknown.
	amb := e.mkSchedule(t, ctx, baseTime, time.Hour, 3, true)
	e.disp.err = errors.New("connection reset after the request was sent")
	if _, err := e.svc.ProcessDue(ctx, baseTime, 0); err == nil {
		t.Fatal("an ambiguous outcome must be reported")
	}
	aop, err := e.svc.ledger.Get(ctx, e.scope, fmt.Sprintf("trigger:%s:fire:1", amb.TriggerID))
	if err != nil {
		t.Fatalf("ledger Get ambiguous: %v", err)
	}
	if aop.Status != operations.StatusSideEffectUnknown {
		t.Fatalf("ambiguous outcome sealed as %s, want %s", aop.Status, operations.StatusSideEffectUnknown)
	}
}

// TestCascadeRevokesBySubjectAndDelegation proves the cascade hooks Identity &
// Policy calls when a subject is dismissed or a delegation tree is revoked
// (PDF rev 3.2, c. 53: "гранты отозваны, триггеры остановлены").
func TestCascadeRevokesBySubjectAndDelegation(t *testing.T) {
	e, ctx := newTestService(t)
	mine := e.mkSchedule(t, ctx, baseTime, time.Hour, 5, true)
	alsoMine := e.mkWebhook(t, ctx, "del-ref-XYZ")
	notMine := e.mkWebhook(t, ctx, "del-ref-OTHER")

	// Another tenant's trigger with the same subject and delegation ref must be
	// untouched: the cascade is tenant-scoped.
	foreignScope := contracts.Scope{Tenant: "foreign" + randHex(4), Deployment: e.scope.Deployment, Subject: e.scope.Subject}
	foreign, err := e.svc.CreateSchedule(ctx, foreignScope, CreateScheduleSpec{
		ParentTask: "parent-f", DelegationRef: "del-ref-XYZ", Target: "agent:echo",
		Interval: time.Hour, MaxFires: 3, Delegated: true, FirstFireAt: &baseTime,
	})
	if err != nil {
		t.Fatalf("foreign CreateSchedule: %v", err)
	}

	stopped, err := e.svc.RevokeBySubject(ctx, e.scope.Tenant, e.scope.Subject)
	if err != nil {
		t.Fatalf("RevokeBySubject: %v", err)
	}
	if stopped != 3 {
		t.Fatalf("RevokeBySubject stopped %d, want 3", stopped)
	}
	for _, id := range []string{mine.TriggerID, alsoMine.TriggerID, notMine.TriggerID} {
		got, err := e.svc.Get(ctx, e.scope, id)
		if err != nil {
			t.Fatalf("Get(%s): %v", id, err)
		}
		if got.Status != StatusRevoked {
			t.Fatalf("trigger %s survived the subject cascade: %s", id, got.Status)
		}
	}
	foreignRow, err := e.svc.Get(ctx, foreignScope, foreign.TriggerID)
	if err != nil {
		t.Fatalf("foreign Get: %v", err)
	}
	if foreignRow.Status != StatusActive {
		t.Fatalf("subject cascade crossed tenants: %s", foreignRow.Status)
	}
	// Idempotent: a second cascade stops nothing.
	if n, err := e.svc.RevokeBySubject(ctx, e.scope.Tenant, e.scope.Subject); err != nil || n != 0 {
		t.Fatalf("second cascade = %d, %v", n, err)
	}
	// The delegation cascade stops exactly the triggers of one reference.
	other := contracts.Scope{Tenant: e.scope.Tenant, Deployment: "d" + randHex(4), Subject: "s" + randHex(4)}
	byRefA, err := e.svc.CreateSchedule(ctx, other, CreateScheduleSpec{
		ParentTask: "parent-a", DelegationRef: "del-ref-A", Target: "agent:echo",
		Interval: time.Hour, MaxFires: 3, Delegated: true, FirstFireAt: &baseTime,
	})
	if err != nil {
		t.Fatalf("CreateSchedule byRefA: %v", err)
	}
	byRefB, err := e.svc.CreateSchedule(ctx, other, CreateScheduleSpec{
		ParentTask: "parent-b", DelegationRef: "del-ref-B", Target: "agent:echo",
		Interval: time.Hour, MaxFires: 3, Delegated: true, FirstFireAt: &baseTime,
	})
	if err != nil {
		t.Fatalf("CreateSchedule byRefB: %v", err)
	}
	if n, err := e.svc.RevokeByDelegation(ctx, other.Tenant, "del-ref-A"); err != nil || n != 1 {
		t.Fatalf("RevokeByDelegation = %d, %v", n, err)
	}
	if a, err := e.svc.Get(ctx, other, byRefA.TriggerID); err != nil || a.Status != StatusRevoked {
		t.Fatalf("delegation cascade missed its trigger: %+v, %v", a, err)
	}
	if b, err := e.svc.Get(ctx, other, byRefB.TriggerID); err != nil || b.Status != StatusActive {
		t.Fatalf("delegation cascade stopped an unrelated trigger: %+v, %v", b, err)
	}
	// A cascade with an empty selector is refused, never "stop everything".
	if _, err := e.svc.RevokeByDelegation(ctx, other.Tenant, ""); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("empty selector code = %q, want %q", contracts.Code(err), contracts.ErrInvalid)
	}
}
