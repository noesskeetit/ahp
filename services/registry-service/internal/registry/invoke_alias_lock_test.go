package registry_test

import (
	"context"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// The erasure control plane holds the subject gate before deleting task pins.
// An alias waiting for that gate must not already hold the row erasure needs.
func TestInvocationAliasWaitsForSubjectGateBeforeLockingPin(t *testing.T) {
	store, tenant := newSyntheticStore(t)
	agent := "alias-lock-" + randSuffix(t)
	m := manifest(t, "alias-lock")
	v := publish(t, store, agent, m)
	inst, err := store.Install(t.Context(), tenant, agent, v.ID, "dev", "alias-lock-"+randSuffix(t), m.Capabilities)
	if err != nil {
		t.Fatal(err)
	}
	scope := contracts.Scope{Tenant: tenant, Deployment: "space", Subject: "owner"}
	source, target := "grant-"+randSuffix(t), "runtime-"+randSuffix(t)
	if _, err := store.PinForSubject(t.Context(), scope, inst.ID, source); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	gate, err := store.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer gate.Rollback(context.Background())
	key := "ahp_registry/subject-erasure/" + tenant + "/" + scope.Subject
	if _, err := gate.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, key); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- store.AliasTaskPin(ctx, tenant, inst.ID, source, target) }()
	defer func() {
		_ = gate.Rollback(context.Background())
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("alias failed after subject gate opened: %v", err)
			}
		case <-time.After(6 * time.Second):
			t.Error("alias did not finish after subject gate opened")
		}
	}()
	for {
		var waiting bool
		err := store.Pool().QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted AND objsubid=1
 AND classid=((hashtextextended($1,0)>>32)&4294967295)::oid
 AND objid=(hashtextextended($1,0)&4294967295)::oid)`, key).Scan(&waiting)
		if err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("alias did not reach the subject gate")
		case <-time.After(5 * time.Millisecond):
		}
	}
	// NOWAIT makes the regression deterministic: the old implementation had
	// already locked this row before waiting for the erasure gate.
	_, err = gate.Exec(ctx, `SELECT task_id FROM ahp_registry.task_pins WHERE tenant=$1 AND task_id=$2 FOR UPDATE NOWAIT`, tenant, source)
	if err != nil {
		t.Fatalf("alias locked the task pin before passing the subject gate: %v", err)
	}
}
