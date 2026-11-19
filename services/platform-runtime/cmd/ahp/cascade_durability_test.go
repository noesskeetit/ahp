//go:build ahp_data

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/memory"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

type unavailableCascade struct{ artifactCascade }

func (c unavailableCascade) cascadeThread(context.Context, contracts.Scope, []string, []string) (cascadeCounters, error) {
	return cascadeCounters{}, contracts.NewError(contracts.ErrUnavailable, "artifact plane unavailable")
}

func TestDeletionRetryAfterRestartPreservesEveryBinding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, art, scope := cascadeEnv(t, ctx)
	st := state.New(pool)
	if err := migrateStateAll(ctx, st); err != nil {
		t.Fatal(err)
	}
	thread, err := st.CreateThread(ctx, scope, "retry-distinct-context", "thread")
	if err != nil {
		t.Fatal(err)
	}
	run, err := st.CreateRun(ctx, scope, thread.ID, "retry-distinct-task", "run", json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{
		sealArtifact(t, ctx, art, scope, artifacts.Binding{ContextID: thread.ContextID}, "by-context"),
		sealArtifact(t, ctx, art, scope, artifacts.Binding{TaskID: run.TaskID}, "by-task"),
		sealArtifact(t, ctx, art, scope, artifacts.Binding{TaskID: run.ID}, "by-run"),
	}
	failed := cascadeServerConfigured(t, st, art, scope, func(s *server) { s.cascade = unavailableCascade{s.cascade} })
	token := signTaskToken(t, failed, scope, run.TaskID, thread.ContextID)
	if code, body := failed.call(t, "DeleteThread", token, `{"thread_id":"`+thread.ID+`"}`); code < 500 {
		t.Fatalf("failed cascade answered %d %v", code, body)
	}
	if _, err := st.GetThread(ctx, scope, thread.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("expected state committed before failed cascade: %v", err)
	}
	// Reconstruct both service and State object; only PostgreSQL carries intent.
	restarted := cascadeServer(t, state.New(pool), art, scope)
	token = signTaskToken(t, restarted, scope, run.TaskID, thread.ContextID)
	if code, body := restarted.call(t, "DeleteThread", token, `{"thread_id":"`+thread.ID+`"}`); code != http.StatusOK {
		t.Errorf("durable retry %d %v", code, body)
	}
	for _, id := range ids {
		if _, err := art.Get(ctx, scope, id); contracts.Code(err) != contracts.ErrNotFound {
			t.Errorf("binding artifact survived retry: %s %v", id, err)
		}
	}
}

func TestMissingCascadeRefusesBeforeStateDeletion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, art, scope := cascadeEnv(t, ctx)
	st := state.New(pool)
	if err := migrateStateAll(ctx, st); err != nil {
		t.Fatal(err)
	}
	thread, err := st.CreateThread(ctx, scope, "missing-cascade", "thread")
	if err != nil {
		t.Fatal(err)
	}
	srv := cascadeServerConfigured(t, st, art, scope, func(s *server) { s.cascade = nil })
	token := signTaskToken(t, srv, scope, "task", thread.ContextID)
	code, body := srv.call(t, "DeleteThread", token, `{"thread_id":"`+thread.ID+`"}`)
	if code != http.StatusServiceUnavailable {
		t.Errorf("missing cascade = %d %v", code, body)
	}
	if _, err := st.GetThread(ctx, scope, thread.ID); err != nil {
		t.Fatalf("state deleted despite missing cascade: %v", err)
	}
}

func TestPendingDeletionRetryStillRequiresLiveAuthorization(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, art, scope := cascadeEnv(t, ctx)
	st := state.New(pool)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	th, err := st.CreateThread(ctx, scope, "revoke-retry", "create")
	if err != nil {
		t.Fatal(err)
	}
	id := sealArtifact(t, ctx, art, scope, artifacts.Binding{ContextID: th.ContextID}, "must-remain-on-denial")
	srv := cascadeServerConfigured(t, st, art, scope, func(s *server) { s.cascade = unavailableCascade{s.cascade} })
	tok := signTaskToken(t, srv, scope, "task", th.ContextID)
	code, body := srv.call(t, "DeleteThread", tok, `{"thread_id":"`+th.ID+`"}`)
	if code != http.StatusServiceUnavailable || body["deletion_id"] == nil || body["state"] != "pending" {
		t.Fatalf("missing durable pending result: %d %v", code, body)
	}
	srv.revoked.Store(true)
	code, body = srv.call(t, "DeleteThread", tok, `{"thread_id":"`+th.ID+`"}`)
	if code != http.StatusForbidden {
		t.Fatalf("revoked retry reached cascade: %d %v", code, body)
	}
	if _, err := art.Get(ctx, scope, id); err != nil {
		t.Fatalf("denied retry changed artifact: %v", err)
	}
}

type unavailableSubjectCascade struct {
	artifactCascade
	memoryDeletion
}

func (c unavailableSubjectCascade) cascadeSubject(context.Context, contracts.Scope) (cascadeCounters, error) {
	return cascadeCounters{}, contracts.NewError(contracts.ErrUnavailable, "artifact subject plane unavailable")
}

func TestSubjectDeletionChecksMemoryPermissionBeforeCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, art, scope := cascadeEnv(t, ctx)
	st := state.New(pool)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	th, err := st.CreateThread(ctx, scope, "memory-preflight", "create")
	if err != nil {
		t.Fatal(err)
	}
	denied, err := memory.New(pool, art, func(context.Context, contracts.Scope, string, string) error {
		return contracts.NewError(contracts.ErrForbidden, "memory erasure denied")
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := cascadeServerConfigured(t, st, art, scope, func(s *server) { s.cascade = withMemoryDeletion(s.cascade, denied) })
	tok, err := srv.signer.Sign(auth.Claims{Kind: auth.KindHuman, Audience: "ahp", Tenant: scope.Tenant, Subject: "compliance-admin"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	code, body := srv.adminCall(t, "DeleteSubject", tok, scope, `{"key":"preflight"}`)
	if code != http.StatusForbidden {
		t.Fatalf("preflight = %d %v", code, body)
	}
	if _, err := st.GetThread(ctx, scope, th.ID); err != nil {
		t.Fatalf("memory denial followed State commit: %v", err)
	}
}

func TestSubjectDeletionPersistsMemoryAckBeforeArtifactRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, art, scope := cascadeEnv(t, ctx)
	st := state.New(pool)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	th, err := st.CreateThread(ctx, scope, "memory-retry", "create")
	if err != nil {
		t.Fatal(err)
	}
	id := sealArtifact(t, ctx, art, scope, artifacts.Binding{ContextID: th.ContextID}, "erase-on-retry")
	srv := cascadeServerConfigured(t, st, art, scope, func(s *server) { s.cascade = unavailableSubjectCascade{s.cascade, s.cascade.(memoryDeletion)} })
	mem, err := memory.New(pool, art, func(context.Context, contracts.Scope, string, string) error { return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	own, err := mem.Propose(ctx, scope, memory.ProposeRequest{Kind: memory.KindUser, Text: "erase subject memory", OpKey: "own"})
	if err != nil {
		t.Fatal(err)
	}
	foreign := scope
	foreign.Subject = "other-subject"
	other, err := mem.Propose(ctx, foreign, memory.ProposeRequest{Kind: memory.KindUser, Text: "keep other subject memory", OpKey: "other"})
	if err != nil {
		t.Fatal(err)
	}
	tok, err := srv.signer.Sign(auth.Claims{Kind: auth.KindHuman, Audience: "ahp", Tenant: scope.Tenant, Subject: "compliance-admin"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	code, body := srv.adminCall(t, "DeleteSubject", tok, scope, `{"key":"subject-retry"}`)
	if code != http.StatusServiceUnavailable || body["memory_done"] != true {
		t.Fatalf("durable memory acknowledgement = %d %v", code, body)
	}
	if _, err := mem.Get(ctx, scope, own.Memory.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("memory not erased: %v", err)
	}
	restarted := cascadeServer(t, state.New(pool), art, scope)
	tok, err = restarted.signer.Sign(auth.Claims{Kind: auth.KindHuman, Audience: "ahp", Tenant: scope.Tenant, Subject: "compliance-admin"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	code, body = restarted.adminCall(t, "DeleteSubject", tok, scope, `{"key":"subject-retry"}`)
	if code != http.StatusOK {
		t.Fatalf("retry = %d %v", code, body)
	}
	if _, err := art.Get(ctx, scope, id); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("artifact survived retry: %v", err)
	}
	if _, err := mem.Get(ctx, foreign, other.Memory.ID); err != nil {
		t.Fatalf("foreign memory erased: %v", err)
	}
	var mutations int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM ahp_memory.operations WHERE tenant=$1 AND subject=$2 AND op_key LIKE 'delete-%'`, scope.Tenant, scope.Subject).Scan(&mutations); err != nil {
		t.Fatal(err)
	}
	if mutations != 1 {
		t.Fatalf("memory mutation applied %d times", mutations)
	}
}
