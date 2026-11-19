//go:build ahp_data

package main

import (
	"context"
	"encoding/json"
	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/memory"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

func TestControlSelfDeletionResumesAfterRevocationWithoutHumanCredential(t *testing.T) {
	ctx := context.Background()
	var fail atomic.Bool
	pool, art, scope := cascadeEnv(t, ctx, func(c context.Context, target contracts.Scope, action, _ string) error {
		if action == "delete" && fail.Load() && permitsDeletion(c, target) {
			return contracts.NewError(contracts.ErrUnavailable, "owner temporarily offline")
		}
		return nil
	})
	scope.Subject = "compliance-admin"
	st := state.New(pool)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	th, err := st.CreateThread(ctx, scope, "self", "self")
	if err != nil {
		t.Fatal(err)
	}
	artID := sealArtifact(t, ctx, art, scope, artifacts.Binding{ContextID: th.ContextID}, "self bytes")
	// A cancelled process context represents the old process stopping after
	// persisting the intent. The restarted consumer below has no browser JWT.
	stopped, stop := context.WithCancel(ctx)
	stop()
	var composed *server
	srv := cascadeServerConfigured(t, st, art, scope, func(s *server) { composed = s }, ControlDeletionConfig{ConsumerContext: stopped})
	mem, err := memory.New(pool, art, memoryStoreAuthorize(func(c context.Context, claims auth.Claims, _ string) error {
		var revoked bool
		if err := pool.QueryRow(c, `SELECT EXISTS(SELECT 1 FROM ahp_identity.subjects WHERE tenant=$1 AND subject=$2)`, claims.Tenant, claims.Subject).Scan(&revoked); err != nil {
			return err
		}
		if revoked {
			return contracts.NewError(contracts.ErrForbidden, "human is revoked")
		}
		return nil
	}), nil)
	if err != nil {
		t.Fatal(err)
	}
	base := composed.cascade.(*memoryAwareCascade)
	base.memory = mem
	// Proposal is seeded through the owner fixture; the deletion store itself
	// uses the real composition authorization adapter above.
	seed, err := memory.New(pool, art, func(context.Context, contracts.Scope, string, string) error { return nil }, nil)
	if err != nil {
		t.Fatal(err)
	}
	note, err := seed.Propose(ctx, scope, memory.ProposeRequest{Kind: memory.KindUser, Text: "self note", OpKey: "self-note"})
	if err != nil {
		t.Fatal(err)
	}
	token, err := srv.signer.Sign(auth.Claims{Kind: auth.KindHuman, Audience: "ahp", Tenant: scope.Tenant, Subject: scope.Subject}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	fail.Store(true)
	code, body := srv.adminCall(t, "ControlDeleteSubject", token, scope, `{"key":"self-durable"}`)
	if code != http.StatusServiceUnavailable || body["state"] != "pending" {
		t.Fatalf("self intent not persisted: %d %v", code, body)
	}
	var revoked bool
	if err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_identity.subjects WHERE tenant=$1 AND subject=$2)`, scope.Tenant, scope.Subject).Scan(&revoked); err != nil || !revoked {
		t.Fatalf("not revoked: %v %v", revoked, err)
	}
	fail.Store(false)
	consumer := &stateDeletionCoordinator{state: state.New(pool), cascade: composed.cascade}
	if err = consumer.consumeControlDeletions(ctx); err != nil {
		t.Fatal(err)
	}
	job, err := st.GetControlDeletion(ctx, scope.Tenant, scope.Subject, "self-durable")
	if err != nil || !job.Completed {
		t.Fatalf("service did not resume: completed=%v err=%v", job.Completed, err)
	}
	if _, err = st.GetThread(ctx, scope, th.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("state survives: %v", err)
	}
	if _, err = art.Get(ctx, scope, artID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("artifact survives: %v", err)
	}
	if _, err = seed.Get(ctx, scope, note.Memory.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("memory survives: %v", err)
	}
	// Durable authority cannot authorize writes, another subject, or another tenant.
	service := withDeletionAuthority(ctx, job)
	authorize := memoryStoreAuthorize(nil)
	for _, tc := range []struct {
		s                contracts.Scope
		action, resource string
	}{{scope, memory.CapabilityReview, "memory:new"}, {contracts.Scope{Tenant: scope.Tenant, Deployment: scope.Deployment, Subject: "other"}, memory.CapabilityDeleteSubject, "subject:other"}, {contracts.Scope{Tenant: "other", Deployment: scope.Deployment, Subject: scope.Subject}, memory.CapabilityDeleteSubject, "subject:" + scope.Subject}} {
		if err = authorize(service, tc.s, tc.action, tc.resource); contracts.Code(err) != contracts.ErrForbidden {
			t.Fatalf("authority escaped: %v", err)
		}
	}
	if err = consumer.consumeControlDeletions(ctx); err != nil {
		t.Fatal(err)
	}
	again, err := st.GetControlDeletion(ctx, scope.Tenant, scope.Subject, "self-durable")
	if err != nil || again.ID != job.ID {
		t.Fatal("receipt changed")
	}
	// A legacy job is not executable merely because it is present in the queue.
	raw, _ := json.Marshal(subjectPlan{Scope: scope})
	legacy, err := st.BeginControlDeletion(ctx, scope, "legacy-admin", "legacy-job", raw)
	if err != nil {
		t.Fatal(err)
	}
	if err = consumer.resumeControlDeletion(ctx, legacy); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("legacy authority accepted: %v", err)
	}
}
