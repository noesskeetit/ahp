package main

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestOwnerHistorySurvivesExecutionExpiryWithoutMintingGrant(t *testing.T) {
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("real PostgreSQL required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	p, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ids := identity.New(p)
	rs := registry.New(p, registry.WithSyntheticSupplyChain("owner observer wire fixture"))
	st := state.New(p)
	for _, migrate := range []func(context.Context) error{ids.Migrate, rs.Migrate, st.Migrate} {
		if err := migrate(ctx); err != nil {
			t.Fatal(err)
		}
	}
	scope := contracts.Scope{Tenant: "owner-" + contracts.NewID(), Deployment: "custom", Subject: "owner"}
	digest := "sha256:" + strings.Repeat("a", 64)
	image := "example/agent@sha256:" + strings.Repeat("b", 64)
	binding := registry.RuntimeBinding{Tenant: scope.Tenant, Deployment: scope.Deployment, Environment: "prod", AgentID: "agent", Digest: digest, Image: image, Endpoint: "https://unavailable.invalid/a2a", ReadyURL: "https://unavailable.invalid/readyz", Attestation: "operator:test"}
	if err := rs.RegisterRuntime(ctx, binding); err != nil {
		t.Fatal(err)
	}
	thread, err := st.CreateThread(ctx, scope, "old-context-"+contracts.NewID(), "thread")
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(map[string]any{"digest": digest, "config_revision": 3})
	run, err := st.CreateRun(ctx, scope, thread.ID, "old-task", "run", payload)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := st.ClaimRunFor(ctx, scope.Tenant, scope.Deployment, "worker", run.ID, time.Minute)
	if err != nil || lease == nil {
		t.Fatalf("claim %v", err)
	}
	meta, _ := json.Marshal(map[string]any{"version_digest": digest, "image": image, "config_revision": 3})
	_, err = st.PutCheckpoint(ctx, scope, state.Fence{RunID: run.ID, Worker: lease.Worker, Epoch: lease.Epoch}, state.Checkpoint{ID: "cp", ThreadID: thread.ID, Namespace: run.ID, Version: 1, Metadata: meta, Parts: []state.Part{{Channel: "output", Version: "v1", Data: []byte("durable owner answer")}}}, "result")
	if err != nil {
		t.Fatal(err)
	}
	if err = st.Release(ctx, scope, run.ID, lease.Worker, lease.Epoch, state.RunDone); err != nil {
		t.Fatal(err)
	}
	_, err = ids.CreateRoot(ctx, scope, identity.RootRequest{Task: run.TaskID, Context: thread.ContextID, Deployment: scope.Deployment, Capabilities: []string{"state"}, TTL: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	if _, err = ids.Evaluate(ctx, scope, run.TaskID, "state"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("execution grant must be expired: %v", err)
	}
	reader := registryStateObserver{ids: ids, registry: rs, state: st, images: map[string]bool{image: true}}
	config := registry.EffectiveConfig{Tenant: scope.Tenant, Environment: "prod", AgentID: "agent", Digest: digest, Task: run.TaskID, ConfigRevision: 3, Manifest: registry.Manifest{Components: []registry.Component{{A2A: true, Image: image}}}}
	humanScope := scope
	humanScope.Deployment = "space"
	raw, err := reader.ObserveOwned(ctx, humanScope, config)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "durable owner answer") || !strings.Contains(string(raw), `"state":"completed"`) {
		t.Fatalf("missing durable result: %s", raw)
	}
	for _, foreign := range []contracts.Scope{{Tenant: scope.Tenant, Deployment: "space", Subject: "outsider"}, {Tenant: "foreign", Deployment: "space", Subject: scope.Subject}} {
		if _, err := reader.ObserveOwned(ctx, foreign, config); contracts.Code(err) != contracts.ErrNotFound {
			t.Fatalf("foreign history disclosed: %v", err)
		}
	}
	ownTables := config
	ownTables.Manifest.State.OwnTables = true
	if _, err := reader.ObserveOwned(ctx, humanScope, ownTables); contracts.Code(err) != contracts.ErrUnavailable {
		t.Fatalf("own_tables result guessed: %v", err)
	}
	reader.images = nil
	if _, err := reader.ObserveOwned(ctx, humanScope, config); contracts.Code(err) != contracts.ErrUnavailable {
		t.Fatalf("undeclared harness guessed checkpoint: %v", err)
	}
	// Re-evaluation still refuses execution: owner history minted no replacement.
	if _, err = ids.Evaluate(ctx, scope, run.TaskID, "state"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("owner read restored execution: %v", err)
	}

	user := identity.BrowserUser{UserID: "user-" + contracts.NewID(), Tenant: scope.Tenant, Subject: scope.Subject, DisplayName: "Owner", Email: "owner@example.test"}
	if err := ids.SeedBrowserUser(ctx, user, user.UserID, "test-password-long-enough", []string{"user"}, []string{"registry"}); err != nil {
		t.Fatal(err)
	}
	session, err := ids.StartBrowserSession(ctx, user.UserID)
	if err != nil {
		t.Fatal(err)
	}
	jti := contracts.NewID()
	if err := ids.BindBrowserToken(ctx, jti, session); err != nil {
		t.Fatal(err)
	}
	authorize := registryObservationAuthority(ids, []string{"space"})
	c := auth.Claims{Kind: auth.KindHuman, Tenant: scope.Tenant, Deployment: "space", Subject: scope.Subject, JTI: jti}
	if err := authorize(ctx, c, "registry"); err != nil {
		t.Fatal(err)
	}
	other := c
	other.Deployment = "rogue"
	if err := authorize(ctx, other, "registry"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("rogue edge admitted: %v", err)
	}
	other = c
	other.Subject = "outsider"
	if err := authorize(ctx, other, "registry"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("human session subject mismatch admitted: %v", err)
	}
	if err := ids.RevokeBrowserSession(ctx, session.ID); err != nil {
		t.Fatal(err)
	}
	if err := authorize(ctx, c, "registry"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("revoked human admitted: %v", err)
	}
}
