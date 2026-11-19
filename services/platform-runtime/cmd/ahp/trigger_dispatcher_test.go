package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/evals"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/internal/triggers"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Protocol integration: real PostgreSQL Registry pins and Identity child grants.
// HTTP targets record actual routing; separate stand acceptance proves model I/O.
func TestTriggerDispatcherUsesSelectedInstallationAndPinnedVersion(t *testing.T) {
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("isolated PostgreSQL required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ids := identity.New(pool)
	if err = ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	evaluations := evals.NewStore(pool)
	if err = evaluations.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	store := registry.New(pool, registry.WithEvalGate(evaluations), registry.WithSyntheticSupplyChain("trigger dispatcher wire fixture"))
	if err = store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	scope := contracts.Scope{Tenant: "trigger-route-" + contracts.NewID(), Deployment: "source", Subject: "alice"}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner("ahp", "test", priv)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier("ahp", map[string]ed25519.PublicKey{"test": pub})
	root, err := ids.CreateRoot(ctx, scope, identity.RootRequest{Task: "parent", Context: contracts.NewID(), Deployment: scope.Deployment, Capabilities: []string{"state", "model", "execution", "identity:delegate", "identity:cancel"}, TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	token, err := ids.MintTask(ctx, scope, root.Task, signer, "ahp", 0)
	if err != nil {
		t.Fatal(err)
	}
	authority := runtimeAuthority{ids: ids, registry: store, verifier: verifier, signer: signer}
	authority.budgets, err = newRuntimeBudgetAdmitter(ctx, pool, ids)
	if err != nil {
		t.Fatal(err)
	}
	agent := "agent-" + contracts.NewID()
	if _, err = store.RegisterAgent(ctx, "publisher", agent); err != nil {
		t.Fatal(err)
	}
	var versions []registry.Version
	var calls [2]atomic.Int64
	for i := 0; i < 2; i++ {
		i := i
		manifest := registry.Manifest{Kind: registry.KindAgent, System: "trigger-selected", Visibility: registry.VisibilityTenant, Audience: registry.AudienceShared, Class: registry.ClassC, Components: []registry.Component{{Name: "main", A2A: true, MinReplicas: 1, MaxReplicas: 1, Image: "example/selected@sha256:" + strings.Repeat(string(rune('a'+i)), 64)}}, State: registry.StateConfig{Partition: "subject", Compat: "v1"}, Capabilities: registry.Capabilities{Models: []string{"default-chat"}, SelfTasks: 1}, Limits: registry.Limits{Depth: 2, ActiveTasks: 8, TaskTimeout: "5m"}}
		digest, err := contracts.Digest(manifest)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = evaluations.SetPolicy(ctx, evals.Policy{ID: "protocol-fixture-policy", TaskSetID: "protocol-fixture", RequiredTaskIDs: []string{"fixture"}, MaxAgeSeconds: 3600}); err != nil {
			t.Fatal(err)
		}
		if _, err = evaluations.Record(ctx, evals.RecordInput{AgentDigest: digest, TaskSetID: "protocol-fixture", Outcome: evals.OutcomePass, Setup: map[string]any{"test": "routing only, no model acceptance"}, Tasks: []evals.TaskResult{{TaskID: "fixture", Passed: true, TerminalState: evals.TerminalCompleted}}, PolicyID: "protocol-fixture-policy", TargetKind: evals.TargetFixture}); err != nil {
			t.Fatal(err)
		}
		version, err := store.PutVersion(ctx, "publisher", agent, digest, manifest)
		if err != nil {
			t.Fatal(err)
		}
		versions = append(versions, version)
		binding := registry.RuntimeBinding{Tenant: scope.Tenant, Environment: "dev", AgentID: agent, Deployment: "target-" + version.ID, Digest: version.Digest, Image: manifest.Components[0].Image, Attestation: "HTTP transport fixture only"}
		target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/readyz" {
				_ = json.NewEncoder(w).Encode(map[string]any{"ready": true, "agent_id": agent, "version_digest": binding.Digest, "image": binding.Image})
				return
			}
			childScope := scope
			childScope.Deployment = binding.Deployment
			claims, err := verifier.Verify(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), "ahp", auth.KindTask, auth.WorkloadID(childScope))
			if err != nil || claims.Scope() != childScope {
				t.Errorf("wrong target credential: %v", err)
				http.Error(w, "forbidden", 403)
				return
			}
			if err = ids.Authorize(r.Context(), claims, "execution"); err != nil {
				t.Errorf("non-live child: %v", err)
				http.Error(w, "forbidden", 403)
				return
			}
			grant, err := ids.Get(r.Context(), childScope, claims.Task)
			if err != nil || grant.Parent != root.ID || grant.Root != root.Root {
				t.Errorf("parent provenance lost: %v", err)
				http.Error(w, "forbidden", 403)
				return
			}
			var wire map[string]any
			if json.NewDecoder(r.Body).Decode(&wire) != nil {
				t.Error("invalid A2A wire")
				return
			}
			metadata := wire["params"].(map[string]any)["metadata"].(map[string]any)["ahp"].(map[string]any)
			if metadata["version_digest"] != binding.Digest || metadata["task_id"] != claims.Task {
				t.Errorf("pin/credential disagreement: %v", metadata)
			}
			calls[i].Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": wire["id"], "result": map[string]any{"task": map[string]any{"id": claims.Task, "contextId": claims.Context}}})
		}))
		defer target.Close()
		binding.Endpoint = target.URL + "/a2a"
		binding.ReadyURL = target.URL + "/readyz"
		if err = store.RegisterRuntime(ctx, binding); err != nil {
			t.Fatal(err)
		}
	}
	installation, err := store.Install(ctx, scope.Tenant, agent, versions[0].ID, "dev", "selected", versions[0].Manifest.Capabilities)
	if err != nil {
		t.Fatal(err)
	}
	var coreCalls atomic.Int64
	core := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		coreCalls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"result": map[string]any{"task": map[string]any{"id": "wrong-core-task"}}})
	}))
	defer core.Close()
	routed, err := registry.NewRoutedA2AInvokeDispatcher(store, nil)
	if err != nil {
		t.Fatal(err)
	}
	routed.SetRuntimeTokenIssuer(authority)
	dispatcher := &registryTriggerDispatcher{store: store, dispatcher: routed}
	triggerID := contracts.NewID()
	fire := func(sequence int64, target string) (triggers.FireResult, error) {
		return dispatcher.Fire(ctx, scope, triggers.FireRequest{TriggerID: triggerID, Sequence: sequence, Token: token, Target: target, Kind: triggers.KindWebhook, Payload: json.RawMessage(`{"webhook":{"body":{"event":"selected-target-content"}}}`)})
	}
	result, err := fire(1, "installation:"+installation.ID)
	if err != nil || result.TaskID != triggers.FireMessageID(triggerID, 1) || calls[0].Load() != 1 || coreCalls.Load() != 0 {
		t.Fatalf("selected target ignored: result=%+v err=%v selected=%d core=%d", result, err, calls[0].Load(), coreCalls.Load())
	}
	if _, err = store.Promote(ctx, scope.Tenant, installation.ID, versions[1].ID); err != nil {
		t.Fatal(err)
	}
	if _, err = fire(1, installation.ID); err != nil {
		t.Fatal(err)
	}
	if calls[0].Load() != 2 || calls[1].Load() != 0 {
		t.Fatal("same fire changed immutable version after promotion")
	}
	if _, err = fire(2, installation.ID); err != nil {
		t.Fatal(err)
	}
	if calls[1].Load() != 1 {
		t.Fatal("new fire did not select promoted version")
	}
	if _, err = fire(3, "installation:missing"); err == nil || calls[0].Load() != 2 || calls[1].Load() != 1 || coreCalls.Load() != 0 {
		t.Fatalf("missing target fell back: %v", err)
	}
	foreign := scope
	foreign.Tenant = "foreign"
	if _, err = dispatcher.Fire(ctx, foreign, triggers.FireRequest{TriggerID: triggerID, Sequence: 4, Token: token, Target: installation.ID}); err == nil {
		t.Fatal("foreign installation invoked")
	}
	if err = ids.CancelSubtree(ctx, scope, root.Task); err != nil {
		t.Fatal(err)
	}
	if _, err = fire(5, installation.ID); err == nil || calls[1].Load() != 1 {
		t.Fatalf("revoked delegation sent: %v", err)
	}
}
