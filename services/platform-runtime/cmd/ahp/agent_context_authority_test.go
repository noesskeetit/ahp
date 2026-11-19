package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Scope/JWT fixture, not evaluation or runtime acceptance. The actual Evals and
// TLS routing gates have separate tests; this gate only supplies a valid manifest.
type agentContextFixtureGate struct{}

func (agentContextFixtureGate) RequirePassedVerdict(context.Context, string) error { return nil }

type agentContextFixture struct {
	a       runtimeAuthority
	scope   contracts.Scope
	binding registry.RuntimeBinding
	req     registry.InvokeRequest
	dialog  string
}

func newAgentContextFixture(t *testing.T) agentContextFixture {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires isolated real PostgreSQL")
	}
	pool, err := pgxpool.New(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	ids := identity.New(pool)
	rs := registry.New(pool, registry.WithEvalGate(agentContextFixtureGate{}), registry.WithSyntheticSupplyChain("JWT context fixture, no runtime deployment"))
	for _, migrate := range []func(context.Context) error{ids.Migrate, rs.Migrate} {
		if err = migrate(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner("ahp", "test", private)
	if err != nil {
		t.Fatal(err)
	}
	f := agentContextFixture{a: runtimeAuthority{ids: ids, registry: rs, signer: signer, verifier: auth.NewVerifier("ahp", map[string]ed25519.PublicKey{"test": public})}, scope: contracts.Scope{Tenant: "agent-ctx-" + contracts.NewID(), Deployment: "space", Subject: "owner"}, dialog: "dialog-" + contracts.NewID()}
	agent := "agent-ctx-" + strings.ToLower(contracts.NewID())
	image := "example/agent@sha256:" + strings.Repeat("a", 64)
	m := registry.Manifest{Kind: registry.KindAgent, System: "agent-chat", Visibility: registry.VisibilityTenant, Audience: registry.AudienceShared, Class: registry.ClassC, Components: []registry.Component{{Name: "agent", A2A: true, Image: image, MinReplicas: 1, MaxReplicas: 1}}, State: registry.StateConfig{Partition: "subject", Compat: "v1"}, Capabilities: registry.Capabilities{SelfTasks: 1}, Limits: registry.Limits{Depth: 2, ActiveTasks: 4, TaskTimeout: "1m"}}
	digest, err := contracts.Digest(m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = rs.RegisterAgent(t.Context(), f.scope.Subject, agent); err != nil {
		t.Fatal(err)
	}
	v, err := rs.PutVersion(t.Context(), f.scope.Subject, agent, digest, m)
	if err != nil {
		t.Fatal(err)
	}
	f.binding = registry.RuntimeBinding{Tenant: f.scope.Tenant, Deployment: "custom-agent", AgentID: agent, Digest: v.Digest, Image: image}
	f.req = registry.InvokeRequest{Scope: f.scope, Tenant: f.scope.Tenant, AgentID: agent, VersionID: v.ID, VersionDigest: v.Digest, InstallationID: "installed", Capabilities: m.Capabilities}
	return f
}
func (f agentContextFixture) parent(t *testing.T, name, conversation string) string {
	t.Helper()
	_, err := f.a.ids.CreateRoot(t.Context(), f.scope, identity.RootRequest{Task: name, Context: conversation, Deployment: f.scope.Deployment, Capabilities: []string{"state", "execution", "identity:cancel", "identity:delegate"}, TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	token, err := f.a.ids.MintTask(t.Context(), f.scope, name, f.a.signer, "ahp", 0)
	if err != nil {
		t.Fatal(err)
	}
	return token
}
func TestAgentRuntimeAuthorityKeepsConversationAcrossDistinctTurnGrants(t *testing.T) {
	f := newAgentContextFixture(t)
	want := contracts.OperationKey(f.scope, f.dialog, "registry.agent-context", f.req.InstallationID)
	requests := []registry.InvokeRequest{}
	for _, turn := range []string{"turn-one", "turn-two"} {
		r := f.req
		r.TaskID = "child-" + turn
		r.Token = f.parent(t, turn, f.dialog)
		r.ConversationContext = want
		token, err := f.a.IssueRuntimeToken(t.Context(), r, f.binding)
		if err != nil {
			t.Fatal(err)
		}
		targetScope := f.scope
		targetScope.Deployment = f.binding.Deployment
		claims, err := f.a.verifier.Verify(token, "ahp", auth.KindTask, auth.WorkloadID(targetScope))
		if err != nil || claims.Context != want || claims.Task != r.TaskID {
			t.Fatalf("turn got another native conversation: context=%s expected=%s err=%v", claims.Context, want, err)
		}
		requests = append(requests, r)
	}
	r := requests[0]
	r.Observe = true
	if _, err := f.a.IssueRuntimeToken(t.Context(), r, f.binding); err != nil {
		t.Fatal("original parent cannot observe its task", err)
	}
	r.Token = requests[1].Token
	if _, err := f.a.IssueRuntimeToken(t.Context(), r, f.binding); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal("sibling turn observed another turn's child", err)
	}
}
func TestAgentRuntimeAuthorityRejectsAnInventedConversation(t *testing.T) {
	f := newAgentContextFixture(t)
	r := f.req
	r.TaskID = "invented-child"
	r.Token = f.parent(t, "parent", f.dialog)
	r.ConversationContext = "arbitrary-other-dialog"
	if _, err := f.a.IssueRuntimeToken(t.Context(), r, f.binding); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal("invented target conversation was granted", err)
	}
	target := f.scope
	target.Deployment = f.binding.Deployment
	if _, err := f.a.ids.Get(t.Context(), target, r.TaskID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatal("refused context still created a grant", err)
	}
}
func TestAgentRuntimeAuthorityDoesNotAdoptALegacyTaskContext(t *testing.T) {
	f := newAgentContextFixture(t)
	r := f.req
	r.TaskID = "legacy-child"
	r.Token = f.parent(t, "parent", f.dialog)
	if _, err := f.a.IssueRuntimeToken(t.Context(), r, f.binding); err != nil {
		t.Fatal(err)
	}
	r.Observe = true
	r.ConversationContext = contracts.OperationKey(f.scope, f.dialog, "registry.agent-context", r.InstallationID)
	if _, err := f.a.IssueRuntimeToken(t.Context(), r, f.binding); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal("legacy grant silently moved into another conversation", err)
	}
}
