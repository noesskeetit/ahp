//go:build ahp_data

package main

import (
	"context"
	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/tools"
	"github.com/ahp-platform/ahp/internal/triggers"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"strings"
	"testing"
	"time"
)

// This is real owner-store/control HTTP acceptance, not an LLM or provider test.
// OAuth protocol/Vault is tested in internal/tools against real Vault separately.
func TestControlSubjectAllOwnersResumeAfterSelfRevocation(t *testing.T) {
	ctx := context.Background()
	pool, art, scope := cascadeEnv(t, ctx)
	scope.Subject = "compliance-admin"
	st := state.New(pool)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	owners := map[string]SubjectDeletionOwner{}
	for name, owner := range tenantTestOwners(t, pool) {
		subject, ok := owner.(SubjectDeletionOwner)
		if !ok {
			t.Fatalf("missing %s subject contract", name)
		}
		owners[name] = subject
	}
	suffix := contracts.NewID()
	toolSchema := "ahp_subjecttools_" + suffix[len(suffix)-12:]
	tool, err := tools.New(ctx, pool, toolSchema, tools.Config{Guardian: tools.NewPolicyGuardian("test"), Credentials: tools.NewVaultResolver(nil, nil), Connections: map[string]tools.Connector{"fixture": func(context.Context, tools.Credential) (tools.MCPClient, error) {
		return nil, contracts.NewError(contracts.ErrUnavailable, "not invoked")
	}}, Tools: map[string]tools.ToolSpec{"echo": {Connection: "fixture"}}})
	if err != nil {
		t.Fatal(err)
	}
	owners["tools"] = tool
	ids := identity.New(pool)
	if err = ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	owners["identity"] = ids
	reg := registry.New(pool, registry.WithTaskSubjectResolver(ids.TaskSubject))
	if err = reg.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	owners["registry"] = reg
	stopped, stop := context.WithCancel(ctx)
	stop()
	cfg := ControlDeletionConfig{SubjectOwners: owners, RequiredSubjectOwners: requiredSubjectOwners, ConsumerContext: stopped}
	var composed *server
	srv := cascadeServerConfigured(t, st, art, scope, func(s *server) { composed = s }, cfg)
	for _, missing := range requiredSubjectOwners {
		partial := snapshotSubjectOwners(owners)
		delete(partial, missing)
		c := stateDeletionCoordinator{control: ControlDeletionConfig{SubjectOwners: partial, RequiredSubjectOwners: requiredSubjectOwners}}
		if _, err = c.subjectOwnerNames(); err == nil {
			t.Fatalf("missing %s accepted", missing)
		}
	}
	th, err := st.CreateThread(ctx, scope, "subject-context", "subject-thread")
	if err != nil {
		t.Fatal(err)
	}
	artifact := sealArtifact(t, ctx, art, scope, artifacts.Binding{ContextID: th.ContextID}, "erase me")
	foreign := scope
	foreign.Subject = "bob"
	foreignThread, err := st.CreateThread(ctx, foreign, "foreign-context", "foreign-thread")
	if err != nil {
		t.Fatal(err)
	}
	computer, err := owners["compute"].(*compute.Store).CreateComputer(ctx, scope, compute.NewComputerRequest{Class: compute.ClassS, Profile: compute.ProfileEphemeral}, "computer")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = owners["gateway"].(*gateway.Store).AcquireContext(ctx, scope, "caller", ""); err != nil {
		t.Fatal(err)
	}
	if _, err = owners["triggers"].(*triggers.Service).CreateSchedule(ctx, scope, triggers.CreateScheduleSpec{ParentTask: "parent", DelegationRef: "ref", Target: "agent", Interval: time.Minute, MaxFires: 3, Delegated: true}); err != nil {
		t.Fatal(err)
	}
	user := identity.BrowserUser{UserID: contracts.NewID(), Tenant: scope.Tenant, Subject: scope.Subject, DisplayName: "self"}
	if err = ids.SeedBrowserUser(ctx, user, "subject-"+contracts.NewID(), "synthetic-password", nil, nil); err != nil {
		t.Fatal(err)
	}
	// Legacy Registry snapshots must be bound from real Identity authority
	// before its grants are erased, including a different subject in this tenant.
	version, installation, agent := contracts.NewID(), contracts.NewID(), contracts.NewID()
	if _, err = pool.Exec(ctx, `INSERT INTO ahp_registry.versions(id,agent_id,digest,manifest) VALUES($1,$2,$3,'{}')`, version, agent, "sha256:"+strings.Repeat("b", 64)); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO ahp_registry.installations(id,tenant,agent_id,deployment,environment,version_id,idem_key,request_digest) VALUES($1,$2,$3,'legacy-deploy','dev',$4,$1,'fixture')`, installation, scope.Tenant, agent, version); err != nil {
		t.Fatal(err)
	}
	pinnedTasks := map[string]string{}
	for _, target := range []contracts.Scope{scope, foreign} {
		task := contracts.NewID()
		pinnedTasks[target.Subject] = task
		if _, err = ids.CreateRoot(ctx, target, identity.RootRequest{Task: task, Context: contracts.NewID(), Deployment: target.Deployment, Capabilities: []string{"read"}, TTL: time.Hour}); err != nil {
			t.Fatal(err)
		}
		if _, err = reg.Pin(ctx, target.Tenant, installation, task); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err = tool.CreateApprovalRequest(ctx, scope, "approval", "echo", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	ledger, _ := operations.New(pool, toolSchema)
	if _, err = ledger.Accept(ctx, scope, operations.Request{Key: "unknown", Kind: "tool", Target: "remote", Arguments: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err = ledger.Dispatch(ctx, scope, "unknown"); err != nil {
		t.Fatal(err)
	}
	token, err := srv.signer.Sign(auth.Claims{Kind: auth.KindHuman, Audience: "ahp", Tenant: scope.Tenant, Subject: scope.Subject}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	code, body := srv.adminCall(t, "ControlDeleteSubject", token, scope, `{"key":"all-owners"}`)
	if code != 503 || body["state"] != "pending" {
		t.Fatalf("unknown effect deleted: %d %v", code, body)
	}
	if _, err = ids.BrowserUserBySubject(ctx, scope.Tenant, scope.Subject); err != nil {
		t.Fatal("identity proof erased before other owners settled", err)
	}
	if _, err = ledger.Get(ctx, scope, "unknown"); err != nil {
		t.Fatal("UNKNOWN evidence lost", err)
	}
	if _, err = ledger.Reconcile(ctx, scope, "unknown", []byte(`{"outcome":"known"}`), false); err != nil {
		t.Fatal(err)
	}
	consumer := stateDeletionCoordinator{state: state.New(pool), cascade: composed.cascade, control: cfg}
	if err = consumer.consumeControlDeletions(ctx); err != nil {
		t.Fatal(err)
	}
	job, err := st.GetControlDeletion(ctx, scope.Tenant, scope.Subject, "all-owners")
	if err != nil || !job.Completed {
		t.Fatalf("consumer did not finish: %+v %v", job, err)
	}
	for _, name := range requiredSubjectOwners {
		found := false
		for _, step := range job.Steps {
			if step == "owner:"+name {
				found = true
			}
		}
		if !found {
			t.Fatal("owner step missing", name)
		}
	}
	if _, err = ids.BrowserUserBySubject(ctx, scope.Tenant, scope.Subject); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatal("identity data survives", err)
	}
	if _, err = owners["compute"].(*compute.Store).GetComputer(ctx, scope, computer.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatal("compute survives", err)
	}
	if _, err = art.Get(ctx, scope, artifact); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatal("artifact survives", err)
	}
	if _, err = st.GetThread(ctx, foreign, foreignThread.ID); err != nil {
		t.Fatal("foreign thread harmed", err)
	}
	if _, err = reg.PinnedVersion(ctx, scope.Tenant, pinnedTasks[scope.Subject]); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatal("own legacy pin survived", err)
	}
	if _, err = reg.PinnedVersion(ctx, foreign.Tenant, pinnedTasks[foreign.Subject]); err != nil {
		t.Fatal("foreign legacy pin harmed", err)
	}
	if _, err = reg.GetInstallation(ctx, scope.Tenant, installation); err != nil {
		t.Fatal("shared catalog harmed", err)
	}
	if err = consumer.consumeControlDeletions(ctx); err != nil {
		t.Fatal(err)
	}
}
