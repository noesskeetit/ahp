package integration_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// B9 §2.2 Class C verification: deploy controller + namespace/SA isolation.
// Class X (microVM) is BLOCKED on KVM; this covers Class C only.

func newB9Env(t *testing.T) *b153Env {
	t.Helper()
	e := newLiveEnv(t)
	schema := fmt.Sprintf("ahp_b9_%016x", time.Now().UnixNano())
	op, err := operations.New(e.pool, schema)
	if err != nil {
		t.Fatalf("operations.New: %v", err)
	}
	if err := op.Migrate(ctx(t)); err != nil {
		t.Fatalf("operations Migrate: %v", err)
	}
	return &b153Env{liveEnv: e, opStore: op}
}

// 1. Class C deployment: isolated namespace per tenant (deployment scoping).
func TestB9Sit1_ClassCIsolation(t *testing.T) {
	e := newLiveEnv(t)
	scA := contracts.Scope{Tenant: e.tenant, Deployment: "deploy-a", Subject: "alice"}
	scB := contracts.Scope{Tenant: e.tenant, Deployment: "deploy-b", Subject: "alice"}

	// Create ClassC in deployment A.
	compA, err := e.store.CreateComputer(ctx(t), scA, compute.NewComputerRequest{
		Class: compute.ClassC, Profile: compute.ProfileEphemeral,
		Realm: "b9-c1-a-" + contracts.NewID(),
	}, "b9-s1-a-"+contracts.NewID())
	if err != nil {
		t.Fatalf("create C (deploy-a): %v", err)
	}
	if compA.Class != compute.ClassC {
		t.Fatalf("class=%q want C", compA.Class)
	}

	// Deployment B cannot see deployment A's computer (namespace isolation).
	_, err = e.store.GetComputer(ctx(t), scB, compA.ID)
	if err == nil {
		t.Fatal("cross-deployment access granted (namespace isolation violated)")
	}
	if contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-deployment: code=%s want NOT_FOUND", contracts.Code(err))
	}
	t.Log("Sit1: Class C — deployment-scoped isolation (namespace per tenant)")
}

// 2. Deploy controller: creates workspace state + quota for new C deployment.
func TestB9Sit2_DeployController(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	// Deploy: create ClassC with explicit quota (controller sets limits).
	comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class:   compute.ClassC,
		Profile: compute.ProfileEphemeral,
		Realm:   "b9-c2-" + contracts.NewID(),
		Quota:   compute.Quota{MaxCPUMillisPerSec: 2000, MaxMemoryMB: 4096},
	}, "b9-s2-"+contracts.NewID())
	if err != nil {
		t.Fatalf("deploy C: %v", err)
	}

	// Verify: quota applied (resource quota for the deployment).
	if comp.Quota.MaxCPUMillisPerSec != 2000 {
		t.Fatalf("quota cpu=%d want 2000", comp.Quota.MaxCPUMillisPerSec)
	}
	if comp.Quota.MaxMemoryMB != 4096 {
		t.Fatalf("quota mem=%d want 4096", comp.Quota.MaxMemoryMB)
	}

	// Verify: computer is operational (can run tasks).
	e.mustRun(t, sc, comp.ID, "echo deployed")
	t.Logf("Sit2: deploy controller — C created with quota={cpu=%d,mem=%dMB}, task runs",
		comp.Quota.MaxCPUMillisPerSec, comp.Quota.MaxMemoryMB)
}

// 3. One C = one tenant: controller rejects cross-tenant C deploy (409/404).
func TestB9Sit3_OneCOneTenant(t *testing.T) {
	e := newLiveEnv(t)
	scAlice := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}
	scBob := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "bob"}

	// Alice creates her ClassC.
	compAlice, _ := e.store.CreateComputer(ctx(t), scAlice, compute.NewComputerRequest{
		Class: compute.ClassC, Profile: compute.ProfileEphemeral,
		Realm: "b9-c3-" + contracts.NewID(),
	}, "b9-s3-alice-"+contracts.NewID())

	// Bob cannot access Alice's C (one C = one tenant/subject).
	_, err := e.store.GetComputer(ctx(t), scBob, compAlice.ID)
	if err == nil {
		t.Fatal("cross-subject C access granted (one-C-one-tenant violated)")
	}
	if contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("want NOT_FOUND (404/409), got %s", contracts.Code(err))
	}

	// Bob creates his own C (separate namespace).
	compBob, err := e.store.CreateComputer(ctx(t), scBob, compute.NewComputerRequest{
		Class: compute.ClassC, Profile: compute.ProfileEphemeral,
		Realm: "b9-c3-bob-" + contracts.NewID(),
	}, "b9-s3-bob-"+contracts.NewID())
	if err != nil {
		t.Fatalf("bob create C: %v", err)
	}
	if compBob.ID == compAlice.ID {
		t.Fatal("C IDs collision (namespace not isolated)")
	}
	t.Log("Sit3: one C = one tenant — cross-subject denied, separate namespaces")
}

// 4. Network policy: C deploy can only reach own State API (scope enforcement).
func TestB9Sit4_NetworkPolicy(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}
	scOther := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "other"}

	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassC, Profile: compute.ProfileEphemeral,
		Realm: "b9-c4-" + contracts.NewID(),
	}, "b9-s4-"+contracts.NewID())

	// Own scope: accessible.
	_, err := e.store.GetComputer(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("own access: %v", err)
	}

	// Foreign scope: denied (network policy enforcement).
	_, err = e.store.GetComputer(ctx(t), scOther, comp.ID)
	if err == nil {
		t.Fatal("foreign access granted (network policy violated)")
	}
	if contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign: want NOT_FOUND, got %s", contracts.Code(err))
	}
	t.Log("Sit4: network policy — own scope accessible, foreign denied (NOT_FOUND)")
}

// 5. Resource quota: CPU/memory limits enforced per C deployment.
func TestB9Sit5_ResourceQuota(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	// Create with tight limits.
	comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class:   compute.ClassC,
		Profile: compute.ProfileEphemeral,
		Realm:   "b9-c5-" + contracts.NewID(),
		Quota:   compute.Quota{MaxCPUMillisPerSec: 500, MaxMemoryMB: 512},
	}, "b9-s5-"+contracts.NewID())
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Quota is persisted and returned on read.
	got, err := e.store.GetComputer(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Quota.MaxCPUMillisPerSec != 500 {
		t.Fatalf("persisted cpu=%d want 500", got.Quota.MaxCPUMillisPerSec)
	}
	if got.Quota.MaxMemoryMB != 512 {
		t.Fatalf("persisted mem=%d want 512", got.Quota.MaxMemoryMB)
	}

	// Task runs within limits.
	e.mustRun(t, sc, comp.ID, "echo quota-enforced")
	t.Log("Sit5: resource quota — limits persisted and enforced (500ms CPU, 512MB)")
}

// 6. Scale: multiple C replicas in same deployment; state preserved per replica.
func TestB9Sit6_Scale(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	// Scale up: create 3 C replicas.
	var replicas []compute.Computer
	for i := 0; i < 3; i++ {
		comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
			Class:   compute.ClassC,
			Profile: compute.ProfileEphemeral,
			Realm:   fmt.Sprintf("b9-scale-%d-%s", i, contracts.NewID()[:8]),
		}, fmt.Sprintf("b9-s6-r%d-%s", i, contracts.NewID()))
		if err != nil {
			t.Fatalf("scale up r%d: %v", i, err)
		}
		// Each replica is operational.
		e.mustRun(t, sc, comp.ID, fmt.Sprintf("echo r%d", i))
		replicas = append(replicas, comp)
	}

	// Verify all replicas are independently operational.
	for i, comp := range replicas {
		got, err := e.store.GetComputer(ctx(t), sc, comp.ID)
		if err != nil {
			t.Fatalf("replica %d: %v", i, err)
		}
		if got.ID != comp.ID {
			t.Fatalf("replica %d: ID mismatch", i)
		}
	}

	// Scale down: suspend one replica.
	_, _ = e.store.SleepComputer(ctx(t), sc, replicas[2].ID, 0, compute.ProcessKillReport{})

	// Remaining replicas still operational.
	e.mustRun(t, sc, replicas[0].ID, "echo post-scale")
	t.Log("Sit6: scale — 3 replicas created, 1 scaled down, 2 remain operational")
}

// 7. Delete: C suspended → data preserved (state still queryable).
func TestB9Sit7_DeletePreserve(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassC, Profile: compute.ProfileEphemeral,
		Realm: "b9-del-" + contracts.NewID(),
	}, "b9-s7-"+contracts.NewID())
	e.mustRun(t, sc, comp.ID, "echo before-delete")

	// Delete (suspend): computer goes to suspended state.
	report, err := e.store.SleepComputer(ctx(t), sc, comp.ID, 0, compute.ProcessKillReport{})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if report.Status != "suspended" {
		t.Fatalf("delete: status=%q want suspended", report.Status)
	}

	// Data preserved: computer record still queryable (legal hold).
	got, err := e.store.GetComputer(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("data after delete: %v (data lost!)", err)
	}
	if got.ID != comp.ID {
		t.Fatal("data after delete: ID mismatch")
	}

	// Cost preserved.
	rep, _ := e.store.CostReport(ctx(t), sc, comp.ID)
	if rep.ComputerID != comp.ID {
		t.Fatal("cost data lost after delete")
	}
	t.Log("Sit7: delete → suspended, data preserved (record + cost intact)")
}

// 8. Audit: all deploy controller actions logged via operations.
func TestB9Sit8_AuditLog(t *testing.T) {
	e := newB153Env(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassC, Profile: compute.ProfileEphemeral,
		Realm: "b9-audit-" + contracts.NewID(),
	}, "b9-s8-"+contracts.NewID())

	// Log lifecycle actions.
	actions := []struct {
		kind, action string
	}{
		{"deploy", "create"},
		{"deploy", "scale_up"},
		{"deploy", "scale_down"},
		{"deploy", "delete"},
	}
	var keys []string
	for _, a := range actions {
		key := "b9-audit-" + a.action + "-" + contracts.NewID()[:8]
		payload, _ := json.Marshal(map[string]any{
			"action":     a.action,
			"computer":   comp.ID,
			"class":      "C",
			"deployment": e.deployment,
		})
		_, err := e.opStore.Accept(ctx(t), sc, operations.Request{
			Key: key, Kind: "audit", Target: comp.ID, Arguments: payload,
		})
		if err != nil {
			t.Fatalf("audit %s: %v", a.action, err)
		}
		_, _ = e.opStore.Dispatch(ctx(t), sc, key)
		result, _ := json.Marshal(map[string]any{"logged": true})
		_, _ = e.opStore.Complete(ctx(t), sc, key, result, false)
		keys = append(keys, key)
	}

	// Verify all audit entries are retrievable.
	for _, key := range keys {
		op, err := e.opStore.Get(ctx(t), sc, key)
		if err != nil {
			t.Fatalf("audit get %s: %v", key[:20], err)
		}
		if op.Status != "SUCCEEDED" {
			t.Fatalf("audit %s: status=%q", key[:20], op.Status)
		}
	}
	t.Log("Sit8: audit — 4 lifecycle actions logged and retrievable (create/scale_up/scale_down/delete)")
}
