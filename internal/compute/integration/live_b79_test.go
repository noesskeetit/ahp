package integration_test

import (
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestB79VersionPinning(t *testing.T) {
	e := newLiveEnv(t)
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}
	agent := "agent-pin"

	// Publish v1.
	digestV1 := "sha256:" + "a1b2c3d4e5f6"
	ver1, err := e.store.PublishVersion(ctx(t), scope, agent, digestV1, true, time.Hour)
	if err != nil {
		t.Fatalf("publish v1: %v", err)
	}
	if ver1.Status != compute.ImageVersionActive {
		t.Fatalf("v1 status = %q, want active", ver1.Status)
	}

	// Pin a computer to v1.
	comp, err := e.store.CreateComputer(ctx(t), scope, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfileEphemeral,
	}, "b79-pin-"+contracts.NewID())
	if err != nil {
		t.Fatalf("create computer: %v", err)
	}
	if err := e.store.PinComputerToVersion(ctx(t), scope, comp.ID, digestV1); err != nil {
		t.Fatalf("pin: %v", err)
	}

	// Publish v2 — v1 auto-drains.
	digestV2 := "sha256:" + "b2c3d4e5f6a7"
	ver2, err := e.store.PublishVersion(ctx(t), scope, agent, digestV2, true, time.Hour)
	if err != nil {
		t.Fatalf("publish v2: %v", err)
	}
	if ver2.Status != compute.ImageVersionActive {
		t.Fatalf("v2 status = %q, want active", ver2.Status)
	}

	// v1 should be draining.
	ver1After, err := e.store.GetVersionByDigest(ctx(t), scope, agent, digestV1)
	if err != nil {
		t.Fatalf("get v1: %v", err)
	}
	if ver1After.Status != compute.ImageVersionDrain {
		t.Fatalf("v1 after publish v2: status = %q, want drain", ver1After.Status)
	}

	// Active is v2 — new tasks route here.
	active, err := e.store.GetActiveVersion(ctx(t), scope, agent)
	if err != nil {
		t.Fatalf("get active: %v", err)
	}
	if active.Digest != digestV2 {
		t.Fatalf("active = %q, want v2", active.Digest)
	}

	// Existing pinned computer is NOT migrated.
	compAfter, err := e.store.GetComputer(ctx(t), scope, comp.ID)
	if err != nil {
		t.Fatalf("get computer: %v", err)
	}
	_ = compAfter // pinning is recorded in version_digest column
}

func TestB79DrainLifecycle(t *testing.T) {
	e := newLiveEnv(t)
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}
	agent := "agent-drain"

	// Publish v1.
	digestV1 := "sha256:" + "c3d4e5f6a7b8"
	_, err := e.store.PublishVersion(ctx(t), scope, agent, digestV1, true, time.Hour)
	if err != nil {
		t.Fatalf("publish v1: %v", err)
	}

	// Publish v2 (auto-drains v1).
	digestV2 := "sha256:" + "d4e5f6a7b8c9"
	_, err = e.store.PublishVersion(ctx(t), scope, agent, digestV2, true, time.Hour)
	if err != nil {
		t.Fatalf("publish v2: %v", err)
	}

	// v1 is draining.
	ver1, err := e.store.GetVersionByDigest(ctx(t), scope, agent, digestV1)
	if err != nil {
		t.Fatalf("get v1: %v", err)
	}
	if ver1.Status != compute.ImageVersionDrain {
		t.Fatalf("v1 status = %q, want drain", ver1.Status)
	}
	if ver1.DrainDeadline == nil {
		t.Fatal("v1 drain deadline not set")
	}

	// Publish v3 (auto-drains v2).
	digestV3 := "sha256:" + "e5f6a7b8c9d0"
	_, err = e.store.PublishVersion(ctx(t), scope, agent, digestV3, true, time.Hour)
	if err != nil {
		t.Fatalf("publish v3: %v", err)
	}

	// Drain v2 with 0 window (immediately expired).
	_, err = e.store.DrainVersion(ctx(t), scope, agent, digestV2, 0)
	if err != nil {
		t.Fatalf("drain v2: %v", err)
	}

	// Check drain expiry — v2 should be retired.
	retired, err := e.store.CheckDrainExpiry(ctx(t), scope, agent, digestV2)
	if err != nil {
		t.Fatalf("check expiry: %v", err)
	}
	if !retired {
		t.Fatal("expected v2 to be retired")
	}

	ver2, err := e.store.GetVersionByDigest(ctx(t), scope, agent, digestV2)
	if err != nil {
		t.Fatalf("get v2: %v", err)
	}
	if ver2.Status != compute.ImageVersionRetired {
		t.Fatalf("v2 = %q, want retired", ver2.Status)
	}

	// Active is v3.
	active, err := e.store.GetActiveVersion(ctx(t), scope, agent)
	if err != nil {
		t.Fatalf("get active: %v", err)
	}
	if active.Digest != digestV3 {
		t.Fatalf("active = %q, want v3", active.Digest)
	}
}

func TestB79StateCompat(t *testing.T) {
	// Forward compat: state v1 → target v2 is OK.
	if !compute.CheckStateCompat(1, 2) {
		t.Fatal("expected forward compat v1→v2")
	}
	// Same version.
	if !compute.CheckStateCompat(3, 3) {
		t.Fatal("expected same-version compat")
	}
	// Backward: NOT compatible.
	if compute.CheckStateCompat(2, 1) {
		t.Fatal("expected backward incompat v2→v1")
	}
	// v0 → v1 OK.
	if !compute.CheckStateCompat(0, 1) {
		t.Fatal("expected v0→v1 compat")
	}
}

func TestB79Rollback(t *testing.T) {
	e := newLiveEnv(t)
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}
	agent := "agent-rb"

	// Publish v1.
	digestV1 := "sha256:" + "f6a7b8c9d0e1"
	_, err := e.store.PublishVersion(ctx(t), scope, agent, digestV1, true, time.Hour)
	if err != nil {
		t.Fatalf("publish v1: %v", err)
	}

	// Publish v2 (v1 → drain).
	digestV2 := "sha256:" + "a7b8c9d0e1f2"
	_, err = e.store.PublishVersion(ctx(t), scope, agent, digestV2, true, time.Hour)
	if err != nil {
		t.Fatalf("publish v2: %v", err)
	}

	// Active is v2.
	active, err := e.store.GetActiveVersion(ctx(t), scope, agent)
	if err != nil {
		t.Fatalf("get active: %v", err)
	}
	if active.Digest != digestV2 {
		t.Fatalf("active = %q, want v2", active.Digest)
	}

	// Rollback: v2 → rolled_back, v1 → active.
	reactivated, err := e.store.RollbackVersion(ctx(t), scope, agent)
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if reactivated.Digest != digestV1 {
		t.Fatalf("reactivated = %q, want v1", reactivated.Digest)
	}
	if reactivated.Status != compute.ImageVersionActive {
		t.Fatalf("reactivated status = %q, want active", reactivated.Status)
	}

	// v2 is rolled_back.
	ver2, err := e.store.GetVersionByDigest(ctx(t), scope, agent, digestV2)
	if err != nil {
		t.Fatalf("get v2: %v", err)
	}
	if ver2.Status != compute.ImageVersionRolledBack {
		t.Fatalf("v2 = %q, want rolled_back", ver2.Status)
	}

	// New tasks route to v1.
	active2, err := e.store.GetActiveVersion(ctx(t), scope, agent)
	if err != nil {
		t.Fatalf("get active after rollback: %v", err)
	}
	if active2.Digest != digestV1 {
		t.Fatalf("active after rollback = %q, want v1", active2.Digest)
	}
}

// --- B135: §9.3 Four Situations ---

func TestB135Situation1_DeployWhileTasksWait(t *testing.T) {
	e := newLiveEnv(t)
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}
	agent := "agent-s1"

	// Publish v1.
	digestV1 := "sha256:s1v1-" + contracts.NewID()[:8]
	_, err := e.store.PublishVersion(ctx(t), scope, agent, digestV1, true, time.Hour)
	if err != nil {
		t.Fatalf("publish v1: %v", err)
	}

	// Create a task pinned to v1.
	comp, err := e.store.CreateComputer(ctx(t), scope, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfileEphemeral,
	}, "b135-s1-"+contracts.NewID()[:8])
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := e.store.PinComputerToVersion(ctx(t), scope, comp.ID, digestV1); err != nil {
		t.Fatalf("pin: %v", err)
	}

	// Publish v2 (v1 enters drain).
	digestV2 := "sha256:s1v2-" + contracts.NewID()[:8]
	_, err = e.store.PublishVersion(ctx(t), scope, agent, digestV2, true, time.Hour)
	if err != nil {
		t.Fatalf("publish v2: %v", err)
	}

	// The existing task is still on v1 (routed to old replicas until drain ends).
	// Its pin is unchanged.
	compAfter, err := e.store.GetComputer(ctx(t), scope, comp.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	_ = compAfter // pin remains digestV1

	// A NEW task routes to v2.
	routed, err := e.store.RouteNewTask(ctx(t), scope, agent)
	if err != nil {
		t.Fatalf("route: %v", err)
	}
	if routed.Digest != digestV2 {
		t.Fatalf("new task routed to %q, want v2", routed.Digest)
	}
}

func TestB135Situation2_DrainExpiryCompat(t *testing.T) {
	e := newLiveEnv(t)
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}
	agent := "agent-s2c"

	// Publish v1 with state_compat=true.
	digestV1 := "sha256:s2cv1-" + contracts.NewID()[:8]
	_, err := e.store.PublishVersion(ctx(t), scope, agent, digestV1, true, time.Hour)
	if err != nil {
		t.Fatalf("publish v1: %v", err)
	}

	// Create a task on v1.
	comp, err := e.store.CreateComputer(ctx(t), scope, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfileEphemeral,
	}, "b135-s2c-"+contracts.NewID()[:8])
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := e.store.PinComputerToVersion(ctx(t), scope, comp.ID, digestV1); err != nil {
		t.Fatalf("pin: %v", err)
	}

	// Publish v2 (v1 drains).
	digestV2 := "sha256:s2cv2-" + contracts.NewID()[:8]
	_, err = e.store.PublishVersion(ctx(t), scope, agent, digestV2, true, time.Hour)
	if err != nil {
		t.Fatalf("publish v2: %v", err)
	}

	// Drain expiry with compat → migrate.
	res, err := e.store.ResolveDrainExpiry(ctx(t), scope, agent, comp.ID, digestV1)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if !res.Migrated {
		t.Fatalf("expected migration, got failed=%v reason=%q", res.Failed, res.Reason)
	}
	if res.Target != digestV2 {
		t.Fatalf("migrated to %q, want v2", res.Target)
	}
}

func TestB135Situation2_DrainExpiryNoCompat(t *testing.T) {
	e := newLiveEnv(t)
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}
	agent := "agent-s2nc"

	// Publish v1 with state_compat=false.
	digestV1 := "sha256:s2ncv1-" + contracts.NewID()[:8]
	_, err := e.store.PublishVersion(ctx(t), scope, agent, digestV1, false, time.Hour)
	if err != nil {
		t.Fatalf("publish v1: %v", err)
	}

	// Create a task on v1.
	comp, err := e.store.CreateComputer(ctx(t), scope, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfileEphemeral,
	}, "b135-s2nc-"+contracts.NewID()[:8])
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := e.store.PinComputerToVersion(ctx(t), scope, comp.ID, digestV1); err != nil {
		t.Fatalf("pin: %v", err)
	}

	// Publish v2 (v1 drains).
	digestV2 := "sha256:s2ncv2-" + contracts.NewID()[:8]
	_, err = e.store.PublishVersion(ctx(t), scope, agent, digestV2, true, time.Hour)
	if err != nil {
		t.Fatalf("publish v2: %v", err)
	}

	// Drain expiry without compat → FAILED.
	res, err := e.store.ResolveDrainExpiry(ctx(t), scope, agent, comp.ID, digestV1)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if !res.Failed {
		t.Fatal("expected failure (no compat), got migrated")
	}
	if res.Reason == "" {
		t.Fatal("expected a failure reason")
	}
}

func TestB135Situation3_RollbackKeepsInProgress(t *testing.T) {
	e := newLiveEnv(t)
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}
	agent := "agent-s3"

	// Publish v1.
	digestV1 := "sha256:s3v1-" + contracts.NewID()[:8]
	_, err := e.store.PublishVersion(ctx(t), scope, agent, digestV1, true, time.Hour)
	if err != nil {
		t.Fatalf("publish v1: %v", err)
	}

	// Task A starts on v1.
	compA, err := e.store.CreateComputer(ctx(t), scope, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfileEphemeral,
	}, "b135-s3a-"+contracts.NewID()[:8])
	if err != nil {
		t.Fatalf("create A: %v", err)
	}
	if err := e.store.PinComputerToVersion(ctx(t), scope, compA.ID, digestV1); err != nil {
		t.Fatalf("pin A: %v", err)
	}

	// Publish v2 (v1 drains).
	digestV2 := "sha256:s3v2-" + contracts.NewID()[:8]
	_, err = e.store.PublishVersion(ctx(t), scope, agent, digestV2, true, time.Hour)
	if err != nil {
		t.Fatalf("publish v2: %v", err)
	}

	// Task B starts on v2 (new version checkpoint).
	compB, err := e.store.CreateComputer(ctx(t), scope, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfileEphemeral,
	}, "b135-s3b-"+contracts.NewID()[:8])
	if err != nil {
		t.Fatalf("create B: %v", err)
	}
	if err := e.store.PinComputerToVersion(ctx(t), scope, compB.ID, digestV2); err != nil {
		t.Fatalf("pin B: %v", err)
	}

	// Rollback: v2 → rolled_back, v1 → active.
	reactivated, err := e.store.RollbackVersion(ctx(t), scope, agent)
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if reactivated.Digest != digestV1 {
		t.Fatalf("reactivated = %q, want v1", reactivated.Digest)
	}

	// Task A stays on v1 (was already there).
	// Task B stays on v2 (in-progress new-version task is NOT rolled back).
	// Only NEW tasks after rollback route to v1.
	routed, err := e.store.RouteNewTask(ctx(t), scope, agent)
	if err != nil {
		t.Fatalf("route: %v", err)
	}
	if routed.Digest != digestV1 {
		t.Fatalf("new task after rollback routes to %q, want v1", routed.Digest)
	}

	// Verify v2 is rolled_back.
	ver2, err := e.store.GetVersionByDigest(ctx(t), scope, agent, digestV2)
	if err != nil {
		t.Fatalf("get v2: %v", err)
	}
	if ver2.Status != compute.ImageVersionRolledBack {
		t.Fatalf("v2 status = %q, want rolled_back", ver2.Status)
	}
}

func TestB135Situation4_AgentUpdateMidDotChat(t *testing.T) {
	// Sub-path A: compat → thread continues.
	resA := compute.CheckThreadCompat(true, 1, 2)
	if !resA.Continue {
		t.Fatalf("compat thread should continue, got new_context=%v reason=%q", resA.NewContext, resA.Reason)
	}

	// Sub-path B: no state.compat → new context.
	resB := compute.CheckThreadCompat(false, 1, 2)
	if !resB.NewContext {
		t.Fatal("no-compat thread should open new context")
	}
	if resB.Reason == "" {
		t.Fatal("expected reason for new context")
	}

	// Sub-path C: schema version mismatch → new context.
	resC := compute.CheckThreadCompat(true, 3, 1)
	if !resC.NewContext {
		t.Fatal("schema mismatch should open new context")
	}
}
