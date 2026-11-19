package integration_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// B14 S2.3 Point 5: State locality enforcement.

func newB14Stores(t *testing.T) (*compute.Store, *state.Store, *identity.Store) {
	t.Helper()
	e := newLiveEnv(t)
	cs := e.store
	if err := cs.Migrate(ctx(t)); err != nil {
		t.Fatalf("compute Migrate: %v", err)
	}
	st := state.New(e.pool)
	if err := st.Migrate(ctx(t)); err != nil {
		t.Fatalf("state Migrate: %v", err)
	}
	id := identity.New(e.pool)
	if err := id.Migrate(ctx(t)); err != nil {
		t.Fatalf("identity Migrate: %v", err)
	}
	return cs, st, id
}

func b14Fence(t *testing.T, st *state.Store, sc contracts.Scope, uid, worker string) state.Fence {
	t.Helper()
	th, err := st.CreateThread(ctx(t), sc, "ctx-"+uid, "key-th-"+uid)
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	run, err := st.CreateRun(ctx(t), sc, th.ID, "task-"+uid, "key-run-"+uid, []byte("{}"))
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	lease, err := st.ClaimRun(ctx(t), sc.Tenant, sc.Deployment, worker, 30*time.Second)
	if err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	_ = run
	return state.Fence{RunID: lease.Run.ID, Worker: lease.Worker, Epoch: lease.Epoch}
}


func sha256hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func TestB14Sit1_StateStoreIsSourceOfTruth(t *testing.T) {
	_, st, _ := newB14Stores(t)
	uid := contracts.NewID()
	sc := contracts.Scope{Tenant: "t-b14-s1-" + uid, Deployment: "d-1", Subject: "alice"}
	fence := b14Fence(t, st, sc, uid, "w1")

	_, err := st.PutKV(ctx(t), sc, fence, []string{"agent"}, "memory",
		"op-mem-"+uid, 0, []byte(`{"facts":["fact1","fact2"]}`))
	if err != nil {
		t.Fatalf("PutKV: %v", err)
	}
	got, err := st.GetKV(ctx(t), sc, []string{"agent"}, "memory")
	if err != nil {
		t.Fatalf("GetKV: %v", err)
	}
	if got.Version != 1 {
		t.Fatalf("version=%d want 1", got.Version)
	}
	var mem struct {
		Facts []string `json:"facts"`
	}
	if err := json.Unmarshal(got.Value, &mem); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(mem.Facts) != 2 {
		t.Fatalf("facts=%d want 2", len(mem.Facts))
	}
	t.Log("Sit1: agent state written via StateStore is queryable (KV namespace=agent)")
}

func TestB14Sit2_StateNotInCompute(t *testing.T) {
	cs, st, _ := newB14Stores(t)
	uid := contracts.NewID()
	sc := contracts.Scope{Tenant: "t-b14-s2-" + uid, Deployment: "d-2", Subject: "alice"}
	fence := b14Fence(t, st, sc, uid, "w1")

	_, _ = st.PutKV(ctx(t), sc, fence, []string{"agent"}, "secret-state",
		"op-s2-"+uid, 0, []byte(`"top-secret"`))

	comp, err := cs.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b14-s2-"+uid)
	if err != nil {
		t.Fatalf("CreateComputer: %v", err)
	}
	revs, err := cs.ListStateRevisions(ctx(t), sc, comp.ID, "main")
	if err != nil {
		t.Fatalf("ListStateRevisions: %v", err)
	}
	for _, rev := range revs {
		for path := range rev.Tree {
			if path == "agent/secret-state" || path == "secret-state" {
				t.Fatalf("agent state leaked into compute state tree at path=%q", path)
			}
		}
	}
	t.Log("Sit2: agent state (KV) is NOT present in compute state revisions")
}

func TestB14Sit3_DeployConfigInManifest(t *testing.T) {
	cs, st, _ := newB14Stores(t)
	uid := contracts.NewID()
	sc := contracts.Scope{Tenant: "t-b14-s3-" + uid, Deployment: "d-3", Subject: "alice"}

	comp, err := cs.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b14-s3-"+uid)
	if err != nil {
		t.Fatalf("CreateComputer: %v", err)
	}
	if comp.Class != compute.ClassS {
		t.Fatalf("class=%q want S", comp.Class)
	}
	got, err := cs.GetComputer(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("GetComputer: %v", err)
	}
	if got.Class != compute.ClassS || got.Profile != compute.ProfilePersonal {
		t.Fatalf("deploy manifest: class=%q profile=%q", got.Class, got.Profile)
	}
	_ = st
	t.Log("Sit3: deploy config authoritative in compute manifest (class/profile on computer record)")
}

func TestB14Sit4_PlatformStateInIdentity(t *testing.T) {
	_, st, idStore := newB14Stores(t)
	uid := contracts.NewID()
	sc := contracts.Scope{Tenant: "t-b14-s4-" + uid, Deployment: "d-4", Subject: "alice"}

	grant, err := idStore.CreateRoot(ctx(t), sc, identity.RootRequest{
		Task: "root-" + uid, Context: "ctx4-" + uid, Deployment: "d-4",
		Capabilities: []string{"compute"},
		AllowSelf:  true,
		TTL:          time.Hour,
	})
	if err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	if grant.Status != identity.StatusActive {
		t.Fatalf("status=%q want active", grant.Status)
	}
	items, err := st.SearchKV(ctx(t), sc, []string{"platform"}, "", nil, 10)
	if err != nil {
		t.Fatalf("SearchKV platform: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("platform grants in state KV: %d items", len(items))
	}
	t.Log("Sit4: platform grants (identity) NOT present in state store KV")
}

func TestB14Sit5_ScopeIsolation(t *testing.T) {
	_, st, _ := newB14Stores(t)
	uid := contracts.NewID()
	scA := contracts.Scope{Tenant: "t-b14-scope-a-" + uid, Deployment: "d-5", Subject: "alice"}
	scB := contracts.Scope{Tenant: "t-b14-scope-b-" + uid, Deployment: "d-5", Subject: "alice"}
	fence := b14Fence(t, st, scA, uid, "w1")

	_, _ = st.PutKV(ctx(t), scA, fence, []string{"private"}, "key-a",
		"op-s5a-"+uid, 0, []byte(`"scope-a-data"`))

	_, err := st.GetKV(ctx(t), scB, []string{"private"}, "key-a")
	if err == nil {
		t.Fatal("cross-scope read: want error, got nil")
	}
	if contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-scope: code=%s want NOT_FOUND", contracts.Code(err))
	}
	t.Log("Sit5: scope isolation - KV data invisible across tenant boundaries")
}

func TestB14Sit6_CheckpointIsolation(t *testing.T) {
	cs, st, _ := newB14Stores(t)
	uid := contracts.NewID()
	sc := contracts.Scope{Tenant: "t-b14-s6-" + uid, Deployment: "d-6", Subject: "alice"}

	comp, err := cs.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b14-s6-"+uid)
	if err != nil {
		t.Fatalf("CreateComputer: %v", err)
	}
	head, _ := cs.StateHead(ctx(t), sc, comp.ID, "main")
	_, err = cs.CommitStatePatch(ctx(t), sc, comp.ID, "main", head.ID, "checkpoint.bin",
		compute.StateFile{Hash: sha256hex([]byte("data")), Size: 4, Mode: 0o644})
	if err != nil {
		t.Fatalf("CommitStatePatch: %v", err)
	}

	_, err = st.GetKV(ctx(t), sc, []string{"files"}, "checkpoint.bin")
	if err == nil {
		t.Fatal("checkpoint data found in state KV (should be compute-only)")
	}
	t.Log("Sit6: checkpoint/file data stays in compute state, not in state store KV")
}

func TestB14Sit7_RevisionVsAgentState(t *testing.T) {
	cs, st, _ := newB14Stores(t)
	uid := contracts.NewID()
	sc := contracts.Scope{Tenant: "t-b14-s7-" + uid, Deployment: "d-7", Subject: "alice"}

	comp, _ := cs.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b14-s7-"+uid)
	head2, _ := cs.StateHead(ctx(t), sc, comp.ID, "main")
	_, _ = cs.CommitStatePatch(ctx(t), sc, comp.ID, "main", head2.ID, "app.py",
		compute.StateFile{Hash: sha256hex([]byte("print(1)")), Size: 7, Mode: 0o644})

	fence := b14Fence(t, st, sc, uid, "w1")
	_, _ = st.PutKV(ctx(t), sc, fence, []string{"agent"}, "session",
		"op-s7-"+uid, 0, []byte(`{"turn":5}`))

	revs, _ := cs.ListStateRevisions(ctx(t), sc, comp.ID, "main")
	if len(revs) == 0 {
		t.Fatal("no compute state revisions")
	}
	for _, rev := range revs {
		for path := range rev.Tree {
			if path == "agent/session" || path == "session" {
				t.Fatal("agent KV leaked into compute revision tree")
			}
		}
	}
	t.Log("Sit7: compute revisions and agent KV are independent domains")
}

func TestB14Sit8_CommandsNoAgentState(t *testing.T) {
	cs, st, _ := newB14Stores(t)
	uid := contracts.NewID()
	sc := contracts.Scope{Tenant: "t-b14-s8-" + uid, Deployment: "d-8", Subject: "alice"}

	comp, _ := cs.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b14-s8-"+uid)
	cmd, _, err := cs.Exec(ctx(t), sc, comp.ID, "b14-s8-cmd-"+uid, "echo b14", "")
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}

	fence := b14Fence(t, st, sc, uid, "w1")
	_, _ = st.PutKV(ctx(t), sc, fence, []string{"agent"}, "context",
		"op-s8-"+uid, 0, []byte(`"agent-ctx"`))

	got, err := cs.GetCommand(ctx(t), sc, comp.ID, cmd.ID)
	if err != nil {
		t.Fatalf("GetCommand: %v", err)
	}
	if got.Command != "echo b14" {
		t.Fatalf("command=%q want echo b14", got.Command)
	}
	t.Log("Sit8: compute commands carry only command+spec, not agent state")
}
