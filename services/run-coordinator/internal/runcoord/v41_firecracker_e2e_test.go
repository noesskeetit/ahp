//go:build linux

package runcoord

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/vmruntime"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	state "github.com/ahp-platform/ahp/sdk/go/state/journal"
	sdktools "github.com/ahp-platform/ahp/sdk/go/tools"
)

func TestV41FirecrackerToolGatewayPostgres(t *testing.T) {
	if os.Getenv("AHP_FC_E2E") != "1" {
		t.Skip("requires Firecracker with KVM and PostgreSQL")
	}
	root := os.Getenv("AHP_FC_E2E_CGROUP")
	if !filepath.IsAbs(root) || os.Getenv("AHP_JOURNAL_TEST_DATABASE_URL") == "" {
		t.Fatal("configure the disposable PostgreSQL database and cgroup directory")
	}
	f := &v41GatewayFixture{}
	var a, b contracts.Scope
	var dataDir, cgroupDir string
	p := setupV32RuntimeIntegration(t, func(tenant string) []contracts.Scope {
		a = contracts.Scope{Tenant: tenant + "-a", Deployment: "fc-a", Subject: "alice"}
		b = contracts.Scope{Tenant: tenant + "-b", Deployment: "fc-b", Subject: "bob"}
		return []contracts.Scope{a, b}
	}, v41Conversation, func(t *testing.T, dir string) (vmruntime.Driver, string) {
		dataDir = filepath.Join(dir, "v")
		cgroupDir = filepath.Join(root, filepath.Base(dir))
		for _, path := range []string{dataDir, cgroupDir} {
			if err := os.Mkdir(path, 0700); err != nil {
				t.Fatal(err)
			}
		}
		t.Cleanup(func() {
			if err := os.Remove(cgroupDir); err != nil {
				t.Error(err)
			}
		})
		t.Setenv("AHP_COMPUTE_RUNTIME", "firecracker")
		t.Setenv("AHP_FC_DATADIR", dataDir)
		t.Setenv("AHP_FC_TAP", "")
		t.Setenv("AHP_FC_GUEST_MAC", "")
		driver, err := vmruntime.FirecrackerDriver(cgroupDir)
		if err != nil {
			t.Fatal(err)
		}
		return driver, ""
	}, f)
	frozen, recovered, warm := v41Execution(a, 1, 1), v41Execution(a, 1, 2), v41Execution(b, 1, 1)
	cleaned := false
	defer func() {
		if !cleaned {
			for _, e := range []vmruntime.Execution{recovered, frozen, warm} {
				p.cleanupExecution(t, e)
			}
		}
	}()
	stop := v41Reconcile(p)
	defer stop()
	p.send(t, a, "initial", journal.DeliveryTurn, 0, processInput{Mode: "lost-receipt"})
	p.send(t, b, "initial", journal.DeliveryTurn, 0, processInput{Mode: "warm-first"})
	v41WaitState(t, p, b, 1, "ACTIVE")
	p.wait(t, "warm intent committed before admitting follow-up", func() bool {
		return v41ReadIntent(p, b, "warm-first").Status == journal.IntentPrepared
	})
	warmGuest := fcAttempt(t, p, warm)
	p.send(t, b, "followup", journal.DeliveryFollowup, 0, processInput{Mode: "warm-second"})
	p.wait(t, "owner committed before dropped reply", f.lost.Load)
	frozenGuest := fcAttempt(t, p, frozen)
	v41WaitState(t, p, a, 2, "IDLE")
	v41WaitState(t, p, b, 1, "IDLE")
	v41AssertOutcome(t, p, f, a, "lost-receipt", journal.IntentCompleted, 3)
	v41AssertOutcome(t, p, f, b, "warm-first", journal.IntentCompleted, 3)
	v41AssertOutcome(t, p, f, b, "warm-second", journal.IntentCompleted, 3)
	v41AssertSSE(t, p, a, 1, recovered.ID)
	primary := processStreamOutput(t, p.request(t, b, http.MethodGet, "/stream?turn_seq=1", ""))
	stateB, err := p.state.GetEvents(p.ctx, b, warm.SessionID, 0, 128)
	if err != nil {
		t.Fatal(err)
	}
	var followup int64
	for _, e := range stateB.Events {
		if e.Type == "turn.output" && e.TurnSeq != 1 {
			followup = e.TurnSeq
		}
	}
	if followup == 0 {
		t.Fatal("no warm output in journal")
	}
	v41AssertSSE(t, p, b, followup, warm.ID)
	if primary.Execution != warm.ID || fcAttempt(t, p, warm).Attempt != warmGuest.Attempt {
		t.Fatal("warm execution relaunched")
	}
	grant, err := p.identity.Get(p.ctx, a, frozen.ID)
	if err != nil || grant.Status != identity.StatusRevoked {
		t.Fatalf("old grant not revoked: %v", err)
	}
	if running, err := p.driver.Alive(p.ctx, frozenGuest); err != nil || running || p.injector.Listening(frozenGuest) {
		t.Fatalf("old guest remains connected: %t %v", running, err)
	}
	stop()
	for _, e := range []vmruntime.Execution{frozen, recovered, warm} {
		p.cleanupExecution(t, e)
	}
	cleaned = true
	for _, path := range []string{dataDir, cgroupDir} {
		files, err := os.ReadDir(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range files {
			if f.IsDir() {
				t.Errorf("leaked Firecracker attempt: %s", filepath.Join(path, f.Name()))
			}
		}
	}
	t.Log("real Firecracker: two isolated tenant deployments, warm proof renewal, revoked generation recovered the original receipt, 3 MCP invocations total, SSE cursor replay and cleanup")
}

func TestV41FirecrackerGuest(t *testing.T) {
	if os.Getenv("AHP_FC_E2E_GUEST") != "1" {
		t.Skip("started inside Firecracker")
	}
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		switch key {
		case "AHP_FC_E2E_GUEST", "PATH", "HOME", "TERM":
		default:
			t.Fatalf("guest environment leaked %s", key)
		}
	}
	interfaces, err := os.ReadDir("/sys/class/net")
	if err != nil || len(interfaces) != 1 || interfaces[0].Name() != "lo" {
		t.Fatalf("unexpected network interfaces: %v %v", interfaces, err)
	}
	port := journal.RuntimeInjectorVSockPort
	sdk, err := state.NewVSockInjectorClient(port)
	if err != nil {
		t.Fatal(err)
	}
	gw, err := sdktools.NewVSockInjectorClient(port)
	if err != nil {
		t.Fatal(err)
	}
	if err := runV41Guest(t.Context(), sdk, gw); err != nil {
		t.Fatal(err)
	}
}
