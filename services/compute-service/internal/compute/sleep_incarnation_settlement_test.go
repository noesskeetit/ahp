package compute

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The root audit named this defect (10-compute.md item 1): "Sleep ... kills the
// host processes and changes the status, but does not close the control
// incarnation", and the acceptance line was "close Sleep -> incarnation
// settlement: a manual StopIncarnation after an ordinary Sleep must not be
// required, and the export guard must not be weakened".
//
// The consequence was not cosmetic. The export guard answers "is this source
// quiescent" by asking whether ANY incarnation of the Computer is not stopped, so
// a Computer that had been put to sleep through the documented path could never
// export - the user had to stop a row they were never told about. The fix settles
// the incarnation inside the sleep transaction; this test measures both halves of
// that claim:
//
//  1. an ordinary Runner.Sleep settles the open incarnation (read out of band from
//     ahp_compute.incarnations, not from the answer) and reports the count;
//  2. the export guard's quiescence check therefore passes after the sleep with NO
//     manual edit of any table - and goes busy again on the next Wake, which is the
//     proof that the settlement did not simply mark the ledger stopped forever;
//  3. the wake after a sleep is a NEW incarnation on the SAME (mock) backend, so
//     settling the old one does not lose the class binding.
//
// Nothing here is evidence about a hypervisor: class X is bound to the MOCK backend
// (measured: /dev/kvm is EACCES for uid 1000 on this Linux polygon), which
// simulates the lifecycle and executes nothing.
func TestSleepSettlesTheControlIncarnationAndOpensExport(t *testing.T) {
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		t.Fatal("isolated real PostgreSQL required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := New(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	redis := NewRedis(os.Getenv("AHP_REDIS_ADDR"))
	if err := redis.Ping(ctx); err != nil {
		t.Fatal("real Redis required: the sleep gate is the lease count: " + err.Error())
	}
	leases, err := NewLeaseService(store, redis)
	if err != nil {
		t.Fatal(err)
	}
	backends := NewRegistry(NewMockBackend())
	backends.SetClassBackend(ClassX, MockBackendName)
	runner := NewRunner(store, backends, t.TempDir(), "h22-sleep-node", nil).WithLeaseService(leases)

	scope := contracts.Scope{Tenant: "h22-sleep-" + contracts.NewID(), Deployment: "compute", Subject: "owner"}
	computer, err := store.CreateComputer(ctx, scope, NewComputerRequest{Class: ClassX, Profile: ProfileEphemeral}, "sleep-settle")
	if err != nil {
		t.Fatal(err)
	}
	incarnationStatus := func(id string) string {
		t.Helper()
		var status string
		if err := pool.QueryRow(ctx, `SELECT status FROM ahp_compute.incarnations WHERE id=$1`, id).Scan(&status); err != nil {
			t.Fatalf("out-of-band incarnation read: %v", err)
		}
		return status
	}
	// quiescent asks the SAME question an export asks. The guard is entered before
	// any export bytes move, and entering it IS the quiescence decision, so a
	// refusal to begin is the honest "not quiescent" answer rather than a failure
	// of this fixture.
	quiescent := func() (bool, string) {
		t.Helper()
		guard, err := store.beginExportGuard(ctx, scope, computer.ID)
		if err != nil {
			if contracts.ReasonOf(err) == "export_source_busy" {
				return false, err.Error()
			}
			t.Fatalf("beginExportGuard refused for a reason that is not quiescence: %v", err)
		}
		if err := guard.check(ctx); err != nil {
			defer guard.close()
			return false, err.Error()
		}
		guard.close()
		return true, ""
	}

	awake, err := runner.Wake(ctx, scope, computer.ID)
	if err != nil {
		t.Fatal(err)
	}
	if awake.Backend != MockBackendName {
		t.Fatalf("fixture woke on backend %q, want the mock", awake.Backend)
	}
	if status := incarnationStatus(awake.ID); status == "stopped" {
		t.Fatalf("the incarnation an awake Computer reports is already stopped: %q", status)
	}
	if ok, why := quiescent(); ok {
		t.Fatal("the export guard called a live Computer quiescent before the sleep: the guard it protects is not the one under test")
	} else if !strings.Contains(why, "quiescent") {
		t.Fatalf("awake export refusal is not the quiescence refusal: %s", why)
	}

	// The ordinary path: Sleep only. No StopIncarnation, no UPDATE by hand.
	report, err := runner.Sleep(ctx, scope, computer.ID)
	if err != nil {
		t.Fatalf("Sleep: %v", err)
	}
	if report.IncarnationsStopped != 1 {
		t.Fatalf("sleep settled %d incarnations, want 1: %+v", report.IncarnationsStopped, report)
	}
	if status := incarnationStatus(awake.ID); status != "stopped" {
		t.Fatalf("after an ordinary Sleep the control incarnation is %q: the sleep still leaves the source live and the export unreachable without a manual row edit", status)
	}
	if ok, why := quiescent(); !ok {
		t.Fatalf("the source is still not exportable after a normal sleep (%s): the defect is open", why)
	}

	// And the settlement is per incarnation, not a blanket "everything is stopped".
	woken, err := runner.Wake(ctx, scope, computer.ID)
	if err != nil {
		t.Fatalf("Wake after Sleep: %v", err)
	}
	if woken.ID == awake.ID {
		t.Fatal("wake after sleep returned the settled incarnation")
	}
	if woken.Backend != MockBackendName {
		t.Fatalf("wake after sleep substituted backend %q", woken.Backend)
	}
	if ok, why := quiescent(); ok {
		t.Fatal("a woken Computer reports a quiescent source: the settlement marked the ledger stopped regardless of reality")
	} else if !strings.Contains(why, "quiescent") {
		t.Fatalf("unexpected refusal after wake: %s", why)
	}
	if err := store.StopIncarnation(ctx, scope, computer.ID, woken.ID); err != nil {
		t.Fatal(err)
	}
}
