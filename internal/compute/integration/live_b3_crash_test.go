//go:build compute_live3

// CONTRACT item 4 with a real crash: the EXECUTOR PROCESS is SIGKILLed in the
// middle of a command, and what happens afterwards is measured, not argued.
//
// The executor is a real second process (this same test binary, started with an
// env marker) running the real compute.Runner against the same live PostgreSQL,
// Redis and jail base. The parent waits until the ledger row is 'dispatched'
// and the sandboxed command process is actually running, then kills ONLY the
// executor. That leaves the exact hazard a crash leaves: a command that
// outlived the process which dispatched it, still able to land its external
// effect, and a ledger row that will never be answered by anyone.
//
// What this proves on a green run:
//   - the command really outlived its executor (orphaned, reparented) - so the
//     hazard is real and not simulated away;
//   - the restarted executor's startup reaper kills that orphan and proves it
//     gone (ESRCH), so the pending effect can never land;
//   - the crashed key is never re-dispatched (no repeated external effect), and
//     replaying it through RunCommand does not execute it either;
//   - the work is not lost: a fresh key after the restart runs exactly once.
//
// Gated behind //go:build compute_live3 like the rest of the live compute suite.
package integration_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

const (
	crashChildEnv  = "AHP_B3_CRASH_CHILD"
	crashJailEnv   = "AHP_B3_JAIL"
	crashCompEnv   = "AHP_B3_COMPUTER"
	crashRunEnv    = "AHP_B3_RUN"
	crashStepEnv   = "AHP_B3_STEP"
	crashCmdEnv    = "AHP_B3_COMMAND"
	crashTenanEnv  = "AHP_B3_TENANT"
	crashDeploEnv  = "AHP_B3_DEPLOYMENT"
	crashSubjEnv   = "AHP_B3_SUBJECT"
	crashHelperRun = "^TestExecutorCrashHelper$"
)

// TestExecutorCrashHelper is not a test of its own: it is the executor process
// that TestB3ExecutorCrashMidCommand... starts and kills. It runs one real
// command through its own real compute.Runner and would print the outcome -
// which it never gets to do, because the parent kills it mid-command.
func TestExecutorCrashHelper(t *testing.T) {
	if os.Getenv(crashChildEnv) != "1" {
		t.Skip("helper process for the executor-crash test; it only runs when the parent starts it")
	}
	jail := childEnvValue(t, crashJailEnv)
	compID := childEnvValue(t, crashCompEnv)
	command := childEnvValue(t, crashCmdEnv)
	// The key is DERIVED here, in the process that is about to be killed, from the
	// position of the step - nobody hands it over. That is the whole recovery rule:
	// an effect is keyed by where in the run it belongs.
	step, err := strconv.Atoi(childEnvValue(t, crashStepEnv))
	if err != nil {
		t.Fatalf("%s: %v", crashStepEnv, err)
	}
	key := compute.CommandKeyForStep(childEnvValue(t, crashRunEnv), step)
	sc := contracts.Scope{
		Tenant: childEnvValue(t, crashTenanEnv), Deployment: childEnvValue(t, crashDeploEnv),
		Subject: childEnvValue(t, crashSubjEnv),
	}

	pool, perr := pgxpool.New(context.Background(), liveDSN(t))
	if perr != nil {
		t.Fatalf("child pgxpool: %v", perr)
	}
	defer pool.Close()
	store := compute.New(pool)
	leases, err := compute.NewLeaseService(store, compute.NewRedis(liveRedisAddr(t)))
	if err != nil {
		t.Fatalf("child lease service: %v", err)
	}
	runner := compute.NewRunner(store, compute.NewRegistry(compute.NewHostBackend(jail)),
		jail, "test-node-crashed", nil).WithLeaseService(leases)

	fmt.Printf("EXECUTOR_PID %d\n", os.Getpid())
	fmt.Println("DISPATCH_BEGIN")
	rec, err := runner.RunCommand(context.Background(), sc, compID, key, command, "")
	// Only reached if nobody killed this process: the test would then be proving
	// nothing, and the parent's earlier kill assertion fails first.
	fmt.Printf("RUN_RETURNED status=%s err=%v\n", rec.Status, err)
}

func childEnvValue(t *testing.T, name string) string {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		t.Fatalf("%s must be set for the executor-crash child process", name)
	}
	return v
}

// TestB3ExecutorCrashMidCommandReapsAndNeverRepeats is the crash-recovery
// evidence for a whole executor process, not for a command process.
func TestB3ExecutorCrashMidCommandReapsAndNeverRepeats(t *testing.T) {
	e := newLiveEnv(t)
	sc := scope("b3-executor-crash")
	comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
		Quota: compute.Quota{MaxSessionTTL: 60 * time.Second},
	}, "b3-crash-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateComputer: %v", err)
	}
	home := e.home(comp.ID)
	marker := filepath.Join(home, "effect.marker")
	counter := filepath.Join(home, "effect.count")
	// The parent derives the SAME key from the same logical position, without ever
	// telling the child what it is: equality of the two derivations is what makes a
	// restarted executor recognise the crashed command instead of repeating it.
	runID := "run-" + contracts.NewID()
	step := 0
	key := compute.CommandKeyForStep(runID, step)
	// The effect lands 6s in: the parent kills the executor long before that.
	command := "sleep 6; printf EFFECT > effect.marker; printf x >> effect.count"

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("wd: %v", err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("executable: %v", err)
	}
	childTmp := filepath.Join(e.jailBase, "child-tmp")
	if err := os.MkdirAll(childTmp, 0o700); err != nil {
		t.Fatal(err)
	}
	child := exec.Command(exe, "-test.run", crashHelperRun, "-test.v", "-test.timeout", "240s")
	child.Dir = wd
	child.Env = append(os.Environ(),
		crashChildEnv+"=1", crashJailEnv+"="+e.jailBase, crashCompEnv+"="+comp.ID,
		crashRunEnv+"="+runID, crashStepEnv+"="+strconv.Itoa(step), crashCmdEnv+"="+command,
		crashTenanEnv+"="+sc.Tenant, crashDeploEnv+"="+sc.Deployment, crashSubjEnv+"="+sc.Subject,
		// The child must not queue on the package flock its parent holds.
		"TMPDIR="+childTmp,
	)
	stdout, err := child.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	lines := make(chan string, 64)
	go func() {
		scan := bufio.NewScanner(stdout)
		for scan.Scan() {
			lines <- scan.Text()
		}
		close(lines)
	}()
	if err := child.Start(); err != nil {
		t.Fatalf("start the executor process: %v", err)
	}
	executorPID := child.Process.Pid
	t.Cleanup(func() { _ = syscall.Kill(executorPID, syscall.SIGKILL) })

	waitChildLine(t, lines, "DISPATCH_BEGIN", 30*time.Second)
	t.Logf("executor process %d dispatched key %s", executorPID, key[:12])

	// The crash must happen AFTER the ledger recorded the dispatch: that is the
	// window where a naive restart would repeat a side effect.
	storedKey, status := waitForLedgerRow(t, e.pool, comp.ID, key, string(compute.CommandDispatched), 20*time.Second)
	if storedKey != key {
		t.Fatalf("the killed executor recorded its command under %q, not under the position-derived key %q", storedKey, key)
	}
	if status != string(compute.CommandDispatched) {
		t.Fatalf("ledger status before the crash = %q, want %q", status, compute.CommandDispatched)
	}
	t.Logf("both processes derived key %s independently (run %s, step %d)", key[:16], runID[:12], step)
	sandboxPID := waitForRegisteredProcess(t, e.jailBase, comp.ID, 20*time.Second)
	if ppid := procPPid(sandboxPID); ppid != executorPID {
		t.Fatalf("the sandboxed command %d is not a child of the executor %d (ppid=%d): this is not the crash being tested",
			sandboxPID, executorPID, ppid)
	}
	t.Logf("crash point: ledger=dispatched, sandboxed command pid %d running under executor %d", sandboxPID, executorPID)

	// KILL THE EXECUTOR. Not the command, not its process group: the whole
	// point is that the command survives the process that dispatched it.
	if err := child.Process.Kill(); err != nil {
		t.Fatalf("SIGKILL the executor process: %v", err)
	}
	_ = child.Wait()
	if err := syscall.Kill(executorPID, 0); err != syscall.ESRCH {
		t.Fatalf("the executor process %d is somehow still alive: %v", executorPID, err)
	}
	if !procAlive(sandboxPID) {
		t.Fatalf("the sandboxed command died together with the executor: this host gives no orphans to reap, so the run would prove nothing")
	}
	// Reparenting is what makes it an orphan rather than a child that is merely
	// not waited for yet.
	if ppid := procPPid(sandboxPID); ppid == executorPID {
		t.Fatalf("the sandboxed command still reports the dead executor as its parent")
	} else {
		t.Logf("the command outlived its executor: pid %d reparented to %d", sandboxPID, ppid)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the effect landed before the crash (stat: %v): the crash point is not mid-command", err)
	}

	// RESTART: the same thing compute-host does before it serves a request.
	reaped, err := compute.ReapProcessesAtStartup(e.jailBase)
	if err != nil {
		t.Fatalf("startup reaper of the restarted executor: %v", err)
	}
	if reaped.Survivors() != nil {
		t.Fatalf("the reaper left processes alive: %+v", reaped.Survivors())
	}
	if reaped.ReapedProcs() == 0 {
		t.Fatal("the restarted executor found nothing to reap: the crashed command was never in the process registry")
	}
	if gone, zombie := procNotRunning(sandboxPID); !gone {
		t.Fatalf("the orphaned command %d is still executing after the reaper", sandboxPID)
	} else if zombie {
		t.Logf("the orphaned command %d is a zombie: killed and no longer executing, only waiting to be reaped", sandboxPID)
	}
	if left := registeredProcesses(e.jailBase, comp.ID); len(left) != 0 {
		t.Fatalf("the process registry still lists %d entr(ies) for the computer after reaping: %v", len(left), left)
	}
	t.Logf("reaped at restart: %d process(es) proven dead for computer %s", reaped.ReapedProcs(), comp.ID[:12])

	// The pending effect can no longer land: wait past the moment the orphan
	// would have written it and look again.
	time.Sleep(9 * time.Second)
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the orphaned command landed its external effect after being reaped (stat: %v)", err)
	}
	if _, err := os.Stat(counter); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the orphaned command wrote the counter after being reaped (stat: %v)", err)
	}

	// A new executor over the same database and jail base.
	restarted, rstore := restartedExecutor(t, e.jailBase, "test-node-restarted")
	rec, mayDispatch, err := rstore.Exec(ctx(t), sc, comp.ID, key, command, "")
	if err != nil {
		t.Fatalf("recovery Exec: %v", err)
	}
	if mayDispatch {
		t.Fatal("the crashed key was re-dispatchable: a restart would repeat the external effect")
	}
	if rec.Status != compute.CommandDispatched {
		t.Fatalf("the crashed row settled to %q on its own; the recovery path must leave the outcome unknown, not invent one", rec.Status)
	}
	t.Logf("recovery refuses key %s (row %s stays %s: outcome never blindly recomputed)", key[:12], rec.ID[:12], rec.Status)

	// Replaying the key through the runner must not execute it either.
	if _, err := restarted.RunCommand(ctx(t), sc, comp.ID, key, command, ""); err != nil {
		t.Fatalf("replaying the crashed key: %v", err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replaying the crashed key executed the command after the crash: %v", err)
	}

	// And the work itself is not lost: a fresh key runs it exactly once.
	rec2, err := restarted.RunCommand(ctx(t), sc, comp.ID, "b3-after-"+contracts.NewID(), command, "")
	if err != nil {
		t.Fatalf("post-recovery run: %v", err)
	}
	if rec2.Status != compute.CommandSucceeded {
		t.Fatalf("post-recovery run status=%s stderr=%q", rec2.Status, rec2.Stderr)
	}
	body, err := os.ReadFile(counter)
	if err != nil {
		t.Fatalf("the work was lost as well as not repeated: %v", err)
	}
	if len(body) != 1 {
		t.Fatalf("the command ran %d times after the crash, want exactly 1", len(body))
	}
	t.Logf("after the crash the command ran exactly once (effect file %d write(s), crashed key never re-executed)", len(body))
}

// TestB3ReapOrphanProcessesIsScopedAndVerified covers the per-Computer reaper
// on its own: it kills a background job the Computer left behind and proves it
// dead, it reports nothing when there is nothing to reap, a foreign scope
// cannot use it to kill another tenant's processes, and a runner without a jail
// base refuses instead of pretending.
func TestB3ReapOrphanProcessesIsScopedAndVerified(t *testing.T) {
	e := newLiveEnv(t)
	sc := scope("b3-reap-owner")
	foreign := scope("b3-reap-mallory")
	comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
		Quota: compute.Quota{MaxSessionTTL: 60 * time.Second},
	}, "b3-reap-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateComputer: %v", err)
	}

	// Nothing running yet: reaping must be a clean no-op, not a phantom report.
	empty, err := e.runner.ReapOrphanProcesses(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("reaping an idle computer: %v", err)
	}
	if len(empty.Processes) != 0 {
		t.Fatalf("an idle computer reported %d killed process(es): %+v", len(empty.Processes), empty.Processes)
	}

	// A command that finishes but leaves a background job in its process group:
	// exactly the leftover a crashed or detached session can hand to a restart.
	rec, err := e.runner.RunCommand(ctx(t), sc, comp.ID, "b3-reap-"+contracts.NewID(),
		"sleep 60 </dev/null >/dev/null 2>&1 & printf bg > bg.marker; printf done", "")
	if err != nil {
		t.Fatalf("run the leaking command: %v", err)
	}
	if rec.Status != compute.CommandSucceeded {
		t.Fatalf("the leaking command status=%s stderr=%q", rec.Status, rec.Stderr)
	}
	left := registeredProcesses(e.jailBase, comp.ID)
	if len(left) == 0 {
		t.Fatal("the finished command left no process-group entry: the substrate does not track leftovers, so reaping is unverifiable here")
	}
	// The registered leader has already exited; the leaked job is still in the
	// group it started, so the groups are enumerated by the recorded pgid.
	members := liveGroupMembers(left)
	if len(members) == 0 {
		t.Fatal("the leaked process group has no live members: nothing was left behind to reap")
	}
	t.Logf("the finished command left %d live process(es) in %d registered group(s)", len(members), len(left))

	// A foreign scope must not be able to kill this Computer's processes.
	if _, err := e.runner.ReapOrphanProcesses(ctx(t), foreign, comp.ID); contracts.Code(err) != string(contracts.ErrNotFound) {
		t.Fatalf("foreign scope reaping: code=%v err=%v, want NOT_FOUND", contracts.Code(err), err)
	}
	for _, m := range members {
		if !procAlive(m) {
			t.Fatalf("the reap that was refused for a foreign scope still killed process %d", m)
		}
	}

	reaped, err := e.runner.ReapOrphanProcesses(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("reap the leftovers: %v", err)
	}
	if len(reaped.Processes) == 0 {
		t.Fatal("the reaper reported no process for a computer that had live leftovers")
	}
	for _, p := range reaped.Processes {
		if !p.Dead {
			t.Fatalf("pid %d (pgid %d) was signalled but never verified dead", p.PID, p.PGID)
		}
	}
	for _, m := range members {
		if gone, _ := procNotRunning(m); !gone {
			t.Fatalf("pid %d is still executing after a reap that claimed otherwise", m)
		}
	}
	if left := registeredProcesses(e.jailBase, comp.ID); len(left) != 0 {
		t.Fatalf("the registry still lists %d entr(ies) after reaping: %v", len(left), left)
	}
	t.Logf("reaper proved %d process(es) dead (ESRCH) and cleared the registry", len(reaped.Processes))

	// A runner with no jail base cannot read a registry: it must refuse.
	noBase := compute.NewRunner(e.store, compute.NewRegistry(compute.NewHostBackend(e.jailBase)),
		"", "test-node-no-base", nil)
	if _, err := noBase.ReapOrphanProcesses(ctx(t), sc, comp.ID); err == nil {
		t.Fatal("a runner without a jail base claimed to reap processes")
	}
}

// ---------------------------------------------------------------------------
// /proc and registry probes (read-only; the same substrate the reaper reads)
// ---------------------------------------------------------------------------

func procAlive(pid int) bool { return syscall.Kill(pid, 0) == nil }

func procPPid(pid int) int {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(b), "\n") {
		if rest, ok := strings.CutPrefix(line, "PPid:"); ok {
			v, _ := strconv.Atoi(strings.TrimSpace(rest))
			return v
		}
	}
	return 0
}

// procNotRunning reports whether pid can no longer execute anything: the kernel
// has forgotten the pid, or the process is a zombie waiting to be reaped. Only
// the second proof is available for a process this test is the parent of.
func procNotRunning(pid int) (gone bool, zombie bool) {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return true, false
	}
	s := string(b)
	i := strings.LastIndex(s, ")")
	if i < 0 {
		return true, false
	}
	fields := strings.Fields(s[i+2:])
	if len(fields) == 0 {
		return true, false
	}
	if fields[0] != "Z" {
		return false, false
	}
	return true, true
}

// liveGroupMembers lists the currently executing processes in the process
// groups recorded in the registry (spawned pid -> pgid). The leader of a group
// is usually already gone by the time a leftover is reaped, so the group is
// enumerated by its recorded id and not through its leader's /proc entry.
func liveGroupMembers(registry map[int]int) []int {
	pgids := map[int]bool{}
	for _, pgid := range registry {
		pgids[pgid] = true
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	var out []int
	for _, ent := range entries {
		candidate, cerr := strconv.Atoi(ent.Name())
		if cerr != nil {
			continue
		}
		if pgids[procPgrp(candidate)] {
			out = append(out, candidate)
		}
	}
	sort.Ints(out)
	return out
}

func procPgrp(pid int) int {
	b, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0
	}
	s := string(b)
	i := strings.LastIndex(s, ")")
	if i < 0 {
		return 0
	}
	fields := strings.Fields(s[i+2:])
	if len(fields) < 3 {
		return 0
	}
	v, err := strconv.Atoi(fields[2])
	if err != nil {
		return 0
	}
	return v
}

// registeredProcesses reads the node's process registry for one Computer:
// spawned pid -> process-group id. Empty jail base / computer id yields nothing.
func registeredProcesses(jailBase, computerID string) map[int]int {
	out := map[int]int{}
	if jailBase == "" || computerID == "" {
		return out
	}
	entries, err := os.ReadDir(filepath.Join(jailBase, "procs", computerID))
	if err != nil {
		return out
	}
	for _, ent := range entries {
		pid, cerr := strconv.Atoi(ent.Name())
		if cerr != nil {
			continue
		}
		pgid := pid
		if b, err := os.ReadFile(filepath.Join(jailBase, "procs", computerID, ent.Name())); err == nil {
			if v, verr := strconv.Atoi(strings.TrimSpace(string(b))); verr == nil {
				pgid = v
			}
		}
		out[pid] = pgid
	}
	return out
}

func waitForRegisteredProcess(t *testing.T, jailBase, computerID string, d time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		for pid := range registeredProcesses(jailBase, computerID) {
			if procAlive(pid) {
				return pid
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("the sandboxed command process never appeared in the node's process registry")
	return 0
}

// waitForLedgerRow polls the command ledger until the crashed command reaches the
// wanted status, and returns the key the row was actually stored under.
func waitForLedgerRow(t *testing.T, pool *pgxpool.Pool, computerID, key, want string, d time.Duration) (string, string) {
	t.Helper()
	deadline := time.Now().Add(d)
	last := "<none>"
	for time.Now().Before(deadline) {
		var stored, status string
		err := pool.QueryRow(context.Background(),
			`SELECT key, status FROM ahp_compute.commands WHERE computer_id = $1 AND key = $2`,
			computerID, key).Scan(&stored, &status)
		if err == nil {
			last = status
			if status == want {
				return stored, status
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("the ledger never reached status %q for the dispatched command (last status %q)", want, last)
	return "", last
}

func restartedExecutor(t *testing.T, jailBase, node string) (*compute.Runner, *compute.Store) {
	t.Helper()
	pool, err := pgxpool.New(ctx(t), liveDSN(t))
	if err != nil {
		t.Fatalf("restarted executor pool: %v", err)
	}
	t.Cleanup(pool.Close)
	store := compute.New(pool)
	leases, err := compute.NewLeaseService(store, compute.NewRedis(liveRedisAddr(t)))
	if err != nil {
		t.Fatalf("restarted executor leases: %v", err)
	}
	return compute.NewRunner(store, compute.NewRegistry(compute.NewHostBackend(jailBase)),
		jailBase, node, nil).WithLeaseService(leases), store
}

func waitChildLine(t *testing.T, lines <-chan string, want string, d time.Duration) {
	t.Helper()
	deadline := time.After(d)
	for {
		select {
		case line, ok := <-lines:
			if !ok {
				t.Fatalf("the executor process exited before printing %q", want)
			}
			if strings.Contains(line, want) {
				return
			}
			t.Logf("executor process: %s", line)
		case <-deadline:
			t.Fatalf("the executor process never printed %q", want)
		}
	}
}
