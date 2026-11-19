package compute

import (
	"context"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// limitScript is the whole of the host sandbox's quota enforcement, and until now
// it had no test: the pids and open-file ceilings were asserted nowhere, so a
// dropped or mis-ordered prefix would have been invisible. The memory ceiling
// added with this file is tested through the same function AND through a real
// Exec, because a script that merely mentions ulimit proves nothing about the
// process that actually ran.

func TestLimitScriptCarriesQuotaLimitsBeforeTheCommand(t *testing.T) {
	script, err := limitScript(Quota{MaxPIDs: 4096, MaxOpenFiles: 256, MaxMemoryMB: 512}, "echo run-me")
	if err != nil {
		t.Fatalf("limitScript: %v", err)
	}
	for _, want := range []string{"ulimit -u 4096 || exit 70;", "ulimit -n 256 || exit 70;", "ulimit -v 524288 || exit 70;"} {
		if !strings.Contains(script, want) {
			t.Fatalf("script must apply %q, got %q", want, script)
		}
	}
	if !strings.HasSuffix(script, "echo run-me") {
		t.Fatalf("the command must be last, ceilings set before it: %q", script)
	}
}

func TestLimitScriptOmitsCeilingsTheQuotaDoesNotDeclare(t *testing.T) {
	script, err := limitScript(Quota{MaxPIDs: 4096}, "echo run-me")
	if err != nil {
		t.Fatalf("limitScript: %v", err)
	}
	if strings.Contains(script, "ulimit -v") || strings.Contains(script, "ulimit -n") {
		t.Fatalf("an unset quota field must not invent a ceiling: %q", script)
	}
}

// TestHostExecEnforcesMemoryQuota runs the real code path: Acquire a jail, Exec a
// command that allocates past the class quota, and require the allocation to
// fail. The control subtest runs the SAME command under a ceiling that fits, so a
// red here cannot be explained by a missing interpreter or a broken jail.
func TestHostExecEnforcesMemoryQuota(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("the RLIMIT_AS ceiling is applied on Linux only")
	}
	if _, err := os.Stat("/usr/bin/python3"); err != nil {
		t.Skip("no /usr/bin/python3 to allocate with")
	}
	const alloc = `python3 -c "b = bytearray(400 * 1024 * 1024); print('allocated', len(b))"`

	base := t.TempDir()
	b := NewHostBackend(base)
	if err := b.Probe(context.Background()); err != nil {
		t.Fatalf("host backend probe: %v", err)
	}
	ctx := context.Background()

	sb, err := b.Acquire(ctx, AcquireSpec{ComputerID: "cmp-quota", Incarnation: 1, Class: ClassC, Quota: Quota{MaxMemoryMB: 256}})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer func() { _ = b.Release(ctx, sb.ID()) }()

	quota := Quota{MaxPIDs: 4096, MaxOpenFiles: 512, MaxMemoryMB: 256}
	res, err := b.Exec(ctx, sb, ExecSpec{Command: alloc, Quota: quota, Timeout: 30 * time.Second})
	if err != nil {
		var ce *contracts.Error
		if !isUnavailable(err) {
			t.Fatalf("over-quota exec must report the confined failure, got %T: %v", err, err)
		}
		_ = ce
	}
	if res.ExitCode == 0 {
		t.Fatalf("a 400 MiB allocation under a 256 MiB ceiling must not succeed: exit=%d stdout=%q", res.ExitCode, res.Stdout)
	}
	if !strings.Contains(res.Stderr, "MemoryError") {
		t.Fatalf("the command must die on the memory ceiling (MemoryError), got exit=%d stderr=%q", res.ExitCode, res.Stderr)
	}

	t.Run("control: same command under a ceiling that fits", func(t *testing.T) {
		sb2, err := b.Acquire(ctx, AcquireSpec{ComputerID: "cmp-quota-ctl", Incarnation: 1, Class: ClassC, Quota: Quota{MaxMemoryMB: 4096}})
		if err != nil {
			t.Fatalf("acquire control: %v", err)
		}
		defer func() { _ = b.Release(ctx, sb2.ID()) }()
		res, err := b.Exec(ctx, sb2, ExecSpec{Command: alloc, Quota: Quota{MaxPIDs: 4096, MaxOpenFiles: 512, MaxMemoryMB: 4096}, Timeout: 30 * time.Second})
		if err != nil || res.ExitCode != 0 {
			t.Fatalf("the same command must pass with room: exit=%d err=%v stderr=%q", res.ExitCode, err, res.Stderr)
		}
		if !strings.Contains(res.Stdout, "allocated 419430400") {
			t.Fatalf("control must report the allocation, got %q", res.Stdout)
		}
	})
}

func isUnavailable(err error) bool { return err != nil && strings.Contains(err.Error(), "unavailable") }
