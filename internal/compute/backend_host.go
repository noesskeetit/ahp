package compute

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// HostBackend runs commands as ordinary processes confined by resource limits
// (RLIMIT_NPROC / RLIMIT_NOFILE) and a per-Computer jail directory. It is a
// REAL substrate for the trusted class S and per-deployment class C, and it is
// explicitly labelled "host" — it is NOT a microVM and never presents itself
// as one. Class X is never routed here (see Registry.Require).
type HostBackend struct {
	base string
}

// NewHostBackend builds a host backend rooted at base. base holds one jail
// directory per live sandbox and must be a scratch location the supervisor
// owns.
func NewHostBackend(base string) *HostBackend {
	if base == "" {
		base = filepath.Join(os.TempDir(), "ahp-compute-host")
	}
	return &HostBackend{base: base}
}

// Name is "host" — the truthful label, never "microvm".
func (*HostBackend) Name() string { return "host" }

// Probe verifies the jail base is usable. The host backend is available on any
// POSIX node; it never reports itself as a microVM.
func (b *HostBackend) Probe(ctx context.Context) error {
	if err := os.MkdirAll(b.base, 0o700); err != nil {
		return Unavailable("host", "jail base is not writable: "+b.base)
	}
	return nil
}

type hostSandbox struct {
	id         string
	node       string
	dir        string
	home       string
	computerID string
}

func (s *hostSandbox) ID() string      { return s.id }
func (*hostSandbox) Backend() string   { return "host" }
func (s *hostSandbox) Node() string    { return s.node }
func (s *hostSandbox) dirPath() string { return s.dir }

// Acquire creates the scratch jail directory for one incarnation. No VM is
// started; the confinement is the directory plus per-exec resource limits.
func (b *HostBackend) Acquire(ctx context.Context, spec AcquireSpec) (Sandbox, error) {
	if err := os.MkdirAll(b.base, 0o700); err != nil {
		return nil, Unavailable("host", "jail base is not writable: "+b.base)
	}
	id := "host-" + contracts.NewID()
	dir := filepath.Join(b.base, id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "compute: cannot create host jail: "+dir)
	}
	host, _ := os.Hostname()
	sb := &hostSandbox{id: id, node: "host:" + host, dir: dir, computerID: spec.ComputerID}
	// A personal (or project) Computer's working tree outlives incarnations:
	// its home is a durable per-Computer directory under the jail base, while
	// the incarnation scratch stays disposable.
	if spec.Profile == ProfilePersonal || spec.Profile == ProfileProject {
		sb.home = filepath.Join(b.base, "computers", spec.ComputerID, "home")
		if err := os.MkdirAll(sb.home, 0o700); err != nil {
			return nil, contracts.NewError(contracts.ErrInvalid, "compute: cannot create home: "+sb.home)
		}
	}
	return sb, nil
}

// workDir is the default working directory: the durable home when one exists,
// otherwise the disposable scratch jail.
func (s *hostSandbox) workDir() string {
	if s.home != "" {
		return s.home
	}
	return s.dir
}

// Release removes the incarnation's scratch jail. Durable per-Computer state
// (homes, environment prefixes, the process registry) lives under separate
// directories and is untouched: it is the Computer's state, not the
// incarnation's.
func (b *HostBackend) Release(_ context.Context, sandboxID string) error {
	dir := filepath.Join(b.base, filepath.Base(sandboxID))
	if strings.HasPrefix(dir, filepath.Join(b.base, "computers")+string(os.PathSeparator)) {
		return nil
	}
	return os.RemoveAll(dir)
}

// Exec runs one command inside the sandbox's jail as a confined process. The
// class quota is applied as resource limits (pids -> RLIMIT_NPROC, open files ->
// RLIMIT_NOFILE, memory -> RLIMIT_AS) before the command starts, capped at what the
// host may grant, and a limit that cannot be applied stops the command instead of
// being dropped. RLIMIT_AS is a ceiling on address space, not cgroup RSS accounting:
// it refuses an allocation past the quota and does not claim measured residency.
// CPU rate, disk quota and egress policy are NOT enforced by this backend -- those
// are class X (microVM) enforcement, and this backend never claims them; the registry
// also refuses to route class X here at all, so no quota in this path is ever called
// isolation.
// Output is captured and bounded.
func (b *HostBackend) Exec(ctx context.Context, sb Sandbox, spec ExecSpec) (ExecResult, error) {
	hs, ok := sb.(*hostSandbox)
	if !ok {
		return ExecResult{}, contracts.NewError(contracts.ErrInvalid, "compute: sandbox not from host backend")
	}
	if err := CheckProcessBudget(spec.Quota); err != nil {
		return ExecResult{}, err
	}
	timeout := spec.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	shell, err := limitShell(spec.Quota)
	if err != nil {
		return ExecResult{}, err
	}
	script, err := limitScript(spec.Quota, spec.Command)
	if err != nil {
		return ExecResult{}, err
	}
	cmd := exec.CommandContext(runCtx, shell, "-c", script)
	wd := hs.workDir()
	if spec.Cwd != "" {
		// Confined to the jail base (e.g. an environment install prefix);
		// anything outside is silently ignored, never trusted.
		if abs, err := filepath.Abs(spec.Cwd); err == nil &&
			strings.HasPrefix(abs, filepath.Clean(b.base)+string(os.PathSeparator)) {
			wd = abs
		}
	}
	cmd.Dir = wd
	if len(spec.Env) > 0 {
		cmd.Env = spec.Env
	} else {
		cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + wd}
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{}
	setProcessGroup(cmd.SysProcAttr)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return ExecResult{}, contracts.NewError(contracts.ErrInvalid, "compute: host exec failed to start")
	}
	pid := cmd.Process.Pid
	// The process-group entry stays registered until Sleep: background jobs
	// orphaned by the command keep the group alive and must be killed (and
	// proven dead) at sleep time, not when the command itself exits.
	registerProcess(b.base, hs.computerID, pid, pid, spec.CommandID)
	err = cmd.Wait()

	res := ExecResult{Stdout: boundBytes(&stdout), Stderr: boundBytes(&stderr)}
	res.Truncated = stdout.Len() > maxOutputBytes || stderr.Len() > maxOutputBytes
	switch {
	case err == nil:
		res.ExitCode = 0
	case runCtx.Err() == context.DeadlineExceeded:
		res.ExitCode = -1
		res.Stderr += "\ncompute: command exceeded timeout"
	case errorsAsExit(err):
		res.ExitCode = exitCodeOf(err)
	default:
		// Failed to start the process at all (not a command exit): surface it.
		return ExecResult{}, contracts.NewError(contracts.ErrInvalid, "compute: host exec failed to start")
	}
	return res, nil
}

// OpenPTY opens a real interactive pseudoterminal inside the jail. On the
// Linux target runtime this uses /dev/ptmx; on other platforms it fails closed
// with Unavailable rather than pretending to provide a terminal.
func (b *HostBackend) OpenPTY(_ context.Context, sb Sandbox, spec PTYSpec) (PTYSession, error) {
	hs, ok := sb.(*hostSandbox)
	if !ok {
		return nil, contracts.NewError(contracts.ErrInvalid, "compute: sandbox not from host backend")
	}
	master, slavePath, err := openHostPTY(spec)
	if err != nil {
		return nil, err
	}
	shell := spec.Shell
	if shell == "" {
		shell = "/bin/sh"
	}
	cmd := exec.Command(shell)
	cmd.Dir = hs.dir
	if len(spec.Env) > 0 {
		cmd.Env = spec.Env
	} else {
		cmd.Env = []string{"PATH=/usr/bin:/bin", "TERM=xterm-256color", "HOME=" + hs.dir}
	}
	slave, err := os.OpenFile(slavePath, os.O_RDWR, 0)
	if err != nil {
		_ = master.Close()
		return nil, contracts.NewError(contracts.ErrInvalid, "compute: cannot open pty slave")
	}
	cmd.Stdin, cmd.Stdout, cmd.Stderr = slave, slave, slave
	cmd.SysProcAttr = &syscall.SysProcAttr{}
	setProcessGroup(cmd.SysProcAttr)
	if err := cmd.Start(); err != nil {
		_ = slave.Close()
		_ = master.Close()
		return nil, contracts.NewError(contracts.ErrInvalid, "compute: cannot start pty shell")
	}
	// An interactive shell is not a ledger command, so it registers with no command row.
	registerProcess(b.base, hs.computerID, cmd.Process.Pid, cmd.Process.Pid, "")
	// The parent no longer needs the slave; the child holds it.
	_ = slave.Close()
	return &hostPTY{master: master, cmd: cmd}, nil
}

// hostPTY is a live pseudoterminal backed by a child process.
type hostPTY struct {
	master   *os.File
	cmd      *exec.Cmd
	waitOnce sync.Once
	waitErr  error
	code     int
	closed   bool
	mu       sync.Mutex
}

func (p *hostPTY) Read(b []byte) (int, error)  { return p.master.Read(b) }
func (p *hostPTY) Write(b []byte) (int, error) { return p.master.Write(b) }

func (p *hostPTY) Resize(cols, rows uint16) error { return ptyResize(p.master.Fd(), cols, rows) }

func (p *hostPTY) Wait() (int, error) {
	p.waitOnce.Do(func() { p.waitErr = p.cmd.Wait(); p.code = exitCodeOfWait(p.waitErr, p.cmd) })
	return p.code, p.waitErr
}

func (p *hostPTY) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	if p.cmd.Process != nil {
		_ = p.cmd.Process.Kill()
	}
	return p.master.Close()
}

// limitShell picks the shell that runs a confined command. RLIMIT_NPROC is set with
// "ulimit -u", which dash does not implement -- and dash is what /bin/sh is on the
// nodes this lane runs on. The previous wrapper hid that behind 2>/dev/null, so every
// command ran with NO process ceiling while the code, the quota and the reports said
// it had one. bash implements it. A host with neither bash nor a capable /bin/sh is
// refused here rather than silently downgraded: a limit that cannot be applied is a
// fact about the host, and the caller must hear it before the command runs.
func limitShell(q Quota) (string, error) {
	if q.MaxPIDs <= 0 {
		return "/bin/sh", nil
	}
	if p, err := exec.LookPath("bash"); err == nil {
		return p, nil
	}
	return "", Unavailable("host", "the sandbox cannot apply the quota's process limit: "+
		"no bash on this host and /bin/sh has no `ulimit -u`; refusing to run unconfined")
}

// limitScript clamps the quota to what this host may grant and prefixes the command
// with the limits. Each assignment is followed by a hard stop (exit 70, EX_SOFTWARE):
// a limit the shell could not set must end the command, because continuing is exactly
// the masking that made the missing ceiling invisible for so long.
func limitScript(q Quota, command string) (string, error) {
	var pre []byte
	if q.MaxPIDs > 0 {
		n, err := clampedRLimit(sandboxNProc, q.MaxPIDs)
		if err != nil {
			return "", err
		}
		pre = append(pre, []byte(fmt.Sprintf("ulimit -u %d || exit 70; ", n))...)
	}
	if q.MaxOpenFiles > 0 {
		n, err := clampedRLimit(sandboxNoFile, q.MaxOpenFiles)
		if err != nil {
			return "", err
		}
		pre = append(pre, []byte(fmt.Sprintf("ulimit -n %d || exit 70; ", n))...)
	}
	if q.MaxMemoryMB > 0 {
		if !supportsMemoryCeiling() {
			return "", Unavailable("host", "this platform cannot bound a confined command's address space; "+
				"refusing to run a command that declares a memory quota this backend would not enforce")
		}
		// The ceiling is in bytes, so it cannot travel through clampedRLimit, whose
		// int32 argument would overflow past 2 GiB. Same clamp, wider arithmetic.
		wantKB := int64(q.MaxMemoryMB) * 1024
		if max, err := hardLimit(sandboxAS); err == nil {
			const infinity = ^uint64(0)
			if asBytes := wantKB * 1024; max != infinity && uint64(asBytes) > max {
				wantKB = int64(max / 1024)
			}
		}
		pre = append(pre, []byte(fmt.Sprintf("ulimit -v %d || exit 70; ", wantKB))...)
	}
	return string(append(pre, []byte(command)...)), nil
}

// clampedRLimit returns the largest value of want that may be set for res: lowering
// is always allowed, raising past the hard limit is refused by the kernel, and a
// quota whose class default already sits above this host's ceiling would otherwise
// stop every command on the exit-70 guard. An infinite ceiling leaves want alone.
func clampedRLimit(res int, want int32) (int64, error) {
	max, err := hardLimit(res)
	if err != nil {
		// No readable ceiling (a build host without the Linux rlimit interface).
		// The requested value still goes to the shell, whose own guard stops the
		// command if the limit cannot be applied -- so this can never silently run
		// unconfined; it only skips the clamp.
		return int64(want), nil
	}
	n := int64(want)
	const infinity = ^uint64(0)
	if max != infinity && uint64(n) > max {
		n = int64(max)
	}
	return n, nil
}

func boundBytes(b *bytes.Buffer) string {
	if b.Len() > maxOutputBytes {
		return b.String()[:maxOutputBytes]
	}
	return b.String()
}

func errorsAsExit(err error) bool {
	_, ok := err.(*exec.ExitError)
	return ok
}

func exitCodeOf(err error) int {
	if ee, ok := err.(*exec.ExitError); ok {
		return ee.ExitCode()
	}
	return 0
}

func exitCodeOfWait(waitErr error, cmd *exec.Cmd) int {
	if cmd.ProcessState != nil {
		return cmd.ProcessState.ExitCode()
	}
	_ = waitErr
	return 0
}
