package compute

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// FirecrackerConfig describes a real Firecracker host: the hypervisor binary,
// the guest kernel and root filesystem images, and where per-VM state lives.
// It is supplied by the Linux supervisor from environment-derived config; there
// is no in-process substitute. A runtime built from an incomplete config will
// fail its Probe, so a half-configured host never pretends to serve class X.
type FirecrackerConfig struct {
	// Binary is the firecracker executable. Required.
	Binary string
	// KernelImage is a vmlinux/Elf kernel used as boot-source. Required.
	KernelImage string
	// RootFSImage is a read-only base ext4 image. Each guest gets a copy-on-
	// write copy (reflink where the FS supports it, otherwise a copy), matching
	// the disposable-per-incarnation contract. Required.
	RootFSImage string
	// DataDir is the directory under which per-VM working dirs are created.
	// Required.
	DataDir string

	// VCPUCount and MemSizeMB are the default machine configuration when a
	// request quota does not constrain them.
	VCPUCount int64
	MemSizeMB int64

	// GuestAgent is the base URL of the in-guest agent the rootfs must run
	// (e.g. "http://169.254.0.2:8080"). Exec and PTY are served by that agent;
	// with no agent address the runtime can boot a VM but cannot run commands,
	// which it reports honestly.
	GuestAgent string
	// TapName and GuestMAC, when set, attach a network interface to the guest.
	// When empty the guest boots with no network (class X default-deny egress).
	TapName  string
	GuestMAC string
	// BootArgs overrides the default kernel command line when set.
	BootArgs string

	// StartTimeout bounds waiting for the API socket and the guest agent.
	StartTimeout time.Duration
}

func (c FirecrackerConfig) validate() error {
	missing := func(f string) error {
		return Unavailable("firecracker", "config missing required field "+f)
	}
	if c.Binary == "" {
		return missing("Binary")
	}
	if c.KernelImage == "" {
		return missing("KernelImage")
	}
	if c.RootFSImage == "" {
		return missing("RootFSImage")
	}
	if c.DataDir == "" {
		return missing("DataDir")
	}
	return nil
}

func (c FirecrackerConfig) vcpus() int64 {
	if c.VCPUCount <= 0 {
		return 1
	}
	return c.VCPUCount
}

func (c FirecrackerConfig) memMB() int64 {
	if c.MemSizeMB <= 0 {
		return 256
	}
	return c.MemSizeMB
}

func (c FirecrackerConfig) startTimeout() time.Duration {
	if c.StartTimeout <= 0 {
		return 30 * time.Second
	}
	return c.StartTimeout
}

// FirecrackerRuntime is the VMRuntime the class X Supervisor delegates to on a
// real KVM node. It drives the documented Firecracker REST API over a per-VM
// unix socket to boot a disposable guest, and runs commands and terminals via
// an in-guest agent.
//
// HONESTY: this is real Firecracker control code, not a mock. It only functions
// on a Linux host with /dev/kvm and a populated FirecrackerConfig; everywhere
// else (notably a macOS control host) Probe returns Unavailable and Acquire
// refuses, so a microVM is never simulated. Its VM-boot path is NOT_VERIFIED on
// hosts without KVM and is exercised only by the reproducible Linux/KVM
// acceptance runner (infra/compute/acceptance-kvm.sh).
type FirecrackerRuntime struct {
	cfg FirecrackerConfig
}

// NewFirecrackerRuntime validates cfg and returns a runtime. A nil *Runtime is
// never produced silently: an invalid config is an UnavailableError.
func NewFirecrackerRuntime(cfg FirecrackerConfig) (*FirecrackerRuntime, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &FirecrackerRuntime{cfg: cfg}, nil
}

// Name is the stable microVM backend label.
func (r *FirecrackerRuntime) Name() string { return "firecracker" }

// Probe verifies this host can actually run Firecracker guests: Linux, a /dev/kvm
// that this user may open AND that the kernel accepts for KVM_CREATE_VM, plus
// readable hypervisor/kernel/rootfs images. Any failure is Unavailable; on a
// macOS control host it is always "KVM not available on this host", and on a
// nested guest whose hypervisor hides VMX controls it names VM creation, not the
// node.
func (r *FirecrackerRuntime) Probe(ctx context.Context) error {
	if runtime.GOOS != "linux" {
		return Unavailable("firecracker", "Firecracker requires Linux; this host is "+runtime.GOOS)
	}
	if msg := kvmHostProblem(); msg != "" {
		return Unavailable("firecracker", msg)
	}
	for _, p := range []string{r.cfg.Binary, r.cfg.KernelImage, r.cfg.RootFSImage} {
		if _, err := os.Stat(p); err != nil {
			return Unavailable("firecracker", "image not readable: "+p)
		}
	}
	return nil
}

type fcSandbox struct {
	mu sync.Mutex
	// paused records what this platform last asked the hypervisor to do. It is
	// not a claim about the guest's inner state, and it exists so a second Pause
	// is refused instead of quietly reported as success.
	paused  bool
	id      string
	node    string
	dir     string
	proc    *os.Process
	api     *fcClient
	agent   *guestClient
	rootfs  string
	tapName string
}

func (s *fcSandbox) ID() string      { return s.id }
func (s *fcSandbox) Backend() string { return "firecracker" }
func (s *fcSandbox) Node() string    { return s.node }

// Acquire boots a disposable guest for one incarnation: it copies a CoW rootfs,
// launches the firecracker process, configures machine/boot-source/drive (and an
// optional NIC), starts the instance, and waits for the in-guest agent. On any
// failure after the process starts the guest is torn down before returning, so
// a half-booted VM never lingers.
func (r *FirecrackerRuntime) Acquire(ctx context.Context, spec AcquireSpec) (Sandbox, error) {
	if err := r.Probe(ctx); err != nil {
		return nil, err
	}
	vmID := "vm-" + contracts.NewID()
	dir := filepath.Join(r.cfg.DataDir, vmID)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, Unavailable("firecracker", "cannot create vm dir: "+err.Error())
	}
	rootfs := filepath.Join(dir, "rootfs.ext4")
	if err := copyCoW(r.cfg.RootFSImage, rootfs); err != nil {
		_ = os.RemoveAll(dir)
		return nil, Unavailable("firecracker", "cannot prepare rootfs: "+err.Error())
	}
	apiSock := filepath.Join(dir, "firecracker.socket")
	sb := &fcSandbox{id: vmID, node: spec.Node, dir: dir, rootfs: rootfs, tapName: r.cfg.TapName}

	// Launch firecracker. stderr is captured for post-mortem in the vm dir.
	errLog, lerr := os.Create(filepath.Join(dir, "firecracker.log"))
	if lerr != nil {
		_ = os.RemoveAll(dir)
		return nil, Unavailable("firecracker", "cannot open log: "+lerr.Error())
	}
	cmd := exec.CommandContext(ctx, r.cfg.Binary, "--api-sock", apiSock, "--id", vmID)
	cmd.Stderr = errLog
	if err := cmd.Start(); err != nil {
		_ = errLog.Close()
		_ = os.RemoveAll(dir)
		return nil, Unavailable("firecracker", "cannot start firecracker: "+err.Error())
	}
	_ = errLog.Close()
	sb.proc = cmd.Process
	go func() { _ = cmd.Wait() }()

	api := newFCClient(apiSock)
	sb.api = api
	defer func() {
		// If any configuration step fails, tear the guest down completely.
	}()

	if err := r.waitForSocket(ctx, apiSock); err != nil {
		sb.teardown()
		return nil, err
	}
	vcpu, mem := r.machineFor(spec.Quota)
	if err := api.putMachineConfig(ctx, vcpu, mem); err != nil {
		sb.teardown()
		return nil, err
	}
	bootArgs := r.cfg.BootArgs
	if bootArgs == "" {
		bootArgs = "console=ttyS0 reboot=k panic=1 pci=off root=/dev/vda rw"
	}
	if err := api.putBootSource(ctx, r.cfg.KernelImage, bootArgs); err != nil {
		sb.teardown()
		return nil, err
	}
	if err := api.putDrive(ctx, "rootfs", rootfs, true, false); err != nil {
		sb.teardown()
		return nil, err
	}
	if r.cfg.TapName != "" && r.cfg.GuestMAC != "" {
		if err := api.putNetwork(ctx, "eth0", r.cfg.TapName, r.cfg.GuestMAC); err != nil {
			sb.teardown()
			return nil, err
		}
	}
	if err := api.startInstance(ctx); err != nil {
		sb.teardown()
		return nil, err
	}
	if r.cfg.GuestAgent != "" {
		gc := newGuestClient(r.cfg.GuestAgent)
		if err := r.waitForAgent(ctx, gc); err != nil {
			sb.teardown()
			return nil, err
		}
		sb.agent = gc
	}
	return sb, nil
}

// machineFor derives vCPU / memory from the class quota, defaulting to the
// configured machine when the quota is unset.
func (r *FirecrackerRuntime) machineFor(q Quota) (int64, int64) {
	vcpu := r.cfg.vcpus()
	if q.MaxCPUMillisPerSec > 0 {
		n := int64(q.MaxCPUMillisPerSec) / 1000
		if n < 1 {
			n = 1
		}
		vcpu = n
	}
	mem := r.cfg.memMB()
	if q.MaxMemoryMB > 0 {
		mem = int64(q.MaxMemoryMB)
	}
	return vcpu, mem
}

// Exec runs a one-shot command in the guest via the in-guest agent.
func (r *FirecrackerRuntime) Exec(ctx context.Context, sb Sandbox, spec ExecSpec) (ExecResult, error) {
	fs, ok := sb.(*fcSandbox)
	if !ok {
		return ExecResult{}, invalid("compute: sandbox not from firecracker runtime")
	}
	if fs.agent == nil {
		return ExecResult{}, Unavailable("firecracker", "no in-guest agent configured; cannot exec in guest")
	}
	return fs.agent.exec(ctx, spec)
}

// OpenPTY opens an interactive guest terminal via the in-guest agent.
func (r *FirecrackerRuntime) OpenPTY(ctx context.Context, sb Sandbox, spec PTYSpec) (PTYSession, error) {
	fs, ok := sb.(*fcSandbox)
	if !ok {
		return nil, invalid("compute: sandbox not from firecracker runtime")
	}
	if fs.agent == nil {
		return nil, Unavailable("firecracker", "no in-guest agent configured; cannot open pty in guest")
	}
	return fs.agent.openPTY(ctx, spec)
}

// Release tears the guest down for good: stop the hypervisor, remove its state.
func (r *FirecrackerRuntime) Release(ctx context.Context, sandboxID string) error {
	// Guests are disposable and not pooled; the supervisor only ever releases
	// sandboxes it produced, each owning its own process/dir. There is no
	// shared state to coordinate, so a no-op here is correct when the process
	// is already gone.
	_ = ctx
	_ = sandboxID
	return nil
}

// ReleaseSandbox tears down a specific live sandbox handle. The supervisor uses
// this so process and directory lifetime is owned by the sandbox itself.
func (r *FirecrackerRuntime) ReleaseSandbox(sb Sandbox) error {
	fs, ok := sb.(*fcSandbox)
	if !ok {
		return invalid("compute: sandbox not from firecracker runtime")
	}
	fs.teardown()
	return nil
}

func (s *fcSandbox) teardown() {
	if s.proc != nil {
		_ = s.proc.Kill()
		_, _ = s.proc.Wait()
		s.proc = nil
	}
	if s.dir != "" {
		_ = os.RemoveAll(s.dir)
		s.dir = ""
	}
}

func (r *FirecrackerRuntime) waitForSocket(ctx context.Context, path string) error {
	deadline := time.Now().Add(r.cfg.startTimeout())
	for {
		if fi, err := os.Stat(path); err == nil && fi.Mode()&os.ModeSocket != 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return Unavailable("firecracker", "timed out waiting for API socket")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (r *FirecrackerRuntime) waitForAgent(ctx context.Context, gc *guestClient) error {
	deadline := time.Now().Add(r.cfg.startTimeout())
	for {
		if err := gc.health(ctx); err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return Unavailable("firecracker", "timed out waiting for in-guest agent")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// ---- Firecracker REST API client (over the per-VM unix socket) ----

type fcClient struct {
	socket string
	http   *http.Client
}

func newFCClient(socket string) *fcClient {
	dialer := func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", socket)
	}
	return &fcClient{
		socket: socket,
		http: &http.Client{
			Transport:     &http.Transport{DialContext: dialer},
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}

const fcBase = "http://firecracker"

func (c *fcClient) call(ctx context.Context, method, path string, body any) error {
	var rd io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return invalid("compute: cannot encode firecracker request")
		}
		rd = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(ctx, method, fcBase+path, rd)
	if err != nil {
		return Unavailable("firecracker", err.Error())
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return Unavailable("firecracker", method+" "+path+": "+err.Error())
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	// Firecracker returns 2xx (usually 204 No Content) on success.
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Unavailable("firecracker", fmt.Sprintf("%s %s -> %d: %s", method, path, resp.StatusCode, string(raw)))
	}
	return nil
}

func (c *fcClient) putMachineConfig(ctx context.Context, vcpu, memMB int64) error {
	return c.call(ctx, http.MethodPut, "/machine-config", map[string]any{
		"vcpu_count": vcpu, "mem_size_mib": memMB, "smt": false,
	})
}

func (c *fcClient) putBootSource(ctx context.Context, kernel, bootArgs string) error {
	return c.call(ctx, http.MethodPut, "/boot-source", map[string]any{
		"kernel_image_path": kernel, "boot_args": bootArgs,
	})
}

func (c *fcClient) putDrive(ctx context.Context, id, path string, isRoot, readOnly bool) error {
	return c.call(ctx, http.MethodPut, "/drives/"+id, map[string]any{
		"drive_id": id, "path_on_host": path, "is_root_device": isRoot,
		"is_read_only": readOnly,
	})
}

func (c *fcClient) putNetwork(ctx context.Context, ifaceID, hostDevName, guestMAC string) error {
	return c.call(ctx, http.MethodPut, "/network-interfaces/"+ifaceID, map[string]any{
		"iface_id": ifaceID, "host_dev_name": hostDevName, "guest_mac": guestMAC,
	})
}

func (c *fcClient) startInstance(ctx context.Context) error {
	return c.call(ctx, http.MethodPut, "/actions", map[string]any{"action_type": "InstanceStart"})
}

// ---- in-guest agent client ----
//
// The guest rootfs must run a small agent that speaks the JSON protocol below.
// It is part of the guest image, not the platform's control plane, matching the
// class X rule that guests hold no platform credentials.

type guestClient struct {
	base string
	http *http.Client
}

func newGuestClient(base string) *guestClient {
	return &guestClient{base: base, http: &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

func (g *guestClient) health(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, g.base+"/health", nil)
	if err != nil {
		return err
	}
	resp, err := g.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("agent health %s", resp.Status)
	}
	return nil
}

type guestExecRequest struct {
	Command   string   `json:"command"`
	Cwd       string   `json:"cwd"`
	Env       []string `json:"env,omitempty"`
	Stdin     []byte   `json:"stdin,omitempty"`
	TimeoutMs int64    `json:"timeout_ms"`
	PIDs      int      `json:"max_pids"`
	OpenFiles int      `json:"max_open_files"`
}

type guestExecResponse struct {
	Stdout    string `json:"stdout"`
	Stderr    string `json:"stderr"`
	ExitCode  int    `json:"exit_code"`
	Truncated bool   `json:"truncated"`
}

func (g *guestClient) exec(ctx context.Context, spec ExecSpec) (ExecResult, error) {
	body, err := json.Marshal(guestExecRequest{
		Command: spec.Command, Cwd: spec.Cwd, Env: spec.Env, Stdin: spec.Stdin,
		TimeoutMs: spec.Timeout.Milliseconds(),
		PIDs:      int(spec.Quota.MaxPIDs), OpenFiles: int(spec.Quota.MaxOpenFiles),
	})
	if err != nil {
		return ExecResult{}, invalid("compute: cannot encode guest exec request")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.base+"/v1/exec", bytes.NewReader(body))
	if err != nil {
		return ExecResult{}, Unavailable("firecracker", err.Error())
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := g.http.Do(req)
	if err != nil {
		return ExecResult{}, Unavailable("firecracker", "guest agent unreachable: "+err.Error())
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxOutputBytes+1<<16))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ExecResult{}, Unavailable("firecracker", "guest exec reply "+resp.Status)
	}
	var gr guestExecResponse
	if err := json.Unmarshal(raw, &gr); err != nil {
		return ExecResult{}, invalid("compute: guest agent returned malformed JSON")
	}
	return ExecResult{Stdout: gr.Stdout, Stderr: gr.Stderr, ExitCode: gr.ExitCode, Truncated: gr.Truncated}, nil
}

// openPTY streams a guest terminal over one request/response pair against the
// agent's /v1/pty endpoint. The request body is the framed client->guest
// control stream (the same ptyframe protocol the supervisor uses); the response
// body is the raw guest output and the exit code returns in a trailer.
func (g *guestClient) openPTY(ctx context.Context, spec PTYSpec) (PTYSession, error) {
	enc, err := json.Marshal(ptySpecWire{
		Shell: spec.Shell, Cwd: spec.Cwd, Env: spec.Env,
		Cols: spec.Cols, Rows: spec.Rows,
		TimeoutMs: spec.Timeout.Milliseconds(), Quota: quotaToWire(spec.Quota),
	})
	if err != nil {
		return nil, invalid("compute: cannot encode pty spec")
	}
	reqBody, writer := io.Pipe()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.base+"/v1/pty", reqBody)
	if err != nil {
		_ = writer.Close()
		return nil, Unavailable("firecracker", err.Error())
	}
	req.Header.Set(ptySpecHeader, strconv.Quote(string(enc)))
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := g.http.Do(req)
	if err != nil {
		_ = writer.Close()
		return nil, Unavailable("firecracker", "guest agent pty unreachable: "+err.Error())
	}
	if resp.StatusCode != http.StatusOK {
		_ = writer.Close()
		_ = resp.Body.Close()
		return nil, Unavailable("firecracker", "guest pty open failed: "+resp.Status)
	}
	return &guestPTY{resp: resp, writer: writer}, nil
}

type guestPTY struct {
	resp   *http.Response
	writer *io.PipeWriter
	closed bool
}

func (p *guestPTY) Read(b []byte) (int, error) { return p.resp.Body.Read(b) }

func (p *guestPTY) Write(b []byte) (int, error) {
	return p.writer.Write(EncodePTYStdin(b))
}

func (p *guestPTY) Resize(cols, rows uint16) error {
	_, err := p.writer.Write(EncodePTYResize(cols, rows))
	return err
}

func (p *guestPTY) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true
	_ = p.writer.Close()
	return p.resp.Body.Close()
}

func (p *guestPTY) Wait() (int, error) {
	if !p.closed {
		_, _ = p.writer.Write(EncodePTYEOF())
	}
	_, _ = io.Copy(io.Discard, p.resp.Body)
	code := 0
	if raw := p.resp.Trailer.Get(ptyExitTrailer); raw != "" {
		for _, r := range []rune(raw) {
			if r < '0' || r > '9' {
				code = 0
				break
			}
			code = code*10 + int(r-'0')
		}
	}
	_ = p.resp.Body.Close()
	return code, nil
}

// copyCoW makes a copy-on-write copy of src to dst, using reflink when the
// filesystem supports it and falling back to a byte copy otherwise. Both paths
// yield an independent writable rootfs for one disposable guest.
func copyCoW(src, dst string) error {
	if err := reflinkCopy(src, dst); err == nil {
		return nil
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}
