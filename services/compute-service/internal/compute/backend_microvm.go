package compute

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"time"
)

// MicroVMBackend is a client to the Linux microVM supervisor (Firecracker or
// Kata) that actually starts a disposable guest per incarnation. It talks to
// the supervisor over a unix socket using a small JSON-over-HTTP protocol
// (/v1/health, /v1/acquire, /v1/exec, /v1/release, /v1/pty).
//
// It is a real substrate and it is honest about its environment: Probe fails
// with UnavailableError whenever /dev/kvm is absent (e.g. on a macOS control
// host) or the supervisor socket is not answering /v1/health. It NEVER starts
// a process on the control host and NEVER claims to have run a microVM when it
// did not; class X therefore fails closed on hosts without KVM instead of
// silently degrading to the host backend (see Registry.Require).
type MicroVMBackend struct {
	name   string
	socket string
	client *http.Client

	// skipKVMCheck is a test-only escape hatch so the client transport can be
	// exercised on a control host without KVM against a loopback fake
	// supervisor. NewMicroVMBackend never sets it; production always verifies
	// /dev/kvm before touching the socket.
	skipKVMCheck bool
}

// supervisorBase is the opaque URL host used for HTTP-over-unix requests; the
// dialer ignores it and always dials the unix socket.
const supervisorBase = "http://ahp-supervisor"

// NewMicroVMBackend builds a microVM backend named name ("firecracker" or
// "kata") that reaches the supervisor at the unix socket socket.
func NewMicroVMBackend(name, socket string) *MicroVMBackend {
	if name == "" {
		name = "firecracker"
	}
	dialer := func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "unix", socket)
	}
	return &MicroVMBackend{
		name:   name,
		socket: socket,
		client: &http.Client{
			Transport: &http.Transport{DialContext: dialer},
			// No redirect following: the supervisor is a fixed local peer.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}

// Name is the concrete microVM backend label ("firecracker"/"kata").
func (b *MicroVMBackend) Name() string { return b.name }

// kvmDevice is where the kernel exposes hardware virtualisation.
const kvmDevice = "/dev/kvm"

// kvmDevicePath is the node the KVM gate looks at. It is a variable so a test
// can point it at a device it is not allowed to open; production uses the
// kernel's node.
var kvmDevicePath = kvmDevice

// kvmProblem names the reason this process cannot boot a guest, or "" when it
// can. The gate opens the device read-write instead of stat-ing it, because
// existence is not availability: on a shared VM the node is usually there with
// mode 0660 root:kvm while the calling user is not in group kvm. A Stat-only
// gate reports that host as ready, the guest then dies on EACCES, and the
// blocker has been masked by the one component trusted to name it.
func kvmProblem() string {
	f, err := os.OpenFile(kvmDevicePath, os.O_RDWR, 0)
	if err == nil {
		_ = f.Close()
		return ""
	}
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return "KVM not available on this host: " + kvmDevicePath + " does not exist"
	case errors.Is(err, fs.ErrPermission):
		return "KVM device is present but this user may not use it: opening " + kvmDevicePath +
			" read-write is refused (add the user to group kvm or grant an ACL); this is not a missing-hypervisor error"
	default:
		return "KVM device is not usable: " + err.Error()
	}
}

// kvmHostProblem is the gate the production callers use. It keeps the two
// existing reasons (missing node, node that this user may not open) and adds the
// third one those two cannot see: a node that opens happily and is still
// refused by the kernel when a VM is requested. Openability was never
// bootability, and a class-X claim built on it would be a false isolation claim.
func kvmHostProblem() string {
	if msg := kvmProblem(); msg != "" {
		return msg
	}
	return kvmVMCreateProblem()
}

// Probe reports readiness. It requires BOTH a usable /dev/kvm on the node and
// a supervisor that answers /v1/health. Either failing is an UnavailableError;
// on a macOS control host this is always "KVM not available on this host",
// which is the truthful state, not a simulation of a microVM.
func (b *MicroVMBackend) Probe(ctx context.Context) error {
	if !b.skipKVMCheck {
		if msg := kvmHostProblem(); msg != "" {
			return Unavailable(b.name, msg)
		}
	}
	hctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(hctx, http.MethodGet, supervisorBase+"/v1/health", nil)
	if err != nil {
		return Unavailable(b.name, err.Error())
	}
	resp, err := b.client.Do(req)
	if err != nil {
		return Unavailable(b.name, "supervisor not reachable on "+b.socket)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return Unavailable(b.name, "supervisor unhealthy: "+resp.Status)
	}
	return nil
}

// microSandbox is a live guest identified by the supervisor's VM id.
type microSandbox struct {
	id      string
	node    string
	backend string
}

func (s *microSandbox) ID() string      { return s.id }
func (s *microSandbox) Backend() string { return s.backend }
func (s *microSandbox) Node() string    { return s.node }

// ---- wire shapes (backend<->supervisor protocol) ----

type quotaWire struct {
	MaxCPUMillisPerSec int32 `json:"max_cpu_millis_per_sec"`
	MaxMemoryMB        int32 `json:"max_memory_mb"`
	MaxPIDs            int32 `json:"max_pids"`
	MaxPTYs            int32 `json:"max_ptys"`
	MaxOpenFiles       int32 `json:"max_open_files"`
	MaxStorageMB       int32 `json:"max_storage_mb"`
	MaxSessionTTLMs    int64 `json:"max_session_ttl_ms"`
}

func quotaToWire(q Quota) quotaWire {
	return quotaWire{
		MaxCPUMillisPerSec: q.MaxCPUMillisPerSec, MaxMemoryMB: q.MaxMemoryMB,
		MaxPIDs: q.MaxPIDs, MaxPTYs: q.MaxPTYs, MaxOpenFiles: q.MaxOpenFiles,
		MaxStorageMB: q.MaxStorageMB, MaxSessionTTLMs: q.MaxSessionTTL.Milliseconds(),
	}
}

func (q quotaWire) toQuota() Quota {
	return Quota{
		MaxCPUMillisPerSec: q.MaxCPUMillisPerSec, MaxMemoryMB: q.MaxMemoryMB,
		MaxPIDs: q.MaxPIDs, MaxPTYs: q.MaxPTYs, MaxOpenFiles: q.MaxOpenFiles,
		MaxStorageMB: q.MaxStorageMB, MaxSessionTTL: time.Duration(q.MaxSessionTTLMs) * time.Millisecond,
	}
}

type acquireRequest struct {
	ComputerID  string    `json:"computer_id"`
	Incarnation int64     `json:"incarnation"`
	VolumeID    string    `json:"volume_id"`
	Fence       int64     `json:"fence"`
	Class       string    `json:"class"`
	Profile     string    `json:"profile"`
	Quota       quotaWire `json:"quota"`
	JailRoots   []string  `json:"jail_roots"`
	Node        string    `json:"node"`
}

type acquireResponse struct {
	SandboxID string `json:"sandbox_id"`
	Node      string `json:"node"`
	// MockMarker is filled only by a backend that implements Marker, i.e. a
	// simulation. A real backend omits the field entirely, so the wire shape of
	// every accepted deployment is unchanged; a caller that sees it knows the
	// sandbox it just acquired executes nothing.
	MockMarker string `json:"mock_marker,omitempty"`
}

type execRequest struct {
	SandboxID string    `json:"sandbox_id"`
	Command   string    `json:"command"`
	Cwd       string    `json:"cwd"`
	Env       []string  `json:"env,omitempty"`
	Stdin     []byte    `json:"stdin,omitempty"`
	TimeoutMs int64     `json:"timeout_ms"`
	Quota     quotaWire `json:"quota"`
	Egress    []string  `json:"egress,omitempty"`
}

type execResponse struct {
	Stdout    string `json:"stdout"`
	Stderr    string `json:"stderr"`
	ExitCode  int    `json:"exit_code"`
	Truncated bool   `json:"truncated"`
}

type releaseRequest struct {
	SandboxID string `json:"sandbox_id"`
}

// ptySpecHeader carries the base64 JSON of a PTYSpec on the /v1/pty request so
// the request body is reserved for the framed client->guest stream.
const ptySpecHeader = "Ahp-PTY-Spec"

// ptyExitTrailer is set by the supervisor when the guest shell exits.
const ptyExitTrailer = "Ahp-PTY-Exit"

// ptySandboxHeader routes the /v1/pty request to a specific live guest.
const ptySandboxHeader = "Ahp-PTY-Sandbox"

type ptySpecWire struct {
	Shell     string    `json:"shell"`
	Cwd       string    `json:"cwd"`
	Env       []string  `json:"env,omitempty"`
	Cols      uint16    `json:"cols"`
	Rows      uint16    `json:"rows"`
	TimeoutMs int64     `json:"timeout_ms"`
	Quota     quotaWire `json:"quota"`
}

// Acquire asks the supervisor to start a disposable guest for this incarnation.
func (b *MicroVMBackend) Acquire(ctx context.Context, spec AcquireSpec) (Sandbox, error) {
	body := acquireRequest{
		ComputerID: spec.ComputerID, Incarnation: spec.Incarnation,
		VolumeID: spec.VolumeID, Fence: spec.Fence,
		Class: string(spec.Class), Profile: string(spec.Profile),
		Quota: quotaToWire(spec.Quota), JailRoots: spec.JailRoots, Node: spec.Node,
	}
	var out acquireResponse
	if err := b.post(ctx, "/v1/acquire", body, &out); err != nil {
		return nil, err
	}
	if out.SandboxID == "" {
		return nil, Unavailable(b.name, "supervisor returned an empty sandbox id")
	}
	node := out.Node
	if node == "" {
		node = b.name + ":" + spec.Node
	}
	return &microSandbox{id: out.SandboxID, node: node, backend: b.name}, nil
}

// Release tears the guest down for good; it is never returned to a pool.
func (b *MicroVMBackend) Release(ctx context.Context, sandboxID string) error {
	return b.post(ctx, "/v1/release", releaseRequest{SandboxID: sandboxID}, nil)
}

// Exec runs one command inside the guest. The supervisor applies the class
// quota and egress policy inside the VM; this client only transports them.
func (b *MicroVMBackend) Exec(ctx context.Context, sb Sandbox, spec ExecSpec) (ExecResult, error) {
	ms, ok := sb.(*microSandbox)
	if !ok {
		return ExecResult{}, invalid("compute: sandbox not from microvm backend")
	}
	body := execRequest{
		SandboxID: ms.id, Command: spec.Command, Cwd: spec.Cwd,
		Env: spec.Env, Stdin: spec.Stdin, TimeoutMs: spec.Timeout.Milliseconds(),
		Quota: quotaToWire(spec.Quota), Egress: spec.Egress,
	}
	var out execResponse
	if err := b.post(ctx, "/v1/exec", body, &out); err != nil {
		return ExecResult{}, err
	}
	return ExecResult{Stdout: out.Stdout, Stderr: out.Stderr, ExitCode: out.ExitCode, Truncated: out.Truncated}, nil
}

// OpenPTY opens an interactive terminal inside the guest over a single
// streaming request: the request body is the framed stdin/control stream and
// the response body is the raw guest output. The spec travels on a header.
func (b *MicroVMBackend) OpenPTY(ctx context.Context, sb Sandbox, spec PTYSpec) (PTYSession, error) {
	ms, ok := sb.(*microSandbox)
	if !ok {
		return nil, invalid("compute: sandbox not from microvm backend")
	}
	enc, err := json.Marshal(ptySpecWire{
		Shell: spec.Shell, Cwd: spec.Cwd, Env: spec.Env,
		Cols: spec.Cols, Rows: spec.Rows,
		TimeoutMs: spec.Timeout.Milliseconds(), Quota: quotaToWire(spec.Quota),
	})
	if err != nil {
		return nil, invalid("compute: cannot encode pty spec")
	}
	if len(enc) > maxSpecBytes {
		return nil, invalid("compute: pty spec exceeds limit")
	}
	reqBody, writer := io.Pipe()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, supervisorBase+"/v1/pty", reqBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set(ptySpecHeader, base64.StdEncoding.EncodeToString(enc))
	req.Header.Set(ptySandboxHeader, ms.id)
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := b.client.Do(req)
	if err != nil {
		_ = writer.Close()
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		_ = writer.Close()
		_ = resp.Body.Close()
		return nil, Unavailable(b.name, "supervisor pty open failed: "+resp.Status)
	}
	return &microPTY{resp: resp, writer: writer}, nil
}

// microPTY streams over the single /v1/pty request/response pair.
type microPTY struct {
	resp   *http.Response
	writer *io.PipeWriter
	closed bool
}

func (p *microPTY) Read(b []byte) (int, error) { return p.resp.Body.Read(b) }

func (p *microPTY) Write(b []byte) (int, error) {
	return p.writer.Write(EncodePTYStdin(b))
}

func (p *microPTY) Resize(cols, rows uint16) error {
	_, err := p.writer.Write(EncodePTYResize(cols, rows))
	return err
}

func (p *microPTY) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true
	_ = p.writer.Close()
	return p.resp.Body.Close()
}

// Wait drains the remaining guest output and reads the exit code the
// supervisor reports in a trailer.
func (p *microPTY) Wait() (int, error) {
	if !p.closed {
		_, _ = p.writer.Write(mustEOF())
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

func mustEOF() []byte {
	return EncodePTYEOF()
}

// post issues a JSON POST to the supervisor and decodes an optional response.
func (b *MicroVMBackend) post(ctx context.Context, path string, in, out any) error {
	payload, err := json.Marshal(in)
	if err != nil {
		return invalid("compute: cannot encode supervisor request")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, supervisorBase+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxOutputBytes))
	if err != nil {
		return err
	}
	switch {
	case resp.StatusCode == http.StatusServiceUnavailable:
		return Unavailable(b.name, "supervisor reports unavailable: "+string(raw))
	case resp.StatusCode < 200 || resp.StatusCode >= 300:
		return unavailableStatus(b.name, resp.StatusCode, string(raw))
	}
	if out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return invalid("compute: supervisor returned malformed JSON")
		}
	}
	return nil
}

// unavailableStatus renders a non-2xx supervisor reply as a classified error.
// 429/5xx mean the guest or supervisor is transiently unavailable and surface
// as Unavailable so the control plane can react; other codes are client errors.
func unavailableStatus(name string, code int, raw string) error {
	msg := fmt.Sprintf("%s supervisor reply %d: %s", name, code, raw)
	if code == http.StatusTooManyRequests || code >= http.StatusInternalServerError {
		return Unavailable(name, msg)
	}
	return invalid(msg)
}
