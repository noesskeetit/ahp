package compute

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// VMRuntime is the seam where a real microVM substrate (a Firecracker or Kata
// hypervisor driver running on a KVM node) plugs into the Supervisor. It is
// exactly the union of the Backend lifecycle plus command and PTY execution.
//
// There is deliberately NO in-process implementation of VMRuntime that runs a
// command on the control host. The Supervisor must be handed a real runtime by
// the Linux supervisor entrypoint; without one it refuses to start, so a
// "microVM" is never simulated beneath a misleading label.
type VMRuntime interface {
	Backend
	CommandBackend
	PTYBackend
}

// Supervisor is the out-of-guest daemon that serves the MicroVMBackend client
// protocol over a unix socket and delegates every operation to a VMRuntime. It
// is the host-side half of the class X path described in the architecture:
// /v1/health, /v1/acquire, /v1/exec, /v1/pty, /v1/release.
//
// It owns the set of live guests so a later exec/pty call can be routed to the
// concrete Sandbox produced by acquire. Guests are disposable: Release tears a
// guest down for good and it is never returned to a pool.
type Supervisor struct {
	socket string
	rt     VMRuntime

	mu   sync.Mutex
	live map[string]Sandbox
}

// NewSupervisor builds a supervisor bound to the unix socket socket, backed by
// the runtime rt. It fails closed when no real runtime is supplied: a
// supervisor with no hypervisor cannot honestly serve class X.
func NewSupervisor(socket string, rt VMRuntime) (*Supervisor, error) {
	if socket == "" {
		return nil, invalid("compute: supervisor socket must not be empty")
	}
	if rt == nil {
		return nil, Unavailable("supervisor", "no microVM runtime supplied; refusing to start a supervisor without a real hypervisor")
	}
	return &Supervisor{socket: socket, rt: rt, live: make(map[string]Sandbox)}, nil
}

// Socket reports the unix socket path the supervisor serves on.
func (s *Supervisor) Socket() string { return s.socket }

// Handler exposes the protocol routes, useful for embedding and loopback tests.
func (s *Supervisor) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/health", s.handleHealth)
	mux.HandleFunc("/v1/acquire", s.handleAcquire)
	mux.HandleFunc("/v1/exec", s.handleExec)
	mux.HandleFunc("/v1/release", s.handleRelease)
	mux.HandleFunc("/v1/pty", s.handlePTY)
	mux.HandleFunc("/v1/pause", s.handlePause)
	mux.HandleFunc("/v1/resume", s.handleResume)
	mux.HandleFunc("/v1/snapshot", s.handleSnapshot)
	return mux
}

// ListenAndServe binds the unix socket and serves until ctx is cancelled. A
// stale socket file from a previous crash is replaced. The socket is removed on
// exit so a restart can bind cleanly.
func (s *Supervisor) ListenAndServe(ctx context.Context) error {
	if err := os.MkdirAll(filepath.Dir(s.socket), 0o750); err != nil {
		return Unavailable("supervisor", "cannot create socket directory: "+err.Error())
	}
	// Replace a stale socket left behind by a previous process.
	if fi, err := os.Stat(s.socket); err == nil && fi.Mode()&os.ModeSocket != 0 {
		_ = os.Remove(s.socket)
	}
	ln, err := net.Listen("unix", s.socket)
	if err != nil {
		return Unavailable("supervisor", "cannot bind socket: "+err.Error())
	}
	_ = os.Chmod(s.socket, 0o660)
	srv := &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 0}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	err = srv.Serve(ln)
	_ = os.Remove(s.socket)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func (s *Supervisor) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := s.rt.Probe(r.Context()); err != nil {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Supervisor) handleAcquire(w http.ResponseWriter, r *http.Request) {
	var in acquireRequest
	if err := decodeSupervisorJSON(r, &in); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	spec := AcquireSpec{
		ComputerID: in.ComputerID, Incarnation: in.Incarnation,
		VolumeID: in.VolumeID, Fence: in.Fence,
		Class: Class(in.Class), Profile: Profile(in.Profile),
		Quota: in.Quota.toQuota(), JailRoots: in.JailRoots, Node: in.Node,
	}
	sb, err := s.rt.Acquire(r.Context(), spec)
	if err != nil {
		supervisorError(w, err)
		return
	}
	s.mu.Lock()
	s.live[sb.ID()] = sb
	s.mu.Unlock()
	resp := acquireResponse{SandboxID: sb.ID(), Node: sb.Node()}
	if m, ok := s.rt.(Marker); ok {
		resp.MockMarker = m.MockMarker()
	}
	writeSupervisorJSON(w, resp)
}

func (s *Supervisor) handleExec(w http.ResponseWriter, r *http.Request) {
	var in execRequest
	if err := decodeSupervisorJSON(r, &in); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	sb, ok := s.sandbox(in.SandboxID)
	if !ok {
		http.Error(w, "unknown sandbox", http.StatusNotFound)
		return
	}
	cb, ok := s.rt.(CommandBackend)
	if !ok {
		supervisorError(w, Unavailable("supervisor", "runtime does not support exec"))
		return
	}
	res, err := cb.Exec(r.Context(), sb, ExecSpec{
		Command: in.Command, Cwd: in.Cwd, Env: in.Env, Stdin: in.Stdin,
		Timeout: millisToDuration(in.TimeoutMs), Quota: in.Quota.toQuota(), Egress: in.Egress,
	})
	if err != nil {
		supervisorError(w, err)
		return
	}
	writeSupervisorJSON(w, execResponse{Stdout: res.Stdout, Stderr: res.Stderr, ExitCode: res.ExitCode, Truncated: res.Truncated})
}

func (s *Supervisor) handleRelease(w http.ResponseWriter, r *http.Request) {
	var in releaseRequest
	if err := decodeSupervisorJSON(r, &in); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	_, ok := s.live[in.SandboxID]
	delete(s.live, in.SandboxID)
	s.mu.Unlock()
	if !ok {
		// Releasing an already-gone guest is idempotent.
		w.WriteHeader(http.StatusOK)
		return
	}
	if err := s.rt.Release(r.Context(), in.SandboxID); err != nil {
		supervisorError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// handlePTY streams a guest terminal over one request/response pair. The
// request body is the framed client->guest control stream; the response body is
// the raw guest output and the exit code travels back in a trailer. When the
// client sends an EOF frame (it is detaching), the supervisor closes the guest
// session.
func (s *Supervisor) handlePTY(w http.ResponseWriter, r *http.Request) {
	spec, err := decodePTYSpecHeader(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	sb, ok := s.sandbox(r.Header.Get(ptySandboxHeader))
	if !ok {
		http.Error(w, "unknown sandbox", http.StatusNotFound)
		return
	}
	pb, ok := s.rt.(PTYBackend)
	if !ok {
		supervisorError(w, Unavailable("supervisor", "runtime does not support pty"))
		return
	}
	session, err := pb.OpenPTY(r.Context(), sb, spec)
	if err != nil {
		supervisorError(w, err)
		return
	}

	// Announce the exit-code trailer before the first body byte.
	w.Header().Set("Trailer", ptyExitTrailer)
	rc := http.NewResponseController(w)

	// Pump the framed control stream into the guest. When the client sends an
	// EOF frame or closes the stream, close the session so the copy loop ends.
	go func() {
		for {
			typ, payload, derr := DecodePTYFrame(r.Body)
			if derr != nil {
				_ = session.Close()
				return
			}
			switch typ {
			case 'r':
				if cols, rows, ok := DecodePTYResize(payload); ok {
					_ = session.Resize(cols, rows)
				}
			case 'd':
				if _, werr := session.Write(payload); werr != nil {
					_ = session.Close()
					return
				}
			case 'e':
				_ = session.Close()
				return
			default:
				_ = session.Close()
				return
			}
		}
	}()

	buf := make([]byte, 32*1024)
	for {
		n, rerr := session.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				_ = session.Close()
				return
			}
			_ = rc.Flush()
		}
		if rerr != nil {
			break
		}
	}
	code, _ := session.Wait()
	w.Header().Set(ptyExitTrailer, strconv.Itoa(code))
}

func (s *Supervisor) sandbox(id string) (Sandbox, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sb, ok := s.live[id]
	return sb, ok
}

func decodeSupervisorJSON(r *http.Request, out any) error {
	defer func() { _ = r.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxSpecBytes*8))
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}

func decodePTYSpecHeader(r *http.Request) (PTYSpec, error) {
	var out PTYSpec
	raw, err := base64.StdEncoding.DecodeString(r.Header.Get(ptySpecHeader))
	if err != nil {
		return out, err
	}
	var w ptySpecWire
	if err := json.Unmarshal(raw, &w); err != nil {
		return out, err
	}
	return PTYSpec{
		Shell: w.Shell, Cwd: w.Cwd, Env: w.Env, Cols: w.Cols, Rows: w.Rows,
		Timeout: millisToDuration(w.TimeoutMs), Quota: w.Quota.toQuota(),
	}, nil
}

func writeSupervisorJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// supervisorError maps a runtime error to a supervisor status. An unavailable
// runtime surfaces as 503 (the client reclassifies it as Unavailable); every
// other error is a client-visible 400.
func supervisorError(w http.ResponseWriter, err error) {
	if IsUnavailable(err) {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	http.Error(w, err.Error(), http.StatusBadRequest)
}

// millisToDuration converts a wire int64 millisecond value to a Duration.
func millisToDuration(ms int64) time.Duration { return time.Duration(ms) * time.Millisecond }
