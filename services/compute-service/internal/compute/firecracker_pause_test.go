package compute

// firecracker_pause_test.go — the sleep/resume wire contract.
//
// Class X has to be able to sleep and wake up: the platform freezes a guest,
// writes its state somewhere it owns, and later brings THAT guest back rather
// than rebooting a fresh one. Until this file nothing in the repo spoke the three
// calls that make that possible (PATCH /vm state, PUT /snapshot/create,
// PUT /snapshot/load), so the capability did not exist at any layer - a host that
// was given KVM would still have had nothing to execute.
//
// The fake hypervisor below records every request, so these tests assert the
// WIRE: method, path and body. They do not need /dev/kvm, and they are not a
// claim that a guest booted - booting is gated elsewhere and refuses this host
// by name.

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// shortSandboxDir makes a directory whose path fits the unix-socket path limit.
// t.TempDir() is honest but long, and the hypervisor API is reached over a unix
// socket: a test that cannot bind its own socket proves nothing about the wire.
var socketDirSeq int

func shortSandboxDir(t *testing.T) string {
	t.Helper()
	socketDirSeq++
	dir := filepath.Join("/tmp", "ahp-fcp-"+itoa(os.Getpid())+"-"+itoa(socketDirSeq))
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("cannot create the short sandbox dir %s: %v", dir, err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func itoa(n int) string { return strconv.FormatInt(int64(n), 10) }

type recordedCall struct {
	Method string
	Path   string
	Body   map[string]any
}

// fakeFirecracker is a stand-in for one guest's API socket. It answers 204 to
// everything and remembers the calls in order.
type fakeFirecracker struct {
	t      *testing.T
	socket string
	ln     net.Listener
	mu     sync.Mutex
	calls  []recordedCall
	server *http.Server
}

func newFakeFirecracker(t *testing.T, socket string) *fakeFirecracker {
	t.Helper()
	return newFakeFirecrackerAfter(t, socket, 0)
}

// newFakeFirecrackerAfter starts answering after a delay. A real hypervisor
// creates its API socket only once the process is up, and the restore path is
// allowed - and required - to clear a stale socket file before waiting for that,
// so a test that bound the socket up front would have it deleted under it.
func newFakeFirecrackerAfter(t *testing.T, socket string, delay time.Duration) *fakeFirecracker {
	t.Helper()
	f := &fakeFirecracker{t: t, socket: socket}
	bind := func() {
		if delay > 0 {
			time.Sleep(delay)
		}
		_ = os.Remove(socket)
		ln, err := net.Listen("unix", socket)
		if err != nil {
			f.t.Errorf("fake hypervisor cannot listen on %s: %v", socket, err)
			return
		}
		f.mu.Lock()
		f.ln = ln
		f.mu.Unlock()
		go func() {
			if err := f.server.Serve(ln); err != nil && err != http.ErrServerClosed {
				f.t.Errorf("fake hypervisor stopped: %v", err)
			}
		}()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if r.Body != nil {
			buf := make([]byte, 4096)
			n, _ := r.Body.Read(buf)
			if n > 0 {
				_ = json.Unmarshal(buf[:n], &body)
			}
		}
		f.mu.Lock()
		f.calls = append(f.calls, recordedCall{Method: r.Method, Path: r.URL.Path, Body: body})
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	f.server = &http.Server{Handler: mux}
	if delay == 0 {
		// Bind before returning: a fake that answers "later" makes the first dial
		// a race, and a flaky wire test is worse than no test.
		bind()
	} else {
		go bind()
	}
	t.Cleanup(func() { _ = f.server.Close(); _ = os.Remove(socket) })
	return f
}

func (f *fakeFirecracker) snapshot() []recordedCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recordedCall(nil), f.calls...)
}

// liveSandbox hands back a sandbox handle whose API socket is the fake, without
// needing a hypervisor: it is the state a runtime holds after a successful boot.
func liveSandbox(dir, socket, node string) *fcSandbox {
	return &fcSandbox{id: "vm-test", node: node, dir: dir, api: newFCClient(socket)}
}

func TestPauseResumeSpeaksPatchVMState(t *testing.T) {
	dir := shortSandboxDir(t)
	sock := filepath.Join(dir, "firecracker.socket")
	fake := newFakeFirecracker(t, sock)
	rt := &FirecrackerRuntime{cfg: FirecrackerConfig{DataDir: dir, StartTimeout: time.Second}}
	sb := liveSandbox(dir, sock, "node-a")

	pauser, ok := interface{}(rt).(SleepResumer)
	if !ok {
		t.Fatalf("the firecracker runtime must be able to sleep and wake a guest; %T does not implement compute.SleepResumer", rt)
	}
	ctx := context.Background()
	if err := pauser.Pause(ctx, sb); err != nil {
		t.Fatalf("Pause: %v", err)
	}
	if err := pauser.Resume(ctx, sb); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	calls := fake.snapshot()
	if len(calls) != 2 {
		t.Fatalf("want exactly two hypervisor calls, got %d: %+v", len(calls), calls)
	}
	if calls[0].Method != http.MethodPatch || calls[0].Path != "/vm" {
		t.Errorf("pause must be PATCH /vm, got %s %s", calls[0].Method, calls[0].Path)
	}
	if got, _ := calls[0].Body["state"].(string); got != "Paused" {
		t.Errorf("pause body must name state=Paused, got %v", calls[0].Body)
	}
	if calls[1].Method != http.MethodPatch || calls[1].Path != "/vm" {
		t.Errorf("resume must be PATCH /vm, got %s %s", calls[1].Method, calls[1].Path)
	}
	if got, _ := calls[1].Body["state"].(string); got != "Resumed" {
		t.Errorf("resume body must name state=Resumed, got %v", calls[1].Body)
	}
	if sb.paused {
		t.Errorf("a resumed guest must not be tracked as paused")
	}
}

func TestSnapshotPausesFirstAndWritesInsideTheVMDir(t *testing.T) {
	dir := shortSandboxDir(t)
	sock := filepath.Join(dir, "firecracker.socket")
	fake := newFakeFirecracker(t, sock)
	rt := &FirecrackerRuntime{cfg: FirecrackerConfig{DataDir: dir, StartTimeout: time.Second}}
	sb := liveSandbox(dir, sock, "node-a")

	snap, ok := interface{}(rt).(Snapshotter)
	if !ok {
		t.Fatalf("the firecracker runtime must be able to snapshot a guest; %T does not implement compute.Snapshotter", rt)
	}
	ref, err := snap.Snapshot(context.Background(), sb)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	calls := fake.snapshot()
	if len(calls) != 2 {
		t.Fatalf("snapshot must pause the guest then create the snapshot, got %d calls: %+v", len(calls), calls)
	}
	if got, _ := calls[0].Body["state"].(string); got != "Paused" {
		t.Fatalf("a snapshot of a running guest must pause it first, first call was %+v", calls[0])
	}
	if calls[1].Method != http.MethodPut || calls[1].Path != "/snapshot/create" {
		t.Fatalf("snapshot must be PUT /snapshot/create, got %s %s", calls[1].Method, calls[1].Path)
	}
	if got, _ := calls[1].Body["snapshot_type"].(string); got != "Full" {
		t.Errorf("snapshot_type must be Full, got %v", calls[1].Body["snapshot_type"])
	}
	statePath, _ := calls[1].Body["snapshot_path"].(string)
	memPath, _ := calls[1].Body["mem_file_path"].(string)
	for _, p := range []string{statePath, memPath} {
		if p == "" {
			t.Fatalf("snapshot request must name both files, got %+v", calls[1].Body)
		}
		if filepath.Dir(p) != dir {
			t.Errorf("snapshot files must live inside the sandbox dir %s, got %s", dir, p)
		}
	}
	if ref.SandboxID != sb.ID() || ref.StatePath != statePath || ref.MemPath != memPath {
		t.Errorf("the returned reference must be the one written: %+v vs state=%s mem=%s", ref, statePath, memPath)
	}
	if ref.Node != "node-a" {
		t.Errorf("the reference must carry the node the guest sleeps on, got %q", ref.Node)
	}
	if !sb.paused {
		t.Errorf("a snapshotted guest stays paused until something wakes it")
	}
}

func TestRestoreLoadsSnapshotAndResumes(t *testing.T) {
	dir := shortSandboxDir(t)
	// A snapshot is only restorable against the disk it was taken from, and
	// refusing that is part of the contract.
	for name, content := range map[string]string{
		"rootfs.ext4":        "rootfs",
		"vm-state.snapshot":  "state",
		"vm-memory.snapshot": "memory",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	sock := filepath.Join(dir, "firecracker.socket")
	ref := SnapshotRef{
		SandboxID: "vm-test",
		Node:      "node-a",
		Dir:       dir,
		APISocket: sock,
		StatePath: filepath.Join(dir, "vm-state.snapshot"),
		MemPath:   filepath.Join(dir, "vm-memory.snapshot"),
	}
	fake := newFakeFirecrackerAfter(t, sock, 150*time.Millisecond)
	// "firecracker" here is a command that exits at once: the runtime must not
	// depend on the process staying alive to reach the API, and the fake owns the
	// socket. A real restore of course needs the real binary.
	rt := &FirecrackerRuntime{cfg: FirecrackerConfig{
		Binary:       "/usr/bin/true",
		DataDir:      filepath.Dir(dir),
		RootFSImage:  filepath.Join(dir, "rootfs.ext4"),
		StartTimeout: 2 * time.Second,
	}}
	snap, ok := interface{}(rt).(Snapshotter)
	if !ok {
		t.Fatalf("the firecracker runtime must be able to restore a snapshot; %T does not implement compute.Snapshotter", rt)
	}
	restored, err := snap.RestoreSnapshot(context.Background(), ref)
	if err != nil {
		t.Fatalf("RestoreSnapshot: %v", err)
	}
	if restored.ID() != ref.SandboxID || restored.Node() != ref.Node {
		t.Errorf("a restored guest is the SAME incarnation: got id=%q node=%q", restored.ID(), restored.Node())
	}
	calls := fake.snapshot()
	if len(calls) == 0 {
		t.Fatalf("restore must talk to the hypervisor")
	}
	last := calls[len(calls)-1]
	if last.Method != http.MethodPut || last.Path != "/snapshot/load" {
		t.Fatalf("restore must be PUT /snapshot/load, got %s %s", last.Method, last.Path)
	}
	if got, _ := last.Body["snapshot_path"].(string); got != ref.StatePath {
		t.Errorf("load must name the state file %s, got %v", ref.StatePath, last.Body["snapshot_path"])
	}
	if got, _ := last.Body["resume_vm"].(bool); !got {
		t.Errorf("loading a snapshot for a live incarnation must resume it, got %v", last.Body["resume_vm"])
	}
	mem, _ := last.Body["mem_backend"].(map[string]any)
	if mem == nil {
		t.Fatalf("load must carry a mem_backend, got %+v", last.Body)
	}
	if bt, _ := mem["backend_type"].(string); bt != "File" {
		t.Errorf("mem_backend.backend_type must be File, got %v", mem["backend_type"])
	}
	if bp, _ := mem["backend_path"].(string); !strings.HasSuffix(bp, "vm-memory.snapshot") {
		t.Errorf("mem_backend.backend_path must be the memory file, got %v", mem["backend_path"])
	}
}

func TestHostSandboxCannotClaimSleepResume(t *testing.T) {
	// Class C runs in a host jail. Freezing and thawing a jail is a different
	// operation with different guarantees, so claiming the guest capability here
	// would let a class-C sandbox be reported as a resumable class-X guest.
	if _, ok := interface{}(&HostBackend{}).(SleepResumer); ok {
		t.Fatalf("the host runtime must not advertise guest sleep/resume")
	}
	if _, ok := interface{}(&HostBackend{}).(Snapshotter); ok {
		t.Fatalf("the host runtime must not advertise guest snapshots")
	}
}

func TestPauseRejectsAForeignSandboxHandle(t *testing.T) {
	rt := &FirecrackerRuntime{cfg: FirecrackerConfig{DataDir: t.TempDir(), StartTimeout: time.Second}}
	err := rt.Pause(context.Background(), foreignSandbox{})
	if err == nil {
		t.Fatalf("pausing a sandbox this runtime did not create must be refused, not ignored")
	}
}

type foreignSandbox struct{}

func (foreignSandbox) ID() string      { return "not-mine" }
func (foreignSandbox) Backend() string { return "other" }
func (foreignSandbox) Node() string    { return "node-z" }
