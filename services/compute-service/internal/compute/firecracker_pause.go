package compute

// Sleep and resume for class X guests.
//
// A guest that cannot sleep is a guest that must be destroyed and rebuilt, and a
// rebuilt guest is a DIFFERENT incarnation: its memory, its open files and its
// position in the run are gone. The architecture asks for the incarnation itself
// to be parked and woken, which is exactly the three Firecracker calls this file
// speaks and nothing else did before:
//
//	PATCH /vm              {"state":"Paused"} / {"state":"Resumed"}
//	PUT   /snapshot/create {"snapshot_type":"Full","snapshot_path":...,"mem_file_path":...}
//	PUT   /snapshot/load   {"snapshot_path":...,"mem_backend":{...},"resume_vm":true}
//
// Both capabilities are OPTIONAL on purpose. A host jail (class C) has no guest
// to freeze, and a runtime that cannot freeze one must be refused by name rather
// than answer "done" - a caller that believes a sandbox slept when it kept
// running has been handed a false isolation and scheduling fact.
//
// What this file does NOT do: it never claims a guest booted. Booting stays
// behind the KVM gate in backend_microvm.go, which refuses a host whose /dev/kvm
// this user may not open. The supervisor that owns live guests already passed
// that gate at startup, which is why restoring is not gated a second time here;
// a snapshot the kernel refuses to load is reported as the hypervisor's own
// error, not swallowed.

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// VMStatePaused / VMStateResumed are the two states of the guest the wire names.
// They are Firecracker's vocabulary, verbatim, because a translation layer here
// would only add a way to be wrong.
const (
	VMStatePaused  = "Paused"
	VMStateResumed = "Resumed"
)

// SleepResumer is the optional capability of a runtime that can freeze and thaw
// one of its own live sandboxes without losing guest state.
type SleepResumer interface {
	// Pause stops the vCPUs. The guest keeps its memory and devices; nothing is
	// written to disk by this call, so a paused guest still dies with its process.
	Pause(ctx context.Context, sb Sandbox) error
	// Resume lets a paused guest run again. Resuming a guest that was never
	// paused is not an error the platform invents: the hypervisor is asked, and
	// its answer is returned.
	Resume(ctx context.Context, sb Sandbox) error
}

// SnapshotRef points at a parked guest: the two files the hypervisor wrote plus
// enough identity to bring the SAME incarnation back.
type SnapshotRef struct {
	// SandboxID and Node are preserved so a restored handle is the incarnation
	// that was parked, not a new one wearing its id.
	SandboxID string
	Node      string
	// Dir is the sandbox working directory the snapshot belongs to. A snapshot
	// records the guest's device paths, so it is only loadable from the same dir.
	Dir string
	// APISocket is the socket path the parked guest's process used; the same path
	// is reused on restore, which is what keeps the recorded paths valid.
	APISocket string
	// StatePath holds the serialized device/vCPU state, MemPath the guest memory.
	StatePath string
	MemPath   string
	CreatedAt time.Time
}

// Snapshotter is the optional capability of a runtime that can park a guest to
// disk and bring that exact incarnation back.
type Snapshotter interface {
	// Snapshot freezes the guest (pausing it first if it is running) and writes
	// its state and memory inside the sandbox directory. The guest stays paused:
	// a caller that wants it running again calls Resume.
	Snapshot(ctx context.Context, sb Sandbox) (SnapshotRef, error)
	// RestoreSnapshot starts a hypervisor over the recorded socket, loads the
	// snapshot and resumes it. A missing or unreadable snapshot is an error; it is
	// never silently replaced by a fresh boot, because that would hand the caller
	// a blank guest dressed up as the one it parked.
	RestoreSnapshot(ctx context.Context, ref SnapshotRef) (Sandbox, error)
}

// snapshotFileNames are fixed, not caller-supplied: an operator-provided path
// here would let one guest's snapshot be written over another's, and the files
// are only ever meaningful next to the rootfs they belong to.
const (
	snapshotStateFile = "vm-state.snapshot"
	snapshotMemFile   = "vm-memory.snapshot"
	snapshotAPISock   = "firecracker.socket"
)

// Pause freezes one of this runtime's own guests.
func (r *FirecrackerRuntime) Pause(ctx context.Context, sb Sandbox) error {
	fs, err := r.ownSandbox(sb, "pause")
	if err != nil {
		return err
	}
	fs.mu.Lock()
	already := fs.paused
	fs.mu.Unlock()
	if already {
		// Answering twice as if the hypervisor had been asked again would hide a
		// caller that lost track of its guest. Say so instead.
		return invalid("compute: guest " + fs.id + " is already paused")
	}
	if err := fs.api.patchVMState(ctx, VMStatePaused); err != nil {
		return err
	}
	fs.mu.Lock()
	fs.paused = true
	fs.mu.Unlock()
	return nil
}

// Resume wakes a paused guest.
func (r *FirecrackerRuntime) Resume(ctx context.Context, sb Sandbox) error {
	fs, err := r.ownSandbox(sb, "resume")
	if err != nil {
		return err
	}
	if err := fs.api.patchVMState(ctx, VMStateResumed); err != nil {
		return err
	}
	fs.mu.Lock()
	fs.paused = false
	fs.mu.Unlock()
	return nil
}

// Snapshot parks the guest to disk inside its own directory.
func (r *FirecrackerRuntime) Snapshot(ctx context.Context, sb Sandbox) (SnapshotRef, error) {
	fs, err := r.ownSandbox(sb, "snapshot")
	if err != nil {
		return SnapshotRef{}, err
	}
	fs.mu.Lock()
	dir, id, node, paused := fs.dir, fs.id, fs.node, fs.paused
	fs.mu.Unlock()
	if dir == "" {
		return SnapshotRef{}, invalid("compute: guest " + id + " has no working directory to snapshot into")
	}
	if !paused {
		if err := r.Pause(ctx, sb); err != nil {
			return SnapshotRef{}, err
		}
	}
	state := filepath.Join(dir, snapshotStateFile)
	mem := filepath.Join(dir, snapshotMemFile)
	if err := fs.api.createSnapshot(ctx, state, mem); err != nil {
		return SnapshotRef{}, err
	}
	return SnapshotRef{
		SandboxID: id, Node: node, Dir: dir, APISocket: filepath.Join(dir, snapshotAPISock),
		StatePath: state, MemPath: mem, CreatedAt: time.Now().UTC(),
	}, nil
}

// RestoreSnapshot brings the recorded incarnation back and resumes it.
func (r *FirecrackerRuntime) RestoreSnapshot(ctx context.Context, ref SnapshotRef) (_ Sandbox, restoreErr error) {
	// A restore needs the hypervisor binary and the recorded files, and NOT the
	// boot configuration: the kernel and the machine shape are inside the snapshot
	// state, which is exactly why waking a guest is not booting one. Asking for
	// KernelImage here would reject a host that can restore and cannot boot, and
	// the reverse would be worse - booting a fresh guest and calling it restored.
	if r.cfg.Binary == "" {
		return nil, invalid("compute: restoring a snapshot needs the hypervisor binary")
	}
	if ref.Dir == "" || ref.StatePath == "" || ref.MemPath == "" {
		return nil, invalid("compute: a snapshot reference must name its directory, state file and memory file")
	}
	// The snapshot is only a checkpoint of the paths it was written with: the
	// rootfs it recorded must still be where it was, or waking the guest would
	// resume it against someone else's disk.
	for _, p := range []string{ref.StatePath, ref.MemPath} {
		if _, err := os.Stat(p); err != nil {
			return nil, Unavailable("firecracker", "snapshot file is not restorable: "+p)
		}
	}
	rootfs := filepath.Join(ref.Dir, "rootfs.ext4")
	if _, err := os.Stat(rootfs); err != nil {
		return nil, Unavailable("firecracker", "the rootfs this snapshot was taken against is gone: "+ref.Dir)
	}
	sock := ref.APISocket
	if sock == "" {
		sock = filepath.Join(ref.Dir, snapshotAPISock)
	}
	// A parked guest owns no process, so the socket file left behind by the old
	// one is stale and must not be confused with a live API.
	_ = os.Remove(sock)
	sb := &fcSandbox{id: ref.SandboxID, node: ref.Node, dir: ref.Dir, rootfs: rootfs, tapName: r.cfg.TapName}
	cmd := exec.Command(r.cfg.Binary, "--api-sock", sock, "--id", ref.SandboxID)
	if err := sb.start(ctx, cmd); err != nil {
		return nil, Unavailable("firecracker", "cannot start hypervisor to restore: "+err.Error())
	}
	defer func() {
		if restoreErr != nil {
			stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			restoreErr = errors.Join(restoreErr, sb.stop(stopCtx, false))
		}
	}()

	api := newFCClient(sock)
	sb.api = api
	if err := r.waitForSocket(ctx, sock); err != nil {
		return nil, err
	}
	if err := api.loadSnapshot(ctx, ref.StatePath, ref.MemPath, true); err != nil {
		return nil, err
	}
	if r.cfg.GuestAgent != "" {
		gc := newGuestClient(r.cfg.GuestAgent)
		sb.agent = gc
	}
	return sb, nil
}

// ownSandbox keeps every sleep/resume call honest about ownership: the handle a
// caller passes must be one this runtime produced.
func (r *FirecrackerRuntime) ownSandbox(sb Sandbox, verb string) (*fcSandbox, error) {
	fs, ok := sb.(*fcSandbox)
	if !ok {
		return nil, invalid("compute: cannot " + verb + " a sandbox this runtime did not create")
	}
	if fs.api == nil {
		return nil, invalid("compute: guest " + fs.id + " has no live hypervisor API to " + verb + " through")
	}
	return fs, nil
}

// compile-time proof that the firecracker runtime really carries the two
// capabilities the sleep/resume story needs.
var (
	_ SleepResumer = (*FirecrackerRuntime)(nil)
	_ Snapshotter  = (*FirecrackerRuntime)(nil)
)

// ---- wire ----

func (c *fcClient) patchVMState(ctx context.Context, state string) error {
	return c.call(ctx, "PATCH", "/vm", map[string]any{"state": state})
}

func (c *fcClient) createSnapshot(ctx context.Context, statePath, memPath string) error {
	return c.call(ctx, "PUT", "/snapshot/create", map[string]any{
		"snapshot_type": "Full",
		"snapshot_path": statePath,
		"mem_file_path": memPath,
	})
}

func (c *fcClient) loadSnapshot(ctx context.Context, statePath, memPath string, resume bool) error {
	return c.call(ctx, "PUT", "/snapshot/load", map[string]any{
		"snapshot_path": statePath,
		"mem_backend": map[string]any{
			"backend_type": "File",
			"backend_path": memPath,
		},
		"enable_diff_snapshots": false,
		"resume_vm":             resume,
	})
}
