package compute

// MOCK compute backend.
//
// This file exists because the X-class microVM cannot be booted where the
// platform is currently developed and tested (measured: open /dev/kvm -> EACCES
// for uid 1000, cgroup2 root not writable, no firecracker/jailer/vmlinux/rootfs
// on the host). The customer decision is to exercise the control plane against a
// mock backend instead of waiting for that environment.
//
// What this backend IS: an in-process state machine that satisfies the Supervisor
// contract -- acquire / pause / resume / snapshot / restore / release -- so the
// lifecycle, the fencing and the refusal paths can be tested and demonstrated.
//
// What this backend is NOT, stated as many times as it needs to be: it is not a
// virtual machine, it provides NO isolation of any kind, it never executes a
// command, and it must never be presented as evidence about escape resistance,
// latency, density or resource-accounting. It therefore deliberately does NOT
// implement CommandBackend or PTYBackend: there is no code path here that could
// run untrusted bytes on the host, and the test suite pins that absence rather
// than asserting it in a comment.
//
// Every answer this backend gives carries the mock marker: in the API payload
// (acquireResponse.MockMarker), in the audit line written for each lifecycle
// operation, and in the backend's own description. A caller that ignores the
// marker is choosing to, not being misled by us.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

const (
	// MockBackendName is the stable label stored on an incarnation produced by
	// this backend. It is "mock", never "host" and never "microvm".
	MockBackendName = "mock"
	// MockMarker is the disclosure text. It is one constant so the API field, the
	// audit line and any refusal cannot drift into three different claims.
	MockMarker = "MOCK-COMPUTE-BACKEND: in-process simulation, no hypervisor, no VM, no isolation; " +
		"untrusted code is never executed by this backend. Not evidence about isolation, escape, latency or density."
)

// Marker is implemented by backends whose nature a caller must be told about on
// every answer. The supervisor asks for it rather than pattern-matching a name,
// so a real backend stays byte-identical on the wire (no field, because omitempty).
type Marker interface {
	MockMarker() string
}

// mockState is the lifecycle of a simulated guest.
type mockState string

const (
	mockRunning     mockState = "running"
	mockPaused      mockState = "paused"
	mockSnapshotted mockState = "snapshotted"
	mockReleased    mockState = "released"
)

// ErrMockNoExecution is returned by the one door that could be asked for by
// mistake. It is exported because a caller must be able to branch on it.
var ErrMockNoExecution = errors.New("mock backend executes nothing: " + MockMarker)

type mockBox struct {
	id        string
	node      string
	state     mockState
	snapshot  *SnapshotRef
	createdAt time.Time
}

// MockBackend is the in-process backend described above. It is safe for
// concurrent use; its only state is the table of simulated sandboxes.
type MockBackend struct {
	mu    sync.Mutex
	boxes map[string]*mockBox
	// now is injectable so a test can pin CreatedAt without sleeping.
	now func() time.Time
}

// NewMockBackend builds a mock backend. There is nothing to probe and nothing to
// configure: its determinism is the reason it exists.
func NewMockBackend() *MockBackend {
	return &MockBackend{boxes: map[string]*mockBox{}, now: time.Now}
}

// Name is "mock" -- the truthful label.
func (*MockBackend) Name() string { return MockBackendName }

// MockMarker implements Marker: the nature of this backend travels with it.
func (*MockBackend) MockMarker() string { return MockMarker }

// Describe is the operator-facing sentence (logs, --version style output).
func (*MockBackend) Describe() string {
	return "backend=mock marker=" + MockMarker
}

// Probe always succeeds. A backend that needs /dev/kvm answers Unavailable when
// the hypervisor is missing; this one has no dependency to be missing, and it
// says so in the same audit channel rather than pretending to be an X-class host.
func (*MockBackend) Probe(context.Context) error { return nil }

// audit writes the lifecycle record. The marker is a field, not an implication:
// anybody reading the audit later sees the row was produced by a simulation.
func (*MockBackend) audit(op, sandboxID string, state mockState, err error) {
	rec := slog.Info
	if err != nil {
		rec = slog.Warn
	}
	fields := []any{
		"op", op, "backend", MockBackendName, "sandbox_id", sandboxID,
		"state", string(state), "mock_marker", MockMarker,
	}
	if err != nil {
		fields = append(fields, "error", err.Error())
	}
	rec("compute: mock backend lifecycle", fields...)
}

// Acquire creates a simulated guest. Nothing is allocated on the host beyond a
// map entry: no process, no cgroup, no namespace, no file.
func (b *MockBackend) Acquire(_ context.Context, spec AcquireSpec) (Sandbox, error) {
	var rnd [6]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		b.audit("acquire", "", "", err)
		return nil, err
	}
	id := "mock-" + hex.EncodeToString(rnd[:])
	node := spec.Node
	if node == "" {
		node = "mock-local"
	}
	box := &mockBox{id: id, node: node, state: mockRunning, createdAt: b.now()}
	b.mu.Lock()
	b.boxes[id] = box
	b.mu.Unlock()
	b.audit("acquire", id, mockRunning, nil)
	return box, nil
}

// Release tears the simulated guest down for good.
func (b *MockBackend) Release(_ context.Context, sandboxID string) error {
	b.mu.Lock()
	box, ok := b.boxes[sandboxID]
	if !ok {
		b.mu.Unlock()
		err := errors.New("mock backend: unknown sandbox " + sandboxID)
		b.audit("release", sandboxID, "", err)
		return err
	}
	prev := box.state
	box.state = mockReleased
	delete(b.boxes, sandboxID)
	b.mu.Unlock()
	b.audit("release", sandboxID, prev, nil)
	return nil
}

func (b *MockBackend) stateOf(sandboxID string) (mockState, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	box, ok := b.boxes[sandboxID]
	if !ok {
		return "", errors.New("mock backend: unknown sandbox " + sandboxID)
	}
	return box.state, nil
}

func (b *MockBackend) transition(sandboxID string, from, to mockState, op string) error {
	b.mu.Lock()
	box, ok := b.boxes[sandboxID]
	if !ok {
		b.mu.Unlock()
		err := errors.New("mock backend: unknown sandbox " + sandboxID)
		b.audit(op, sandboxID, "", err)
		return err
	}
	if box.state != from {
		from_, to_ := box.state, to
		b.mu.Unlock()
		err := errors.New("mock backend: " + op + " requires state " + string(from) + ", sandbox is " + string(from_))
		b.audit(op, sandboxID, to_, err)
		return err
	}
	box.state = to
	b.mu.Unlock()
	b.audit(op, sandboxID, to, nil)
	return nil
}

// Pause freezes the simulated guest. Only a running guest can be frozen.
func (b *MockBackend) Pause(_ context.Context, sb Sandbox) error {
	return b.transition(sb.ID(), mockRunning, mockPaused, "pause")
}

// Resume wakes a frozen guest. Waking a guest that is running is refused rather
// than reported as success, so a scheduler can never count a second resume as a
// state change that did not happen.
func (b *MockBackend) Resume(_ context.Context, sb Sandbox) error {
	return b.transition(sb.ID(), mockPaused, mockRunning, "resume")
}

// Snapshot parks the guest: a running guest is paused first (the same rule the
// Firecracker backend enforces, because a snapshot of a running guest is not a
// consistent one), and the guest stays parked afterwards.
func (b *MockBackend) Snapshot(_ context.Context, sb Sandbox) (SnapshotRef, error) {
	if st, err := b.stateOf(sb.ID()); err != nil {
		b.audit("snapshot", sb.ID(), "", err)
		return SnapshotRef{}, err
	} else if st == mockRunning {
		if err := b.transition(sb.ID(), mockRunning, mockPaused, "pause"); err != nil {
			return SnapshotRef{}, err
		}
	}
	ref := SnapshotRef{
		SandboxID: sb.ID(), Node: sb.Node(), Dir: "/mock/" + sb.ID(),
		APISocket: "/mock/" + sb.ID() + "/api.socket",
		StatePath: "/mock/" + sb.ID() + "/vmstate", MemPath: "/mock/" + sb.ID() + "/mem",
		CreatedAt: b.now(),
	}
	b.mu.Lock()
	if box, ok := b.boxes[sb.ID()]; ok {
		box.state = mockSnapshotted
		box.snapshot = &ref
	}
	b.mu.Unlock()
	b.audit("snapshot", sb.ID(), mockSnapshotted, nil)
	return ref, nil
}

// RestoreSnapshot brings the SAME incarnation back. A reference that this backend
// did not park is refused: inventing a guest would be worse than an error.
func (b *MockBackend) RestoreSnapshot(_ context.Context, ref SnapshotRef) (Sandbox, error) {
	b.mu.Lock()
	box, ok := b.boxes[ref.SandboxID]
	switch {
	case !ok:
		b.mu.Unlock()
		err := errors.New("mock backend: no parked guest for sandbox " + ref.SandboxID)
		b.audit("restore", ref.SandboxID, "", err)
		return nil, err
	case box.snapshot == nil || box.snapshot.StatePath != ref.StatePath:
		box_ := box.state
		b.mu.Unlock()
		err := errors.New("mock backend: snapshot reference does not match the parked guest " + ref.SandboxID)
		b.audit("restore", ref.SandboxID, box_, err)
		return nil, err
	}
	box.state = mockRunning
	box.snapshot = nil
	b.mu.Unlock()
	b.audit("restore", ref.SandboxID, mockRunning, nil)
	return box, nil
}

// mockSandboxGuard makes the refusal explicit for a released handle.
func (*MockBackend) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	http.Error(w, MockMarker, http.StatusNotImplemented)
}

func (m *mockBox) ID() string      { return m.id }
func (m *mockBox) Backend() string { return MockBackendName }
func (m *mockBox) Node() string    { return m.node }

// Compile-time proof of what this backend is and is not. A reader who doubts the
// comment above can delete one of these lines and watch the build fail; the
// negative assertions live in backend_mock_test.go because Go has no
// "must not implement" directive.
var (
	_ Backend      = (*MockBackend)(nil)
	_ SleepResumer = (*MockBackend)(nil)
	_ Snapshotter  = (*MockBackend)(nil)
	_ Marker       = (*MockBackend)(nil)
)
