package compute

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// sleepStub is a VMRuntime that can also freeze a guest, used to prove the
// supervisor routes reach the runtime at all.
type sleepStub struct {
	name        string
	paused      []string
	resumed     []string
	snapshotted int
	live        Sandbox
}

func (f *sleepStub) Name() string                { return f.name }
func (f *sleepStub) Probe(context.Context) error { return nil }
func (f *sleepStub) Acquire(context.Context, AcquireSpec) (Sandbox, error) {
	return nil, Unavailable(f.name, "not needed by this test")
}
func (f *sleepStub) Release(context.Context, string) error { return nil }
func (f *sleepStub) Exec(context.Context, Sandbox, ExecSpec) (ExecResult, error) {
	return ExecResult{}, Unavailable(f.name, "not needed by this test")
}
func (f *sleepStub) OpenPTY(context.Context, Sandbox, PTYSpec) (PTYSession, error) {
	return nil, Unavailable(f.name, "not needed by this test")
}
func (f *sleepStub) Pause(_ context.Context, sb Sandbox) error {
	f.paused = append(f.paused, sb.ID())
	return nil
}
func (f *sleepStub) Resume(_ context.Context, sb Sandbox) error {
	f.resumed = append(f.resumed, sb.ID())
	return nil
}
func (f *sleepStub) Snapshot(_ context.Context, sb Sandbox) (SnapshotRef, error) {
	f.snapshotted++
	return SnapshotRef{SandboxID: sb.ID(), Node: sb.Node(), Dir: "/var/lib/ahp/vm/" + sb.ID(),
		APISocket: "/var/lib/ahp/vm/" + sb.ID() + "/firecracker.socket",
		StatePath: "/var/lib/ahp/vm/" + sb.ID() + "/vm-state.snapshot",
		MemPath:   "/var/lib/ahp/vm/" + sb.ID() + "/vm-memory.snapshot"}, nil
}

func (f *sleepStub) RestoreSnapshot(_ context.Context, ref SnapshotRef) (Sandbox, error) {
	return stubSandbox{id: ref.SandboxID, node: ref.Node}, nil
}

// noSleepStub is the same runtime WITHOUT the capability: what the supervisor must
// refuse rather than acknowledge.
type noSleepStub struct{ *sleepStub }

func (n *noSleepStub) Name() string { return "host-like" }

// the embedded stub would satisfy the capability by inheritance, so the wrapper
// that must refuse declares its own methods are unreachable: build it from a type
// that never had them.
type plainStub struct{}

func (plainStub) Name() string                { return "plain" }
func (plainStub) Probe(context.Context) error { return nil }
func (plainStub) Acquire(context.Context, AcquireSpec) (Sandbox, error) {
	return nil, Unavailable("plain", "stub")
}
func (plainStub) Release(context.Context, string) error { return nil }
func (plainStub) Exec(context.Context, Sandbox, ExecSpec) (ExecResult, error) {
	return ExecResult{}, Unavailable("plain", "stub")
}
func (plainStub) OpenPTY(context.Context, Sandbox, PTYSpec) (PTYSession, error) {
	return nil, Unavailable("plain", "stub")
}

func supervisorWith(t *testing.T, rt VMRuntime, live Sandbox) *Supervisor {
	t.Helper()
	sup, err := NewSupervisor(t.TempDir()+"/sup.sock", rt)
	if err != nil {
		t.Fatalf("NewSupervisor: %v", err)
	}
	if live != nil {
		sup.live[live.ID()] = live
	}
	return sup
}

func callSupervisor(t *testing.T, sup *Supervisor, route, sandboxID string) *httptest.ResponseRecorder {
	t.Helper()
	body, _ := json.Marshal(pauseRequest{SandboxID: sandboxID})
	req := httptest.NewRequest(http.MethodPost, route, strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	sup.Handler().ServeHTTP(rec, req)
	return rec
}

type stubSandbox struct{ id, node string }

func (s stubSandbox) ID() string      { return s.id }
func (s stubSandbox) Backend() string { return "stub" }
func (s stubSandbox) Node() string    { return s.node }

func TestSupervisorRoutesReachTheRuntime(t *testing.T) {
	stub := &sleepStub{name: "firecracker", live: stubSandbox{id: "vm-1", node: "node-a"}}
	sup := supervisorWith(t, stub, stub.live)

	for _, route := range []string{"/v1/pause", "/v1/resume", "/v1/snapshot"} {
		rec := callSupervisor(t, sup, route, "vm-1")
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: want 200, got %d %s", route, rec.Code, rec.Body.String())
		}
	}
	if len(stub.paused) != 1 || stub.paused[0] != "vm-1" {
		t.Errorf("pause must reach the runtime once for vm-1, got %v", stub.paused)
	}
	if len(stub.resumed) != 1 || stub.resumed[0] != "vm-1" {
		t.Errorf("resume must reach the runtime once for vm-1, got %v", stub.resumed)
	}
	if stub.snapshotted != 1 {
		t.Errorf("snapshot must reach the runtime once, got %d", stub.snapshotted)
	}
}

func TestSupervisorSnapshotReturnsTheReferenceNeededToWakeTheSameGuest(t *testing.T) {
	stub := &sleepStub{name: "firecracker"}
	sup := supervisorWith(t, stub, stubSandbox{id: "vm-9", node: "node-b"})
	rec := callSupervisor(t, sup, "/v1/snapshot", "vm-9")
	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d %s", rec.Code, rec.Body.String())
	}
	var out snapshotResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("snapshot must answer a reference the caller can store: %v", err)
	}
	if out.SandboxID != "vm-9" || out.Node != "node-b" || out.StatePath == "" || out.MemPath == "" || out.APISocket == "" {
		t.Fatalf("incomplete snapshot reference: %+v", out)
	}
}

func TestSupervisorRefusesSleepResumeOnARuntimeThatCannotFreezeAGuest(t *testing.T) {
	// A runtime without the capability must be refused by name, not answered 200:
	// reporting a sleep that never happened would be a false isolation fact.
	sup := supervisorWith(t, plainStub{}, stubSandbox{id: "vm-2", node: "node-c"})
	for _, route := range []string{"/v1/pause", "/v1/resume", "/v1/snapshot"} {
		rec := callSupervisor(t, sup, route, "vm-2")
		if rec.Code == http.StatusOK {
			t.Fatalf("%s: a backend that cannot freeze a guest must not answer 200", route)
		}
		if !strings.Contains(rec.Body.String(), "plain") {
			t.Errorf("%s: refusal must name the backend, got %s", route, rec.Body.String())
		}
	}
}

func TestSupervisorRefusesAnUnknownSandbox(t *testing.T) {
	stub := &sleepStub{name: "firecracker"}
	sup := supervisorWith(t, stub, nil)
	rec := callSupervisor(t, sup, "/v1/pause", "vm-nope")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("pausing a guest the supervisor does not own must be 404, got %d", rec.Code)
	}
	if len(stub.paused) != 0 {
		t.Errorf("a refused call must not reach the runtime, got %v", stub.paused)
	}
}
