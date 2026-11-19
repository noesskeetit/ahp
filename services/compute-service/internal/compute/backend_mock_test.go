package compute

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func captureMockAudit(t *testing.T) *strings.Builder {
	t.Helper()
	var buf strings.Builder
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	return &buf
}

// The state machine is the whole product of this backend, so it is pinned
// transition by transition rather than sampled.
func TestMockStateMachineTransitions(t *testing.T) {
	b := NewMockBackend()
	ctx := context.Background()
	sb, err := b.Acquire(ctx, AcquireSpec{ComputerID: "c1", Incarnation: 1})
	if err != nil {
		t.Fatal(err)
	}
	if sb.Backend() != MockBackendName {
		t.Fatalf("a mock sandbox must say mock, got %q", sb.Backend())
	}
	if st, err := b.stateOf(sb.ID()); err != nil || st != mockRunning {
		t.Fatalf("fresh sandbox state = %q (%v), want running", st, err)
	}
	if err := b.Pause(ctx, sb); err != nil {
		t.Fatalf("pause of a running guest: %v", err)
	}
	if err := b.Pause(ctx, sb); err == nil {
		t.Fatal("pausing an already-paused guest must be refused, not reported as success")
	}
	if err := b.Resume(ctx, sb); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if err := b.Resume(ctx, sb); err == nil {
		t.Fatal("resuming a running guest must be refused")
	}
	ref, err := b.Snapshot(ctx, sb)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if st, _ := b.stateOf(sb.ID()); st != mockSnapshotted {
		t.Fatalf("snapshot must leave the guest parked, state = %q", st)
	}
	if ref.SandboxID != sb.ID() || ref.StatePath == "" || ref.MemPath == "" {
		t.Fatalf("snapshot reference must name the same incarnation and its two files: %+v", ref)
	}
	restored, err := b.RestoreSnapshot(ctx, ref)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if restored.ID() != sb.ID() {
		t.Fatalf("restore must return the parked incarnation, got %q", restored.ID())
	}
	if st, _ := b.stateOf(sb.ID()); st != mockRunning {
		t.Fatalf("restored guest must run, state = %q", st)
	}
}

// Every refusal path that could turn into a false accounting fact.
func TestMockRefusesUnknownMismatchedAndReleased(t *testing.T) {
	b := NewMockBackend()
	ctx := context.Background()
	sb, err := b.Acquire(ctx, AcquireSpec{ComputerID: "c1", Incarnation: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Release(ctx, sb.ID()); err != nil {
		t.Fatal(err)
	}
	if err := b.Pause(ctx, sb); err == nil {
		t.Fatal("pausing a released sandbox must fail")
	}
	if err := b.Release(ctx, sb.ID()); err == nil {
		t.Fatal("releasing twice must fail rather than report a second teardown")
	}
	if _, err := b.RestoreSnapshot(ctx, SnapshotRef{SandboxID: "mock-never-existed"}); err == nil {
		t.Fatal("restore of an unknown guest must fail, never boot a replacement")
	}
	good, err := b.Acquire(ctx, AcquireSpec{ComputerID: "c2", Incarnation: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Snapshot(ctx, good); err != nil {
		t.Fatal(err)
	}
	if _, err := b.RestoreSnapshot(ctx, SnapshotRef{SandboxID: good.ID(), StatePath: "/some/other/guest/vmstate"}); err == nil {
		t.Fatal("a mismatched snapshot reference must be refused")
	}
}

// The disclosure is the acceptance criterion: a caller cannot mistake this for a
// VM by reading the name, the description, or the audit trail.
func TestMockMarkerIsPresentInNameDescriptionAndAudit(t *testing.T) {
	buf := captureMockAudit(t)
	b := NewMockBackend()
	if b.Name() != "mock" {
		t.Fatalf("Name() = %q, want mock", b.Name())
	}
	if b.MockMarker() != MockMarker {
		t.Fatal("Marker must return the single disclosure constant")
	}
	for _, s := range []string{b.Describe(), b.MockMarker()} {
		if !strings.Contains(s, "no isolation") || !strings.Contains(s, "never executed") {
			t.Fatalf("description must state what this backend is not, got %q", s)
		}
	}
	ctx := context.Background()
	sb, err := b.Acquire(ctx, AcquireSpec{ComputerID: "c1", Incarnation: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Snapshot(ctx, sb); err != nil {
		t.Fatal(err)
	}
	if err := b.Release(ctx, sb.ID()); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, op := range []string{"acquire", "pause", "snapshot", "release"} {
		if !strings.Contains(out, "op="+op) {
			t.Fatalf("audit is missing the %q lifecycle row: %s", op, out)
		}
	}
	if got := strings.Count(out, "mock_marker="); got < 4 {
		t.Fatalf("every audit row must carry the mock marker, saw %d of >=4: %s", got, out)
	}
	if !strings.Contains(out, "backend=mock") {
		t.Fatal("audit must name the mock backend")
	}
}

// The safety claim is testable and narrow: this backend has no door through which
// untrusted bytes could reach a host process. Anything beyond this sentence --
// isolation, escape resistance, latency, density -- is NOT claimed and NOT
// tested here, because nothing in this file could support such a claim.
func TestMockBackendHasNoExecutionOrPTYDoor(t *testing.T) {
	b := NewMockBackend()
	if _, ok := interface{}(b).(CommandBackend); ok {
		t.Fatal("mock backend must not implement CommandBackend: it would execute untrusted code on the host")
	}
	if _, ok := interface{}(b).(PTYBackend); ok {
		t.Fatal("mock backend must not implement PTYBackend: an interactive shell is host execution")
	}
	if !strings.Contains(ErrMockNoExecution.Error(), MockMarker) {
		t.Fatal("the refusal for an execution request must carry the marker")
	}
}

// A backend whose Probe can fail is a backend that can be silently skipped. The
// mock is deterministic on purpose; that property is pinned too.
func TestMockProbeIsDeterministicAndDescribesItself(t *testing.T) {
	b := NewMockBackend()
	for i := 0; i < 3; i++ {
		if err := b.Probe(context.Background()); err != nil {
			t.Fatalf("Probe must not fail for a backend with no dependencies: %v", err)
		}
	}
	if !strings.Contains(b.Describe(), "backend=mock") {
		t.Fatal("Describe must be greppable by an operator")
	}
}

// mockRuntime is what the supervisor actually consumes: the mock backend plus the
// two doors the mock refuses to open. They refuse with the marker, so even a
// mis-wired caller is told what it is talking to.
type mockRuntime struct{ *MockBackend }

func (mockRuntime) Exec(context.Context, Sandbox, ExecSpec) (ExecResult, error) {
	return ExecResult{}, ErrMockNoExecution
}
func (mockRuntime) OpenPTY(context.Context, Sandbox, PTYSpec) (PTYSession, error) {
	return nil, ErrMockNoExecution
}

// The API-level half of the disclosure: a client that acquires from a mock
// supervisor sees the marker in the answer, and the refusal to execute is the
// mock's own error, not a host process starting.
func TestSupervisorAcquireAnswerCarriesTheMockMarker(t *testing.T) {
	sup := supervisorWith(t, &mockRuntime{NewMockBackend()}, nil)
	body, err := json.Marshal(acquireRequest{ComputerID: "c1", Incarnation: 1})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/acquire", strings.NewReader(string(body)))
	rec := httptest.NewRecorder()
	sup.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("acquire code = %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "mock_marker") {
		t.Fatalf("acquire answer must carry the marker, got %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "no isolation") {
		t.Fatalf("the marker text must travel verbatim, got %s", rec.Body.String())
	}
	var out acquireResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if _, err := sup.rt.Exec(context.Background(), mustAcquire(t, sup, out.SandboxID), ExecSpec{Command: "id"}); err == nil {
		t.Fatal("the mock supervisor must refuse execution, not run 'id' somewhere")
	} else if !strings.Contains(err.Error(), "MOCK-COMPUTE-BACKEND") {
		t.Fatalf("the refusal must name the mock, got %v", err)
	}
}

func mustAcquire(t *testing.T, sup *Supervisor, id string) Sandbox {
	t.Helper()
	sb, ok := sup.sandbox(id)
	if !ok {
		t.Fatalf("acquired sandbox %q is not live", id)
	}
	return sb
}
