package compute

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

type releaseProbe struct {
	plainStub
	calls int
}

func (p *releaseProbe) ReleaseSandbox(context.Context, Sandbox) error {
	p.calls++
	if p.calls == 1 {
		return Unavailable("firecracker", "stop not confirmed")
	}
	return nil
}

func TestSupervisorReleaseKeepsHandleUntilStopConfirmed(t *testing.T) {
	p := &releaseProbe{}
	guest := stubSandbox{id: "vm-1"}
	supervisor := supervisorWith(t, p, guest)

	for i, want := range []int{http.StatusServiceUnavailable, http.StatusOK, http.StatusOK} {
		if got := callSupervisor(t, supervisor, "/v1/release", guest.ID()).Code; got != want {
			t.Fatalf("release %d returned %d, want %d", i+1, got, want)
		}
	}
	if p.calls != 2 {
		t.Fatalf("stop called %d times, want 2", p.calls)
	}
}

func TestFirecrackerReleaseRefusesUnknownLiveHandle(t *testing.T) {
	dir := t.TempDir()
	runtime := &FirecrackerRuntime{cfg: FirecrackerConfig{DataDir: dir}}
	supervisor := supervisorWith(t, runtime, nil)
	const id = "vm-1"
	if err := os.Mkdir(filepath.Join(dir, id), 0o750); err != nil {
		t.Fatal(err)
	}
	if got := callSupervisor(t, supervisor, "/v1/release", id).Code; got != http.StatusServiceUnavailable {
		t.Fatalf("release without the live handle returned %d, want 503", got)
	}
	if err := os.Remove(filepath.Join(dir, id)); err != nil {
		t.Fatal(err)
	}
	if got := callSupervisor(t, supervisor, "/v1/release", id).Code; got != http.StatusOK {
		t.Fatalf("release after teardown returned %d, want 200", got)
	}
}
