//go:build linux

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Build the actual parser with a scheduling probe between PR_SET_NO_NEW_PRIVS
// and seccomp. The probe parks a goroutine on the available OS thread. Without
// thread affinity, that is the thread carrying no_new_privs, and the installer
// resumes on a different thread with no_new_privs=0 (seccomp returns EACCES).
// The overlay changes neither the production API nor the filter/rlimits.
func TestSandboxThreadAffinityDuringScheduling(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	sourcePath := filepath.Join(dir, "sandbox_linux.go")
	source, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	const marker = "\tvar arch uint32\n"
	if strings.Count(string(source), marker) != 1 {
		t.Fatal("sandbox scheduling probe insertion point changed")
	}
	const probe = `
	setTID := unix.Gettid()
	ready := make(chan int)
	release := make(chan struct{})
	defer close(release)
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		ready <- unix.Gettid()
		<-release
	}()
	parkedTID := <-ready
	runtime.Gosched()
	filterTID := unix.Gettid()
	nnp, probeErr := unix.PrctlRetInt(unix.PR_GET_NO_NEW_PRIVS, 0, 0, 0, 0)
	if probeErr != nil {
		return probeErr
	}
	fmt.Fprintf(os.Stderr, "thread-affinity set=%d parked=%d filter=%d nnp=%d\n", setTID, parkedTID, filterTID, nnp)
`
	modified := strings.Replace(string(source), marker, probe+marker, 1)
	modified = strings.Replace(modified, "import (", "import (\n\t\"os\"", 1)
	tmp := t.TempDir()
	replacement := filepath.Join(tmp, "sandbox_linux.go")
	if err := os.WriteFile(replacement, []byte(modified), 0600); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(map[string]any{"Replace": map[string]string{sourcePath: replacement}})
	if err != nil {
		t.Fatal(err)
	}
	overlay := filepath.Join(tmp, "overlay.json")
	if err := os.WriteFile(overlay, data, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	binary := filepath.Join(tmp, "artifact-parser")
	build := exec.CommandContext(ctx, "go", "build", "-overlay", overlay, "-o", binary, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build scheduling probe: %v: %s", err, out)
	}
	for i := 0; i < 3; i++ {
		body := []byte("bounded UTF-8 привет\n")
		cmd := exec.CommandContext(ctx, binary, "text/plain")
		cmd.Env = []string{"GOMAXPROCS=1", "GOMEMLIMIT=64MiB", "GODEBUG=netdns=go"}
		cmd.Stdin = bytes.NewReader(body)
		var out, diagnostic bytes.Buffer
		cmd.Stdout, cmd.Stderr = &out, &diagnostic
		if err := cmd.Run(); err != nil {
			t.Fatalf("forced scheduling parser: %v; %s", err, diagnostic.String())
		}
		var setTID, parkedTID, filterTID, nnp int
		if n, err := fmt.Sscanf(diagnostic.String(), "thread-affinity set=%d parked=%d filter=%d nnp=%d", &setTID, &parkedTID, &filterTID, &nnp); err != nil || n != 4 {
			t.Fatalf("missing scheduling proof: %q: %v", diagnostic.String(), err)
		}
		if setTID != filterTID || parkedTID == filterTID || nnp != 1 || !bytes.Equal(out.Bytes(), body) {
			t.Fatalf("thread-affinity violated: %s output_matches=%v", diagnostic.String(), bytes.Equal(out.Bytes(), body))
		}
		t.Log(strings.TrimSpace(diagnostic.String()))
	}
}
