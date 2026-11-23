//go:build unix

package vmruntime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestTrustedProcessWorker(t *testing.T) {
	if os.Getenv("AHP_INJECTOR_SOCKET") == "" {
		return
	}
	values, err := json.Marshal(os.Environ())
	if err != nil || os.WriteFile("guest-env.json", values, 0o600) != nil {
		os.Exit(3)
	}
	for {
		time.Sleep(time.Second)
	}
}

func TestProcessDriverStartsCredentialFreeWorkerAndWaitsForStop(t *testing.T) {
	t.Setenv("AHP_TEST_PRIVATE_CREDENTIAL", "ambient-fixture-should-not-reach-child")
	root := t.TempDir()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	driver, err := NewProcessDriver(root, binary, "-test.run=^TestTrustedProcessWorker$")
	if err != nil {
		t.Fatal(err)
	}
	guest := Guest{Execution: execution(), Attempt: 1, InjectorSocket: filepath.Join(root, "fixture.sock")}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := driver.Start(ctx, guest); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stop, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := driver.StopAndWait(stop, guest); err != nil {
			t.Errorf("stop process: %v", err)
		}
	})

	name := filepath.Join(root, guest.ID(), "guest-env.json")
	var values []string
	for {
		data, err := os.ReadFile(name)
		if err == nil {
			if err := json.Unmarshal(data, &values); err == nil {
				break
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("worker did not write environment")
		case <-time.After(10 * time.Millisecond):
		}
	}
	found := make([]string, 0, len(values))
	for _, value := range values {
		key, _, _ := strings.Cut(value, "=")
		found = append(found, key)
	}
	sort.Strings(found)
	if strings.Join(found, ",") != "AHP_INJECTOR_SOCKET,HOME,PATH,TMPDIR" {
		t.Fatalf("unexpected guest environment keys %v", found)
	}
	if err := driver.workers[guest.ID()].process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	if err := driver.StopAndWait(ctx, guest); err != nil {
		t.Fatal(err)
	}
	if alive, err := driver.Alive(ctx, guest); err != nil || alive {
		t.Fatalf("process still alive after confirmed stop: %v, %v", alive, err)
	}
}

func TestProcessDriverDoesNotSignalAfterReapingLeader(t *testing.T) {
	root := t.TempDir()
	driver, err := NewProcessDriver(root, "/bin/sh", "-c", "exit 0")
	if err != nil {
		t.Fatal(err)
	}
	callbackStarted, releaseCallback := make(chan struct{}), make(chan struct{})
	guest := Guest{Execution: execution(), Attempt: 1, InjectorSocket: filepath.Join(root, "fixture.sock")}
	guest.OnExit = func() {
		close(callbackStarted)
		<-releaseCallback
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := driver.Start(ctx, guest); err != nil {
		t.Fatal(err)
	}
	defer func() {
		close(releaseCallback)
		if err := driver.StopAndWait(ctx, guest); err != nil {
			t.Errorf("stop after callback exits: %v", err)
		}
	}()
	select {
	case <-callbackStarted:
	case <-ctx.Done():
		t.Fatal("reaped worker did not run its exit callback")
	}

	signals := 0
	driver.workers[guest.ID()].signalGroup = func(int, syscall.Signal) error {
		signals++
		return nil
	}
	stop, stopCancel := context.WithCancel(ctx)
	stopCancel()
	if err := driver.StopAndWait(stop, guest); err == nil {
		t.Fatal("stop confirmed while the old attempt's exit callback is still running")
	}
	if signals != 0 {
		t.Fatalf("attempted to signal a numeric PGID after reaping its leader (%d calls)", signals)
	}
}
