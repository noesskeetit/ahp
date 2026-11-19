package integration_test

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// lockFileName serializes this package against internal/workspace tests, which
// share the same PG database and ahp_workspace schema. With per-run database
// isolation (isolation_test.go) they normally no longer contend at all; the
// lock stays because isolation is best-effort by design and a run that fell
// back to the configured database must still not interleave DDL with a
// neighbour.
const lockFileName = "ahp-vm2compute-ws.lock"

func TestMain(m *testing.M) {
	lockPath := filepath.Join(os.TempDir(), lockFileName)
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		// Cannot create lock file; proceed without serialization (best effort).
		code := m.Run()
		dropRunDatabase()
		os.Exit(code)
	}
	defer f.Close()
	deadline := time.Now().Add(120 * time.Second)
	for {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			// Timed out waiting; proceed to avoid hanging the suite.
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	code := m.Run()
	syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	// The per-run database is dropped after the last pool closed and after the
	// smoke service was terminated by its own t.Cleanup.
	dropRunDatabase()
	os.Exit(code)
}
