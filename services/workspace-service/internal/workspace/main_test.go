package workspace

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// lockFile serializes this package against internal/compute/integration tests,
// which share the same PG database and ahp_workspace schema.
const lockFileName = "ahp-vm2compute-ws.lock"

func TestMain(m *testing.M) {
	lockPath := filepath.Join(os.TempDir(), lockFileName)
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		os.Exit(m.Run())
	}
	defer f.Close()
	deadline := time.Now().Add(120 * time.Second)
	for {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	code := m.Run()
	syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	os.Exit(code)
}
