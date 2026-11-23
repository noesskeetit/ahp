//go:build unix

package vmruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	"golang.org/x/sys/unix"
)

// Driver addresses attempts using an identity known before Start. Stop must
// find an attempt even if Start was interrupted before returning its result.
type Driver interface {
	Start(ctx context.Context, attempt Guest) error
	Alive(ctx context.Context, attempt Guest) (bool, error)
	StopAndWait(ctx context.Context, attempt Guest) error
}

type Guest struct {
	Execution Execution
	Attempt   int64
}

// ID is suitable for a local resource name and includes the attempt. A stop
// for an old attempt therefore cannot address a newer guest.
func (g Guest) ID() string {
	return fmt.Sprintf("exec-%s-%d", slotKey(g.Execution)[:24], g.Attempt)
}

type Host struct {
	ID     string
	Dir    string
	Driver Driver
}

type slot struct {
	Fenced  bool  `json:"fenced"`
	Attempt int64 `json:"attempt"`
	Ready   bool  `json:"ready"`
}

func (h Host) StartOrAdopt(ctx context.Context, exec Execution, attempt int64) error {
	return h.withSlot(ctx, exec, func(path string, s slot) error {
		if s.Fenced || attempt < 1 || attempt < s.Attempt {
			return contracts.NewError(contracts.ErrConflict, "execution attempt is fenced or superseded")
		}
		if s.Attempt > 0 {
			previous := Guest{Execution: exec, Attempt: s.Attempt}
			if attempt == s.Attempt && s.Ready {
				alive, err := h.Driver.Alive(ctx, previous)
				if err != nil || alive {
					return err
				}
			}
			// A process may exist even if its Start never returned. Clean up
			// the recorded attempt before calling Start for any replacement.
			if err := h.Driver.StopAndWait(ctx, previous); err != nil {
				return err
			}
		}
		if err := persistSlot(path, slot{Attempt: attempt}); err != nil {
			return err
		}
		if err := h.Driver.Start(ctx, Guest{Execution: exec, Attempt: attempt}); err != nil {
			return err
		}
		return persistSlot(path, slot{Attempt: attempt, Ready: true})
	})
}

func (h Host) Running(ctx context.Context, exec Execution, attempt int64) (alive bool, err error) {
	err = h.withSlot(ctx, exec, func(_ string, s slot) error {
		if s.Fenced || !s.Ready || s.Attempt != attempt {
			return nil
		}
		var err error
		alive, err = h.Driver.Alive(ctx, Guest{Execution: exec, Attempt: attempt})
		return err
	})
	return alive, err
}

func (h Host) FenceAndWait(ctx context.Context, exec Execution) error {
	return h.withSlot(ctx, exec, func(path string, s slot) error {
		s.Fenced = true
		if err := persistSlot(path, s); err != nil {
			return err
		}
		if s.Attempt == 0 {
			return nil
		}
		if err := h.Driver.StopAndWait(ctx, Guest{Execution: exec, Attempt: s.Attempt}); err != nil {
			return err
		}
		return persistSlot(path, slot{Fenced: true})
	})
}

func (h Host) withSlot(ctx context.Context, exec Execution, fn func(string, slot) error) error {
	if h.ID == "" || !filepath.IsAbs(h.Dir) || h.Driver == nil {
		return unavailable("execution host is not configured")
	}
	if err := exec.Scope.Validate(); err != nil {
		return err
	}
	if !journal.ValidExecutionID(exec.ID) {
		return contracts.NewError(contracts.ErrInvalid, "invalid execution id")
	}
	base := filepath.Join(h.Dir, slotKey(exec))
	lock, err := os.OpenFile(base+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return unavailable("cannot open execution slot: " + err.Error())
	}
	defer lock.Close()
	for {
		if err := ctx.Err(); err != nil {
			return unavailable("execution slot unavailable: " + err.Error())
		}
		err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			break
		}
		if !errors.Is(err, unix.EWOULDBLOCK) {
			return unavailable("cannot lock execution slot: " + err.Error())
		}
		select {
		case <-ctx.Done():
			return unavailable("execution slot unavailable: " + ctx.Err().Error())
		case <-time.After(10 * time.Millisecond):
		}
	}
	path := base + ".json"
	var s slot
	raw, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return unavailable("cannot read execution slot: " + err.Error())
	}
	if err == nil {
		if err := json.Unmarshal(raw, &s); err != nil || s.Attempt < 0 || s.Attempt == 0 && (!s.Fenced || s.Ready) {
			return unavailable("invalid execution slot record")
		}
	}
	return fn(path, s)
}

func slotKey(exec Execution) string {
	sum := sha256.Sum256([]byte(exec.Scope.Tenant + "\x00" + exec.Scope.Deployment + "\x00" + exec.Scope.Subject + "\x00" + exec.ID))
	return hex.EncodeToString(sum[:])
}

func persistSlot(path string, state slot) error {
	if err := writeSlot(path, state); err != nil {
		return unavailable("cannot persist execution slot: " + err.Error())
	}
	return nil
}

func writeSlot(path string, state slot) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".slot-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if err := json.NewEncoder(f).Encode(state); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(f.Name(), path); err != nil {
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
