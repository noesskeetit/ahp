//go:build unix

package vmruntime

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// ProcessDriver executes a trusted integration worker in a distinct process
// group. The group and environment discipline are for lifecycle testing.
type ProcessDriver struct {
	root, binary string
	args         []string
	mu           sync.Mutex
	workers      map[string]*processGuest
}

type processGuest struct {
	process     *os.Process
	done        chan struct{}
	signalGroup func(int, syscall.Signal) error
	mu          sync.Mutex
	reaping     bool
	groupKilled bool
}

func NewProcessDriver(root, binary string, args ...string) (*ProcessDriver, error) {
	if !processWaitSupported {
		return nil, unavailable("trusted process driver requires a supported process exit observer")
	}
	if !filepath.IsAbs(root) || !filepath.IsAbs(binary) {
		return nil, contracts.NewError(contracts.ErrInvalid, "process driver requires absolute root and worker paths")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, err
	}
	info, err := os.Stat(binary)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return nil, contracts.NewError(contracts.ErrInvalid, "trusted worker binary is not executable")
	}
	return &ProcessDriver{root: root, binary: binary, args: append([]string(nil), args...),
		workers: make(map[string]*processGuest)}, nil
}

func (d *ProcessDriver) Start(ctx context.Context, guest Guest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !filepath.IsAbs(guest.InjectorSocket) {
		return contracts.NewError(contracts.ErrInvalid, "trusted worker requires an absolute injector socket path")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.workers[guest.ID()] != nil {
		return executionConflict()
	}
	dir := filepath.Join(d.root, guest.ID())
	if err := os.Mkdir(dir, 0o700); err != nil {
		return unavailable("cannot reserve trusted process attempt: " + err.Error())
	}
	// The directory is an uncertainty marker if a host crashes after spawn.
	// A fresh daemon cannot safely signal a PID whose identity it never held.
	parent, err := os.Open(d.root)
	if err == nil {
		err = parent.Sync()
		_ = parent.Close()
	}
	if err != nil {
		_ = os.Remove(dir)
		return unavailable("cannot persist trusted process attempt: " + err.Error())
	}
	cmd := exec.Command(d.binary, d.args...)
	cmd.Dir = dir
	cmd.Env = []string{
		"PATH=/usr/bin:/bin",
		"HOME=" + dir,
		"TMPDIR=" + dir,
		"AHP_INJECTOR_SOCKET=" + guest.InjectorSocket,
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		_ = os.Remove(dir)
		return unavailable("trusted integration worker could not start: " + err.Error())
	}
	p := &processGuest{process: cmd.Process, done: make(chan struct{}), signalGroup: syscall.Kill}
	d.workers[guest.ID()] = p
	go p.reap(cmd, guest.OnExit)
	return nil
}

func (p *processGuest) reap(cmd *exec.Cmd, onExit func()) {
	observed := waitProcessExit(p.process.Pid) == nil
	p.mu.Lock()
	if observed {
		// The kernel still reserves the leader's PID. Terminate any group
		// members before releasing that identity with Wait.
		_ = p.killGroupLocked()
	}
	p.reaping = true
	p.mu.Unlock()
	_ = cmd.Wait()
	if onExit != nil {
		onExit()
	}
	close(p.done)
}

func (p *processGuest) killGroupLocked() error {
	if p.reaping || p.groupKilled {
		return nil
	}
	if err := p.signalGroup(-p.process.Pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	p.groupKilled = true
	return nil
}

func (d *ProcessDriver) Alive(_ context.Context, guest Guest) (bool, error) {
	p, err := d.lookup(guest)
	if p == nil {
		return false, err
	}
	select {
	case <-p.done:
		return false, nil
	default:
		return true, nil
	}
}

func (d *ProcessDriver) StopAndWait(ctx context.Context, guest Guest) error {
	p, err := d.lookup(guest)
	if p == nil {
		return err
	}
	p.mu.Lock()
	err = p.killGroupLocked()
	p.mu.Unlock()
	if err != nil {
		return unavailable("cannot stop trusted worker's process group: " + err.Error())
	}
	select {
	case <-p.done:
	case <-ctx.Done():
		return unavailable("trusted worker exit not confirmed: " + ctx.Err().Error())
	}

	// Once Wait releases the PID the numeric group can no longer be
	// addressed safely. Only confirm its disappearance.

	timer := time.NewTicker(20 * time.Millisecond)
	defer timer.Stop()
	for {
		err := syscall.Kill(-p.process.Pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			break
		}
		if err != nil {
			return unavailable("cannot check the trusted worker group: " + err.Error())
		}
		select {
		case <-ctx.Done():
			return unavailable("trusted worker group exit not confirmed")
		case <-timer.C:
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := os.RemoveAll(filepath.Join(d.root, guest.ID())); err != nil {
		return unavailable("cannot retire trusted process attempt: " + err.Error())
	}
	delete(d.workers, guest.ID())
	return nil
}

func (d *ProcessDriver) lookup(guest Guest) (*processGuest, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if p := d.workers[guest.ID()]; p != nil {
		return p, nil
	}
	if _, err := os.Stat(filepath.Join(d.root, guest.ID())); errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return nil, unavailable("trusted process attempt has no confirmed local owner")
}
