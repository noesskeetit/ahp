//go:build linux

package compute

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// ManagedFirecracker owns an attempt by its reserved cgroup. The kernel places
// the child in the cgroup at fork, so a restart between spawn and persisting
// RUNNING cannot make the hypervisor undiscoverable.
type ManagedFirecracker struct {
	Runtime *FirecrackerRuntime
	Root    string
	guests  sync.Map // attempt ID -> managedVM, populated only for children of this process
}

type managedVM struct {
	sandbox   *fcSandbox
	afterExit chan struct{}
}

var managedVMID = regexp.MustCompile(`^exec-[a-f0-9]{24}-[1-9][0-9]*$`)

func (m *ManagedFirecracker) InjectorSocket(id string, port uint32) (string, error) {
	if m.Runtime == nil || !managedVMID.MatchString(id) || port == 0 || port >= 1<<30 {
		return "", invalid("invalid managed VM injector address")
	}
	return filepath.Join(m.Runtime.cfg.DataDir, id, fmt.Sprintf("vsock.sock_%d", port)), nil
}

func (m *ManagedFirecracker) Start(ctx context.Context, id string) error {
	return m.start(ctx, id, "", nil)
}

func (m *ManagedFirecracker) StartWithInjector(ctx context.Context, id string, port uint32, socket string, onExit func()) error {
	expected, err := m.InjectorSocket(id, port)
	if err != nil {
		return err
	}
	st, err := os.Lstat(socket)
	if socket != expected || err != nil || st.Mode().Type() != os.ModeSocket {
		return Unavailable("firecracker", "managed attempt has no verified host injector socket")
	}
	return m.start(ctx, id, filepath.Join(filepath.Dir(socket), "vsock.sock"), onExit)
}

func (m *ManagedFirecracker) start(ctx context.Context, id string, vsockPath string, onExit func()) error {
	path, err := m.group(id)
	if err != nil {
		return err
	}
	if err := os.Mkdir(path, 0o750); err != nil {
		return Unavailable("firecracker", "cannot create a new execution cgroup: "+err.Error())
	}
	group, err := os.Open(path)
	if err != nil {
		return Unavailable("firecracker", "cannot open execution cgroup: "+err.Error())
	}
	defer group.Close()
	if err := unix.Access(filepath.Join(path, "cgroup.kill"), unix.W_OK); err != nil {
		return Unavailable("firecracker", "cannot guarantee stop for this cgroup: "+err.Error())
	}
	guest, err := m.Runtime.acquire(ctx, AcquireSpec{Class: ClassX}, id,
		&syscall.SysProcAttr{UseCgroupFD: true, CgroupFD: int(group.Fd())}, vsockPath)
	if err != nil {
		return err
	}
	vm := &managedVM{sandbox: guest.(*fcSandbox), afterExit: make(chan struct{})}
	m.guests.Store(id, vm)
	if onExit == nil {
		close(vm.afterExit)
	} else {
		go func() {
			<-vm.sandbox.done
			onExit()
			close(vm.afterExit)
		}()
	}
	return nil
}

func (m *ManagedFirecracker) Alive(_ context.Context, id string) (bool, error) {
	path, err := m.group(id)
	if err != nil {
		return false, err
	}
	return executionCgroupPopulated(path)
}

func (m *ManagedFirecracker) StopAndWait(ctx context.Context, id string) error {
	path, err := m.group(id)
	if err != nil {
		return err
	}
	alive, err := executionCgroupPopulated(path)
	if err != nil {
		return err
	}
	if alive {
		if err := os.WriteFile(filepath.Join(path, "cgroup.kill"), []byte("1"), 0o200); err != nil {
			return Unavailable("firecracker", "cannot kill execution cgroup: "+err.Error())
		}
	}
	for alive {
		select {
		case <-ctx.Done():
			return Unavailable("firecracker", "execution cgroup is still populated")
		case <-time.After(20 * time.Millisecond):
		}
		alive, err = executionCgroupPopulated(path)
		if err != nil {
			return err
		}
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return Unavailable("firecracker", "cannot remove empty execution cgroup: "+err.Error())
	}
	if guest, ok := m.guests.Load(id); ok {
		vm := guest.(*managedVM)
		if err := m.Runtime.ReleaseSandbox(ctx, vm.sandbox); err != nil {
			return err
		}
		select {
		case <-vm.afterExit:
			m.guests.Delete(id)
			return nil
		case <-ctx.Done():
			return Unavailable("firecracker", "guest stopped but injector closure is unconfirmed")
		}
	}
	if err := os.RemoveAll(filepath.Join(m.Runtime.cfg.DataDir, id)); err != nil {
		return Unavailable("firecracker", "cannot clean execution files: "+err.Error())
	}
	return nil
}

func (m *ManagedFirecracker) group(id string) (string, error) {
	if !managedVMID.MatchString(id) {
		return "", invalid("invalid managed VM handle")
	}
	if m.Runtime == nil || !filepath.IsAbs(m.Root) {
		return "", Unavailable("firecracker", "managed cgroup root is not configured")
	}
	var root unix.Statfs_t
	if err := unix.Statfs(m.Root, &root); err != nil || root.Type != unix.CGROUP2_SUPER_MAGIC {
		return "", Unavailable("firecracker", "managed root is not on an available cgroup v2 filesystem")
	}
	return filepath.Join(m.Root, id), nil
}

func executionCgroupPopulated(path string) (bool, error) {
	data, err := os.ReadFile(filepath.Join(path, "cgroup.events"))
	if errors.Is(err, os.ErrNotExist) {
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
	}
	if err != nil {
		return false, Unavailable("firecracker", "cannot read execution cgroup state: "+err.Error())
	}
	for _, line := range strings.Split(string(data), "\n") {
		switch line {
		case "populated 0":
			return false, nil
		case "populated 1":
			return true, nil
		}
	}
	return false, Unavailable("firecracker", "execution cgroup returned invalid population status")
}
