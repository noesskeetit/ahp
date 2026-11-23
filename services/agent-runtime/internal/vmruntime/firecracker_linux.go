//go:build linux

package vmruntime

import (
	"context"

	"github.com/ahp-platform/ahp/internal/compute"
)

func FirecrackerDriver(root string) (Driver, error) {
	runtime, err := compute.RuntimeFromEnv()
	if err != nil {
		return nil, err
	}
	fc, ok := runtime.(*compute.FirecrackerRuntime)
	if !ok {
		return nil, unavailable("managed execution requires Firecracker")
	}
	return firecrackerDriver{&compute.ManagedFirecracker{Runtime: fc, Root: root}}, nil
}

type firecrackerDriver struct{ vm *compute.ManagedFirecracker }

func (d firecrackerDriver) Start(ctx context.Context, g Guest) error {
	return d.vm.Start(ctx, g.ID())
}

func (d firecrackerDriver) Alive(ctx context.Context, g Guest) (bool, error) {
	return d.vm.Alive(ctx, g.ID())
}

func (d firecrackerDriver) StopAndWait(ctx context.Context, g Guest) error {
	return d.vm.StopAndWait(ctx, g.ID())
}
