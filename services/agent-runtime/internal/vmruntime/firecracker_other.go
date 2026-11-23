//go:build unix && !linux

package vmruntime

func FirecrackerDriver(string) (Driver, error) {
	return nil, unavailable("managed Firecracker execution requires Linux with KVM")
}
