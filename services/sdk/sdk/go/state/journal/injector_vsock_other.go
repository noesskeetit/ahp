//go:build !linux

package journal

import "github.com/ahp-platform/ahp/pkg/contracts"

func NewVSockInjectorClient(uint32) (*Client, error) {
	return nil, contracts.NewError(contracts.ErrUnavailable, "the injector vsock client requires Linux")
}
