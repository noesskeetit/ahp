//go:build !linux

package clienthttp

import "github.com/ahp-platform/ahp/pkg/contracts"

func NewVSockInjector(uint32) (*Client, error) {
	return nil, contracts.NewError(contracts.ErrUnavailable, "the injector vsock client requires Linux")
}
