package tools

import "github.com/ahp-platform/ahp/sdk/go/internal/clienthttp"

// NewVSockInjectorClient connects to the guest's fixed host injector port.
func NewVSockInjectorClient(port uint32) (*Client, error) {
	wire, err := clienthttp.NewVSockInjector(port)
	if err != nil {
		return nil, err
	}
	return &Client{wire}, nil
}
