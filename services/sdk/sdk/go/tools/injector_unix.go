//go:build unix

package tools

import "github.com/ahp-platform/ahp/sdk/go/internal/clienthttp"

// NewUnixInjectorClient connects to the host injector without guest credentials.
func NewUnixInjectorClient(socketPath string) (*Client, error) {
	wire, err := clienthttp.NewUnixInjector(socketPath)
	if err != nil {
		return nil, err
	}
	return &Client{wire}, nil
}
