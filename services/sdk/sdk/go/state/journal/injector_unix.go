//go:build unix

package journal

import (
	"context"

	"github.com/ahp-platform/ahp/sdk/go/internal/clienthttp"
)

// NewUnixInjectorClient uses the host's private socket. Authentication to State
// API happens at the injector, with credentials outside the guest.
func NewUnixInjectorClient(socketPath string) (*Client, error) {
	wire, err := clienthttp.NewUnixInjector(socketPath)
	if err != nil {
		return nil, err
	}
	return &Client{http: wire}, nil
}

func ReadRuntimeBootstrap(ctx context.Context, socketPath string) (RuntimeBootstrap, error) {
	client, err := NewUnixInjectorClient(socketPath)
	if err != nil {
		return RuntimeBootstrap{}, err
	}
	return client.RuntimeBootstrap(ctx)
}
