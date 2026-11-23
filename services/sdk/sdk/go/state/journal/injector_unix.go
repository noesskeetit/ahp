//go:build unix

package journal

import (
	"context"
	"net"
	"path/filepath"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/sdk/go/internal/clienthttp"
)

// NewUnixInjectorClient uses the host's private socket. Authentication to State
// API happens at the injector, with credentials outside the guest.
func NewUnixInjectorClient(socketPath string) (*Client, error) {
	if !filepath.IsAbs(socketPath) {
		return nil, contracts.NewError(contracts.ErrInvalid, "injector socket path must be absolute")
	}
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	return &Client{http: clienthttp.NewLocal(func(ctx context.Context) (net.Conn, error) {
		return dialer.DialContext(ctx, "unix", socketPath)
	})}, nil
}

func ReadRuntimeBootstrap(ctx context.Context, socketPath string) (RuntimeBootstrap, error) {
	client, err := NewUnixInjectorClient(socketPath)
	if err != nil {
		return RuntimeBootstrap{}, err
	}
	return client.RuntimeBootstrap(ctx)
}
