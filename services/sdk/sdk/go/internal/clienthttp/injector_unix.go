//go:build unix

package clienthttp

import (
	"context"
	"net"
	"path/filepath"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// NewUnixInjector uses the host's private socket.
func NewUnixInjector(socketPath string) (*Client, error) {
	if !filepath.IsAbs(socketPath) {
		return nil, contracts.NewError(contracts.ErrInvalid, "injector socket path must be absolute")
	}
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	return NewLocal(func(ctx context.Context) (net.Conn, error) {
		return dialer.DialContext(ctx, "unix", socketPath)
	}), nil
}
