package journal

import "github.com/ahp-platform/ahp/sdk/go/internal/clienthttp"

// NewVSockInjectorClient connects to the mount's host CID over virtio-vsock.
func NewVSockInjectorClient(port uint32) (*Client, error) {
	wire, err := clienthttp.NewVSockInjector(port)
	if err != nil {
		return nil, err
	}
	return &Client{http: wire}, nil
}
