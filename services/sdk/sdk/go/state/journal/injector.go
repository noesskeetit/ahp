package journal

import (
	"context"
	"net/http"

	"github.com/ahp-platform/ahp/pkg/contracts/journal"
)

type RuntimeBootstrap = journal.RuntimeBootstrap

const RuntimeInjectorVSockPort = journal.RuntimeInjectorVSockPort

// RuntimeBootstrap reads the execution assignment from a local injector.
func (c *Client) RuntimeBootstrap(ctx context.Context) (RuntimeBootstrap, error) {
	var out RuntimeBootstrap
	err := c.http.CallMethod(ctx, http.MethodGet, "/v1/runtime/bootstrap", nil, &out, false)
	return out, err
}
