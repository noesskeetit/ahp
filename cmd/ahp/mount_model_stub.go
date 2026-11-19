//go:build !ahp_exec

package main

import (
	"context"
	"fmt"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/transport"
	"net/http"
)

func (s *server) tryMountModelGateway(context.Context, *http.ServeMux, *auth.Verifier, transport.Authorize) error {
	return fmt.Errorf("model gateway requires the ahp_exec build tag")
}
