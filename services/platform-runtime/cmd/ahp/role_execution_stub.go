//go:build !ahp_exec

package main

import (
	"context"
	"net/http"
)

// Stubs for the default (untagged) build. They keep cmd/ahp compiling without
// the execution plane (internal/gateway, a2a-go) so the state / identity /
// model roles and the whole default build are never broken by the optional
// execution role. Rebuild with -tags ahp_exec to enable the real mount.
func (s *server) serveExecution(context.Context, string) error {
	return errFeatureNotBuilt
}

func (s *server) tryMountExecution(context.Context, *http.ServeMux) error {
	return errFeatureNotBuilt
}
