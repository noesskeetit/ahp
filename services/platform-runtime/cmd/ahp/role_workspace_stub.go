//go:build !ahp_data

package main

import (
	"context"
	"net/http"
)

// Stubs for the default (untagged) build: they keep cmd/ahp compiling without
// internal/workspace, internal/artifacts and minio-go, so the state / identity /
// model roles and the whole default build are never broken by the optional
// Workspace face. Rebuild with -tags ahp_data to enable the real mount.
func (s *server) tryMountWorkspace(context.Context, *http.ServeMux) error {
	return errFeatureNotBuilt
}
