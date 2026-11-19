//go:build !ahp_data

package main

import (
	"context"
	"net/http"
)

// Stubs for the default (untagged) build. They keep cmd/ahp compiling without
// the data plane (internal/artifacts, minio-go) so the state / identity / model
// roles and the whole default build are never broken by the optional data
// role. Rebuild with -tags ahp_data to enable the real mount.
func (s *server) serveData(context.Context, string) error {
	return errFeatureNotBuilt
}

func (s *server) tryMountData(context.Context, *http.ServeMux) error {
	return errFeatureNotBuilt
}
