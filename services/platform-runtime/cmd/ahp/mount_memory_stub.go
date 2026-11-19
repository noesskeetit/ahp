//go:build !ahp_data

package main

import (
	"context"
	"net/http"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// Stubs for the default (untagged) build. internal/memory depends on
// internal/artifacts (and through it on minio-go) and on pgvector, so the
// memory mount only exists in a build that asked for the data plane tags. The
// stub keeps cmd/ahp compiling without them and reports the feature as not
// built, exactly like the data/execution/workspace mounts.
func (s *server) tryMountMemory(context.Context, *http.ServeMux,
	*auth.Verifier, transport.Authorize, string, GrantLabels) error {
	return errFeatureNotBuilt
}
