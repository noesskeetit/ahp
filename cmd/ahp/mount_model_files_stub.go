//go:build ahp_exec && !ahp_data

package main

import (
	"context"
	"errors"
	"github.com/ahp-platform/ahp/internal/execbridge"
	"github.com/ahp-platform/ahp/internal/identity"
	"os"
)

func (s *server) configureModelFiles(_ context.Context, _ *execbridge.Bridge, _ *identity.Store) error {
	if os.Getenv("AHP_MODEL_FILES_CONFIG") != "" {
		return errors.New("model files: Artifact owner requires ahp_data in this topology")
	}
	return nil
}
