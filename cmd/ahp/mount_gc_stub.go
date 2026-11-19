//go:build !ahp_data

package main

import "context"

// Stub for the default (untagged) build. The background GC scheduler needs
// internal/artifacts and minio-go, which are compiled out unless the binary is
// built with -tags ahp_data. Rebuild with that tag to enable the real mount;
// here enabling AHP_ENABLE_GC_SCHEDULER reports the feature is not built, and
// the scheduler hook on the server (gcDrainOnShutdown) is never set, so shutdown
// stays exactly as it is for every other role.
func (s *server) tryMountGCScheduler(context.Context) error {
	return errFeatureNotBuilt
}
