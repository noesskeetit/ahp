package main

// Composition root: the execution plane's view of the Registry's artifact record.
//
// A task started by /v1/registry/Invoke names the artifact it was invoked as, and
// the plane that runs it has to check that name against what is installed before
// anything runs (internal/gateway/invocation.go). The Registry lives in this
// process's own Postgres (the canonical stand is one database, many schemas), so
// the check is a read through the same pool -- no second service, no second
// credential, and the execution plane still does not import registry types: this
// adapter is the only place the two shapes meet.
//
// The reader performs no DDL. Where the registry plane is not mounted in this
// process the read fails and an invocation claim is refused rather than trusted;
// traffic that names no artifact is unaffected.

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/registry"
)

// artifactVerifier is the composition-root answer to "what does this tenant have
// installed", built over the process pool without importing the registry into the
// execution role's file.
func artifactVerifier(pool *pgxpool.Pool) gateway.InstalledArtifactReader {
	return registryArtifactReader{store: registry.New(pool)}
}

type registryArtifactReader struct {
	store *registry.Store
}

func (r registryArtifactReader) InstallationArtifact(ctx context.Context, tenant, installationID string) (string, string, error) {
	inv, err := r.store.InstallationArtifact(ctx, tenant, installationID)
	if err != nil {
		return "", "", err
	}
	return inv.VersionID, inv.VersionDigest, nil
}
