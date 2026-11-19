//go:build ahp_data

package main

import (
	"context"
	"sync"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/references"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The typed facade preserves both owners' gates. State authorizes its write;
// Artifacts separately evaluates the same verified task's current capability
// and derives its context from that principal, never from descriptor fields.
type checkpointArtifactOwner struct {
	*artifacts.Store
	identity *identity.Store
}

// Owner I/O must progress even when every State writer holds a connection.
// Read-only authority queries also need their own slots: GC holds an artifact
// transaction while obtaining a fresh State reference answer.
func checkpointOwnerPools(ctx context.Context, source *pgxpool.Pool) (*pgxpool.Pool, *pgxpool.Pool, func(), error) {
	if source == nil {
		return nil, nil, nil, contracts.NewError(contracts.ErrInvalid, "owner pool required")
	}
	dataConfig, readConfig := source.Config(), source.Config()
	dataConfig.MaxConns, readConfig.MaxConns = 4, 2
	dataConfig.MinConns, readConfig.MinConns = 0, 0
	data, err := pgxpool.NewWithConfig(ctx, dataConfig)
	if err != nil {
		return nil, nil, nil, err
	}
	reader, err := pgxpool.NewWithConfig(ctx, readConfig)
	if err != nil {
		data.Close()
		return nil, nil, nil, err
	}
	var once sync.Once
	closePools := func() { once.Do(func() { data.Close(); reader.Close() }) }
	return data, reader, closePools, nil
}

func (o checkpointArtifactOwner) PinCheckpointReferences(ctx context.Context, scope contracts.Scope, operation string, refs []references.CheckpointArtifact) error {
	p, ok := transport.FromContext(ctx)
	if !ok || p.Scope != scope || p.Claims.Kind != auth.KindTask {
		return contracts.NewError(contracts.ErrForbidden, "checkpoint artifact task principal required")
	}
	if err := dataLiveAuthority(o.identity)(ctx, p.Claims, "artifacts"); err != nil {
		return err
	}
	return o.Store.PinCheckpointReferences(artifacts.WithCallerContext(ctx, p.Claims.Context), scope, operation, refs)
}
