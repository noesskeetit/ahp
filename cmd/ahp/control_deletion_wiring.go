package main

import (
	"context"
	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/jackc/pgx/v5/pgxpool"
)

func buildControlDeletionConfig(pool *pgxpool.Pool) (ControlDeletionConfig, error) {
	r := registry.New(pool)
	cfg := ControlDeletionConfig{ResolveHook: func(ctx context.Context, tenant string, target registry.DeletionTarget) (string, error) {
		b, err := r.ResolveRuntime(ctx, tenant, target.Environment, target.Agent, target.Digest)
		if err != nil {
			return "", err
		}
		return b.Endpoint, nil
	}}
	if cert := envOr("AHP_RUNTIME_CLIENT_CERT", ""); cert != "" {
		client, err := space.NewMTLSClient(cert, envOr("AHP_RUNTIME_CLIENT_KEY", ""), envOr("AHP_RUNTIME_CLIENT_CA", ""))
		if err != nil {
			return ControlDeletionConfig{}, err
		}
		cfg.HTTPClient = client
	}
	return cfg, nil
}
