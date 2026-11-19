package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// lanePool connects the sampler directly to the lane database (never through
// the harness proxy) using the injected test DSN.
func lanePool() (*pgxpool.Pool, error) {
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		return nil, fmt.Errorf("AHP_TEST_DATABASE_URL not set")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 2
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return pgxpool.NewWithConfig(ctx, cfg)
}
