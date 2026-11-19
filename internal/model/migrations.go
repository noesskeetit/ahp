package model

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// migrateModel serializes all Model DDL, including the shared tenant guard.
// The content version prevents replacing triggers while other replicas serve
// traffic. The lock, version check and DDL use the same transaction/connection,
// so a pool limited to one connection cannot deadlock on nested pool acquisition.
func migrateModel(ctx context.Context, pool *pgxpool.Pool, ddl string) error {
	sum := sha256.Sum256([]byte(ddl))
	version := hex.EncodeToString(sum[:])
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, e := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('ahp_model/schema-migration',0))`); e != nil {
			return e
		}
		if _, e := tx.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS ahp_model; CREATE TABLE IF NOT EXISTS ahp_model.schema_migrations(version text PRIMARY KEY)`); e != nil {
			return e
		}
		var done bool
		if e := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_model.schema_migrations WHERE version=$1)`, version).Scan(&done); e != nil {
			return e
		}
		if done {
			return nil
		}
		if _, e := tx.Exec(ctx, ddl); e != nil {
			return e
		}
		_, e := tx.Exec(ctx, `INSERT INTO ahp_model.schema_migrations VALUES($1)`, version)
		return e
	})
}
