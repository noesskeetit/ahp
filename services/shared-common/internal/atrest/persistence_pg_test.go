//go:build ahp_data

package atrest

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// This suite runs only against the real, isolated PostgreSQL provisioned for the
// lane (AHP_TEST_DATABASE_URL). There is no skip path: a missing store is a hard
// failure, because plaintext-in-bytes can only be disproven against real durable
// bytes. Nothing here prints plaintext or key material.

const (
	pgSchema   = "ahp_crypto_lab"
	pgSentinel = "PG-PLAINTEXT-MARKER-77c2"
)

func pgPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("AHP_TEST_DATABASE_URL must be set for at-rest persistence tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// setupLab creates a private schema and a bytea-backed table that models a
// persistence owner storing a sealed payload column. It is idempotent per run.
func setupLab(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stmts := []string{
		`CREATE SCHEMA IF NOT EXISTS ` + pgSchema,
		`CREATE TABLE IF NOT EXISTS ` + pgSchema + `.sealed_state (
			id           bigserial PRIMARY KEY,
			tenant       text NOT NULL,
			payload      bytea NOT NULL,
			written_at   timestamptz NOT NULL DEFAULT clock_timestamp()
		)`,
		`TRUNCATE TABLE ` + pgSchema + `.sealed_state`,
	}
	for _, s := range stmts {
		if _, err := pool.Exec(ctx, s); err != nil {
			t.Fatalf("setup %q: %v", s, err)
		}
	}
}

// rawStored returns the persisted bytea exactly as PostgreSQL stored it.
func rawStored(t *testing.T, pool *pgxpool.Pool, id int64) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var raw []byte
	if err := pool.QueryRow(ctx, `SELECT payload FROM `+pgSchema+`.sealed_state WHERE id=$1`, id).Scan(&raw); err != nil {
		t.Fatalf("read raw: %v", err)
	}
	return raw
}

func restartAuthority(t *testing.T, tenants ...string) *SyntheticKeyManager {
	t.Helper()
	dir := t.TempDir()
	root, err := randomTenantKeyMaterial()
	if err != nil {
		t.Fatal(err)
	}
	rootFile := filepath.Join(dir, "root-secret")
	if err := os.WriteFile(rootFile, []byte(hex.EncodeToString(root)), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvRootSecretFile, rootFile)
	t.Setenv(EnvByokSecretFile, "")
	auth, err := LoadAuthorityFromEnv(context.Background(), tenants)
	if err != nil {
		t.Fatal(err)
	}
	return auth
}

func TestPGSealedPayloadRoundtripAndByteCheck(t *testing.T) {
	pool := pgPool(t)
	setupLab(t, pool)
	auth := restartAuthority(t, testTenant)
	codec, err := NewCodec(auth)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	plaintext := "state-snapshot {\"note\":\"" + pgSentinel + "\"}"

	env, err := codec.Seal(ctx, testTenant, "state.payload", []byte(plaintext))
	if err != nil {
		t.Fatal(err)
	}
	var id int64
	if err := pool.QueryRow(ctx, `INSERT INTO `+pgSchema+`.sealed_state (tenant, payload) VALUES ($1,$2) RETURNING id`, testTenant, env).Scan(&id); err != nil {
		t.Fatal(err)
	}

	// Read the persisted bytes back through a fresh, independent query and
	// prove they are not plaintext.
	stored := rawStored(t, pool, id)
	if bytes.Contains(stored, []byte(pgSentinel)) {
		t.Fatal("persisted PostgreSQL bytes contain the plaintext marker")
	}
	if bytes.Equal(stored, []byte(plaintext)) {
		t.Fatal("persisted bytes equal plaintext")
	}
	// The owner can still decrypt exactly what it wrote.
	got, err := codec.Open(ctx, testTenant, "state.payload", stored)
	if err != nil {
		t.Fatalf("open stored: %v", err)
	}
	if string(got) != plaintext {
		t.Fatalf("roundtrip mismatch: %q", got)
	}
}

func TestPGWrongTenantRejected(t *testing.T) {
	pool := pgPool(t)
	setupLab(t, pool)
	auth := restartAuthority(t, testTenant, "tenant-b")
	codec, _ := NewCodec(auth)
	ctx := context.Background()
	env, err := codec.Seal(ctx, testTenant, "state.payload", []byte(pgSentinel))
	if err != nil {
		t.Fatal(err)
	}
	var id int64
	if err := pool.QueryRow(ctx, `INSERT INTO `+pgSchema+`.sealed_state (tenant, payload) VALUES ($1,$2) RETURNING id`, testTenant, env).Scan(&id); err != nil {
		t.Fatal(err)
	}
	stored := rawStored(t, pool, id)
	// A different tenant's authorized read path must not be able to open it.
	if _, err := codec.Open(ctx, "tenant-b", "state.payload", stored); !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("want ErrTenantMismatch, got %v", err)
	}
}

// After a restart that replays the same operator provisioning (and, for the
// rotated case, the same rotate), every persisted row must still be readable.
func TestPGRestartReadsPriorRows(t *testing.T) {
	pool := pgPool(t)
	setupLab(t, pool)
	// Deterministic operator profile shared across simulated restarts.
	dir := t.TempDir()
	root, _ := randomTenantKeyMaterial()
	rootFile := filepath.Join(dir, "root-secret")
	if err := os.WriteFile(rootFile, []byte(hex.EncodeToString(root)), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvRootSecretFile, rootFile)
	t.Setenv(EnvByokSecretFile, "")
	ctx := context.Background()

	// A real operator persists its master keys, so a rotated version must be
	// reproducible across restarts. Model a durable key store by deriving the
	// version-2 master deterministically from the operator root.
	v2Material, err := deriveKEK(root, testTenant, "root", 2)
	if err != nil {
		t.Fatal(err)
	}
	boot := func() *Codec {
		auth, err := LoadAuthorityFromEnv(ctx, []string{testTenant})
		if err != nil {
			t.Fatal(err)
		}
		// Replay the durable rotation deterministically.
		if _, err := auth.Rotate(ctx, testTenant, "root", v2Material); err != nil {
			t.Fatal(err)
		}
		c, err := NewCodec(auth)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}

	// First process: write one row on v1 (provisioned) then rotate+write v2.
	auth1, err := LoadAuthorityFromEnv(ctx, []string{testTenant})
	if err != nil {
		t.Fatal(err)
	}
	c1, _ := NewCodec(auth1)
	v1, err := c1.Seal(ctx, testTenant, "state.payload", []byte("v1-"+pgSentinel))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := auth1.Rotate(ctx, testTenant, "root", v2Material); err != nil {
		t.Fatal(err)
	}
	v2, err := c1.Seal(ctx, testTenant, "state.payload", []byte("v2-"+pgSentinel))
	if err != nil {
		t.Fatal(err)
	}
	var id1, id2 int64
	if err := pool.QueryRow(ctx, `INSERT INTO `+pgSchema+`.sealed_state (tenant, payload) VALUES ($1,$2) RETURNING id`, testTenant, v1).Scan(&id1); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO `+pgSchema+`.sealed_state (tenant, payload) VALUES ($1,$2) RETURNING id`, testTenant, v2).Scan(&id2); err != nil {
		t.Fatal(err)
	}

	// Simulated restart: a brand-new pool and authority reading both rows.
	pool2 := pgPool(t)
	c2 := boot()
	for _, id := range []int64{id1, id2} {
		stored := rawStored(t, pool2, id)
		if _, err := c2.Open(ctx, testTenant, "state.payload", stored); err != nil {
			t.Fatalf("after restart, row %d unreadable: %v", id, err)
		}
	}
}
