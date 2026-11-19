package tools

import (
	"context"
	"errors"
	"github.com/ahp-platform/ahp/internal/model/secretstore"
	"github.com/jackc/pgx/v5/pgxpool"
	"testing"
	"time"
)

func TestSubjectDeletionOAuthVaultIsolationAndLateCallback(t *testing.T) {
	e := newOAuthTest(t)
	e.connect(t)
	other := e.actor
	other.Subject = "other"
	consent, err := e.m.Begin(e.ctx, other, "personal")
	if err != nil {
		t.Fatal(err)
	}
	if err = e.m.Complete(e.ctx, other, e.code(t, consent)); err != nil {
		t.Fatal(err)
	}
	pending, err := e.m.Begin(e.ctx, e.actor, "personal")
	if err != nil {
		t.Fatal(err)
	}
	callback := e.code(t, pending)
	rows, err := e.pool.Query(e.ctx, "SELECT path FROM "+e.m.schema+".oauth_secrets WHERE tenant=$1 AND subject=$2", e.scope.Tenant, e.scope.Subject)
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	for rows.Next() {
		var p string
		if err = rows.Scan(&p); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, p)
	}
	rows.Close()
	if err = e.m.FenceSubject(e.ctx, e.scope.Tenant, e.scope.Subject); err != nil {
		t.Fatal(err)
	}
	if err = e.m.Complete(e.ctx, e.actor, callback); err == nil {
		t.Fatal("late callback accepted")
	}
	if err = e.m.DeleteSubject(e.ctx, e.scope.Tenant, e.scope.Subject); err != nil {
		t.Fatal(err)
	}
	if e.f.revokes.Load() < 2 {
		t.Fatal("known remote tokens not revoked")
	}
	for _, p := range paths {
		if _, _, err = e.vault.ReadVersion(e.ctx, p); !errors.Is(err, secretstore.ErrSecretNotFound) {
			t.Fatalf("vault token survives: %v", err)
		}
	}
	foreign := e.scope
	foreign.Subject = other.Subject
	if _, err = e.m.Resolve(e.ctx, foreign, "personal"); err != nil {
		t.Fatalf("foreign grant affected: %v", err)
	}
	if _, err = e.m.Begin(e.ctx, e.actor, "personal"); err == nil {
		t.Fatal("subject reconnected")
	}
	if err = e.m.DeleteSubject(e.ctx, e.scope.Tenant, e.scope.Subject); err != nil {
		t.Fatal(err)
	}
}

func TestSubjectDeletionOAuthWaitsForAdmittedVaultWriterWithOnePoolConnection(t *testing.T) {
	e := newOAuthTest(t)
	cfg := e.pool.Config()
	cfg.MaxConns = 1
	p, err := pgxpool.NewWithConfig(e.ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	m, err := NewOAuthManager(e.ctx, p, e.m.schema, e.cfg, e.opts...)
	if err != nil {
		t.Fatal(err)
	}
	c, release, err := m.lock(e.ctx, e.scope.Tenant, e.scope.Subject, ownerKey(e.scope, "personal"), false)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- m.FenceSubject(e.ctx, e.scope.Tenant, e.scope.Subject) }()
	select {
	case err = <-done:
		t.Fatalf("fence passed admitted writer: %v", err)
	case <-time.After(40 * time.Millisecond):
	}
	path := e.cfg.SecretPrefix + "/inflight"
	if _, err = c.Exec(e.ctx, "INSERT INTO "+m.schema+".oauth_secrets VALUES($1,$2,'personal',$3)", e.scope.Tenant, e.scope.Subject, path); err != nil {
		t.Fatal(err)
	}
	if _, err = e.vault.WriteCAS(e.ctx, path, map[string]any{"verifier": "synthetic"}, 0); err != nil {
		t.Fatal(err)
	}
	release()
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("MaxConns1 fence stalled")
	}
	if err = m.DeleteSubject(e.ctx, e.scope.Tenant, e.scope.Subject); err != nil {
		t.Fatal(err)
	}
	if _, _, err = e.vault.ReadVersion(e.ctx, path); !errors.Is(err, secretstore.ErrSecretNotFound) {
		t.Fatalf("late Vault path survives: %v", err)
	}
}

func TestSubjectDeletionOAuthPreservesUnknownGrantEvidence(t *testing.T) {
	e := newOAuthTest(t)
	e.connect(t)
	if _, err := e.pool.Exec(e.ctx, "UPDATE "+e.m.schema+".oauth_connections SET status='refresh_unknown' WHERE tenant=$1 AND subject=$2", e.scope.Tenant, e.scope.Subject); err != nil {
		t.Fatal(err)
	}
	if err := e.m.DeleteSubject(context.Background(), e.scope.Tenant, e.scope.Subject); err == nil {
		t.Fatal("unknown provider grant erased")
	}
	var n int
	if err := e.pool.QueryRow(e.ctx, "SELECT count(*) FROM "+e.m.schema+".oauth_secrets WHERE tenant=$1 AND subject=$2", e.scope.Tenant, e.scope.Subject).Scan(&n); err != nil || n == 0 {
		t.Fatalf("inventory lost: %d %v", n, err)
	}
}
