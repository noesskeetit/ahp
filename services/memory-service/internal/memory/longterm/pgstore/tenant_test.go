package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/memory/longterm"
)

func TestPostgresTenantFenceAndOwnerDirectoryShareAdmissionTransaction(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("AHP_TEST_DATABASE_URL")
	}
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL or AHP_TEST_DATABASE_URL is not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	id := memory.Identity{TenantID: "test-" + hex.EncodeToString(random[:]), UserID: "owner", ActorID: "runner"}
	defer func() {
		_, _ = store.Pool.Exec(ctx, `DELETE FROM ahp_longterm.memory_records WHERE owner_key IN (SELECT owner_key FROM ahp_longterm.owner_directory WHERE tenant=$1)`, id.TenantID)
		_, _ = store.Pool.Exec(ctx, `DELETE FROM ahp_longterm.memory_owners WHERE owner_key IN (SELECT owner_key FROM ahp_longterm.owner_directory WHERE tenant=$1)`, id.TenantID)
		_, _ = store.Pool.Exec(ctx, `DELETE FROM ahp_longterm.erased_tenants WHERE tenant=$1`, id.TenantID)
	}()
	rollback := errors.New("abort owner admission")
	err = store.Within(ctx, id.Owner(), func(tx memory.Tx) error {
		if err := tx.RegisterIdentity(id); err != nil {
			return err
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	owners, err := store.TenantOwners(ctx, id.TenantID)
	if err != nil || len(owners) != 0 {
		t.Fatal("rolled-back owner escaped into directory", owners, err)
	}
	admitted, release, committed := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		committed <- store.Within(ctx, id.Owner(), func(tx memory.Tx) error {
			if err := tx.RegisterIdentity(id); err != nil {
				close(admitted)
				return err
			}
			close(admitted)
			select {
			case <-release:
				return tx.Put("test", "admitted", true)
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	select {
	case <-admitted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	fenced := make(chan error, 1)
	go func() { fenced <- store.FenceTenant(ctx, id.TenantID) }()
	close(release)
	if err := <-committed; err != nil {
		t.Fatal(err)
	}
	if err := <-fenced; err != nil {
		t.Fatal(err)
	}
	owners, err = store.TenantOwners(ctx, id.TenantID)
	if err != nil || len(owners) != 1 || owners[0].UserID != id.UserID {
		t.Fatal("tenant inventory lost admitted owner", owners, err)
	}
	late := id
	late.UserID = "late-owner"
	err = store.Within(ctx, late.Owner(), func(tx memory.Tx) error { return tx.RegisterIdentity(late) })
	var denied *memory.Error
	if !errors.As(err, &denied) || denied.Code != "tenant_deleted" {
		t.Fatal("late owner passed tenant fence", err)
	}
}
