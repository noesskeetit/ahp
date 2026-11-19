package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/ahp-platform/ahp/internal/memory/longterm"
)

// Run against a disposable database: TEST_DATABASE_URL=... go test ./internal/postgres.
func TestPostgresTransactionsAndConcurrentOwnerUpdates(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("AHP_TEST_DATABASE_URL")
	}
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL or AHP_TEST_DATABASE_URL is not configured")
	}
	ctx := context.Background()
	s, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		t.Fatal(err)
	}
	owner := "integration-" + hex.EncodeToString(id[:])
	defer func() {
		_, _ = s.Pool.Exec(ctx, "DELETE FROM ahp_longterm.memory_records WHERE owner_key=$1", owner)
		_, _ = s.Pool.Exec(ctx, "DELETE FROM ahp_longterm.memory_owners WHERE owner_key=$1", owner)
	}()
	err = s.Within(ctx, owner, func(tx memory.Tx) error {
		if err := tx.Put("counter", "n", 0); err != nil {
			return err
		}
		return errors.New("rollback")
	})
	if err == nil {
		t.Fatal("transaction should roll back")
	}
	err = s.Within(ctx, owner, func(tx memory.Tx) error {
		var n int
		err := tx.Get("counter", "n", &n)
		if !errors.Is(err, memory.ErrMissing) {
			return errors.New("rolled back data exists")
		}
		return tx.Put("counter", "n", 0)
	})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for n := 0; n < 20; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- s.Within(ctx, owner, func(tx memory.Tx) error {
				var n int
				if err := tx.Get("counter", "n", &n); err != nil {
					return err
				}
				return tx.Put("counter", "n", n+1)
			})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	err = s.Within(ctx, owner, func(tx memory.Tx) error {
		var n int
		if err := tx.Get("counter", "n", &n); err != nil {
			return err
		}
		if n != 20 {
			t.Errorf("lost update: %d", n)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
