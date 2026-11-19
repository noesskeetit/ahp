package main

import (
	"context"
	"fmt"

	"github.com/ahp-platform/ahp/internal/state"
)

// migrateStateAll runs the FULL State Store schema migration set. Store.Migrate
// (embedded schema.sql) only creates the control-plane tables (threads, runs);
// the checkpoint, KV, event and intent tables live in separate embedded
// migrations that are otherwise only exercised from tests. A runnable service
// role must create every table it serves on a fresh database, so all of them
// are invoked here. Each migration is idempotent and non-destructive, so this
// is safe to call on an already-migrated database.
//
// Fix for the integration-validation "state migration gap": without these calls
// a fresh deployment answered ~33 of 35 State operations with
// "relation does not exist" (masked upstream by an authentication 403).
func migrateStateAll(ctx context.Context, st *state.Store) error {
	type step struct {
		name string
		run  func(context.Context) error
	}
	steps := []step{
		{"schema", st.Migrate},
		{"control", st.MigrateControl},
		{"checkpoints", st.MigrateCheckpoints},
		{"kv", st.MigrateKV},
		{"events", st.MigrateEvents},
		{"intents", st.MigrateIntents},
	}
	for _, s := range steps {
		if err := s.run(ctx); err != nil {
			return fmt.Errorf("state %s migrate: %w", s.name, err)
		}
	}
	return nil
}
