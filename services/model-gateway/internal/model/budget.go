// Package model holds the hierarchical budget ledger: root budgets, carved
// child budgets, reservations and settlement. Idempotency keys are scoped
// inside the caller's own namespace; budget ids are generated, never the key.
package model

import (
	"context"
	"errors"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const maxAmount int64 = 1e12

const budgetCols = `id, parent, task, tenant, subject, deployment, key, "limit", reserved, spent, closed, digest, owner`

type Budget struct {
	ID       string
	Parent   string // "" => root
	Task     string
	Scope    contracts.Scope
	Key      string
	Limit    int64
	Reserved int64
	Spent    int64
	Closed   bool
	Owner    string
	digest   string
}

type Reservation struct {
	ID       string
	BudgetID string
	Key      string
	Amount   int64
	Actual   int64
	Settled  bool
	digest   string
}

type BudgetStore struct{ pool *pgxpool.Pool }

func NewBudgetStore(pool *pgxpool.Pool) *BudgetStore { return &BudgetStore{pool: pool} }

func (s *BudgetStore) Migrate(ctx context.Context) error {
	err := migrateModel(ctx, s.pool, `
CREATE SCHEMA IF NOT EXISTS ahp_model;
CREATE TABLE IF NOT EXISTS ahp_model.budgets (
  id         text PRIMARY KEY,
  parent     text REFERENCES ahp_model.budgets(id),
  task       text NOT NULL,
  tenant     text NOT NULL,
  subject    text NOT NULL,
  deployment text NOT NULL,
  key        text NOT NULL,
  "limit"    bigint NOT NULL CHECK ("limit" >= 0 AND "limit" <= 1000000000000),
  reserved   bigint NOT NULL DEFAULT 0 CHECK (reserved >= 0),
  spent      bigint NOT NULL DEFAULT 0 CHECK (spent >= 0),
  closed     boolean NOT NULL DEFAULT false,
  digest     text NOT NULL,
  UNIQUE (tenant, subject, deployment, task, key)
);
CREATE TABLE IF NOT EXISTS ahp_model.budget_reservations (
  id         text PRIMARY KEY,
  budget_id  text NOT NULL REFERENCES ahp_model.budgets(id),
  key        text NOT NULL,
  amount     bigint NOT NULL CHECK (amount > 0 AND amount <= 1000000000000),
  actual     bigint NOT NULL DEFAULT 0 CHECK (actual >= 0),
  settled    boolean NOT NULL DEFAULT false,
  digest     text NOT NULL,
  UNIQUE (budget_id, key)
);
-- Admission is bounded under the tree lock. Settlement must still record a
-- provider's actual bill if its response exceeds the admitted estimate.
ALTER TABLE ahp_model.budgets DROP CONSTRAINT IF EXISTS budgets_check;
ALTER TABLE ahp_model.budgets ADD COLUMN IF NOT EXISTS owner text NOT NULL DEFAULT '' CHECK (owner IN ('','state','gateway'));
CREATE INDEX IF NOT EXISTS budgets_completion_idx ON ahp_model.budgets(key,tenant,subject,id DESC) WHERE parent IS NOT NULL AND NOT closed;
CREATE TABLE IF NOT EXISTS ahp_model.budget_task_aliases (
 tenant text NOT NULL,subject text NOT NULL,deployment text NOT NULL,task text NOT NULL,key text NOT NULL,
 budget_id text NOT NULL REFERENCES ahp_model.budgets(id),
 PRIMARY KEY(tenant,subject,deployment,task,key)
);`+tenantInsertGuards("budgets", "budget_reservations", "budget_task_aliases"))
	return err
}

func validAmount(v int64) error {
	if v <= 0 || v > maxAmount {
		return contracts.NewError(contracts.ErrInvalid, "amount out of bounds")
	}
	return nil
}

func digest(parts ...any) (string, error) {
	return contracts.Digest(parts)
}

func (s *BudgetStore) scanBudget(row pgx.Row) (Budget, error) {
	var b Budget
	var parent *string
	err := row.Scan(&b.ID, &parent, &b.Task, &b.Scope.Tenant, &b.Scope.Subject,
		&b.Scope.Deployment, &b.Key, &b.Limit, &b.Reserved, &b.Spent, &b.Closed, &b.digest, &b.Owner)
	if errors.Is(err, pgx.ErrNoRows) {
		return b, contracts.NewError(contracts.ErrNotFound, "budget not found")
	}
	if err != nil {
		return b, err
	}
	if parent != nil {
		b.Parent = *parent
	}
	return b, nil
}

func (s *BudgetStore) scanReservation(row pgx.Row) (Reservation, error) {
	var r Reservation
	err := row.Scan(&r.ID, &r.BudgetID, &r.Key, &r.Amount, &r.Actual, &r.Settled, &r.digest)
	if errors.Is(err, pgx.ErrNoRows) {
		return r, contracts.NewError(contracts.ErrNotFound, "reservation not found")
	}
	return r, err
}

// Get returns the budget only when it lives inside the given scope.
func (s *BudgetStore) Get(ctx context.Context, scope contracts.Scope, id string) (Budget, error) {
	if err := scope.Validate(); err != nil {
		return Budget{}, err
	}
	return s.scanBudget(s.pool.QueryRow(ctx,
		`SELECT `+budgetCols+` FROM ahp_model.budgets
		 WHERE id=$1 AND tenant=$2 AND subject=$3 AND deployment=$4`,
		id, scope.Tenant, scope.Subject, scope.Deployment))
}

// Root creates (or replays) a root budget. The budget id is generated; key is
// the idempotency key scoped to (tenant,subject,deployment,task), so the same
// key in different scopes creates independent budgets.
func (s *BudgetStore) Root(ctx context.Context, scope contracts.Scope, task, key string, limit int64) (Budget, error) {
	if err := scope.Validate(); err != nil {
		return Budget{}, err
	}
	if task == "" || key == "" {
		return Budget{}, contracts.NewError(contracts.ErrInvalid, "task and key required")
	}
	if err := validAmount(limit); err != nil {
		return Budget{}, err
	}
	dg, err := digest("budget.root", scope, task, key, limit)
	if err != nil {
		return Budget{}, err
	}
	return s.insertBudget(ctx, "", contracts.Scope{}, task, scope, key, limit, dg)
}

// Allocate carves a child budget out of parent capacity. The child must share
// the parent tenant and subject (deployment may differ). The parent is part of
// the idempotency namespace, so the same key under a different parent is a
// distinct request.
func (s *BudgetStore) Allocate(ctx context.Context, parentScope contracts.Scope, parentID string, childScope contracts.Scope, childTask, key string, amount int64) (Budget, error) {
	if err := parentScope.Validate(); err != nil {
		return Budget{}, err
	}
	if err := childScope.Validate(); err != nil {
		return Budget{}, err
	}
	if parentScope.Tenant != childScope.Tenant || parentScope.Subject != childScope.Subject {
		return Budget{}, contracts.NewError(contracts.ErrForbidden, "child must share tenant and subject")
	}
	if parentID == "" || childTask == "" || key == "" {
		return Budget{}, contracts.NewError(contracts.ErrInvalid, "parentID, childTask and key required")
	}
	if err := validAmount(amount); err != nil {
		return Budget{}, err
	}
	dg, err := digest("budget.allocate", parentScope, parentID, childScope, childTask, key, amount)
	if err != nil {
		return Budget{}, err
	}
	return s.insertBudget(ctx, parentID, parentScope, childTask, childScope, "alloc/"+parentID+"/"+key, amount, dg)
}

// insertBudget is idempotent on (tenant,subject,deployment,task,key). The
// budget id is generated, never the key. When parentID != "" the parent row
// is locked FOR UPDATE and the amount reserved against it.
func (s *BudgetStore) insertBudget(ctx context.Context, parentID string, parentScope contracts.Scope, task string, scope contracts.Scope, key string, amount int64, dg string) (Budget, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Budget{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if parentID != "" {
		if _, err := lockBudgetTree(ctx, tx, parentScope, parentID); err != nil {
			return Budget{}, err
		}
	}
	if err := lockBudgetKey(ctx, tx, scope, task, key); err != nil {
		return Budget{}, err
	}
	if err := rejectBudgetAlias(ctx, tx, scope, task, key); err != nil {
		return Budget{}, err
	}

	var exists bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM ahp_model.budgets
		 WHERE tenant=$1 AND subject=$2 AND deployment=$3 AND task=$4 AND key=$5)`,
		scope.Tenant, scope.Subject, scope.Deployment, task, key).Scan(&exists); err != nil {
		return Budget{}, err
	}
	if exists {
		b, err := s.scanBudget(tx.QueryRow(ctx,
			`SELECT `+budgetCols+` FROM ahp_model.budgets
			 WHERE tenant=$1 AND subject=$2 AND deployment=$3 AND task=$4 AND key=$5`,
			scope.Tenant, scope.Subject, scope.Deployment, task, key))
		if err != nil {
			return Budget{}, err
		}
		if b.digest != dg {
			return Budget{}, contracts.NewError(contracts.ErrKeyReuse, "idempotency key reused with different request")
		}
		if err := tx.Commit(ctx); err != nil {
			return Budget{}, err
		}
		return b, nil
	}

	if parentID != "" {
		if err := admitBudgetTree(ctx, tx, parentID); err != nil {
			return Budget{}, err
		}
		var avail int64
		var closed bool
		err = tx.QueryRow(ctx,
			`SELECT "limit" - spent - reserved, closed FROM ahp_model.budgets WHERE id=$1`, parentID).Scan(&avail, &closed)
		if errors.Is(err, pgx.ErrNoRows) {
			return Budget{}, contracts.NewError(contracts.ErrNotFound, "parent budget not found")
		}
		if err != nil {
			return Budget{}, err
		}
		if closed {
			return Budget{}, contracts.NewError(contracts.ErrConflict, "parent budget closed")
		}
		if amount > avail {
			return Budget{}, contracts.NewError(contracts.ErrConflict, "insufficient parent capacity")
		}
		if _, err := tx.Exec(ctx,
			`UPDATE ahp_model.budgets SET reserved = reserved + $2 WHERE id = $1`, parentID, amount); err != nil {
			return Budget{}, err
		}
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO ahp_model.budgets (id,parent,task,tenant,subject,deployment,key,"limit",digest)
		 VALUES ($1,NULLIF($2,''),$3,$4,$5,$6,$7,$8,$9)`,
		contracts.NewID(), parentID, task, scope.Tenant, scope.Subject, scope.Deployment, key, amount, dg); err != nil {
		var pgErr interface{ SQLState() string }
		if errors.As(err, &pgErr) && pgErr.SQLState() == "23505" {
			return Budget{}, errConflictRetry
		}
		return Budget{}, err
	}
	b, err := s.scanBudget(tx.QueryRow(ctx,
		`SELECT `+budgetCols+` FROM ahp_model.budgets
		 WHERE tenant=$1 AND subject=$2 AND deployment=$3 AND task=$4 AND key=$5`,
		scope.Tenant, scope.Subject, scope.Deployment, task, key))
	if err != nil {
		return Budget{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Budget{}, err
	}
	return b, nil
}

func lockBudget(ctx context.Context, tx pgx.Tx, scope contracts.Scope, id string) (Budget, error) {
	var b Budget
	var parent *string
	// IDs and namespace keys never change. NO KEY UPDATE serializes money
	// mutations while remaining compatible with an alias FK's KEY SHARE lock.
	err := tx.QueryRow(ctx,
		`SELECT `+budgetCols+` FROM ahp_model.budgets
		 WHERE id=$1 AND tenant=$2 AND subject=$3 AND deployment=$4 FOR NO KEY UPDATE`,
		id, scope.Tenant, scope.Subject, scope.Deployment).
		Scan(&b.ID, &parent, &b.Task, &b.Scope.Tenant, &b.Scope.Subject, &b.Scope.Deployment,
			&b.Key, &b.Limit, &b.Reserved, &b.Spent, &b.Closed, &b.digest, &b.Owner)
	if errors.Is(err, pgx.ErrNoRows) {
		return b, contracts.NewError(contracts.ErrNotFound, "budget not found")
	}
	if parent != nil {
		b.Parent = *parent
	}
	return b, err
}

var errConflictRetry = errors.New("budget: concurrent idempotent insert, retry")
