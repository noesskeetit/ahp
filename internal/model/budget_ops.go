package model

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

const reservationCols = `id, budget_id, key, amount, actual, settled, digest`

// retryable reports whether err is a Postgres serialization/deadlock failure.
func retryable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && (pgErr.SQLState() == "40001" || pgErr.SQLState() == "40P01")
}

// Reserve places an immutable hold on a budget. Replays with the same digest
// return the original reservation; a different digest is ErrKeyReuse.
func (s *BudgetStore) Reserve(ctx context.Context, scope contracts.Scope, budgetID, key string, amount int64) (Reservation, error) {
	if err := scope.Validate(); err != nil {
		return Reservation{}, err
	}
	if budgetID == "" || key == "" {
		return Reservation{}, contracts.NewError(contracts.ErrInvalid, "budgetID and key required")
	}
	if err := validAmount(amount); err != nil {
		return Reservation{}, err
	}
	dg, err := digest("budget.reserve", scope, budgetID, key, amount)
	if err != nil {
		return Reservation{}, err
	}
	for attempt := 0; attempt < 6; attempt++ {
		r, retry, err := s.tryReserve(ctx, scope, budgetID, key, amount, dg)
		if !retry {
			return r, err
		}
	}
	return Reservation{}, errConflictRetry
}

func (s *BudgetStore) tryReserve(ctx context.Context, scope contracts.Scope, budgetID, key string, amount int64, dg string) (Reservation, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Reservation{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := lockBudgetTree(ctx, tx, scope, budgetID); err != nil {
		return Reservation{}, false, err
	}
	var (
		existing  Reservation
		found     bool
		closed    bool
		remaining int64
	)
	err = tx.QueryRow(ctx,
		`SELECT closed, "limit" - spent - reserved FROM ahp_model.budgets WHERE id=$1`, budgetID).
		Scan(&closed, &remaining)
	if err != nil {
		return Reservation{}, false, err
	}
	err = tx.QueryRow(ctx,
		`SELECT `+reservationCols+` FROM ahp_model.budget_reservations
		 WHERE budget_id=$1 AND key=$2 FOR UPDATE`, budgetID, key).
		Scan(&existing.ID, &existing.BudgetID, &existing.Key, &existing.Amount, &existing.Actual, &existing.Settled, &existing.digest)
	switch {
	case err == nil:
		found = true
	case errors.Is(err, pgx.ErrNoRows):
	case retryable(err):
		return Reservation{}, true, nil
	default:
		return Reservation{}, false, err
	}
	if found {
		if existing.digest != dg {
			return Reservation{}, false, contracts.NewError(contracts.ErrKeyReuse, "idempotency key reused with different request")
		}
		if err := tx.Commit(ctx); err != nil {
			return Reservation{}, false, err
		}
		return existing, false, nil
	}
	if closed {
		return Reservation{}, false, contracts.NewError(contracts.ErrConflict, "budget closed")
	}
	if err := admitBudgetTree(ctx, tx, budgetID); err != nil {
		return Reservation{}, false, err
	}
	if amount > remaining {
		return Reservation{}, false, contracts.NewError(contracts.ErrConflict, "insufficient budget capacity")
	}
	if _, err := tx.Exec(ctx,
		`UPDATE ahp_model.budgets SET reserved = reserved + $2 WHERE id = $1`, budgetID, amount); err != nil {
		return Reservation{}, false, err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO ahp_model.budget_reservations (id,budget_id,key,amount,digest)
		 VALUES ($1,$2,$3,$4,$5)`, contracts.NewID(), budgetID, key, amount, dg); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.SQLState() == "23505" {
			return Reservation{}, true, nil
		}
		return Reservation{}, false, err
	}
	r, err := s.scanReservation(tx.QueryRow(ctx,
		`SELECT `+reservationCols+` FROM ahp_model.budget_reservations
		 WHERE budget_id=$1 AND key=$2`, budgetID, key))
	if err != nil {
		return Reservation{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		if retryable(err) {
			return Reservation{}, true, nil
		}
		return Reservation{}, false, err
	}
	return r, false, nil
}

// Settle closes a reservation with its actual nonnegative cost. actual==0
// cancels it. An unsettled reservation keeps its hold (no auto release).
// Replays with the same actual are idempotent; a different actual is a
// settlement mismatch conflict.
func (s *BudgetStore) Settle(ctx context.Context, scope contracts.Scope, budgetID, key string, actual int64) (Reservation, error) {
	if err := scope.Validate(); err != nil {
		return Reservation{}, err
	}
	if actual < 0 || actual > maxAmount {
		return Reservation{}, contracts.NewError(contracts.ErrInvalid, "actual out of bounds")
	}
	for attempt := 0; attempt < 6; attempt++ {
		r, retry, err := s.trySettle(ctx, scope, budgetID, key, actual)
		if !retry {
			return r, err
		}
	}
	return Reservation{}, errConflictRetry
}

func (s *BudgetStore) trySettle(ctx context.Context, scope contracts.Scope, budgetID, key string, actual int64) (Reservation, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Reservation{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := lockBudgetTree(ctx, tx, scope, budgetID); err != nil {
		return Reservation{}, false, err
	}
	r, err := s.scanReservation(tx.QueryRow(ctx,
		`SELECT `+reservationCols+` FROM ahp_model.budget_reservations
		 WHERE budget_id=$1 AND key=$2 FOR UPDATE`, budgetID, key))
	if err != nil {
		return Reservation{}, false, err
	}
	if r.Settled {
		if r.Actual != actual {
			return Reservation{}, false, contracts.NewError(contracts.ErrConflict, "settlement mismatch for key")
		}
		if err := tx.Commit(ctx); err != nil {
			if retryable(err) {
				return Reservation{}, true, nil
			}
			return Reservation{}, false, err
		}
		return r, false, nil
	}
	if _, err := tx.Exec(ctx,
		`UPDATE ahp_model.budgets SET reserved = reserved - $2, spent = spent + $3 WHERE id = $1`,
		budgetID, r.Amount, actual); err != nil {
		return Reservation{}, false, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE ahp_model.budget_reservations SET settled = true, actual = $2 WHERE id = $1`, r.ID, actual); err != nil {
		return Reservation{}, false, err
	}
	r.Actual = actual
	r.Settled = true
	if err := tx.Commit(ctx); err != nil {
		if retryable(err) {
			return Reservation{}, true, nil
		}
		return Reservation{}, false, err
	}
	return r, false, nil
}

// Close closes a budget that has no open reservations and no open children.
// Its spent total propagates to the parent and the parent hold carved for this
// child is released. Parent is locked before child (root-first order).
// Closing an already-closed budget is a no-op.
func (s *BudgetStore) Close(ctx context.Context, scope contracts.Scope, budgetID string) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	for attempt := 0; attempt < 6; attempt++ {
		retry, err := s.tryClose(ctx, scope, budgetID)
		if !retry {
			return err
		}
	}
	return errConflictRetry
}

func (s *BudgetStore) tryClose(ctx context.Context, scope contracts.Scope, budgetID string) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	b, err := lockBudgetTree(ctx, tx, scope, budgetID)
	if err != nil {
		return false, err
	}
	if b.Closed {
		if err := tx.Commit(ctx); err != nil {
			return retryable(err), err
		}
		return false, nil
	}
	var open int
	if err := tx.QueryRow(ctx,
		`SELECT count(*) FROM ahp_model.budget_reservations WHERE budget_id=$1 AND settled = false`, budgetID).Scan(&open); err != nil {
		return false, err
	}
	if open > 0 {
		return false, contracts.NewError(contracts.ErrConflict, "budget has open reservations")
	}
	var children int
	if err := tx.QueryRow(ctx,
		`SELECT count(*) FROM ahp_model.budgets WHERE parent = $1 AND closed = false`, budgetID).Scan(&children); err != nil {
		return false, err
	}
	if children > 0 {
		return false, contracts.NewError(contracts.ErrConflict, "budget has open children")
	}
	if b.Parent != "" {
		var pScope contracts.Scope
		err := tx.QueryRow(ctx,
			`SELECT tenant, subject, deployment FROM ahp_model.budgets WHERE id=$1`, b.Parent).
			Scan(&pScope.Tenant, &pScope.Subject, &pScope.Deployment)
		if err != nil {
			return false, err
		}
		if _, err := lockBudget(ctx, tx, pScope, b.Parent); err != nil {
			return false, err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE ahp_model.budgets SET reserved = reserved - $2, spent = spent + $3 WHERE id = $1`,
			b.Parent, b.Limit, b.Spent); err != nil {
			return false, err
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE ahp_model.budgets SET closed = true WHERE id = $1`, budgetID); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return retryable(err), err
	}
	return false, nil
}

// GetReservation reads one reservation by its idempotency key inside scope.
// It is the reconciliation read for replay paths: when a Settle returns a
// mismatch because an earlier attempt already committed the settlement, the
// caller adopts the recorded actual instead of charging a second time.
func (s *BudgetStore) GetReservation(ctx context.Context, scope contracts.Scope, budgetID, key string) (Reservation, error) {
	if err := scope.Validate(); err != nil {
		return Reservation{}, err
	}
	if budgetID == "" || key == "" {
		return Reservation{}, contracts.NewError(contracts.ErrInvalid, "budgetID and key required")
	}
	return s.scanReservation(s.pool.QueryRow(ctx,
		`SELECT r.id, r.budget_id, r.key, r.amount, r.actual, r.settled, r.digest
		 FROM ahp_model.budget_reservations r
		 JOIN ahp_model.budgets b ON b.id = r.budget_id
		 WHERE r.budget_id=$1 AND r.key=$2 AND b.tenant=$3 AND b.subject=$4 AND b.deployment=$5`,
		budgetID, key, scope.Tenant, scope.Subject, scope.Deployment))
}
