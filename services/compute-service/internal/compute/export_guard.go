package compute

import (
	"context"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// Long-lived export guards do not consume the ordinary owner pool: Identity
// and lease callbacks must still make progress even with MaxConns=1. Bound
// both additional connections and waiting operations by the request context.
var exportGuardSlots = make(chan struct{}, 2)

type exportGuard struct {
	conn     *pgx.Conn
	tx       pgx.Tx
	computer Computer
}

func (s *Store) beginExportGuard(ctx context.Context, scope contracts.Scope, computerID string) (*exportGuard, error) {
	select {
	case exportGuardSlots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	release := true
	defer func() {
		if release {
			<-exportGuardSlots
		}
	}()
	conn, err := pgx.ConnectConfig(ctx, s.pool.Config().ConnConfig.Copy())
	if err != nil {
		return nil, err
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		closeExportConn(conn)
		return nil, err
	}
	g := &exportGuard{conn: conn, tx: tx}
	fail := func(e error) (*exportGuard, error) { _ = tx.Rollback(ctx); closeExportConn(conn); return nil, e }
	if err = s.checkOwnerWritable(ctx, tx, scope); err != nil {
		return fail(err)
	}
	g.computer, err = scanComputer(tx.QueryRow(ctx, `SELECT `+computerSelect+` FROM ahp_compute.computers WHERE id=$1 AND tenant=$2 AND deployment=$3 AND subject=$4 FOR UPDATE`, computerID, scope.Tenant, scope.Deployment, scope.Subject))
	if err != nil {
		return fail(err)
	}
	if err = g.check(ctx); err != nil {
		return fail(err)
	}
	release = false
	return g, nil
}
func closeExportConn(conn *pgx.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = conn.Close(ctx)
}
func (g *exportGuard) close() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = g.tx.Rollback(ctx)
	closeExportConn(g.conn)
	<-exportGuardSlots
}

// check refuses a source that is not quiescent. The command statuses it holds on are
// chosen for one reason - a writer that might still be alive - and 'lost' was removed
// from that list with DEF-COMPUTE-6 because reconciliation only ever produces it with
// proof that no backend was ever invoked for the command (Store.MarkBackendStarted is
// the evidence boundary). An unproven effect keeps holding the guard exactly as before:
// 'side_effect_unknown' is still refused, because there the ledger genuinely cannot say
// the workspace stopped being touched.
func (g *exportGuard) check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if g.computer.Status == ComputerDestroyed {
		return contracts.NewError(contracts.ErrForbidden, "destroyed computer cannot export")
	}
	var busy bool
	err := g.tx.QueryRow(ctx, `SELECT
 EXISTS(SELECT 1 FROM ahp_compute.incarnations WHERE computer_id=$1 AND status<>'stopped') OR
 EXISTS(SELECT 1 FROM ahp_compute.commands WHERE computer_id=$1 AND status IN ('accepted','dispatched','side_effect_unknown')) OR
 EXISTS(SELECT 1 FROM ahp_compute.activity_leases WHERE computer_id=$1 AND expires>clock_timestamp()) OR
 EXISTS(SELECT 1 FROM ahp_compute.env_revisions WHERE computer_id=$1 AND status='pending')`, g.computer.ID).Scan(&busy)
	if err != nil {
		return err
	}
	if busy {
		return contracts.NewReasoned(contracts.ErrConflict, "export_source_busy", "export requires a quiescent computer; active command, PTY or install must finish")
	}
	return nil
}
