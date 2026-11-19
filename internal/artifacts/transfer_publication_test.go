package artifacts

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync/atomic"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

// A real PG connection drops only the outer transfer COMMIT acknowledgement,
// after PostgreSQL has emitted CommandComplete. No production test hook is used.
type transferCommitACKConn struct {
	net.Conn
	publication bool
	dropped     *atomic.Bool
	buffer      []byte
}

func (c *transferCommitACKConn) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte("checkpoint_transfer_operations SET status='complete'")) {
		c.publication = true
	}
	return c.Conn.Write(p)
}

func (c *transferCommitACKConn) Read(p []byte) (int, error) {
	if len(c.buffer) == 0 {
		header := make([]byte, 5)
		if _, err := io.ReadFull(c.Conn, header); err != nil {
			return 0, err
		}
		n := binary.BigEndian.Uint32(header[1:])
		if n < 4 || n > 16<<20 {
			return 0, io.ErrUnexpectedEOF
		}
		body := make([]byte, int(n)-4)
		if _, err := io.ReadFull(c.Conn, body); err != nil {
			return 0, err
		}
		if c.publication && header[0] == 'C' && string(body) == "COMMIT\x00" && c.dropped.CompareAndSwap(false, true) {
			c.Conn.Close()
			return 0, io.ErrUnexpectedEOF
		}
		c.buffer = append(header, body...)
	}
	n := copy(p, c.buffer)
	c.buffer = c.buffer[n:]
	return n, nil
}

func TestTransferPublicationLostCommitACKReconciles(t *testing.T) {
	e, authority, copier, _, target, sourceRef, ticket := transferFixture(t)
	ctx := context.Background()
	cfg := e.store.pool.Config().Copy()
	if cfg.ConnConfig.TLSConfig != nil {
		t.Fatal("PG protocol fault requires this lane's local non-TLS PG connection")
	}
	var dropped atomic.Bool
	cfg.ConnConfig.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &transferCommitACKConn{Conn: conn, dropped: &dropped}, nil
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	owner, err := New(pool, e.client, e.bucket, e.store.authorize, WithTransferAuthority(authority, TransferLimits{}), WithTransferCopier(copier))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.CopyFromTransferGrant(ctx, target, ticket.Grant, "commit-ack"); contracts.ReasonOf(err) != "transfer_outcome_unknown" {
		t.Fatal("lost COMMIT acknowledgement must be UNKNOWN", err)
	}
	if !dropped.Load() {
		t.Fatal("commit ACK fault did not fire")
	}
	var status, version string
	if err := e.store.pool.QueryRow(ctx, `SELECT o.status,a.version FROM ahp_artifacts.checkpoint_transfer_operations o JOIN ahp_artifacts.artifacts a ON a.id=o.target_id WHERE o.source_ref->>'id'=$1`, sourceRef.ID).Scan(&status, &version); err != nil || status != "complete" || version == "" {
		t.Fatal("commit did not durably publish both records", status, version, err)
	}
	got, err := e.store.CopyFromTransferGrant(ctx, target, ticket.Grant, "commit-ack")
	if err != nil || got.Version != version || copier.calls != 1 {
		t.Fatal("fresh owner did not reconcile exact committed copy", got.Version, copier.calls, err)
	}
}

func TestTransferPublicationTransactionFailureStaysPrivate(t *testing.T) {
	e, _, copier, _, target, sourceRef, ticket := transferFixture(t)
	ctx := context.Background()
	// A real deferred trigger aborts the publication transaction at commit,
	// after both UPDATE statements ran. Earlier durable dispatch survives.
	_, err := e.store.pool.Exec(ctx, `CREATE FUNCTION ahp_artifacts.fail_transfer_publication() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected commit-time failure'; END $$;
CREATE CONSTRAINT TRIGGER fail_transfer_publication AFTER UPDATE ON ahp_artifacts.checkpoint_transfer_operations DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.status='complete') EXECUTE FUNCTION ahp_artifacts.fail_transfer_publication()`)
	if err != nil {
		t.Fatal(err)
	}
	defer e.store.pool.Exec(ctx, `DROP TRIGGER IF EXISTS fail_transfer_publication ON ahp_artifacts.checkpoint_transfer_operations; DROP FUNCTION IF EXISTS ahp_artifacts.fail_transfer_publication()`)
	if _, err := e.store.CopyFromTransferGrant(ctx, target, ticket.Grant, "publication-failure"); contracts.ReasonOf(err) != "transfer_outcome_unknown" {
		t.Fatal("publication commit failure must remain UNKNOWN", err)
	}
	var status, id string
	var sealed bool
	if err := e.store.pool.QueryRow(ctx, `SELECT o.status,o.target_id,a.version IS NOT NULL FROM ahp_artifacts.checkpoint_transfer_operations o JOIN ahp_artifacts.artifacts a ON a.id=o.target_id WHERE o.source_ref->>'id'=$1`, sourceRef.ID).Scan(&status, &id, &sealed); err != nil || status != "dispatched" || sealed {
		t.Fatal("failed publication leaked partial durable state", status, sealed, err)
	}
	targetCtx := WithCallerContext(ctx, target.Binding.ContextID)
	if _, err := e.store.Get(targetCtx, target.Scope, id); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatal("uncommitted bytes are readable", err)
	}
	if _, err := e.store.Seal(targetCtx, target.Scope, id); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatal("ordinary Seal bypassed failed publication", err)
	}
	if _, err := e.store.pool.Exec(ctx, `DROP TRIGGER fail_transfer_publication ON ahp_artifacts.checkpoint_transfer_operations; DROP FUNCTION ahp_artifacts.fail_transfer_publication()`); err != nil {
		t.Fatal(err)
	}
	got, err := e.store.CopyFromTransferGrant(ctx, target, ticket.Grant, "publication-failure")
	if err != nil || got.ID != id || copier.calls != 1 {
		t.Fatal("retry did not finalize existing dispatch", copier.calls, err)
	}
}

func TestTransferCompletedReplayStillRequiresBothCurrentGrants(t *testing.T) {
	for _, whom := range []string{"source", "target"} {
		t.Run(whom, func(t *testing.T) {
			e, authority, copier, source, target, _, ticket := transferFixture(t)
			ctx := context.Background()
			if _, err := e.store.CopyFromTransferGrant(ctx, target, ticket.Grant, "complete-then-revoke"); err != nil {
				t.Fatal(err)
			}
			task := source.Binding.TaskID
			if whom == "target" {
				task = target.Binding.TaskID
			}
			authority.mu.Lock()
			delete(authority.identities, task)
			authority.mu.Unlock()
			if _, err := e.store.CopyFromTransferGrant(ctx, target, ticket.Grant, "complete-then-revoke"); contracts.Code(err) != contracts.ErrForbidden {
				t.Fatal("completed receipt bypassed current grant", err)
			}
			if copier.calls != 1 {
				t.Fatal("completed replay redispatched")
			}
		})
	}
}
