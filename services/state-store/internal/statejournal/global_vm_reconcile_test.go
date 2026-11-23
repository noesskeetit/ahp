package statejournal

import (
	"crypto/rand"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
)

func TestGlobalVMReconcileUsesTenantTieBreaker(t *testing.T) {
	ctx, pool, store := journalTestStore(t)
	const session = "session"
	at := time.Date(2003, time.April, 1, 0, 0, 0, 0, time.UTC)
	tenant := "global-pager-" + rand.Text()
	scopes := []contracts.Scope{
		{Tenant: tenant + "-a", Deployment: "agent-1", Subject: "alice"},
		{Tenant: tenant + "-b", Deployment: "agent-2", Subject: "bob"},
		{Tenant: tenant + "-c", Deployment: "agent-3", Subject: "claire"},
	}
	for _, status := range []string{SessionQueued, SessionActive} {
		t.Run(status, func(t *testing.T) {
			id := session + "-" + status
			for _, scope := range scopes {
				_, req := newVMAdmission(t)
				req.SessionID = id
				receipt, err := store.EnqueueTurn(ctx, scope, req)
				if err != nil {
					t.Fatal(err)
				}
				if status == SessionActive {
					_, err := store.ClaimVMTurn(ctx, scope, id, journal.VMTurnClaimRequest{
						TurnSeq: receipt.HeadSeq, Generation: 1,
						ExecutionID: journal.DeterministicExecutionID(scope, id, receipt.HeadSeq, 1),
					}, time.Minute)
					if err != nil {
						t.Fatal(err)
					}
				}
				_, err = pool.Exec(ctx, `UPDATE ahp_journal.sessions
					SET queued_at=$3, lease_expires_at=CASE WHEN status='ACTIVE' THEN $3::timestamptz ELSE NULL END
					WHERE tenant_id=$1 AND session_id=$2`, scope.Tenant, id, at)
				if err != nil {
					t.Fatal(err)
				}
			}
			page, err := store.ListGlobalRecoverableVMPage(ctx, status, &journal.VMReconcileCursor{At: at, TenantID: tenant, SessionID: id}, 2)
			if err != nil || len(page.Sessions) != 2 || page.Sessions[0].Scope != scopes[0] ||
				page.Sessions[1].Scope != scopes[1] || page.Next == nil || !page.Next.At.Equal(at) ||
				page.Next.TenantID != scopes[1].Tenant || page.Next.SessionID != id {
				t.Fatalf("first page: %+v, next %+v, %v", page.Sessions, page.Next, err)
			}
			second, err := store.ListGlobalRecoverableVMPage(ctx, status, page.Next, 1)
			if err != nil || len(second.Sessions) != 1 || second.Sessions[0].Scope != scopes[2] ||
				second.Sessions[0].SessionID != id || second.Sessions[0].Status != status {
				t.Fatalf("second page: %+v, %v", second, err)
			}
			if status == SessionActive && second.Sessions[0].ExecutionID !=
				journal.DeterministicExecutionID(scopes[2], id, 1, 1) {
				t.Fatalf("expired active execution omitted: %+v", second.Sessions[0])
			}
		})
	}
}
