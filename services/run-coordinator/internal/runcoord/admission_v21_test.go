package runcoord

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/ahp-platform/ahp/pkg/transport"
)

type coldAdmissionState struct {
	State
	replayed bool
}

func (s coldAdmissionState) EnqueueTurn(_ context.Context, r journal.EnqueueRequest) (journal.EnqueueReceipt, error) {
	return journal.EnqueueReceipt{SessionID: r.SessionID, HeadSeq: 3, Epoch: 1, Status: "QUEUED",
		AcceptedMode: journal.DeliveryTurn, Replayed: s.replayed}, nil
}

type startupProbe struct {
	fenced    int
	starts    []Execution
	failStart bool
}

func (p *startupProbe) EnsureRunning(_ context.Context, e Execution) error {
	p.starts = append(p.starts, e)
	if p.failStart {
		return errors.New("temporary Runtime error")
	}
	return nil
}
func (p *startupProbe) Fence(_ context.Context, _ Execution) error {
	p.fenced++
	return errors.New("temporary fence error")
}

func TestSendColdStartAndDurableFallback(t *testing.T) {
	scope := contracts.Scope{Tenant: "tenant", Deployment: "agent", Subject: "user"}
	for _, tc := range []struct {
		name         string
		replay, fail bool
		starts       int
	}{
		{"cold", false, false, 1},
		{"runtime unavailable", false, true, 1},
		{"replayed old admission", true, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/v1/agents/agent/sessions/s:send", strings.NewReader(`{"harness_type":"go","format_version":1,"key":"input","input":{"inline":"e30="}}`))
			r.Header.Set("Content-Type", "application/json")
			r = r.WithContext(transport.WithPrincipal(r.Context(), transport.Principal{Scope: scope}))
			w := httptest.NewRecorder()
			dispatcher := &startupProbe{failStart: tc.fail}
			(&Coordinator{Scope: scope, State: coldAdmissionState{replayed: tc.replay}, Dispatcher: dispatcher}).Handler().ServeHTTP(w, r)
			if w.Code != http.StatusAccepted || len(dispatcher.starts) != tc.starts {
				t.Fatalf("HTTP %d, starts %d, want %d", w.Code, len(dispatcher.starts), tc.starts)
			}
			if tc.starts != 0 {
				want := journal.DeterministicExecutionID(scope, "s", 3, 2)
				if dispatcher.starts[0].ExecutionID != want {
					t.Fatalf("execution ID %q want %q", dispatcher.starts[0].ExecutionID, want)
				}
			}
		})
	}
}

func TestReconcileDrainsBothQueuesPastFailedFences(t *testing.T) {
	ctx, pool, db := openPG(t)
	_, err := pool.Exec(ctx, `
		INSERT INTO ahp_journal.sessions
			(tenant_id,deployment_id,subject_id,session_id,harness_type,format_version,status,execution_mode,
			 lease_epoch,lease_owner,queued_at,lease_expires_at,turn_input_seq,head_seq,updated_at)
		SELECT $1,$2,$3,'failed-'||i,'go',1,'ACTIVE','vm_managed',
		       1,'exec-old-'||i,now()-interval '1 hour',now()-interval '1 minute',1,1,now()-interval '10 minutes'
		FROM generate_series(1,65) AS i`, db.scope.Tenant, db.scope.Deployment, db.scope.Subject)
	if err != nil {
		t.Fatal(err)
	}
	queued, err := db.EnqueueTurn(ctx, journal.EnqueueRequest{
		SessionID: "queued-after-failures", Harness: "go", FormatVersion: 1, Key: "input",
		ExecutionMode: journal.ExecutionVMManaged, Input: journal.Payload{Inline: []byte(`{}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, "UPDATE ahp_journal.sessions SET queued_at=now()-interval '3 seconds' WHERE tenant_id=$1 AND session_id=$2", db.scope.Tenant, queued.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	probe := &startupProbe{}
	c := Coordinator{Scope: db.scope, State: db, Dispatcher: probe}
	if err := c.Reconcile(ctx); err == nil {
		t.Fatal("expected reported Runtime failures")
	}
	if probe.fenced != 65 || len(probe.starts) != 1 || probe.starts[0].SessionID != queued.SessionID {
		t.Fatalf("fenced %d, starts %v", probe.fenced, probe.starts)
	}
}
