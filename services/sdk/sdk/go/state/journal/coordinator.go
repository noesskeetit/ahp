package journal

import (
	"context"
	"net/url"
	"strconv"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
)

const coordinatorPath = "/internal/v1/state/coordinator"

type (
	VMReconcileCursor = journal.VMReconcileCursor
	VMReconcilePage   = journal.VMReconcilePage
)

// CoordinatorClient addresses the platform-wide VM coordination API.
type CoordinatorClient struct {
	client *Client
}

// Coordinator uses the same task token source and mTLS client as c. Its
// routes require the coordinator workload identity and state.coordinate.
func (c *Client) Coordinator() *CoordinatorClient {
	return &CoordinatorClient{client: c}
}

func (c *CoordinatorClient) ListGlobalRecoverableVMPage(ctx context.Context, status string, after *VMReconcileCursor) (VMReconcilePage, error) {
	var page VMReconcilePage
	query := url.Values{"status": {status}}
	if after != nil {
		query.Set("after", after.At.Format(time.RFC3339Nano))
		query.Set("after_tenant", after.TenantID)
		query.Set("after_session", after.SessionID)
	}
	err := c.client.get(ctx, coordinatorPath+"/vm-reconcile", query, &page)
	return page, err
}

// ScopedCoordinatorClient operates on VM-managed sessions in one target scope.
type ScopedCoordinatorClient struct {
	client   *Client
	sessions string
}

func (c *CoordinatorClient) ForScope(scope contracts.Scope) *ScopedCoordinatorClient {
	return &ScopedCoordinatorClient{client: c.client, sessions: coordinatorPath + "/scopes/" +
		coordinatorSegment(scope.Tenant) + "/" + coordinatorSegment(scope.Deployment) + "/" + coordinatorSegment(scope.Subject) +
		"/sessions"}
}

func (c *ScopedCoordinatorClient) EnqueueTurn(ctx context.Context, input EnqueueRequest) (EnqueueReceipt, error) {
	var receipt EnqueueReceipt
	err := c.client.http.Call(ctx, c.sessions+":enqueue", input, &receipt, true)
	return receipt, err
}

func (c *ScopedCoordinatorClient) GetTurnStatus(ctx context.Context, session string) (TurnStatus, error) {
	var status TurnStatus
	err := c.client.get(ctx, c.sessionPath(session), nil, &status)
	return status, err
}

func (c *ScopedCoordinatorClient) InspectSession(ctx context.Context, session string) (ResumeState, error) {
	var state ResumeState
	err := c.client.get(ctx, c.sessionPath(session), url.Values{"view": {"resume"}}, &state)
	return state, err
}

func (c *ScopedCoordinatorClient) GetEvents(ctx context.Context, session string, afterSeq int64, limit int, stateNamespace string) (EventPage, error) {
	var page EventPage
	query := url.Values{"after_seq": {strconv.FormatInt(afterSeq, 10)}, "limit": {strconv.Itoa(limit)}}
	if stateNamespace != "" {
		query.Set("state_namespace", stateNamespace)
	}
	err := c.client.get(ctx, c.sessionPath(session)+"/events", query, &page)
	return page, err
}

func (c *ScopedCoordinatorClient) CancelSession(ctx context.Context, session string, turnSeq int64) (TurnStatus, error) {
	var status TurnStatus
	req := struct {
		TurnSeq int64 `json:"turn_seq,omitempty"`
	}{turnSeq}
	err := c.client.http.Call(ctx, c.sessionPath(session)+"/vm:cancel", req, &status, true)
	return status, err
}

func (c *ScopedCoordinatorClient) RequeueFencedVMTurn(ctx context.Context, session string, req FenceRequeueRequest) error {
	return c.client.http.Call(ctx, c.sessionPath(session)+"/vm:requeue-fenced", req, nil, true)
}

func (c *ScopedCoordinatorClient) sessionPath(session string) string {
	return c.sessions + "/" + coordinatorSegment(session)
}

func coordinatorSegment(id string) string {
	switch id {
	case ".":
		return "%2E"
	case "..":
		return "%2E%2E"
	default:
		return url.PathEscape(id)
	}
}
