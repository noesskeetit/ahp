// Package stateapi is the thin HTTP facade over the durable State store.
//
// Identity rules (non-negotiable):
//   - the isolation scope is ALWAYS transport.FromContext; bodies may never
//     carry scope, tenant, subject or deployment fields (strict JSON rejects
//     unknown keys);
//   - task routes demand mTLS + a bound task token via transport.Task;
//     ClaimRun is deployment-only via transport.Workload and accepts no
//     subject;
//   - a granted lease is only returned together with a freshly issued task
//     token; if issuing is denied the lease is released as canceled and the
//     denial is returned instead.
package stateapi

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

const (
	// audience is the fixed token audience of the State API.
	audience = "ahp"
	// capability is the capability authorize is consulted with on task routes.
	capability = "state"
	// defaultTTL applies when a request omits ttl_ms.
	defaultTTL = 30 * time.Second
	// maxBodyBytes bounds every request body.
	maxBodyBytes = 1 << 20
)

// TaskIssuer renews the active grant token for a leased run. It returns the
// signed task token the worker must use for follow-up calls.
type TaskIssuer func(ctx context.Context, scope contracts.Scope, taskID string) (string, error)

// Handler is the mounted State API: POST /v1/state/{Operation}.
type Handler struct {
	store          *state.Store
	issue          TaskIssuer
	task           func(http.Handler) http.Handler
	workload       func(http.Handler) http.Handler
	operations     map[string]func(http.ResponseWriter, *http.Request)
	admin          func(http.Handler) http.Handler
	deletion       DeletionCoordinator
	adminAuthorize transport.Authorize
}

// New wires the facade. Every dependency is mandatory: a nil anywhere fails
// closed at construction time.
func New(store *state.Store, verifier *auth.Verifier, authorize transport.Authorize, issue TaskIssuer) (http.Handler, error) {
	switch {
	case store == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "stateapi: store must not be nil")
	case verifier == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "stateapi: verifier must not be nil")
	case authorize == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "stateapi: authorize must not be nil")
	case issue == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "stateapi: task issuer must not be nil")
	}
	h := &Handler{
		store:    store,
		issue:    issue,
		task:     transport.Task(verifier, audience, capability, authorize),
		workload: transport.Workload(),
	}
	h.operations = map[string]func(http.ResponseWriter, *http.Request){
		"CreateThread":       h.createThread,
		"GetThread":          h.getThread,
		"GetThreadByContext": h.getThreadByContext,
		"ListThreads":        h.listThreads,
		"CreateRun":          h.createRun,
		"GetRun":             h.getRun,
		"ListRuns":           h.listRuns,
		"Heartbeat":          h.heartbeat,
		"Release":            h.release,
		"Suspend":            h.suspend,
		"CancelWaitingRun":   h.cancelWaitingRun,
		"PutCheckpoint":      h.putCheckpoint,
		"GetCheckpoint":      h.getCheckpoint,
		"ListCheckpoints":    h.listCheckpoints,
		"GetPart":            h.getPart,
		"PutWrites":          h.putWrites,
		"GetWrites":          h.getWrites,
		"PutKV":              h.putKV,
		"PutKVBatch":         h.putKVBatch,
		"GetKVBatch":         h.getKVBatch,
		"DeleteKV":           h.deleteKV,
		"GetKV":              h.getKV,
		"SearchKV":           h.searchKV,
		"RecordIntent":       h.recordIntent,
		"UpdateIntent":       h.updateIntent,
		"ReconcileIntent":    h.reconcileIntent,
		"GetIntent":          h.getIntent,
		"Enqueue":            h.enqueue,
		"ReadInbox":          h.readInbox,
		"AckInbox":           h.ackInbox,
		"AppendStream":       h.appendStream,
		"ReadStream":         h.readStream,
		"FlushStream":        h.flushStream,
		"PutBatch":           h.putBatch,
		"DeleteThread":       h.deleteThread,
		"ImportThread":       h.importThread,
	}
	if store.CommonRecordsEnabled() {
		h.operations["PutCommonRoot"] = h.putCommonRoot
		h.operations["CommitRecords"] = h.commitRecords
		h.operations["ReadRecords"] = h.readRecords
		h.operations["ReadRecordsBundle"] = h.readRecordsBundle
	}
	return h, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "stateapi: unknown route"))
		return
	}
	if strings.HasPrefix(r.URL.Path, "/v1/state-admin/") {
		h.serveAdmin(w, r)
		return
	}
	rest, ok := strings.CutPrefix(r.URL.Path, "/v1/state/")
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "stateapi: unknown route"))
		return
	}
	if adminOperation(rest) {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "stateapi: operation requires the internal admin route"))
		return
	}
	switch rest {
	case "ClaimRun":
		h.workload(http.HandlerFunc(h.claimRun)).ServeHTTP(w, r)
		return
	}
	fn, ok := h.operations[rest]
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "stateapi: unknown route"))
		return
	}
	h.task(http.HandlerFunc(fn)).ServeHTTP(w, r)
}

// principal extracts the middleware-installed scope; handlers never accept
// identity from any other source.
func principal(r *http.Request) (contracts.Scope, error) {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		return contracts.Scope{}, contracts.NewError(contracts.ErrForbidden,
			"stateapi: no verified principal on this request")
	}
	return p.Scope, nil
}

// ---------------------------------------------------------------------------
// Request shapes. Unknown fields (including any scope/tenant/subject/
// deployment key) are rejected by transport.ReadJSON.
// ---------------------------------------------------------------------------

type createThreadRequest struct {
	ContextID string `json:"context_id"`
	Key       string `json:"key"`
}

type getThreadRequest struct {
	ThreadID string `json:"thread_id"`
}

type getThreadByContextRequest struct {
	ContextID string `json:"context_id"`
}

type createRunRequest struct {
	ThreadID string          `json:"thread_id"`
	Key      string          `json:"key"`
	Payload  json.RawMessage `json:"payload"`
	TaskID   string          `json:"task_id,omitempty"`
}

type getRunRequest struct {
	RunID string `json:"run_id"`
}

type listRunsRequest struct {
	ThreadID string `json:"thread_id"`
}

type claimRunRequest struct {
	RunID       string `json:"run_id,omitempty"`
	PayloadType string `json:"payload_type,omitempty"`
	Worker      string `json:"worker"`
	TTLms       int64  `json:"ttl_ms"`
}

type heartbeatRequest struct {
	RunID  string `json:"run_id"`
	Worker string `json:"worker"`
	Epoch  int64  `json:"epoch"`
	TTLms  int64  `json:"ttl_ms"`
}

type releaseRequest struct {
	RunID  string `json:"run_id"`
	Worker string `json:"worker"`
	Epoch  int64  `json:"epoch"`
	Status string `json:"status"`
	// CloseReason is optional and only meaningful on a terminal status: the caller
	// that ended the run names why, in the execution plane's own vocabulary
	// (model_finished / step_limit / attempt_ceiling_exceeded / provider_transport /
	// error, plus canceled for a run the user stopped). State stores it verbatim in
	// the run's existing payload document and invents nothing when it is absent.
	CloseReason string `json:"close_reason,omitempty"`
	// FailureCause is optional and only meaningful on a terminal status: the refusing
	// plane's own sentence, stored verbatim under the run payload's failure_cause key
	// (see internal/state close_reason.go for why it is a separate key from the
	// dictionary word above). Optional on the wire exactly like CloseReason.
	FailureCause string `json:"failure_cause,omitempty"`
}

type suspendRequest struct {
	RunID    string     `json:"run_id"`
	Worker   string     `json:"worker"`
	Epoch    int64      `json:"epoch"`
	WaitFor  string     `json:"wait_for"`
	Deadline *time.Time `json:"deadline,omitempty"`
}

type claimResponse struct {
	Lease     *state.Lease `json:"lease"`
	TaskToken string       `json:"task_token"`
}

type heartbeatResponse struct {
	ExpiresAt time.Time `json:"expires_at"`
}

type okResponse struct {
	OK bool `json:"ok"`
}

// ttl converts a requested ttl_ms (0 => default) into a duration.
func ttl(ms int64) (time.Duration, error) {
	if ms == 0 {
		return defaultTTL, nil
	}
	if ms < 0 {
		return 0, contracts.NewError(contracts.ErrInvalid, "ttl_ms must not be negative")
	}
	return time.Duration(ms) * time.Millisecond, nil
}

var okBody = okResponse{OK: true}
