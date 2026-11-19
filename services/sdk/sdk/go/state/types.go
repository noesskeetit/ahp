// Package state is the public HTTP client SDK for the AHP State API.
//
// Wire contract: every call is POST {base}/v1/state/{Operation} with a strict
// JSON body. Isolation scope is derived server-side from the caller identity
// (mTLS certificate plus a bound task token), so no request body ever carries
// scope, tenant, subject or deployment fields. mTLS is supplied by the
// http.Client the caller passes to New; this package never configures TLS
// itself.
//
// Security properties enforced here:
//   - the base URL must be a bare HTTPS origin: no path, query, fragment or
//     userinfo;
//   - the caller's http.Client is cloned and redirects are refused
//     (http.ErrUseLastResponse), so a bearer token can never be forwarded to
//     a redirect destination;
//   - responses are bounded to 16 MiB and error values never embed the raw
//     response body or a bearer token;
//   - no automatic retries; checkpoint payload spill/hydration uses the
//     authenticated Artifacts owner API in addition to the State request.
package state

import (
	"encoding/json"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Run lifecycle statuses. They are part of the wire contract.
const (
	RunPending  = "pending"
	RunRunning  = "running"
	RunWaiting  = "waiting"
	RunDone     = "done"
	RunFailed   = "failed"
	RunCanceled = "canceled"
)

// IsTerminalStatus reports whether status ends the run's lifecycle.
func IsTerminalStatus(status string) bool {
	switch status {
	case RunDone, RunFailed, RunCanceled:
		return true
	default:
		return false
	}
}

// Thread is a durable execution lane owned by one subject inside one
// deployment. Epoch is the lease fence: it grows on every claim.
type Thread struct {
	ID        string          `json:"id"`
	ContextID string          `json:"context_id"`
	Scope     contracts.Scope `json:"scope"`
	Epoch     int64           `json:"epoch"`
	CreatedAt time.Time       `json:"created_at"`
}

// Run is one unit of work inside a thread. Runs are ordered by Sequence, which
// is dense and monotone per thread. WaitFor is non-empty only while the run is
// waiting for an external event.
type Run struct {
	TraceParent string          `json:"traceparent,omitempty"`
	ID          string          `json:"id"`
	ThreadID    string          `json:"thread_id"`
	TaskID      string          `json:"task_id"`
	Status      string          `json:"status"`
	WaitFor     string          `json:"wait_for,omitempty"`
	Sequence    int64           `json:"sequence"`
	Payload     json.RawMessage `json:"payload"`
	// Deadline is the optional wake-up bound of a waiting run.
	Deadline *time.Time `json:"deadline,omitempty"`
}

// Lease is the proof of exclusive execution rights over a thread for one run
// and one worker. Epoch must be presented on every lease operation.
type Lease struct {
	Thread    Thread          `json:"thread"`
	Run       Run             `json:"run"`
	Worker    string          `json:"worker"`
	Epoch     int64           `json:"epoch"`
	ExpiresAt time.Time       `json:"expires_at"`
	Scope     contracts.Scope `json:"scope"`
}

// Fence identifies the exact lease a lease operation must act on.
type Fence struct {
	RunID  string `json:"run_id"`
	Worker string `json:"worker"`
	Epoch  int64  `json:"epoch"`
}

// Claimed is the result of a successful ClaimRun: the granted lease plus the
// freshly issued task token to use on follow-up calls. The SDK never stores
// the token; the caller owns it.
type Claimed struct {
	Lease     Lease  `json:"lease"`
	TaskToken string `json:"task_token"`
}

// ---------------------------------------------------------------------------
// Request DTOs. None of them can carry scope: the server rejects unknown keys.
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
}

type getRunRequest struct {
	RunID string `json:"run_id"`
}

type listRunsRequest struct {
	ThreadID string `json:"thread_id"`
}

type listRunsResponse struct {
	Runs []Run `json:"runs"`
}

type claimRunRequest struct {
	RunID       string `json:"run_id,omitempty"`
	PayloadType string `json:"payload_type,omitempty"`
	Worker      string `json:"worker"`
	TTLms       int64  `json:"ttl_ms"`
}

// ClaimFilter selects eligible FIFO heads without bypassing preceding runs.
// Empty fields impose no restriction. Workload mTLS still determines the scope.
type ClaimFilter struct {
	RunID       string
	PayloadType string
}

type heartbeatRequest struct {
	RunID  string `json:"run_id"`
	Worker string `json:"worker"`
	Epoch  int64  `json:"epoch"`
	TTLms  int64  `json:"ttl_ms"`
}

type heartbeatResponse struct {
	ExpiresAt time.Time `json:"expires_at"`
}

type releaseRequest struct {
	RunID  string `json:"run_id"`
	Worker string `json:"worker"`
	Epoch  int64  `json:"epoch"`
	Status string `json:"status"`
	// CloseReason is the caller's own terminal verdict, in the execution plane's
	// vocabulary (model_finished / step_limit / canceled / error /
	// attempt_ceiling_exceeded / provider_transport). It is optional and the key is
	// absent when the caller states nothing: State stores what it is given and
	// invents nothing. Without this field a client on this public SDK could never
	// say why a run ended, and the run's terminal row would be silent forever.
	CloseReason string `json:"close_reason,omitempty"`
	// FailureCause is the closing plane's own sentence, stored verbatim under the run
	// payload's failure_cause key. It is optional and absent exactly like CloseReason,
	// so an existing sender's bytes do not move. It exists because since
	// DEF-RELEASE-REASON-1 the door requires one of the two keys on a failed or
	// canceled status: an SDK that could offer only the dictionary word would leave
	// callers holding a sentence no wire could carry.
	FailureCause string `json:"failure_cause,omitempty"`
}

type suspendRequest struct {
	RunID    string     `json:"run_id"`
	Worker   string     `json:"worker"`
	Epoch    int64      `json:"epoch"`
	WaitFor  string     `json:"wait_for"`
	Deadline *time.Time `json:"deadline,omitempty"`
}

type okResponse struct {
	OK bool `json:"ok"`
}

// validateTTL enforces the wire rule for ttl_ms: strictly positive and far
// enough from math.MaxInt64 that nanosecond conversion cannot overflow.
func validateTTL(ms int64) error {
	switch {
	case ms <= 0:
		return contracts.NewError(contracts.ErrInvalid, "state sdk: ttl_ms must be positive")
	case ms > maxTTLms:
		return contracts.NewError(contracts.ErrInvalid, "state sdk: ttl_ms overflows a nanosecond duration")
	}
	return nil
}

// maxTTLms is math.MaxInt64 / int64(time.Millisecond): above it,
// time.Duration(ms)*time.Millisecond would overflow.
const maxTTLms = int64(^uint64(0)>>1) / int64(time.Millisecond)

// validateFence checks the lease coordinates every lease operation needs.
func validateFence(f Fence) error {
	switch {
	case f.RunID == "":
		return contracts.NewError(contracts.ErrInvalid, "state sdk: fence run_id must not be empty")
	case f.Worker == "":
		return contracts.NewError(contracts.ErrInvalid, "state sdk: fence worker must not be empty")
	case f.Epoch <= 0:
		return contracts.NewError(contracts.ErrInvalid, "state sdk: fence epoch must be positive")
	}
	return nil
}

// newInvalid is a shorthand for an ErrInvalid within the SDK package.
func newInvalid(msg string) error {
	return contracts.NewError(contracts.ErrInvalid, "state sdk: "+msg)
}

type deleteThreadRequest struct {
	RunID    string `json:"run_id"`
	Worker   string `json:"worker"`
	Epoch    int64  `json:"epoch"`
	ThreadID string `json:"thread_id"`
}

// ListThreadsResponse is the paginated result of a ListThreads call.
type ListThreadsResponse struct {
	Threads    []Thread `json:"threads"`
	NextCursor string   `json:"next_cursor,omitempty"`
}

// fenceDTO is the nested fence object used in all fence-scoped State API requests.
// The real contract (DisallowUnknownFields) requires fence as a nested object,
// not flat run_id/worker/epoch at top level.
type fenceDTO struct {
	RunID  string `json:"run_id"`
	Worker string `json:"worker"`
	Epoch  int64  `json:"epoch"`
}

func toFenceDTO(f Fence) fenceDTO {
	return fenceDTO{RunID: f.RunID, Worker: f.Worker, Epoch: f.Epoch}
}
