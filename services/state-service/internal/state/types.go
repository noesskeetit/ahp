// Package state implements the durable State API core: threads that carry a
// FIFO sequence of runs, and a single row-level lease per thread guarded by a
// monotonically increasing epoch.
//
// Invariants enforced here (and relied upon by every upper layer):
//
//   - Scope (tenant, deployment, subject) filters every normal operation.
//     Foreign resources are reported as NOT_FOUND, never as forbidden, so the
//     API cannot be used to probe other tenants.
//   - ClaimRun is deployment-level: it only ever considers runs inside the
//     caller's own tenant/deployment pair. Subject is not a claim filter.
//   - Exactly one unexpired lease may exist per thread. Every claim or reclaim
//     increments the thread epoch, so any older lease token is rejected
//     atomically with STALE_EPOCH.
//   - All lease timing uses the PostgreSQL clock (clock_timestamp()); the
//     application never compares lease times with its own clock.
//   - Runs execute strictly in sequence order: the earliest unfinished run of
//     a thread is the only claimable candidate, and a waiting run blocks every
//     later run until an inbox event or its deadline requeues it.
//   - Thread and run creation is idempotent per (scope, key) and (thread, key)
//     respectively; replaying the identical body returns the original record
//     and a different body under the same key fails with KEY_REUSE_MISMATCH.
//
// This package owns the ahp_state schema exclusively and never touches SQL
// belonging to other services.
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
	// Deadline is the optional wake-up bound of a waiting run. Once the
	// database clock passes it, the run becomes claimable again.
	Deadline *time.Time `json:"deadline,omitempty"`
}

// Lease is the proof of exclusive execution rights over a thread for one run
// and one worker. Epoch must be presented on every lease operation; ExpiresAt
// is derived purely from the database clock.
type Lease struct {
	Thread    Thread          `json:"thread"`
	Run       Run             `json:"run"`
	Worker    string          `json:"worker"`
	Epoch     int64           `json:"epoch"`
	ExpiresAt time.Time       `json:"expires_at"`
	Scope     contracts.Scope `json:"scope"`
}
