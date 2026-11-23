package journal

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

const (
	ExecutionPull      = "worker_pull"
	ExecutionVMManaged = "vm_managed"
	DeliveryTurn       = "turn"
	DeliverySteer      = "steer"
	DeliveryFollowup   = "followup"
	MaxFollowups       = 16
)

type VMTurnClaimRequest struct {
	TurnSeq         int64  `json:"turn_seq"`
	Generation      int64  `json:"generation"`
	ExecutionID     string `json:"execution_id"`
	TTLMilliseconds int64  `json:"ttl_ms"`
}

type FenceRequeueRequest struct {
	TurnSeq     int64  `json:"turn_seq"`
	Epoch       int64  `json:"lease_epoch"`
	ExecutionID string `json:"execution_id"`
}

type VMSession struct {
	Scope       contracts.Scope `json:"scope"`
	SessionID   string          `json:"session_id"`
	TurnSeq     int64           `json:"turn_seq"`
	Epoch       int64           `json:"lease_epoch"`
	ExecutionID string          `json:"execution_id,omitempty"`
	Status      string          `json:"status"`
	Harness     string          `json:"harness_type"`
}

type SteerAck struct {
	TurnSeq int64 `json:"turn_seq"`
	UpToSeq int64 `json:"up_to_seq"`
	Count   int   `json:"count"`
}

type TurnControlDelta struct {
	TurnSeq         int64      `json:"turn_seq,omitempty"`
	CancelRequested bool       `json:"cancel_requested,omitempty"`
	PendingSteer    []Event    `json:"pending_steer,omitempty"`
	NextTurn        *TurnLease `json:"next_turn,omitempty"`
}

type TurnCompleted struct {
	TurnSeq             int64   `json:"turn_seq"`
	Outcome             string  `json:"outcome"`
	EndSeq              int64   `json:"end_seq"`
	NextTurnSeq         int64   `json:"next_turn_seq,omitempty"`
	CanceledControlSeqs []int64 `json:"canceled_control_seqs,omitempty"`
}

type CheckpointReceipt struct {
	ID      string            `json:"checkpoint_id"`
	Control *TurnControlDelta `json:"control,omitempty"`
}

// ScopedSessionID maps a public conversation identifier to a VM journal key.
func ScopedSessionID(scope contracts.Scope, conversationID string) string {
	raw := scope.Tenant + "\x00" + scope.Deployment + "\x00" + scope.Subject + "\x00" + conversationID
	sum := sha256.Sum256([]byte(raw))
	return "vms-" + hex.EncodeToString(sum[:16])
}

func DeterministicExecutionID(scope contracts.Scope, session string, turnSeq, generation int64) string {
	raw := fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%d\x00%d",
		scope.Tenant, scope.Deployment, scope.Subject, session, turnSeq, generation)
	sum := sha256.Sum256([]byte(raw))
	return "exec-" + hex.EncodeToString(sum[:12])
}

// ValidExecutionID checks the canonical shape before a physical execution or
// a grant fence can be addressed by an ID.
func ValidExecutionID(id string) bool {
	if len(id) != len("exec-")+24 || id[:len("exec-")] != "exec-" {
		return false
	}
	for _, c := range id[len("exec-"):] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

const SessionSignalChannel = "ahp_rc_sig"

type SessionSignal struct {
	Scope     contracts.Scope `json:"scope"`
	SessionID string          `json:"session_id"`
}

// VMReconcileCursor is a position in one status queue. TenantID is used for global paging.
type VMReconcileCursor struct {
	At        time.Time `json:"at"`
	TenantID  string    `json:"tenant_id,omitempty"`
	SessionID string    `json:"session_id"`
}

type VMReconcilePage struct {
	Sessions []VMSession        `json:"sessions"`
	Next     *VMReconcileCursor `json:"next,omitempty"`
}
