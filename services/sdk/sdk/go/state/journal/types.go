package journal

import (
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts/journal"
)

type (
	Payload                = journal.Payload
	BlobRef                = journal.BlobRef
	EventInput             = journal.EventInput
	KVMutation             = journal.KVMutation
	PreparedIntent         = journal.PreparedIntent
	SHA256Digest           = journal.SHA256Digest
	AppendRequest          = journal.AppendRequest
	PointerUpdate          = journal.PointerUpdate
	AppendReceipt          = journal.AppendReceipt
	Event                  = journal.Event
	EventPatch             = journal.EventPatch
	EventPage              = journal.EventPage
	PendingWrite           = journal.PendingWrite
	Checkpoint             = journal.Checkpoint
	CheckpointCommit       = journal.CheckpointCommit
	CheckpointTuple        = journal.CheckpointTuple
	IntentTransition       = journal.IntentTransition
	ToolIntent             = journal.ToolIntent
	StandaloneToolRequest  = journal.StandaloneToolRequest
	StandaloneToolDecision = journal.StandaloneToolDecision
	EnqueueRequest         = journal.EnqueueRequest
	EnqueueReceipt         = journal.EnqueueReceipt
	TurnLease              = journal.TurnLease
	TurnStatus             = journal.TurnStatus
	Lease                  = journal.Lease
	SessionPage            = journal.SessionPage
	ResumeState            = journal.ResumeState
	KVEntry                = journal.KVEntry
	ChatMessage            = journal.ChatMessage
	ChatDelta              = journal.ChatDelta
	ChatHistory            = journal.ChatHistory
	OpenClawCatalogUpdate  = journal.OpenClawCatalogUpdate
	CatalogPage            = journal.CatalogPage
	CatalogSession         = journal.CatalogSession
)

// CatalogQuery filters a page of harness-native session cards.
type CatalogQuery struct {
	Harness, AppID, AgentID, UserID, Status, Search, Cursor string
	Label, SpawnedBy                                        string
	AgentIDs                                                []string
	UpdatedAfter                                            *time.Time
	Limit, Offset                                           int
}
