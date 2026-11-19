package stateapi

import (
	"encoding/json"
	"net/http"

	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// ---------------------------------------------------------------------------
// Checkpoint handlers
// ---------------------------------------------------------------------------

type putCheckpointRequest struct {
	Fence state.Fence      `json:"fence"`
	CP    state.Checkpoint `json:"checkpoint"`
	Key   string           `json:"key"`
}

func (h *Handler) putCheckpoint(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in putCheckpointRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	cp, err := h.store.PutCheckpoint(r.Context(), scope, in.Fence, in.CP, in.Key)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, cp)
}

type getCheckpointRequest struct {
	ThreadID  string `json:"thread_id"`
	Namespace string `json:"namespace"`
	ID        string `json:"id"`
}

func (h *Handler) getCheckpoint(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in getCheckpointRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	cp, err := h.store.GetCheckpoint(r.Context(), scope, in.ThreadID, in.Namespace, in.ID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, cp)
}

type listCheckpointsRequest struct {
	ThreadID  string `json:"thread_id"`
	Namespace string `json:"namespace"`
	Limit     int    `json:"limit"`
}

func (h *Handler) listCheckpoints(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in listCheckpointsRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	cps, err := h.store.ListCheckpoints(r.Context(), scope, in.ThreadID, in.Namespace, in.Limit)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string][]state.Checkpoint{"checkpoints": cps})
}

type getPartRequest struct {
	ThreadID  string `json:"thread_id"`
	Namespace string `json:"namespace"`
	Channel   string `json:"channel"`
	Version   string `json:"version"`
}

func (h *Handler) getPart(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in getPartRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	data, err := h.store.GetPartEncoded(r.Context(), scope, in.ThreadID, in.Namespace, in.Channel, in.Version)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, data)
}

type putWritesRequest struct {
	Fence        state.Fence          `json:"fence"`
	Namespace    string               `json:"namespace"`
	CheckpointID string               `json:"checkpoint_id"`
	Writes       []state.PendingWrite `json:"writes"`
}

func (h *Handler) putWrites(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in putWritesRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.store.PutWrites(r.Context(), scope, in.Fence, in.Namespace, in.CheckpointID, in.Writes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

type getWritesRequest struct {
	ThreadID     string `json:"thread_id"`
	Namespace    string `json:"namespace"`
	CheckpointID string `json:"checkpoint_id"`
}

func (h *Handler) getWrites(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in getWritesRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	writes, err := h.store.GetWrites(r.Context(), scope, in.ThreadID, in.Namespace, in.CheckpointID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string][]state.PendingWrite{"writes": writes})
}

// ---------------------------------------------------------------------------
// KV handlers
// ---------------------------------------------------------------------------

type putKVRequest struct {
	Fence           state.Fence     `json:"fence"`
	Namespace       []string        `json:"namespace"`
	Key             string          `json:"key"`
	OperationKey    string          `json:"operation_key"`
	ExpectedVersion int64           `json:"expected_version"`
	Value           json.RawMessage `json:"value"`
}

func (h *Handler) putKV(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in putKVRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	item, err := h.store.PutKV(r.Context(), scope, in.Fence, in.Namespace, in.Key, in.OperationKey, in.ExpectedVersion, in.Value)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, item)
}

type deleteKVRequest struct {
	Fence           state.Fence `json:"fence"`
	Namespace       []string    `json:"namespace"`
	Key             string      `json:"key"`
	OperationKey    string      `json:"operation_key"`
	ExpectedVersion int64       `json:"expected_version"`
}

func (h *Handler) deleteKV(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in deleteKVRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.store.DeleteKV(r.Context(), scope, in.Fence, in.Namespace, in.Key, in.OperationKey, in.ExpectedVersion); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

type getKVRequest struct {
	Namespace []string `json:"namespace"`
	Key       string   `json:"key"`
}

func (h *Handler) getKV(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in getKVRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	item, err := h.store.GetKV(r.Context(), scope, in.Namespace, in.Key)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, item)
}

type searchKVRequest struct {
	NamespacePrefix []string        `json:"namespace_prefix"`
	KeyPrefix       string          `json:"key_prefix"`
	Filter          json.RawMessage `json:"filter,omitempty"`
	Limit           int             `json:"limit"`
}

func (h *Handler) searchKV(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in searchKVRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	items, err := h.store.SearchKV(r.Context(), scope, in.NamespacePrefix, in.KeyPrefix, in.Filter, in.Limit)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string][]state.KVItem{"items": items})
}

// ---------------------------------------------------------------------------
// Intent handlers
// ---------------------------------------------------------------------------

type recordIntentRequest struct {
	Fence  state.Fence     `json:"fence"`
	Node   string          `json:"node"`
	Call   string          `json:"call"`
	Kind   string          `json:"kind"`
	Target string          `json:"target"`
	Args   json.RawMessage `json:"args,omitempty"`
}

func (h *Handler) recordIntent(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in recordIntentRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	intent, err := h.store.RecordIntent(r.Context(), scope, in.Fence.RunID, in.Fence.Worker, in.Fence.Epoch, in.Node, in.Call, in.Kind, in.Target, in.Args)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, intent)
}

type updateIntentRequest struct {
	Fence  state.Fence     `json:"fence"`
	Node   string          `json:"node"`
	Call   string          `json:"call"`
	Status string          `json:"status"`
	Result json.RawMessage `json:"result,omitempty"`
}

func (h *Handler) updateIntent(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in updateIntentRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	intent, err := h.store.UpdateIntent(r.Context(), scope, in.Fence.RunID, in.Fence.Worker, in.Fence.Epoch, in.Node, in.Call, in.Status, in.Result)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, intent)
}

type reconcileIntentRequest struct {
	Fence  state.Fence     `json:"fence"`
	Node   string          `json:"node"`
	Call   string          `json:"call"`
	Status string          `json:"status"`
	Digest string          `json:"digest"`
	Result json.RawMessage `json:"result,omitempty"`
}

func (h *Handler) reconcileIntent(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in reconcileIntentRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	intent, err := h.store.ReconcileIntent(r.Context(), scope, in.Fence.RunID, in.Fence.Worker, in.Fence.Epoch, in.Node, in.Call, in.Status, in.Digest, in.Result)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, intent)
}

type getIntentRequest struct {
	RunID string `json:"run_id"`
	Node  string `json:"node"`
	Call  string `json:"call"`
}

func (h *Handler) getIntent(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in getIntentRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	intent, err := h.store.GetIntent(r.Context(), scope, in.RunID, in.Node, in.Call)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, intent)
}

// ---------------------------------------------------------------------------
// Event handlers
// ---------------------------------------------------------------------------

type enqueueRequest struct {
	ThreadID string          `json:"thread_id"`
	Key      string          `json:"key"`
	Kind     string          `json:"kind"`
	Payload  json.RawMessage `json:"payload"`
}

func (h *Handler) enqueue(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in enqueueRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	ev, err := h.store.Enqueue(r.Context(), scope, in.ThreadID, in.Key, in.Kind, in.Payload)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, ev)
}

type readInboxRequest struct {
	ThreadID string `json:"thread_id"`
	After    int64  `json:"after"`
	Limit    int    `json:"limit"`
}

func (h *Handler) readInbox(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in readInboxRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	events, err := h.store.ReadInbox(r.Context(), scope, in.ThreadID, in.After, in.Limit)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string][]state.InboxEvent{"events": events})
}

type ackInboxRequest struct {
	Fence state.Fence `json:"fence"`
	IDs   []string    `json:"ids"`
}

func (h *Handler) ackInbox(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in ackInboxRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.store.AckInbox(r.Context(), scope, in.Fence.RunID, in.Fence.Worker, in.Fence.Epoch, in.IDs); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

type appendStreamRequest struct {
	Fence   state.Fence     `json:"fence"`
	Key     string          `json:"key"`
	Kind    string          `json:"kind"`
	Payload json.RawMessage `json:"payload"`
}

func (h *Handler) appendStream(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in appendStreamRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	ev, err := h.store.AppendStream(r.Context(), scope, in.Fence.RunID, in.Fence.Worker, in.Fence.Epoch, in.Key, in.Kind, in.Payload)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, ev)
}

// flushStreamRequest carries only the lease fence: a boundary flush persists the
// run's volatile stream tail and takes no body content of its own.
type flushStreamRequest struct {
	Fence state.Fence `json:"fence"`
}

// flushStream persists the experimental per-run volatile stream tail at a step or
// turn boundary in one transaction. A no-op in the default chunk mode. A failed
// flush returns an error so the runtime never acknowledges a terminal past it.
func (h *Handler) flushStream(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in flushStreamRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	events, err := h.store.FlushStream(r.Context(), scope, in.Fence.RunID, in.Fence.Worker, in.Fence.Epoch)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if events == nil {
		events = []state.StreamEvent{}
	}
	transport.WriteJSON(w, http.StatusOK, map[string][]state.StreamEvent{"events": events})
}

type readStreamRequest struct {
	ThreadID         string `json:"thread_id"`
	After            int64  `json:"after"`
	Limit            int    `json:"limit"`
	MaxResponseBytes *int   `json:"max_response_bytes"`
	DurableOnly      bool   `json:"durable_only,omitempty"`
}

func (h *Handler) readStream(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in readStreamRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if in.MaxResponseBytes != nil {
		read := h.store.ReadStreamPage
		if in.DurableOnly {
			read = h.store.ReadDurableStreamPage
		}
		page, err := read(r.Context(), scope, in.ThreadID, in.After, in.Limit, *in.MaxResponseBytes)
		if err != nil {
			transport.WriteError(w, r, err)
			return
		}
		transport.WriteJSON(w, http.StatusOK, page)
		return
	}
	if in.DurableOnly {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "durable_only requires an explicit response byte budget"))
		return
	}
	events, err := h.store.ReadStream(r.Context(), scope, in.ThreadID, in.After, in.Limit)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string][]state.StreamEvent{"events": events})
}

// ---------------------------------------------------------------------------
// Batch handler
// ---------------------------------------------------------------------------

type putBatchRequest struct {
	Fence state.Fence        `json:"fence"`
	Batch state.BatchRequest `json:"batch"`
}

func (h *Handler) putBatch(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in putBatchRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	result, err := h.store.PutBatch(r.Context(), scope, in.Fence, in.Batch)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, result)
}

// ---------------------------------------------------------------------------
// Control-plane handlers
// ---------------------------------------------------------------------------

type deleteThreadRequest struct {
	ThreadID string `json:"thread_id"`
}

func (h *Handler) deleteThread(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in deleteThreadRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if h.deletion != nil {
		if err := h.deletion.Ready(r.Context(), scope, "DeleteThread"); err != nil {
			transport.WriteError(w, r, err)
			return
		}
	}
	if err := h.store.DeleteThread(r.Context(), scope, in.ThreadID); err != nil {
		if contracts.Code(err) != contracts.ErrNotFound {
			transport.WriteError(w, r, err)
			return
		}
		if _, receiptErr := h.store.GetDeletion(r.Context(), scope, "DeleteThread", in.ThreadID); receiptErr != nil {
			transport.WriteError(w, r, err)
			return
		}
	}
	if h.deletion != nil {
		if err := h.deletion.Complete(r.Context(), scope, "DeleteThread", in.ThreadID); err != nil {
			h.writeDeletionFailure(w, r, scope, "DeleteThread", in.ThreadID, err)
			return
		}
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

type deleteSubjectRequest struct {
	Key string `json:"key"`
}

func (h *Handler) deleteSubject(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in deleteSubjectRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if in.Key == "" {
		in.Key = "delete-subject"
	}
	if h.deletion != nil {
		if err := h.deletion.Ready(r.Context(), scope, "DeleteSubject"); err != nil {
			transport.WriteError(w, r, err)
			return
		}
	}
	deleted, err := h.store.DeleteSubjectWithKey(r.Context(), scope, in.Key)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if h.deletion != nil {
		if err := h.deletion.Complete(r.Context(), scope, "DeleteSubject", in.Key); err != nil {
			h.writeDeletionFailure(w, r, scope, "DeleteSubject", in.Key, err)
			return
		}
	}
	transport.WriteJSON(w, http.StatusOK, map[string]int64{"deleted": deleted})
}

type setLegalholdRequest struct {
	ThreadID string `json:"thread_id"`
	Reason   string `json:"reason"`
}

func (h *Handler) setLegalhold(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in setLegalholdRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.store.SetLegalhold(r.Context(), scope, in.ThreadID, in.Reason); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

type removeLegalholdRequest struct {
	ThreadID string `json:"thread_id"`
}

func (h *Handler) removeLegalhold(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in removeLegalholdRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.store.RemoveLegalhold(r.Context(), scope, in.ThreadID); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

type exportThreadRequest struct {
	ThreadID string `json:"thread_id"`
}

func (h *Handler) exportThread(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in exportThreadRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	exp, err := h.store.ExportThread(r.Context(), scope, in.ThreadID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, exp)
}

type importThreadRequest struct {
	Export state.ControlExport `json:"export"`
	// ContextID retargets the import. The exported context_id belongs to the
	// scope that produced the bytes, so restoring into a context the tenant
	// already owns is only possible by naming a new one here; the thread id is
	// always freshly allocated. Empty means "keep the exported context_id",
	// which works only while nothing in the tenant owns it.
	ContextID string `json:"context_id,omitempty"`
}

func (h *Handler) importThread(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in importThreadRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	th, err := h.store.ImportThread(r.Context(), scope, in.Export, in.ContextID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, th)
}
