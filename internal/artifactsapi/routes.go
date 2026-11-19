package artifactsapi

import (
	"net/http"
	"time"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
	"github.com/ahp-platform/ahp/pkg/transport"
)

type idRequest struct {
	ID string `json:"id"`
}

type beginUploadRequest struct {
	Key          string     `json:"key"`
	Name         string     `json:"name"`
	MIME         string     `json:"mime"`
	SHA256       string     `json:"sha256"`
	Size         int64      `json:"size"`
	Labels       labels.Set `json:"labels"`
	JournalStage bool       `json:"journal_stage,omitempty"`
}

type deleteRequest struct {
	ID  string `json:"id"`
	Key string `json:"key"`
}

type setRetentionRequest struct {
	ID          string     `json:"id"`
	RetainUntil *time.Time `json:"retain_until,omitempty"`
}

type setLegalHoldRequest struct {
	ID string `json:"id"`
	On bool   `json:"on"`
}

type markDerivationRequest struct {
	ID string `json:"id"`
	artifacts.Derivation
}

type checkPublicationRequest struct {
	IDs  []string        `json:"ids"`
	Sink labels.Audience `json:"sink"`
}

type publishRequest struct {
	ID        string `json:"id"`
	Workspace string `json:"workspace"`
}

type deleteByContextRequest struct {
	ContextID string `json:"context_id"`
}

type deleteByTasksRequest struct {
	TaskIDs []string `json:"task_ids"`
}

// claimsOf returns the verified token claims of this request. The transport
// middleware installed them; a request without them reaches no handler at all.
func claimsOf(r *http.Request) (auth.Claims, error) {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		return auth.Claims{}, contracts.NewError(contracts.ErrForbidden,
			"artifactsapi: no verified principal on this request")
	}
	return p.Claims, nil
}

func (h *Handler) beginUpload(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in beginUploadRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	// The binding comes from the verified token, never from the body: the
	// context and task the caller was authorised under are what a later thread
	// or subject cascade selects on.
	pc, err := claimsOf(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	up, err := h.store.BeginUploadWithBinding(r.Context(), scope, artifacts.Binding{
		ContextID: pc.Context, TaskID: pc.Task,
	}, in.Key, artifacts.UploadRequest{
		Name: in.Name, MIME: in.MIME, SHA256: in.SHA256, Size: in.Size, Labels: in.Labels, JournalStage: in.JournalStage,
	})
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, up)
}

func (h *Handler) seal(w http.ResponseWriter, r *http.Request) {
	ctx, scope, err := readRequest(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in idRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	ref, err := h.store.Seal(ctx, scope, in.ID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, ref)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	ctx, scope, err := readRequest(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in idRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	ref, err := h.store.Get(ctx, scope, in.ID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, ref)
}

func (h *Handler) delete(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in deleteRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.store.Delete(r.Context(), scope, in.ID, in.Key); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

func (h *Handler) purge(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in idRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.store.Purge(r.Context(), scope, in.ID); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

func (h *Handler) setRetention(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in setRetentionRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.store.SetRetention(r.Context(), scope, in.ID, in.RetainUntil); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

func (h *Handler) setLegalHold(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in setLegalHoldRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.store.SetLegalHold(r.Context(), scope, in.ID, in.On); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

func (h *Handler) markDerivation(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in markDerivationRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.store.MarkDerivation(r.Context(), scope, in.ID, in.Derivation); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

func (h *Handler) readiness(w http.ResponseWriter, r *http.Request) {
	ctx, scope, err := readRequest(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in idRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	rd, err := h.store.Readiness(ctx, scope, in.ID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, rd)
}

func (h *Handler) getLifecycle(w http.ResponseWriter, r *http.Request) {
	ctx, scope, err := readRequest(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in idRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	lc, err := h.store.GetLifecycle(ctx, scope, in.ID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, lc)
}

func (h *Handler) checkPublication(w http.ResponseWriter, r *http.Request) {
	ctx, scope, err := readRequest(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in checkPublicationRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.store.CheckPublication(ctx, scope, in.IDs, in.Sink); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

func (h *Handler) publish(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in publishRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.store.PublishToWorkspace(r.Context(), scope, in.ID, in.Workspace); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

type contextGrantRequest struct {
	ID            string `json:"id"`
	TargetContext string `json:"target_context"`
	OpKey         string `json:"op_key"`
}

// grantContext widens one artifact of the caller's OWN subject to one more of
// the subject's conversations, and only when the caller IS the conversation the
// artifact belongs to. The store owns every check; the handler only carries the
// verified principal and the verified context claim (installed by the face).
func (h *Handler) grantContext(w http.ResponseWriter, r *http.Request) {
	ctx, scope, err := readRequest(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in contextGrantRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.store.GrantContext(ctx, scope, in.ID, in.TargetContext, in.OpKey); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

func (h *Handler) revokeContext(w http.ResponseWriter, r *http.Request) {
	ctx, scope, err := readRequest(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in contextGrantRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.store.RevokeContext(ctx, scope, in.ID, in.TargetContext, in.OpKey); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

// getContextGrant is the operator's view of one durable grant: which context the
// bytes came from, which conversation was admitted, and by which key.
func (h *Handler) getContextGrant(w http.ResponseWriter, r *http.Request) {
	ctx, scope, err := readRequest(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in contextGrantRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	grant, err := h.store.GetContextGrant(ctx, scope, in.ID, in.TargetContext)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, grant)
}

// deleteByContext is the control-plane cascade entry point: it tombstones every
// artifact of the caller's own scope written under context_id. A body never
// carries scope, and a caller can only ever reach its own scope.
func (h *Handler) deleteByContext(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in deleteByContextRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	res, err := h.store.DeleteByContext(r.Context(), scope, in.ContextID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, res)
}

// deleteByTasks cascades over the tasks the control plane says belong to the
// deleted thread, reaching artifacts whose writer recorded that task.
func (h *Handler) deleteByTasks(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in deleteByTasksRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	res, err := h.store.DeleteByTasks(r.Context(), scope, in.TaskIDs)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, res)
}

// deleteBySubject is the artifact half of State's DeleteSubject.
func (h *Handler) deleteBySubject(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in struct{}
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	res, err := h.store.DeleteBySubject(r.Context(), scope)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, res)
}

// ---------------------------------------------------------------------------
// Storage GC, lifecycle restore and live-reference pins
//
// These are the control-plane operations of the deletion plane: the sweep that
// applies retention, the un-delete inside the retention window, the registry of
// references that keeps a collector off live work, and the audit trail that says
// what was taken and what was refused. The PDF puts them on the admin side of
// the line ("DeleteSubject, retention, legal hold, Export - только control plane
// AHP", с. 34), so the store consults the current authorization decision for
// each one ("gc", "restore", "pin") and a task token that the policy does not
// carry those actions is refused.
// ---------------------------------------------------------------------------

type gcRequest struct {
	Key                     string `json:"key,omitempty"`
	StagedGraceSeconds      *int64 `json:"staged_grace_seconds,omitempty"`
	TombstoneRetentionSecs  *int64 `json:"tombstone_retention_seconds,omitempty"`
	OrphanGraceSeconds      *int64 `json:"orphan_grace_seconds,omitempty"`
	BatchSize               int    `json:"batch_size,omitempty"`
	MaxRows                 int    `json:"max_rows,omitempty"`
	CollectUnpinnedVersions bool   `json:"collect_unpinned_versions,omitempty"`
	DryRun                  bool   `json:"dry_run,omitempty"`
}

// gcResponseSeconds guards against a caller asking a collector to delete
// anything younger than a minute: a negative or tiny floor is how a running
// journey loses its artifacts.
func gcDurationField(field string, secs *int64) (time.Duration, error) {
	if secs == nil {
		return 0, nil
	}
	if *secs < 60 && *secs != 0 {
		return 0, contracts.NewError(contracts.ErrInvalid,
			"artifactsapi: "+field+" must be 0 (default) or at least 60")
	}
	return time.Duration(*secs) * time.Second, nil
}

func (h *Handler) gcRun(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in gcRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	staged, err := gcDurationField("staged_grace_seconds", in.StagedGraceSeconds)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	tombstone, err := gcDurationField("tombstone_retention_seconds", in.TombstoneRetentionSecs)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	orphan, err := gcDurationField("orphan_grace_seconds", in.OrphanGraceSeconds)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	// The swept tenant is the caller's own, taken from the verified token and
	// nowhere else: a collector that could be pointed at another tenant would
	// turn a retention job into a cross-tenant delete.
	report, err := h.gc.RunOnce(r.Context(), scope, artifacts.GCPolicy{
		Key:                     in.Key,
		StagedGrace:             staged,
		TombstoneRetention:      tombstone,
		OrphanGrace:             orphan,
		BatchSize:               in.BatchSize,
		MaxRows:                 in.MaxRows,
		CollectUnpinnedVersions: in.CollectUnpinnedVersions,
		DryRun:                  in.DryRun,
	})
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, report)
}

func (h *Handler) restore(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in idRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	ref, err := h.store.Restore(r.Context(), scope, in.ID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, ref)
}

type pinRequest struct {
	Kind       string `json:"kind"`
	RefID      string `json:"ref_id"`
	ArtifactID string `json:"artifact_id,omitempty"`
}

func (h *Handler) pinRef(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in pinRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if in.ArtifactID != "" {
		err = h.store.PinArtifact(r.Context(), scope, in.ArtifactID, in.Kind, in.RefID)
	} else {
		err = h.store.PinRef(r.Context(), scope, in.Kind, in.RefID)
	}
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

func (h *Handler) unpinRef(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in pinRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if in.ArtifactID != "" {
		err = h.store.UnpinArtifact(r.Context(), scope, in.ArtifactID, in.Kind, in.RefID)
	} else {
		err = h.store.UnpinRef(r.Context(), scope, in.Kind, in.RefID)
	}
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

func (h *Handler) listPins(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in pinRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	pins, err := h.store.ListPins(r.Context(), scope, in.Kind, in.RefID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"pins": pins})
}

type gcAuditRequest struct {
	Event      string `json:"event,omitempty"`
	ArtifactID string `json:"artifact_id,omitempty"`
	SweepID    string `json:"sweep_id,omitempty"`
	Limit      int    `json:"limit,omitempty"`
}

func (h *Handler) gcAudit(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in gcAuditRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	events, err := h.store.GCAudit(r.Context(), scope, artifacts.GCAuditFilter{
		Event: in.Event, ArtifactID: in.ArtifactID, SweepID: in.SweepID, Limit: in.Limit,
	})
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"events": events})
}
