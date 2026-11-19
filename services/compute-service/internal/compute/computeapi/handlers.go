package computeapi

import (
	"context"
	"encoding/base64"
	"net/http"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// decode reads a strict JSON request body; malformed input surfaces as the
// transport's INVALID_ARGUMENT (400).
func decode(w http.ResponseWriter, r *http.Request, out any) bool {
	if err := transport.ReadJSON(w, r, out, maxBodyBytes); err != nil {
		writeError(w, r, err)
		return false
	}
	return true
}

// leaseTTL converts a requested ttl_ms (0 => default) into a duration; the
// store re-validates the 1s..12h window.
func leaseTTL(ms int64) (time.Duration, error) {
	if ms == 0 {
		return defaultLeaseTTL, nil
	}
	if ms < 0 {
		return 0, contracts.NewError(contracts.ErrInvalid, "ttl_ms must not be negative")
	}
	return time.Duration(ms) * time.Millisecond, nil
}

func (h *Handler) createComputer(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in createComputerRequest
	if !decode(w, r, &in) {
		return
	}
	c, err := h.store.CreateComputer(r.Context(), scope, compute.NewComputerRequest{
		Class:       compute.Class(in.Class),
		Profile:     compute.Profile(in.Profile),
		WorkspaceID: in.WorkspaceID,
		Realm:       in.Realm,
		Quota:       in.Quota.toQuota(),
	}, in.Key)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, computerFrom(c))
}

func (h *Handler) getComputer(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in computerRefRequest
	if !decode(w, r, &in) {
		return
	}
	c, err := h.store.GetComputer(r.Context(), scope, in.ComputerID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, computerFrom(c))
}

func (h *Handler) acquireIncarnation(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in acquireIncarnationRequest
	if !decode(w, r, &in) {
		return
	}
	inc, err := h.store.AcquireIncarnation(r.Context(), scope, in.ComputerID, in.Backend, in.Node, in.VMGenID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, incarnationFrom(inc))
}

func (h *Handler) stopIncarnation(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in stopIncarnationRequest
	if !decode(w, r, &in) {
		return
	}
	if err := h.store.StopIncarnation(r.Context(), scope, in.ComputerID, in.IncarnationID); err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

func (h *Handler) destroy(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in computerRefRequest
	if !decode(w, r, &in) {
		return
	}
	c, err := h.store.SetComputerStatus(r.Context(), scope, in.ComputerID, compute.ComputerDestroyed)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, computerFrom(c))
}

func (h *Handler) exec(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in execRequest
	if !decode(w, r, &in) {
		return
	}
	rec, mayDispatch, err := h.store.Exec(r.Context(), scope, in.ComputerID, in.Key, in.Command, in.Cwd)
	if err != nil {
		writeError(w, r, err)
		return
	}
	reason := ""
	// What may_dispatch is allowed to claim depends on who is answering, and the two cases
	// must not be conflated - an earlier form of this check got them wrong and broke a
	// mounted-plane SDK contract that depends on the second one:
	//
	//   - this plane HAS a runner, so it is the plane that would execute the command. Then
	//     may_dispatch is a capability claim about this plane, and claiming it for a class
	//     whose backend implements no execution door - the MOCK backend, exactly - is the
	//     overclaim DEF-COMPUTE-5 measured: the caller is sent off to do something that can
	//     only fail later, while the ledger reads as though somebody is working.
	//   - this plane has NO runner. Then it is a ledger front and some worker elsewhere
	//     claims the row; nothing this plane knows can invalidate "a fresh accepted row is
	//     waiting to be dispatched", and answering false here would strand real work on a
	//     deployment that is configured correctly. It stays what the store said.
	//
	// Either way the record itself is untouched: refusing the promise is not refusing the
	// record.
	if mayDispatch && h.runner != nil {
		if ok, why := h.runner.DispatchCapability(r.Context(), scope, in.ComputerID); !ok {
			mayDispatch = false
			reason = why
		}
	}
	transport.WriteJSON(w, http.StatusOK, execResponse{Command: commandFrom(rec), MayDispatch: mayDispatch, DispatchReason: reason})
}

func (h *Handler) run(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if h.runner == nil {
		writeError(w, r, contracts.NewError(contracts.ErrForbidden,
			"computeapi: execution runner is not configured on this host"))
		return
	}
	var in execRequest
	if !decode(w, r, &in) {
		return
	}
	rec, err := h.runner.RunCommand(r.Context(), scope, in.ComputerID, in.Key, in.Command, in.Cwd)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, commandFrom(rec))
}

func (h *Handler) markDispatched(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in commandRefRequest
	if !decode(w, r, &in) {
		return
	}
	rec, err := h.store.MarkDispatched(r.Context(), scope, in.ComputerID, in.CommandID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, commandFrom(rec))
}

func (h *Handler) complete(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in completeRequest
	if !decode(w, r, &in) {
		return
	}
	rec, err := h.store.CompleteWithOutput(r.Context(), scope, in.ComputerID, in.CommandID,
		in.ExitCode, in.Stdout, in.Stderr, in.OutputTruncated)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, commandFrom(rec))
}

func (h *Handler) getCommand(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in commandRefRequest
	if !decode(w, r, &in) {
		return
	}
	rec, err := h.store.GetCommand(r.Context(), scope, in.ComputerID, in.CommandID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, commandFrom(rec))
}

func (h *Handler) fence(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in commandRefRequest
	if !decode(w, r, &in) {
		return
	}
	rec, err := h.store.Fence(r.Context(), scope, in.ComputerID, in.CommandID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, commandFrom(rec))
}

func (h *Handler) cancel(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in commandRefRequest
	if !decode(w, r, &in) {
		return
	}
	rec, phase, err := h.store.Cancel(r.Context(), scope, in.ComputerID, in.CommandID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, cancelResponse{Command: commandFrom(rec), CancelPhase: string(phase)})
}

func (h *Handler) restart(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in commandRefRequest
	if !decode(w, r, &in) {
		return
	}
	rec, err := h.store.Restart(r.Context(), scope, in.ComputerID, in.CommandID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, commandFrom(rec))
}

func (h *Handler) commitWorkspace(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in commitWorkspaceRequest
	if !decode(w, r, &in) {
		return
	}
	rev, err := h.store.CommitWorkspace(r.Context(), scope, in.ComputerID, in.ExpectedHead, in.Digest)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, workspaceFrom(rev))
}

func (h *Handler) workspaceHead(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in computerRefRequest
	if !decode(w, r, &in) {
		return
	}
	rev, err := h.store.WorkspaceHead(r.Context(), scope, in.ComputerID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, workspaceFrom(rev))
}

func (h *Handler) ensureEnvironment(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in ensureEnvironmentRequest
	if !decode(w, r, &in) {
		return
	}
	rev, err := h.store.EnsureEnvironment(r.Context(), scope, in.ComputerID, in.Spec)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, envFrom(rev))
}

func (h *Handler) beginInstall(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in beginInstallRequest
	if !decode(w, r, &in) {
		return
	}
	if h.runner != nil && h.runner.LeaseService() != nil {
		// Full environment operation: durable revision + Redis writer lease
		// + real throwaway prefix materialised from the applied environment.
		rev, envDir, err := h.runner.BeginEnvironmentInstall(r.Context(), scope, in.ComputerID, in.Spec, in.Holder)
		if err != nil {
			writeError(w, r, err)
			return
		}
		transport.WriteJSON(w, http.StatusOK, beginInstallResponse{Revision: envFrom(rev), EnvDir: envDir})
		return
	}
	rev, err := h.store.BeginEnvironmentInstall(r.Context(), scope, in.ComputerID, in.Spec, in.Holder)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, envFrom(rev))
}

func (h *Handler) finishInstall(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in finishInstallRequest
	if !decode(w, r, &in) {
		return
	}
	if h.runner != nil && h.runner.LeaseService() != nil {
		// Settle with the real filesystem outcome: applied cursor move on
		// success, throwaway prefix deletion on failure (rollback).
		rev, err := h.runner.FinishEnvironmentInstall(r.Context(), scope, in.ComputerID, in.RevisionID, in.Holder, in.OK, in.ErrorMsg)
		if err != nil {
			writeError(w, r, err)
			return
		}
		transport.WriteJSON(w, http.StatusOK, envFrom(rev))
		return
	}
	rev, err := h.store.FinishEnvironmentInstall(r.Context(), scope, in.ComputerID, in.RevisionID, in.Holder, in.OK, in.ErrorMsg)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, envFrom(rev))
}

func (h *Handler) currentEnvironment(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in computerRefRequest
	if !decode(w, r, &in) {
		return
	}
	rev, err := h.store.CurrentEnvironment(r.Context(), scope, in.ComputerID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, envFrom(rev))
}

func (h *Handler) snapshot(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in snapshotRequest
	if !decode(w, r, &in) {
		return
	}
	s, err := h.store.RecordSnapshot(r.Context(), scope, in.ComputerID, compute.SnapshotKind(in.Kind), in.Path, in.Digest, in.SizeBytes)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, snapshotFrom(s))
}

func (h *Handler) export(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if h.runner == nil {
		writeError(w, r, contracts.NewError(contracts.ErrForbidden,
			"computeapi: export runner is not configured on this host"))
		return
	}
	var in exportRequest
	if !decode(w, r, &in) {
		return
	}
	p, ok := transport.FromContext(r.Context())
	if !ok {
		writeError(w, r, contracts.NewError(contracts.ErrForbidden, "verified export principal required"))
		return
	}
	check := func(ctx context.Context) error {
		if p.Claims.ExpiresAt <= time.Now().Unix() {
			return contracts.NewError(contracts.ErrForbidden, "export task expired")
		}
		if err := h.authorize(ctx, p.Claims, capability); err != nil {
			return err
		}
		return h.authorize(ctx, p.Claims, "artifacts")
	}
	ref, err := h.runner.ExportPath(r.Context(), scope, compute.ExportOptions{TaskID: p.Claims.Task, ContextID: p.Claims.Context, Key: in.Key, Token: strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), Check: check}, in.ComputerID, in.Path)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, artifactFrom(ref))
}

func (h *Handler) acquireLease(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in acquireLeaseRequest
	if !decode(w, r, &in) {
		return
	}
	ttl, err := leaseTTL(in.TTLms)
	if err != nil {
		writeError(w, r, err)
		return
	}
	id, err := h.store.AcquireLease(r.Context(), scope, in.ComputerID, compute.ActivityLeaseKind(in.Kind), in.Holder, ttl)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, leaseResponse{LeaseID: id})
}

func (h *Handler) releaseLease(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in releaseLeaseRequest
	if !decode(w, r, &in) {
		return
	}
	if err := h.store.ReleaseLease(r.Context(), scope, in.ComputerID, compute.ActivityLeaseKind(in.Kind), in.Holder); err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

func (h *Handler) currentFence(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in computerRefRequest
	if !decode(w, r, &in) {
		return
	}
	f, err := h.store.CurrentFence(r.Context(), scope, in.ComputerID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, fenceFrom(f))
}

// ---------------------------------------------------------------------------
// Computer state (PDF 3.6: state as revisions; typed patch; derivative branch)
// ---------------------------------------------------------------------------

func (h *Handler) typedPatch(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if h.runner == nil {
		writeError(w, r, contracts.NewError(contracts.ErrForbidden,
			"computeapi: execution runner is not configured on this host"))
		return
	}
	var in typedPatchRequest
	if !decode(w, r, &in) {
		return
	}
	content, err := base64.StdEncoding.DecodeString(in.ContentB64)
	if err != nil {
		writeError(w, r, contracts.NewError(contracts.ErrInvalid, "content_b64 is not base64"))
		return
	}
	rev, err := h.runner.TypedPatch(r.Context(), scope, in.ComputerID, in.Branch, in.Path, in.ExpectedHash, content, in.Mode)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, stateRevisionFrom(rev))
}

func (h *Handler) stateHead(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in branchRequest
	if !decode(w, r, &in) {
		return
	}
	rev, err := h.store.StateHead(r.Context(), scope, in.ComputerID, in.Branch)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, stateRevisionFrom(rev))
}

func (h *Handler) stateRevisions(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in branchRequest
	if !decode(w, r, &in) {
		return
	}
	revs, err := h.store.ListStateRevisions(r.Context(), scope, in.ComputerID, in.Branch)
	if err != nil {
		writeError(w, r, err)
		return
	}
	out := make([]stateRevisionDTO, 0, len(revs))
	for _, rv := range revs {
		out = append(out, stateRevisionFrom(rv))
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"revisions": out})
}

func (h *Handler) deriveBranch(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in branchRequest
	if !decode(w, r, &in) {
		return
	}
	rev, err := h.runner.DeriveBranch(r.Context(), scope, in.ComputerID, in.Name)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, stateRevisionFrom(rev))
}

func (h *Handler) mergeBranch(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in branchRequest
	if err != nil {
		writeError(w, r, err)
		return
	}
	if !decode(w, r, &in) {
		return
	}
	rev, err := h.runner.MergeBranch(r.Context(), scope, in.ComputerID, in.Name)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, stateRevisionFrom(rev))
}

// ---------------------------------------------------------------------------
// Sleep / wake / cost (PDF 3.6: activity leases, cost, no surviving processes)
// ---------------------------------------------------------------------------

func (h *Handler) sleep(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if h.runner == nil {
		writeError(w, r, contracts.NewError(contracts.ErrForbidden,
			"computeapi: execution runner is not configured on this host"))
		return
	}
	var in computerRefRequest
	if !decode(w, r, &in) {
		return
	}
	rep, err := h.runner.Sleep(r.Context(), scope, in.ComputerID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, sleepReportFrom(rep))
}

func (h *Handler) wake(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if h.runner == nil {
		writeError(w, r, contracts.NewError(contracts.ErrForbidden,
			"computeapi: execution runner is not configured on this host"))
		return
	}
	var in computerRefRequest
	if !decode(w, r, &in) {
		return
	}
	inc, err := h.runner.Wake(r.Context(), scope, in.ComputerID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, incarnationFrom(inc))
}

func (h *Handler) costReport(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in computerRefRequest
	if !decode(w, r, &in) {
		return
	}
	rep, err := h.store.CostReport(r.Context(), scope, in.ComputerID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, costReportFrom(rep))
}

// ---------------------------------------------------------------------------
// Redis lease layer (PDF 3.6 B50 writer lease, B52 activity leases)
// ---------------------------------------------------------------------------

func (h *Handler) leaseService() (*compute.LeaseService, error) {
	if h.runner == nil {
		return nil, contracts.NewError(contracts.ErrForbidden,
			"computeapi: lease service is not configured on this host")
	}
	if ls := h.runner.LeaseService(); ls != nil {
		return ls, nil
	}
	return nil, contracts.NewError(contracts.ErrForbidden,
		"computeapi: lease service is not configured on this host")
}

func (h *Handler) takeWriterLease(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	ls, err := h.leaseService()
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in writerLeaseRequest
	if !decode(w, r, &in) {
		return
	}
	ttl, err := leaseTTL(in.TTLms)
	if err != nil {
		writeError(w, r, err)
		return
	}
	id, err := ls.AcquireWriter(r.Context(), scope, in.ComputerID, in.Holder, ttl)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, writerLeaseAcquiredResponse{LeaseID: id})
}

func (h *Handler) releaseWriterLease(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	ls, err := h.leaseService()
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in writerLeaseRequest
	if !decode(w, r, &in) {
		return
	}
	if err := ls.ReleaseWriter(r.Context(), scope, in.ComputerID, in.Holder); err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

func (h *Handler) acquireActivityLease(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	ls, err := h.leaseService()
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in activityLeaseRequest
	if !decode(w, r, &in) {
		return
	}
	ttl, err := leaseTTL(in.TTLms)
	if err != nil {
		writeError(w, r, err)
		return
	}
	id, err := ls.AcquireActivity(r.Context(), scope, in.ComputerID, compute.ActivityLeaseKind(in.Kind), in.Holder, ttl)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, leaseResponse{LeaseID: id})
}

func (h *Handler) releaseActivityLease(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	ls, err := h.leaseService()
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in activityLeaseRequest
	if !decode(w, r, &in) {
		return
	}
	if err := ls.ReleaseActivity(r.Context(), scope, in.ComputerID, compute.ActivityLeaseKind(in.Kind), in.Holder); err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

func (h *Handler) activityLeases(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	ls, err := h.leaseService()
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in computerRefRequest
	if !decode(w, r, &in) {
		return
	}
	leases, err := ls.ActiveActivityLeases(r.Context(), scope, in.ComputerID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	out := make([]leaseInfoDTO, 0, len(leases))
	for _, l := range leases {
		out = append(out, leaseInfoDTO{Kind: string(l.Kind), Holder: l.Holder})
	}
	transport.WriteJSON(w, http.StatusOK, activityLeasesResponse{Leases: out, Count: len(out)})
}

// ---------------------------------------------------------------------------
// Environment install with real rollback (PDF 3.6 B51)
// ---------------------------------------------------------------------------

func (h *Handler) runInstall(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if h.runner == nil {
		writeError(w, r, contracts.NewError(contracts.ErrForbidden,
			"computeapi: execution runner is not configured on this host"))
		return
	}
	var in installCommandRequest
	if !decode(w, r, &in) {
		return
	}
	code, stdout, stderr, err := h.runner.RunEnvironmentInstall(r.Context(), scope, in.ComputerID, in.RevisionID, in.Command)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, runInstallResponse{ExitCode: code, Stdout: stdout, Stderr: stderr})
}

func (h *Handler) listEnvironment(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if h.runner == nil {
		writeError(w, r, contracts.NewError(contracts.ErrForbidden,
			"computeapi: execution runner is not configured on this host"))
		return
	}
	var in computerRefRequest
	if !decode(w, r, &in) {
		return
	}
	cur, files, err := h.runner.ListEnvironment(r.Context(), scope, in.ComputerID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, envFilesResponse{CurrentRevision: cur, Files: files})
}

// --- B79: Version management (pinning, drain, compat, rollback) ---

type publishVersionRequest struct {
	Agent         string `json:"agent"`
	Digest        string `json:"digest"`
	StateCompat   bool   `json:"state_compat"`
	DrainWindowMs int64  `json:"drain_window_ms"`
}

func (h *Handler) publishVersion(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in publishVersionRequest
	if !decode(w, r, &in) {
		return
	}
	window := time.Duration(in.DrainWindowMs) * time.Millisecond
	if window <= 0 {
		window = 24 * time.Hour
	}
	ver, err := h.store.PublishVersion(r.Context(), scope, in.Agent, in.Digest, in.StateCompat, window)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, toVersionDTO(ver))
}

type drainVersionRequest struct {
	Agent         string `json:"agent"`
	Digest        string `json:"digest"`
	DrainWindowMs int64  `json:"drain_window_ms"`
}

func (h *Handler) drainVersion(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in drainVersionRequest
	if !decode(w, r, &in) {
		return
	}
	window := time.Duration(in.DrainWindowMs) * time.Millisecond
	if window <= 0 {
		window = 24 * time.Hour
	}
	ver, err := h.store.DrainVersion(r.Context(), scope, in.Agent, in.Digest, window)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, toVersionDTO(ver))
}

type rollbackVersionRequest struct {
	Agent string `json:"agent"`
}

func (h *Handler) rollbackVersion(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in rollbackVersionRequest
	if !decode(w, r, &in) {
		return
	}
	ver, err := h.store.RollbackVersion(r.Context(), scope, in.Agent)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, toVersionDTO(ver))
}

type getActiveVersionRequest struct {
	Agent string `json:"agent"`
}

func (h *Handler) getActiveVersion(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	var in getActiveVersionRequest
	if !decode(w, r, &in) {
		return
	}
	ver, err := h.store.GetActiveVersion(r.Context(), scope, in.Agent)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, toVersionDTO(ver))
}
