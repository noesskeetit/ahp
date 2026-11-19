// Package workspaceapi is the thin HTTP facade over the workspace store.
//
// Identity rules match the artifact API: the isolation scope always comes from
// transport.FromContext (mTLS + bound task token), request bodies never carry
// scope fields, and the fine-grained membership/rights decision lives in the
// store and is re-checked per call. ReadFile streams the sealed artifact and
// honours Range; it never returns a presigned URL.
package workspaceapi

import (
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/ahp-platform/ahp/internal/workspace"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

const (
	audience     = "ahp"
	capability   = "workspace"
	maxBodyBytes = 1 << 20
)

// Handler is the mounted Workspace API: POST /v1/workspace/{Operation}.
type Handler struct {
	store      *workspace.Store
	task       func(http.Handler) http.Handler
	operations map[string]func(http.ResponseWriter, *http.Request)
}

// New wires the facade. Every dependency is mandatory.
func New(store *workspace.Store, verifier *auth.Verifier, authorize transport.Authorize) (http.Handler, error) {
	return newHandler(store, verifier, authorize, "")
}

// NewAcceptingUsers wires the same facade on a plane that a sanctioned user edge
// forwards end users to: a task token still works exactly as before, and a user
// credential presented over the edge's client certificate is verified by THIS
// plane, per the director's ruling 20261101T154234Z item 2. userAudience is the
// audience identity puts on the credential a browser holds; it is required here
// rather than defaulted, because widening who may call a data-plane face is a
// deployment decision, and the plane that gets it wrong is the one holding the
// project files. Which deployments may carry user credentials at all stays
// policy in authorize, which receives the completed scope.
func NewAcceptingUsers(store *workspace.Store, verifier *auth.Verifier, authorize transport.Authorize, userAudience string) (http.Handler, error) {
	if userAudience == "" {
		return nil, contracts.NewError(contracts.ErrInvalid,
			"workspaceapi: a user audience is required when the face accepts user credentials")
	}
	return newHandler(store, verifier, authorize, userAudience)
}

func newHandler(store *workspace.Store, verifier *auth.Verifier, authorize transport.Authorize, userAudience string) (http.Handler, error) {
	switch {
	case store == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "workspaceapi: store must not be nil")
	case verifier == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "workspaceapi: verifier must not be nil")
	case authorize == nil:
		return nil, contracts.NewError(contracts.ErrInvalid, "workspaceapi: authorize must not be nil")
	}
	guard := transport.Task(verifier, audience, capability, authorize)
	if userAudience != "" {
		guard = transport.TaskOrUser(verifier, audience, userAudience, capability, authorize)
	}
	h := &Handler{store: store, task: guard}
	h.operations = map[string]func(http.ResponseWriter, *http.Request){
		"Create":               h.create,
		"AddMember":            h.addMember,
		"RemoveMember":         h.removeMember,
		"ListMembers":          h.listMembers,
		"GetWorkspace":         h.getWorkspace,
		"CreateCandidate":      h.createCandidate,
		"UpdateCandidateFiles": h.updateCandidateFiles,
		"SetCheck":             h.setCheck,
		"RegisterCIRunner":     h.registerCIRunner,
		"DeregisterCIRunner":   h.deregisterCIRunner,
		"ListCIRunners":        h.listCIRunners,
		"GetCandidate":         h.getCandidate,
		"CandidateDetails":     h.candidateDetails,
		"VerifyCandidate":      h.verifyCandidate,
		"ListCandidates":       h.listCandidates,
		"RejectCandidate":      h.rejectCandidate,
		"Publish":              h.publish,
		"GetHead":              h.getHead,
		"GetRevision":          h.getRevision,
		"ListRevisions":        h.listRevisions,
		"ListFiles":            h.listFiles,
		"ReadFile":             h.readFile,
		"Pin":                  h.pin,
		"Unpin":                h.unpin,
		"GetBranch":            h.getBranch,
		"PurgeRevision":        h.purgeRevision,
		"VerifyIntegrity":      h.verifyIntegrity,
		"ListConflictBranches": h.listConflictBranches,
	}
	return h, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "workspaceapi: unknown route"))
		return
	}
	rest, ok := strings.CutPrefix(r.URL.Path, "/v1/workspace/")
	if !ok || strings.Contains(rest, "/") {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "workspaceapi: unknown route"))
		return
	}
	fn, ok := h.operations[rest]
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "workspaceapi: unknown route"))
		return
	}
	h.task(http.HandlerFunc(fn)).ServeHTTP(w, r)
}

func principal(r *http.Request) (contracts.Scope, error) {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		return contracts.Scope{}, contracts.NewError(contracts.ErrForbidden,
			"workspaceapi: no verified principal on this request")
	}
	return p.Scope, nil
}

var okBody = map[string]bool{"ok": true}

type workspaceRequest struct {
	Workspace string `json:"workspace"`
}

type memberRequest struct {
	Workspace string `json:"workspace"`
	Subject   string `json:"subject"`
}

type createCandidateRequest struct {
	Workspace string `json:"workspace"`
	BaseRev   string `json:"base_rev,omitempty"`
}

type candidateIDRequest struct {
	CandidateID string `json:"candidate_id"`
}

type updateFilesRequest struct {
	CandidateID string           `json:"candidate_id"`
	Files       []workspace.File `json:"files"`
}

type setCheckRequest struct {
	CandidateID string `json:"candidate_id"`
	Name        string `json:"name"`
	Status      string `json:"status"`
	Detail      string `json:"detail,omitempty"`
}

type ciRunnerRequest struct {
	Workspace string `json:"workspace"`
	Subject   string `json:"subject"`
}

type listCandidatesRequest struct {
	Workspace string   `json:"workspace"`
	States    []string `json:"states,omitempty"`
}

type publishRequest struct {
	CandidateID  string `json:"candidate_id"`
	ExpectedHead string `json:"expected_head"`
}

type revisionRequest struct {
	Workspace string `json:"workspace"`
	Revision  string `json:"revision"`
}

type readFileRequest struct {
	Workspace string `json:"workspace"`
	Revision  string `json:"revision"`
	Path      string `json:"path"`
}

type branchRequest struct {
	Workspace string `json:"workspace"`
	Branch    string `json:"branch"`
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in workspaceRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	ws, err := h.store.Create(r.Context(), scope, in.Workspace)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, ws)
}

func (h *Handler) addMember(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in memberRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.store.AddMember(r.Context(), scope, in.Workspace, in.Subject); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

func (h *Handler) removeMember(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in memberRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.store.RemoveMember(r.Context(), scope, in.Workspace, in.Subject); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

func (h *Handler) listMembers(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in workspaceRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	members, err := h.store.ListMembers(r.Context(), scope, in.Workspace)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, members)
}

func (h *Handler) getWorkspace(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in workspaceRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	ws, err := h.store.GetWorkspace(r.Context(), scope, in.Workspace)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, ws)
}

func (h *Handler) createCandidate(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in createCandidateRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	cand, err := h.store.CreateCandidate(r.Context(), scope, in.Workspace, in.BaseRev)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, cand)
}

func (h *Handler) updateCandidateFiles(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in updateFilesRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	cand, err := h.store.UpdateCandidateFiles(r.Context(), scope, in.CandidateID, in.Files)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, cand)
}

func (h *Handler) setCheck(w http.ResponseWriter, r *http.Request) {
	// A sanctioned legacy user edge may forward human credentials to other
	// Workspace operations. It must never turn a supplied verdict into CI.
	p, ok := transport.FromContext(r.Context())
	if !ok || p.Claims.Kind != auth.KindTask {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "trusted check verdicts require a task principal; humans may request server-side integrity verification"))
		return
	}
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in setCheckRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	// Provenance comes from the verified claims, never from the body. Whether
	// this task belongs to a registered CI workload is the store's decision;
	// nothing the caller sends can assert it.
	prov := workspace.CheckProvenance{Task: p.Claims.Task, Root: p.Claims.Root}
	if err := h.store.SetCheckWithProvenance(r.Context(), scope, in.CandidateID, in.Name, in.Status, in.Detail, prov); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

// registerCIRunner designates a CI workload whose verdicts carry trusted
// provenance. Provenance itself is decided in the store from the registry,
// never from a request body.
func (h *Handler) registerCIRunner(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in ciRunnerRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.store.RegisterCIRunner(r.Context(), scope, in.Workspace, in.Subject); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

func (h *Handler) deregisterCIRunner(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in ciRunnerRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.store.DeregisterCIRunner(r.Context(), scope, in.Workspace, in.Subject); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

func (h *Handler) listCIRunners(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in workspaceRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	runners, err := h.store.ListCIRunners(r.Context(), scope, in.Workspace)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, runners)
}

func (h *Handler) getCandidate(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in candidateIDRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	cand, err := h.store.GetCandidate(r.Context(), scope, in.CandidateID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, cand)
}

func (h *Handler) listCandidates(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in listCandidatesRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	cands, err := h.store.ListCandidates(r.Context(), scope, in.Workspace, in.States)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, cands)
}

func (h *Handler) rejectCandidate(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in candidateIDRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	cand, err := h.store.RejectCandidate(r.Context(), scope, in.CandidateID)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, cand)
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
	rev, err := h.store.Publish(r.Context(), scope, in.CandidateID, in.ExpectedHead)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, rev)
}

func (h *Handler) getHead(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in workspaceRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	head, err := h.store.GetHead(r.Context(), scope, in.Workspace)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]string{"head_rev": head})
}

func (h *Handler) getRevision(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in revisionRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	rev, err := h.store.GetRevision(r.Context(), scope, in.Workspace, in.Revision)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, rev)
}

func (h *Handler) listRevisions(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in workspaceRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	revs, err := h.store.ListRevisions(r.Context(), scope, in.Workspace)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, revs)
}

func (h *Handler) listFiles(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in revisionRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	files, err := h.store.ListFiles(r.Context(), scope, in.Workspace, in.Revision)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, files)
}

func (h *Handler) pin(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in revisionRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.store.Pin(r.Context(), scope, in.Workspace, in.Revision); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

func (h *Handler) unpin(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in revisionRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.store.Unpin(r.Context(), scope, in.Workspace, in.Revision); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

func (h *Handler) getBranch(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in branchRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	rev, err := h.store.GetBranch(r.Context(), scope, in.Workspace, in.Branch)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, rev)
}

func (h *Handler) purgeRevision(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in revisionRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	if err := h.store.PurgeRevision(r.Context(), scope, in.Workspace, in.Revision); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, okBody)
}

// readFile streams the sealed artifact backing a path, honouring a single byte
// range. Rights are re-checked by the store on every call.
func (h *Handler) readFile(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in readFileRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	w.Header().Set("Accept-Ranges", "bytes")
	offset, count, partial, ok := parseRange(r.Header.Get("Range"))
	if !ok {
		partial = false
	}
	rc, file, size, err := h.store.ReadFile(r.Context(), scope, in.Workspace, in.Revision, in.Path, offset, count)
	if err != nil {
		if isRangeNotSatisfiable(err) {
			writeRangeNotSatisfiable(w, err)
			return
		}
		transport.WriteError(w, r, err)
		return
	}
	defer func() { _ = rc.Close() }()
	etag := quoteETag(file.Digest)
	w.Header().Set("ETag", etag)
	w.Header().Set("Content-Type", "application/octet-stream")
	if partial {
		end := offset + size - 1
		if count > 0 && offset+count-1 < end {
			end = offset + count - 1
		}
		w.Header().Set("Content-Range", "bytes "+
			strconv.FormatInt(offset, 10)+"-"+strconv.FormatInt(end, 10)+
			"/"+strconv.FormatInt(size, 10))
		w.Header().Set("Content-Length", strconv.FormatInt(end-offset+1, 10))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		w.WriteHeader(http.StatusOK)
	}
	_, _ = io.Copy(w, rc)
}

type verifyIntegrityRequest struct {
	Workspace string `json:"workspace"`
	Revision  string `json:"revision"`
}

func (h *Handler) verifyIntegrity(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in verifyIntegrityRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		return
	}
	report, err := h.store.VerifyRevisionIntegrity(r.Context(), scope, in.Workspace, in.Revision)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, report)
}

type listConflictBranchesRequest struct {
	Workspace string `json:"workspace"`
}

func (h *Handler) listConflictBranches(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in listConflictBranchesRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		return
	}
	branches, err := h.store.ListConflictBranches(r.Context(), scope, in.Workspace)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, branches)
}

func (h *Handler) candidateDetails(w http.ResponseWriter, r *http.Request) {
	h.reviewCandidate(w, r, false)
}
func (h *Handler) verifyCandidate(w http.ResponseWriter, r *http.Request) {
	h.reviewCandidate(w, r, true)
}
func (h *Handler) reviewCandidate(w http.ResponseWriter, r *http.Request, verify bool) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in candidateIDRequest
	if err = transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var result workspace.CandidateReview
	if verify {
		result, err = h.store.VerifyCandidate(r.Context(), scope, in.CandidateID)
	} else {
		result, err = h.store.CandidateDetails(r.Context(), scope, in.CandidateID)
	}
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, result)
}
