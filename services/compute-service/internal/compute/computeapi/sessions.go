package computeapi

// The PTY / browser-worker session facade.
//
// Requirement (PDF rev3.2 s.29-30): Compute owns "команды, PTY, окружения, браузерные
// воркеры", and s.36/s.48 hold their effects to the same lease/audit rules as commands.
// The finding this answers was that polling GetCommand is not a PTY: a caller had no
// session to hold, no way to send input, and no honest refusal.
//
// Authorization is the same as RunCommand's - the route is behind transport.Task with
// the "compute" capability, and the scope comes only from the verified principal - and
// the durable effects a session has (its activity lease, the PTY budget) go through the
// same Store the runner uses, so a live session pins sleep and export exactly as a live
// command does.
//
// The implementation behind this facade is compute.SessionService, which is a MOCK: it
// starts no shell, opens no browser and sends no network traffic. When no session facade
// is mounted - which is every deployment that has not explicitly opted into the mock -
// these routes answer 503 UNAVAILABLE with the reason, because a route that returns a
// plausible empty session would be the exact overclaim this plane keeps having to
// retract. Every answer carries the mock marker in its body and, at the mount, in the
// AHP-Compute-Backend header.

import (
	"net/http"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/pkg/transport"
)

type openSessionRequest struct {
	ComputerID string `json:"computer_id"`
	Shell      string `json:"shell,omitempty"`
	Cwd        string `json:"cwd,omitempty"`
	URL        string `json:"url,omitempty"`
	Cols       uint16 `json:"cols,omitempty"`
	Rows       uint16 `json:"rows,omitempty"`
}

type sessionRefRequest struct {
	ComputerID string `json:"computer_id"`
	SessionID  string `json:"session_id"`
}

type sessionInputRequest struct {
	ComputerID string `json:"computer_id"`
	SessionID  string `json:"session_id"`
	Input      string `json:"input"`
}

type sessionOutputRequest struct {
	ComputerID string `json:"computer_id"`
	SessionID  string `json:"session_id"`
	AfterSeq   int64  `json:"after_seq,omitempty"`
}

type browserActionRequest struct {
	ComputerID string `json:"computer_id"`
	SessionID  string `json:"session_id"`
	Action     string `json:"action"`
	Target     string `json:"target,omitempty"`
}

// sessionUnavailable is the honest answer when no session facade is mounted. It names
// what is missing so an operator can mount it, and it never dresses up as an empty
// session.
func sessionUnavailable() error {
	return compute.Unavailable("sessions",
		"no session facade is mounted: the MOCK session facade is opt-in (AHP_COMPUTE_MOCK_BACKEND=1), "+
			"and a real PTY/browser facade is not implemented. Nothing is simulated silently.")
}

func (h *Handler) openSession(w http.ResponseWriter, r *http.Request, kind compute.SessionKind) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if h.sessions == nil {
		writeError(w, r, sessionUnavailable())
		return
	}
	var in openSessionRequest
	if !decode(w, r, &in) {
		return
	}
	sess, err := h.sessions.Open(r.Context(), scope, in.ComputerID, kind, compute.SessionSpec{
		Shell: in.Shell, Cwd: in.Cwd, URL: in.URL, Cols: in.Cols, Rows: in.Rows,
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, sess)
}

func (h *Handler) openPTY(w http.ResponseWriter, r *http.Request) {
	h.openSession(w, r, compute.SessionPTY)
}

func (h *Handler) openBrowser(w http.ResponseWriter, r *http.Request) {
	h.openSession(w, r, compute.SessionBrowser)
}

func (h *Handler) ptyInput(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if h.sessions == nil {
		writeError(w, r, sessionUnavailable())
		return
	}
	var in sessionInputRequest
	if !decode(w, r, &in) {
		return
	}
	sess, err := h.sessions.Input(r.Context(), scope, in.ComputerID, in.SessionID, in.Input)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, sess)
}

func (h *Handler) browserAction(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if h.sessions == nil {
		writeError(w, r, sessionUnavailable())
		return
	}
	var in browserActionRequest
	if !decode(w, r, &in) {
		return
	}
	sess, err := h.sessions.BrowserAction(r.Context(), scope, in.ComputerID, in.SessionID, in.Action, in.Target)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, sess)
}

func (h *Handler) ptyOutput(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if h.sessions == nil {
		writeError(w, r, sessionUnavailable())
		return
	}
	var in sessionOutputRequest
	if !decode(w, r, &in) {
		return
	}
	out, err := h.sessions.Output(r.Context(), scope, in.ComputerID, in.SessionID, in.AfterSeq)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, out)
}

func (h *Handler) closeSession(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if h.sessions == nil {
		writeError(w, r, sessionUnavailable())
		return
	}
	var in sessionRefRequest
	if !decode(w, r, &in) {
		return
	}
	sess, err := h.sessions.Close(r.Context(), scope, in.ComputerID, in.SessionID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, sess)
}
