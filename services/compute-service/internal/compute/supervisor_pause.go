package compute

// The supervisor half of sleep/resume. Without these routes the capability would
// exist only inside the runtime package and nothing on the control plane could
// ask for it - which is exactly how it stayed unwired: the client protocol spoke
// health/acquire/exec/pty/release and stopped there.
//
// The refusal rule matters more than the happy path. A runtime that cannot freeze
// a guest answers 503 naming the backend, because a silent 200 would tell the
// scheduler that a sandbox sleeps while it is still burning a core, and that is a
// false isolation and accounting fact, not a cosmetic one.

import (
	"net/http"
)

// pauseRequest names the guest to freeze or wake. It is the same shape release
// already uses, so the protocol does not grow a second way to name a sandbox.
type pauseRequest struct {
	SandboxID string `json:"sandbox_id"`
}

// snapshotResponse is the parked guest's reference, handed back so the caller can
// restore the SAME incarnation later.
type snapshotResponse struct {
	SandboxID string `json:"sandbox_id"`
	Node      string `json:"node"`
	Dir       string `json:"dir"`
	APISocket string `json:"api_socket"`
	StatePath string `json:"state_path"`
	MemPath   string `json:"mem_path"`
}

func (s *Supervisor) handlePause(w http.ResponseWriter, r *http.Request) {
	_, sb, ok := s.sleepTarget(w, r, "pause")
	if !ok {
		return
	}
	sr, ok := s.sleepResumer(w)
	if !ok {
		return
	}
	if err := sr.Pause(r.Context(), sb); err != nil {
		supervisorError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Supervisor) handleResume(w http.ResponseWriter, r *http.Request) {
	_, sb, ok := s.sleepTarget(w, r, "resume")
	if !ok {
		return
	}
	sr, ok := s.sleepResumer(w)
	if !ok {
		return
	}
	if err := sr.Resume(r.Context(), sb); err != nil {
		supervisorError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Supervisor) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	_, sb, ok := s.sleepTarget(w, r, "snapshot")
	if !ok {
		return
	}
	snap, isSnap := s.rt.(Snapshotter)
	if !isSnap {
		supervisorError(w, Unavailable("supervisor", s.rt.Name()+" backend cannot snapshot a guest; only a real hypervisor can park one, refusing instead of reporting a snapshot that was never written"))
		return
	}
	ref, err := snap.Snapshot(r.Context(), sb)
	if err != nil {
		supervisorError(w, err)
		return
	}
	writeSupervisorJSON(w, snapshotResponse{
		SandboxID: ref.SandboxID, Node: ref.Node, Dir: ref.Dir, APISocket: ref.APISocket,
		StatePath: ref.StatePath, MemPath: ref.MemPath,
	})
}

// sleepTarget resolves the named guest. An unknown id is a 404 - the supervisor
// owns the live set and never guesses.
func (s *Supervisor) sleepTarget(w http.ResponseWriter, r *http.Request, verb string) (pauseRequest, Sandbox, bool) {
	var in pauseRequest
	if err := decodeSupervisorJSON(r, &in); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return in, nil, false
	}
	sb, ok := s.sandbox(in.SandboxID)
	if !ok {
		http.Error(w, "unknown sandbox", http.StatusNotFound)
		return in, nil, false
	}
	return in, sb, true
}

// sleepResumer asserts the capability on the runtime the supervisor was built
// with, and refuses by name when it is absent.
func (s *Supervisor) sleepResumer(w http.ResponseWriter) (SleepResumer, bool) {
	sr, ok := s.rt.(SleepResumer)
	if !ok {
		supervisorError(w, Unavailable("supervisor", s.rt.Name()+" backend cannot pause or resume a guest; sleep/resume belongs to a hypervisor class only, refusing instead of pretending"))
		return nil, false
	}
	return sr, true
}
