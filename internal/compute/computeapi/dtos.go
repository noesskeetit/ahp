package computeapi

import (
	"time"

	"github.com/ahp-platform/ahp/internal/compute"
)

// The domain types in internal/compute deliberately carry no JSON tags (they
// are durable records, not wire shapes). These DTOs fix the public wire format
// (snake_case) and convert one way at the edge.

type quotaDTO struct {
	CPUMillisPerSec int32 `json:"cpu_millis_per_sec,omitempty"`
	MemoryMB        int32 `json:"memory_mb,omitempty"`
	MaxPIDs         int32 `json:"max_pids,omitempty"`
	MaxPTYs         int32 `json:"max_ptys,omitempty"`
	MaxOpenFiles    int32 `json:"max_open_files,omitempty"`
	MaxStorageMB    int32 `json:"max_storage_mb,omitempty"`
	MaxSessionTTLMS int64 `json:"max_session_ttl_ms,omitempty"`
}

func (q quotaDTO) toQuota() compute.Quota {
	return compute.Quota{
		MaxCPUMillisPerSec: q.CPUMillisPerSec,
		MaxMemoryMB:        q.MemoryMB,
		MaxPIDs:            q.MaxPIDs,
		MaxPTYs:            q.MaxPTYs,
		MaxOpenFiles:       q.MaxOpenFiles,
		MaxStorageMB:       q.MaxStorageMB,
		MaxSessionTTL:      time.Duration(q.MaxSessionTTLMS) * time.Millisecond,
	}
}

func quotaFrom(q compute.Quota) quotaDTO {
	return quotaDTO{
		CPUMillisPerSec: q.MaxCPUMillisPerSec,
		MemoryMB:        q.MaxMemoryMB,
		MaxPIDs:         q.MaxPIDs,
		MaxPTYs:         q.MaxPTYs,
		MaxOpenFiles:    q.MaxOpenFiles,
		MaxStorageMB:    q.MaxStorageMB,
		MaxSessionTTLMS: int64(q.MaxSessionTTL / time.Millisecond),
	}
}

type computerDTO struct {
	ID          string    `json:"id"`
	Tenant      string    `json:"tenant"`
	Subject     string    `json:"subject"`
	Deployment  string    `json:"deployment,omitempty"`
	Class       string    `json:"class"`
	Profile     string    `json:"profile"`
	Status      string    `json:"status"`
	VolumeID    string    `json:"volume_id"`
	WorkspaceID string    `json:"workspace_id,omitempty"`
	Realm       string    `json:"realm,omitempty"`
	Incarnation int64     `json:"incarnation"`
	Quota       quotaDTO  `json:"quota"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func computerFrom(c compute.Computer) computerDTO {
	return computerDTO{
		ID:          c.ID,
		Tenant:      c.Scope.Tenant,
		Subject:     c.Scope.Subject,
		Deployment:  c.Scope.Deployment,
		Class:       string(c.Class),
		Profile:     string(c.Profile),
		Status:      string(c.Status),
		VolumeID:    c.VolumeID,
		WorkspaceID: c.WorkspaceID,
		Realm:       c.Realm,
		Incarnation: c.Incarnation,
		Quota:       quotaFrom(c.Quota),
		CreatedAt:   c.CreatedAt,
		UpdatedAt:   c.UpdatedAt,
	}
}

type incarnationDTO struct {
	ID         string     `json:"id"`
	ComputerID string     `json:"computer_id"`
	Seq        int64      `json:"seq"`
	Status     string     `json:"status"`
	Backend    string     `json:"backend"`
	Node       string     `json:"node"`
	VMGenID    string     `json:"vm_gen_id,omitempty"`
	Fence      int64      `json:"fence"`
	AcquiredAt time.Time  `json:"acquired_at"`
	StoppedAt  *time.Time `json:"stopped_at,omitempty"`
}

func incarnationFrom(i compute.Incarnation) incarnationDTO {
	return incarnationDTO{
		ID:         i.ID,
		ComputerID: i.ComputerID,
		Seq:        i.Seq,
		Status:     string(i.Status),
		Backend:    i.Backend,
		Node:       i.Node,
		VMGenID:    i.VMGenID,
		Fence:      i.Fence,
		AcquiredAt: i.AcquiredAt,
		StoppedAt:  i.StoppedAt,
	}
}

// commandDTO is what an observer of a command sees, its logs included.
// OutputTruncated says a stream hit the 4 MiB capture cap, so Stdout/Stderr are a
// prefix of what the command wrote: without it a bounded log would be handed out as if
// it were the whole one. The field is omitted when nothing was thrown away.
type commandDTO struct {
	ID          string `json:"id"`
	ComputerID  string `json:"computer_id"`
	Incarnation int64  `json:"incarnation"`
	Key         string `json:"key"`
	ExecutionID string `json:"execution_id,omitempty"`
	Status      string `json:"status"`
	Command     string `json:"command"`
	Cwd         string `json:"cwd,omitempty"`
	Stdout      string `json:"stdout,omitempty"`
	Stderr      string `json:"stderr,omitempty"`
	ExitCode    int    `json:"exit_code"`
	HasExit     bool   `json:"has_exit"`
	Truncated   bool   `json:"output_truncated,omitempty"`
	// TerminalReason names who closed a command that never answered and on what
	// evidence (dispatch_deadline_exceeded / backend_invoked_without_answer). Empty
	// for every command that reached its own answer, so the field only appears when
	// the platform had to infer a verdict - which is exactly when an operator needs
	// to know that it did.
	TerminalReason string    `json:"terminal_reason,omitempty"`
	OutputSeq      int64     `json:"output_seq"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

func commandFrom(c compute.CommandRecord) commandDTO {
	return commandDTO{
		ID:             c.ID,
		ComputerID:     c.ComputerID,
		Incarnation:    c.Incarnation,
		Key:            c.Key,
		ExecutionID:    c.ExecutionID,
		Status:         string(c.Status),
		Command:        c.Command,
		Cwd:            c.Cwd,
		Stdout:         c.Stdout,
		Stderr:         c.Stderr,
		ExitCode:       c.ExitCode,
		HasExit:        c.HasExit,
		Truncated:      c.OutputTruncated,
		TerminalReason: c.TerminalReason,
		OutputSeq:      c.OutputSeq,
		CreatedAt:      c.CreatedAt,
		UpdatedAt:      c.UpdatedAt,
	}
}

type snapshotDTO struct {
	ID         string    `json:"id"`
	ComputerID string    `json:"computer_id"`
	Kind       string    `json:"kind"`
	Path       string    `json:"path"`
	Digest     string    `json:"digest"`
	SizeBytes  int64     `json:"size_bytes"`
	CreatedAt  time.Time `json:"created_at"`
}

func snapshotFrom(s compute.Snapshot) snapshotDTO {
	return snapshotDTO{
		ID:         s.ID,
		ComputerID: s.ComputerID,
		Kind:       string(s.Kind),
		Path:       s.Path,
		Digest:     s.Digest,
		SizeBytes:  s.SizeBytes,
		CreatedAt:  s.CreatedAt,
	}
}

type envRevisionDTO struct {
	ID         string    `json:"id"`
	ComputerID string    `json:"computer_id"`
	Revision   int64     `json:"revision"`
	Spec       string    `json:"spec"`
	Status     string    `json:"status"`
	ParentID   string    `json:"parent_id,omitempty"`
	ErrorMsg   string    `json:"error_msg,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

func envFrom(e compute.EnvRevision) envRevisionDTO {
	return envRevisionDTO{
		ID:         e.ID,
		ComputerID: e.ComputerID,
		Revision:   e.Revision,
		Spec:       e.Spec,
		Status:     e.Status,
		ParentID:   e.ParentID,
		ErrorMsg:   e.ErrorMsg,
		CreatedAt:  e.CreatedAt,
	}
}

type workspaceRevisionDTO struct {
	ID         string    `json:"id"`
	ComputerID string    `json:"computer_id"`
	Revision   int64     `json:"revision"`
	BaseRev    int64     `json:"base_rev"`
	Digest     string    `json:"digest"`
	Branch     string    `json:"branch"`
	CreatedAt  time.Time `json:"created_at"`
}

func workspaceFrom(w compute.WorkspaceRevision) workspaceRevisionDTO {
	return workspaceRevisionDTO{
		ID:         w.ID,
		ComputerID: w.ComputerID,
		Revision:   w.Revision,
		BaseRev:    w.BaseRev,
		Digest:     w.Digest,
		Branch:     w.Branch,
		CreatedAt:  w.CreatedAt,
	}
}

type artifactRefDTO struct {
	ArtifactID string    `json:"artifact_id"`
	Version    string    `json:"version"`
	Digest     string    `json:"digest"`
	SizeBytes  int64     `json:"size_bytes"`
	SealedAt   time.Time `json:"sealed_at,omitzero"`
}

func artifactFrom(a compute.ArtifactRef) artifactRefDTO {
	return artifactRefDTO{
		ArtifactID: a.ArtifactID,
		Version:    a.Version,
		Digest:     a.Digest,
		SizeBytes:  a.SizeBytes,
		SealedAt:   a.SealedAt,
	}
}

type volumeFenceDTO struct {
	ComputerID  string    `json:"computer_id"`
	VolumeID    string    `json:"volume_id"`
	Fence       int64     `json:"fence"`
	Incarnation string    `json:"incarnation,omitempty"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func fenceFrom(f compute.VolumeFence) volumeFenceDTO {
	return volumeFenceDTO{
		ComputerID:  f.ComputerID,
		VolumeID:    f.VolumeID,
		Fence:       f.Fence,
		Incarnation: f.Incarnation,
		UpdatedAt:   f.UpdatedAt,
	}
}

// ---------------------------------------------------------------------------
// request shapes (strict; unknown keys rejected by transport.ReadJSON)
// ---------------------------------------------------------------------------

type createComputerRequest struct {
	Class       string   `json:"class"`
	Profile     string   `json:"profile"`
	WorkspaceID string   `json:"workspace_id,omitempty"`
	Realm       string   `json:"realm,omitempty"`
	Key         string   `json:"key"`
	Quota       quotaDTO `json:"quota,omitempty"`
}

type computerRefRequest struct {
	ComputerID string `json:"computer_id"`
}

type acquireIncarnationRequest struct {
	ComputerID string `json:"computer_id"`
	Backend    string `json:"backend,omitempty"`
	Node       string `json:"node,omitempty"`
	VMGenID    string `json:"vm_gen_id,omitempty"`
}

type stopIncarnationRequest struct {
	ComputerID    string `json:"computer_id"`
	IncarnationID string `json:"incarnation_id"`
}

type execRequest struct {
	ComputerID string `json:"computer_id"`
	Key        string `json:"key"`
	Command    string `json:"command"`
	Cwd        string `json:"cwd,omitempty"`
}

type commandRefRequest struct {
	ComputerID string `json:"computer_id"`
	CommandID  string `json:"command_id"`
}

type completeRequest struct {
	ComputerID string `json:"computer_id"`
	CommandID  string `json:"command_id"`
	ExitCode   int    `json:"exit_code"`
	Stdout     string `json:"stdout,omitempty"`
	Stderr     string `json:"stderr,omitempty"`
	// OutputTruncated is what a backend that captured the output itself (the guest
	// agent path) reports about the capture cap. Absent means "not truncated".
	OutputTruncated bool `json:"output_truncated,omitempty"`
}

type commitWorkspaceRequest struct {
	ComputerID   string `json:"computer_id"`
	ExpectedHead int64  `json:"expected_head"`
	Digest       string `json:"digest"`
}

type ensureEnvironmentRequest struct {
	ComputerID string `json:"computer_id"`
	Spec       string `json:"spec"`
}

type beginInstallRequest struct {
	ComputerID string `json:"computer_id"`
	Spec       string `json:"spec"`
	Holder     string `json:"holder"`
}

type finishInstallRequest struct {
	ComputerID string `json:"computer_id"`
	RevisionID string `json:"revision_id"`
	Holder     string `json:"holder"`
	OK         bool   `json:"ok"`
	ErrorMsg   string `json:"error_msg,omitempty"`
}

type snapshotRequest struct {
	ComputerID string `json:"computer_id"`
	Kind       string `json:"kind"`
	Path       string `json:"path"`
	Digest     string `json:"digest"`
	SizeBytes  int64  `json:"size_bytes"`
}

type exportRequest struct {
	Key        string `json:"key"`
	ComputerID string `json:"computer_id"`
	Path       string `json:"path"`
}

type acquireLeaseRequest struct {
	ComputerID string `json:"computer_id"`
	Kind       string `json:"kind"`
	Holder     string `json:"holder"`
	TTLms      int64  `json:"ttl_ms"`
}

type releaseLeaseRequest struct {
	ComputerID string `json:"computer_id"`
	Kind       string `json:"kind"`
	Holder     string `json:"holder"`
}

// ---------------------------------------------------------------------------
// response envelopes
// ---------------------------------------------------------------------------

type execResponse struct {
	Command     commandDTO `json:"command"`
	MayDispatch bool       `json:"may_dispatch"`
	// DispatchReason explains a may_dispatch=false. The boolean alone leaves a caller
	// guessing between "this key was already answered" and "nothing here can ever run it",
	// which call for opposite reactions; DEF-COMPUTE-5 is the cost of that guess.
	DispatchReason string `json:"dispatch_reason,omitempty"`
}

type cancelResponse struct {
	Command     commandDTO `json:"command"`
	CancelPhase string     `json:"cancel_phase"`
}

type leaseResponse struct {
	LeaseID string `json:"lease_id"`
}

type okResponse struct {
	OK bool `json:"ok"`
}

var okBody = okResponse{OK: true}

// ---------------------------------------------------------------------------
// Computer state / leases / sleep / environment (PDF 3.6)
// ---------------------------------------------------------------------------

type typedPatchRequest struct {
	ComputerID   string `json:"computer_id"`
	Branch       string `json:"branch,omitempty"`
	Path         string `json:"path"`
	ExpectedHash string `json:"expected_hash,omitempty"`
	ContentB64   string `json:"content_b64"`
	Mode         int    `json:"mode"`
}

type branchRequest struct {
	ComputerID string `json:"computer_id"`
	Branch     string `json:"branch,omitempty"`
	Name       string `json:"name,omitempty"`
}

type stateRevisionDTO struct {
	ID         string                  `json:"id"`
	ComputerID string                  `json:"computer_id"`
	Seq        int64                   `json:"seq"`
	Branch     string                  `json:"branch"`
	ParentID   string                  `json:"parent_id,omitempty"`
	BaseSeq    int64                   `json:"base_seq"`
	Tree       map[string]stateFileDTO `json:"tree"`
	Digest     string                  `json:"digest"`
	CreatedAt  time.Time               `json:"created_at"`
}

type stateFileDTO struct {
	Hash string `json:"hash"`
	Size int64  `json:"size"`
	Mode int    `json:"mode"`
}

func stateRevisionFrom(rv compute.StateRevision) stateRevisionDTO {
	tree := make(map[string]stateFileDTO, len(rv.Tree))
	for p, f := range rv.Tree {
		tree[p] = stateFileDTO{Hash: f.Hash, Size: f.Size, Mode: f.Mode}
	}
	return stateRevisionDTO{
		ID: rv.ID, ComputerID: rv.ComputerID, Seq: rv.Seq, Branch: rv.Branch,
		ParentID: rv.ParentID, BaseSeq: rv.BaseSeq, Tree: tree,
		Digest: rv.Digest, CreatedAt: rv.CreatedAt,
	}
}

type processKillDTO struct {
	PID  int  `json:"pid"`
	PGID int  `json:"pgid"`
	Dead bool `json:"dead"`
	// Zombie mirrors compute.ProcessKill: the process was proven dead as a
	// zombie (killed, waiting to be reaped) rather than by its pid being freed.
	Zombie bool `json:"zombie,omitempty"`
}

func processKillFrom(p compute.ProcessKill) processKillDTO {
	return processKillDTO{PID: p.PID, PGID: p.PGID, Dead: p.Dead, Zombie: p.Zombie}
}

type sleepReportDTO struct {
	ComputerID          string           `json:"computer_id"`
	Status              string           `json:"status"`
	LeasesHeld          int              `json:"leases_held"`
	Processes           []processKillDTO `json:"processes"`
	SleptAt             time.Time        `json:"slept_at"`
	IncarnationsStopped int64            `json:"incarnations_stopped"`
}

func sleepReportFrom(rep compute.SleepReport) sleepReportDTO {
	procs := make([]processKillDTO, 0, len(rep.Processes.Processes))
	for _, p := range rep.Processes.Processes {
		procs = append(procs, processKillFrom(p))
	}
	return sleepReportDTO{
		ComputerID: rep.ComputerID, Status: string(rep.Status),
		LeasesHeld: rep.LeasesHeld, Processes: procs, SleptAt: rep.SleptAt,
		IncarnationsStopped: rep.IncarnationsStopped,
	}
}

type costWindowDTO struct {
	ID        string     `json:"id"`
	StartedAt time.Time  `json:"started_at"`
	EndedAt   *time.Time `json:"ended_at"`
}

type costReportDTO struct {
	ComputerID string          `json:"computer_id"`
	AwakeNanos int64           `json:"awake_ns"`
	AwakeHuman string          `json:"awake_human"`
	Open       bool            `json:"open_window"`
	Windows    []costWindowDTO `json:"windows"`
}

func costReportFrom(rep compute.CostReport) costReportDTO {
	windows := make([]costWindowDTO, 0, len(rep.Windows))
	for _, w := range rep.Windows {
		cwd := costWindowDTO{ID: w.ID, StartedAt: w.StartedAt}
		if w.EndedAt != nil {
			cwd.EndedAt = w.EndedAt
		}
		windows = append(windows, cwd)
	}
	return costReportDTO{
		ComputerID: rep.ComputerID, AwakeNanos: rep.AwakeNanos,
		AwakeHuman: rep.AwakeHuman, Open: rep.Open, Windows: windows,
	}
}

type writerLeaseRequest struct {
	ComputerID string `json:"computer_id"`
	Holder     string `json:"holder"`
	TTLms      int64  `json:"ttl_ms"`
}

type activityLeaseRequest struct {
	ComputerID string `json:"computer_id"`
	Kind       string `json:"kind"`
	Holder     string `json:"holder"`
	TTLms      int64  `json:"ttl_ms"`
}

type leaseInfoDTO struct {
	Kind   string `json:"kind"`
	Holder string `json:"holder"`
}

type activityLeasesResponse struct {
	Leases []leaseInfoDTO `json:"leases"`
	Count  int            `json:"count"`
}

type installCommandRequest struct {
	ComputerID string `json:"computer_id"`
	RevisionID string `json:"revision_id"`
	Command    string `json:"command"`
}

type runInstallResponse struct {
	ExitCode int    `json:"exit_code"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
}

type envFilesResponse struct {
	CurrentRevision int64             `json:"current_revision"`
	Files           []compute.EnvFile `json:"files"`
}

type beginInstallResponse struct {
	Revision envRevisionDTO `json:"revision"`
	EnvDir   string         `json:"env_dir"`
}

type writerLeaseAcquiredResponse struct {
	LeaseID string `json:"lease_id"`
}

// --- B79: Version DTOs ---

type versionDTO struct {
	ID            string     `json:"id"`
	Agent         string     `json:"agent"`
	Digest        string     `json:"digest"`
	Status        string     `json:"status"`
	DrainDeadline *time.Time `json:"drain_deadline,omitempty"`
	StateCompat   bool       `json:"state_compat"`
	PublishedAt   time.Time  `json:"published_at"`
}

func toVersionDTO(v compute.ImageVersion) versionDTO {
	return versionDTO{
		ID:            v.ID,
		Agent:         v.Agent,
		Digest:        v.Digest,
		Status:        string(v.Status),
		DrainDeadline: v.DrainDeadline,
		StateCompat:   v.StateCompat,
		PublishedAt:   v.PublishedAt,
	}
}
