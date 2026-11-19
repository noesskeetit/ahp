package compute

import "time"

type PatchRequest struct {
	ComputerID   string `json:"computer_id"`
	Branch       string `json:"branch,omitempty"`
	Path         string `json:"path"`
	ExpectedHash string `json:"expected_hash,omitempty"`
	ContentB64   string `json:"content_b64"`
	Mode         int    `json:"mode"`
}

type BranchRequest struct {
	ComputerID string `json:"computer_id"`
	Branch     string `json:"branch,omitempty"`
	Name       string `json:"name,omitempty"`
}

type StateRevision struct {
	ID         string               `json:"id"`
	ComputerID string               `json:"computer_id"`
	Seq        int64                `json:"seq"`
	Branch     string               `json:"branch"`
	ParentID   string               `json:"parent_id,omitempty"`
	BaseSeq    int64                `json:"base_seq"`
	Tree       map[string]StateFile `json:"tree"`
	Digest     string               `json:"digest"`
	CreatedAt  time.Time            `json:"created_at"`
}

type StateFile struct {
	Hash string `json:"hash"`
	Size int64  `json:"size"`
	Mode int    `json:"mode"`
}

type ProcessKill struct {
	PID  int  `json:"pid"`
	PGID int  `json:"pgid"`
	Dead bool `json:"dead"`
	// Zombie mirrors compute.ProcessKill: the process was proven dead as a
	// zombie (killed, waiting to be reaped) rather than by its pid being freed.
	Zombie bool `json:"zombie,omitempty"`
}

type SleepReport struct {
	ComputerID string        `json:"computer_id"`
	Status     string        `json:"status"`
	LeasesHeld int           `json:"leases_held"`
	Processes  []ProcessKill `json:"processes"`
	SleptAt    time.Time     `json:"slept_at"`
}

type CostWindow struct {
	ID        string     `json:"id"`
	StartedAt time.Time  `json:"started_at"`
	EndedAt   *time.Time `json:"ended_at"`
}

type CostReport struct {
	ComputerID string       `json:"computer_id"`
	AwakeNanos int64        `json:"awake_ns"`
	AwakeHuman string       `json:"awake_human"`
	Open       bool         `json:"open_window"`
	Windows    []CostWindow `json:"windows"`
}

type WriterLeaseRequest struct {
	ComputerID string `json:"computer_id"`
	Holder     string `json:"holder"`
	TTLms      int64  `json:"ttl_ms"`
}

type ActivityLeaseRequest struct {
	ComputerID string `json:"computer_id"`
	Kind       string `json:"kind"`
	Holder     string `json:"holder"`
	TTLms      int64  `json:"ttl_ms"`
}

type LeaseInfo struct {
	Kind   string `json:"kind"`
	Holder string `json:"holder"`
}

type ActivityLeases struct {
	Leases []LeaseInfo `json:"leases"`
	Count  int         `json:"count"`
}

type InstallCommandRequest struct {
	ComputerID string `json:"computer_id"`
	RevisionID string `json:"revision_id"`
	Command    string `json:"command"`
}

type InstallResult struct {
	ExitCode int    `json:"exit_code"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
}

type EnvironmentFiles struct {
	CurrentRevision int64     `json:"current_revision"`
	Files           []EnvFile `json:"files"`
}

type InstallSession struct {
	Revision EnvRevision `json:"revision"`
	EnvDir   string      `json:"env_dir"`
}

type Version struct {
	ID            string     `json:"id"`
	Agent         string     `json:"agent"`
	Digest        string     `json:"digest"`
	Status        string     `json:"status"`
	DrainDeadline *time.Time `json:"drain_deadline,omitempty"`
	StateCompat   bool       `json:"state_compat"`
	PublishedAt   time.Time  `json:"published_at"`
}
type EnvFile struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	Digest string `json:"digest"`
}
type PublishVersionRequest struct {
	Agent         string `json:"agent"`
	Digest        string `json:"digest"`
	StateCompat   bool   `json:"state_compat"`
	DrainWindowMS int64  `json:"drain_window_ms"`
}
type DrainVersionRequest struct {
	Agent         string `json:"agent"`
	Digest        string `json:"digest"`
	DrainWindowMS int64  `json:"drain_window_ms"`
}
