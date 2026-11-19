package compute

import (
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/netpolicy"
)

// MaxPTYsPerWorkspace is the default interactive-terminal budget of a single
// workspace (SBX-03). Each workspace gets its OWN PTY quota applied strictly
// inside its microVM, so exhausting terminals in one workspace can never
// evict another workspace's session (the OpenAI-Codex LRU footgun where the
// 65th PTY killed a neighbour over a shared global cap of 64).
const MaxPTYsPerWorkspace = 64

// Hard ceilings applied to class X regardless of any requested quota. X runs
// arbitrary untrusted code, so a tenant can never widen its own envelope past
// these values (the request may only tighten them).
const (
	xMaxCPUMillisPerSec int32 = 2000
	xMaxMemoryMB        int32 = 4096
	xMaxPIDs            int32 = 8192
	xMaxOpenFiles       int32 = 65536
	xMaxStorageMB       int32 = 10240
	xMaxSessionTTL            = time.Hour
)

// ClassDefaults returns the resource envelope for a security class. Zero is
// never returned; every class has a concrete, defensible default.
func ClassDefaults(c Class) Quota {
	switch c {
	case ClassS:
		// Trusted multi-tenant Agents Space runtime: its own node pool, may be
		// generous, and may hold a personal Computer for a full working day.
		return Quota{
			MaxCPUMillisPerSec: 8000, MaxMemoryMB: 16384, MaxPIDs: 65536,
			MaxPTYs: MaxPTYsPerWorkspace, MaxOpenFiles: 1048576,
			MaxStorageMB: 204800, MaxSessionTTL: 24 * time.Hour,
		}
	case ClassC:
		// Developer code per deployment: bounded but usable for builds.
		return Quota{
			MaxCPUMillisPerSec: 4000, MaxMemoryMB: 8192, MaxPIDs: 32768,
			MaxPTYs: MaxPTYsPerWorkspace, MaxOpenFiles: 262144,
			MaxStorageMB: 51200, MaxSessionTTL: 4 * time.Hour,
		}
	default: // ClassX
		// Arbitrary code in a microVM: the tightest envelope, and the shortest
		// sleep timer so an idle sandbox is reclaimed quickly.
		return Quota{
			MaxCPUMillisPerSec: xMaxCPUMillisPerSec, MaxMemoryMB: xMaxMemoryMB,
			MaxPIDs: xMaxPIDs, MaxPTYs: MaxPTYsPerWorkspace,
			MaxOpenFiles: xMaxOpenFiles, MaxStorageMB: xMaxStorageMB,
			MaxSessionTTL: xMaxSessionTTL,
		}
	}
}

// ResolveQuota merges a request's partial quota with the class defaults: a
// zero field means "use the default", a non-zero field narrows it. For class
// X every field is additionally clamped to the hard ceilings so a caller can
// only ever tighten, never widen, the untrusted envelope.
func ResolveQuota(c Class, req Quota) Quota {
	d := ClassDefaults(c)
	q := Quota{
		MaxCPUMillisPerSec: firstPositive(req.MaxCPUMillisPerSec, d.MaxCPUMillisPerSec),
		MaxMemoryMB:        firstPositive(req.MaxMemoryMB, d.MaxMemoryMB),
		MaxPIDs:            firstPositive(req.MaxPIDs, d.MaxPIDs),
		MaxPTYs:            firstPositive(req.MaxPTYs, d.MaxPTYs),
		MaxOpenFiles:       firstPositive(req.MaxOpenFiles, d.MaxOpenFiles),
		MaxStorageMB:       firstPositive(req.MaxStorageMB, d.MaxStorageMB),
		MaxSessionTTL:      req.MaxSessionTTL,
	}
	if q.MaxSessionTTL <= 0 || q.MaxSessionTTL > d.MaxSessionTTL {
		q.MaxSessionTTL = d.MaxSessionTTL
	}
	if c == ClassX {
		q = clamp(q, Quota{
			MaxCPUMillisPerSec: xMaxCPUMillisPerSec, MaxMemoryMB: xMaxMemoryMB,
			MaxPIDs: xMaxPIDs, MaxPTYs: MaxPTYsPerWorkspace,
			MaxOpenFiles: xMaxOpenFiles, MaxStorageMB: xMaxStorageMB,
			MaxSessionTTL: xMaxSessionTTL,
		})
	}
	return q
}

func firstPositive(req, def int32) int32 {
	if req > 0 && req < def {
		return req
	}
	return def
}

// clamp takes the per-field minimum, never above the ceiling.
func clamp(q, ceiling Quota) Quota {
	return Quota{
		MaxCPUMillisPerSec: minPos(q.MaxCPUMillisPerSec, ceiling.MaxCPUMillisPerSec),
		MaxMemoryMB:        minPos(q.MaxMemoryMB, ceiling.MaxMemoryMB),
		MaxPIDs:            minPos(q.MaxPIDs, ceiling.MaxPIDs),
		MaxPTYs:            minPos(q.MaxPTYs, ceiling.MaxPTYs),
		MaxOpenFiles:       minPos(q.MaxOpenFiles, ceiling.MaxOpenFiles),
		MaxStorageMB:       minPos(q.MaxStorageMB, ceiling.MaxStorageMB),
		MaxSessionTTL:      minDur(q.MaxSessionTTL, ceiling.MaxSessionTTL),
	}
}

func minPos(a, b int32) int32 {
	if a > 0 && a < b {
		return a
	}
	return b
}

func minDur(a, b time.Duration) time.Duration {
	if a > 0 && a < b {
		return a
	}
	return b
}

// EgressPolicy returns the outbound-network policy enforced for a class. It is
// deliberately the same shape netpolicy.NewClient consumes so a class policy
// compiles directly into an egress-guarded http.Client.
//
//   - Class X: deny-all. X traffic may only leave through the Blind Credential
//     Egress Proxy outside the VM; nothing inside the sandbox reaches the
//     network directly, and the metadata IP 169.254.169.254 is unreachable by
//     construction (netpolicy forbids it regardless).
//   - Class C: a modest allow-list of public build/package hosts over https.
//   - Class S: like C but may additionally reach registered internal service
//     hosts over PrivateHosts (still never loopback/metadata/link-local).
func EgressPolicy(c Class) netpolicy.Policy {
	switch c {
	case ClassS:
		return netpolicy.Policy{
			AllowedHosts: []string{"proxy.golang.org", "pypi.org", "registry.npmjs.org"},
			PrivateHosts: []string{},
		}
	case ClassC:
		return netpolicy.Policy{
			AllowedHosts: []string{"proxy.golang.org", "pypi.org", "registry.npmjs.org"},
		}
	default: // ClassX — deny-all, blind egress via the external proxy only.
		return netpolicy.Policy{}
	}
}

// JailRoots lists the filesystem roots a Computer may access. Everything else
// is denied by the backend (chroot/bind/unshare in the host backend, and by
// the VM's own mounts in the microVM backend). The roots key off the profile:
//
//   - personal  -> the employee's persistent HOME volume;
//   - project   -> the project workspace volume /workspace/<ws_id>;
//   - ephemeral -> a throwaway scratch root.
//
// The base argument is the volume mount base the backend is configured with.
func JailRoots(profile Profile, base, subject, workspaceID string) []string {
	switch profile {
	case ProfilePersonal:
		return []string{base + "/home/" + subject}
	case ProfileProject:
		return []string{base + "/workspace/" + workspaceID}
	default: // ProfileEphemeral
		return []string{base + "/scratch"}
	}
}

// CheckComputerAccess is a defense-in-depth class check applied again at the
// API boundary even though the store already refuses it: classes C and X must
// never hold or act upon a personal Computer. X additionally may never carry a
// realm (personal memory / coordinator identity).
func CheckComputerAccess(class Class, profile Profile, realm string) error {
	if !PersonalRequiresTrustedClass(profile, class) {
		return contracts.NewError(contracts.ErrForbidden,
			"compute: only the trusted class S may hold a personal Computer")
	}
	if class == ClassX && realm != "" {
		return contracts.NewError(contracts.ErrForbidden,
			"compute: class X may not bind a personal realm")
	}
	return nil
}

// CheckPTYBudget rejects opening a new PTY once the workspace already holds
// its quota. active is the count of live PTY leases on the Computer; the new
// session is the active+1'th and is refused when it would exceed the envelope.
func CheckPTYBudget(q Quota, active int) error {
	lim := q.MaxPTYs
	if lim <= 0 {
		lim = MaxPTYsPerWorkspace
	}
	if int64(active)+1 > int64(lim) {
		return contracts.NewError(contracts.ErrConflict,
			"compute: workspace PTY quota exhausted; close a session first")
	}
	return nil
}

// CheckProcessBudget rejects dispatching a command when the class quota caps
// concurrent commands at zero (a degenerate, denied envelope).
func CheckProcessBudget(q Quota) error {
	if q.MaxPIDs <= 0 {
		return contracts.NewError(contracts.ErrForbidden,
			"compute: quota forbids spawning processes")
	}
	return nil
}
