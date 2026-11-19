package compute

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"
)

// Backend is the execution substrate that actually runs a Computer's commands.
// Two real implementations exist: the host backend (a process jail on the
// node, labelled "host:", NOT a microVM) and the microVM backend (a client to
// the Linux Firecracker/Kata supervisor over a unix socket).
//
// Fail-closed contract: a backend that cannot honour a call for environmental
// reasons (no /dev/kvm, no supervisor socket, no Firecracker) MUST return an
// error that satisfies IsUnavailable. Callers treat an unavailable backend as
// a hard failure; they never fabricate a success, and the microVM backend
// never silently falls back to the host backend beneath a "microVM" label.
type Backend interface {
	// Name is the stable backend label stored on an incarnation ("host",
	// "firecracker", "kata"). It is never a misleading "microvm" for host.
	Name() string

	// Probe reports whether this backend can serve work right now. A non-nil
	// return means the backend is unusable in this environment.
	Probe(ctx context.Context) error

	// Acquire starts a disposable sandbox for one incarnation. It returns a
	// handle that identifies the concrete VM / jail. The sandbox is never
	// returned to a warm pool under another identity: it is torn down on
	// Release. The host backend creates a bounded scratch jail here.
	Acquire(ctx context.Context, spec AcquireSpec) (Sandbox, error)

	// Release tears the sandbox down for good.
	Release(ctx context.Context, sandboxID string) error
}

// CommandBackend is implemented by backends that can run a one-shot command.
type CommandBackend interface {
	Exec(ctx context.Context, sb Sandbox, spec ExecSpec) (ExecResult, error)
}

// PTYBackend is implemented by backends that can open an interactive
// pseudoterminal inside a sandbox.
type PTYBackend interface {
	OpenPTY(ctx context.Context, sb Sandbox, spec PTYSpec) (PTYSession, error)
}

// AcquireSpec identifies the incarnation a sandbox is created for and the
// envelope it must be confined to.
type AcquireSpec struct {
	ComputerID  string
	Incarnation int64
	VolumeID    string
	Fence       int64
	Class       Class
	Profile     Profile
	Quota       Quota
	JailRoots   []string
	Node        string
}

// Sandbox is a live, disposable execution environment.
type Sandbox interface {
	// ID is the concrete sandbox / VM identifier (Firecracker VM gen id or a
	// host jail directory id).
	ID() string
	// Backend is the Name of the backend that produced it.
	Backend() string
	// Metadata reports the guest-side node / host the sandbox runs on.
	Node() string
}

// ExecSpec is one command execution inside a sandbox.
type ExecSpec struct {
	Command string
	Cwd     string
	Env     []string
	Stdin   []byte
	Timeout time.Duration
	Quota   Quota
	// Egress, when non-nil, is the outbound network policy the guest must be
	// confined to. A nil egress means "class default" resolved by the caller.
	Egress []string // allow-listed host strings; empty slice = deny-all

	// CommandID names the ledger row this execution is running. It is carried so
	// the process registry can tie a live pid to the command row it belongs to,
	// letting the restart reaper report which dispatched row owns each orphan it
	// kills. Empty for executions that are not a ledger command (PTY shells,
	// environment installs), which is honest: those rows never exist.
	CommandID string
}

// ExecResult is the outcome of one command.
type ExecResult struct {
	Stdout    string
	Stderr    string
	ExitCode  int
	Truncated bool
}

// PTYSpec opens an interactive terminal.
type PTYSpec struct {
	Shell   string
	Cwd     string
	Env     []string
	Cols    uint16
	Rows    uint16
	Timeout time.Duration
	Quota   Quota
}

// PTYSession is a live pseudoterminal. It behaves like a bidirectional byte
// stream with window resize and a blocking Wait.
type PTYSession interface {
	io.ReadWriteCloser
	Resize(cols, rows uint16) error
	Wait() (int, error)
}

// UnavailableError reports a backend that exists but cannot run in this
// environment (no KVM, no supervisor socket). It is deliberately a distinct
// type so that "this host cannot provide a microVM" can never be mistaken for
// a successful execution.
type UnavailableError struct {
	Backend string
	Reason  string
}

func (e *UnavailableError) Error() string {
	return fmt.Sprintf("compute: backend %q unavailable: %s", e.Backend, e.Reason)
}

// IsUnavailable reports whether err is (or wraps) an UnavailableError.
func IsUnavailable(err error) bool {
	var ue *UnavailableError
	return errors.As(err, &ue)
}

// Unavailable builds an UnavailableError.
func Unavailable(backend, reason string) error {
	return &UnavailableError{Backend: backend, Reason: reason}
}

// Registry maps backend names to implementations and resolves which backend a
// security class must use. Class X may ONLY run on a microVM backend; if no
// microVM backend is registered or the registered one is unavailable, class X
// work fails closed rather than degrading to the host backend.
type Registry struct {
	byName     map[string]Backend
	defaultSet map[Class]string
}

// NewRegistry builds a registry from the given backends. classBackend maps a
// class to its preferred backend name; a class with no entry falls back to
// "host" (which is safe for S/C but never used for X, see Require).
func NewRegistry(backends ...Backend) *Registry {
	r := &Registry{byName: make(map[string]Backend, len(backends))}
	for _, b := range backends {
		r.byName[b.Name()] = b
	}
	r.defaultSet = map[Class]string{
		ClassS: "host", ClassC: "host", ClassX: "firecracker",
	}
	return r
}

// Disclosure reports what a caller must be told about the backends this registry
// can hand out: the name of a backend that declared a nature, and the disclosure
// text itself. It returns ("", "") when no backend implements Marker -- a registry
// of ordinary backends has nothing to disclose and the HTTP layer must invent no
// header.
//
// The mock backend is the case this exists for. A mock that is visible only in one
// response field can be forwarded, cached, or stripped and then read as a real
// sandbox, which is the exact overclaim the mock was written not to make. Asking
// the registry (not one handler) keeps the disclosure true for EVERY answer the
// Compute plane gives, including the ones that never reach a backend: an
// unauthenticated call, an unknown sandbox, a refusal.
//
// If an operator ever mounts two disclosing backends, the first one wins and this
// says so -- silently merging two natures into one label would be a claim.
func (r *Registry) Disclosure() (backend, marker string) {
	names := make([]string, 0, len(r.byName))
	for name := range r.byName {
		names = append(names, name)
	}
	sort.Strings(names) // a header must not depend on map order
	for _, name := range names {
		b := r.byName[name]
		if m, ok := b.(Marker); ok {
			if backend != "" && backend != b.Name() {
				// Two different disclosing backends: report the ambiguity rather
				// than a single label that would be true of only one of them.
				return "mixed", m.MockMarker()
			}
			backend = b.Name()
			marker = m.MockMarker()
		}
	}
	return backend, marker
}

// SetClassBackend overrides which backend a class prefers.
func (r *Registry) SetClassBackend(c Class, name string) { r.defaultSet[c] = name }

// Get returns a backend by name.
func (r *Registry) Get(name string) (Backend, bool) {
	b, ok := r.byName[name]
	return b, ok
}

// Require resolves and probes the backend a class must run on, failing closed
// when it is missing or unavailable. For class X it refuses any backend that
// is not a microVM (host is never acceptable for untrusted code).
func (r *Registry) Require(ctx context.Context, c Class) (Backend, error) {
	name := r.defaultSet[c]
	b, ok := r.byName[name]
	if !ok {
		if c == ClassX {
			return nil, Unavailable(name, "no microVM backend registered; class X cannot run on the host backend")
		}
		return nil, Unavailable(name, "backend not registered")
	}
	// Class X runs arbitrary untrusted code and may NEVER be routed to the host
	// backend, even if an operator overrides the class-to-backend binding via
	// SetClassBackend. The refusal is keyed on the resolved backend's own truthful
	// Name(): HostBackend always reports "host". This closes the hole where
	// SetClassBackend(ClassX, "host") would otherwise be honoured.
	if c == ClassX && b.Name() == "host" {
		return nil, Unavailable("host", "class X may not run on the host backend; a microVM backend is required")
	}
	if err := b.Probe(ctx); err != nil {
		return nil, err
	}
	return b, nil
}
