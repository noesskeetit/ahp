package compute

// The client half of sleep/resume: what a scheduler on the control plane calls.
// The supervisor answers on the same unix socket as every other class X operation,
// and a refusal is propagated verbatim - the client never converts "this backend
// cannot park a guest" into success.

import (
	"context"
)

// Pause freezes one of this backend's live guests.
func (b *MicroVMBackend) Pause(ctx context.Context, sandboxID string) error {
	return b.post(ctx, "/v1/pause", pauseRequest{SandboxID: sandboxID}, nil)
}

// Resume wakes a guest this backend parked.
func (b *MicroVMBackend) Resume(ctx context.Context, sandboxID string) error {
	return b.post(ctx, "/v1/resume", pauseRequest{SandboxID: sandboxID}, nil)
}

// Snapshot parks a guest to disk and returns the reference its owner must keep to
// wake that same incarnation.
func (b *MicroVMBackend) Snapshot(ctx context.Context, sandboxID string) (SnapshotRef, error) {
	var out snapshotResponse
	if err := b.post(ctx, "/v1/snapshot", pauseRequest{SandboxID: sandboxID}, &out); err != nil {
		return SnapshotRef{}, err
	}
	if out.SandboxID == "" || out.StatePath == "" || out.MemPath == "" {
		return SnapshotRef{}, Unavailable(b.name, "supervisor returned an incomplete snapshot reference")
	}
	return SnapshotRef{
		SandboxID: out.SandboxID, Node: out.Node, Dir: out.Dir, APISocket: out.APISocket,
		StatePath: out.StatePath, MemPath: out.MemPath,
	}, nil
}

// compile-time proof that the microVM client really carries the capability its
// supervisor can serve. The host backend is deliberately absent from this list.
var _ interface {
	Pause(context.Context, string) error
	Resume(context.Context, string) error
} = (*MicroVMBackend)(nil)
