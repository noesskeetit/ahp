package artifacts

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestNativeCheckpointDeleteKeepsCommittedRecoveryReadable(t *testing.T) {
	e := newCheckpointTestEnv(t)
	ctx := context.Background()
	if _, err := e.state.PutCheckpoint(ctx, e.scope, e.fence, e.checkpoint, "published"); err != nil {
		t.Fatal(err)
	}
	err := e.store.Delete(ctx, e.scope, e.ref.ID, "attempt-delete")
	if contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("live checkpoint artifact tombstone was allowed: %v", err)
	}
	if _, err := e.store.Get(ctx, e.scope, e.ref.ID); err != nil {
		t.Fatalf("published checkpoint bytes are not readable: %v", err)
	}
	if err := e.state.DeleteThread(ctx, e.scope, e.thread.ID); err != nil {
		t.Fatal(err)
	}
	if err := e.store.Delete(ctx, e.scope, e.ref.ID, "after-retirement"); err != nil {
		t.Fatalf("retired checkpoint permanently blocked deletion: %v", err)
	}
}

func TestNativeCheckpointDeleteRefusesUnknownReferenceAuthority(t *testing.T) {
	e := newCheckpointTestEnv(t)
	ctx := context.Background()
	e.store.keep = ReferenceScanFunc(func(context.Context, string, []ArtifactQuery) (map[string]bool, error) {
		return nil, errors.New("owned reference reader unavailable")
	})
	err := e.store.Delete(ctx, e.scope, e.ref.ID, "unavailable")
	if contracts.Code(err) != contracts.ErrUnavailable {
		t.Fatalf("unavailable reference owner permitted tombstone: %v", err)
	}
	if _, err := e.store.Get(ctx, e.scope, e.ref.ID); err != nil {
		t.Fatalf("failed reference lookup changed readability: %v", err)
	}
}

func TestNativeCheckpointDeleteHonorsProvisionalPin(t *testing.T) {
	for _, cascade := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "cascade"}[cascade], func(t *testing.T) {
			e := newCheckpointTestEnv(t)
			ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
			defer cancel()
			pinned, proceed := make(chan struct{}), make(chan struct{})
			e.state.SetCheckpointArtifacts(checkpointFaultGuard{Store: e.store, afterPin: func(context.Context, string) error { close(pinned); <-proceed; return nil }})
			done := make(chan error, 1)
			go func() { _, err := e.state.PutCheckpoint(ctx, e.scope, e.fence, e.checkpoint, "in-flight"); done <- err }()
			select {
			case <-pinned:
			case <-ctx.Done():
				close(proceed)
				t.Fatal("publication did not reach durable pin")
			}
			var deletion error
			if cascade {
				r, err := e.store.DeleteByContext(ctx, e.scope, e.thread.ContextID)
				deletion = err
				if err == nil && (r.Referenced != 1 || r.Tombstoned != 0) {
					deletion = errors.New("cascade tombstoned the provisionally pinned artifact")
				}
			} else {
				err := e.store.Delete(ctx, e.scope, e.ref.ID, "pin-race")
				if contracts.Code(err) != contracts.ErrConflict {
					deletion = errors.New("single tombstone bypassed provisional pin")
				}
			}
			close(proceed)
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("protected publisher failed: %v", err)
				}
			case <-ctx.Done():
				t.Fatal("publisher did not finish")
			}
			if deletion != nil {
				t.Fatal(deletion)
			}
			if _, err := e.store.Get(ctx, e.scope, e.ref.ID); err != nil {
				t.Fatalf("acknowledged checkpoint points to unreadable artifact: %v", err)
			}
		})
	}
}
