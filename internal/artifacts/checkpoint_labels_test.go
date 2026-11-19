package artifacts

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

func labeledCheckpointEnv(t *testing.T, targetContext ...string) *checkpointTestEnv {
	t.Helper()
	e := newCheckpointTestEnv(t)
	e.store.grantLabels = func(context.Context, contracts.Scope) (labels.Set, error) {
		return labels.Set{Subjects: []string{e.scope.Subject}, Workspaces: []string{"ws-a", "ws-b"}}, nil
	}
	contextID := e.thread.ContextID
	if len(targetContext) > 0 && targetContext[0] != "" {
		contextID = targetContext[0]
	}
	e.ref, e.upload = e.sealBoundWithKey(t, Binding{ContextID: contextID, TaskID: "task"}, "labeled", "labeled-checkpoint")
	e.checkpoint.Metadata = checkpointLabelsDescriptor(t, e.ref, e.ref.Labels)
	e.store.grantLabels = nil
	return e
}

func checkpointLabelsDescriptor(t *testing.T, ref ArtifactRef, set labels.Set) []byte {
	t.Helper()
	var descriptor map[string]any
	if err := json.Unmarshal(checkpointDescriptor(ref), &descriptor); err != nil {
		t.Fatal(err)
	}
	descriptor["labels"] = set
	raw, err := json.Marshal(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestCheckpointRejectsChangedStoredLabels(t *testing.T) {
	for _, route := range []string{"metadata", "part", "pending", "batch", "import"} {
		t.Run(route, func(t *testing.T) {
			target := ""
			if route == "import" {
				target = "import-" + contracts.NewID()
			}
			e := labeledCheckpointEnv(t, target)
			ctx := context.Background()
			raw := checkpointLabelsDescriptor(t, e.ref, labels.Set{})
			cp := state.Checkpoint{ID: "invalid", Version: 1, Metadata: raw, MetadataEncoding: state.CheckpointArtifactEncoding}
			var err error
			switch route {
			case "metadata":
				_, err = e.state.PutCheckpoint(ctx, e.scope, e.fence, cp, "invalid")
			case "part":
				cp.Metadata, cp.MetadataEncoding = nil, ""
				cp.Parts = []state.Part{{Channel: "ch", Version: "v1", Data: raw, Encoding: state.CheckpointArtifactEncoding}}
				_, err = e.state.PutCheckpoint(ctx, e.scope, e.fence, cp, "invalid")
			case "pending":
				err = e.state.PutWrites(ctx, e.scope, e.fence, "", cp.ID, []state.PendingWrite{{Task: "pending", Channel: "ch", Data: raw, Encoding: state.CheckpointArtifactEncoding}})
			case "batch":
				_, err = e.state.PutBatch(ctx, e.scope, e.fence, state.BatchRequest{Key: "invalid", Checkpoint: &cp})
			case "import":
				_, err = e.state.ImportThread(ctx, e.scope, state.ControlExport{Thread: e.thread, Checkpoints: []state.Checkpoint{cp}}, target)
			}
			if contracts.Code(err) != contracts.ErrInvalid {
				t.Fatalf("changed stored labels accepted: %v", err)
			}
			if _, err := e.state.GetCheckpoint(ctx, e.scope, e.thread.ID, "", cp.ID); contracts.Code(err) != contracts.ErrNotFound {
				t.Fatalf("invalid descriptor committed checkpoint: %v", err)
			}
			threads, _, err := e.state.ListThreads(ctx, e.scope, "", 100)
			if err != nil {
				t.Fatal(err)
			}
			for _, thread := range threads {
				checkpoints, err := e.state.ListCheckpoints(ctx, e.scope, thread.ID, "", 100)
				if err != nil || len(checkpoints) != 0 {
					t.Fatalf("invalid descriptor committed in thread %s: %+v %v", thread.ID, checkpoints, err)
				}
			}
			writes, err := e.state.GetWrites(ctx, e.scope, e.thread.ID, "", cp.ID)
			if err != nil || len(writes) != 0 || e.pinCount(t) != 0 {
				t.Fatalf("invalid descriptor left writes or pins: %v %v", writes, err)
			}
		})
	}
}

func TestCheckpointStoredLabelsAreCanonicalAndIgnoreLiveGrantLabels(t *testing.T) {
	e := labeledCheckpointEnv(t)
	ctx := context.Background()
	// Equivalent sets may be ordered differently and contain duplicates.
	equivalent := labels.Set{Subjects: []string{e.scope.Subject, e.scope.Subject}, Workspaces: []string{"ws-b", "ws-a", "ws-b"}, Taints: []string{}}
	cp := e.checkpoint
	cp.Metadata = checkpointLabelsDescriptor(t, e.ref, equivalent)
	cp.Parts = []state.Part{{Channel: "ch", Version: "v1", Data: e.checkpoint.Metadata, Encoding: state.CheckpointArtifactEncoding}}
	// Admission reads the new restriction, but descriptor identity remains the
	// labels frozen when the artifact was sealed, not EffectiveLabels.
	e.store.grantLabels = func(context.Context, contracts.Scope) (labels.Set, error) {
		return labels.Set{Workspaces: []string{"added-after-seal"}}, nil
	}
	if _, err := e.state.PutCheckpoint(ctx, e.scope, e.fence, cp, "canonical"); err != nil {
		t.Fatalf("canonical immutable descriptor changed with current grant: %v", err)
	}
	actual, err := e.store.Get(ctx, e.scope, e.ref.ID)
	if err != nil || len(actual.Labels.Workspaces) != 2 || len(actual.EffectiveLabels.Workspaces) != 3 {
		t.Fatalf("test did not distinguish immutable and effective labels: %+v %v", actual, err)
	}
	cp.ID = "effective-is-not-identity"
	cp.Metadata = checkpointLabelsDescriptor(t, e.ref, actual.EffectiveLabels)
	if _, err := e.state.PutCheckpoint(ctx, e.scope, e.fence, cp, cp.ID); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("effective labels accepted as stored identity: %v", err)
	}
	// Current authorization still applies independently to a fresh reference.
	e.store.grantLabels = func(context.Context, contracts.Scope) (labels.Set, error) {
		return labels.Set{Subjects: []string{"foreign"}}, nil
	}
	cp.ID = "revoked-labels"
	cp.Metadata = e.checkpoint.Metadata
	if _, err := e.state.PutCheckpoint(ctx, e.scope, e.fence, cp, cp.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("current label denial bypassed: %v", err)
	}
}
