package artifacts

import (
	"context"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

func TestUploadOperationKeyBindsContextAndTask(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	req := UploadRequest{Name: "same.txt", MIME: "text/plain", SHA256: strings.Repeat("a", 64), Size: 4}
	binding := Binding{ContextID: "context-a", TaskID: "task-a"}
	first, err := e.store.BeginUploadWithBinding(ctx, e.scope, binding, "same-key", req)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := e.store.BeginUploadWithBinding(ctx, e.scope, binding, "same-key", req)
	if err != nil || first.ID != replay.ID {
		t.Fatalf("exact replay changed: %v", err)
	}
	for _, changed := range []Binding{{ContextID: "context-b", TaskID: "task-a"}, {ContextID: "context-a", TaskID: "task-b"}, {}} {
		_, err = e.store.BeginUploadWithBinding(ctx, e.scope, changed, "same-key", req)
		if contracts.Code(err) != contracts.ErrKeyReuse {
			t.Errorf("different binding %+v reused old artifact: %v", changed, err)
		}
	}
	var original Binding
	if err = e.store.pool.QueryRow(ctx, `SELECT context_id,task_id FROM ahp_artifacts.artifacts WHERE id=$1`, first.ID).Scan(&original.ContextID, &original.TaskID); err != nil {
		t.Fatal(err)
	}
	if original != binding {
		t.Fatalf("artifact was reattributed: %+v", original)
	}
}

func TestLegacyUploadDigestOnlyReplaysItsPersistedBinding(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	req := UploadRequest{Name: "legacy.txt", MIME: "text/plain", SHA256: strings.Repeat("b", 64), Size: 4}
	binding := Binding{ContextID: "legacy-context", TaskID: "legacy-task"}
	first, err := e.store.BeginUploadWithBinding(ctx, e.scope, binding, "legacy-key", req)
	if err != nil {
		t.Fatal(err)
	}
	oldDigest, err := requestDigest(req, labels.Set{Subjects: []string{e.scope.Subject}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = e.store.pool.Exec(ctx, `UPDATE ahp_artifacts.artifacts SET request_digest=$2 WHERE id=$1`, first.ID, oldDigest); err != nil {
		t.Fatal(err)
	}
	replay, err := e.store.BeginUploadWithBinding(ctx, e.scope, binding, "legacy-key", req)
	if err != nil || replay.ID != first.ID {
		t.Fatalf("exact legacy binding replay: %v", err)
	}
	_, err = e.store.BeginUploadWithBinding(ctx, e.scope, Binding{ContextID: "different", TaskID: binding.TaskID}, "legacy-key", req)
	codeIs(t, err, contracts.ErrKeyReuse)
	empty, err := e.store.BeginUpload(ctx, e.scope, "legacy-empty", req)
	if err != nil {
		t.Fatal(err)
	}
	if replay, err = e.store.BeginUpload(ctx, e.scope, "legacy-empty", req); err != nil || replay.ID != empty.ID {
		t.Fatalf("empty binding replay changed: %v", err)
	}
	_, err = e.store.BeginUploadWithBinding(ctx, e.scope, binding, "legacy-empty", req)
	codeIs(t, err, contracts.ErrKeyReuse)
}
