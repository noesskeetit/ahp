package artifacts

import (
	"context"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

func TestSealFreezesCurrentGrantLabels(t *testing.T) {
	e := setup(t, WithGrantLabels(func(context.Context, contracts.Scope) (labels.Set, error) {
		return labels.Set{Workspaces: []string{"project-restricted"}}, nil
	}))
	ref, _ := e.uploadSeal(t, "seal-current-labels", "classified checkpoint")
	if len(ref.Labels.Workspaces) != 1 || ref.Labels.Workspaces[0] != "project-restricted" {
		t.Fatal("Seal did not freeze the task's current labels")
	}
	e.store.grantLabels = nil
	read, err := e.store.Get(context.Background(), e.scope, ref.ID)
	if err != nil || len(read.Labels.Workspaces) != 1 {
		t.Fatalf("stored labels lost after grant changed: %+v %v", read.Labels, err)
	}
}
