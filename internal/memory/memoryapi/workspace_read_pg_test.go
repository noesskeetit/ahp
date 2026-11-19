package memoryapi

import (
	"context"
	"fmt"
	"testing"

	"github.com/ahp-platform/ahp/internal/memory"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

func TestWorkspaceReadCannotTaintOrReadAnotherProject(t *testing.T) {
	var emitted []labels.Set
	s, h, scope := operationEnv(t, memory.WithWorkspaces(func(context.Context, contracts.Scope) ([]string, error) { return []string{"selected", "other"}, nil }),
		memory.WithReadLabels(func(_ context.Context, _ contracts.Scope, set labels.Set) error {
			emitted = append(emitted, set)
			return nil
		}))
	ids := map[string]string{}
	for _, workspace := range []string{"selected", "other"} {
		p, err := s.Propose(t.Context(), scope, memory.ProposeRequest{Kind: memory.KindWorkspace, Workspace: workspace, Text: "record in " + workspace, OpKey: "p-" + workspace, Evidence: []memory.Evidence{{SourceKind: "user_statement"}}})
		if err != nil {
			t.Fatal(err)
		}
		ids[workspace] = p.Memory.ID
	}
	emitted = nil
	operationCall(t, h, scope, "Read", fmt.Sprintf(`{"id":%q,"workspace":"selected"}`, ids["selected"]), 200)
	if len(emitted) != 1 {
		t.Fatal("selected source was not journaled")
	}
	emitted = nil
	operationCall(t, h, scope, "Read", fmt.Sprintf(`{"id":%q,"workspace":"selected"}`, ids["other"]), 404)
	if len(emitted) != 0 {
		t.Fatal("foreign project source reached label journal")
	}
}
