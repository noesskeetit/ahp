package routing_test

// Deficit legs for PDF rev3.2 :1620-1630 (Model Gateway owns "маршруты с учётом
// меток" and "копии файлов у провайдера") and for the cost read-face the
// acceptance needs (:347 "стоимость задачи", observability without content).
//
// These legs are written against the tree as it stands, so they are RED until the
// product grows the thing they ask for -- which is the point: an absent
// capability must fail a leg, not be argued about in a report. Each leg reads
// only files and the schema contract, so it runs anywhere, with or without a
// database.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// repoFile walks up from the test's package directory to the module root, so the
// leg works from any checkout (mine, the canonical one, a CI clone).
func repoFile(t *testing.T, rel string) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("wd: %v", err)
	}
	for i := 0; i < 12; i++ {
		candidate := filepath.Join(dir, rel)
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			t.Fatalf("%s does not exist at the module root %s", rel, dir)
		}
		dir = filepath.Dir(dir)
	}
	t.Fatal("could not find the module root")
	return ""
}

// repoExistsInModule reports whether rel exists under the module root.
func repoExistsInModule(t *testing.T, rel string) (string, bool) {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("wd: %v", err)
	}
	for i := 0; i < 12; i++ {
		if st, err := os.Stat(filepath.Join(dir, rel)); err == nil && !st.IsDir() {
			return filepath.Join(dir, rel), true
		}
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return "", false
		}
		dir = filepath.Dir(dir)
	}
	return "", false
}

// TestProviderConfigHasASingleSourcePoint: the Model Gateway is described in
// infrastructure files today by copying values around, so the acceptance cannot
// answer "which providers and models does this deployment route to" from one
// place. This leg asks for that one place: a committed provider table that loads
// and validates.
func TestProviderConfigHasASingleSourcePoint(t *testing.T) {
	path, ok := repoExistsInModule(t, "infra/model/providers.json")
	if !ok {
		t.Fatal(`no provider config point: infra/model/providers.json does not exist; provider names, hosts and model ids are duplicated across scripts, tests and stand files (see reports, item 3)`)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read provider config: %v", err)
	}
	if len(strings.TrimSpace(string(raw))) == 0 {
		t.Fatal("provider config is empty")
	}
	// A config point that carries a credential is not a config point, it is a
	// leak. Only references are allowed.
	for _, forbidden := range []string{"api_key\"", "\"key\":", "Bearer ", "sk-"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("provider config carries what looks like a credential fragment %q; only references are allowed", forbidden)
		}
	}
}

// TestModelGatewayIsNotLabelBlind: PDF :1620-1630 gives the Model Gateway
// "маршруты с учётом меток". The platform already has one label vocabulary
// (pkg/labels: subjects, workspaces, taints) and already refuses writes to a
// destination that may not receive them. A provider is exactly such a
// destination -- external -- so a generation whose data carries a label the
// provider may not receive must not be sent to it.
//
// The leg is structural: it fails while no file in the model plane even names
// the label vocabulary, which is the measured state (zero references), and it
// passes once routing consults it.
func TestModelGatewayIsNotLabelBlind(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("wd: %v", err)
	}
	// walk up to module root
	for i := 0; i < 12; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			break
		}
		dir = filepath.Dir(dir)
	}
	var consulted []string
	for _, root := range []string{
		filepath.Join(dir, "internal", "model"),
		filepath.Join(dir, "internal", "execbridge"),
	} {
		_ = filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return nil
			}
			raw, rerr := os.ReadFile(p)
			if rerr != nil {
				return nil
			}
			if strings.Contains(string(raw), "pkg/labels") || strings.Contains(string(raw), "labels.CheckWrite") {
				consulted = append(consulted, strings.TrimPrefix(p, dir+"/"))
			}
			return nil
		})
	}
	if len(consulted) == 0 {
		t.Fatal(`label-aware routing is missing: no file under internal/model or internal/execbridge references the platform label vocabulary (pkg/labels / labels.CheckWrite), so a request carrying taints (external, secret) routes to a provider exactly as an unlabelled one does`)
	}
}

// TestWholeTreeCostHasAReadModel: the acceptance has to compare one quantity at
// both ends of A->B, and the UI has to show a cost the ledger really holds.
//
// What already exists is BudgetStore.TreeSnapshot: one fail-closed read of a
// tree's budget nodes, given the ROOT BUDGET ID, with totals that cannot
// double-count. What the acceptance and the UI cannot do with it is the actual
// question: they hold a TASK id, one task can carry several budget trees of
// different denominations (tokens for a task root, steps for a run), and the
// budget ledger alone cannot say how many generations or how many input/output
// tokens produced the charge. So the leg asks for the read model that composes
// those three things, and fails while none of it is joined anywhere.
func TestWholeTreeCostHasAReadModel(t *testing.T) {
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("wd: %v", err)
	}
	for i := 0; i < 12; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			break
		}
		dir = filepath.Dir(dir)
	}
	var treeWalk, joinsGenerations, resolvesByTask bool
	root := filepath.Join(dir, "internal", "model")
	_ = filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		raw, rerr := os.ReadFile(p)
		if rerr != nil {
			return nil
		}
		text := string(raw)
		if strings.Contains(text, "WITH RECURSIVE") {
			treeWalk = true
		}
		if strings.Contains(text, "ahp_model.generations") && strings.Contains(text, "usage_prompt") {
			joinsGenerations = true
		}
		if strings.Contains(text, "a2a-task:") || strings.Contains(text, "treedelegate-run:") {
			resolvesByTask = true
		}
		return nil
	})
	if !treeWalk {
		t.Fatal("no tree walk in the model plane at all")
	}
	if !joinsGenerations {
		t.Fatal(`no whole-tree cost read model: nothing in internal/model joins the budget tree with ahp_model.generations, so a tree total cannot report steps or tokens_in/tokens_out, and nothing can state whether what was charged equals what the provider actually used`)
	}
	if !resolvesByTask {
		t.Fatal(`the cost read model cannot be asked by task: the tree read needs a root budget id, while the acceptance and the UI hold a task id (a2a-task:<id>, treedelegate-run:<id>), and no read model in the model plane resolves those names`)
	}
}
