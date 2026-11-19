package deploycontroller

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Command contract test; this deliberately does not claim a Kubernetes apply.
func TestApplyRequiresExplicitContextAndDryRunsBeforeMutation(t *testing.T) {
	dir := t.TempDir()
	trace := filepath.Join(dir, "trace")
	fake := filepath.Join(dir, "kubectl")
	script := "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$AHP_APPLY_TRACE\"\ncat >/dev/null\ncase \"$*\" in *--dry-run=server*) exit 1;; esac\n"
	if err := os.WriteFile(fake, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("AHP_APPLY_TRACE", trace)
	objects := []map[string]any{{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": "ahp-owned"}}}
	if err := Apply(context.Background(), "", objects); err == nil {
		t.Fatal("implicit context admitted")
	}
	if _, err := os.Stat(trace); !os.IsNotExist(err) {
		t.Fatal("kubectl invoked before explicit context")
	}
	if err := Apply(context.Background(), "isolated-test", objects); err == nil {
		t.Fatal("dry-run refusal ignored")
	}
	raw, err := os.ReadFile(trace)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 1 || !strings.Contains(lines[0], "--context isolated-test") || !strings.Contains(lines[0], "--dry-run=server") {
		t.Fatalf("mutated after refused dry-run: %q", raw)
	}
}
