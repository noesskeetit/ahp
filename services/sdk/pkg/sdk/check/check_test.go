package check

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCheckAllDetectsDirectDB(t *testing.T) {
	dir := t.TempDir()
	src := `package agent

import "github.com/jackc/pgx/v5"

var _ = pgx.Connect
`
	filepath.Walk(dir, func(p string, i os.FileInfo, e error) error { return nil })
	if err := os.WriteFile(filepath.Join(dir, "db.go"), []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	result, err := CheckAll(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Violations) == 0 {
		t.Fatal("no violations found")
	}
	if result.Violations[0].Rule != RuleDirectDB {
		t.Errorf("rule = %q, want %q", result.Violations[0].Rule, RuleDirectDB)
	}
	if result.Violations[0].Severity != Error {
		t.Error("severity should be Error")
	}
}

func TestCheckAllDetectsExec(t *testing.T) {
	dir := t.TempDir()
	src := `package agent

import "os/exec"

var _ = exec.Command
`
	if err := os.WriteFile(filepath.Join(dir, "run.go"), []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	result, err := CheckAll(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Violations) == 0 {
		t.Fatal("no violations found")
	}
	if result.Violations[0].Rule != RuleSandboxToken {
		t.Errorf("rule = %q, want %q", result.Violations[0].Rule, RuleSandboxToken)
	}
}

func TestCheckAllCleanFile(t *testing.T) {
	dir := t.TempDir()
	src := `package agent

import (
    sdkstate "github.com/ahp-platform/ahp/sdk/go/state"
    "encoding/json"
)

var _ = sdkstate.RunDone
var _ = json.Marshal
`
	if err := os.WriteFile(filepath.Join(dir, "agent.go"), []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	result, err := CheckAll(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Violations) != 0 {
		t.Errorf("unexpected violations: %+v", result.Violations)
	}
}
