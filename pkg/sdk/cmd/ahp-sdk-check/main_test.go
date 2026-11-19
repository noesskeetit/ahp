package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCLIExitCodes(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "ahp-sdk-check")
	if out, err := exec.Command("go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build checker: %v %s", err, out)
	}
	for _, tc := range []struct {
		name, source string
		code         int
	}{
		{"clean", "package agent\nfunc build(){}", 0},
		{"syntax", "package agent\nfunc secret_source_canary( {", 2},
		{"network", "package agent\nimport web \"net/http\"\nfunc build(){web.Get(\"https://example.invalid\")}", 1},
		{"disk", "package agent\nimport \"os\"\nfunc build(){os.WriteFile(\"durable\",nil,0600)}", 1},
		{"missing", "", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(dir, tc.name+".go")
			if tc.source != "" {
				if err := os.WriteFile(path, []byte(tc.source), 0600); err != nil {
					t.Fatal(err)
				}
			}
			out, err := exec.Command(binary, path).CombinedOutput()
			code := 0
			if err != nil {
				if exit, ok := err.(*exec.ExitError); ok {
					code = exit.ExitCode()
				} else {
					t.Fatal(err)
				}
			}
			if code != tc.code {
				t.Fatalf("exit=%d want=%d output=%s", code, tc.code, out)
			}
			if strings.Contains(string(out), "secret_source_canary") {
				t.Fatal("checker echoed malformed source")
			}
		})
	}
}
