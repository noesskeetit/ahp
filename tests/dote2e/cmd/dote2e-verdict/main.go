// Command dote2e-verdict reads the VERDICT FIELD of dot-e2e scenario artifacts and
// exits accordingly. It exists because `go test` returns 0 while the artifact it
// just wrote says verdict=BLOCKED, and the lane already lost an acceptance reading
// to that confusion - the same class as "HTTP 200 means success".
//
// The rule it enforces, in one line: rc=0 from go test is not acceptance; the
// verdict field of the artifact is.
//
// Usage:
//
//	dote2e-verdict [-expect-rev <sha>] <artifact.json>...
//
// Exit codes (distinct on purpose, so a caller can tell blocked from failed):
//
//	0  every artifact is verdict=PASS and admissible as acceptance evidence
//	3  an artifact could not be read/parsed, or is not admissible (dry run, dirty
//	   worktree, no legs, product_revision missing or different from -expect-rev)
//	4  a legitimate artifact says verdict=BLOCKED
//	5  a legitimate artifact says verdict=FAIL, DEFECT or any other non-PASS value
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
)

// exit codes, named so the mapping below cannot drift silently
const (
	exitOK        = 0
	exitUnusable  = 3
	exitBlocked   = 4
	exitNotPassed = 5
)

// verdictDocument is the subset of a scenario artifact this tool reads. It stays
// deliberately small: an acceptor must not need the harness to read a verdict.
type verdictDocument struct {
	Scenario       string         `json:"scenario"`
	Verdict        string         `json:"verdict"`
	ProductRev     string         `json:"product_revision"`
	DryRun         bool           `json:"dry_run"`
	DryRunNote     string         `json:"dry_run_note"`
	Dirty          *bool          `json:"dirty"`
	CompletedLegs  []string       `json:"completed_legs"`
	BlockedLegs    []string       `json:"blocked_legs"`
	FailedLegs     []string       `json:"failed_legs"`
	BlockedReasons map[string]any `json:"blocked_reasons"`
	FailedReasons  map[string]any `json:"failed_reasons"`
	RecordedAt     string         `json:"recorded_at"`
}

// classify answers one artifact: an exit code plus the one line a human needs.
func classify(path, expectRev string) (int, string) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return exitUnusable, fmt.Sprintf("%s: unreadable: %v", path, err)
	}
	var doc verdictDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return exitUnusable, fmt.Sprintf("%s: unparsable: %v", path, err)
	}
	verdict := strings.TrimSpace(doc.Verdict)
	if verdict == "" {
		return exitUnusable, fmt.Sprintf("%s: no verdict field - an artifact without a verdict proves nothing", path)
	}
	if doc.DryRun {
		return exitUnusable, fmt.Sprintf("%s: dry_run=true (%s) - preparation evidence is not acceptance evidence", path, doc.DryRunNote)
	}
	if doc.Dirty == nil {
		return exitUnusable, fmt.Sprintf("%s: dirty is absent - the artifact does not say whether the lane tree was clean", path)
	}
	if *doc.Dirty {
		return exitUnusable, fmt.Sprintf("%s: the lane worktree was dirty at record time", path)
	}
	if len(doc.CompletedLegs) == 0 && verdict == "PASS" {
		return exitUnusable, fmt.Sprintf("%s: verdict=PASS with no completed legs - a PASS with no legs is a bug, not a result", path)
	}
	// Without a product_revision the artifact cannot be attributed to a candidate,
	// so it proves nothing regardless of its verdict (root's W1 lesson: the binary
	// that ran and the binary that was "verified" were different things).
	if strings.TrimSpace(doc.ProductRev) == "" {
		return exitUnusable, fmt.Sprintf("%s: product_revision is absent - the artifact cannot be attributed to a candidate", path)
	}
	if expectRev != "" && doc.ProductRev != expectRev {
		return exitUnusable, fmt.Sprintf("%s: product_revision=%q, expected %q", path, doc.ProductRev, expectRev)
	}
	switch verdict {
	case "PASS":
		return exitOK, fmt.Sprintf("%s: %s verdict=PASS completed=%d blocked=%d failed=%d product_revision=%s",
			path, doc.Scenario, len(doc.CompletedLegs), len(doc.BlockedLegs), len(doc.FailedLegs), doc.ProductRev)
	case "BLOCKED":
		return exitBlocked, fmt.Sprintf("%s: %s verdict=BLOCKED blocked=%v reasons=%s",
			path, doc.Scenario, doc.BlockedLegs, summarise(doc.BlockedReasons))
	default:
		return exitNotPassed, fmt.Sprintf("%s: %s verdict=%s failed=%v reasons=%s",
			path, doc.Scenario, verdict, doc.FailedLegs, summarise(doc.FailedReasons))
	}
}

// expandPathArguments accepts @list.txt as well as plain paths. Artifact names
// under this workspace contain spaces, and a shell word-splitting them would make
// the acceptor read the wrong file - an explicit list file removes that failure.
func expandPathArguments(args []string) []string {
	expanded := []string{}
	for _, arg := range args {
		if !strings.HasPrefix(arg, "@") {
			expanded = append(expanded, arg)
			continue
		}
		raw, err := os.ReadFile(strings.TrimPrefix(arg, "@"))
		if err != nil {
			expanded = append(expanded, arg) // reported as unreadable below
			continue
		}
		for _, line := range strings.Split(string(raw), "\n") {
			if trimmed := strings.TrimSpace(line); trimmed != "" {
				expanded = append(expanded, trimmed)
			}
		}
	}
	return expanded
}

func summarise(reasons map[string]any) string {
	if len(reasons) == 0 {
		return "none recorded"
	}
	keys := make([]string, 0, len(reasons))
	for key := range reasons {
		keys = append(keys, key)
	}
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[j] < keys[i] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%v", key, reasons[key]))
	}
	return strings.Join(parts, "; ")
}

func main() {
	expectRev := flag.String("expect-rev", "", "require product_revision to equal this exact SHA")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: dote2e-verdict [-expect-rev <sha>] <artifact.json>...")
	}
	flag.Parse()
	// Emptiness is decided AFTER list expansion. A @list.txt that exists but is
	// empty, or holds only blank lines, names zero artifacts; deciding on the raw
	// arguments saw one argument and walked an empty expansion, which left the
	// best code it never used - 0. A green verdict over zero artifacts is the same
	// lie this tool already refuses for a PASS with zero legs.
	paths := expandPathArguments(flag.Args())
	if len(paths) == 0 {
		fmt.Fprintln(os.Stderr, "dote2e-verdict: no artifacts named - a verdict over zero artifacts is not a result")
		flag.Usage()
		os.Exit(exitUnusable)
	}
	worst := exitOK
	worse := func(code int) {
		// Ordering: unusable is reported over verdicts (a reader cannot accept an
		// artifact they cannot read), and FAIL outranks BLOCKED.
		rank := map[int]int{exitOK: 0, exitBlocked: 1, exitNotPassed: 2, exitUnusable: 3}
		if rank[code] > rank[worst] {
			worst = code
		}
	}
	for _, path := range paths {
		code, line := classify(path, strings.TrimSpace(*expectRev))
		fmt.Println(line)
		worse(code)
	}
	os.Exit(worst)
}
