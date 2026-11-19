package consumer

// The driver is used as a gate by other lanes, so its two contract surfaces are
// pinned here without a server: the usage error that must become exit code 2,
// and the rule that a broken handshake is an error value rather than a process
// that terminates its host.

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunReportsUsageInsteadOfExiting(t *testing.T) {
	for _, args := range [][]string{
		nil,
		{"-handshake", "/tmp/hs.json"},
		{"-handshake", "/tmp/hs.json", "-evidence", "/tmp/e.jsonl"},
	} {
		err := Run(args)
		if !errors.Is(err, ErrUsage) {
			t.Fatalf("Run(%v) = %v, want ErrUsage", args, err)
		}
	}
	// An unknown flag is a command-line failure too, and it must come back as
	// an error, not as os.Exit.
	if err := Run([]string{"-nope"}); err == nil {
		t.Fatal("an unknown flag must be reported")
	}
}

func TestRunReportsABrokenHandshakeAsAnError(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "absent.json")
	if err := Run([]string{"-handshake", missing, "-evidence", filepath.Join(dir, "e.jsonl"), "-worker", "w"}); err == nil {
		t.Fatal("a missing handshake file must be an error")
	} else if !strings.Contains(err.Error(), "handshake") {
		t.Fatalf("the error must say what failed: %v", err)
	}

	incomplete := filepath.Join(dir, "hs.json")
	if err := os.WriteFile(incomplete, []byte(`{"base_url":"https://127.0.0.1:1"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	err := Run([]string{"-handshake", incomplete, "-evidence", filepath.Join(dir, "e.jsonl"), "-worker", "w"})
	if err == nil || !strings.Contains(err.Error(), "base_url/task_token/cert_file") {
		t.Fatalf("a handshake without credentials must be refused precisely, got %v", err)
	}

	notJSON := filepath.Join(dir, "junk.json")
	if err := os.WriteFile(notJSON, []byte("not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Run([]string{"-handshake", notJSON, "-evidence", filepath.Join(dir, "e.jsonl"), "-worker", "w"}); err == nil {
		t.Fatal("a malformed handshake must be an error")
	}
}

func TestContractOperationListIsCompleteAndUnique(t *testing.T) {
	// The gate reports ops_succeeded=N/M against this list, so a duplicate or an
	// empty name would silently inflate the coverage number.
	seen := map[string]bool{}
	for _, op := range contractOps {
		if op == "" {
			t.Fatal("the contract operation list contains an empty name")
		}
		if seen[op] {
			t.Fatalf("duplicate operation in the contract list: %s", op)
		}
		seen[op] = true
	}
	if len(contractOps) < 36 {
		t.Fatalf("the contract has 36 named operations plus ImportThread, list holds %d", len(contractOps))
	}
	for _, required := range []string{"PutKV", "DeleteKV", "GetKV", "SearchKV", "PutWrites",
		"PutCheckpoint", "Heartbeat", "Release", "Suspend", "PutBatch", "ImportThread"} {
		if !seen[required] {
			t.Fatalf("operation %s is missing from the coverage list", required)
		}
	}
}
