package execution_test

// evidence_jsonl_test.go -- the block contract asks for MACHINE-READABLE evidence,
// not only t.Log prose: "evidence .jsonl: http-коды, task id, ссылки на
// артефакт/tool-след, версия кандидата". A director verifying a live leg must be
// able to diff it against the database without reading a test log.
//
// Discipline, the same as for every other real dependency in this package: the
// evidence file is a real deliverable. If the run declares AHP_EVIDENCE_REQUIRE=1
// and no path was given, the leg FAILS with the reason instead of quietly
// producing a green run and no artifact. AHP_CANDIDATE_SHA stamps every line with
// the candidate the numbers belong to -- with -buildvcs=false the binary carries
// no VCS revision, so an unstamped line would be an evidence line about nobody.

import (
	"encoding/json"
	"os"
	"runtime/debug"
	"strings"
	"testing"
	"time"
)

const (
	evidencePathEnv = "AHP_EVIDENCE_JSONL"
	evidenceRequire = "AHP_EVIDENCE_REQUIRE"
	evidenceCandEnv = "AHP_CANDIDATE_SHA"
)

// candidateIdentity is the candidate the evidence describes: the operator-stamped
// sha when the runner set one, otherwise the build's own VCS revision, otherwise
// the literal "unknown" -- never a guess.
func candidateIdentity(t *testing.T) string {
	t.Helper()
	if v := strings.TrimSpace(os.Getenv(evidenceCandEnv)); v != "" {
		return v
	}
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			if s.Key == "vcs.revision" && s.Value != "" {
				return s.Value
			}
		}
	}
	t.Log("EVIDENCE_CANDIDATE_UNKNOWN: set " + evidenceCandEnv + " so evidence lines name the candidate they describe")
	return "unknown"
}

// writeEvidence appends one JSON line describing one accepted claim. It records
// what the wire actually said -- the caller passes the codes, ids and artifact
// references it observed, so the line is a transcript, not a summary.
func writeEvidence(t *testing.T, leg string, fields map[string]any) {
	t.Helper()
	path := strings.TrimSpace(os.Getenv(evidencePathEnv))
	if path == "" {
		if strings.TrimSpace(os.Getenv(evidenceRequire)) == "1" {
			t.Fatalf("missing real deliverable: %s=1 asks for machine-readable evidence for leg %q but %s is unset; export %s=<path.jsonl> (the acceptance runner does) or drop the require flag",
				evidenceRequire, leg, evidencePathEnv, evidencePathEnv)
		}
	}
	rec := map[string]any{
		"ts":        time.Now().UTC().Format(time.RFC3339Nano),
		"leg":       leg,
		"test":      t.Name(),
		"candidate": candidateIdentity(t),
	}
	for k, v := range fields {
		rec[k] = v
	}
	line, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal evidence line: %v", err)
	}
	if path == "" {
		t.Logf("EVIDENCE_JSONL_UNSET (line printed, no file): %s", line)
		return
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open evidence file %s: %v", path, err)
	}
	defer f.Close()
	if _, err := f.Write(append(line, '\n')); err != nil {
		t.Fatalf("append evidence line to %s: %v", path, err)
	}
	t.Logf("EVIDENCE %s", line)
}
