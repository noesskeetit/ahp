package harness

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// LiveStep is what a viewer needs the moment a rung finishes, before the
// whole run has an artifact: the previous design printed only p95/rps to the
// log, so a live table could not show p50/p99, sampler density or machine
// load, and the audience could not tell a dense step from a starved one.
type LiveStep struct {
	AtUTC     string         `json:"at_utc"`
	Phase     string         `json:"phase"`
	Step      StepResult     `json:"step"`
	Load1     *float64       `json:"load1"`
	ProgressR string         `json:"progress_rps,omitempty"`
	Extra     map[string]any `json:"extra,omitempty"`
}

// Load1 returns the 1-minute load average, or nil where the platform does not
// expose one (never 0: an unread metric must not read as an idle machine).
func Load1() *float64 {
	if runtime.GOOS == "darwin" || runtime.GOOS == "freebsd" {
		out, err := exec.Command("sysctl", "-n", "vm.loadavg").Output()
		if err != nil {
			return nil
		}
		f := strings.Fields(strings.TrimSpace(string(out)))
		if len(f) == 0 {
			return nil
		}
		if v, err := strconv.ParseFloat(strings.Trim(f[0], "{}"), 64); err == nil {
			return &v
		}
		return nil
	}
	b, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return nil
	}
	f := strings.Fields(string(b))
	if len(f) == 0 {
		return nil
	}
	if v, err := strconv.ParseFloat(f[0], 64); err == nil {
		return &v
	}
	return nil
}

// AppendLiveStep appends one rung to <dir>/<stem>-live.jsonl and returns the
// path. It is append-only and flushed per record: a viewer tailing the file
// sees rungs while the run is still in flight, and the file survives a crash
// of the run, so partial evidence is never lost.
func AppendLiveStep(dir, stem, phase string, s StepResult, extra map[string]any) string {
	if dir == "" {
		return ""
	}
	rec := LiveStep{AtUTC: time.Now().UTC().Format(time.RFC3339Nano), Phase: phase, Step: s, Load1: Load1(), Extra: extra}
	f, err := os.OpenFile(filepath.Join(dir, stem+"-live.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return ""
	}
	defer f.Close()
	b, err := json.Marshal(rec)
	if err != nil {
		return ""
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		return ""
	}
	return f.Name()
}
