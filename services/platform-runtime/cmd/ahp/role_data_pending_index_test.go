//go:build ahp_data

package main

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// DEF-STAND-1 follow-up: a data plane that mounted WITHOUT its durable
// idempotency index is safe to serve from (upload idempotency is held by the
// advisory lock) but must never be silent about it, or the operator reads a
// green start as "index installed".

type fakePendingIndex bool

func (f fakePendingIndex) IdempotencyIndexPending() bool { return bool(f) }

func TestIdempotencyIndexPendingIsSurfacedAtMount(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(".", "role_data.go"))
	if err != nil {
		t.Fatalf("read mount source: %v", err)
	}
	src := string(raw)
	// The mount call, not the helper definition: a helper nobody calls protects
	// nothing.
	const call = "warnIdempotencyIndexPending(s.logger, artStore)"
	if !strings.Contains(src, call) {
		t.Fatal("buildDataHandler must surface a pending idempotency index at start")
	}
	at := strings.Index(src, "artStore.Migrate(ctx)")
	warn := strings.Index(src, call)
	if at < 0 || warn < 0 || warn < at {
		t.Fatal("the warning must follow the migration that can leave the index pending")
	}
}

func TestWarnIdempotencyIndexPending(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))

	if warnIdempotencyIndexPending(log, fakePendingIndex(false)) {
		t.Fatal("nothing must be warned while the index is installed")
	}
	if buf.Len() != 0 {
		t.Fatalf("a healthy migration must be silent, logged: %s", buf.String())
	}
	buf.Reset()
	if !warnIdempotencyIndexPending(log, fakePendingIndex(true)) {
		t.Fatal("a pending index must be reported")
	}
	line := buf.String()
	for _, want := range []string{"WARN", "idempotency", "advisory"} {
		if !strings.Contains(line, want) {
			t.Fatalf("the warning must name the condition, the guarantee that holds, and log at WARN; got: %s", line)
		}
	}
	// A nil logger must not break a start, and must not swallow the answer.
	if !warnIdempotencyIndexPending(nil, fakePendingIndex(true)) {
		t.Fatal("the condition must still be reported even when nobody asked for logs")
	}
	if warnIdempotencyIndexPending(log, nil) {
		t.Fatal("no store means nothing to report")
	}
}
