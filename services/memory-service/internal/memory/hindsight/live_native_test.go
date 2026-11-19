package hindsight_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/memory/hindsight"
	memory "github.com/ahp-platform/ahp/internal/memory/longterm"
)

// TestLiveNativeRoundTrip drives the real Hindsight server: configure a bank,
// retain a fact, wait for the native operation to settle, then recall it.
//
// The rest of this package proves the client contract against a recorded
// transport, which cannot show that a running native server accepts the shapes.
// This test needs the handbook-owned Hindsight and refuses to pretend otherwise:
//
//	docker compose -p ahp-handbook -f infra/memory/compose.hindsight.yaml \
//	  --env-file /path/to/ignored/memory.env up -d
//	export AHP_MEMORY_HINDSIGHT_URL=http://127.0.0.1:38888
//
// The personal Hindsight service on 127.0.0.1:8889 and its data must never be
// used for product verification.
func TestLiveNativeRoundTrip(t *testing.T) {
	origin := strings.TrimSpace(os.Getenv("AHP_MEMORY_HINDSIGHT_URL"))
	if origin == "" {
		t.Skip("AHP_MEMORY_HINDSIGHT_URL is not set: the handbook-owned Hindsight is an external prerequisite (see the command in this test's doc comment)")
	}
	c, err := hindsight.New(origin, os.Getenv("AHP_MEMORY_HINDSIGHT_API_KEY"))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	var rnd [6]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		t.Fatal(err)
	}
	bank := memory.Bank{ID: "zeno-live-" + hex.EncodeToString(rnd[:]), Scope: memory.Personal}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if err := c.Configure(ctx, bank); err != nil {
		t.Fatalf("configure bank: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = c.DeleteBank(ctx, bank.ID)
	})

	job := memory.Job{
		ID:        "op-" + hex.EncodeToString(rnd[:]),
		Kind:      "retain",
		BankID:    bank.ID,
		Scope:     memory.Personal,
		Identity:  memory.Identity{TenantID: "zeno-live", UserID: "u-zeno-live"},
		Content:   "I prefer Russian-language answers and I do local development on macOS.",
		CreatedAt: time.Now().UTC(),
	}
	if err := c.Retain(ctx, bank, job); err != nil {
		t.Fatalf("retain: %v", err)
	}

	status := "pending"
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		status, err = c.Status(ctx, bank.ID, job.ID)
		if err != nil {
			t.Fatalf("status: %v", err)
		}
		if status != "pending" && status != "processing" && status != "not_found" {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if strings.Contains(strings.ToLower(status), "fail") || strings.Contains(strings.ToLower(status), "error") {
		t.Fatalf("native retain did not succeed: status=%q (the Hindsight LLM endpoint is an external prerequisite)", status)
	}

	// Extraction and auto-consolidation are asynchronous on the native server:
	// the operation reports completed before the fact is listable, and
	// consolidation rewrites the raw world fact as an observation some seconds
	// later. Reading once the instant the queue drains is a race, not a check.
	var (
		facts    []memory.NativeFact
		total    int
		recalled []memory.NativeFact
	)
	gone := time.Now().Add(2 * time.Minute)
	for {
		facts, total, err = c.List(ctx, bank, 20, 0)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		recalled, err = c.Recall(ctx, bank, "zeno canary plumbed", 800)
		if err != nil {
			t.Fatalf("recall: %v", err)
		}
		if total > 0 && len(recalled) > 0 {
			break
		}
		if time.Now().After(gone) {
			t.Fatalf("native server accepted the write but produced no readable memory: status=%s listed=%d recall=%d", status, total, len(recalled))
		}
		time.Sleep(3 * time.Second)
	}
	types := make([]string, 0, len(facts))
	for _, f := range facts {
		types = append(types, f.Type)
	}
	t.Logf("native bank %s: status=%s listed=%d types=%v recall=%d", bank.ID, status, total, types, len(recalled))
	// Extraction is the native LLM's decision under the bank mission, so the
	// assertion is on meaning, not on the wording that was sent.
	if !strings.Contains(strings.ToLower(recalled[0].Text), "macos") &&
		!strings.Contains(strings.ToLower(recalled[0].Text), "russian") {
		t.Fatalf("native recall answered an unrelated memory: %q", recalled[0].Text)
	}

	// Mission discipline in the other direction: the product mission says to
	// extract nothing when there is no durable user fact. A runtime status line
	// must therefore complete cleanly and still leave the bank unchanged - the
	// old fixture used exactly such noise and looked like a silent data loss.
	before := make(map[string]bool, len(facts))
	for _, f := range facts {
		before[f.Text] = true
	}
	noise := memory.Job{
		ID:        "noise-" + hex.EncodeToString(rnd[:]),
		Kind:      "retain",
		BankID:    bank.ID,
		Scope:     memory.Personal,
		Identity:  memory.Identity{TenantID: "zeno-live", UserID: "u-zeno-live"},
		Content:   "The zeno canary value is plumbed through Hindsight live on the handbook instance.",
		CreatedAt: time.Now().UTC(),
	}
	if err := c.Retain(ctx, bank, noise); err != nil {
		t.Fatalf("retain noise: %v", err)
	}
	noiseState, err := c.Status(ctx, bank.ID, noise.ID)
	if err != nil {
		t.Fatalf("noise status: %v", err)
	}
	for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline); {
		noiseState, err = c.Status(ctx, bank.ID, noise.ID)
		if err != nil {
			t.Fatalf("noise status: %v", err)
		}
		if noiseState != "pending" && noiseState != "processing" {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if strings.Contains(strings.ToLower(noiseState), "fail") || strings.Contains(strings.ToLower(noiseState), "error") {
		t.Fatalf("noise retain errored instead of extracting nothing: %s", noiseState)
	}
	after, afterTotal, err := c.List(ctx, bank, 20, 0)
	if err != nil {
		t.Fatalf("list after noise: %v", err)
	}
	// The count is not the assertion: auto-consolidation turns the earlier world
	// facts into observations, so the population grows on its own. What must not
	// appear is any memory derived from the noise.
	for _, f := range after {
		lowered := strings.ToLower(f.Text)
		if strings.Contains(lowered, "canary") || strings.Contains(lowered, "handbook") {
			t.Fatalf("the mission let runtime noise become memory: %q", f.Text)
		}
	}
	t.Logf("noise retain: status=%s listed=%d/%d (no noise-derived memory, %d memories predate it)", noiseState, len(after), afterTotal, len(before))
}
