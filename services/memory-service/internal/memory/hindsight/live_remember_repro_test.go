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

// TestLivePersonalRememberRetellsCodeWord is the minimal repro for DEF-DOTE2E-12.
// It drives the real product memory path (the same hindsight.Client the durable
// worker runs) against the AHP-owned live Hindsight: one personal Retain whose
// content is exactly the shape that broke j5 - two sentences, the second storing
// a code word only "for this conversation" - then the same personal Recall the
// j5 leg's Search performs. Before the fix the operation reported completed but
// the code word never became a recallable fact (extraction kept only the first
// sentence); the mission/mode fix must make the code word come back. The old
// behaviour was captured on the live server as token_kept=false.
//
// It also proves the fix does NOT weaken noise discipline: a runtime status line
// must still never become memory (this is the same property the live round-trip
// test depends on), so the j5 private/noise/taint isolation behaviour is unchanged.
//
// Requires the handbook-owned Hindsight, exactly like TestLiveNativeRoundTrip:
//
//	export AHP_MEMORY_HINDSIGHT_URL=http://127.0.0.1:38888
func TestLivePersonalRememberRetellsCodeWord(t *testing.T) {
	origin := strings.TrimSpace(os.Getenv("AHP_MEMORY_HINDSIGHT_URL"))
	if origin == "" {
		t.Skip("AHP_MEMORY_HINDSIGHT_URL is not set: the AHP-owned Hindsight is an external prerequisite")
	}
	c, err := hindsight.New(origin, os.Getenv("AHP_MEMORY_HINDSIGHT_API_KEY"))
	if err != nil {
		t.Fatal(err)
	}
	var rnd [6]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		t.Fatal(err)
	}
	token := "journey-otter-" + hex.EncodeToString(rnd[:])
	bank := memory.Bank{ID: "zeno-j5-" + hex.EncodeToString(rnd[:]), Scope: memory.Personal}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := c.Configure(ctx, bank); err != nil {
		t.Fatalf("configure personal bank: %v", err)
	}
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dcancel()
		_ = c.DeleteBank(dctx, bank.ID)
	})
	// The exact j5 payload: a stable first sentence plus an explicitly stored,
	// conversation-scoped code word as a second sentence.
	content := "The employee's personal Dot keeps research notes. The employee stores the journey code word " + token + " for this conversation."
	job := memory.Job{
		ID: "op-" + hex.EncodeToString(rnd[:]), Kind: "retain", BankID: bank.ID, Scope: memory.Personal,
		Identity: memory.Identity{TenantID: "zeno-j5", UserID: "u-zeno-j5"}, Content: content, CreatedAt: time.Now().UTC(),
	}
	if err := c.Retain(ctx, bank, job); err != nil {
		t.Fatalf("retain: %v", err)
	}
	status := "pending"
	for deadline := time.Now().Add(2 * time.Minute); time.Now().Before(deadline); {
		status, err = c.Status(ctx, bank.ID, job.ID)
		if err != nil {
			t.Fatalf("status: %v", err)
		}
		if status != "pending" && status != "processing" && status != "not_found" {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if strings.Contains(strings.ToLower(status), "fail") {
		t.Fatalf("retain errored: %s", status)
	}

	// The j5 read side: recall the personal bank the same way Search does, and
	// also list, until the explicitly stored code word comes back.
	deadline := time.Now().Add(2 * time.Minute)
	found := ""
	// reachedBank counts memory_units the extractor actually produced for THIS
	// retain (matched by the operation id the client stamps into unit metadata).
	// A positive count proves the content reached the bank and was processed, so
	// a missing token is "the extractor dropped it", not "the bank never got it".
	// This is exactly the distinction the fix must make observable.
	reachedBank := 0
	for time.Now().Before(deadline) && found == "" {
		recalled, err := c.Recall(ctx, bank, "What is the employee's journey code word?", 800)
		if err != nil {
			t.Fatalf("recall: %v", err)
		}
		for _, f := range recalled {
			if strings.Contains(f.Text, token) {
				found = f.Text
			}
		}
		if found == "" {
			listed, _, err := c.List(ctx, bank, 50, 0)
			if err != nil {
				t.Fatalf("list: %v", err)
			}
			for _, f := range listed {
				if f.Metadata["operation_id"] == job.ID {
					reachedBank++
				}
				if strings.Contains(f.Text, token) {
					found = f.Text
				}
			}
		}
		if found == "" {
			time.Sleep(3 * time.Second)
		}
	}
	if found == "" {
		if reachedBank > 0 {
			t.Fatalf("content reached the bank (%d memory_units for this retain) but the extractor dropped the stored code word: the retain acked %q while the fact was silently lost", reachedBank, status)
		}
		t.Fatalf("the personal Dot could not retell the code word and no memory_unit reached the bank for this retain (status=%s): upstream did not process the write", status)
	}

	// Noise discipline must survive the fix: a runtime status line must never
	// become memory, exactly as the live round-trip test asserts.
	noise := memory.Job{
		ID: "noise-" + hex.EncodeToString(rnd[:]), Kind: "retain", BankID: bank.ID, Scope: memory.Personal,
		Identity: memory.Identity{TenantID: "zeno-j5", UserID: "u-zeno-j5"},
		Content:  "The zeno canary value is plumbed through Hindsight live on the handbook instance.", CreatedAt: time.Now().UTC(),
	}
	if err := c.Retain(ctx, bank, noise); err != nil {
		t.Fatalf("retain noise: %v", err)
	}
	for deadline := time.Now().Add(90 * time.Second); time.Now().Before(deadline); {
		st, err := c.Status(ctx, bank.ID, noise.ID)
		if err != nil {
			t.Fatalf("noise status: %v", err)
		}
		if st != "pending" && st != "processing" {
			break
		}
		time.Sleep(2 * time.Second)
	}
	time.Sleep(3 * time.Second)
	after, _, err := c.List(ctx, bank, 50, 0)
	if err != nil {
		t.Fatalf("list after noise: %v", err)
	}
	for _, f := range after {
		l := strings.ToLower(f.Text)
		if strings.Contains(l, "canary") || strings.Contains(l, "handbook") {
			t.Fatalf("the fix let runtime noise become memory: %q", f.Text)
		}
	}
	t.Logf("j5 repro green: code word retold=%q, noise excluded from %d memories", found, len(after))
}
