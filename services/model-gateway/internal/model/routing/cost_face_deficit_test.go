package routing_test

// Deficit leg for the read face the acceptance asked for (PDF rev3.2 :347
// "стоимость задачи"; the measured complaint from the board: POST
// /v1/space/GetTurn in a terminal state carries no cost/usage/tokens, so the UI
// can only show tariff text).
//
// TreeCostByTask (accepted) answers the question inside the process. Nothing
// answers it over HTTP, which means a verifier still has to read Postgres by
// hand and the UI still cannot show the number. This leg fails while the model
// plane exposes no such face, so the gap stays a fact and not an opinion.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestTreeCostHasAReadOnlyHTTPFace: a mounted face under /v1/model/ that answers
// tree cost, behind the same verified-principal contract the accepted read faces
// use, and with no writer next to it.
func TestTreeCostHasAReadOnlyHTTPFace(t *testing.T) {
	root := moduleRoot(t)
	var faceMounted, answersTreeCost, writerPresent bool
	for _, dir := range []string{filepath.Join(root, "internal", "model"), filepath.Join(root, "cmd", "ahp")} {
		_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return nil
			}
			raw, rerr := os.ReadFile(p)
			if rerr != nil {
				return nil
			}
			text := string(raw)
			if strings.Contains(text, `"/v1/model/`) {
				faceMounted = true
			}
			if strings.Contains(text, "TreeCost") && strings.Contains(text, "/v1/model/") {
				answersTreeCost = true
			}
			// A read face must stay read-only: the cost ledger is written by the
			// gateway's own admission path, never by an HTTP handler.
			lower := strings.ToLower(text)
			if strings.Contains(lower, "budget") && strings.Contains(text, "/v1/model/") &&
				(strings.Contains(lower, "reserve(") || strings.Contains(lower, "release(") || strings.Contains(lower, "settle(")) {
				writerPresent = true
			}
			return nil
		})
	}
	if !faceMounted {
		t.Fatal(`no HTTP face under /v1/model/: the tree cost read model exists in-process only, so an independent verifier still reads ahp_model by hand and the UI still has no cost to show`)
	}
	if !answersTreeCost {
		t.Fatal(`the /v1/model/ face does not answer TreeCost: the accepted read model (BudgetStore.TreeCostByTask) is not reachable over HTTP`)
	}
	if writerPresent {
		t.Fatal(`the read face performs reservations/settles: cost accounting is written by the gateway admission path, and an HTTP writer would let a reader fabricate spend`)
	}
}
