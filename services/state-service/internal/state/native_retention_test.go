package state

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func nativePruneRequest(t *testing.T, e *cpHistoryEnv, root string) CheckpointPruneRequest {
	t.Helper()
	var req CheckpointPruneRequest
	// JSON keeps this a behavioral RED against the old type: unknown fields
	// are ignored here rather than causing a missing-field compiler failure.
	must := fmt.Sprintf(`{"thread_id":%q,"namespace":"native","limit":1,"preserve_ancestors":true,"protected_roots":[{"namespace":"native","id":%q}]}`, e.threadID, root)
	if err := json.Unmarshal([]byte(must), &req); err != nil {
		t.Fatal(err)
	}
	return req
}

func TestNativeRetentionKeepsPublishedRootAndAllAncestors(t *testing.T) {
	e := cpHistoryFixture(t)
	ids := e.writeChain(t, "native", 6)
	r, err := e.s.PruneCheckpointHistory(e.ctx, e.scope, nativePruneRequest(t, e, ids[5]))
	if err != nil {
		t.Fatal(err)
	}
	if r.Pruned != 0 || r.Kept != 6 || r.TruncatedChains != 0 {
		t.Fatalf("native ancestry was cut: %+v", r)
	}
	for _, id := range ids {
		if _, err := e.s.GetCheckpoint(e.ctx, e.scope, e.threadID, "native", id); err != nil {
			t.Fatalf("lost ancestor %s: %v", id, err)
		}
	}
}

func TestNativeRetentionProtectsOlderPublishedBranchAgainstNewerStaging(t *testing.T) {
	e := cpHistoryFixture(t)
	ids := e.writeChain(t, "native", 5)
	_, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, Checkpoint{ID: "new-unpublished-anchor", Namespace: "native", Version: 6, Parts: []Part{{Channel: "snapshot", Version: "standalone", Data: []byte("unpublished")}}}, "new-stage")
	if err != nil {
		t.Fatal(err)
	}
	r, err := e.s.PruneCheckpointHistory(e.ctx, e.scope, nativePruneRequest(t, e, ids[4]))
	if err != nil {
		t.Fatal(err)
	}
	if r.Pruned != 0 || r.Kept != 6 {
		t.Fatalf("published branch or staged anchor lost: %+v", r)
	}
}

func TestNativeRetentionCanReclaimOldChainAfterIndependentAnchor(t *testing.T) {
	e := cpHistoryFixture(t)
	e.writeChain(t, "native", 5)
	_, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, Checkpoint{ID: "published-anchor", Namespace: "native", Version: 6, Parts: []Part{{Channel: "snapshot", Version: "anchor", Data: []byte("full native history")}}}, "anchor-stage")
	if err != nil {
		t.Fatal(err)
	}
	r, err := e.s.PruneCheckpointHistory(e.ctx, e.scope, nativePruneRequest(t, e, "published-anchor"))
	if err != nil {
		t.Fatal(err)
	}
	if r.Pruned != 5 || r.Kept != 1 || r.TruncatedChains != 0 {
		t.Fatalf("independent old chain not reclaimed: %+v", r)
	}
}

func TestNativeRetentionRefusesUnknownProtectedRootBeforeAnyDeletion(t *testing.T) {
	e := cpHistoryFixture(t)
	e.writeChain(t, "native", 4)
	_, err := e.s.PruneCheckpointHistory(e.ctx, e.scope, nativePruneRequest(t, e, "unknown-or-foreign"))
	if contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("missing protected root not refused: %v", err)
	}
	got, err := e.s.ListCheckpoints(e.ctx, e.scope, e.threadID, "native", 100)
	if err != nil || len(got) != 4 {
		t.Fatalf("failed retention changed history: %d, %v", len(got), err)
	}
}

func TestNativeRetentionKeepsAncestorsOfPendingWrites(t *testing.T) {
	e := cpHistoryFixture(t)
	ids := e.writeChain(t, "native", 4)
	if err := e.s.PutWrites(e.ctx, e.scope, e.fence, "native", ids[3], []PendingWrite{{Task: "pending", Index: 0, Channel: "native", Data: []byte("unfinished")}}); err != nil {
		t.Fatal(err)
	}
	_, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, Checkpoint{ID: "other-root", Namespace: "native", Version: 5, Parts: []Part{{Channel: "snapshot", Version: "other", Data: []byte("independent")}}}, "other")
	if err != nil {
		t.Fatal(err)
	}
	r, err := e.s.PruneCheckpointHistory(e.ctx, e.scope, nativePruneRequest(t, e, "other-root"))
	if err != nil {
		t.Fatal(err)
	}
	if r.Pruned != 0 || r.Kept != 5 || r.TruncatedChains != 0 {
		t.Fatalf("pending native work lost dependencies: %+v", r)
	}
}

func TestNativeRetentionRequiresExplicitValidRootSelection(t *testing.T) {
	e := cpHistoryFixture(t)
	e.writeChain(t, "native", 3)
	for _, fields := range []string{
		`"preserve_ancestors":true`,
		`"protected_roots":[{"namespace":"native","id":"cp-02"}]`,
		`"preserve_ancestors":true,"protected_roots":[{"namespace":"other","id":"cp-02"}]`,
	} {
		var req CheckpointPruneRequest
		if err := json.Unmarshal([]byte(fmt.Sprintf(`{"thread_id":%q,"namespace":"native","limit":1,%s}`, e.threadID, fields)), &req); err != nil {
			t.Fatal(err)
		}
		_, err := e.s.PruneCheckpointHistory(e.ctx, e.scope, req)
		if contracts.Code(err) != contracts.ErrInvalid {
			t.Fatalf("invalid root selection accepted: %s %v", fields, err)
		}
	}
	got, err := e.s.ListCheckpoints(e.ctx, e.scope, e.threadID, "native", 100)
	if err != nil || len(got) != 3 {
		t.Fatalf("invalid retention requests changed history: %d %v", len(got), err)
	}
}
