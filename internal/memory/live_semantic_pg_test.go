//go:build ahp_data && ahp_semantic_live

package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/memory/embed"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// TestLiveSemanticRetrieval requires actual HTTP embeddings and an isolated
// pgvector database. It deliberately has no skip/fake path. All sent text is
// synthetic. Negative namespaces are seeded with their own explicit policies.
func TestLiveSemanticRetrieval(t *testing.T) {
	pool := isolatedMemDB(t)
	ctx := context.Background()
	backend, err := embed.FromEnv()
	mustOK(t, err)
	if backend == nil {
		t.Fatal("actual embedding endpoint is mandatory")
	}
	t.Logf("actual provider: %s identity=%s (configured revision, not provider weight attestation)", backend.Describe(), backend.Identity())
	makeStore := func(scope contracts.Scope) *Store {
		policy := map[string]any{"version": 1, "providers": []any{map[string]any{"name": "synthetic", "subject_from_scope": true, "audience": map[string]any{"kind": "private"}, "secret_ref": "AHP_MEMORY_EMBEDDING_KEY_FILE", "models": []string{backend.Model()}}}, "routes": []any{map[string]any{"name": "synthetic-only", "when": map[string]any{"tenants": []string{scope.Tenant}}, "then": map[string]any{"provider": "synthetic", "model": backend.Model()}}}}
		b, _ := json.Marshal(policy)
		path := filepath.Join(t.TempDir(), "policy.json")
		mustOK(t, os.WriteFile(path, b, 0600))
		t.Setenv(embed.EnvPolicyFile, path)
		t.Setenv(embed.EnvProvider, "synthetic")
		gate, err := embed.PolicyFromEnv(backend)
		mustOK(t, err)
		s, err := New(pool, nil, dimTestAuth, nil, WithEmbedding(backend), WithEmbeddingPolicy(gate), WithLabels(func(context.Context, contracts.Scope) (labels.Set, error) { return labels.Set{}, nil }), WithReadLabels(func(context.Context, contracts.Scope, labels.Set) error { return nil }))
		mustOK(t, err)
		mustOK(t, s.Migrate(ctx))
		return s
	}
	alice := contracts.Scope{Tenant: "synthetic-memory-demo", Deployment: "semantic-demo", Subject: "synthetic-alice"}
	legacy, err := New(pool, nil, dimTestAuth, nil)
	mustOK(t, err)
	mustOK(t, legacy.Migrate(ctx))
	legacyNote, err := legacy.Propose(ctx, alice, ProposeRequest{Kind: KindUser, Text: "Legacy operational note: approved weekly checklist.", OpKey: "legacy-canonical", SourceKind: "user_statement", Evidence: []Evidence{{SourceKind: "user_statement", Detail: "synthetic legacy evidence"}}, Provenance: map[string]any{"synthetic": true, "origin": "legacy-lexical"}})
	mustOK(t, err)
	_, err = legacy.Review(ctx, alice, legacyNote.Memory.ID, DecisionApprove, "approve-legacy")
	mustOK(t, err)
	canonicalBefore := canonicalMemoryDigest(t, ctx, pool, legacyNote.Memory.ID)
	if got := columnType(ctx, t, pool); got != "vector(64)" {
		t.Fatalf("legacy width=%s", got)
	}
	store := makeStore(alice)
	if got := columnType(ctx, t, pool); got != "vector(1024)" {
		t.Fatalf("upgraded width=%s", got)
	}
	legacyIndexed, err := store.Reindex(ctx, alice, ReindexRequest{OpKey: "reindex-legacy"})
	mustOK(t, err)
	if legacyIndexed.Indexed != 1 || canonicalMemoryDigest(t, ctx, pool, legacyNote.Memory.ID) != canonicalBefore {
		t.Fatal("legacy canonical memory not preserved through migration and actual embedding")
	}
	t.Logf("legacy existing row vector(64) NULL -> vector(1024) actual embedding; canonical digest preserved=%s", canonicalBefore)
	put := func(s *Store, scope contracts.Scope, key, text string, active bool) Memory {
		p, err := s.Propose(ctx, scope, ProposeRequest{Kind: KindUser, Text: text, OpKey: key, SourceKind: "verified_observation", Provenance: map[string]any{"synthetic": true}})
		mustOK(t, err)
		if !active {
			return p.Memory
		}
		m, err := s.Review(ctx, scope, p.Memory.ID, DecisionApprove, "approve-"+key)
		mustOK(t, err)
		return m
	}
	cases := []struct{ text, query string }{
		{"The physician prescribed a remedy for the illness.", "doctor medication disease"},
		{"A canine chased a tennis ball across the lawn.", "dog fetching toy garden"},
		{"Сотрудник предпочитает трудиться из квартиры.", "работник выбирает удалёнку дома"},
		{"Покупателю вернули деньги за сломанный холодильник.", "refund broken appliance"},
	}
	ids := make([]string, len(cases))
	for i, c := range cases {
		ids[i] = put(store, alice, fmt.Sprintf("fact-%d", i), c.text, true).ID
	}
	// Identical private and tenant-foreign facts would beat a semantic paraphrase
	// if the index were allowed to grant access.
	bob := alice
	bob.Subject = "synthetic-bob"
	bobStore := store // Same backend, same operator policy, two verified subjects.
	hidden := put(bobStore, bob, "private", cases[0].query, true)
	foreign := alice
	foreign.Tenant = "synthetic-foreign"
	foreignStore := makeStore(foreign)
	foreignFact := put(foreignStore, foreign, "foreign", cases[0].query, true)
	pending := put(store, alice, "pending", cases[0].query, false)
	deleted := put(store, alice, "deleted", cases[0].query, true)
	mustOK(t, store.Delete(ctx, alice, deleted.ID, "delete-one"))
	expired := put(store, alice, "expired", cases[0].query, true)
	_, err = pool.Exec(ctx, `UPDATE ahp_memory.memories SET ttl=now()-interval '1 second' WHERE id=$1`, expired.ID)
	mustOK(t, err)
	superseded := put(store, alice, "superseded", cases[0].query, true)
	replacement, err := store.Propose(ctx, alice, ProposeRequest{Kind: KindUser, Text: "A sailor navigates the ocean.", OpKey: "replacement", Supersedes: superseded.ID})
	mustOK(t, err)
	_, err = store.Review(ctx, alice, replacement.Memory.ID, DecisionApprove, "approve-replacement")
	mustOK(t, err)
	for i, c := range cases {
		lexical, err := store.lexCandidates(ctx, alice, nil, KindUser, c.query)
		mustOK(t, err)
		for _, id := range lexical {
			if id == ids[i] {
				t.Fatalf("case %d target has lexical overlap", i)
			}
		}
		started := time.Now()
		got, err := store.Search(ctx, alice, SearchRequest{Query: c.query, Kind: KindUser, Limit: 10})
		mustOK(t, err)
		if got.Mode != "hybrid" || len(got.Hits) == 0 || got.Hits[0].Memory.ID != ids[i] {
			t.Fatalf("case %d target not top semantic result: %+v", i, got)
		}
		for _, h := range got.Hits {
			for _, id := range []string{hidden.ID, foreignFact.ID, pending.ID, deleted.ID, expired.ID, superseded.ID} {
				if h.Memory.ID == id {
					t.Fatalf("hidden/stale source emitted: %s", id)
				}
			}
		}
		t.Logf("semantic case=%d lexical_target_matches=0 top_id=%s score=%.8f elapsed=%s", i, got.Hits[0].Memory.ID, got.Hits[0].Score, time.Since(started))
	}
	// Bob uses the SAME runtime/policy, with his own subject-bound audience.
	bobLex, err := store.lexCandidates(ctx, bob, nil, KindUser, cases[0].text)
	mustOK(t, err)
	for _, id := range bobLex {
		if id == hidden.ID {
			t.Fatal("Bob semantic target has lexical overlap")
		}
	}
	bobResult, err := store.Search(ctx, bob, SearchRequest{Query: cases[0].text, Kind: KindUser, Limit: 10})
	mustOK(t, err)
	if len(bobResult.Hits) != 1 || bobResult.Hits[0].Memory.ID != hidden.ID {
		t.Fatalf("Bob private semantic result incorrect: %+v", bobResult)
	}
	_, err = store.Get(ctx, bob, ids[0])
	mustErr(t, err, contracts.ErrNotFound)
	_, err = store.Get(ctx, alice, hidden.ID)
	mustErr(t, err, contracts.ErrNotFound)
	t.Log("same backend + same operator policy: Alice and Bob own semantic retrieval PASS, bidirectional private Read denied")
	// Existing lexical rows are only indexed by an explicit idempotent mutation.
	_, err = pool.Exec(ctx, `UPDATE ahp_memory.memories SET embedding=NULL,embedding_space='' WHERE id=$1`, ids[0])
	mustOK(t, err)
	rebuilt, err := store.Reindex(ctx, alice, ReindexRequest{OpKey: "reindex-explicit"})
	mustOK(t, err)
	if rebuilt.Indexed != 1 {
		t.Fatalf("reindex=%+v", rebuilt)
	}
	replay, err := store.Reindex(ctx, alice, ReindexRequest{OpKey: "reindex-explicit"})
	mustOK(t, err)
	if !replay.Replay || replay.Indexed != 1 {
		t.Fatalf("replay=%+v", replay)
	}
	t.Log("actual embeddings + pgvector paraphrases + RU/EN + tenant/subject/lifecycle isolation + explicit reindex passed")
}
