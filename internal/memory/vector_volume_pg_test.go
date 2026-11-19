package memory

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestVectorRetrievalOnRealPostgresAtVolume pins the pgvector leg of PDF rev3.2
// section 5 (гибридный поиск) on a corpus larger than a single screen, against
// the real extension in the lane database. Correctness and isolation, not
// throughput: the deterministic test embedder has no semantic direction, so
// what is proven here is that the vector column, the cosine ordering and the
// admissibility filter all survive volume - ranking quality needs a real
// embedder and stays unproven (see the report's NOT PROVEN section).
func TestVectorRetrievalOnRealPostgresAtVolume(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()

	// If the column were not physically pgvector's type, every cosine operator
	// below would be exercising something else entirely.
	var columnType string
	mustOK(t, e.pool.QueryRow(ctx, `SELECT coalesce(format_type(atttypid, atttypmod),'') FROM pg_attribute
		WHERE attrelid='ahp_memory.memories'::regclass AND attname='embedding'`).Scan(&columnType))
	if !strings.HasPrefix(columnType, "vector") {
		t.Fatalf("embedding column is %q, not a pgvector column", columnType)
	}
	var extVersion string
	if err := e.pool.QueryRow(ctx, `SELECT coalesce(extversion,'') FROM pg_extension WHERE extname='vector'`).Scan(&extVersion); err != nil || extVersion == "" {
		t.Fatalf("pgvector extension is not installed in the lane database: %v", err)
	}
	t.Logf("lane database uses pgvector %s, column type %s", extVersion, columnType)

	seed := func(scope contracts.Scope, count int, tag string) map[string]string {
		t.Helper()
		ids := make(map[string]string, count)
		for i := 0; i < count; i++ {
			text := fmt.Sprintf("%s note %d: release %s rolls back with the canary in region %d", tag, i, tag, i)
			p, err := e.mem.Propose(ctx, scope, ProposeRequest{Kind: KindUser, Text: text, OpKey: tag + "-propose-" + strconv.Itoa(i)})
			mustOK(t, err)
			if _, err := e.mem.Review(ctx, scope, p.Memory.ID, DecisionApprove, tag+"-review-"+strconv.Itoa(i)); err != nil {
				t.Fatalf("review %s/%d: %v", tag, i, err)
			}
			ids[text] = p.Memory.ID
		}
		return ids
	}

	started := time.Now()
	alice := seed(e.scope("alice"), 90, "alice")
	seed(e.scope("bob"), 40, "bob")
	seed(contracts.Scope{Tenant: e.tenant + "-foreign", Deployment: "d-1", Subject: "carol"}, 20, "foreign")
	t.Logf("seeded 150 approved records in %s", time.Since(started).Round(time.Millisecond))

	// Volume changes nothing about the reported mode: an embedder is wired, so
	// the answer must claim hybrid retrieval, and it must not claim more.
	var (
		queried  string
		topID    string
		seenIDs  []string
		res      SearchResult
		searched time.Duration
	)
	for text, id := range alice {
		queried, topID = text, id
		break
	}
	began := time.Now()
	res, err := e.mem.Search(ctx, e.scope("alice"), SearchRequest{Query: queried, Limit: searchMaxN})
	mustOK(t, err)
	searched = time.Since(began)
	if res.Mode != "hybrid" {
		t.Fatalf("an embedder is wired, the answer must say hybrid, got %q", res.Mode)
	}
	if len(res.Hits) == 0 || res.Hits[0].Memory.ID != topID {
		t.Fatalf("the exact-match record must rank first at volume, got %d hits", len(res.Hits))
	}
	for _, h := range res.Hits {
		seenIDs = append(seenIDs, h.Memory.ID)
	}
	t.Logf("search over 150 records answered %d hits in %s", len(res.Hits), searched.Round(time.Millisecond))

	aliceOwned := make(map[string]bool, len(alice))
	for _, id := range alice {
		aliceOwned[id] = true
	}
	for _, id := range seenIDs {
		if !aliceOwned[id] {
			t.Fatalf("ranked search leaked a record alice does not own: %s", id)
		}
	}

	// Enumeration must also hold at volume: a full browse page belongs to alice
	// alone even though bob and a foreign tenant were seeded into the same table.
	browse, err := e.mem.Search(ctx, e.scope("alice"), SearchRequest{Limit: searchMaxN})
	mustOK(t, err)
	if browse.Mode != "browse" {
		t.Fatalf("an empty query must enumerate, got mode %q", browse.Mode)
	}
	if len(browse.Hits) < searchMaxN {
		t.Fatalf("browse returned %d of alice's 90 records, want a full page", len(browse.Hits))
	}
	for _, h := range browse.Hits {
		if !aliceOwned[h.Memory.ID] {
			t.Fatalf("browse leaked a foreign record into alice's page: %s", h.Memory.ID)
		}
	}
	t.Logf("browse returned a full page of %d alice records from a 150-record corpus", len(browse.Hits))

	if searched > 15*time.Second {
		t.Fatalf("retrieval over 150 records took %s, which is a correctness problem not a profile", searched)
	}
}
