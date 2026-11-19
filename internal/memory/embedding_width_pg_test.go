//go:build ahp_data

package memory

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// isolatedMemDB creates a throwaway database so this test can exercise the
// embedding-column lifecycle without touching the schema other memory tests
// share. The role needs CREATEDB; without it the test says so out loud instead
// of passing quietly.
func isolatedMemDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("AHP_TEST_DATABASE_URL must be set for memory integration tests")
	}
	conf, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse DSN: %v", err)
	}
	var rnd [4]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		t.Fatal(err)
	}
	name := "ahp_hive_h24dim_" + hex.EncodeToString(rnd[:])
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	admin, err := pgxpool.NewWithConfig(ctx, conf)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	create := "CREATE DATABASE " + quoteIdent(name)
	if template := os.Getenv("AHP_TEST_DATABASE_TEMPLATE"); template != "" {
		create += " TEMPLATE " + quoteIdent(template)
	}
	if _, err := admin.Exec(ctx, create); err != nil {
		t.Fatalf("this test needs CREATEDB to isolate a database: %v", err)
	}
	t.Cleanup(func() {
		_ = dropIsolated(admin, name)
	})
	conf.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, conf)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping isolated db: %v", err)
	}
	return pool
}

// quoteIdent quotes an identifier for the CREATE/DROP DATABASE statements the
// test issues against its own throwaway database.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// dropIsolated removes the throwaway database even while the test pool is
// closing: without FORCE the drop loses the race against its own backends.
func dropIsolated(admin *pgxpool.Pool, name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, err := admin.Exec(ctx, "DROP DATABASE IF EXISTS "+quoteIdent(name)+" WITH (FORCE)")
	return err
}

var memScope = contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}

type fixedSource struct{ width int }

func (f fixedSource) Embed(context.Context, string) ([]float32, error) {
	v := make([]float32, f.width)
	v[0] = 1
	return v, nil
}
func (f fixedSource) Dimensions() int { return f.width }

// A stand created before an embedder existed has a 64-wide column. Wiring a
// real 384-wide embedder must not be rejected, and must not silently keep the
// old width: an empty column is rescaled, so semantic search can be turned on
// by configuration alone.
func TestMigrateRescalesAnEmptyEmbeddingColumn(t *testing.T) {
	pool := isolatedMemDB(t)
	ctx := context.Background()

	legacy, err := New(pool, nil, dimTestAuth, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := legacy.Migrate(ctx); err != nil {
		t.Fatalf("legacy migrate: %v", err)
	}
	if got := columnType(ctx, t, pool); got != "vector(64)" {
		t.Fatalf("fresh legacy column is %q, want vector(64)", got)
	}

	// Existing canonical memory survives enabling semantics; an empty embedding
	// column does not imply an empty corpus.
	old, err := legacy.Propose(ctx, memScope, ProposeRequest{Kind: KindUser, Text: "preserve the existing memory", OpKey: "legacy-note", SourceKind: "user_statement", Evidence: []Evidence{{SourceKind: "user_statement", Detail: "synthetic regression evidence"}}, Provenance: map[string]any{"migration": "preserve-canonical"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.Review(ctx, memScope, old.Memory.ID, DecisionApprove, "approve-legacy"); err != nil {
		t.Fatal(err)
	}
	before := canonicalMemoryDigest(t, ctx, pool, old.Memory.ID)

	upgraded, err := New(pool, nil, dimTestAuth, nil, WithEmbedding(fixedSource{width: 384}))
	if err != nil {
		t.Fatal(err)
	}
	if err := upgraded.Migrate(ctx); err != nil {
		t.Fatalf("migrate with a wired embedder on an empty column: %v", err)
	}
	if got := columnType(ctx, t, pool); got != "vector(384)" {
		t.Fatalf("column stayed %q, want vector(384): semantic search cannot work against a narrower column", got)
	}
	if got := canonicalMemoryDigest(t, ctx, pool, old.Memory.ID); got != before {
		t.Fatal("width migration changed canonical memory")
	}
	rebuilt, err := upgraded.Reindex(ctx, memScope, ReindexRequest{OpKey: "reindex-existing"})
	if err != nil || rebuilt.Indexed != 1 {
		t.Fatalf("reindex existing corpus: %+v %v", rebuilt, err)
	}
	if got := canonicalMemoryDigest(t, ctx, pool, old.Memory.ID); got != before {
		t.Fatal("reindex changed content, labels, provenance or lifecycle")
	}
	var width int
	if err := pool.QueryRow(ctx, `SELECT vector_dims(embedding) FROM ahp_memory.memories WHERE id=$1`, old.Memory.ID).Scan(&width); err != nil || width != 384 {
		t.Fatalf("reindexed width=%d err=%v", width, err)
	}
	t.Logf("existing record canonical digest %s preserved across resize and reindex", before)

}

// Once the column holds vectors, a different embedder is a data decision: the
// restart must refuse rather than drop or reinterpret someone's index.
func TestMigrateRefusesToDiscardExistingVectors(t *testing.T) {
	pool := isolatedMemDB(t)
	ctx := context.Background()
	first, err := New(pool, nil, dimTestAuth, nil, WithEmbedding(fixedSource{width: 64}))
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO ahp_memory.memories
		(id, tenant, deployment, kind, subject, text, text_digest, embedding, op_key, op_digest)
		VALUES ('m1','t1','d1','user','s1','held','dig',$1::vector,'k1','dig')`, serializeVector(make([]float32, 64))); err != nil {
		t.Fatalf("seed a stored vector: %v", err)
	}

	narrow, err := New(pool, nil, dimTestAuth, nil, WithEmbedding(fixedSource{width: 384}))
	if err != nil {
		t.Fatal(err)
	}
	err = narrow.Migrate(ctx)
	var e *contracts.Error
	if err == nil || !asContracts(err, &e) {
		t.Fatalf("a width change over stored vectors must be refused, got %v", err)
	}
	if e.Code != contracts.ErrConflict {
		t.Fatalf("code %s, want CONFLICT (an operator decision, not a dependency outage)", e.Code)
	}
	for _, want := range []string{"vector(64)", "vector(384)"} {
		if !strings.Contains(e.Message, want) {
			t.Fatalf("the refusal must name %s, got %q", want, e.Message)
		}
	}
	if got := columnType(ctx, t, pool); got != "vector(64)" {
		t.Fatalf("a refused migration must leave the column alone, found %q", got)
	}
}

// The claim proved here is deliberately narrow: once an embedder is wired, a
// query sharing NO word with any stored memory still returns hits, while the
// same query on the same data with no embedder returns nothing. That is the
// write-embed-store-rank path working end to end through the public store API.
// It is not a claim about semantic quality, which needs a real model.
func TestVectorSignalRetrievesWhatLexicalCannot(t *testing.T) {
	pool := isolatedMemDB(t)
	ctx := context.Background()
	store, err := New(pool, nil, dimTestAuth, nil, WithEmbedding(fixedSource{width: 8}))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	propose := func(opKey, text string) string {
		res, perr := store.Propose(ctx, memScope, ProposeRequest{Kind: KindUser, Text: text, OpKey: opKey})
		if perr != nil {
			t.Fatalf("propose %s: %v", opKey, perr)
		}
		if _, rerr := store.Review(ctx, memScope, res.Memory.ID, "approve", "review-"+res.Memory.ID); rerr != nil {
			t.Fatalf("review %s: %v", opKey, rerr)
		}
		return res.Memory.ID
	}
	propose("obs2-lex", "quarterly revenue guidance increased")
	propose("obs2-orth", "zzz qqq xxxx")

	// The write path stored a vector at the wired width: without this the
	// search signal below would have nothing to rank against.
	var stored bool
	if err := pool.QueryRow(ctx, `SELECT embedding IS NOT NULL
	    FROM ahp_memory.memories WHERE op_key = 'obs2-lex'`).Scan(&stored); err != nil {
		t.Fatalf("read stored embedding: %v", err)
	}
	if !stored {
		t.Fatal("Propose did not store an embedding although an embedder is wired")
	}
	if w := columnWidth(ctx, t, pool); w != 8 {
		t.Fatalf("stored vector width %d, want 8", w)
	}

	unrelated := "unobtainable quintessence xylophone"
	bare, err := New(pool, nil, dimTestAuth, nil)
	if err != nil {
		t.Fatal(err)
	}
	lexicalOnly, err := bare.Search(ctx, memScope, SearchRequest{Query: unrelated, Limit: 5})
	if err != nil {
		t.Fatalf("lexical search: %v", err)
	}
	if len(lexicalOnly.Hits) != 0 {
		t.Fatalf("fixture broken: %q matched lexically (%d hits)", unrelated, len(lexicalOnly.Hits))
	}
	// Sanity: the lexical path itself is not dead.
	control, err := bare.Search(ctx, memScope, SearchRequest{Query: "revenue", Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(control.Hits) == 0 {
		t.Fatal("fixture broken: the lexical match itself was not found")
	}

	res, err := store.Search(ctx, memScope, SearchRequest{Query: unrelated, Limit: 5})
	if err != nil {
		t.Fatalf("hybrid search: %v", err)
	}
	if len(res.Hits) == 0 {
		t.Fatal("the vector signal never reached the result set although embeddings are stored")
	}
}

func columnWidth(ctx context.Context, t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	// columnType reports "vector(N)"; the digits are the pgvector width.
	digits := strings.TrimRight(strings.TrimLeft(columnType(ctx, t, pool), "vector("), ")")
	w, err := strconv.Atoi(digits)
	if err != nil {
		t.Fatalf("unparsable column type %q: %v", digits, err)
	}
	return w
}

func asContracts(err error, target **contracts.Error) bool {
	e, ok := err.(*contracts.Error)
	if ok {
		*target = e
	}
	return ok
}

func columnType(ctx context.Context, t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var typ string
	err := pool.QueryRow(ctx, `SELECT format_type(a.atttypid, a.atttypmod)
	  FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
	  JOIN pg_namespace n ON n.oid = c.relnamespace
	 WHERE n.nspname='ahp_memory' AND c.relname='memories' AND a.attname='embedding'`).Scan(&typ)
	if err != nil {
		t.Fatal(err)
	}
	return typ
}

// canonicalMemoryDigest excludes derived search/vector fields only. It includes
// source text, labels, provenance, evidence, state, TTL and original timestamps.
func canonicalMemoryDigest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string) string {
	t.Helper()
	var digest string
	if err := pool.QueryRow(ctx, `SELECT md5((to_jsonb(m)-'embedding'-'embedding_space'-'search')::text) FROM ahp_memory.memories m WHERE id=$1`, id).Scan(&digest); err != nil {
		t.Fatal(err)
	}
	return digest
}
