package artifacts

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"
)

// Each word is short, but the complete positional tsvector exceeds PostgreSQL's
// 1 MiB ceiling despite the valid source being below our 2 MiB document limit.
func distinctWordDocument(words int) string {
	var b strings.Builder
	for i := 0; i < words; i++ {
		sum := md5.Sum([]byte(fmt.Sprint(i)))
		b.WriteString(hex.EncodeToString(sum[:]))
		b.WriteByte(' ')
	}
	return b.String()
}

func uploadIndexedDocument(t *testing.T, e *env, body string) ArtifactRef {
	t.Helper()
	sum := sha256.Sum256([]byte(body))
	up, err := e.store.BeginUpload(context.Background(), e.scope, "index-regression", UploadRequest{Name: "valid.txt", MIME: "text/plain", SHA256: hex.EncodeToString(sum[:]), Size: int64(len(body))})
	if err != nil {
		t.Fatal(err)
	}
	put(t, up.PresignedURL, body)
	ref, err := e.store.Seal(context.Background(), e.scope, up.ID)
	if err != nil {
		t.Fatalf("valid supported %d-byte document failed Seal: %v", len(body), err)
	}
	return ref
}

func TestSearchIndexHandlesWholeDocumentVectorLimit(t *testing.T) {
	parser := os.Getenv("AHP_ARTIFACT_PARSER")
	if parser == "" {
		t.Skip("requires built Linux parser")
	}
	e := setup(t, WithParserCommand(ParserCommand{Path: parser}))
	body := distinctWordDocument(40000)
	if len(body) != 1320000 || len(body) > MaxExtractBytes {
		t.Fatal("invalid regression fixture")
	}
	ref := uploadIndexedDocument(t, e, body)
	ctx := context.Background()
	rd, err := e.store.Readiness(ctx, e.scope, ref.ID)
	if err != nil || !rd.ExtractionReady || !rd.SearchReady || !rd.Ready {
		t.Fatalf("large valid document not ready: %+v %v", rd, err)
	}
	var read strings.Builder
	var offset int64
	for {
		chunk, err := e.store.ReadTextChunk(ctx, e.scope, ref.ID, ref.Version, offset, MaxTextChunkBytes, MaxTextChunkBytes)
		if err != nil {
			t.Fatal(err)
		}
		read.WriteString(chunk.Text)
		offset = chunk.NextOffset
		if chunk.EOF {
			break
		}
	}
	if read.String() != body {
		t.Fatal("extraction was truncated")
	}
	words := strings.Fields(body)
	for _, query := range []string{words[0], words[len(words)-1], words[0] + " " + words[len(words)-1]} {
		found, err := e.store.SearchText(ctx, e.scope, ref.ID, ref.Version, query)
		if err != nil || !found {
			t.Fatalf("full-document search missed query %q: %v", query, err)
		}
	}
	var chunks, maxVectorBytes int
	if err := e.store.pool.QueryRow(ctx, `SELECT count(*),COALESCE(max(pg_column_size(search_vector)),0) FROM ahp_artifacts.extraction_search_chunks WHERE tenant=$1 AND sha256=$2`, e.scope.Tenant, ref.SHA256).Scan(&chunks, &maxVectorBytes); err != nil || chunks <= 1 || maxVectorBytes >= 1<<20 {
		t.Fatalf("unbounded search vectors chunks=%d max=%d err=%v", chunks, maxVectorBytes, err)
	}
	t.Logf("document_bytes=%d indexed_chunks=%d max_vector_bytes=%d", len(body), chunks, maxVectorBytes)
	replay, err := e.store.Seal(ctx, e.scope, ref.ID)
	if err != nil || replay.Version != ref.Version {
		t.Fatalf("seal replay changed version: %v", err)
	}
}

func TestSearchIndexFailurePreservesExtractionAndRecovers(t *testing.T) {
	parser := os.Getenv("AHP_ARTIFACT_PARSER")
	if parser == "" {
		t.Skip("requires built Linux parser")
	}
	e := setup(t, WithParserCommand(ParserCommand{Path: parser}))
	ctx := context.Background()
	// Real PostgreSQL error during indexing, after extraction has committed.
	// Rejecting chunk 1 also proves no partial searchable index becomes ready.
	constraint := "review_index_failure"
	_, err := e.store.pool.Exec(ctx, `ALTER TABLE ahp_artifacts.extraction_search_chunks ADD CONSTRAINT `+constraint+` CHECK (tenant <> '`+e.scope.Tenant+`' OR chunk_no < 1)`)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		e.store.pool.Exec(context.Background(), `ALTER TABLE ahp_artifacts.extraction_search_chunks DROP CONSTRAINT IF EXISTS `+constraint)
	})
	body := distinctWordDocument(300)
	ref := uploadIndexedDocument(t, e, body)
	rd, err := e.store.Readiness(ctx, e.scope, ref.ID)
	if err != nil || !rd.ContentReady || !rd.ExtractionReady || rd.SearchReady || rd.Ready || rd.Quarantined || rd.SearchStatus != "failed" || rd.SearchDetail != "index_failed" {
		t.Fatalf("index failure not durable and independent: %+v %v", rd, err)
	}
	t.Logf("injected index refusal readiness=%+v", rd)
	var count int
	if err := e.store.pool.QueryRow(ctx, `SELECT count(*) FROM ahp_artifacts.extraction_search_chunks WHERE tenant=$1 AND sha256=$2`, e.scope.Tenant, ref.SHA256).Scan(&count); err != nil || count != 0 {
		t.Fatalf("partial index survived rollback: %d %v", count, err)
	}
	chunk, err := e.store.ReadTextChunk(ctx, e.scope, ref.ID, ref.Version, 0, MaxTextChunkBytes, MaxTextChunkBytes)
	if err != nil || !chunk.EOF || chunk.Text != body {
		t.Fatalf("failed index lost extraction: %v", err)
	}
	rc, _, err := e.store.Open(ctx, e.scope, ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	readAll(t, rc, body)
	if _, err := e.store.SearchText(ctx, e.scope, ref.ID, ref.Version, strings.Fields(body)[0]); err == nil {
		t.Fatal("partial search was exposed")
	}
	replay, err := e.store.Seal(ctx, e.scope, ref.ID)
	if err != nil || replay.Version != ref.Version {
		t.Fatalf("failed-index Seal replay lost immutable ref: %v", err)
	}
	if rd, err := e.store.Readiness(ctx, e.scope, ref.ID); err != nil || rd.SearchStatus != "failed" || !rd.ExtractionReady {
		t.Fatalf("retry lost terminal state: %+v %v", rd, err)
	}
	if _, err = e.store.pool.Exec(ctx, `ALTER TABLE ahp_artifacts.extraction_search_chunks DROP CONSTRAINT `+constraint); err != nil {
		t.Fatal(err)
	}
	e.store.parser.Path = "/parser-not-available"
	rd, err = e.store.Process(ctx, e.scope, ref.ID)
	if err != nil || !rd.Ready || rd.SearchStatus != "ready" || rd.SearchDetail != "" {
		t.Fatalf("retry could not index preserved extraction: %+v %v", rd, err)
	}
	found, err := e.store.SearchText(ctx, e.scope, ref.ID, ref.Version, strings.Fields(body)[0]+" "+strings.Fields(body)[299])
	if err != nil || !found {
		t.Fatalf("recovered full index incomplete: %v", err)
	}
}

func TestSearchIndexMatchesPlainQueryAcrossLexemeChunks(t *testing.T) {
	parser := os.Getenv("AHP_ARTIFACT_PARSER")
	if parser == "" {
		t.Skip("requires built Linux parser")
	}
	e := setup(t, WithParserCommand(ParserCommand{Path: parser}))
	body := distinctWordDocument(300) + "Привет-world can't user@example.com путь/файл"
	ref := uploadIndexedDocument(t, e, body)
	queries := []string{"Привет-world", "can't", "user@example.com", "путь/файл", "привет файл", "missing-token", "!!!"}
	for _, query := range queries {
		var want bool
		if err := e.store.pool.QueryRow(context.Background(), `SELECT to_tsvector('simple',$1) @@ plainto_tsquery('simple',$2)`, body, query).Scan(&want); err != nil {
			t.Fatal(err)
		}
		got, err := e.store.SearchText(context.Background(), e.scope, ref.ID, ref.Version, query)
		if err != nil || got != want {
			t.Fatalf("query %q got=%v want=%v err=%v", query, got, want, err)
		}
	}
}

func TestSearchIndexChunksFollowExtractionCachePurge(t *testing.T) {
	parser := os.Getenv("AHP_ARTIFACT_PARSER")
	if parser == "" {
		t.Skip("requires built Linux parser")
	}
	e := setup(t, WithParserCommand(ParserCommand{Path: parser}))
	ctx := context.Background()
	body := distinctWordDocument(300)
	first := uploadIndexedDocument(t, e, body)
	up, err := e.store.BeginUpload(ctx, e.scope, "second-index-reference", UploadRequest{Name: "valid.txt", MIME: "text/plain", SHA256: first.SHA256, Size: first.Size})
	if err != nil {
		t.Fatal(err)
	}
	put(t, up.PresignedURL, body)
	second, err := e.store.Seal(ctx, e.scope, up.ID)
	if err != nil {
		t.Fatal(err)
	}
	for i, ref := range []ArtifactRef{first, second} {
		if err := e.store.Delete(ctx, e.scope, ref.ID, fmt.Sprintf("purge-index-%d", i)); err != nil {
			t.Fatal(err)
		}
		if err := e.store.Purge(ctx, e.scope, ref.ID); err != nil {
			t.Fatal(err)
		}
		var count int
		if err := e.store.pool.QueryRow(ctx, `SELECT count(*) FROM ahp_artifacts.extraction_search_chunks WHERE tenant=$1 AND sha256=$2`, e.scope.Tenant, first.SHA256).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if i == 0 && count == 0 {
			t.Fatal("purged shared live index")
		}
		if i == 1 && count != 0 {
			t.Fatal("derived chunks survived last retained artifact")
		}
	}
}
