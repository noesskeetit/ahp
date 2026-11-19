package artifactsapi

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/artifacts"
)

func TestHTTPIngestionExactVersionBudgetAndContext(t *testing.T) {
	parser := os.Getenv("AHP_ARTIFACT_PARSER")
	if parser == "" {
		t.Skip("requires Linux parser executable")
	}
	e := newEnv(t, artifacts.WithParserCommand(artifacts.ParserCommand{Path: parser}))
	owner := e.signBound(t, alice, "ctx-document", "task-document")
	other := e.signBound(t, alice, "ctx-other", "task-other")
	data := "real HTTP document alpha beta gamma"
	sum := sha256.Sum256([]byte(data))
	code, up := e.call(t, "BeginUpload", owner, mustJSON(t, map[string]any{"key": "http-ingest", "name": "doc.txt", "mime": "text/plain", "size": len(data), "sha256": hex.EncodeToString(sum[:])}))
	mustStatus(t, code, 200, up)
	req, _ := http.NewRequest(http.MethodPut, up["presigned_url"].(string), strings.NewReader(data))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 200 {
		t.Fatalf("PUT status %d", res.StatusCode)
	}
	code, denial := e.call(t, "Seal", other, mustJSON(t, map[string]any{"id": up["id"]}))
	mustStatus(t, code, 403, denial)
	code, ref := e.call(t, "Seal", owner, mustJSON(t, map[string]any{"id": up["id"]}))
	mustStatus(t, code, 200, ref)
	code, rd := e.call(t, "Readiness", owner, mustJSON(t, map[string]any{"id": up["id"]}))
	mustStatus(t, code, 200, rd)
	if rd["ready"] != true || rd["extraction_ready"] != true || rd["search_ready"] != true {
		t.Fatalf("not processed: %v", rd)
	}
	chunkRequest := map[string]any{"id": ref["id"], "version": ref["version"], "offset": 0, "limit": 64, "context_budget": 7}
	code, chunk := e.call(t, "ReadTextChunk", owner, mustJSON(t, chunkRequest))
	mustStatus(t, code, 200, chunk)
	if chunk["text"] != "real HT" || chunk["next_offset"] != float64(7) {
		t.Fatalf("budget ignored: %v", chunk)
	}
	for _, token := range []string{other, e.signTask(t, mallory)} {
		code, body := e.call(t, "ReadTextChunk", token, mustJSON(t, chunkRequest))
		if code == 200 || strings.Contains(mustJSON(t, body), "real HTTP") {
			t.Fatal("unauthorized extracted text")
		}
	}
	code, found := e.call(t, "SearchText", owner, mustJSON(t, map[string]any{"id": ref["id"], "version": ref["version"], "query": "gamma"}))
	mustStatus(t, code, 200, found)
	if found["matched"] != true {
		t.Fatal("text index missed document")
	}
	chunkRequest["context_budget"] = artifacts.MaxTextChunkBytes + 1
	code, _ = e.call(t, "ReadTextChunk", owner, mustJSON(t, chunkRequest))
	if code != 400 {
		t.Fatalf("oversized context budget status=%d", code)
	}
}
