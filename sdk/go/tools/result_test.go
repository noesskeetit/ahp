package tools_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
	toolssdk "github.com/ahp-platform/ahp/sdk/go/tools"
)

// stubOwner is a DOUBLe of the tools owner face: it answers with the codes the
// real face returns (Boyle's TOOLS-RESULT contract, 2026-11-09) so the SDK's
// classification is measured. The real-owner leg over PostgreSQL, Identity and
// mTLS lives in cmd/ahp/sdk_public_clients_test.go.
type stubOwner struct {
	t        *testing.T
	handler  func(w http.ResponseWriter, r *http.Request)
	requests atomic.Int32
	lastBody string
	lastAuth string
	lastPath string
}

func (s *stubOwner) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.requests.Add(1)
	s.lastPath = r.URL.Path
	s.lastAuth = r.Header.Get("Authorization")
	raw, _ := io.ReadAll(r.Body)
	s.lastBody = string(raw)
	s.handler(w, r)
}

func newClient(t *testing.T, stub *stubOwner) (*toolssdk.Client, *stubOwner, func()) {
	t.Helper()
	if stub.handler == nil {
		stub.handler = func(http.ResponseWriter, *http.Request) {}
	}
	srv := httptest.NewTLSServer(stub)
	cfg := toolssdk.Config{
		BaseURL:     srv.URL,
		HTTPClient:  srv.Client(),
		TokenSource: func(context.Context) (string, error) { return "task-token", nil },
	}
	c, err := toolssdk.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c, stub, srv.Close
}

func writeJSON(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}

const inlineResult = `{"key":"k1","target":"echo","status":"SUCCEEDED","digest":"sha256:` +
	`aaa","result_storage":"inline","provenance":{"schema":"tools-result-v1","grant_task":"task-1","grant_context":"ctx-1","source":"conn-1"},"payload":{"content":[{"type":"text","text":"written"}]}}`

func TestToolResultPassesOwnerCategoriesThroughUnchanged(t *testing.T) {
	cases := []struct {
		name       string
		status     int
		body       string
		wantCode   string
		wantErrMsg string
	}{
		{"owner_answers_result", 200, inlineResult, "", ""},
		// Absent AND foreign both answer NOT_FOUND: existence is not disclosed.
		{"absent_or_foreign", 404, `{"code":"NOT_FOUND","message":"no such operation"}`, contracts.ErrNotFound, ""},
		// 403 covers no verified context, foreign/missing provenance, another
		// context of the same subject, revoked grant, non-task bearer and a
		// reconciled row nobody may label.
		{"forbidden", 403, `{"code":"FORBIDDEN","message":"provenance does not match the reading authority"}`, contracts.ErrForbidden, ""},
		// 409 covers pending, uncertain, failed and stale: a decision, not a
		// transient. The SDK must not retry and must not soften it.
		{"conflict", 409, `{"code":"CONFLICT","message":"operation is not SUCCEEDED"}`, contracts.ErrConflict, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			owner := &stubOwner{t: t, handler: func(w http.ResponseWriter, r *http.Request) {
				writeJSON(w, tc.status, tc.body)
			}}
			c, stub, closeServer := newClient(t, owner)
			defer closeServer()
			res, err := c.ToolResult(context.Background(), "k1")
			if tc.wantCode == "" {
				if err != nil {
					t.Fatalf("a valid owner result was reported as an error: %v", err)
				}
				if res.Status != "SUCCEEDED" || res.Storage != toolssdk.StorageInline || len(res.Payload) == 0 {
					t.Fatalf("result lost content: %+v", res)
				}
				if res.Provenance.Schema != "tools-result-v1" || res.Provenance.GrantContext != "ctx-1" {
					t.Fatalf("provenance not carried: %+v", res.Provenance)
				}
			} else {
				if contracts.Code(err) != tc.wantCode {
					t.Fatalf("code = %q, want %q (err=%v)", contracts.Code(err), tc.wantCode, err)
				}
				var apiErr *toolssdk.Error
				if !errors.As(err, &apiErr) || apiErr.Status != tc.status {
					t.Fatalf("HTTP status lost: %v", err)
				}
				// A read must never be reported as an unknown outcome: the
				// effect already settled, and "unknown" invites a re-send.
				if apiErr.OutcomeUnknown {
					t.Fatal("a result read was classified as an unknown effect outcome")
				}
			}
			if stub.requests.Load() != 1 {
				t.Fatalf("the SDK sent %d requests; a result read must never retry", stub.requests.Load())
			}
			if stub.lastPath != "/v1/tools/GetResult" {
				t.Fatalf("path = %s", stub.lastPath)
			}
			if stub.lastBody != `{"key":"k1"}` {
				t.Fatalf("body = %s", stub.lastBody)
			}
			if stub.lastAuth != "Bearer task-token" {
				t.Fatalf("bearer not presented: %q", stub.lastAuth)
			}
		})
	}
}

func TestToolResultRefusesSelfContradictorySuccess(t *testing.T) {
	cases := []struct{ name, body string }{
		{"inline_without_payload", `{"key":"k","status":"SUCCEEDED","result_storage":"inline","digest":"sha256:aa"}`},
		{"inline_with_artifact_ref", `{"key":"k","status":"SUCCEEDED","result_storage":"inline","digest":"sha256:aa","payload":{"a":1},"artifact_ref":{"artifact_id":"a1","digest":"sha256:aa","size":1}}`},
		{"artifact_without_reference", `{"key":"k","status":"SUCCEEDED","result_storage":"artifact","digest":"sha256:aa"}`},
		{"artifact_with_inline_payload", `{"key":"k","status":"SUCCEEDED","result_storage":"artifact","digest":"sha256:aa","payload":{"a":1},"artifact_ref":{"artifact_id":"a1","digest":"sha256:aa","size":1}}`},
		{"unknown_storage", `{"key":"k","status":"SUCCEEDED","result_storage":"telepathy","digest":"sha256:aa"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			owner := &stubOwner{t: t, handler: func(w http.ResponseWriter, r *http.Request) {
				writeJSON(w, 200, tc.body)
			}}
			c, _, closeServer := newClient(t, owner)
			defer closeServer()
			res, err := c.ToolResult(context.Background(), "k")
			if err == nil {
				t.Fatalf("contradictory result accepted: %+v", res)
			}
			if contracts.Code(err) != "UNKNOWN" {
				t.Fatalf("code = %q, want UNKNOWN", contracts.Code(err))
			}
			if len(res.Payload) != 0 || res.Ref != nil {
				t.Fatal("a rejected result was still handed to the caller")
			}
		})
	}
}

func TestToolResultRejectsEmptyKeyBeforeTheNetwork(t *testing.T) {
	c, stub, closeServer := newClient(t, &stubOwner{t: t, handler: func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("an empty key must not reach the owner")
	}})
	defer closeServer()
	if _, err := c.ToolResult(context.Background(), "  "); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("code = %q, want INVALID_ARGUMENT", contracts.Code(err))
	}
	if stub.requests.Load() != 0 {
		t.Fatal("empty key was sent to the owner")
	}
}

type fakeArtifacts struct {
	body []byte
	err  error
}

func (f fakeArtifacts) Download(context.Context, string) ([]byte, error) { return f.body, f.err }

func artifactResult(body []byte, digest string, size int64) toolssdk.ToolResult {
	return toolssdk.ToolResult{
		Key: "k", Status: "SUCCEEDED", Digest: digest, Storage: toolssdk.StorageArtifact,
		Ref: &toolssdk.ArtifactRef{ArtifactID: "a1", Digest: digest, Size: size},
	}
}

func TestFetchResultBytesVerifiesTheDeclaredDigest(t *testing.T) {
	ctx := context.Background()
	body := []byte("written result bytes")
	sum := sha256.Sum256(body)
	good := "sha256:" + hex.EncodeToString(sum[:])

	got, err := toolssdk.FetchResultBytes(ctx, fakeArtifacts{body: body}, artifactResult(body, good, int64(len(body))))
	if err != nil || string(got) != string(body) {
		t.Fatalf("verified fetch: %q, %v", got, err)
	}

	other := sha256.Sum256([]byte("different bytes"))
	mismatched := "sha256:" + hex.EncodeToString(other[:])
	if got, err := toolssdk.FetchResultBytes(ctx, fakeArtifacts{body: body}, artifactResult(body, mismatched, int64(len(body)))); err == nil {
		t.Fatalf("bytes that do not match the digest were returned: %q", got)
	} else if contracts.Code(err) != "UNKNOWN" {
		t.Fatalf("code = %q, want UNKNOWN", contracts.Code(err))
	}
	if _, err := toolssdk.FetchResultBytes(ctx, fakeArtifacts{body: body}, artifactResult(body, good, int64(len(body)+1))); err == nil {
		t.Fatal("size disagreement was returned as a result")
	}
	if _, err := toolssdk.FetchResultBytes(ctx, fakeArtifacts{body: body}, artifactResult(body, "not-hex", int64(len(body)))); err == nil {
		t.Fatal("an undecodable digest was treated as verification")
	}
	// Owner disagreement between the two digests it published is never resolved
	// by picking one of them.
	split := artifactResult(body, good, int64(len(body)))
	split.Digest = mismatched
	if _, err := toolssdk.FetchResultBytes(ctx, fakeArtifacts{body: body}, split); err == nil {
		t.Fatal("result and artifact digests disagreed and bytes were still returned")
	}
	// An owner failure stays the caller's to classify.
	if _, err := toolssdk.FetchResultBytes(ctx, fakeArtifacts{err: errors.New("owner down")}, artifactResult(body, good, int64(len(body)))); err == nil {
		t.Fatal("artifact fetch failure was swallowed")
	}
}

func TestFetchResultBytesRequiresAnArtifactResult(t *testing.T) {
	ctx := context.Background()
	inline := toolssdk.ToolResult{Key: "k", Status: "SUCCEEDED", Storage: toolssdk.StorageInline, Payload: json.RawMessage(`{"a":1}`)}
	if _, err := toolssdk.FetchResultBytes(ctx, fakeArtifacts{}, inline); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("code = %q, want INVALID_ARGUMENT", contracts.Code(err))
	}
	artifact := artifactResult([]byte("x"), "sha256:"+strings.Repeat("0", 64), 1)
	var missing toolssdk.ArtifactReader
	if _, err := toolssdk.FetchResultBytes(ctx, missing, artifact); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("code = %q, want INVALID_ARGUMENT", contracts.Code(err))
	}
}
