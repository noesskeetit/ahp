package artifactsapi

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/labels"
)

// TestHTTPReadIsScopedToTheTokensContext closes the defect measured on the
// stand as artifact-download-20261031T072257Z step "negatives": a task token of
// one conversation read another conversation's artifact of the SAME subject
// (same_subject_other_task_download=200). Two tokens, one subject, two
// conversations: the writer reads, the sibling conversation is refused on both
// verbs and receives no bytes.
//
// The subject boundary is untouched: the sibling token is a fully valid token of
// the same tenant, deployment and subject, and it is still refused.
func TestHTTPReadIsScopedToTheTokensContext(t *testing.T) {
	e := newEnv(t)
	writer := e.signBound(t, alice, "ctx-home", "task-home")
	sibling := e.signBound(t, alice, "ctx-other", "task-other")

	id := e.uploadSeal(t, writer, "k-scope-1", "conversation-bytes", labels.Set{})

	code, ref := e.call(t, "Get", writer, mustJSON(t, map[string]any{"id": id}))
	mustStatus(t, code, http.StatusOK, ref)
	if ref["context_id"] != "ctx-home" {
		t.Fatalf("artifact did not record the writer's context: %v", ref)
	}
	if code, body := e.call(t, "Get", sibling, mustJSON(t, map[string]any{"id": id})); code != http.StatusForbidden {
		t.Fatalf("Get from another conversation = %d, want 403 (body %v)", code, body)
	}
	// A refusal answers 403 and its body is the JSON error, not the object: the
	// assertion is about the sealed bytes never appearing, not about an empty body.
	status, served, hdr := e.downloadRaw(t, sibling, mustJSON(t, map[string]any{"id": id}), "")
	if status != http.StatusForbidden || strings.Contains(served, "conversation-bytes") {
		t.Fatalf("Download from another conversation = %d body %q, want 403 without the sealed bytes",
			status, served)
	}
	if hdr.Get("Content-Range") != "" || hdr.Get("ETag") == quoteETag("conversation-bytes") {
		t.Fatalf("refused Download still describes the object: %v", hdr)
	}
	// A range request is the same read and must not leak a prefix either.
	if status, served, _ := e.downloadRaw(t, sibling, mustJSON(t, map[string]any{"id": id}), "bytes=0-3"); status == http.StatusPartialContent || strings.Contains(served, "conve") {
		t.Fatalf("ranged Download from another conversation served %q (status %d)", served, status)
	}
	// Publication writes the bytes somewhere else, so it is a read too. The
	// refusal must come from the conversation gate and not from a label rule, so
	// the writer's identical call is measured as the control.
	sink := map[string]any{"ids": []string{id},
		"sink": labels.Audience{Kind: labels.KindWorkspace, Workspace: "w1"}}
	code, body := e.call(t, "CheckPublication", sibling, mustJSON(t, sink))
	if code != http.StatusForbidden || !strings.Contains(fmt.Sprint(body), "another context") {
		t.Fatalf("CheckPublication from another conversation = %d %v, want 403 naming the context gate",
			code, body)
	}
	if _, control := e.call(t, "CheckPublication", writer, mustJSON(t, sink)); strings.Contains(fmt.Sprint(control), "another context") {
		t.Fatalf("the writer was refused by its own conversation gate: %v", control)
	}
	// The writer still reads its own artifact afterwards: the gate narrows
	// strangers, it does not break the owner.
	if status, served, _ := e.downloadRaw(t, writer, mustJSON(t, map[string]any{"id": id}), ""); status != http.StatusOK || served != "conversation-bytes" {
		t.Fatalf("owner Download = %d %q, want 200 and the sealed bytes", status, served)
	}
}

// TestHTTPSiblingTaskInsideOneConversationReads pins the boundary that is NOT
// enforced here, so it cannot be mistaken for a delivery gap of the data plane.
//
// Measured on the live stand: /v1/identity/control/exchange stamps the context
// claim from the CALLER's request (and copies the task into it when the request
// omits it), so two exchanges that name the same conversation produce tokens
// whose only difference is task/root/grant - ctx identical. Reading those two
// apart needs a claim that distinguishes them, which is identity's to issue:
// either exchange narrows ctx per task, or the artifact records the writing
// task and the reader's task is checked against the grant tree. Until then a
// token of the same conversation reads that conversation, which is the
// behaviour the fleet's own artifact-download leg depends on (its owner token is
// a freshly exchanged task token, not the run's task), so this test asserts the
// current rule rather than the wish.
func TestHTTPSiblingTaskInsideOneConversationReads(t *testing.T) {
	e := newEnv(t)
	writer := e.signBound(t, alice, "ctx-shared", "task-writer")
	sibling := e.signBound(t, alice, "ctx-shared", "task-sibling")

	id := e.uploadSeal(t, writer, "k-scope-2", "shared-conversation-bytes", labels.Set{})

	if status, served, _ := e.downloadRaw(t, sibling, mustJSON(t, map[string]any{"id": id}), ""); status != http.StatusOK || served != "shared-conversation-bytes" {
		t.Fatalf("sibling task in one conversation = %d %q, want 200 and the bytes (see the rule above)",
			status, served)
	}
}
