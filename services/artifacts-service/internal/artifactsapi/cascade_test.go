package artifactsapi

import (
	"net/http"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// signBound mints a task token for a specific context and task, which is what a
// run's token carries and therefore what the artifact plane records.
func (e *env) signBound(t *testing.T, subject, contextID, taskID string) string {
	t.Helper()
	tok, err := e.signer.Sign(auth.Claims{
		Audience: audience, Kind: auth.KindTask,
		Tenant: e.tenant, Deployment: e.deployment,
		Subject: subject, Task: taskID, Root: "root-1",
		Context: contextID, Grant: "grant-1", Depth: 0,
	}, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func intOf(t *testing.T, body map[string]any, key string) int {
	t.Helper()
	v, ok := body[key].(float64)
	if !ok {
		t.Fatalf("body %v has no numeric %q", body, key)
	}
	return int(v)
}

// TestHTTPCascadeByContext is the HTTP shape of the defect the fleet measured:
// after the thread is deleted the artifacts of its context must be neither
// readable nor downloadable, and the platform must not need the deploy that
// produced them.
func TestHTTPCascadeByContext(t *testing.T) {
	e := newEnv(t)
	tok := e.signBound(t, alice, "ctx-thread-1", "task-of-thread-1")

	first := e.uploadSeal(t, tok, "k-c1-a", "first-thread-bytes", labels.Set{})
	second := e.uploadSeal(t, tok, "k-c1-b", "second-thread-bytes", labels.Set{})

	code, ref := e.call(t, "Get", tok, mustJSON(t, map[string]any{"id": first}))
	mustStatus(t, code, http.StatusOK, ref)
	if ref["context_id"] != "ctx-thread-1" || ref["task_id"] != "task-of-thread-1" {
		t.Fatalf("artifact does not carry the token's context/task: %v", ref)
	}
	if ref["tenant"] != e.tenant || ref["deployment"] != e.deployment || ref["subject"] != alice {
		t.Fatalf("artifact does not carry the full isolation scope: %v", ref)
	}

	code, res := e.call(t, "DeleteByContext", tok, mustJSON(t, map[string]any{"context_id": "ctx-thread-1"}))
	mustStatus(t, code, http.StatusOK, res)
	if intOf(t, res, "tombstoned") != 2 || intOf(t, res, "held") != 0 {
		t.Fatalf("cascade = %v, want tombstoned=2 held=0", res)
	}

	for _, id := range []string{first, second} {
		if code, body := e.call(t, "Get", tok, mustJSON(t, map[string]any{"id": id})); code == http.StatusOK {
			t.Fatalf("Get after cascade still serves %s: %v", id, body)
		}
		status, served, _ := e.downloadRaw(t, tok, mustJSON(t, map[string]any{"id": id}), "")
		if status == http.StatusOK || served == "first-thread-bytes" || served == "second-thread-bytes" {
			t.Fatalf("Download after cascade still serves %s (status %d, %d bytes)", id, status, len(served))
		}
	}

	// A repeated cascade is a no-op and reports the rows it found already gone.
	code, again := e.call(t, "DeleteByContext", tok, mustJSON(t, map[string]any{"context_id": "ctx-thread-1"}))
	mustStatus(t, code, http.StatusOK, again)
	if intOf(t, again, "tombstoned") != 0 || intOf(t, again, "already_deleted") != 2 {
		t.Fatalf("repeated cascade = %v, want tombstoned=0 already_deleted=2", again)
	}
}

// TestHTTPCascadeLeavesLegalHoldAlone: the hold is part of the same PDF
// obligation, so a cascade must skip it rather than honour it silently wrong.
func TestHTTPCascadeLeavesLegalHoldAlone(t *testing.T) {
	e := newEnv(t)
	tok := e.signBound(t, alice, "ctx-hold", "task-hold")
	held := e.uploadSeal(t, tok, "k-hold-1", "pinned-by-hold", labels.Set{})
	free := e.uploadSeal(t, tok, "k-hold-2", "not-pinned", labels.Set{})

	code, body := e.call(t, "SetLegalHold", tok, mustJSON(t, map[string]any{"id": held, "on": true}))
	mustStatus(t, code, http.StatusOK, body)

	code, res := e.call(t, "DeleteByContext", tok, mustJSON(t, map[string]any{"context_id": "ctx-hold"}))
	mustStatus(t, code, http.StatusOK, res)
	if intOf(t, res, "tombstoned") != 1 || intOf(t, res, "held") != 1 {
		t.Fatalf("cascade = %v, want tombstoned=1 held=1", res)
	}
	code, ref := e.call(t, "Get", tok, mustJSON(t, map[string]any{"id": held}))
	mustStatus(t, code, http.StatusOK, ref)
	if status, served, _ := e.downloadRaw(t, tok, mustJSON(t, map[string]any{"id": held}), ""); status != http.StatusOK || served != "pinned-by-hold" {
		t.Fatalf("held artifact must stay downloadable, status %d", status)
	}
	if code, body := e.call(t, "Get", tok, mustJSON(t, map[string]any{"id": free})); code == http.StatusOK {
		t.Fatalf("unpinned artifact survived the cascade: %v", body)
	}
}

// TestHTTPCascadeCannotCrossSubjectOrContext: the cascade selects inside the
// caller's own scope only, and an empty or foreign selector deletes nothing.
func TestHTTPCascadeCannotCrossSubjectOrContext(t *testing.T) {
	e := newEnv(t)
	aliceTok := e.signBound(t, alice, "ctx-shared", "task-alice")
	malloryTok := e.signBound(t, mallory, "ctx-shared", "task-mallory")
	aliceID := e.uploadSeal(t, aliceTok, "k-shared-a", "alice-bytes", labels.Set{})
	malloryID := e.uploadSeal(t, malloryTok, "k-shared-m", "mallory-bytes", labels.Set{})

	code, res := e.call(t, "DeleteByContext", malloryTok, mustJSON(t, map[string]any{"context_id": "ctx-shared"}))
	mustStatus(t, code, http.StatusOK, res)
	if intOf(t, res, "tombstoned") != 1 {
		t.Fatalf("mallory cascade = %v, want exactly her own artifact", res)
	}
	if code, body := e.call(t, "Get", aliceTok, mustJSON(t, map[string]any{"id": aliceID})); code != http.StatusOK {
		t.Fatalf("alice lost bytes to another subject's cascade: %d %v", code, body)
	}
	if code, body := e.call(t, "Get", malloryTok, mustJSON(t, map[string]any{"id": malloryID})); code == http.StatusOK {
		t.Fatalf("mallory's own cascade left her artifact readable: %v", body)
	}

	// Malformed and empty selectors are refused, not treated as "everything".
	if code, body := e.call(t, "DeleteByContext", aliceTok, mustJSON(t, map[string]any{"context_id": ""})); code == http.StatusOK {
		t.Fatalf("empty context cascade accepted: %v", body)
	}
	if code, body := e.call(t, "DeleteByContext", aliceTok, mustJSON(t, map[string]any{"context_id": "bad/context"})); code == http.StatusOK {
		t.Fatalf("malformed context cascade accepted: %v", body)
	}
	if code, _ := e.call(t, "DeleteBySubject", "", `{}`); code == http.StatusOK {
		t.Fatalf("anonymous cascade accepted")
	}

	// DeleteBySubject reaches the rest of the caller's own artifacts only.
	code, res = e.call(t, "DeleteBySubject", aliceTok, `{}`)
	mustStatus(t, code, http.StatusOK, res)
	if intOf(t, res, "tombstoned") != 1 {
		t.Fatalf("subject cascade = %v, want tombstoned=1", res)
	}
	if code, body := e.call(t, "Get", aliceTok, mustJSON(t, map[string]any{"id": aliceID})); code == http.StatusOK {
		t.Fatalf("subject cascade left alice's artifact readable: %v", body)
	}
}

// TestHTTPCascadeByTasks covers artifacts whose writer only knew its task.
func TestHTTPCascadeByTasks(t *testing.T) {
	e := newEnv(t)
	tok := e.signBound(t, alice, "ctx-tasks", "task-only")
	id := e.uploadSeal(t, tok, "k-task-only", "task-bound-bytes", labels.Set{})

	code, res := e.call(t, "DeleteByTasks", tok, mustJSON(t, map[string]any{"task_ids": []string{"task-only", "task-unknown"}}))
	mustStatus(t, code, http.StatusOK, res)
	if intOf(t, res, "tombstoned") != 1 {
		t.Fatalf("task cascade = %v, want tombstoned=1", res)
	}
	if code, body := e.call(t, "Get", tok, mustJSON(t, map[string]any{"id": id})); code == http.StatusOK {
		t.Fatalf("task cascade left the artifact readable: %v", body)
	}
}
