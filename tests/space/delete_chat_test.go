package space_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// delete_chat_test.go — a user must be able to DELETE their own chat.
//
// The durable half has existed for a long time and is published on the State API:
// DeleteThread is a scope-filtered DELETE of the thread row plus an ON DELETE
// CASCADE over runs, intents, stream/inbox events and checkpoints, FORBIDDEN under
// legalhold and NOT_FOUND for an absent or foreign thread. What never existed is
// the user-facing seam: no *space.StateClient method issued the op, no Coordinator
// method owned the rules, and no route was mounted on the Space edge - so the
// browser could list a chat forever and never get rid of one.
//
// Two rules this file pins down, because both are security-relevant rather than
// cosmetic:
//
//  1. Authorization decides BEFORE anything is deleted, with the same no-leak
//     semantics as every other thread op: another subject's chat answers 404 (not
//     403) and stays intact.
//  2. A chat whose run is still LIVE (pending/running/waiting) is NOT deleted. The
//     cascade would take that run's intent journal with it, and the journal is the
//     only barrier that stops a re-delivered run from repeating an external effect.
//     Deleting under a live run would destroy the evidence its own retry needs, so
//     the edge refuses with CONFLICT and leaves the chat untouched; once the run is
//     terminal - here after CancelTurn closes it durably - the same delete succeeds.

// finishChat drives one browser subject through AcceptTurn -> DispatchTurn ->
// terminal, in a thread of its own (distinct context_id per tag), and returns the
// (turn_id, thread_id) pair. Distinct context ids matter: State keys a thread by
// context_id, so reusing one would silently collapse two chats into one.
func finishChat(t *testing.T, base, subject, tag string) (string, string) {
	t.Helper()
	code, body := postSpaceAs(t, base, "AcceptTurn", map[string]any{
		"key":        "dc-" + tag + "-" + contracts.NewID(),
		"context_id": "ctx-" + tag,
		"input":      map[string]any{"role": "user", "content": tag},
		"max_steps":  1,
	}, subject)
	if code != http.StatusOK {
		t.Fatalf("AcceptTurn(%s/%s) = %d, want 200 (body=%s)", subject, tag, code, body)
	}
	acc := decodeMap(t, body)
	turnID, _ := acc["turn_id"].(string)
	threadID, _ := acc["thread_id"].(string)
	if turnID == "" || threadID == "" {
		t.Fatalf("AcceptTurn(%s/%s) missing ids: %s", subject, tag, body)
	}
	if c, b := postSpaceAs(t, base, "DispatchTurn", nil, subject); c != http.StatusOK {
		t.Fatalf("DispatchTurn(%s/%s) = %d, want 200 (body=%s)", subject, tag, c, b)
	}
	deadline := time.Now().Add(5 * time.Second)
	done := false
	for !done && time.Now().Before(deadline) {
		c, b := postSpaceAs(t, base, "GetTurn", map[string]any{"turn_id": turnID}, subject)
		if c != http.StatusOK {
			t.Fatalf("GetTurn(%s/%s) poll = %d (body=%s)", subject, tag, c, b)
		}
		status, _ := decodeMap(t, b)["status"].(string)
		done = status == string(space.TurnDone)
		if !done {
			time.Sleep(15 * time.Millisecond)
		}
	}
	if !done {
		t.Fatalf("chat(%s/%s) never reached done", subject, tag)
	}
	// Wait for the DURABLE row too, not just the in-memory snapshot. The worker sets
	// the turn done before its Release reaches State, so a chat can legitimately be
	// reported done in memory while its run row still says running - and the delete
	// gate under test reads that row. Polling the snapshot alone would make this test
	// fail or pass on transport timing rather than on the rule.
	waitDurableTerminal(t, base, subject, threadID)
	return turnID, threadID
}

// waitDurableTerminal blocks until every durable run of the thread reports a
// terminal status, read through ListRunHistory (State, not the turn table).
func waitDurableTerminal(t *testing.T, base, subject, threadID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		c, b := postSpaceAs(t, base, "ListRunHistory", map[string]any{"thread_id": threadID}, subject)
		if c != http.StatusOK {
			t.Fatalf("ListRunHistory(%s) = %d (body=%s)", threadID, c, b)
		}
		runs, _ := decodeMap(t, b)["runs"].([]any)
		terminal := len(runs) > 0
		for _, item := range runs {
			st, _ := item.(map[string]any)["status"].(string)
			last = st
			if st == "pending" || st == "running" || st == "waiting" {
				terminal = false
			}
		}
		if terminal {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("thread %s never reached a durable terminal run status (last=%q)", threadID, last)
}

// chatIDs is the subject's own chat list, read through the durable ListChats op.
func chatIDs(t *testing.T, base, subject string) map[string]bool {
	t.Helper()
	code, body := postSpaceAs(t, base, "ListChats", map[string]any{"limit": 50}, subject)
	if code != http.StatusOK {
		t.Fatalf("ListChats(%s) = %d, want 200 (body=%s)", subject, code, body)
	}
	return parseChatIDs(t, body)
}

// parseChatIDs reads the thread ids out of a ListChats response body.
func parseChatIDs(t *testing.T, body []byte) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	list, _ := decodeMap(t, body)["threads"].([]any)
	for _, item := range list {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if id, _ := m["thread_id"].(string); id != "" {
			out[id] = true
		}
	}
	return out
}

// TestDeleteChatRemovesTheChatAndItsDurableHistory is the user-visible promise:
// delete a chat and it is gone from the list, its durable history is unreachable,
// an identical second delete reports NOT_FOUND, and the neighbour chat is untouched.
func TestDeleteChatRemovesTheChatAndItsDurableHistory(t *testing.T) {
	hs := newHTTPSpace(t, &stubCore{}, authPerSubject)

	_, keepID := finishChat(t, hs.URL, "alice", "keep")
	_, goneID := finishChat(t, hs.URL, "alice", "delete-me")

	ids := chatIDs(t, hs.URL, "alice")
	if !ids[keepID] || !ids[goneID] {
		t.Fatalf("before delete ListChats = %v, want both %s and %s", ids, keepID, goneID)
	}

	if c, b := postSpaceAs(t, hs.URL, "DeleteChat", map[string]any{"thread_id": goneID}, "alice"); c != http.StatusOK {
		t.Fatalf("DeleteChat(own chat) = %d, want 200 (body=%s)", c, b)
	}

	after := chatIDs(t, hs.URL, "alice")
	if after[goneID] {
		t.Fatalf("deleted chat %s still listed: %v", goneID, after)
	}
	if !after[keepID] {
		t.Fatalf("neighbour chat %s disappeared too: %v", keepID, after)
	}

	// The durable history of a deleted chat must be unreachable, not merely hidden
	// by the list: the cascade is what stops a deleted chat from being readable by
	// id after a restart dropped every in-memory index.
	if c, b := postSpaceAs(t, hs.URL, "ListRunHistory", map[string]any{"thread_id": goneID}, "alice"); c != http.StatusNotFound {
		t.Fatalf("ListRunHistory(deleted) = %d, want 404 (body=%s)", c, b)
	}
	if c, b := postSpaceAs(t, hs.URL, "ReadStream", map[string]any{"thread_id": goneID, "after": 0, "limit": 10}, "alice"); c != http.StatusNotFound {
		t.Fatalf("ReadStream(deleted) = %d, want 404 (body=%s)", c, b)
	}
	// A deleted chat stays deleted: a second delete is NOT_FOUND, never a second 200.
	if c, b := postSpaceAs(t, hs.URL, "DeleteChat", map[string]any{"thread_id": goneID}, "alice"); c != http.StatusNotFound {
		t.Fatalf("DeleteChat(second) = %d, want 404 (body=%s)", c, b)
	}

	// The neighbour keeps its durable history.
	if c, b := postSpaceAs(t, hs.URL, "ListRunHistory", map[string]any{"thread_id": keepID}, "alice"); c != http.StatusOK {
		t.Fatalf("ListRunHistory(neighbour) = %d, want 200 (body=%s)", c, b)
	}
}

// TestDeleteChatDoesNotLeakAnotherSubjectsChat proves the delete seam cannot be
// used to probe or destroy another subject's chat: a foreign delete is 404 (the
// same answer as for a chat that never existed) and the victim's chat is intact.
// It ends with a POSITIVE control, because without one this test passes on a tree
// that has no DeleteChat route at all: an unmounted route answers 404 too, and a 404
// from "no such operation" would be read here as "authorization worked".
func TestDeleteChatDoesNotLeakAnotherSubjectsChat(t *testing.T) {
	hs := newHTTPSpace(t, &stubCore{}, authPerSubject)

	_, aliceID := finishChat(t, hs.URL, "alice", "alice-chat")

	code, body := postSpaceAs(t, hs.URL, "DeleteChat", map[string]any{"thread_id": aliceID}, "bob")
	if code != http.StatusNotFound {
		t.Fatalf("DeleteChat(other subject's chat) = %d, want 404 (body=%s)", code, body)
	}

	if !chatIDs(t, hs.URL, "alice")[aliceID] {
		t.Fatalf("alice's chat vanished after bob's failed delete")
	}
	if c, b := postSpaceAs(t, hs.URL, "ListRunHistory", map[string]any{"thread_id": aliceID}, "alice"); c != http.StatusOK {
		t.Fatalf("ListRunHistory(alice, after bob's delete) = %d, want 200 (body=%s)", c, b)
	}
	if c, b := postSpaceAs(t, hs.URL, "DeleteChat", map[string]any{"thread_id": "thread-that-never-was"}, "alice"); c != http.StatusNotFound {
		t.Fatalf("DeleteChat(unknown) = %d, want 404 (body=%s)", c, b)
	}

	if c, b := postSpaceAs(t, hs.URL, "DeleteChat", map[string]any{"thread_id": aliceID}, "alice"); c != http.StatusOK {
		t.Fatalf("DeleteChat(alice's own chat) = %d, want 200 - the positive control failed, so the 404s above prove nothing (body=%s)", c, b)
	}
	if chatIDs(t, hs.URL, "alice")[aliceID] {
		t.Fatalf("alice's own delete left the chat in ListChats")
	}
}

// TestDeleteChatRefusesWhileARunIsLive pins the safety rule: with a run still
// executing (the Core parks forever), the delete is refused CONFLICT, the chat and
// its durable run rows stay, and only after CancelTurn has closed the run does the
// same delete go through. The refusal is what protects the intent journal that
// keeps a re-delivered run from repeating an external effect.
func TestDeleteChatRefusesWhileARunIsLive(t *testing.T) {
	core := &stubCore{blockForever: true}
	hs := newHTTPSpace(t, core, authPerSubject)

	code, body := postSpaceAs(t, hs.URL, "AcceptTurn", map[string]any{
		"key":        "dc-live-" + contracts.NewID(),
		"context_id": "ctx-live",
		"input":      map[string]any{"role": "user", "content": "hold"},
		"max_steps":  3,
	}, "alice")
	if code != http.StatusOK {
		t.Fatalf("AcceptTurn = %d, want 200 (body=%s)", code, body)
	}
	acc := decodeMap(t, body)
	turnID, _ := acc["turn_id"].(string)
	threadID, _ := acc["thread_id"].(string)
	if c, b := postSpaceAs(t, hs.URL, "DispatchTurn", nil, "alice"); c != http.StatusOK {
		t.Fatalf("DispatchTurn = %d, want 200 (body=%s)", c, b)
	}

	// Wait for the durable row to say running: the gate this test exercises reads
	// State, not the in-memory turn table, so it must see the claim.
	deadline := time.Now().Add(5 * time.Second)
	for {
		c, b := postSpaceAs(t, hs.URL, "ListRunHistory", map[string]any{"thread_id": threadID}, "alice")
		if c != http.StatusOK {
			t.Fatalf("ListRunHistory = %d (body=%s)", c, b)
		}
		runs, _ := decodeMap(t, b)["runs"].([]any)
		if len(runs) == 1 {
			if st, _ := runs[0].(map[string]any)["status"].(string); st == "running" {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("run never reached durable status running")
		}
		time.Sleep(15 * time.Millisecond)
	}

	code, body = postSpaceAs(t, hs.URL, "DeleteChat", map[string]any{"thread_id": threadID}, "alice")
	if code != http.StatusConflict {
		t.Fatalf("DeleteChat(live run) = %d, want 409 (body=%s)", code, body)
	}
	if got, _ := decodeMap(t, body)["code"].(string); got != contracts.ErrConflict {
		t.Fatalf("DeleteChat(live run) code = %q, want %s (body=%s)", got, contracts.ErrConflict, body)
	}
	if !strings.Contains(strings.ToLower(string(body)), "cancel") {
		t.Fatalf("the refusal must say what the user has to do; body=%s", body)
	}

	// Refused means UNTOUCHED: the chat is still listed and its run row is still there.
	if !chatIDs(t, hs.URL, "alice")[threadID] {
		t.Fatalf("chat vanished after a refused delete")
	}
	c, b := postSpaceAs(t, hs.URL, "ListRunHistory", map[string]any{"thread_id": threadID}, "alice")
	if c != http.StatusOK {
		t.Fatalf("ListRunHistory(after refused delete) = %d, want 200 (body=%s)", c, b)
	}
	if runs, _ := decodeMap(t, b)["runs"].([]any); len(runs) != 1 {
		t.Fatalf("run rows after a refused delete = %d, want 1 (body=%s)", len(runs), b)
	}

	// Cancel closes the run durably, and only then is the chat deletable.
	if c, b := postSpaceAs(t, hs.URL, "CancelTurn", map[string]any{"turn_id": turnID}, "alice"); c != http.StatusOK {
		t.Fatalf("CancelTurn = %d, want 200 (body=%s)", c, b)
	}
	cancelDeadline := time.Now().Add(5 * time.Second)
	for {
		c, b := postSpaceAs(t, hs.URL, "ListRunHistory", map[string]any{"thread_id": threadID}, "alice")
		if c != http.StatusOK {
			t.Fatalf("ListRunHistory(after cancel) = %d (body=%s)", c, b)
		}
		runs, _ := decodeMap(t, b)["runs"].([]any)
		if len(runs) == 1 {
			if st, _ := runs[0].(map[string]any)["status"].(string); st == "canceled" {
				break
			}
		}
		if time.Now().After(cancelDeadline) {
			t.Fatalf("run never reached durable status canceled (body=%s)", b)
		}
		time.Sleep(15 * time.Millisecond)
	}

	if c, b := postSpaceAs(t, hs.URL, "DeleteChat", map[string]any{"thread_id": threadID}, "alice"); c != http.StatusOK {
		t.Fatalf("DeleteChat(after cancel) = %d, want 200 (body=%s)", c, b)
	}
	if ids := chatIDs(t, hs.URL, "alice"); ids[threadID] {
		t.Fatalf("canceled chat still listed after delete: %v", ids)
	}
}
