package dote2e_test

// Reload-persistence for the personal Dot (PDF rev3.2 L930, L2044): the turn's
// durable record must survive the client fully disappearing - a fresh browser
// session (new mTLS connection, freshly issued human JWT) reads back the same
// turn, its pinned configuration, and the thread's frame stream from the
// Postgres-backed store. Whether the turn COMPLETES is DEF-DOTE2E-1's problem;
// this scenario proves the history plane only, and says so.

import (
	"context"
	"net/http"
	"net/http/cookiejar"
	"testing"
	"time"
)

func TestDotHistoryReloadsFromDurableStore(t *testing.T) {
	stand := loadStand(t)
	// The AgentConfig is published by the earlier dot-turn scenario in this run
	// (or by this scenario's own call, which is revision-ladder safe).
	publishDotAgentConfig(t, stand, "lane-qwen", []string{"save_artifact", "emil.delegate"},
		"You are the employee's personal Dot.", 6)
	human := stand.humanToken(t, stand.HTTP, mustLogin(t, "email"), mustLogin(t, "password"))
	stamp := time.Now().UTC().Format("150405")
	checks := map[string]any{}

	// (a) A turn is accepted under a unique conversation context.
	var accepted struct {
		TurnID   string `json:"turn_id"`
		ThreadID string `json:"thread_id"`
	}
	input := map[string]any{"parts": []any{map[string]any{"kind": "text", "text": "durable-history " + stamp}}}
	err := stand.call(stand.HTTP, "/v1/space/AcceptTurn", map[string]any{
		"key": "dot-reload-" + stamp, "context_id": "dot-reload-ctx-" + stamp,
		"input": input, "max_steps": 2}, human, &accepted)
	if err != nil {
		stand.record(t, "history-reload-persistence", "FAIL", map[string]any{"accept": map[string]any{
			"status": "FAIL", "error": errText(err)}})
		t.Fatalf("AcceptTurn refused: %v", errText(err))
	}
	checks["accept_turn"] = map[string]any{"status": "PASS", "turn": accepted.TurnID, "thread": accepted.ThreadID}

	// (b) The client "closes the laptop": a brand-new mTLS client, a fresh login
	// and a fresh human JWT. Nothing is reused from the accepting session.
	fresh := &http.Client{Transport: stand.HTTP.Transport.(*http.Transport).Clone(), Timeout: 60 * time.Second, Jar: newCookieJar()}
	freshHuman := stand.humanToken(t, fresh, mustLogin(t, "email"), mustLogin(t, "password"))

	var reload struct {
		ID          string `json:"id"`
		Status      string `json:"status"`
		AgentConfig *struct {
			Revision        int64 `json:"revision"`
			ProfileRevision int64 `json:"profile_revision"`
		} `json:"agent_config"`
	}
	if err := stand.call(fresh, "/v1/space/GetTurn", map[string]any{"turn_id": accepted.TurnID}, freshHuman, &reload); err != nil {
		checks["reload_turn_fresh_session"] = map[string]any{"status": "FAIL", "error": errText(err)}
		stand.record(t, "history-reload-persistence", "FAIL", checks)
		t.Fatalf("a fresh session could not reload the turn: %v", errText(err))
	}
	reloadOK := reload.ID == accepted.TurnID && reload.AgentConfig != nil && reload.AgentConfig.Revision >= 1
	checks["reload_turn_fresh_session"] = map[string]any{"status": "PASS", "same_turn": reload.ID == accepted.TurnID,
		"status_value": reload.Status, "config_revision": reload.AgentConfig.Revision,
		"note": "status reflects DEF-DOTE2E-1 for completion; the record and its pinned configuration reloaded from durable storage"}
	if !reloadOK {
		checks["reload_turn_fresh_session"].(map[string]any)["status"] = "FAIL"
		stand.record(t, "history-reload-persistence", "FAIL", checks)
		t.Fatalf("reloaded turn lost its identity or pinned configuration: %+v", reload)
	}

	// (c) The thread's frames replay from the durable stream on the fresh session.
	var stream struct {
		Events []map[string]any `json:"events"`
	}
	err = stand.call(fresh, "/v1/space/ReadStream", map[string]any{"thread_id": accepted.ThreadID, "after": 0}, freshHuman, &stream)
	checks["stream_replays_on_reload"] = map[string]any{"status": "PASS", "events": len(stream.Events),
		"error": truncate(errText(err), 120)}
	if err != nil {
		checks["stream_replays_on_reload"].(map[string]any)["status"] = "FAIL"
		stand.record(t, "history-reload-persistence", "FAIL", checks)
		t.Fatalf("frame stream did not replay: %v", errText(err))
	}

	// (d) Postgres holds the thread and run rows for this context.
	var threads, runs int
	if err := stand.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ahp_state.threads WHERE tenant=$1 AND context_id=$2`,
		stand.Scope["tenant"], "dot-reload-ctx-"+stamp).Scan(&threads); err != nil {
		t.Fatalf("threads probe: %v", err)
	}
	if err := stand.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ahp_state.runs r JOIN ahp_state.threads t ON t.id=r.thread_id
		  WHERE t.tenant=$1 AND t.context_id=$2`, stand.Scope["tenant"], "dot-reload-ctx-"+stamp).Scan(&runs); err != nil {
		t.Fatalf("runs probe: %v", err)
	}
	checks["postgres_rows"] = map[string]any{"status": "PASS", "threads": threads, "runs": runs}
	if threads == 0 || runs == 0 {
		checks["postgres_rows"].(map[string]any)["status"] = "FAIL"
		stand.record(t, "history-reload-persistence", "FAIL", checks)
		t.Fatal("durable state rows missing for an accepted turn")
	}
	stand.record(t, "history-reload-persistence", "PASS", checks)
}

func newCookieJar() *cookiejar.Jar {
	jar, _ := cookiejar.New(nil)
	return jar
}
