package gateway_test

// legacy_role_test.go -- a task whose journalled row predates the ingress
// role canonicalization must still drain with its text. Rewriting history is
// not required to unstrand it, and this test is why: selection is by canonical
// user turn, not by byte-equal spelling.

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/ahp-platform/ahp/internal/gateway"
)

type legacyRecorder struct {
	mu      sync.Mutex
	texts   []string
	sawNil  int
	attempt int
}

func (r *legacyRecorder) Execute(_ context.Context, in gateway.ExecInput) (gateway.ExecResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.attempt++
	if in.Input == nil {
		r.sawNil++
		return gateway.ExecResult{
			State:   a2a.TaskStateFailed,
			Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("no inbound message")),
		}, nil
	}
	text := ""
	for _, p := range in.Input.Parts {
		if t := p.Text(); t != "" {
			text += t
		}
	}
	r.texts = append(r.texts, text)
	return gateway.ExecResult{
		State:   a2a.TaskStateCompleted,
		Message: a2a.NewMessage(a2a.MessageRoleAgent, a2a.NewTextPart("answered")),
	}, nil
}

func (r *legacyRecorder) snapshot() (int, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sawNil, append([]string(nil), r.texts...)
}

func TestLegacyRoleRowStillReachesExecutor(t *testing.T) {
	e := newEnv(t)
	rec := &legacyRecorder{}
	svc := e.service(t, rec, nil)
	const want = "Составь один короткий абзац про снимки состояния."

	// Accepted and left SUBMITTED: the worker starts only after the row is
	// aged, so the drain provably runs against a legacy-spelled row.
	task := mustSubmit(t, svc, e.scope, e.caller, "legacy-role-"+e.sfx,
		func(r *a2a.SendMessageRequest) { r.Message.Parts = a2a.ContentParts{a2a.NewTextPart(want)} })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := e.pool.Exec(ctx, `UPDATE `+e.schema+`.task_messages
		SET role = 'user', payload = jsonb_set(payload, '{role}', '"user"'::jsonb)
		WHERE task_id = $1`, string(task.ID)); err != nil {
		t.Fatalf("age the journalled row to the v0 spelling: %v", err)
	}
	var payload string
	if err := e.pool.QueryRow(ctx,
		`SELECT payload::text FROM `+e.schema+`.task_messages WHERE task_id = $1`, string(task.ID)).Scan(&payload); err != nil {
		t.Fatalf("read the journalled row back: %v", err)
	}
	// jsonb renders with its own spacing, so the aging is proven by decoding the
	// stored document rather than by matching a byte layout.
	var stored struct {
		Role  string `json:"role"`
		Parts []struct {
			Text string `json:"text"`
		} `json:"parts"`
	}
	if err := json.Unmarshal([]byte(payload), &stored); err != nil {
		t.Fatalf("the journalled row is not the message document: %v (%s)", err, payload)
	}
	if stored.Role != "user" || len(stored.Parts) != 1 || stored.Parts[0].Text != want {
		t.Fatalf("the row was not aged to the legacy spelling, so this test would prove nothing: %s", payload)
	}

	go svc.Run(ctx)
	deadline := time.Now().Add(20 * time.Second)
	var (
		row gateway.TaskRow
		err error
	)
	for time.Now().Before(deadline) {
		if row, err = e.store.Task(ctx, e.scope, string(task.ID)); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				t.Fatalf("load task: %v (%v)", err, ctxErr)
			}
		} else if row.State.Terminal() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !row.State.Terminal() {
		t.Fatalf("task never drained: state = %s", row.State)
	}
	if row.State != a2a.TaskStateCompleted {
		t.Fatalf("a legacy-spelled user row must drain to COMPLETED, got %s (status=%+v)", row.State, row.StatusMessage)
	}

	// The read side tolerates the spelling; it does not rewrite a historical row,
	// so the executor may legitimately see the legacy role -- what it must never
	// see again is a nil input.
	sawNil, texts := rec.snapshot()
	if sawNil != 0 {
		t.Fatalf("the drain handed the executor a nil input %d time(s) over a row that carries the text", sawNil)
	}
	if len(texts) != 1 || texts[0] != want {
		t.Fatalf("executor saw %q, want exactly one copy of %q", texts, want)
	}

	var count int
	if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM `+e.schema+`.task_messages WHERE task_id=$1`, string(task.ID)).Scan(&count); err != nil {
		t.Fatalf("count history: %v", err)
	}
	if count != 1 {
		t.Fatalf("history must still hold the one inbound row, got %d", count)
	}
}
