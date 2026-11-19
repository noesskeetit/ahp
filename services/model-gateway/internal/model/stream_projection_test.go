package model

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// A stream reader needs every terminal field, but never the stored prompt.
func TestStreamProjectionPreservesMetadataAndStoredLargeRequest(t *testing.T) {
	e := newModelEnv(t)
	ctx := context.Background()
	payload := strings.Repeat("x", 102400)
	for _, status := range []string{GenPending, GenRunning, GenStreaming, GenCompleted, GenFailed, GenCanceled} {
		t.Run(status, func(t *testing.T) {
			g, _, err := e.gens.Submit(ctx, e.scope, "projection-task", "projection-"+status, "projection-model", chatReq(payload), "budget-ref", "reserve-ref", 123, "provider-copy")
			if err != nil {
				t.Fatal(err)
			}
			for _, delta := range []string{"first ", "second"} {
				if err := e.gens.AppendOutput(ctx, e.scope, g.ID, delta, Usage{PromptTokens: 4, CompletionTokens: 3, TotalTokens: 7}); err != nil {
					t.Fatal(err)
				}
			}
			if status != GenPending {
				if _, err := e.gens.Transition(ctx, e.scope, g.ID, []string{GenPending}, GenRunning); err != nil {
					t.Fatal(err)
				}
			}
			if status == GenStreaming {
				if _, err := e.gens.Transition(ctx, e.scope, g.ID, []string{GenRunning}, GenStreaming); err != nil {
					t.Fatal(err)
				}
			}
			if status == GenCanceled {
				if _, err := e.gens.RequestCancel(ctx, e.scope, g.ID); err != nil {
					t.Fatal(err)
				}
			}
			if terminal(status) {
				if _, err := e.gens.FinalizeWithOutput(ctx, e.scope, g.ID, status, "fixture-terminal-reason", "terminal tool-wire output", Usage{PromptTokens: 4, CompletionTokens: 3, TotalTokens: 7}, "length"); err != nil {
					t.Fatal(err)
				}
			}
			full, err := e.gens.Get(ctx, e.scope, g.ID)
			if err != nil {
				t.Fatal(err)
			}
			var stored ChatRequest
			if json.Unmarshal(full.Request, &stored) != nil || len(stored.Messages) != 1 || stored.Messages[0].Content != payload {
				t.Fatal("ordinary Get lost or changed the stored large request")
			}
			var storedBytes int
			if err := e.pool.QueryRow(ctx, `SELECT octet_length(request::text) FROM ahp_model.generations WHERE id=$1`, g.ID).Scan(&storedBytes); err != nil || storedBytes < 102400 {
				t.Fatal("database no longer stores the large request")
			}
			projected, err := e.gens.GetForStream(ctx, e.scope, g.ID)
			if err != nil || projected.Request != nil {
				t.Fatal("stream projection failed or loaded request bytes")
			}
			full.Request = nil
			if !reflect.DeepEqual(projected, full) {
				t.Fatal("projection changed generation metadata or terminal fields")
			}
		})
	}
}

func TestStreamProjectionAndDeltasPreserveScopeCursorPagingAndReplay(t *testing.T) {
	e := newModelEnv(t)
	ctx := context.Background()
	g, _, err := e.gens.Submit(ctx, e.scope, "task", "projection-scope", "model", chatReq(strings.Repeat("x", 102400)), "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"one", "two", "three"} {
		if err := e.gens.AppendOutput(ctx, e.scope, g.ID, text, Usage{}); err != nil {
			t.Fatal(err)
		}
	}
	for _, field := range []string{"tenant", "subject", "deployment", "missing", "invalid"} {
		t.Run(field, func(t *testing.T) {
			scope, id, want := e.scope, g.ID, contracts.ErrNotFound
			switch field {
			case "tenant":
				scope.Tenant = "foreign"
			case "subject":
				scope.Subject = "foreign"
			case "deployment":
				scope.Deployment = "foreign"
			case "missing":
				id = "missing"
			case "invalid":
				scope = contracts.Scope{}
				want = contracts.ErrInvalid
			}
			if _, err := e.gens.GetForStream(ctx, scope, id); contracts.Code(err) != want {
				t.Fatalf("projection refusal code %q, want %q", contracts.Code(err), want)
			}
			if _, err := e.gens.ReadDeltas(ctx, scope, id, 0, 2); contracts.Code(err) != want {
				t.Fatalf("delta refusal code %q, want %q", contracts.Code(err), want)
			}
		})
	}
	for _, cursor := range []struct {
		after int64
		limit int
	}{{-1, 1}, {0, 0}, {0, 257}} {
		if _, err := e.gens.ReadDeltas(ctx, e.scope, "missing", cursor.after, cursor.limit); contracts.Code(err) != contracts.ErrInvalid {
			t.Fatal("cursor/page validation precedence changed")
		}
	}
	first, err := e.gens.ReadDeltas(ctx, e.scope, g.ID, 0, 2)
	if err != nil || len(first) != 2 || first[0].Text != "one" || first[0].Sequence != 1 || first[1].Text != "two" || first[1].Sequence != 2 {
		t.Fatal("first delta page changed")
	}
	second, err := e.gens.ReadDeltas(ctx, e.scope, g.ID, 2, 2)
	if err != nil || len(second) != 1 || second[0].Text != "three" || second[0].Sequence != 3 {
		t.Fatal("following delta page changed")
	}
	replay, err := e.gens.ReadDeltas(ctx, e.scope, g.ID, 0, 2)
	if err != nil || !reflect.DeepEqual(first, replay) {
		t.Fatal("delta replay changed")
	}
	empty, err := e.gens.ReadDeltas(ctx, e.scope, g.ID, 100, 2)
	if err != nil || len(empty) != 0 {
		t.Fatal("future delta cursor no longer returns an empty page")
	}
}
