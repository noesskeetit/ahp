package model

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestDeltaJournalOrderedScopedBoundedAndErased(t *testing.T) {
	e := newModelEnv(t)
	ctx := context.Background()
	g, _, err := e.gens.Submit(ctx, e.scope, "task", "deltas", "qwen", chatReq("hi"), "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"first ", "second"} {
		if err = e.gens.AppendOutput(ctx, e.scope, g.ID, text, Usage{}); err != nil {
			t.Fatal(err)
		}
	}
	ds, err := e.gens.ReadDeltas(ctx, e.scope, g.ID, 0, 256)
	if err != nil || len(ds) != 2 || ds[0].Text != "first " || ds[1].Text != "second" || ds[1].Sequence != 2 {
		t.Fatalf("deltas %+v %v", ds, err)
	}
	other := e.scope
	other.Subject = "other"
	if _, err = e.gens.ReadDeltas(ctx, other, g.ID, 0, 1); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign %v", err)
	}
	if err = e.ledger.FenceSubject(ctx, e.scope.Tenant, e.scope.Subject); err != nil {
		t.Fatal(err)
	}
	if err = e.gens.AppendOutput(ctx, e.scope, g.ID, "late", Usage{}); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("late %v", err)
	}
	if err = e.gens.prepareCompletion(ctx, e.scope, g.ID, "first second", "late final", "stop", Usage{}); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("late completion %v", err)
	}
	if _, err = e.pool.Exec(ctx, `DELETE FROM ahp_model.generations WHERE id=$1`, g.ID); err != nil {
		t.Fatal(err)
	}
	var n int
	_ = e.pool.QueryRow(ctx, `SELECT count(*) FROM ahp_model.generation_deltas WHERE generation_id=$1`, g.ID).Scan(&n)
	if n != 0 {
		t.Fatal("orphan deltas")
	}
}

func TestExecuteWithSinglePooledConnection(t *testing.T) {
	e := newModelEnv(t)
	config := e.pool.Config()
	config.MaxConns = 1
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	gens := NewGenerationStore(pool)
	budget := NewBudgetStore(pool)
	ledger, err := operations.New(pool, "ahp_model")
	if err != nil {
		t.Fatal(err)
	}
	inf := &fakeInference{model: "one-connection"}
	inf.streamText([]string{"one", "two"}, Usage{TotalTokens: 2})
	svc := NewService(gens, budget, ledger, inf)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	g, err := svc.Submit(ctx, SubmitArgs{Scope: e.scope, Task: "task", Key: "single-pool", Request: chatReq("go")})
	if err != nil {
		t.Fatal(err)
	}
	g, err = svc.Execute(ctx, e.scope, g.ID)
	if err != nil || g.Status != GenCompleted {
		t.Fatalf("pool starvation: %+v %v", g, err)
	}
}

func TestLedgerRecoveryPreservesTerminalWireAndFinishReason(t *testing.T) {
	e := newModelEnv(t)
	ctx := context.Background()
	inf := &fakeInference{model: "receipt-wire"}
	inf.streamText([]string{"must not run"}, Usage{TotalTokens: 1})
	svc := e.service(inf)
	g, err := svc.Submit(ctx, SubmitArgs{Scope: e.scope, Task: "task", Key: "receipt-wire", Request: chatReq("go")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = e.ledger.Dispatch(ctx, e.scope, g.EffectKey); err != nil {
		t.Fatal(err)
	}
	output := `{"text":"partial","tool_calls":[{"id":"tool-1","type":"function","function":{"name":"save","arguments":"{}"}}]}`
	receipt, _ := json.Marshal(resultPayload{Status: GenCompleted, Usage: Usage{TotalTokens: 7}, Output: output, FinishReason: "tool_calls"})
	if _, err = e.ledger.Complete(ctx, e.scope, g.EffectKey, receipt, false); err != nil {
		t.Fatal(err)
	}
	got, err := svc.Execute(ctx, e.scope, g.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Output != output || got.FinishReason != "tool_calls" || got.Status != GenCompleted || inf.count() != 0 {
		t.Fatalf("receipt lost: %+v calls=%d", got, inf.count())
	}
}

func TestGenerationPersistsLengthWithoutInventingStop(t *testing.T) {
	e := newModelEnv(t)
	ctx := context.Background()
	inf := &fakeInference{model: "length"}
	inf.newStr = func(ChatRequest) (Stream, error) {
		return &fakeStream{errAt: -1, deltas: []Delta{{Text: "unfinished"}, {FinishReason: "length"}}, usage: Usage{TotalTokens: 7}}, nil
	}
	svc := e.service(inf)
	g, err := svc.Submit(ctx, SubmitArgs{Scope: e.scope, Task: "task", Key: "length", Request: chatReq("go")})
	if err != nil {
		t.Fatal(err)
	}
	g, err = svc.Execute(ctx, e.scope, g.ID)
	if err != nil || g.FinishReason != "length" {
		t.Fatalf("finish %+v %v", g, err)
	}
}

func TestCompactCompletionReceiptRecoveryAndDigestMismatch(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(fmt.Sprint(corrupt), func(t *testing.T) {
			e := newModelEnv(t)
			ctx := context.Background()
			inf := &fakeInference{model: "compact"}
			inf.streamText([]string{"must not run"}, Usage{TotalTokens: 1})
			svc := e.service(inf)
			g, err := svc.Submit(ctx, SubmitArgs{Scope: e.scope, Task: "task", Key: "compact-recovery", Request: chatReq("go")})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = e.ledger.Dispatch(ctx, e.scope, g.EffectKey); err != nil {
				t.Fatal(err)
			}
			output := `{"text":"` + strings.Repeat("<", 700000) + `","tool_calls":[{"id":"x","type":"function","function":{"name":"save","arguments":"{}"}}]}`
			usage := Usage{TotalTokens: 7}
			if err = e.gens.prepareCompletion(ctx, e.scope, g.ID, "", output, "tool_calls", usage); err != nil {
				t.Fatal(err)
			}
			receipt, _ := json.Marshal(resultPayload{Status: GenCompleted, Usage: usage, OutputSHA256: outputDigest(output), FinishReason: "tool_calls"})
			if len(receipt) > 1024 {
				t.Fatal("large inline receipt")
			}
			if _, err = e.ledger.Complete(ctx, e.scope, g.EffectKey, receipt, false); err != nil {
				t.Fatal(err)
			}
			if corrupt {
				if _, err = e.pool.Exec(ctx, `UPDATE ahp_model.generations SET output='wrong' WHERE id=$1`, g.ID); err != nil {
					t.Fatal(err)
				}
			}
			got, err := svc.Execute(ctx, e.scope, g.ID)
			if corrupt {
				if contracts.Code(err) != contracts.ErrUnavailable {
					t.Fatalf("corrupt reference accepted: %v", err)
				}
			} else if err != nil || got.Status != GenCompleted || got.Output != output || got.FinishReason != "tool_calls" {
				t.Fatalf("compact receipt recovery failed status=%s error=%v", got.Status, err)
			}
			if inf.count() != 0 {
				t.Fatal("recovery redispatched")
			}
		})
	}
}

func TestOversizedToolEnvelopeFailsTerminalWithActualUsage(t *testing.T) {
	e := newModelEnv(t)
	ctx := context.Background()
	inf := &fakeInference{model: "large-tool"}
	args, _ := json.Marshal(map[string]string{"content": strings.Repeat("<", 3<<20)})
	inf.newStr = func(ChatRequest) (Stream, error) {
		return &fakeStream{errAt: -1, usage: Usage{TotalTokens: 7}, deltas: []Delta{{FinishReason: "tool_calls", ToolCalls: []ToolCall{{ID: "tool", Name: "save", Arguments: args}}}}}, nil
	}
	svc := e.service(inf)
	budget, err := e.budget.Root(ctx, e.scope, "task", "root-large-tool", 100)
	if err != nil {
		t.Fatal(err)
	}
	g, err := svc.Submit(ctx, SubmitArgs{Scope: e.scope, Task: "task", Key: "large-tool", Request: chatReq("go"), BudgetID: budget.ID, ReserveKey: "reserve", ReserveAmount: 50})
	if err != nil {
		t.Fatal(err)
	}
	g, err = svc.Execute(ctx, e.scope, g.ID)
	if err != nil || g.Status != GenFailed || !strings.Contains(g.Error, "encoded stream bound") {
		t.Fatalf("large envelope status=%s error=%v", g.Status, err)
	}
	got, err := e.budget.Get(ctx, e.scope, budget.ID)
	if err != nil || got.Spent != 7 || got.Reserved != 0 {
		t.Fatalf("actual usage lost %+v %v", got, err)
	}
	op, err := e.ledger.Get(ctx, e.scope, g.EffectKey)
	if err != nil || op.Status != operations.StatusFailed {
		t.Fatalf("ledger status=%s error=%v", op.Status, err)
	}
}
