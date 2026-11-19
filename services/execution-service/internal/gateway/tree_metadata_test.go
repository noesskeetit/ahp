package gateway_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Tree fields (ahp.root, ahp.depth) are gateway-set facts per PDF rev3.2 6.1:
// the values a message carries under those keys are ignored, a declared
// parent is audited against the gateway's own store, and a chain the gateway
// cannot audit (unknown parent, cycle, depth beyond the guard) is refused.
// Without this, an agent could pick its own tree -- and with it the tree the
// Model Gateway budgets are accounted against.

func treeMeta(t *testing.T, e *env, taskID string) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	row, err := e.store.Task(ctx, e.scope, taskID)
	if err != nil {
		t.Fatalf("Task(%s): %v", taskID, err)
	}
	return row.Metadata
}

func TestTreeFieldsAreServerSet(t *testing.T) {
	e := newEnv(t)
	svc := e.service(t, echoExecutor(), nil)

	// A root task gets itself as root and depth 0, and client-sent tree keys
	// are overwritten by the server's derivation.
	root := mustSubmit(t, svc, e.scope, e.caller, "msg-tree-root", func(r *a2a.SendMessageRequest) {
		r.Metadata = map[string]any{
			gateway.MetaTreeRoot:  "forged-root",
			gateway.MetaTreeDepth: 999,
		}
	})
	md := treeMeta(t, e, string(root.ID))
	if md[gateway.MetaTreeRoot] != string(root.ID) {
		t.Fatalf("root task ahp.root = %v, want own id %s", md[gateway.MetaTreeRoot], root.ID)
	}
	if depth, _ := md[gateway.MetaTreeDepth].(float64); depth != 0 {
		t.Fatalf("root task ahp.depth = %v, want 0", md[gateway.MetaTreeDepth])
	}

	child := mustSubmit(t, svc, e.scope, e.caller, "msg-tree-child", func(r *a2a.SendMessageRequest) {
		r.Metadata = map[string]any{gateway.MetaParentTask: string(root.ID)}
	})
	md = treeMeta(t, e, string(child.ID))
	if md[gateway.MetaTreeRoot] != string(root.ID) {
		t.Fatalf("child ahp.root = %v, want %s", md[gateway.MetaTreeRoot], root.ID)
	}
	if depth, _ := md[gateway.MetaTreeDepth].(float64); depth != 1 {
		t.Fatalf("child ahp.depth = %v, want 1", md[gateway.MetaTreeDepth])
	}

	grand := mustSubmit(t, svc, e.scope, e.caller, "msg-tree-grand", func(r *a2a.SendMessageRequest) {
		r.Metadata = map[string]any{gateway.MetaParentTask: string(child.ID)}
	})
	md = treeMeta(t, e, string(grand.ID))
	if md[gateway.MetaTreeRoot] != string(root.ID) {
		t.Fatalf("grandchild ahp.root = %v, want %s", md[gateway.MetaTreeRoot], root.ID)
	}
	if depth, _ := md[gateway.MetaTreeDepth].(float64); depth != 2 {
		t.Fatalf("grandchild ahp.depth = %v, want 2", md[gateway.MetaTreeDepth])
	}
}

func TestUnknownParentRefused(t *testing.T) {
	e := newEnv(t)
	svc := e.service(t, echoExecutor(), nil)
	_, err := submit(t, svc, e.scope, e.caller, "msg-tree-orphan", func(r *a2a.SendMessageRequest) {
		r.Metadata = map[string]any{gateway.MetaParentTask: "no-such-parent-" + e.sfx}
	})
	if contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("unknown parent err = %v, want ErrInvalid", err)
	}
}

func TestForgedCycleChainRefused(t *testing.T) {
	e := newEnv(t)
	svc := e.service(t, echoExecutor(), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Plant a two-node cycle directly in the store -- exactly the shape that
	// pre-validation rows or another writer could leave behind. The audit on a
	// NEW child must fail closed rather than walk forever.
	a := "cycle-a-" + e.sfx
	b := "cycle-b-" + e.sfx
	err := pgx.BeginFunc(ctx, e.pool, func(tx pgx.Tx) error {
		if _, err := e.store.InsertTask(ctx, tx, gateway.TaskRow{
			Scope: e.scope, Caller: e.caller, ContextID: "ctx-cycle-" + e.sfx,
			Kind: "generic", ParentTask: b, State: a2a.TaskStateSubmitted, ID: a,
		}); err != nil {
			return err
		}
		_, err := e.store.InsertTask(ctx, tx, gateway.TaskRow{
			Scope: e.scope, Caller: e.caller, ContextID: "ctx-cycle-" + e.sfx,
			Kind: "generic", ParentTask: a, State: a2a.TaskStateSubmitted, ID: b,
		})
		return err
	})
	if err != nil {
		t.Fatalf("plant cycle: %v", err)
	}
	_, err = submit(t, svc, e.scope, e.caller, "msg-cycle-child", func(r *a2a.SendMessageRequest) {
		r.Metadata = map[string]any{gateway.MetaParentTask: a}
	})
	if contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("cyclic parent err = %v, want ErrInvalid", err)
	}
}

func TestDepthGuard(t *testing.T) {
	e := newEnv(t)
	svc := e.service(t, echoExecutor(), nil)

	prev := mustSubmit(t, svc, e.scope, e.caller, "msg-chain-0", nil)
	rootID := string(prev.ID)
	// Depths 1..MaxTaskTreeDepth must all be accepted.
	for d := 1; d <= gateway.MaxTaskTreeDepth; d++ {
		parentID := string(prev.ID)
		msg := "msg-chain-" + strconv.Itoa(d)
		prev = mustSubmit(t, svc, e.scope, e.caller, msg, func(r *a2a.SendMessageRequest) {
			r.Metadata = map[string]any{gateway.MetaParentTask: parentID}
		})
	}
	md := treeMeta(t, e, string(prev.ID))
	if depth, _ := md[gateway.MetaTreeDepth].(float64); depth != float64(gateway.MaxTaskTreeDepth) {
		t.Fatalf("deepest accepted depth = %v, want %d", md[gateway.MetaTreeDepth], gateway.MaxTaskTreeDepth)
	}
	if md[gateway.MetaTreeRoot] != rootID {
		t.Fatalf("deep chain root = %v, want %s", md[gateway.MetaTreeRoot], rootID)
	}
	// One beyond the guard is refused.
	tooDeepID := fmt.Sprintf("msg-chain-%d", gateway.MaxTaskTreeDepth+1)
	parentID := string(prev.ID)
	if _, err := submit(t, svc, e.scope, e.caller, tooDeepID, func(r *a2a.SendMessageRequest) {
		r.Metadata = map[string]any{gateway.MetaParentTask: parentID}
	}); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("depth-beyond-guard err = %v, want ErrInvalid", err)
	}
}
