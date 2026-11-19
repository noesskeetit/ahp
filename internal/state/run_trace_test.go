package state

import (
	"encoding/json"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
	aot "github.com/ahp-platform/ahp/pkg/otel"
)

func TestRunTraceMetadataPreservesOpaquePayloadAndReplay(t *testing.T) {
	e := kvFixture(t)
	existing, err := e.s.GetRun(e.ctx, e.scope, e.fence.RunID)
	if err != nil {
		t.Fatal(err)
	}
	first := "00-11111111111111111111111111111111-2222222222222222-01"
	second := "00-33333333333333333333333333333333-4444444444444444-01"
	for _, payload := range []string{`[1,"x"]`, `"scalar"`} {
		key := contracts.NewID()
		r, err := e.s.CreateRun(aot.WithTraceParent(e.ctx, first), e.scope, existing.ThreadID, "trace-task", key, json.RawMessage(payload))
		if err != nil {
			t.Fatal(err)
		}
		if r.TraceParent != first || !jsonEqualTrace(r.Payload, json.RawMessage(payload)) {
			t.Fatalf("metadata/payload %+v", r)
		}
		replay, err := e.s.CreateRun(aot.WithTraceParent(e.ctx, second), e.scope, existing.ThreadID, "trace-task", key, json.RawMessage(payload))
		if err != nil || replay.TraceParent != first || replay.ID != r.ID {
			t.Fatalf("replay %+v %v", replay, err)
		}
		got, err := e.s.GetRun(e.ctx, e.scope, r.ID)
		if err != nil || got.TraceParent != first {
			t.Fatalf("read %+v %v", got, err)
		}
	}
}

func jsonEqualTrace(a, b json.RawMessage) bool {
	var aa, bb any
	_ = json.Unmarshal(a, &aa)
	_ = json.Unmarshal(b, &bb)
	x, _ := json.Marshal(aa)
	y, _ := json.Marshal(bb)
	return string(x) == string(y)
}
