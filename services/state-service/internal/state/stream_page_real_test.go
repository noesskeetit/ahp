package state

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Source-ready for a separately granted fresh PostgreSQL test database. This
// test never weakens scope masking or bypasses the public append lease fence.
func TestReadStreamPagePreservesScopeAndLegacyCount_RealState(t *testing.T) {
	s := newEventStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	scope := eventScope("bounded-" + contracts.NewID())
	th, err := s.CreateThread(ctx, scope, "ctx-"+contracts.NewID(), "key-"+contracts.NewID())
	if err != nil {
		t.Fatal(err)
	}
	run, err := s.CreateRun(ctx, scope, th.ID, "task-bounded", "key-"+contracts.NewID(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	lease, err := s.ClaimRun(ctx, scope.Tenant, scope.Deployment, "bounded-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Run.ID != run.ID {
		t.Fatal("claimed another run")
	}
	payload := json.RawMessage(`{"text":"` + strings.Repeat("<", MaxPayloadBytes-len(`{"text":""}`)) + `"}`)
	want := make([]StreamEvent, 3)
	for i := range want {
		want[i], err = s.AppendStream(ctx, scope, run.ID, lease.Worker, lease.Epoch, contracts.NewID(), "model_delta", payload)
		if err != nil {
			t.Fatal(err)
		}
	}
	page, err := s.ReadStreamPage(ctx, scope, th.ID, 0, 256, MaxStreamResponseBytes)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(page)
	if len(page.Events) != 2 || !page.HasMore || !page.ByteLimited || page.NextAfter != want[1].Sequence || len(body)+1 > MaxStreamResponseBytes {
		t.Fatal("large scoped page did not stop at full JSON byte budget")
	}
	replay, err := s.ReadStreamPage(ctx, scope, th.ID, page.NextAfter, 256, MaxStreamResponseBytes)
	if err != nil || len(replay.Events) != 1 || replay.Events[0].ID != want[2].ID || replay.HasMore {
		t.Fatalf("excluded row lost on replay: %v", err)
	}
	if events, err := s.ReadStream(ctx, scope, th.ID, 0, 3); events != nil || contracts.Code(err) != contracts.ErrInvalid || contracts.ReasonOf(err) != "STREAM_PAGE_EXCEEDS_RESPONSE_BUDGET" {
		t.Fatalf("legacy count page silently truncated: len=%d err=%v", len(events), err)
	}
	if events, err := s.ReadStream(ctx, scope, th.ID, 0, 2); err != nil || len(events) != 2 {
		t.Fatalf("legacy count2 changed: %v", err)
	}
	for _, foreign := range []contracts.Scope{
		{Tenant: scope.Tenant, Deployment: scope.Deployment, Subject: "other"},
		{Tenant: "other", Deployment: scope.Deployment, Subject: scope.Subject},
		{Tenant: scope.Tenant, Deployment: "other", Subject: scope.Subject},
	} {
		if page, err := s.ReadStreamPage(ctx, foreign, th.ID, 0, 256, MaxStreamResponseBytes); contracts.Code(err) != contracts.ErrNotFound || page.Events != nil {
			t.Fatal("foreign scope not masked")
		}
	}
	if _, err := s.ReadStreamPage(ctx, scope, contracts.NewID(), 0, 256, MaxStreamResponseBytes); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatal("missing thread not masked")
	}
	empty, err := s.ReadStreamPage(ctx, scope, th.ID, want[2].Sequence, 256, MaxStreamResponseBytes)
	if err != nil || len(empty.Events) != 0 || empty.HasMore || empty.NextAfter != want[2].Sequence {
		t.Fatal("drained cursor changed")
	}
}
