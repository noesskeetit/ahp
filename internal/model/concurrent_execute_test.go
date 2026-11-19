package model

import (
	"context"
	"testing"
)

func TestConcurrentExecuteDoesNotDeclareLiveGenerationUnknown(t *testing.T) {
	e := newModelEnv(t)
	started, release := make(chan struct{}), make(chan struct{})
	inf := &fakeInference{model: "concurrent"}
	inf.newStr = func(ChatRequest) (Stream, error) {
		close(started)
		<-release
		return &fakeStream{errAt: -1, deltas: []Delta{{Text: "one answer"}}, usage: Usage{TotalTokens: 4}}, nil
	}
	svc := e.service(inf)
	gen, err := svc.Submit(context.Background(), SubmitArgs{Scope: e.scope, Task: "task", Key: "same-request", Request: chatReq("answer")})
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		gen Generation
		err error
	}
	done := make(chan result, 1)
	go func() { g, err := svc.Execute(context.Background(), e.scope, gen.ID); done <- result{g, err} }()
	<-started
	replay, replayErr := svc.Execute(context.Background(), e.scope, gen.ID)
	close(release)
	first := <-done
	if replayErr != nil || terminal(replay.Status) {
		t.Fatalf("live concurrent replay: status=%s err=%v detail=%s", replay.Status, replayErr, replay.Error)
	}
	if first.err != nil || first.gen.Status != GenCompleted {
		t.Fatalf("original execution: %+v", first)
	}
	if inf.count() != 1 {
		t.Fatalf("provider called %d times", inf.count())
	}
}
