//go:build state_integration

package space_test

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/execbridge"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	aot "github.com/ahp-platform/ahp/pkg/otel"
	"github.com/ahp-platform/ahp/pkg/transport"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type wireStreamProvider struct {
	release chan struct{}
	calls   atomic.Int32
}

func (f *wireStreamProvider) Model() string { return "protocol-stream" }
func (f *wireStreamProvider) Stream(context.Context, model.ChatRequest) (model.Stream, error) {
	f.calls.Add(1)
	return &wireProviderStream{f: f}, nil
}

type wireProviderStream struct {
	f *wireStreamProvider
	n int
}

func (s *wireProviderStream) Next(ctx context.Context) (model.Delta, error) {
	s.n++
	switch s.n {
	case 1:
		return model.Delta{Text: "Привет "}, nil
	case 2:
		select {
		case <-s.f.release:
			return model.Delta{Text: "мир"}, nil
		case <-ctx.Done():
			return model.Delta{}, ctx.Err()
		}
	default:
		return model.Delta{}, model.ErrStreamDone
	}
}
func (s *wireProviderStream) Usage() model.Usage {
	if s.n > 2 {
		return model.Usage{PromptTokens: 4, CompletionTokens: 3, TotalTokens: 7}
	}
	return model.Usage{}
}
func (s *wireProviderStream) Close() error { return nil }

// Actual PG + real mTLS State/Model + production Core/Coordinator/SSE. The
// provider alone is a gated protocol fixture, deliberately not an LLM claim.
func TestIncrementalModelCoreSpaceSSEAndDurableTrace_RealState(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	oldProvider, oldProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(oldProvider)
		otel.SetTextMapPropagator(oldProp)
	})
	e := cNewEnv(t)
	// Install the same production middleware/transport before the first request.
	e.srv.Config.Handler = aot.HTTPMiddleware("state")(e.srv.Config.Handler)
	e.srv.Client().Transport = aot.Transport(e.srv.Client().Transport)
	pool, err := pgxpool.New(e.ctx, os.Getenv("AHP_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	fixture := &wireStreamProvider{release: make(chan struct{})}
	defer func() {
		select {
		case <-fixture.release:
		default:
			close(fixture.release)
		}
	}()
	bridge, err := execbridge.New(e.ctx, pool, execbridge.Config{Inference: fixture})
	if err != nil {
		t.Fatal(err)
	}
	authorize := func(context.Context, auth.Claims, string) error { return nil }
	modelServer := httptest.NewUnstartedServer(aot.HTTPMiddleware("model")(bridge.ModelAPI(e.verifier, authorize)))
	modelServer.TLS = e.srv.TLS.Clone()
	modelServer.StartTLS()
	defer modelServer.Close()
	modelClient := modelServer.Client()
	tr := modelClient.Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{e.ca.clientCert(t, cWorkloadURI(e.tenant, e.deployment))}
	modelClient.Transport = aot.Transport(tr)
	remote, err := execbridge.NewRemoteModel(modelServer.URL, "", modelClient)
	if err != nil {
		t.Fatal(err)
	}
	runtime := core.NewRuntime(core.RuntimeConfig{Model: remote, MaxTokens: 128})
	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, Core: runtime, WorkerID: "stream-worker"})
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: cSubject}
	token := e.signTask(t, "stream-task")
	ctx, span := aot.Tracer("test").Start(e.ctx, "ingress")
	traceID := span.SpanContext().TraceID()
	span.End()
	accepted, err := coord.AcceptTurn(ctx, token, scope, space.AcceptTurnRequest{Key: "stream-turn", ContextID: "ctx-stream", Input: json.RawMessage(`"go"`), MaxSteps: 1})
	if err != nil {
		t.Fatal(err)
	}
	// Cold runner: no in-memory ingress context survives to dispatch.
	cold := space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, Core: runtime, WorkerID: "cold-stream-worker"})
	if err = cold.DispatchTurn(e.ctx, token, scope); err != nil {
		t.Fatal(err)
	}
	handler := space.NewHandler(space.HandlerConfig{Coordinator: cold, Auth: func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r.WithContext(transport.WithPrincipal(r.Context(), transport.Principal{Scope: scope, Claims: auth.Claims{Kind: auth.KindTask, Task: "stream-task", Context: "ctx-stream"}})))
		})
	}})
	edge := httptest.NewServer(handler)
	defer edge.Close()
	streamCtx, stop := context.WithTimeout(e.ctx, 8*time.Second)
	defer stop()
	request, _ := http.NewRequestWithContext(streamCtx, http.MethodGet, edge.URL+"/v1/space/stream/"+accepted.ThreadID, nil)
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := edge.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatalf("SSE %d", response.StatusCode)
	}
	scan := bufio.NewScanner(response.Body)
	scan.Buffer(make([]byte, 4096), 4<<20)
	var firstUseful time.Time
	deltas := 0
	finished := false
	for scan.Scan() {
		line := scan.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var event struct {
			Type    string           `json:"type"`
			Cursor  int64            `json:"cursor"`
			Key     string           `json:"key"`
			Payload space.StepResult `json:"payload"`
		}
		if err = json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
			t.Fatal(err)
		}
		if event.Type == "model_delta" {
			deltas++
			if deltas == 1 {
				firstUseful = time.Now()
				if event.Payload.Done || string(event.Payload.Content) != `"Привет "` {
					t.Fatalf("first %+v", event)
				}
				runs, e2 := e.client.ListRuns(e.ctx, token, accepted.ThreadID)
				if e2 != nil || len(runs) != 1 || runs[0].Status != "running" {
					t.Fatalf("partial incorrectly finished: %+v %v", runs, e2)
				}
				if !strings.Contains(runs[0].TraceParent, traceID.String()) {
					t.Fatalf("durable ancestry lost: %q", runs[0].TraceParent)
				}
				close(fixture.release)
			}
		}
		if event.Type == "model" && deltas != 2 {
			t.Fatalf("final preceded deltas count=%d", deltas)
		}
		if event.Type == "end" {
			finished = true
			break
		}
	}
	if scan.Err() != nil {
		t.Fatal(scan.Err())
	}
	if firstUseful.IsZero() || !finished || deltas != 2 || fixture.calls.Load() != 1 {
		t.Fatalf("stream first=%v terminal=%v deltas=%d calls=%d", firstUseful, finished, deltas, fixture.calls.Load())
	}
	// Span tree must contain both the restored async runner and its Model/State
	// descendants with the same trace ID, not five singleton traces.
	foundRun, foundModel, foundCore := false, false, false
	for _, s := range exporter.GetSpans() {
		if s.SpanContext.TraceID() != traceID {
			continue
		}
		if s.Name == "space.run" {
			foundRun = s.Parent.IsValid()
		}
		if s.Name == "core.model" {
			foundCore = s.Parent.IsValid()
		}
		for _, a := range s.Attributes {
			if string(a.Key) == "service" && a.Value.AsString() == "model" {
				foundModel = s.Parent.IsValid()
			}
		}
	}
	if !foundRun || !foundCore || !foundModel {
		t.Fatalf("trace tree run=%v core=%v model=%v", foundRun, foundCore, foundModel)
	}
	t.Logf("real mTLS+PG: first useful delta before released terminal, ordered deltas=%d, provider dispatch=1, restored trace=%s", deltas, traceID)
}
