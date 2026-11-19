package execbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// A protocol fixture, not an LLM: terminal is externally gated so assertions
// cannot accidentally accept chunks sliced from an already completed answer.
type gatedInference struct {
	release chan struct{}
	calls   atomic.Int32
	stopped chan struct{}
}

func (f *gatedInference) Model() string { return "gated-protocol" }
func (f *gatedInference) Stream(context.Context, model.ChatRequest) (model.Stream, error) {
	f.calls.Add(1)
	return &gatedModelStream{owner: f}, nil
}

type gatedModelStream struct {
	owner *gatedInference
	n     int
}

func (s *gatedModelStream) Next(ctx context.Context) (model.Delta, error) {
	s.n++
	if s.n == 1 {
		return model.Delta{Text: "first "}, nil
	}
	if s.n == 2 {
		select {
		case <-ctx.Done():
			return model.Delta{}, ctx.Err()
		case <-s.owner.release:
			return model.Delta{Text: "second"}, nil
		}
	}
	return model.Delta{}, model.ErrStreamDone
}
func (s *gatedModelStream) Usage() model.Usage {
	if s.n > 2 {
		return model.Usage{PromptTokens: 3, CompletionTokens: 2, TotalTokens: 5}
	}
	return model.Usage{}
}
func (s *gatedModelStream) Close() error {
	select {
	case s.owner.stopped <- struct{}{}:
	default:
	}
	return nil
}

type disconnectWriter struct {
	http.ResponseWriter
	n int
}

func (w *disconnectWriter) Write(p []byte) (int, error) {
	w.n++
	if w.n > 2 {
		return 0, io.ErrClosedPipe
	}
	return w.ResponseWriter.Write(p)
}
func (w *disconnectWriter) Flush()                      { w.ResponseWriter.(http.Flusher).Flush() }
func (w *disconnectWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func TestRemoteStreamRealPGIncrementalReconnectAndSingleDispatch(t *testing.T) {
	pool := dbPool(t)
	inf := &gatedInference{release: make(chan struct{}), stopped: make(chan struct{}, 1)}
	b, err := New(context.Background(), pool, Config{Inference: inf})
	if err != nil {
		t.Fatal(err)
	}
	scope := uniqueScope()
	var requests atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims := auth.Claims{Kind: auth.KindTask, Tenant: scope.Tenant, Subject: scope.Subject, Deployment: scope.Deployment, Task: "task", Context: "ctx"}
		r = r.WithContext(transport.WithPrincipal(r.Context(), transport.Principal{Scope: scope, Claims: claims}))
		if requests.Add(1) == 1 {
			b.serveModel(&disconnectWriter{ResponseWriter: w}, r)
		} else {

			b.serveModel(w, r)
		}
	}))
	defer srv.Close()
	remote, err := NewRemoteModel(srv.URL, "", srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	ctx = core.WithRunBinding(core.WithRunScope(ctx, scope, "fixture-task"), core.RunBinding{TaskID: "task", ContextID: "ctx"})
	first := make(chan struct{})
	done := make(chan error, 1)
	var got []core.ModelDelta
	req := &core.ModelRequest{Messages: []core.ChatMessage{{Role: "user", Content: json.RawMessage(`"go"`)}}, MaxTokens: 128}
	go func() {
		resp, e := remote.GenerateStream(ctx, req, func(d core.ModelDelta) error {
			got = append(got, d)
			if len(got) == 1 {
				close(first)
			}
			return nil
		})
		if e == nil && string(resp.Content) != `"first second"` {
			e = errors.New("wrong final output")
		}
		done <- e
	}()
	select {
	case <-first:
	case e := <-done:
		t.Fatalf("no first useful byte: %v", e)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case e := <-done:
		t.Fatalf("terminal before provider release: %v", e)
	default:
	}
	// Force the first subscription to finish after its first delta; the bounded
	// send heartbeat hits disconnectWriter while the provider remains blocked.
	deadline := time.Now().Add(3 * time.Second)
	for requests.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(30 * time.Millisecond)
	}
	if requests.Load() < 2 {
		t.Fatal("subscriber did not reconnect")
	}
	if inf.calls.Load() != 1 {
		t.Fatalf("redispatched %d", inf.calls.Load())
	}
	close(inf.release)
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if len(got) != 2 || got[0].Sequence != 1 || got[1].Sequence != 2 {
		t.Fatalf("delta replay %+v", got)
	}
	if inf.calls.Load() != 1 {
		t.Fatal("reconnect inferred twice")
	}
}

func TestRemoteStreamCancelStopsStalledProviderAndRetainsUnknown(t *testing.T) {
	pool := dbPool(t)
	inf := &gatedInference{release: make(chan struct{}), stopped: make(chan struct{}, 1)}
	b, err := New(context.Background(), pool, Config{Inference: inf})
	if err != nil {
		t.Fatal(err)
	}
	scope := uniqueScope()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims := auth.Claims{Kind: auth.KindTask, Tenant: scope.Tenant, Subject: scope.Subject, Deployment: scope.Deployment, Task: "task", Context: "ctx"}
		b.serveModel(w, r.WithContext(transport.WithPrincipal(r.Context(), transport.Principal{Scope: scope, Claims: claims})))
	}))
	defer srv.Close()
	remote, _ := NewRemoteModel(srv.URL, "", srv.Client())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = core.WithRunBinding(core.WithRunScope(ctx, scope, "fixture-task"), core.RunBinding{TaskID: "task", ContextID: "ctx"})
	id := ""
	_, err = remote.GenerateStream(ctx, &core.ModelRequest{Messages: []core.ChatMessage{{Role: "user", Content: json.RawMessage(`"go"`)}}, MaxTokens: 128}, func(d core.ModelDelta) error { id = d.GenerationID; cancel(); return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel %v", err)
	}
	select {
	case <-inf.stopped:
	case <-time.After(4 * time.Second):
		t.Fatal("stalled provider not canceled")
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		g, e := b.gens.Get(context.Background(), scope, id)
		if e != nil {
			t.Fatal(e)
		}
		if g.Status == model.GenCanceled {
			var status string
			var settled bool
			if e = pool.QueryRow(context.Background(), `SELECT o.status,r.settled FROM ahp_model.operations o JOIN ahp_model.budget_reservations r ON r.budget_id=$2 AND r.key=$3 WHERE o.key=$1 AND o.tenant=$4`, g.EffectKey, g.BudgetID, g.ReserveKey, scope.Tenant).Scan(&status, &settled); e != nil {
				t.Fatal(e)
			}
			if status != "SIDE_EFFECT_UNKNOWN" || settled {
				t.Fatalf("fabricated accounting status=%s settled=%v", status, settled)
			}
			if _, e = b.gens.ReadDeltas(context.Background(), contracts.Scope{Tenant: scope.Tenant, Subject: "foreign", Deployment: scope.Deployment}, id, 0, 1); contracts.Code(e) != contracts.ErrNotFound {
				t.Fatal("scope leak")
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("canceled outcome not durable")
}

type boundedEscapedInference struct {
	calls  atomic.Int32
	output string
}

func (p *boundedEscapedInference) Model() string { return "review-escaped" }
func (p *boundedEscapedInference) Stream(context.Context, model.ChatRequest) (model.Stream, error) {
	p.calls.Add(1)
	return &boundedEscapedStream{output: p.output}, nil
}

type boundedEscapedStream struct {
	output string
	offset int
}

func (s *boundedEscapedStream) Next(context.Context) (model.Delta, error) {
	if s.offset == len(s.output) {
		return model.Delta{}, model.ErrStreamDone
	}
	end := min(s.offset+10000, len(s.output))
	part := s.output[s.offset:end]
	s.offset = end
	return model.Delta{Text: part}, nil
}
func (s *boundedEscapedStream) Usage() model.Usage { return model.Usage{TotalTokens: 7} }
func (s *boundedEscapedStream) Close() error       { return nil }

func TestEscapedOutputWithinRawBoundStreamsToTerminal(t *testing.T) {
	for _, output := range []string{strings.Repeat("<", 700000), strings.Repeat("\x01", 2<<20)} {
		t.Run(fmt.Sprint(len(output)), func(t *testing.T) {
			pool := dbPool(t)
			provider := &boundedEscapedInference{output: output}
			bridge, err := New(context.Background(), pool, Config{Inference: provider})
			if err != nil {
				t.Fatal(err)
			}
			scope := uniqueScope()
			var subscriptions atomic.Int32
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				subscriptions.Add(1)
				claims := auth.Claims{Kind: auth.KindTask, Tenant: scope.Tenant, Subject: scope.Subject, Deployment: scope.Deployment, Task: "review-task", Context: "review-context"}
				r = r.WithContext(transport.WithPrincipal(r.Context(), transport.Principal{Scope: scope, Claims: claims}))
				bridge.serveModel(w, r)
			}))
			defer srv.Close()
			remote, err := NewRemoteModel(srv.URL, "", srv.Client())
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			ctx = core.WithRunScope(ctx, scope, "fixture-authority")
			ctx = core.WithRunBinding(ctx, core.RunBinding{TaskID: "review-task", ContextID: "review-context"})
			deltas := 0
			got, err := remote.GenerateStream(ctx, &core.ModelRequest{Messages: []core.ChatMessage{{Role: "user", Content: json.RawMessage(`"go"`)}}}, func(d core.ModelDelta) error { deltas++; return nil })
			if err != nil {
				var state, ledger string
				var settled bool
				var rawBytes int
				qerr := pool.QueryRow(context.Background(), `SELECT g.status,o.status,r.settled,octet_length(g.output) FROM ahp_model.generations g JOIN ahp_model.operations o ON o.key=g.effect_key AND o.tenant=g.tenant JOIN ahp_model.budget_reservations r ON r.budget_id=g.budget_id AND r.key=g.reserve_key WHERE g.tenant=$1`, scope.Tenant).Scan(&state, &ledger, &settled, &rawBytes)
				t.Fatalf("raw_output_bytes=%d deltas=%d subscriptions=%d provider_calls=%d gen=%s ledger=%s settled=%v query=%v: %v", rawBytes, deltas, subscriptions.Load(), provider.calls.Load(), state, ledger, settled, qerr, err)
			}
			var content string
			if err := json.Unmarshal(got.Content, &content); err != nil || content != output || provider.calls.Load() != 1 {
				t.Fatalf("terminal mismatch bytes=%d calls=%d decode=%v", len(content), provider.calls.Load(), err)
			}
			var ledgerBytes int
			if err := pool.QueryRow(context.Background(), `SELECT octet_length(o.result::text) FROM ahp_model.operations o WHERE o.tenant=$1`, scope.Tenant).Scan(&ledgerBytes); err != nil {
				t.Fatal(err)
			}
			if ledgerBytes > 1024 {
				t.Fatalf("receipt grew with result: %d", ledgerBytes)
			}
			t.Logf("raw_output_bytes=%d deltas=%d subscriptions=%d provider_calls=%d receipt_bytes=%d", len(content), deltas, subscriptions.Load(), provider.calls.Load(), ledgerBytes)
		})
	}
}
