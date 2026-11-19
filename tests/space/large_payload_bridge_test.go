//go:build state_integration

package space_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	artifactstore "github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/artifactsapi"
	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/execbridge"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/internal/references"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/stateapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	artifacts "github.com/ahp-platform/ahp/sdk/go/artifacts"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type largeFinalProvider struct {
	calls       atomic.Int32
	output      string
	twoAttempts bool
}

func (p *largeFinalProvider) Model() string { return "large-final-wire-fixture" }
func (p *largeFinalProvider) Stream(context.Context, model.ChatRequest) (model.Stream, error) {
	attempt := p.calls.Add(1)
	stop := "stop"
	if p.twoAttempts && attempt == 1 {
		stop = "length"
	}
	return &largeFinalStream{output: p.output, stop: stop}, nil
}

type largeFinalStream struct {
	output string
	offset int
	stop   string
}

func (s *largeFinalStream) Next(context.Context) (model.Delta, error) {
	if s.offset == len(s.output) {
		return model.Delta{}, model.ErrStreamDone
	}
	end := min(s.offset+16384, len(s.output))
	part := s.output[s.offset:end]
	s.offset = end
	stop := ""
	if end == len(s.output) {
		stop = s.stop
	}
	return model.Delta{Text: part, FinishReason: stop}, nil
}
func (s *largeFinalStream) Usage() model.Usage { return model.Usage{TotalTokens: 7} }
func (s *largeFinalStream) Close() error       { return nil }

// The provider is deterministic. Every owner, receipt, streaming HTTP hop,
// State frame, blob and lease recovery below is real (PG/MinIO/mTLS), not LLM E2E.
func TestLargeFinalModelReceiptCoreCoordinatorState_Real(t *testing.T) {
	for _, crash := range []bool{false, true} {
		t.Run(fmt.Sprintf("crash_before_model_frame_%v", crash), func(t *testing.T) {
			started := time.Now()
			var stateRPCs, modelRPCs largeRecoveryRPCs
			defer func() {
				stateJSON, _ := json.Marshal(stateRPCs.snapshot())
				modelJSON, _ := json.Marshal(modelRPCs.snapshot())
				t.Logf("large receipt phase total=%s state_rpc=%s model_rpc=%s", time.Since(started).Round(time.Millisecond), stateJSON, modelJSON)
			}()
			e := cNewEnv(t)
			scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: cSubject}
			pool, err := pgxpool.New(e.ctx, os.Getenv("AHP_TEST_DATABASE_URL"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(pool.Close)
			contextID := "large-final"
			mint := func(task string) (string, error) {
				return e.signer.Sign(auth.Claims{Audience: cAudience, Kind: auth.KindTask, Tenant: e.tenant, Deployment: e.deployment, Subject: cSubject, Task: task, Root: "root", Context: contextID, Grant: "grant"}, 5*time.Minute)
			}
			authorize := func(context.Context, auth.Claims, string) error { return nil }
			states := state.New(pool)
			sh, err := stateapi.New(states, e.verifier, authorize, func(_ context.Context, _ contracts.Scope, task string) (string, error) { return mint(task) })
			if err != nil {
				t.Fatal(err)
			}
			endpoint, err := url.Parse(os.Getenv("AHP_S3_ENDPOINT"))
			if err != nil || endpoint.Host == "" {
				t.Fatal("S3 required")
			}
			blob, err := minio.New(endpoint.Host, &minio.Options{Creds: credentials.NewStaticV4(os.Getenv("AHP_MINIO_ROOT_USER"), os.Getenv("AHP_MINIO_ROOT_PASSWORD"), ""), Secure: endpoint.Scheme == "https"})
			if err != nil {
				t.Fatal(err)
			}
			bucket := fmt.Sprintf("ahp-large-final-%d", time.Now().UnixNano())
			ast, err := artifactstore.New(pool, blob, bucket, func(context.Context, contracts.Scope, string, string) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			states.SetCheckpointArtifacts(ast)
			source := state.ReferenceSource(pool, "ahp_state")
			artifactstore.WithReferenceKeeper(artifactstore.ReferenceScanFunc(func(ctx context.Context, tenant string, arts []artifactstore.ArtifactQuery) (map[string]bool, error) {
				queries := make([]references.Artifact, len(arts))
				for i, a := range arts {
					queries[i] = references.Artifact{ID: a.ID, Digest: a.Digest}
				}
				result, err := source.InspectReferences(ctx, tenant, nil, queries)
				return result.Referenced, err
			}))(ast)
			artifactstore.WithCheckpointSettlement(states.CheckpointOperationSettled)(ast)
			if err = ast.Migrate(e.ctx); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				ctx := context.Background()
				for obj := range blob.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
					if obj.Err == nil {
						_ = blob.RemoveObject(ctx, bucket, obj.Key, minio.RemoveObjectOptions{VersionID: obj.VersionID})
					}
				}
				_ = blob.RemoveBucket(ctx, bucket)
			})
			ah, err := artifactsapi.New(ast, e.verifier, authorize)
			if err != nil {
				t.Fatal(err)
			}
			mux := http.NewServeMux()
			mux.Handle("/v1/artifacts/", ah)
			mux.Handle("/v1/state/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/PutCheckpoint") {
					http.Error(w, "checkpoint disabled: recovery must use model receipt", 503)
					return
				}
				sh.ServeHTTP(w, r)
			}))
			e.srv.Config.Handler = stateRPCs.wrap(mux)
			text := "x"
			if crash {
				text = "\x01"
			}
			provider := &largeFinalProvider{output: strings.Repeat(text, 2<<20), twoAttempts: crash}
			attempts := 1
			if crash {
				attempts = 2
			}
			bridge, err := execbridge.New(e.ctx, pool, execbridge.Config{Inference: provider})
			if err != nil {
				t.Fatal(err)
			}
			modelServer := httptest.NewUnstartedServer(modelRPCs.wrap(bridge.ModelAPI(e.verifier, authorize)))
			modelServer.TLS = e.srv.TLS.Clone()
			modelServer.StartTLS()
			t.Cleanup(modelServer.Close)
			mhc := modelServer.Client()
			tr := mhc.Transport.(*http.Transport).Clone()
			tr.TLSClientConfig.Certificates = []tls.Certificate{e.ca.clientCert(t, cWorkloadURI(e.tenant, e.deployment))}
			mhc.Transport = tr
			remote, err := execbridge.NewRemoteModel(modelServer.URL, "", mhc)
			if err != nil {
				t.Fatal(err)
			}
			ac, err := artifacts.New(artifacts.Config{BaseURL: e.srv.URL, HTTPClient: e.srv.Client()})
			if err != nil {
				t.Fatal(err)
			}
			runtime := core.NewRuntime(core.RuntimeConfig{Model: remote, Artifacts: ac, MaxTokens: 128})
			var runner space.CoreRunner = runtime
			var paused *pauseAtBoundary
			if crash {
				paused = &pauseAtBoundary{inner: runtime, beforeModel: true, hit: make(chan struct{}), resume: make(chan struct{}), done: make(chan struct{})}
				runner = paused
				t.Cleanup(func() {
					select {
					case <-paused.resume:
					default:
						close(paused.resume)
					}
				})
			}
			coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, Core: runner, WorkerID: "first"})
			token, err := mint("bootstrap")
			if err != nil {
				t.Fatal(err)
			}
			acc, err := coord.AcceptTurn(e.ctx, token, scope, space.AcceptTurnRequest{Key: "large", ContextID: contextID, Input: json.RawMessage(`"large final"`), MaxSteps: attempts})
			if err != nil {
				t.Fatal(err)
			}
			if err = coord.DispatchTurn(e.ctx, token, scope); err != nil {
				t.Fatal(err)
			}
			if crash {
				select {
				case <-paused.hit:
					t.Logf("large receipt phase crash boundary after=%s", time.Since(started).Round(time.Millisecond))
				case <-time.After(30 * time.Second):
					t.Fatal("large final never reached durable boundary")
				}
				if _, err = pool.Exec(e.ctx, `UPDATE ahp_state.threads SET lease_expires=now()-interval '1 second' WHERE id=$1`, acc.ThreadID); err != nil {
					t.Fatal(err)
				}
				coord = space.NewCoordinator(space.CoordinatorConfig{StateClient: e.client, Core: core.NewRuntime(core.RuntimeConfig{Model: remote, Artifacts: ac, MaxTokens: 256}), WorkerID: "recovered"})
				if err = coord.DispatchTurn(e.ctx, token, scope); err != nil {
					t.Fatal(err)
				}
				t.Logf("large receipt phase recovered dispatch after=%s", time.Since(started).Round(time.Millisecond))
			}
			deadline := time.Now().Add(30 * time.Second)
			waitStarted := time.Now()
			for {
				run, er := e.client.GetRun(e.ctx, token, acc.TurnID)
				if er != nil {
					t.Fatal(er)
				}
				if run.Status == "done" {
					t.Logf("large receipt phase durable done after=%s recovered_wait=%s", time.Since(started).Round(time.Millisecond), time.Since(waitStarted).Round(time.Millisecond))
					break
				}
				if run.Status == "failed" || time.Now().After(deadline) {
					snapshot, _ := coord.TurnSnapshot(acc.TurnID)
					t.Fatalf("large final run status=%s failure=%s", run.Status, snapshot.FailureReason)
				}
				time.Sleep(20 * time.Millisecond)
			}
			if paused != nil {
				close(paused.resume)
				select {
				case <-paused.done:
				case <-time.After(3 * time.Second):
					t.Fatal("old runtime did not finish")
				}
			}
			if crash && stateRPCs.snapshot()["AppendStream"].Calls > 280 {
				t.Fatalf("recovery re-sent the already confirmed 128-fragment prefix: append calls=%d", stateRPCs.snapshot()["AppendStream"].Calls)
			}
			frames, err := e.client.ReadStream(e.ctx, token, acc.ThreadID, 0, 2)
			if err != nil {
				t.Fatal(err)
			}
			page := frames
			for len(page) == 2 {
				page, err = e.client.ReadStream(e.ctx, token, acc.ThreadID, page[len(page)-1].Sequence, 2)
				if err != nil {
					t.Fatal(err)
				}
				frames = append(frames, page...)
			}
			models, done, deltas := 0, 0, 0
			var ref artifacts.ArtifactRef
			for _, frame := range frames {
				if len(frame.Payload) > 1<<20 {
					t.Fatal("unbounded State frame")
				}
				switch frame.Kind {
				case "model_delta":
					deltas++
				case "done":
					done++
				case "model":
					models++
					var payload struct {
						Content struct {
							Artifact artifacts.ArtifactRef `json:"artifact"`
							Preview  string                `json:"preview"`
						} `json:"content"`
					}
					if er := json.Unmarshal(frame.Payload, &payload); er != nil {
						t.Fatal(er)
					}
					ref = payload.Content.Artifact
					if len(payload.Content.Preview) > 4096 {
						t.Fatal("unbounded preview")
					}
				}
			}
			if models != attempts || done != 1 || deltas == 0 || int(provider.calls.Load()) != attempts || ref.ID == "" {
				t.Fatalf("bad durable result: model=%d done=%d delta=%d provider=%d", models, done, deltas, provider.calls.Load())
			}
			bound := ac.WithTokenSource(func(context.Context) (string, error) { return token, nil })
			actual, err := bound.Download(e.ctx, ref.ID)
			if err != nil {
				t.Fatal(err)
			}
			expected, _ := json.Marshal(provider.output)
			if !bytes.Equal(actual, expected) {
				t.Fatal("large final download bytes differ from provider completion")
			}
			var generations, reservations int
			var spent int64
			if err = pool.QueryRow(e.ctx, `SELECT count(*) FROM ahp_model.generations WHERE tenant=$1`, scope.Tenant).Scan(&generations); err != nil {
				t.Fatal(err)
			}
			if err = pool.QueryRow(e.ctx, `SELECT count(*),sum(actual) FROM ahp_model.budget_reservations WHERE budget_id IN (SELECT id FROM ahp_model.budgets WHERE tenant=$1)`, scope.Tenant).Scan(&reservations, &spent); err != nil {
				t.Fatal(err)
			}
			if generations != attempts || reservations != attempts || spent != int64(attempts*7) {
				t.Fatalf("duplicated model receipt or charge: %d/%d/%d", generations, reservations, spent)
			}
			t.Logf("2MiB provider completion -> %d artifact bytes -> bounded durable model/done; recovery=%v provider_calls=%d generations=%d charge=%d", len(actual), crash, provider.calls.Load(), generations, spent)
		})
	}
}
