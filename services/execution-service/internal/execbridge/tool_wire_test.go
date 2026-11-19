package execbridge

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/ahp-platform/ahp/internal/executor"
	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// scriptedToolInference replays a scripted list of provider turns and records
// the ChatRequest of each call: the tool-wire contract is exactly "definitions
// went out, calls came back, the follow-up carried the tool result".
type scriptedToolInference struct {
	mu      sync.Mutex
	calls   int
	texts   []string
	callsIn []model.ChatRequest
}

func (s *scriptedToolInference) Model() string { return "scripted-tool-model" }

func (s *scriptedToolInference) Stream(_ context.Context, req model.ChatRequest) (model.Stream, error) {
	s.mu.Lock()
	i := s.calls
	s.calls++
	s.callsIn = append(s.callsIn, req)
	s.mu.Unlock()
	if i >= len(s.texts) {
		i = len(s.texts) - 1
	}
	return &scriptedDeltaStream{payloads: []model.Delta{s.payload(i, req)}}, nil
}

func (s *scriptedToolInference) payload(i int, req model.ChatRequest) model.Delta {
	txt := s.texts[i]
	if i == 0 {
		// The scripted provider "sees" the definitions it was handed; the test
		// fails below if the request carried none.
		if len(req.Tools) == 0 {
			return model.Delta{Text: "", FinishReason: "stop"}
		}
		return model.Delta{FinishReason: "tool_calls", ToolCalls: []model.ToolCall{{
			ID: "call_artifact_1", Name: "write_artifact",
			Arguments: json.RawMessage(`{"name":"tool-proof.txt","mime":"text/plain","content":"tool wrote me"}`),
		}}}
	}
	return model.Delta{Text: txt, FinishReason: "stop"}
}

type scriptedDeltaStream struct {
	payloads []model.Delta
	i        int
}

func (s *scriptedDeltaStream) Next(context.Context) (model.Delta, error) {
	if s.i >= len(s.payloads) {
		return model.Delta{}, model.ErrStreamDone
	}
	d := s.payloads[s.i]
	s.i++
	return d, nil
}
func (s *scriptedDeltaStream) Usage() model.Usage {
	return model.Usage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15}
}
func (s *scriptedDeltaStream) Close() error { return nil }

// artifactPlaneSkipReason answers, in machine-readable form, why the legs that
// need the real artifact plane cannot run here. Red-by-default on a missing
// environment is the failure mode that made the wide gate unreadable: a red
// default suite says nothing about the product. Silence is the other wrong
// answer. So the legs skip, and the skip text names the prerequisite and states
// what the skip is NOT -- coverage. Precedent: the registry probe harness
// (D-REG-3), "a skip is not coverage".
//
// A non-empty return means "do not run"; an empty return means every
// prerequisite the lane was given is present.
func artifactPlaneSkipReason() string {
	var missing []string
	for _, k := range []string{"AHP_S3_ENDPOINT", "AHP_MINIO_ROOT_USER", "AHP_MINIO_ROOT_PASSWORD"} {
		if os.Getenv(k) == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return "SKIP_ENV[tag=artifact_plane] reason=object_store_env_absent missing=" + strings.Join(missing, ",") +
			" -- these legs measure the real artifact plane; they are evidence only when they RAN, " +
			"and a skip here means not-covered, not passed"
	}
	return laneObjectStoreSkipReason(os.Getenv("AHP_S3_ENDPOINT"))
}

// laneObjectStoreSkipReason is the identity gate: an endpoint that happens to be
// in the environment is NOT a permission to write. This workspace runs one MinIO
// for every lane (audited: 1768 buckets on 127.0.0.1:29000, with the shared
// ahp-root-check-20261102 carrying objects from several harnesses), so "the env
// named an endpoint" and "this lane may write there" are different claims. A leg
// that conflates them creates buckets and objects on somebody else's instance.
//
// A lane states its store explicitly with AHP_TEST_OBJECT_STORE=<host:port>.
// Without that statement the legs skip, naming why; with a different one they
// skip and name both identities. Credentials are never part of either message.
func laneObjectStoreSkipReason(endpoint string) string {
	have, err := normalizeObjectStoreEndpoint(endpoint)
	if err != nil {
		return "SKIP_ENV[tag=artifact_plane] reason=object_store_endpoint_unparsable -- AHP_S3_ENDPOINT is not a host:port; refusing to guess which instance to write to"
	}
	designated := strings.TrimSpace(os.Getenv("AHP_TEST_OBJECT_STORE"))
	if designated == "" {
		return "SKIP_ENV[tag=artifact_plane] reason=object_store_not_lane_designated endpoint=" + have +
			" -- an inherited AHP_S3_ENDPOINT is not a write permission: this Mac shares one MinIO across lanes. " +
			"Set AHP_TEST_OBJECT_STORE=<host:port> for the instance this lane owns. A skip here means not-covered, not passed"
	}
	want, err := normalizeObjectStoreEndpoint(designated)
	if err != nil {
		return "SKIP_ENV[tag=artifact_plane] reason=object_store_designation_unparsable -- AHP_TEST_OBJECT_STORE is not a host:port, so the lane's own designation cannot be honoured"
	}
	if have != want {
		return "SKIP_ENV[tag=artifact_plane] reason=object_store_identity_mismatch endpoint=" + have +
			" designated=" + want + " -- refusing to write to an object store the lane was not given. A skip here means not-covered, not passed"
	}
	return ""
}

// normalizeObjectStoreEndpoint accepts "host:port", "scheme://host:port" and a
// bare host, and returns lowercase host:port with the scheme default filled in.
// Anything carrying userinfo is rejected rather than echoed.
func normalizeObjectStoreEndpoint(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", errors.New("empty endpoint")
	}
	if !strings.Contains(value, "://") {
		value = "http://" + value
	}
	u, err := url.Parse(value)
	if err != nil || u.Host == "" {
		return "", errors.New("unparsable endpoint")
	}
	if u.User != nil {
		return "", errors.New("endpoint must not carry credentials")
	}
	host := strings.ToLower(u.Hostname())
	port := u.Port()
	if port == "" {
		if u.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	return host + ":" + port, nil
}

func minioFromEnv(t *testing.T) (*minio.Client, string) {
	t.Helper()
	if reason := artifactPlaneSkipReason(); reason != "" {
		t.Log(reason)
		t.Skip(reason)
	}
	raw := os.Getenv("AHP_S3_ENDPOINT")
	ak, sk := os.Getenv("AHP_MINIO_ROOT_USER"), os.Getenv("AHP_MINIO_ROOT_PASSWORD")
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		t.Fatalf("bad AHP_S3_ENDPOINT")
	}
	client, err := minio.New(u.Host, &minio.Options{
		Creds: credentials.NewStaticV4(ak, sk, ""), Secure: u.Scheme == "https",
	})
	if err != nil {
		t.Fatal(err)
	}
	var rnd [6]byte
	_, _ = rand.Read(rnd[:])
	return client, "ahp-toolwire-" + hex.EncodeToString(rnd[:])
}

// TestBridgedTurnDispatchesRealToolCall is the tool-wire end-to-end through
// the REAL Model Gateway (durable generations, real PG) plus the real artifact
// plane: step 0 must arrive at the runtime as a tool call decoded from the
// persisted generation envelope, the runtime executes write_artifact, step 1's
// provider request carries the tool result as a role=tool message, and the
// task ends COMPLETED carrying the sealed artifact bound to the task context.
func TestBridgedTurnDispatchesRealToolCall(t *testing.T) {
	pool := dbPool(t)
	client, bucket := minioFromEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	plane, err := executor.NewArtifactPlane(ctx, pool, executor.ArtifactPlaneConfig{
		S3Endpoint:  os.Getenv("AHP_S3_ENDPOINT"),
		S3AccessKey: os.Getenv("AHP_MINIO_ROOT_USER"), S3SecretKey: os.Getenv("AHP_MINIO_ROOT_PASSWORD"),
		Bucket: bucket,
	})
	if err != nil {
		t.Fatalf("artifact plane: %v", err)
	}
	t.Cleanup(func() {
		c, cn := context.WithTimeout(context.Background(), 20*time.Second)
		defer cn()
		for o := range client.ListObjects(c, bucket, minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
			_ = client.RemoveObject(c, bucket, o.Key, minio.RemoveObjectOptions{VersionID: o.VersionID})
		}
		_ = client.RemoveBucket(c, bucket)
	})

	inf := &scriptedToolInference{texts: []string{"", "готово"}}
	b, err := New(ctx, pool, Config{
		Inference: inf, MaxSteps: 3, TaskTokenLimit: 20000, StepReserve: 4000,
		Tools: plane.Tools, Artifacts: plane,
	})
	if err != nil {
		t.Fatalf("new bridge: %v", err)
	}
	scope := contracts.Scope{Tenant: "t-toolwire", Deployment: "d1", Subject: "svc"}
	var tid [6]byte
	_, _ = rand.Read(tid[:])
	taskID := "toolwire-" + hex.EncodeToString(tid[:])
	res, err := b.Execute(ctx, gateway.ExecInput{
		Scope: scope,
		Task:  gateway.TaskRow{ID: taskID, ContextID: "ctx-" + taskID},
		Input: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("запиши файл")),
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.State != a2a.TaskStateCompleted {
		t.Fatalf("state = %s (%s), want COMPLETED", res.State, messageText(res.Message))
	}
	if len(res.Artifacts) == 0 {
		t.Fatalf("the tool effect must ride the task: artifacts=0")
	}
	// The provider request at step 0 carried the tool definitions...
	inf.mu.Lock()
	defer inf.mu.Unlock()
	if len(inf.callsIn) < 2 {
		t.Fatalf("provider calls = %d, want the tool round-trip (>=2)", len(inf.callsIn))
	}
	if len(inf.callsIn[0].Tools) == 0 {
		t.Fatalf("step 0 request carried no tool definitions")
	}
	// ...and step 1 carried the tool result as a native role=tool message.
	var sawToolResult bool
	for _, m := range inf.callsIn[1].Messages {
		if m.Role == "tool" && m.ToolCallID == "call_artifact_1" {
			sawToolResult = true
		}
	}
	if !sawToolResult {
		t.Fatalf("step 1 must carry the native tool result: %+v", inf.callsIn[1].Messages)
	}
	// The seal is bound to the task context -- reachable by the cascade.
	listed, err := plane.Store.ListByContext(ctx, scope, "ctx-"+taskID)
	if err != nil || len(listed) < 1 {
		t.Fatalf("tool effect must be bound to the task context: %v err=%v", listed, err)
	}
	var sawToolEffect bool
	for _, a := range listed {
		if strings.Contains(a.Name, "tool-proof") {
			sawToolEffect = true
		}
	}
	if !sawToolEffect {
		t.Fatalf("the tool effect artifact must be reachable by the task context, got %v", listed)
	}
}

// TestArtifactPlaneSkipReasonIsMachineReadable pins the contract of the skip
// itself: the reason is greppable (tag + reason=), it names the exact missing
// variables, and it says out loud that a skip is not coverage. Without this pin
// the skip text is free to rot into "skipped" and the counter becomes
// unreadable again -- which is the defect being fixed, not a detail.
func TestArtifactPlaneSkipReasonIsMachineReadable(t *testing.T) {
	t.Setenv("AHP_S3_ENDPOINT", "")
	t.Setenv("AHP_MINIO_ROOT_USER", "")
	t.Setenv("AHP_MINIO_ROOT_PASSWORD", "")
	reason := artifactPlaneSkipReason()
	for _, want := range []string{
		"SKIP_ENV[tag=artifact_plane]",
		"reason=object_store_env_absent",
		"missing=AHP_S3_ENDPOINT,AHP_MINIO_ROOT_USER,AHP_MINIO_ROOT_PASSWORD",
		"not passed",
	} {
		if !strings.Contains(reason, want) {
			t.Fatalf("skip reason must carry %q, got %q", want, reason)
		}
	}

	// A partially configured plane must name only what is actually missing, so an
	// operator fixing the environment is not sent after variables that are set.
	t.Setenv("AHP_S3_ENDPOINT", "http://127.0.0.1:29000")
	t.Setenv("AHP_MINIO_ROOT_USER", "lane-access-key")
	reason = artifactPlaneSkipReason()
	if reason != "" {
		if !strings.Contains(reason, "missing=AHP_MINIO_ROOT_PASSWORD") {
			t.Fatalf("partial configuration must report only the missing variable, got %q", reason)
		}
		if strings.Contains(reason, "missing=AHP_S3_ENDPOINT,") ||
			strings.Contains(reason, "missing=AHP_MINIO_ROOT_USER,") {
			t.Fatalf("a configured variable must not be reported as missing, got %q", reason)
		}
	}

	// And when everything the gate asks for is supplied it must stay silent: an
	// over-eager skip is as destructive to the counter as a spurious failure.
	// The identity of the store is part of "everything" -- credentials alone are
	// not authorisation, so this case designates the store as well.
	t.Setenv("AHP_MINIO_ROOT_PASSWORD", "lane-secret-key")
	t.Setenv("AHP_TEST_OBJECT_STORE", "127.0.0.1:29000")
	if reason := artifactPlaneSkipReason(); reason != "" {
		t.Fatalf("a fully configured AND lane-designated artifact plane must not skip, got %q", reason)
	}
}

// TestArtifactPlaneRefusesAnObjectStoreTheLaneWasNotGiven pins the identity gate.
// The three cases are the only answers a lane should ever get from this gate:
// designated match (run), no designation (skip, inherited env is not permission),
// and mismatch (skip, naming both identities). A silent pass on mismatch is the
// regression that would let a leg write into a shared bucket again.
func TestArtifactPlaneRefusesAnObjectStoreTheLaneWasNotGiven(t *testing.T) {
	t.Setenv("AHP_S3_ENDPOINT", "http://127.0.0.1:29000")
	t.Setenv("AHP_MINIO_ROOT_USER", "lane-access-key")
	t.Setenv("AHP_MINIO_ROOT_PASSWORD", "lane-secret-key")

	t.Setenv("AHP_TEST_OBJECT_STORE", "")
	reason := artifactPlaneSkipReason()
	if !strings.Contains(reason, "SKIP_ENV[tag=artifact_plane]") ||
		!strings.Contains(reason, "reason=object_store_not_lane_designated") ||
		!strings.Contains(reason, "endpoint=127.0.0.1:29000") {
		t.Fatalf("an inherited endpoint without a lane designation must skip by name, got %q", reason)
	}
	// Credentials must never ride along in a skip reason.
	if strings.Contains(reason, "lane-access-key") || strings.Contains(reason, "lane-secret-key") {
		t.Fatalf("the skip reason carries credentials: %q", reason)
	}

	t.Setenv("AHP_TEST_OBJECT_STORE", "127.0.0.1:29000")
	if reason := artifactPlaneSkipReason(); reason != "" {
		t.Fatalf("the designated store must be accepted (form without scheme, port equal): %q", reason)
	}

	t.Setenv("AHP_TEST_OBJECT_STORE", "127.0.0.1:39000")
	reason = artifactPlaneSkipReason()
	if !strings.Contains(reason, "reason=object_store_identity_mismatch") ||
		!strings.Contains(reason, "endpoint=127.0.0.1:29000") ||
		!strings.Contains(reason, "designated=127.0.0.1:39000") {
		t.Fatalf("a mismatching designation must refuse and name both identities, got %q", reason)
	}

	// A designation carrying credentials is refused, not honoured.
	t.Setenv("AHP_TEST_OBJECT_STORE", "http://user:pass@127.0.0.1:29000")
	if reason := artifactPlaneSkipReason(); !strings.Contains(reason, "reason=object_store_designation_unparsable") {
		t.Fatalf("a credential-bearing designation must be refused, got %q", reason)
	}
}
