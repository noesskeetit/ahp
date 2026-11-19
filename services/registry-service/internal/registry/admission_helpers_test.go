package registry_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/evals"
	"github.com/ahp-platform/ahp/internal/registry"
)

func dsn(t *testing.T) string {
	t.Helper()
	url := os.Getenv("AHP_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("AHP_TEST_DATABASE_URL is not set: supply-chain acceptance needs the real PostgreSQL instance")
	}
	return url
}

func srvURL(s *httptest.Server) string { return s.URL }

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// imageRefPair returns two distinct images (with their SBOMs) that can be pinned
// to two versions of one agent for promotion/routing tests.
func (e *supplyEnv) imageRefPair(t *testing.T) (imageEnv, imageEnv) {
	t.Helper()
	a := e.imageSpec(t, "paira")
	b := e.imageSpec(t, "pairb")
	return a, b
}

func (e *supplyEnv) imageSpec(t *testing.T, tag string) imageEnv {
	t.Helper()
	imageRef, digestHex, _ := makeImage(t, tag+"-"+randSuffix(t))
	sbom, sbomDigest := makeSBOM(t, imageRef)
	return imageEnv{imageRef: imageRef, digestHex: digestHex, sbom: sbom, sbomDigest: sbomDigest}
}

// runtimeServer stands up the loopback runtime the dispatcher probes: /readyz
// reports its immutable identity, and any dispatch increments the counter.
func (e *supplyEnv) runtimeServer(t *testing.T, agent, digest, image string, hits *atomic.Int64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/readyz" {
			_ = json.NewEncoder(w).Encode(map[string]any{"ready": true, "agent_id": agent, "version_digest": digest, "image": image})
			return
		}
		hits.Add(1)
		var request struct {
			ID json.RawMessage `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": map[string]any{"id": "task-" + digest[len(digest)-12:]}})
	}))
}

// bindingFor builds a runtime binding carrying the fully verified attestation,
// pointed at a loopback endpoint (the platform's local-container allowance).
func (e *supplyEnv) bindingFor(t *testing.T, im imageEnv, auth *testAuthority) registry.RuntimeBinding {
	t.Helper()
	leaf, key, _ := auth.mintPublisher(t, testPublisher, time.Hour)
	att := sign(t, leaf, key, attestationSpec{
		OCIDigestHex: im.digestHex, ImageRef: im.imageRef, VersionDigest: im.version.Digest,
		SBOMDigest: im.sbomDigest, EvalVerdictDgst: im.version.Digest, SecurityVerdict: im.secID,
		ExpiresAt: time.Now().Add(time.Hour),
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(srv.Close)
	return registry.RuntimeBinding{
		Tenant: e.tenant, Deployment: "rt-" + im.version.ID, Environment: "dev", AgentID: im.agent,
		Digest: im.version.Digest, Image: im.imageRef, Attestation: string(mustJSON(t, att)), SBOM: im.sbom,
		Endpoint: srv.URL + "/a2a", ReadyURL: srv.URL + "/readyz",
	}
}

// evalsRecordFail builds a failing evaluation verdict input for a digest, used to
// make a previously-passing verdict go stale.
func evalsRecordFail(digest string) evals.RecordInput {
	return evals.RecordInput{
		AgentDigest: digest, TaskSetID: harnessTaskSetID, Setup: map[string]any{"v": "late-fail"},
		Outcome: evals.OutcomeFail, Tasks: []evals.TaskResult{{TaskID: "reference-task", Passed: false, Cost: 0.01}},
		PolicyID: harnessPolicyID, TargetKind: evals.TargetFixture,
	}
}

var _ = context.Background
