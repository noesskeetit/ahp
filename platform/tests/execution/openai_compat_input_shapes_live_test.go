package execution_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/model/secretstore"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// liveLLMEnv is the discipline again: a missing REAL provider fails the leg
// with the variables named; AHP_ALLOW_SKIP_REAL_DB=1 downgrades to a printed
// SKIP_EXPLICIT that acceptance does not count.
func liveLLMEnv(t *testing.T) {
	t.Helper()
	missing := []string{}
	for _, k := range []string{"AHP_LLM_BASE_URL", "AHP_LLM_API_KEY", "AHP_LLM_MODEL"} {
		if os.Getenv(k) == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) == 0 {
		return
	}
	if os.Getenv("AHP_ALLOW_SKIP_REAL_DB") == "1" {
		t.Log("SKIP_EXPLICIT live openai-compat provider not configured -- opt-out skip, NOT counted as acceptance")
		t.Skip("explicit opt-out via AHP_ALLOW_SKIP_REAL_DB=1")
	}
	t.Fatalf("missing live provider env: %v -- this leg measures the REAL openai-compat endpoint "+
		"(the object-input defect only existed there); refusing to fake a pass.", missing)
}

type inputShape struct {
	name  string
	input json.RawMessage
}

// TestLiveOpenAICompatBothInputShapes is the required BOTH-shapes leg against
// the real openai-compat provider: the string form has always worked; the
// OBJECT form (what the UI sends) failed the whole run at step 0 with the
// provider's 400 content-part validation on stand b661c60. Both must now run
// to a model answer with zero steps errored.
func TestLiveOpenAICompatBothInputShapes(t *testing.T) {
	var model *core.OpenAICompatClient
	var err error
	if os.Getenv(secretstore.EnvVaultAddr) != "" || os.Getenv(secretstore.EnvVaultAddrAlt) != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		var sec secretstore.QwenSecret
		sec, err = secretstore.QwenFromEnv(ctx)
		if err != nil {
			t.Fatalf("scoped live provider: %v", err)
		}
		model, err = core.NewOpenAICompat(sec.BaseURL, sec.APIKey, sec.Model)
	} else {
		liveLLMEnv(t)
		model, err = core.NewOpenAICompatFromEnv()
	}
	if err != nil {
		t.Fatalf("openai-compat from env: %v", err)
	}
	rt := core.NewRuntime(core.RuntimeConfig{Model: model, MaxTokens: 20000})
	scope := contracts.Scope{Tenant: "t-input-shapes", Deployment: "d1", Subject: "exec-leg"}

	for _, shape := range []inputShape{
		{"string", json.RawMessage(`"Ответь ровно одно слово: принят"`)},
		{"object", json.RawMessage(`{"message":"Ответь ровно одно слово: принят"}`)},
	} {
		t.Run(shape.name, func(t *testing.T) {
			// The stand's provider occasionally drops the TLS handshake under
			// node load (measured separately as the transport-retry defect).
			// This leg measures INPUT-SHAPE deliverability, so a bounded repeat
			// of the transport attempt is honest; a contract 400 is NEVER
			// retried -- only "the answer never arrived" is.
			var lastErr error
			for attempt := 1; attempt <= 3; attempt++ {
				lastErr = runOneShape(t, rt, scope, shape, attempt)
				if lastErr == nil || !strings.Contains(lastErr.Error(), "TLS handshake") {
					break
				}
				t.Logf("attempt %d transport flake: %v", attempt, lastErr)
				time.Sleep(3 * time.Second)
			}
			if lastErr != nil {
				t.Fatalf("%s input: %v", shape.name, lastErr)
			}
		})
	}
}

func runOneShape(t *testing.T, rt *core.Runtime, scope contracts.Scope, shape inputShape, attempt int) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	results := make(chan space.StepResult, 32)
	req := &space.CoreRequest{
		Scope: scope, Input: shape.input, MaxSteps: 2,
		TurnID: fmt.Sprintf("leg-%s-%d", shape.name, attempt), ThreadID: "thr-" + shape.name,
		RunID: fmt.Sprintf("run-%s-%d", shape.name, attempt), Worker: "exec-leg", Epoch: int64(attempt),
	}
	var runErr error
	go func() { runErr = rt.Run(ctx, req, results); close(results) }()
	var (
		sawModelText bool
		sawError     bool
		errText      string
	)
	for sr := range results {
		switch sr.StepType {
		case space.StepModel:
			var txt string
			if json.Unmarshal(sr.Content, &txt) == nil && strings.TrimSpace(txt) != "" {
				sawModelText = true
			}
		case space.StepError:
			sawError = true
			errText = string(sr.Content)
		}
	}
	if runErr != nil {
		return fmt.Errorf("runtime run: %w (last error step: %s)", runErr, errText)
	}
	if sawError {
		return fmt.Errorf("error step: %s", errText)
	}
	if !sawModelText {
		return fmt.Errorf("no model text step -- the turn did not reach the provider answer")
	}
	return nil
}
