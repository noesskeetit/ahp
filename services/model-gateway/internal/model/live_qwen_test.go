//go:build liveqwen

// Live Qwen legs are compiled ONLY under the `liveqwen` build tag, so an ordinary
// `go test ./internal/model` neither contacts a provider nor reports a skip: the
// ambient suite is deterministic and hermetic. Run them explicitly with
//
//	AHP_LIVE_QWEN=1 go test -count=1 -tags liveqwen -run TestLive ./internal/model
//
// (the credential still comes only from the platform secret store).

package model

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/model/secretstore"
)

// Live Qwen smoke tests. They are opt-in: the operator must export
// AHP_LIVE_QWEN=1 to enable them, so the default (CI / load-constrained) suite
// never contacts an external provider nor requires the scoped secret.
//
// Credentials come exclusively from the platform secret store (Vault) via
// secretstore.QwenFromEnv. These tests NEVER read the operator's source env for
// a key and NEVER fall back to any non-Qwen provider. No credential value is
// ever logged: only the key length, the upstream host and the model id appear.

// requireLiveQwen returns the scoped secret, skipping the test when live mode
// is disabled or the secret store is not configured for this process.
func requireLiveQwen(t *testing.T) secretstore.QwenSecret {
	t.Helper()
	if os.Getenv("AHP_LIVE_QWEN") != "1" {
		t.Skip("AHP_LIVE_QWEN!=1: live Qwen smoke disabled")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	sec, err := secretstore.QwenFromEnv(ctx)
	if err != nil {
		if errors.Is(err, secretstore.ErrNotConfigured) {
			t.Skipf("live Qwen enabled but secret store not configured: %v", err)
		}
		t.Fatalf("secretstore.QwenFromEnv: %v", err)
	}
	if err := sec.Validate(); err != nil {
		t.Fatalf("qwen secret invalid: %v", err)
	}
	return sec
}

// redactHost returns host[:port] of the provider base URL (config, not secret).
func redactHost(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse base_url: %v", err)
	}
	return u.Host
}

func TestLiveQwenSecretResolvesFromVault(t *testing.T) {
	sec := requireLiveQwen(t)
	if len(sec.APIKey) == 0 {
		t.Fatal("api_key resolved empty")
	}
	// Assert no fallback: the resolved endpoint is the platform Qwen host, and
	// the key never leaks through the type's String()/default formatting.
	t.Logf("resolved scoped secret: host=%s model=%s api_key_len=%d", redactHost(t, sec.BaseURL), sec.Model, len(sec.APIKey))
	if got := sec.String(); strings.Contains(got, sec.APIKey) {
		t.Fatal("QwenSecret.String leaked the api key")
	}
}

// TestLiveQwenGatewayRoundTrip drives one real turn through the production
// Qwen client, behind the netpolicy egress guard.
//
// What makes this leg deterministic rather than a coin flip: a small MaxTokens on a
// reasoning model can legally return finish="length" with NO content -- measured on
// this very leg, 1 run in 5 -- and that is a provider REFUSAL, not a transport
// failure and not an answer. The leg therefore (a) asks with room enough to answer,
// (b) treats an empty completion as legal only when the provider said why
// (length/content_filter), (c) retries that refusal a BOUNDED number of times, the
// way a caller may, and (d) still requires a usable completion containing the
// marker -- a stricter content assert than "non-empty" ever was. An empty turn with
// no stated reason is a defect and fails on the spot.
func TestLiveQwenGatewayRoundTrip(t *testing.T) {
	sec := requireLiveQwen(t)

	client, err := NewQwenClient(context.Background(), sec)
	if err != nil {
		t.Fatalf("NewQwenClient: %v", err)
	}
	if client.Model() != sec.Model {
		t.Fatalf("client model %q != secret model %q", client.Model(), sec.Model)
	}

	const attempts = 3
	const marker = "AHP-EXEC-LIVE-OK"
	refusals := make([]string, 0, attempts)

	for attempt := 1; attempt <= attempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		// Room enough for a reasoning model to produce content: asking for 128
		// tokens is how the measured empty-at-limit completion was manufactured.
		stream, err := client.Stream(ctx, ChatRequest{
			Messages:  []Message{{Role: "user", Content: "Reply with exactly: " + marker}},
			MaxTokens: 1024,
		})
		if err != nil {
			cancel()
			if IsTransportError(err) {
				t.Skipf("live Qwen transport ambiguous (network egress blocked in this sandbox): %v", err)
			}
			t.Fatalf("live Stream: %v", err)
		}

		var b strings.Builder
		finish := ""
		streamErr := error(nil)
		for {
			d, nerr := stream.Next(ctx)
			if errors.Is(nerr, ErrStreamDone) {
				break
			}
			if nerr != nil {
				streamErr = nerr
				break
			}
			b.WriteString(d.Text)
			if d.FinishReason != "" {
				finish = d.FinishReason
			}
			if b.Len() > 8000 {
				streamErr = errors.New("live stream produced unexpectedly large output")
				break
			}
		}
		usage := stream.Usage()
		_ = stream.Close()
		completion := b.String()
		cancel()

		if streamErr != nil {
			if IsTransportError(streamErr) {
				t.Skipf("live Qwen transport ambiguous mid-stream (network egress blocked in this sandbox): %v", streamErr)
			}
			t.Fatalf("live stream Next: %v", streamErr)
		}
		if usage.TotalTokens <= 0 {
			t.Fatalf("live Qwen reported no usage: %+v", usage)
		}
		if strings.TrimSpace(completion) == "" {
			// Legal only as a named refusal; that is the same verdict the gateway
			// itself now records for this outcome (EmptyCompletionCause).
			if finish != "length" && finish != "content_filter" {
				t.Fatalf("live Qwen returned an empty completion with no refusal reason (finish=%q usage=%+v): an unlabelled empty turn is a defect, not a refusal", finish, usage)
			}
			refusals = append(refusals, fmt.Sprintf("attempt %d: finish=%s usage_total=%d", attempt, finish, usage.TotalTokens))
			t.Logf("provider refused attempt %d/%d (finish=%s, usage_total=%d) -- retrying under the bound", attempt, attempts, finish, usage.TotalTokens)
			continue
		}
		if !strings.Contains(completion, marker) {
			t.Fatalf("live completion does not carry the requested marker (len=%d finish=%q usage_total=%d): the model answered something other than what was asked",
				len(completion), finish, usage.TotalTokens)
		}
		t.Logf("live completion carries the marker: len=%d finish=%q prompt=%d completion=%d total=%d (model=%s, attempts_used=%d)",
			len(completion), finish, usage.PromptTokens, usage.CompletionTokens, usage.TotalTokens, sec.Model, attempt)
		return
	}
	t.Fatalf("live Qwen refused every attempt under the bound of %d: %v -- the gateway records this outcome as %s and a caller may retry only this many times",
		attempts, refusals, EmptyCompletionCause)
}

func TestLiveQwenGenerationNoOverdraft(t *testing.T) {
	if os.Getenv("AHP_LIVE_QWEN") != "1" {
		t.Skip("AHP_LIVE_QWEN!=1: live Qwen smoke disabled")
	}
	if os.Getenv("AHP_TEST_DATABASE_URL") == "" {
		t.Skip("AHP_TEST_DATABASE_URL not set")
	}
	sec := requireLiveQwen(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	client, err := NewQwenClient(ctx, sec)
	if err != nil {
		t.Fatalf("NewQwenClient: %v", err)
	}

	env := newModelEnv(t)
	// Generous limit, tight reservation: the reservation is the admission bound
	// and settle clamps the actual charge to it, so the budget can never overdraft.
	const limit = 100_000
	const reserve = 200
	root, err := env.budget.Root(ctx, env.scope, "task-live", "rb-"+env.u, limit)
	if err != nil {
		t.Fatalf("budget.Root: %v", err)
	}
	svc := env.service(client)

	// The subject of this leg is the INVARIANT, and the invariant has to hold on
	// every terminal outcome -- including the provider refusal this model really
	// returns at a token limit (DEF-A7-1). So: bounded attempts, and after each one
	// the tree is read and the bound re-asserted. A refusal is accepted as the
	// honest verdict it is, provided it charged the tokens it consumed and released
	// its reservation.
	const attempts = 3
	refused := make([]string, 0, attempts)
	for attempt := 1; attempt <= attempts; attempt++ {
		args := SubmitArgs{
			Scope: env.scope,
			Task:  "task-live",
			// Each attempt is its own operation: a distinct key, the same task and
			// the same tree, which is exactly what a bounded caller retry looks like.
			Key:           fmt.Sprintf("gen-%s-%d", env.u, attempt),
			Model:         sec.Model,
			Request:       chatReq("Reply with exactly: AHP-EXEC-LIVE-OK"),
			BudgetID:      root.ID,
			ReserveKey:    fmt.Sprintf("rk-%s-%d", env.u, attempt),
			ReserveAmount: reserve,
		}
		g, err := svc.Submit(ctx, args)
		if err != nil {
			t.Fatalf("submit attempt %d: %v", attempt, err)
		}
		done, err := svc.Execute(ctx, env.scope, g.ID)
		if err != nil {
			if IsTransportError(err) {
				t.Skipf("live Qwen transport ambiguous (network egress blocked in this sandbox): %v", err)
			}
			t.Fatalf("execute attempt %d: %v", attempt, err)
		}

		b, err := env.budget.Get(ctx, env.scope, root.ID)
		if err != nil {
			t.Fatalf("budget.Get after attempt %d: %v", attempt, err)
		}
		// The bound, re-asserted after EVERY attempt rather than only at the end.
		if b.Spent > reserve*int64(attempt) {
			t.Fatalf("OVERDRAFT after attempt %d: spent=%d > %d reserved across %d attempt(s)", attempt, b.Spent, reserve, attempt)
		}
		if b.Spent > limit {
			t.Fatalf("OVERDRAFT after attempt %d: spent=%d > limit=%d", attempt, b.Spent, limit)
		}
		if b.Reserved != 0 {
			t.Fatalf("attempt %d left a dangling reservation: reserved=%d", attempt, b.Reserved)
		}
		if done.Usage.TotalTokens <= 0 {
			t.Fatalf("attempt %d recorded no usage: %+v", attempt, done.Usage)
		}
		switch {
		case done.Status == GenCompleted:
			t.Logf("live generation settled: status=%s usage_total=%d spent=%d reserved=%d limit=%d attempts_used=%d",
				done.Status, done.Usage.TotalTokens, b.Spent, b.Reserved, limit, attempt)
			return
		case IsEmptyCompletionRefusal(done):
			// A refusal at the provider's limit: honest verdict, charged spend, no
			// dangling hold. Retry within the bound; if every attempt refuses the
			// invariant has still been proven, and that is logged as such.
			refused = append(refused, fmt.Sprintf("attempt %d: usage_total=%d spent_so_far=%d", attempt, done.Usage.TotalTokens, b.Spent))
			t.Logf("provider refusal on attempt %d/%d (%s) -- spend stayed inside the bound, retrying under the bound",
				attempt, attempts, done.Error)
			continue
		default:
			t.Fatalf("attempt %d ended %s (%q): only completion or the named empty-completion refusal are expected here",
				attempt, done.Status, done.Error)
		}
	}
	final, err := env.budget.Get(ctx, env.scope, root.ID)
	if err != nil {
		t.Fatalf("budget.Get after refusals: %v", err)
	}
	if final.Spent <= 0 {
		t.Fatalf("every attempt was refused and nothing was charged: spent=%d -- refused tokens must still be accounted (%v)", final.Spent, refused)
	}
	t.Logf("provider refused every attempt within the bound of %d [%s]; the no-overdraft invariant held throughout: spent=%d reserved=%d limit=%d",
		attempts, strings.Join(refused, "; "), final.Spent, final.Reserved, limit)
}
