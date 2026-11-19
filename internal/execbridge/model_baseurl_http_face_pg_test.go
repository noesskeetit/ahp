package execbridge

// D-1, closed where a caller can actually read it.
//
// A refused base_url is refused at admission, and the answer to the caller is
// transport.WriteError(err) -- whose message is err.Error(). Until this file the
// two oldest branches of that error quoted the configured string, so a
// configured value like user:PASS@host/v1 came back in the HTTP body: anyone
// able to send one generation at a broken copy could read the provider's
// credential out of the platform. Status and reason classes are unchanged here
// (409 stays 409); what is asserted is that the credential never appears, at the
// face, for the branch that used to echo it and for the branch that already
// refused structurally.
//
// The bad endpoints name loopback only: the point is the refusal text, and no leg
// of this file may put a request on the public internet.
//
// Honest boundary: this proves the face does not echo configuration. It is not a
// claim that the misconfigured copy works, nor that a real second upstream
// exists -- the copy is reached through the production route and the production
// client constructor, with the secret store substituted, as elsewhere in this
// package.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/internal/model/secretstore"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

const (
	faceCredentialMarker = "MARKER-NEVER-ECHO-8f21"
	faceReasonDuplicated = "model_base_url_duplicated_completion_path"
	faceReasonNotAddress = "model_base_url_not_absolute"
)

// faceTable declares the good copy and a copy the route sends restricted work to.
const faceTable = `{
  "version": 1,
  "providers": [
    { "name": "copy-a", "class": "standard",
      "audience": { "kind": "external", "allow_external": false },
      "secret_ref": "ahp-hive/test/hc-copy-a", "models": ["model-under-test"] },
    { "name": "copy-bad", "class": "restricted",
      "audience": { "kind": "tenant" },
      "secret_ref": "ahp-hive/test/hc-copy-bad", "models": ["model-under-test"] }
  ],
  "routes": [
    { "name": "restricted-to-bad", "when": { "classes": ["restricted"] }, "then": { "provider": "copy-bad" } },
    { "name": "default", "when": { "tenants": ["*"] }, "then": { "provider": "copy-a" } }
  ]
}`

func submitAtTheFace(t *testing.T, b *Bridge, scope contracts.Scope, task, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/v1/model/SubmitModel", bytes.NewBufferString(body))
	claims := auth.Claims{Kind: auth.KindTask, Tenant: scope.Tenant, Deployment: scope.Deployment,
		Subject: scope.Subject, Task: task, Context: "ctx-face"}
	r = r.WithContext(transport.WithPrincipal(r.Context(), transport.Principal{Scope: scope, Claims: claims}))
	w := httptest.NewRecorder()
	b.serveModel(w, r)
	return w
}

func faceBridge(t *testing.T, pool *pgxpool.Pool, badBaseURL string) *Bridge {
	t.Helper()
	return faceBridgeWithResolver(t, pool, func(_ context.Context, kvPath string) (secretstore.QwenSecret, error) {
		if strings.HasSuffix(kvPath, "hc-copy-a") {
			sink := newQwenSink(t, "copy-a", "served by the good copy")
			return qwenSecretFor(sink), nil
		}
		return secretstore.QwenSecret{APIKey: "sk-face-do-not-echo", BaseURL: badBaseURL, Model: "model-under-test"}, nil
	})
}

// faceBridgeWithResolver is the same deployment with the secret store under the
// test's control, so a leg can make the copy's secret unreadable instead of only
// misconfigured. Those are different answers and the face must not confuse them.
func faceBridgeWithResolver(t *testing.T, pool *pgxpool.Pool, resolver func(context.Context, string) (secretstore.QwenSecret, error)) *Bridge {
	t.Helper()
	ctx := context.Background()
	sink := newQwenSink(t, "copy-a", "served by the good copy")
	infA, err := model.NewQwenClientWithHTTP(qwenSecretFor(sink), &http.Client{})
	if err != nil {
		t.Fatalf("good copy client: %v", err)
	}
	quotas := model.NewQuotaStore(pool)
	if err := quotas.Migrate(ctx); err != nil {
		t.Fatalf("quota migrate: %v", err)
	}
	b, err := New(ctx, pool, Config{
		Inference: infA, MaxSteps: 2, TaskTokenLimit: 20000, StepReserve: 4000,
		QuotaProvider: "copy-a", QuotaWindowSeconds: 60,
		RoutingFile:    writeProviderTable(t, faceTable),
		SecretResolver: resolver, InferenceFactory: sinkFactory(),
	})
	if err != nil {
		t.Fatalf("new bridge: %v", err)
	}
	b.executionClass = "restricted"
	return b
}

// One misconfigured copy, one HTTP answer: 409 with its own reason, no secret.
func TestModelHTTPFaceRefusesMisconfiguredCopyWithoutEchoingItsCredential(t *testing.T) {
	pool := dbPool(t)
	ctx := context.Background()
	for _, tc := range []struct {
		name       string
		badBaseURL string
		wantReason string
	}{
		{"duplicated completion path", "http://127.0.0.1:1/v1/chat/completions", faceReasonDuplicated},
		{"unschemed value carrying userinfo", "user:" + faceCredentialMarker + "@127.0.0.1:1/v1", faceReasonNotAddress},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := faceBridge(t, pool, tc.badBaseURL)
			scope := uniqueScope()
			for _, provider := range []string{"copy-a", "copy-bad"} {
				if err := model.NewQuotaStore(pool).Set(ctx, model.ProviderQuota{
					Provider: provider, Tenant: scope.Tenant, RPM: 10, TPM: 100000, WindowSeconds: 60}); err != nil {
					t.Fatalf("quota set: %v", err)
				}
			}
			w := submitAtTheFace(t, b, scope, "task-face", `{"key":"face-1","messages":[{"role":"user","content":"hi"}]}`)
			body := w.Body.String()
			if w.Code != http.StatusConflict {
				t.Fatalf("a refused copy must answer 409, got %d: %s", w.Code, body)
			}
			// transport.domainReason embeds *contracts.Error, so code and message land next
			// to reason: no nested "error" object is produced, and a leg decoding one reads
			// empty strings without noticing. Decoded flat -- and the message is the carrier
			// of the branch text, so it is assertable.
			var shape struct {
				Code    string `json:"code"`
				Message string `json:"message"`
				Reason  string `json:"reason"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &shape); err != nil {
				t.Fatalf("answer is not the contract shape: %v (%s)", err, body)
			}
			if shape.Reason != tc.wantReason {
				t.Fatalf("reason=%q, want %q (body=%s)", shape.Reason, tc.wantReason, body)
			}
			if !strings.Contains(shape.Message, tc.wantReason) {
				t.Fatalf("the message must carry the branch the operator has to fix (%q), got %q", tc.wantReason, shape.Message)
			}
			if strings.Contains(body, faceCredentialMarker) || strings.Contains(body, "user:") {
				t.Fatalf("the answer echoes the configured credential: %s", body)
			}
		})
	}
}

// C2 (review 7c5a8758): the other half of the admission answer.
//
// The merged block pinned that a REFUSED CONFIGURATION keeps its own reason and
// answers 409. Its mirror was never pinned at the face: a copy whose secret_ref
// cannot be read is a dependency failure, so it must answer 503/UNAVAILABLE --
// not 409 (nothing the caller changes will heal it, and a client that reads 409
// as "fix your request" gives up on a store outage) and not a bare 500.
//
// The assertion records what the product actually says rather than what would be
// neat: the contract-level reason field is the coarse provider_copy_unreachable
// class, and the precise branch (provider_secret_unresolved, with the secret_ref
// named) travels in the message. Naming the reference is intended; naming its
// value is the leak this file exists to prevent, so the store's credential is
// asserted absent as well.
func TestModelHTTPFaceRefusesUnreadableSecretAsUnavailable(t *testing.T) {
	pool := dbPool(t)
	ctx := context.Background()
	const storeCredential = "sk-face-store-secret-do-not-echo"
	b := faceBridgeWithResolver(t, pool, func(_ context.Context, kvPath string) (secretstore.QwenSecret, error) {
		if strings.HasSuffix(kvPath, "hc-copy-a") {
			sink := newQwenSink(t, "copy-a", "served by the good copy")
			return qwenSecretFor(sink), nil
		}
		return secretstore.QwenSecret{}, fmt.Errorf("vault: reading %q: permission denied (token %s)", kvPath, storeCredential)
	})
	scope := uniqueScope()
	for _, provider := range []string{"copy-a", "copy-bad"} {
		if err := model.NewQuotaStore(pool).Set(ctx, model.ProviderQuota{
			Provider: provider, Tenant: scope.Tenant, RPM: 10, TPM: 100000, WindowSeconds: 60}); err != nil {
			t.Fatalf("quota set: %v", err)
		}
	}
	w := submitAtTheFace(t, b, scope, "task-face-unreadable", `{"key":"face-unreadable-1","messages":[{"role":"user","content":"hi"}]}`)
	body := w.Body.String()
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("an unreadable secret is a dependency failure and must answer 503, got %d: %s", w.Code, body)
	}
	// transport.domainReason embeds *contracts.Error, so code and message land next
	// to reason: no nested "error" object is produced, and a leg decoding one reads
	// empty strings without noticing. Decoded flat -- and the message is the carrier
	// of the branch text, so it is assertable.
	var shape struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Reason  string `json:"reason"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &shape); err != nil {
		t.Fatalf("answer is not the contract shape: %v (%s)", err, body)
	}
	if shape.Code != "UNAVAILABLE" {
		t.Fatalf("code=%q, want UNAVAILABLE (body=%s)", shape.Code, body)
	}
	if shape.Reason != ReasonProviderCopyUnreachable {
		t.Fatalf("reason=%q, want the coarse dependency class %q (body=%s)", shape.Reason, ReasonProviderCopyUnreachable, body)
	}
	// The branch an operator has to fix must survive to the caller, and the
	// reference must be named: "some secret is broken" is not actionable.
	if !strings.Contains(shape.Message, ReasonProviderSecretUnresolved) {
		t.Fatalf("the message must carry %q, got %q", ReasonProviderSecretUnresolved, shape.Message)
	}
	if !strings.Contains(shape.Message, "ahp-hive/test/hc-copy-bad") {
		t.Fatalf("the message must name the unreadable secret_ref, got %q", shape.Message)
	}
	// And nothing about the value: neither the store's error text nor a credential
	// inside it may be forwarded.
	if strings.Contains(body, storeCredential) {
		t.Fatalf("the answer forwards the store's error text, which carried a credential: %s", body)
	}
}
