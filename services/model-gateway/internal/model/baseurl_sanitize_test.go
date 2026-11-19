package model

// Three defects found in the base_url gate by independent review (Russell) and
// confirmed here before any fix:
//
// D-1 is a credential leak the gate itself introduced. The two branches that
// predate the structured refusals printed the configured string
// ("base_url %q is not an absolute URL") or wrapped net/url's error, whose text
// quotes the URL. A configured value with no "://" but with userinfo therefore
// reached the caller verbatim, and transport.WriteError puts err.Error() into
// the message of the answer: whoever can send a generation at a broken copy
// reads the provider's credential out of the platform's own response. The same
// text is also kept as the durable cause (service.go keeps a base_url refusal's
// own text), so the secret would be journalled forever. A refusal that quotes
// the configuration is not "honest about configuration" -- it is a second leak
// path in the file whose purpose is to close one.
//
// D-2: the completion-suffix check compared bytes, so a configured
// /V1/Chat/Completions was accepted and dialled
// /V1/Chat/Completions/chat/completions -- exactly the silent 404 this gate was
// written to remove.
//
// D-3: NewQwenClientWithHTTP is exported and kept its own weak url.Parse check,
// so any future caller could build a client from a configuration every other
// path refuses.

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/model/secretstore"
)

// credentialMarker is distinctive on purpose: it must appear in no refusal text,
// in no HTTP message and in no durable cause.
const credentialMarker = "MARKER-NEVER-ECHO-8f21"

// The two refusal classes below do not exist yet: every unschemed or unparseable
// base_url today falls into the generic provider_client_unavailable bucket with
// the raw configuration quoted. They are written as wire literals rather than Go
// symbols, because the literal is the contract an operator scripts on.
const (
	reasonNotAbsoluteLiteral = "model_base_url_not_absolute"
	reasonUnparseableLiteral = "model_base_url_unparseable"
)

// A refusal must never quote what it refused. The configured value travels from
// the secret store, so any position inside it -- userinfo, path, port text,
// query -- may be the credential. Each row below puts the marker somewhere
// different in a value the gate refuses, and asks two things: the branch is
// named by its own reason class, and the marker is nowhere in the text.
func TestBaseURLRefusalsNeverQuoteTheConfiguredValue(t *testing.T) {
	cases := []struct {
		name       string
		baseURL    string
		wantReason string
	}{
		{"unschemed value carrying userinfo", "user:" + credentialMarker + "@api.example.com/v1", reasonNotAbsoluteLiteral},
		{"net/url rejects the authority", "http://exa mple.com/" + credentialMarker, reasonUnparseableLiteral},
		{"port text is not a port", "http://api.example.com:" + credentialMarker + "/v1", "model_base_url_port_invalid"},
		{"userinfo written percent-encoded", "http://user%40x:" + credentialMarker + "!host/v1", "model_base_url_port_invalid"},
		{"path already names the endpoint", "https://api.example.com/v1/" + credentialMarker + "/chat/completions", "model_base_url_duplicated_completion_path"},
		{"path carries a dot segment", "https://api.example.com/" + credentialMarker + "/../v1", "model_base_url_path_not_canonical"},
		{"query carries a token", "https://api.example.com/v1?token=" + credentialMarker, "model_base_url_carries_query_or_fragment"},
		{"userinfo over a scheme", "https://user:" + credentialMarker + "@api.example.com/v1", "model_base_url_carries_credentials"},
		{"no host at all", "http:///" + credentialMarker, reasonUnparseableLiteral},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := buildRefused(t, tc.baseURL)
			if got := BaseURLConfigReason(err); got != tc.wantReason {
				t.Fatalf("reason=%q, want %q (err=%v)", got, tc.wantReason, err)
			}
			if strings.Contains(err.Error(), credentialMarker) {
				t.Fatalf("the refusal quotes the configured value: %v", err)
			}
			if strings.Contains(err.Error(), "user:") {
				t.Fatalf("the refusal quotes userinfo: %v", err)
			}
		})
	}
}

// The leak is not only in the answer: a refused base_url keeps its own text as
// the terminal cause. The secret must not reach the ledger either.
func TestBaseURLRefusalCauseCarriesNoCredentialToTheDurableRecord(t *testing.T) {
	ctx := context.Background()
	env := newModelEnv(t)
	sel := &copySelector{ok: &fakeInference{model: "qwen-test"},
		badBaseURL: "user:" + credentialMarker + "@api.example.com/v1"}
	svc := env.service(&fakeInference{model: "qwen-test"}).WithProviderClients(sel)

	root, _ := env.budget.Root(ctx, env.scope, "task1", "root-"+env.u, 100)
	args := SubmitArgs{Scope: env.scope, Task: "task1", Key: "gen-" + env.u, Request: chatReq("hi"),
		Model: "qwen-test", BudgetID: root.ID, ReserveKey: "res-" + env.u, ReserveAmount: 15, Provider: "copy-bad"}
	g, err := svc.Submit(ctx, args)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	sel.bad = true
	done, err := svc.Execute(ctx, env.scope, g.ID)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !strings.Contains(done.Error, reasonNotAbsoluteLiteral) {
		t.Fatalf("the durable cause must name the branch %q, got %q", reasonNotAbsoluteLiteral, done.Error)
	}
	if strings.Contains(done.Error, credentialMarker) || strings.Contains(done.Error, "user:") {
		t.Fatalf("the durable cause carries the configured credential: %q", done.Error)
	}
}

// Byte-exact suffix comparison let a case-varied completion path through, which
// then dialled the doubled path. Case is not a reason to be silent.
func TestBaseURLDuplicatedPathIsRefusedRegardlessOfCase(t *testing.T) {
	for _, base := range []string{
		"https://api.example.com/V1/Chat/Completions",
		"https://api.example.com/v1/CHAT/COMPLETIONS",
		"https://api.example.com/V1/Chat/Completions/",
	} {
		err := buildRefused(t, base)
		if got := BaseURLConfigReason(err); got != ReasonBaseURLDuplicatedPath {
			t.Fatalf("%q: reason=%q, want %q (err=%v)", base, got, ReasonBaseURLDuplicatedPath, err)
		}
	}
	// Refusing the suffix must not become refusing every prefix that resembles it.
	for _, base := range []string{
		"https://api.example.com/v1/chat",
		"https://api.example.com/v1/completions",
	} {
		c, err := newQwenClient(secretstore.QwenSecret{APIKey: "k", BaseURL: base, Model: "m"})
		if err != nil {
			t.Fatalf("%q is a legitimate prefix and must build: %v", base, err)
		}
		if c == nil {
			t.Fatalf("%q: no client for a legitimate prefix", base)
		}
	}
}

// The exported injected-HTTP seam is a way around the gate unless it uses the
// same parser. Today it accepts every shape the guarded path refuses.
func TestNewQwenClientWithHTTPCannotBypassTheBaseURLGate(t *testing.T) {
	for _, tc := range []struct {
		base, reason string
	}{
		{"https://api.example.com/v1/chat/completions", ReasonBaseURLDuplicatedPath},
		{"https://user:" + credentialMarker + "@api.example.com/v1", ReasonBaseURLCredentials},
		{"user:" + credentialMarker + "@api.example.com/v1", reasonNotAbsoluteLiteral},
		{"http://api.example.com:/v1", ReasonBaseURLPortEmpty},
		{"http://api.example.com:99999/v1", ReasonBaseURLPortInvalid},
		{"https://api.example.com/v1?api_key=1", ReasonBaseURLQueryOrFragment},
		{"https://api.example.com/V1/Chat/Completions", ReasonBaseURLDuplicatedPath},
	} {
		c, err := NewQwenClientWithHTTP(secretstore.QwenSecret{APIKey: "k", BaseURL: tc.base, Model: "m"}, &http.Client{})
		if err == nil || c != nil {
			t.Fatalf("the exported seam accepted %q that every other path refuses (client=%v)", tc.base, c != nil)
		}
		if got := BaseURLConfigReason(err); got != tc.reason {
			t.Fatalf("%q: reason=%q, want %q (err=%v)", tc.base, got, tc.reason, err)
		}
		if strings.Contains(err.Error(), credentialMarker) {
			t.Fatalf("%q: the refusal echoes the configured credential: %v", tc.base, err)
		}
	}
	// The seam stays usable: a legitimate fixture still builds.
	c, err := NewQwenClientWithHTTP(secretstore.QwenSecret{APIKey: "k", BaseURL: "http://127.0.0.1:8000/v1", Model: "m"}, &http.Client{})
	if err != nil || c == nil {
		t.Fatalf("the injected-HTTP seam must still build a legitimate fixture: %v", err)
	}
}
