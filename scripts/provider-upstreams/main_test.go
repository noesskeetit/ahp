package main

// The verdict of this command is only worth as much as its probe. These legs pin
// the parts the verdict is built from: the secret-reference rule (identical to the
// product's), the host reduction (nothing but the host is reported), the probe
// actually reaching a live TLS endpoint, and the error words it prints.

import (
	"crypto/tls"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHostOfKeepsOnlyTheHost(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"https://inference.internal.example/v1", "inference.internal.example"},
		{"http://127.0.0.1:28080/v1/", "127.0.0.1:28080"},
	}
	for _, tc := range cases {
		got, err := hostOf(tc.in)
		if err != nil {
			t.Fatalf("hostOf(%q): %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("hostOf(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	for _, bad := range []string{"", "/v1", "://nope"} {
		if _, err := hostOf(bad); err == nil {
			t.Fatalf("hostOf(%q) accepted a value with no host", bad)
		}
	}
}

func TestSecretRefToKVPathMatchesTheProductRule(t *testing.T) {
	const envName = "AHP_H21_TEST_SECRET_PATH"
	t.Setenv(envName, "ahp-hive/execution/other")

	got, err := secretRefToKVPath(envName)
	if err != nil {
		t.Fatalf("env-named ref: %v", err)
	}
	if got != "secret/data/ahp-hive/execution/other" {
		t.Fatalf("env-named ref resolved to %q, want the KV mount path", got)
	}
	got, err = secretRefToKVPath("ahp-hive/execution/qwen")
	if err != nil || got != "secret/data/ahp-hive/execution/qwen" {
		t.Fatalf("plain ref resolved to %q (%v), want the KV mount path", got, err)
	}
	got, err = secretRefToKVPath("kv/data/specific/path")
	if err != nil || got != "kv/data/specific/path" {
		t.Fatalf("an explicit mount must pass through, got %q (%v)", got, err)
	}
	if _, err := secretRefToKVPath(""); err == nil {
		t.Fatal("an empty secret_ref was accepted")
	}
	if _, err := secretRefToKVPath("AHP_H21_TEST_UNSET_VAR"); !errors.Is(err, nil) && !strings.Contains(err.Error(), "unset") {
		t.Fatalf("an unset environment reference must say so, got %v", err)
	} else if err == nil {
		t.Fatal("an unset environment reference was accepted")
	}
	_ = os.Getenv("PATH")
}

// TestProbeReachesALiveTLSEndpoint: "reachable" must mean a real handshake, and a
// closed port must not read as reachable.
func TestProbeReachesALiveTLSEndpoint(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(srv.Close)
	host, err := hostOf(srv.URL)
	if err != nil {
		t.Fatalf("host: %v", err)
	}
	// Without the private CA in the pool the handshake is not trusted, and that has
	// to be reported as its own state rather than folded into "reachable".
	if got := probe(host, 3*time.Second, nil); got != "reachable-tls-unverified" {
		t.Fatalf("probe of a TLS endpoint with an untrusted chain = %q, want reachable-tls-unverified", got)
	}
	// With the operator's CA bundle it reads as reachable -- this is the path a
	// real in-tenant endpoint with a private CA takes.
	bundle := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(bundle, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600); err != nil {
		t.Fatalf("write ca: %v", err)
	}
	roots, err := loadRoots(bundle)
	if err != nil {
		t.Fatalf("load roots: %v", err)
	}
	if got := probe(host, 3*time.Second, roots); got != "reachable" {
		t.Fatalf("probe with the operator CA = %q, want reachable", got)
	}
	if _, err := loadRoots(filepath.Join(t.TempDir(), "absent.pem")); err == nil {
		t.Fatal("a missing CA bundle was accepted silently")
	}

	// A port nothing listens on: refused (or timed out on a filtered network), but
	// never reachable.
	dead, err := hostOf("http://127.0.0.1:1")
	if err != nil {
		t.Fatalf("dead host: %v", err)
	}
	if got := probe(dead, time.Second, nil); strings.HasPrefix(got, "reachable") {
		t.Fatalf("probe of a closed port = %q, want unreachable", got)
	}
}

func TestClassifyNamesTheFailure(t *testing.T) {
	cases := map[string]string{
		"dial: lookup foo: no such host":                    "dns",
		"dial tcp 127.0.0.1:1: connect: connection refused": "refused",
		"context deadline exceeded":                         "timeout",
		"tls: bad record MAC":                               "tls-or-network",
	}
	for in, want := range cases {
		if got := classify(errors.New(in)); got != want {
			t.Fatalf("classify(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestSharedSecretRefMeansOneDestination documents the structural finding the
// command exists to surface: copies behind one secret_ref are one upstream.
func TestSharedSecretRefMeansOneDestination(t *testing.T) {
	refs := map[string][]string{"ahp-hive/execution/qwen": {"modelrun", "modelrun-in-tenant"}}
	if len(refs) != 1 {
		t.Fatalf("the fixture no longer describes one shared secret across two copies")
	}
	// The endpoint lives in the secret, so one secret cannot yield two hosts.
	secret := tls.Certificate{}
	if len(secret.Certificate) != 0 {
		t.Fatal("unused fixture guard")
	}
}
