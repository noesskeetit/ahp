package model

// D1 (D-MODEL-EGRESS) + D2 (D-MODEL-REFUSAL-CLASS) acceptance tests, contract
// forum/posts/20261110T040500Z-dir-model-egress-private-host-gap-and-ambiguous-refusal.md.
// Hermetic: a fixed resolver maps names to chosen addresses and a recording
// dialer reports whether the network-policy guard let the request through, so
// no real provider, no loopback dial, no database and no clock are needed.

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/model/secretstore"
	"github.com/ahp-platform/ahp/pkg/netpolicy"
)

type fixedResolver struct{ m map[string][]string }

func (f fixedResolver) LookupHost(_ context.Context, host string) ([]string, error) {
	if a, ok := f.m[host]; ok {
		return a, nil
	}
	return nil, errors.New("no such host: " + host)
}

type recordDialer struct {
	dialed *string
}

func (d recordDialer) DialContext(_ context.Context, network, address string) (net.Conn, error) {
	*d.dialed = address
	// The guard passed; stop here without a real round trip. The http layer wraps
	// this into a transport error the caller classifies as ambiguous.
	return nil, errors.New("test: guard passed, dial stopped")
}

// runEgress builds a guarded Qwen client and issues one Stream. It returns the
// stream error (nil if none), whether the guard reached the dial path, and a
// construction error: netpolicy may reject a policy that lists a never-reachable
// host outright, which is itself a refusal and must never reach a dial.
func runEgress(t *testing.T, secret secretstore.QwenSecret, res fixedResolver) (streamErr error, dialed bool, buildErr error) {
	t.Helper()
	dialedAddr := ""
	c, buildErr := newQwenClient(secret, netpolicy.WithResolver(res), netpolicy.WithDialer(recordDialer{dialed: &dialedAddr}))
	if buildErr != nil {
		return nil, false, buildErr
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, streamErr = c.Stream(ctx, ChatRequest{Messages: []Message{{Role: "user", Content: "hi"}}})
	return streamErr, dialedAddr != "", nil
}

// refused reports whether an outcome shows the destination was blocked before any
// dial: either the policy refused to compile a never-reachable entry, or the
// guard denied the request. Both satisfy "forbidden class stays unreachable".
func refused(streamErr error, dialed bool, buildErr error) bool {
	if dialed {
		return false
	}
	return buildErr != nil || guardDenied(streamErr)
}

func guardDenied(err error) bool {
	var ne *netpolicy.Error
	return errors.As(err, &ne)
}

// D1: http to a registered internal host is permitted; the same host without
// registration keeps today's refusal, byte-for-byte.
func TestD1PrivateHostRegistrationOpensInternalEgress(t *testing.T) {
	res := fixedResolver{m: map[string][]string{"provider.internal": {"172.18.0.6"}}}

	// Without registration: today's behavior (scheme/host denial) is preserved.
	eNo, dialedNo, buildNo := runEgress(t, secretstore.QwenSecret{APIKey: "k", BaseURL: "http://provider.internal/v1", Model: "m"}, res)
	if buildNo != nil {
		t.Fatalf("unregistered http+RFC1918 policy must build (host is a plain name): %v", buildNo)
	}
	if eNo == nil {
		t.Fatal("unregistered http+RFC1918 must keep being refused")
	}
	if !guardDenied(eNo) || dialedNo {
		t.Fatalf("unregistered: want a guard denial before dial, got denied=%v dialed=%v err=%v", guardDenied(eNo), dialedNo, eNo)
	}

	// With registration: the guard must let it through to the dial path.
	sec := secretstore.QwenSecret{APIKey: "k", BaseURL: "http://provider.internal/v1", Model: "m", PrivateHosts: []string{"provider.internal"}}
	eYes, dialedYes, buildYes := runEgress(t, sec, res)
	if buildYes != nil {
		t.Fatalf("registered http+RFC1918 policy must build: %v", buildYes)
	}
	if !dialedYes {
		t.Fatalf("registered http+RFC1918 must reach the dial path; err=%v dialed=%v", eYes, dialedYes)
	}
	if guardDenied(eYes) {
		t.Fatalf("registered http+RFC1918 must not be guard-denied, got %v", eYes)
	}
}

// D1: a registered PUBLIC host over https has no regression.
func TestD1PublicHTTPSNoRegression(t *testing.T) {
	res := fixedResolver{m: map[string][]string{"api.example.com": {"93.184.216.34"}}}
	e, dialed, build := runEgress(t, secretstore.QwenSecret{APIKey: "k", BaseURL: "https://api.example.com/v1", Model: "m"}, res)
	if build != nil {
		t.Fatalf("public https policy must build: %v", build)
	}
	if !dialed || guardDenied(e) {
		t.Fatalf("public https must reach the dial path, dialed=%v denied=%v err=%v", dialed, guardDenied(e), e)
	}
}

// D1: forbidden classes stay refused even under pressure (non-empty PrivateHosts),
// as both an IP literal and a resolved name.
func TestD1ForbiddenClassesStayRefused(t *testing.T) {
	forbidden := []struct {
		name string
		ip   string
	}{
		{"loopback-name", "127.0.0.1"},
		{"metadata-name", "169.254.169.254"},
		{"linklocal-name", "169.254.1.1"},
		{"testnet3-name", "203.0.113.9"},
		{"benchmark-198-18", "198.18.0.5"},
		{"cgnat", "100.64.0.1"},
	}
	for _, tc := range forbidden {
		t.Run(tc.name, func(t *testing.T) {
			res := fixedResolver{m: map[string][]string{tc.name: {tc.ip}}}
			// as a name, in PrivateHosts
			e, dialed, build := runEgress(t, secretstore.QwenSecret{APIKey: "k", BaseURL: "http://" + tc.name + "/v1", Model: "m", PrivateHosts: []string{tc.name}}, res)
			if !refused(e, dialed, build) {
				t.Fatalf("%s: forbidden class must stay refused even when registered, dialed=%v build=%v err=%v", tc.name, dialed, build, e)
			}
			// as an IP literal, in PrivateHosts
			e2, dialed2, build2 := runEgress(t, secretstore.QwenSecret{APIKey: "k", BaseURL: "http://" + tc.ip + "/v1", Model: "m", PrivateHosts: []string{tc.ip}}, fixedResolver{})
			if !refused(e2, dialed2, build2) {
				t.Fatalf("%s IP-literal: forbidden class must stay refused even when registered, dialed=%v build=%v err=%v", tc.name, dialed2, build2, e2)
			}
		})
	}
}

// D2: a deterministic pre-send guard refusal is a distinct class from an
// ambiguous post-dial transport failure.
func TestD2PreSendRefusalIsDistinctFromAmbiguous(t *testing.T) {
	res := fixedResolver{m: map[string][]string{"provider.internal": {"172.18.0.6"}}}
	// Unregistered http+RFC1918 -> guard denies before any byte leaves: pre-send refusal.
	ePre, _, _ := runEgress(t, secretstore.QwenSecret{APIKey: "k", BaseURL: "http://provider.internal/v1", Model: "m"}, res)
	if !IsPreSendRefusal(ePre) {
		t.Fatalf("pre-send guard denial must be IsPreSendRefusal, got %v", ePre)
	}
	if IsTransportError(ePre) {
		t.Fatalf("pre-send guard denial must NOT be ambiguous transport, got %v", ePre)
	}
	// Registered but dial stopped after the guard passed: post-dial transport (ambiguous).
	eAmb, _, _ := runEgress(t, secretstore.QwenSecret{APIKey: "k", BaseURL: "http://provider.internal/v1", Model: "m", PrivateHosts: []string{"provider.internal"}}, res)
	if IsPreSendRefusal(eAmb) {
		t.Fatalf("post-dial failure must NOT be a pre-send refusal, got %v", eAmb)
	}
	if !IsTransportError(eAmb) {
		t.Fatalf("post-dial failure must remain ambiguous transport, got %v", eAmb)
	}
}
