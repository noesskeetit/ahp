package check

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCheckAllCannotAcceptMissingOrMalformedSource(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		if _, err := CheckAll(filepath.Join(t.TempDir(), "missing")); err == nil {
			t.Fatal("missing source accepted")
		}
	})
	p := filepath.Join(t.TempDir(), "bad.go")
	if err := os.WriteFile(p, []byte("package agent\nfunc broken( {"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := CheckAll(p); err == nil {
		t.Fatal("malformed function after imports accepted")
	}
}

func TestDirectNetworkAndDiskCallsAreErrors(t *testing.T) {
	for _, source := range []string{
		`package agent; import web "net/http"; func run(){ web.Get("https://example.invalid") }`,
		`package agent; import web "net/http"; func run(){ c:=&web.Client{}; c.Do(nil) }`,
		`package agent; import web "net/http"; func run(c *web.Client){ c.Get("https://example.invalid") }`,
		`package agent; import web "net/http"; func run(){ web.DefaultClient.Do(nil) }`,
		`package agent; import "os"; func run(){ os.WriteFile("state.json",nil,0600) }`,
	} {
		p := filepath.Join(t.TempDir(), "agent.go")
		if err := os.WriteFile(p, []byte(source), 0600); err != nil {
			t.Fatal(err)
		}
		result, err := CheckAll(p)
		if err != nil {
			t.Fatal(err)
		}
		blocked := false
		for _, v := range result.Violations {
			if v.Severity == Error {
				blocked = true
			}
		}
		if !blocked {
			t.Fatal("direct external IO not blocked")
		}
	}
}

func TestNamedSDKClientIsNotMistakenForDirectNetwork(t *testing.T) {
	p := filepath.Join(t.TempDir(), "agent.go")
	if err := os.WriteFile(p, []byte(`package agent; type sdkClient struct{}; func (sdkClient) Get(){}; func run(client sdkClient) {client.Get()}`), 0600); err != nil {
		t.Fatal(err)
	}
	result, err := CheckAll(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range result.Violations {
		if v.Severity == Error {
			t.Fatalf("unresolved SDK call blocked: %+v", v)
		}
	}
}

func TestEmptyRootIsNotSuccessfulVerification(t *testing.T) {
	if _, err := CheckAll(t.TempDir()); err == nil {
		t.Fatal("no source files accepted")
	}
}
