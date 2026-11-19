// Command tool-result reads back the stored result of ONE tool operation under
// the caller's current authority. It is a read: it never dispatches, never
// fences, and never treats a refusal as an empty result.
//
// Environment: AHP_API_URL, AHP_CA_FILE, AHP_CERT_FILE, AHP_KEY_FILE,
// AHP_TOKEN_FILE (current task token), AHP_OPERATION_KEY, and for an
// artifact-stored result AHP_ARTIFACTS_API_URL when it differs from AHP_API_URL.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/sdk/go/artifacts"
	"github.com/ahp-platform/ahp/sdk/go/tools"
)

func env(name string) string {
	v := os.Getenv(name)
	if v == "" {
		panic("required configuration: " + name)
	}
	return v
}

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cert, e := tls.LoadX509KeyPair(env("AHP_CERT_FILE"), env("AHP_KEY_FILE"))
	if e != nil {
		return errors.New("invalid mTLS identity")
	}
	ca, e := os.ReadFile(env("AHP_CA_FILE"))
	if e != nil {
		return errors.New("invalid trust bundle")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return errors.New("invalid trust bundle")
	}
	httpc := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		RootCAs: roots, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}}, Timeout: time.Minute}
	defer httpc.CloseIdleConnections()
	token := func(context.Context) (string, error) {
		raw, e := os.ReadFile(env("AHP_TOKEN_FILE"))
		if e != nil {
			return "", errors.New("current token unavailable")
		}
		return string(trim(raw)), nil
	}
	base := env("AHP_API_URL")
	client, e := tools.New(tools.Config{BaseURL: base, HTTPClient: httpc, TokenSource: token})
	if e != nil {
		return e
	}
	result, e := client.ToolResult(ctx, env("AHP_OPERATION_KEY"))
	if e != nil {
		// Every refusal stays a refusal. CONFLICT means the row has not settled
		// (or never will): poll Get. NOT_FOUND means absent or someone else's.
		// FORBIDDEN means this reader is not the authority that paid for it.
		return fmt.Errorf("result read refused with %s: %w", contracts.Code(e), e)
	}
	switch result.Storage {
	case tools.StorageInline:
		out, _ := json.Marshal(result)
		fmt.Println(string(out))
	case tools.StorageArtifact:
		// The bytes live in the Artifacts owner. They are fetched under THIS
		// authority and the declared digest is verified before anything is used.
		artifactsURL := base
		if v := os.Getenv("AHP_ARTIFACTS_API_URL"); v != "" {
			artifactsURL = v
		}
		store, e := artifacts.New(artifacts.Config{BaseURL: artifactsURL, HTTPClient: httpc, TokenSource: token})
		if e != nil {
			return e
		}
		body, e := tools.FetchResultBytes(ctx, store, result)
		if e != nil {
			return fmt.Errorf("artifact result unusable: %w", e)
		}
		fmt.Printf("{\"artifact_id\":%q,\"bytes\":%d,\"digest\":%q}\n", result.Ref.ArtifactID, len(body), result.Ref.Digest)
	}
	return nil
}

func trim(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r' || b[len(b)-1] == ' ' || b[len(b)-1] == '\t') {
		b = b[:len(b)-1]
	}
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\t') {
		b = b[1:]
	}
	return b
}
