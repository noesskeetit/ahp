// Standalone workload SDK example: no Agent Space or agent framework required.
// Input is a private platform hand-out for an already claimed run.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	sdkstate "github.com/ahp-platform/ahp/sdk/go/state"
	"net/http"
	"os"
	"time"
)

type handout struct {
	BaseURL   string         `json:"base_url"`
	CAFile    string         `json:"ca_file"`
	CertFile  string         `json:"cert_file"`
	KeyFile   string         `json:"key_file"`
	TaskToken string         `json:"task_token"`
	Fence     sdkstate.Fence `json:"fence"`
}

func run(path, phase string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return errors.New("cannot read private hand-out")
	}
	var hand handout
	if json.Unmarshal(raw, &hand) != nil {
		return errors.New("invalid hand-out")
	}
	certificate, err := tls.LoadX509KeyPair(hand.CertFile, hand.KeyFile)
	if err != nil {
		return errors.New("cannot load workload certificate")
	}
	pem, err := os.ReadFile(hand.CAFile)
	if err != nil {
		return errors.New("cannot load trusted CA")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return errors.New("invalid trusted CA")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12}}
	defer transport.CloseIdleConnections()
	client, err := sdkstate.New(sdkstate.Config{BaseURL: hand.BaseURL, HTTPClient: &http.Client{Transport: transport, Timeout: 15 * time.Second}, TokenSource: func(context.Context) (string, error) { return hand.TaskToken, nil }})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	namespace := []string{"standalone-example"}
	if phase == "write" {
		// A retry reuses the same key, expected version and body. Never invent a new
		// key because the previous HTTP response was lost.
		_, err = client.PutKV(ctx, hand.Fence, namespace, "from-go", "standalone-go-v1", 0, json.RawMessage(`{"language":"go","value":42}`))
		return err
	}
	if phase != "verify" {
		return errors.New("expected write or verify")
	}
	for _, lang := range []string{"go", "python"} {
		item, err := client.GetKV(ctx, namespace, "from-"+lang)
		if err != nil {
			return err
		}
		var value struct {
			Language string `json:"language"`
			Value    int    `json:"value"`
		}
		if json.Unmarshal(item.Value, &value) != nil || value.Language != lang || value.Value != 42 || item.Version != 1 {
			return errors.New("cross-language value mismatch")
		}
	}
	return nil
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: standalone-go PRIVATE_HANDOUT write|verify")
		os.Exit(2)
	}
	if err := run(os.Args[1], os.Args[2]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("standalone Go State SDK PASS")
}
