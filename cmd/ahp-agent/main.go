package main

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ahp-platform/ahp/internal/agentworkload"
	"github.com/ahp-platform/ahp/pkg/auth"
)

// BuildProfile is immutable executable behavior, set by the image build. The
// operator supplies runtime identity; a tenant cannot replace this profile.
var BuildProfile = "You are an AHP custom agent. Answer accurately and concisely."

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--drain" {
		if err := syscall.Kill(1, syscall.SIGUSR1); err != nil {
			os.Exit(1)
		}
		time.Sleep(5 * time.Second)
		return
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "agent startup:", err)
		os.Exit(1)
	}
}
func required(name string) (string, error) {
	v := os.Getenv(name)
	if strings.TrimSpace(v) == "" {
		return "", fmt.Errorf("%s required", name)
	}
	return v, nil
}
func run() error {
	names := []string{"AHP_AGENT_TENANT", "AHP_AGENT_DEPLOYMENT", "AHP_AGENT_ID", "AHP_AGENT_DIGEST", "AHP_AGENT_IMAGE", "AHP_AGENT_PUBLIC_KEYS_FILE", "AHP_AGENT_GATEWAY_WORKLOADS", "AHP_STATE_URL", "AHP_MODEL_URL", "AHP_TLS_CERT", "AHP_TLS_KEY", "AHP_TLS_CA"}
	values := map[string]string{}
	for _, name := range names {
		v, err := required(name)
		if err != nil {
			return err
		}
		values[name] = v
	}
	cert, err := tls.LoadX509KeyPair(values["AHP_TLS_CERT"], values["AHP_TLS_KEY"])
	if err != nil {
		return err
	}
	pem, err := os.ReadFile(values["AHP_TLS_CA"])
	if err != nil {
		return err
	}
	ca := x509.NewCertPool()
	if !ca.AppendCertsFromPEM(pem) {
		return fmt.Errorf("invalid workload CA")
	}
	raw, err := os.ReadFile(values["AHP_AGENT_PUBLIC_KEYS_FILE"])
	if err != nil {
		return err
	}
	var keys map[string]string
	if err = json.Unmarshal(raw, &keys); err != nil {
		return err
	}
	public := map[string]ed25519.PublicKey{}
	for kid, encoded := range keys {
		b, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil || len(b) != ed25519.PublicKeySize {
			return fmt.Errorf("invalid public verification key")
		}
		public[kid] = b
	}
	client := &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: ca, Certificates: []tls.Certificate{cert}}}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	hostname, _ := os.Hostname()
	svc, err := agentworkload.New(agentworkload.Config{Tenant: values["AHP_AGENT_TENANT"], Deployment: values["AHP_AGENT_DEPLOYMENT"], AgentID: values["AHP_AGENT_ID"], Digest: values["AHP_AGENT_DIGEST"], Image: values["AHP_AGENT_IMAGE"], Profile: BuildProfile, Worker: hostname, StateURL: values["AHP_STATE_URL"], ModelURL: values["AHP_MODEL_URL"], GatewayWorkloads: strings.Split(values["AHP_AGENT_GATEWAY_WORKLOADS"], ","), Client: client, Verifier: auth.NewVerifier("ahp", public)})
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()
	workerDone := make(chan struct{})
	go func() { defer close(workerDone); svc.Run(workerCtx) }()
	drainSignals := make(chan os.Signal, 1)
	signal.Notify(drainSignals, syscall.SIGUSR1)
	defer signal.Stop(drainSignals)
	go func() {
		select {
		case <-ctx.Done():
		case <-drainSignals:
			svc.Drain()
		}
	}()
	listen := os.Getenv("AHP_LISTEN")
	if listen == "" {
		listen = ":8443"
	}
	server := &http.Server{Addr: listen, Handler: svc, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 120 * time.Second, IdleTimeout: 30 * time.Second, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12, ClientAuth: tls.VerifyClientCertIfGiven, ClientCAs: ca, Certificates: []tls.Certificate{cert}}}
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		svc.Drain()
		shutdown, cancel := context.WithTimeout(context.Background(), 40*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
		select {
		case <-workerDone:
		case <-shutdown.Done():
		}
		workerCancel()
	}()
	err = server.ListenAndServeTLS("", "")
	if err == http.ErrServerClosed {
		<-shutdownDone
		return nil
	}
	return err
}
