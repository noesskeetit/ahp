package toolsapi

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func runNativeGatewayTrial(t *testing.T, runtime string, server *httptest.Server, e *env,
	original, replacement, scenario, probe, hermesSource string,
	probeArgs ...string,
) {
	t.Helper()
	dir := t.TempDir()
	handshake := writeProbeHandshake(t, dir, server, e, original, replacement)
	repo, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Second)
	defer cancel()
	arguments := append([]string{filepath.Join("profiling", "state-journal", probe), dir, scenario}, probeArgs...)
	command := exec.CommandContext(ctx, runtime, arguments...)
	command.Dir = repo
	pythonPath := []string{repo, filepath.Join(repo, "sdk", "python")}
	if hermesSource != "" {
		pythonPath = append([]string{hermesSource}, pythonPath...)
	}
	command.Env = append(os.Environ(),
		"AHP_STATE_HANDSHAKE="+handshake,
		"PYTHONPATH="+strings.Join(pythonPath, string(os.PathListSeparator)), "PYTHONDONTWRITEBYTECODE=1",
	)
	if hermesSource != "" {
		home := filepath.Join(dir, "hermes")
		if err := os.Mkdir(home, 0700); err != nil {
			t.Fatal(err)
		}
		command.Env = append(command.Env, "HERMES_HOME="+home)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	result, err := command.Output()
	if err != nil {
		t.Fatalf("native probe %s failed: %v\n%s%s", probe, err, result, stderr.Bytes())
	}
	if !json.Valid(result) {
		t.Fatalf("native probe %s did not report JSON: %s", probe, result)
	}
	t.Logf("%s %s", probe, result)
}

func writeProbeHandshake(t *testing.T, dir string, server *httptest.Server, e *env, original, replacement string) string {
	t.Helper()
	write := func(name string, data []byte) string {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	client := e.currentClient()
	private, err := x509.MarshalPKCS8PrivateKey(client.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	handshake, err := json.Marshal(map[string]string{
		"base_url": server.URL, "task_token": original, "replacement_token": replacement,
		"ca_file":   write("ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})),
		"cert_file": write("client.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: client.Certificate[0]})),
		"key_file":  write("client-key.pem", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private})),
	})
	if err != nil {
		t.Fatal(err)
	}
	return write("handshake.json", handshake)
}
