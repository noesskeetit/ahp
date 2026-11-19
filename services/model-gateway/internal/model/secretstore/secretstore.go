// Package secretstore reads scoped runtime secrets from the platform Vault.
//
// The execution plane must never read the operator's ambient environment to
// obtain live credentials (a contractor is not permitted to fish secrets out of
// a user's source env). Instead the orchestrator/Hive provisions a scoped
// runtime secret in Vault and only a short-lived token is handed to the
// process. This package turns that token into the typed secret a service needs
// without ever logging or returning the raw Vault response.
//
// It is intentionally dependency-free (stdlib HTTP) so model, tools and
// triggers can all import it without pulling extra modules into go.mod.
package secretstore

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// DefaultQwenPath is the KV v2 mount+path under which the orchestrator stores
// the scoped Qwen runtime secret for the execution plane.
const DefaultQwenPath = "secret/data/ahp-hive/execution/qwen"

// Env variable names the process may carry. They hold the Vault endpoint and a
// scoped token, never the secret value itself.
const (
	EnvVaultAddr  = "AHP_VAULT_ADDR"
	EnvVaultToken = "AHP_VAULT_DEV_TOKEN"
	// Fallbacks mirroring the standard Vault CLI variables.
	EnvVaultAddrAlt  = "VAULT_ADDR"
	EnvVaultTokenAlt = "VAULT_TOKEN"
)

// ErrNotConfigured is returned when neither a Vault endpoint nor a token is
// available in the environment. Callers translate this into a clear operator
// error rather than falling back to any other provider.
var ErrNotConfigured = errors.New("secretstore: vault endpoint or token not configured")

// QwenSecret is the scoped runtime secret required to reach a live Qwen
// endpoint. None of its fields may be logged or serialised: QwenSecret never
// implements MarshalJSON, and String redacts every field.
type QwenSecret struct {
	APIKey  string
	BaseURL string
	Model   string
	// PrivateHosts registers the base_url's internal host (and any named
	// internal peers) so the egress guard may reach it over plaintext http and
	// RFC1918/ULA space, exactly like the memory-embedding and model-files
	// mounts. It is an explicit opt-in sourced from the route record or the
	// AHP_LLM_PRIVATE_HOSTS environment override; the empty list reproduces the
	// previous behavior byte-for-byte. It never unlocks loopback, link-local,
	// metadata, multicast or reserved ranges -- the guard forbids those
	// regardless (see pkg/netpolicy).
	PrivateHosts []string
}

// Validate rejects empty credentials so a half-populated secret fails loud.
func (s QwenSecret) Validate() error {
	if strings.TrimSpace(s.APIKey) == "" {
		return errors.New("secretstore: qwen api_key is empty")
	}
	if strings.TrimSpace(s.BaseURL) == "" {
		return errors.New("secretstore: qwen base_url is empty")
	}
	if strings.TrimSpace(s.Model) == "" {
		return errors.New("secretstore: qwen model is empty")
	}
	return nil
}

// String redacts all fields so an accidental %v never leaks a credential.
func (s QwenSecret) String() string {
	return "QwenSecret{redacted}"
}

// Client is a minimal Vault KV v2 reader.
type Client struct {
	addr  string
	token string
	http  *http.Client
}

// New builds a Client from explicit endpoint and token. The token is never
// stored in logs; the returned client keeps it only for request headers.
func New(addr, token string) (*Client, error) {
	addr = strings.TrimRight(strings.TrimSpace(addr), "/")
	if addr == "" || strings.TrimSpace(token) == "" {
		return nil, ErrNotConfigured
	}
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		return nil, fmt.Errorf("secretstore: unsupported vault endpoint scheme in %q", addr)
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if strings.HasPrefix(addr, "https://") {
		tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	return &Client{
		addr:  addr,
		token: strings.TrimSpace(token),
		http:  &http.Client{Transport: tr, Timeout: 15 * time.Second},
	}, nil
}

// NewFromEnv builds a Client from the process environment, honouring the
// AHP_* variables with the standard VAULT_* variables as fallbacks.
func NewFromEnv() (*Client, error) {
	return New(firstEnv(EnvVaultAddr, EnvVaultAddrAlt), firstEnv(EnvVaultToken, EnvVaultTokenAlt))
}

func firstEnv(names ...string) string {
	for _, n := range names {
		if v := strings.TrimSpace(os.Getenv(n)); v != "" {
			return v
		}
	}
	return ""
}

// kvResponse is the KV v2 read envelope: the credential map lives under
// data.data; we only ever project the fields we need.
type kvResponse struct {
	Data struct {
		Data map[string]any `json:"data"`
	} `json:"data"`
	Errors []string `json:"errors"`
}

// Read returns the raw KV v2 secret map at path. The map values are returned
// verbatim; callers must treat them as secrets.
func (c *Client) Read(ctx context.Context, path string) (map[string]any, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("secretstore: empty path")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.addr+"/v1/"+strings.TrimLeft(path, "/"), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Vault-Token", c.token)
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		// Never echo the URL query or token; the path may embed nothing secret.
		return nil, fmt.Errorf("secretstore: vault read failed: %w", stripToken(err, c.token))
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("secretstore: reading vault response: %w", err)
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("secretstore: vault read returned status %d", resp.StatusCode)
	}
	var parsed kvResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, errors.New("secretstore: malformed vault response")
	}
	if len(parsed.Errors) > 0 {
		return nil, errors.New("secretstore: vault returned errors")
	}
	if parsed.Data.Data == nil {
		return nil, errors.New("secretstore: empty secret at path")
	}
	return parsed.Data.Data, nil
}

// ReadQwen reads and validates the scoped Qwen secret at path (default
// DefaultQwenPath). It never returns the raw Vault payload.
func (c *Client) ReadQwen(ctx context.Context, path string) (QwenSecret, error) {
	if strings.TrimSpace(path) == "" {
		path = DefaultQwenPath
	}
	data, err := c.Read(ctx, path)
	if err != nil {
		return QwenSecret{}, err
	}
	sec := QwenSecret{
		APIKey:       stringField(data, "api_key"),
		BaseURL:      stringField(data, "base_url"),
		Model:        stringField(data, "model"),
		PrivateHosts: stringListField(data, "private_hosts"),
	}
	// The per-deployment opt-in applies on EVERY scoped-secret read path, so the
	// execbridge worker seam (vaultSecretResolver -> ReadQwen) honours it too,
	// not only QwenFromEnv. Empty env leaves the record value byte-for-byte.
	sec.PrivateHosts = applyPrivateHostsEnv(sec.PrivateHosts)
	if err := sec.Validate(); err != nil {
		return QwenSecret{}, err
	}
	return sec, nil
}

// applyPrivateHostsEnv merges the AHP_LLM_PRIVATE_HOSTS opt-in into a private
// host list. It is the single place that consults the environment, so every
// secret read applies the same rule. When the environment declares nothing the
// input is returned unchanged (nil stays nil), preserving today's behavior.
func applyPrivateHostsEnv(in []string) []string {
	envHosts := splitHosts(os.Getenv("AHP_LLM_PRIVATE_HOSTS"))
	if len(envHosts) == 0 {
		return in
	}
	return dedupeHosts(append(append([]string{}, in...), envHosts...))
}

// QwenFromEnv resolves the scoped Qwen secret purely from the process
// environment. Absence of configuration is reported as ErrNotConfigured; there
// is deliberately no fallback to another provider.
func QwenFromEnv(ctx context.Context) (QwenSecret, error) {
	c, err := NewFromEnv()
	if err != nil {
		return QwenSecret{}, err
	}
	path := strings.TrimSpace(os.Getenv("AHP_LLM_VAULT_PATH"))
	if path == "" {
		path = DefaultQwenPath
	}
	// ReadQwen already merges the AHP_LLM_PRIVATE_HOSTS opt-in, so both the env
	// seam and the vault/execbridge seam resolve private hosts identically.
	return c.ReadQwen(ctx, path)
}

func stringField(m map[string]any, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

// stringListField reads a []string field that may be stored as a JSON array of
// strings or as a single comma-separated string. Absent or empty yields nil.
func stringListField(m map[string]any, key string) []string {
	switch v := m[key].(type) {
	case []any:
		out := make([]string, 0, len(v))
		for _, e := range v {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}
		return dedupeHosts(out)
	case string:
		return splitHosts(v)
	default:
		return nil
	}
}

// splitHosts parses a comma-separated host list, trimming and dropping empties.
func splitHosts(v string) []string {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return dedupeHosts(out)
}

// dedupeHosts removes empty and repeated entries, preserving first-seen order.
func dedupeHosts(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := in[:0:0]
	for _, h := range in {
		h = strings.TrimSpace(h)
		if h == "" {
			continue
		}
		if _, ok := seen[h]; ok {
			continue
		}
		seen[h] = struct{}{}
		out = append(out, h)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// stripToken removes the token from an error string as a defensive measure so
// a URL parse error can never surface it.
func stripToken(err error, token string) error {
	if token == "" {
		return err
	}
	return errors.New(strings.ReplaceAll(err.Error(), token, "<redacted>"))
}
