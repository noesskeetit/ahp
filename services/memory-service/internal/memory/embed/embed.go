// Package embed is the Memory service's embedding backend: the component that
// turns text into the vector the pgvector column stores and the hybrid search
// signal ranks against.
//
// It speaks the OpenAI-compatible /v1/embeddings contract, which is what
// vLLM, TEI and the Cloud.ru ModelRun inference deployments expose, so a new
// embedder is configuration rather than code. The package deliberately does
// not contain a local scoring function: a hash or a hand-written "embedding"
// looks like a vector to every assertion in the test suite while carrying no
// semantics at all, and that is worse than having no embedder, because it lets
// a lexical-only search be reported as semantic. Absence is therefore
// represented by a nil backend, which callers degrade to lexical search and
// log honestly.
package embed

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/netpolicy"
)

// Environment configuration. A backend exists only when the base URL is set;
// an incomplete configuration is refused rather than half-mounted, because the
// pgvector column width cannot be guessed after the table exists.
const (
	EnvBaseURL      = "AHP_MEMORY_EMBEDDING_BASE_URL"
	EnvModel        = "AHP_MEMORY_EMBEDDING_MODEL"
	EnvDimension    = "AHP_MEMORY_EMBEDDING_DIM"
	EnvAPIKey       = "AHP_MEMORY_EMBEDDING_API_KEY"
	EnvKeyFile      = "AHP_MEMORY_EMBEDDING_KEY_FILE"
	EnvRevision     = "AHP_MEMORY_EMBEDDING_REVISION"
	EnvPrivateHosts = "AHP_MEMORY_EMBEDDING_PRIVATE_HOSTS"
	EnvTimeout      = "AHP_MEMORY_EMBEDDING_TIMEOUT"
)

// maxDimension is pgvector's own ceiling for the vector type.
const maxDimension = 16000

// defaultTimeout bounds one embedding call. Embedding is on the write path of
// every Propose and on the read path of every Search, so an unbounded client
// would hand the dependency a hold on the request.
const defaultTimeout = 30 * time.Second

// Config is an explicit backend configuration; FromEnv is the production
// constructor and this type is what it produces.
type Config struct {
	Revision     string
	PrivateHosts []string
	BaseURL      string
	APIKey       string
	Model        string
	Dimensions   int
	Timeout      time.Duration
}

// Backend calls one OpenAI-compatible embeddings endpoint.
type Backend struct {
	revision string
	baseURL  string
	apiKey   string
	model    string
	dims     int
	hc       *http.Client
}

// New validates a configuration. Every refusal names the field that is wrong
// and never repeats a credential.
func New(cfg Config) (*Backend, error) { return newWithHTTP(cfg, nil) }

// newWithHTTP is a fixture seam; FromEnv and production New always use netpolicy.
func newWithHTTP(cfg Config, injected *http.Client) (*Backend, error) {
	base := strings.TrimRight(cfg.BaseURL, "/")
	switch {
	case base == "":
		return nil, contracts.NewError(contracts.ErrInvalid, "memory/embed: empty base URL ("+EnvBaseURL+")")
	case !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://"):
		return nil, contracts.NewError(contracts.ErrInvalid, "memory/embed: base URL must be http(s) ("+EnvBaseURL+")")
	case cfg.Model == "":
		return nil, contracts.NewError(contracts.ErrInvalid, "memory/embed: MODEL is required ("+EnvModel+")")
	case cfg.Dimensions <= 0 || cfg.Dimensions > maxDimension:
		return nil, contracts.NewError(contracts.ErrInvalid, fmt.Sprintf(
			"memory/embed: DIMENSION %d is outside 1..%d (%s)", cfg.Dimensions, maxDimension, EnvDimension))
	case cfg.APIKey == "":
		return nil, contracts.NewError(contracts.ErrInvalid, "memory/embed: a credential is required ("+EnvAPIKey+" or "+EnvKeyFile+")")
	}
	u, err := url.Parse(base)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, contracts.NewError(contracts.ErrInvalid, "memory/embed: malformed base URL")
	}
	// Accept a base that already carries the /v1 suffix without doubling it,
	// matching the convention of the model-plane OpenAI-compatible client.
	if strings.HasSuffix(base, "/v1") {
		base = strings.TrimSuffix(base, "/v1")
	}
	to := cfg.Timeout
	if to <= 0 {
		to = defaultTimeout
	}
	hc := injected
	if hc == nil {
		policy := netpolicy.Policy{AllowedHosts: []string{u.Host}, PrivateHosts: cfg.PrivateHosts, AllowedMethods: []string{http.MethodPost}, AllowedPathPrefixes: []string{strings.TrimSuffix(u.Path, "/v1") + "/v1/embeddings"}, MaxResponseBytes: 32 << 20, Timeout: to}
		// Explicit private host entries never allow loopback or metadata addresses.
		for _, h := range cfg.PrivateHosts {
			if h == u.Host {
				policy.AllowedHosts = nil
			}
		}
		var err error
		hc, err = netpolicy.NewClient(policy)
		if err != nil {
			return nil, contracts.NewError(contracts.ErrInvalid, "memory/embed: invalid egress policy")
		}
	}
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Backend{baseURL: base, apiKey: cfg.APIKey, model: cfg.Model, dims: cfg.Dimensions, revision: cfg.Revision, hc: hc}, nil
}

// FromEnv builds the configured backend, or (nil, nil) when embedding is not
// configured at all. "Not configured" is a clean answer the caller turns into
// lexical-only search; a half-configured one is a refusal.
func FromEnv() (*Backend, error) {
	base := strings.TrimSpace(os.Getenv(EnvBaseURL))
	if base == "" {
		return nil, nil
	}
	key := strings.TrimSpace(os.Getenv(EnvAPIKey))
	keyFile := strings.TrimSpace(os.Getenv(EnvKeyFile))
	switch {
	case key != "" && keyFile != "":
		return nil, contracts.NewError(contracts.ErrInvalid,
			"memory/embed: "+EnvAPIKey+" and "+EnvKeyFile+" are mutually exclusive; set one credential source")
	case keyFile != "":
		fileKey, err := readKeyFile(keyFile)
		if err != nil {
			return nil, err
		}
		key = fileKey
	}
	if key == "" {
		return nil, contracts.NewError(contracts.ErrInvalid, "memory/embed: a credential is required ("+
			EnvAPIKey+" or "+EnvKeyFile+")")
	}
	rawDim := strings.TrimSpace(os.Getenv(EnvDimension))
	if rawDim == "" {
		return nil, contracts.NewError(contracts.ErrInvalid, "memory/embed: DIMENSION is required ("+EnvDimension+
			"): the pgvector column width must equal the embedder's output width")
	}
	dims, err := strconv.Atoi(rawDim)
	if err != nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "memory/embed: DIMENSION is not a number ("+EnvDimension+")")
	}
	var to time.Duration
	if raw := strings.TrimSpace(os.Getenv(EnvTimeout)); raw != "" {
		parsed, perr := time.ParseDuration(raw)
		if perr != nil {
			return nil, contracts.NewError(contracts.ErrInvalid, "memory/embed: "+EnvTimeout+" is not a duration")
		}
		if parsed <= 0 {
			return nil, contracts.NewError(contracts.ErrInvalid, "memory/embed: timeout must be positive")
		}
		to = parsed
	}
	return New(Config{BaseURL: base, APIKey: key, Model: strings.TrimSpace(os.Getenv(EnvModel)),
		Dimensions: dims, Timeout: to, Revision: strings.TrimSpace(os.Getenv(EnvRevision)), PrivateHosts: strings.FieldsFunc(os.Getenv(EnvPrivateHosts), func(r rune) bool { return r == ',' })})
}

// readKeyFile reads a credential from a file that only its owner may read. A
// credential anyone on the machine can read is not a credential, and a
// prototype is not an excuse to install one.
func readKeyFile(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", contracts.NewError(contracts.ErrInvalid, "memory/embed: "+EnvKeyFile+" is unreadable")
	}
	if !info.Mode().IsRegular() {
		return "", contracts.NewError(contracts.ErrInvalid, "memory/embed: "+EnvKeyFile+" must be a regular file")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return "", contracts.NewError(contracts.ErrInvalid, "memory/embed: "+EnvKeyFile+
			" must not be readable or writable by group or other (chmod 0600)")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", contracts.NewError(contracts.ErrInvalid, "memory/embed: "+EnvKeyFile+" is unreadable")
	}
	key := strings.TrimSpace(string(raw))
	if key == "" {
		return "", contracts.NewError(contracts.ErrInvalid, "memory/embed: "+EnvKeyFile+" is empty")
	}
	return key, nil
}

// Dimensions reports the vector width this backend produces. The Memory store
// takes its pgvector column width from here, so the two cannot drift apart.
func (b *Backend) Dimensions() int { return b.dims }

// Identity binds vectors to an endpoint, model, revision and dimension. Model
// aliases still need operator versioning when the provider changes their weights.
func (b *Backend) Identity() string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\n%s\n%s\n%d", b.baseURL, b.model, b.revision, b.dims)))
	return fmt.Sprintf("sha256:%x", sum)
}

// Model reports the configured model id (non-secret).
func (b *Backend) Model() string { return b.model }

// APIKey is exposed only so the mount can hand the credential to nothing else;
// it is never part of Describe.
func (b *Backend) APIKey() string { return b.apiKey }

// Describe is the log-safe identity of the backend.
func (b *Backend) Describe() string {
	return fmt.Sprintf("openai-compatible model=%s dimensions=%d", b.model, b.dims)
}

// Embed produces one vector, satisfying the memory.Embedder function type.
func (b *Backend) Embed(ctx context.Context, text string) ([]float32, error) {
	out, err := b.EmbedBatch(ctx, []string{text})
	if err != nil {
		return nil, err
	}
	return out[0], nil
}

type embedRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type embedDatum struct {
	Index     int       `json:"index"`
	Embedding []float64 `json:"embedding"`
}

type embedResponse struct {
	Model string       `json:"model"`
	Data  []embedDatum `json:"data"`
}

// EmbedBatch embeds texts in call order. It answers with the model's vectors or
// with a failure: a short answer, a wrong width, a non-finite component or a
// malformed body is reported as UNAVAILABLE, because the store must not write a
// column value it cannot later rank against.
func (b *Backend) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}
	body, err := json.Marshal(embedRequest{Model: b.model, Input: texts})
	if err != nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "memory/embed: request encoding")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.baseURL+"/v1/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "memory/embed: request build")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+b.apiKey)

	resp, err := b.hc.Do(req)
	if err != nil {
		return nil, contracts.NewError(contracts.ErrUnavailable, "memory/embed: embeddings endpoint unreachable")
	}
	defer func() { _, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16)); _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, contracts.NewError(contracts.ErrUnavailable, "memory/embed: embeddings response unreadable")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, contracts.NewError(contracts.ErrUnavailable, fmt.Sprintf(
			"memory/embed: embeddings endpoint answered %d for model %s", resp.StatusCode, b.model))
	}
	var parsed embedResponse
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, contracts.NewError(contracts.ErrUnavailable,
			"memory/embed: embeddings response is not OpenAI-compatible JSON")
	}
	if parsed.Model != "" && parsed.Model != b.model {
		return nil, contracts.NewError(contracts.ErrUnavailable, "memory/embed: response model mismatch")
	}
	if len(parsed.Data) != len(texts) {
		return nil, contracts.NewError(contracts.ErrUnavailable, fmt.Sprintf(
			"memory/embed: asked for %d embeddings, endpoint returned %d", len(texts), len(parsed.Data)))
	}
	out := make([][]float32, len(texts))
	for _, d := range parsed.Data {
		if d.Index < 0 || d.Index >= len(texts) {
			return nil, contracts.NewError(contracts.ErrUnavailable,
				fmt.Sprintf("memory/embed: endpoint returned index %d outside the request", d.Index))
		}
		if len(d.Embedding) != b.dims {
			return nil, contracts.NewError(contracts.ErrUnavailable, fmt.Sprintf(
				"memory/embed: model %s returned %d dimensions, configuration declared %d",
				b.model, len(d.Embedding), b.dims))
		}
		if out[d.Index] != nil {
			return nil, contracts.NewError(contracts.ErrUnavailable, "memory/embed: duplicate embedding index")
		}
		vec := make([]float32, len(d.Embedding))
		var norm float64
		for i, f := range d.Embedding {
			if math.IsNaN(f) || math.IsInf(f, 0) {
				return nil, contracts.NewError(contracts.ErrUnavailable,
					"memory/embed: model returned a non-finite component")
			}
			if math.Abs(f) > math.MaxFloat32 {
				return nil, contracts.NewError(contracts.ErrUnavailable, "memory/embed: component overflows float32")
			}
			vec[i] = float32(f)
			norm += float64(vec[i]) * float64(vec[i])
		}
		if norm == 0 {
			return nil, contracts.NewError(contracts.ErrUnavailable, "memory/embed: zero vector")
		}
		out[d.Index] = vec
	}
	for i, vec := range out {
		if vec == nil {
			return nil, contracts.NewError(contracts.ErrUnavailable, fmt.Sprintf(
				"memory/embed: endpoint returned no vector for input %d", i))
		}
	}
	return out, nil
}

// Interface guards: the backend satisfies both the store's function-typed
// Embedder (through Embed) and the width-declaring Source interface.
