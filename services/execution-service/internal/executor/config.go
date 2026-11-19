package executor

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/model/secretstore"
)

// Env variable names the execution-role process may carry. They hold object
// storage configuration and the Vault endpoint/token (via secretstore); they
// never hold a model credential value, which is read only from Vault.
const (
	EnvS3Endpoint     = "AHP_S3_ENDPOINT"
	EnvS3Region       = "AHP_S3_REGION"
	EnvS3AccessKey    = "AHP_MINIO_ROOT_USER"
	EnvS3SecretKey    = "AHP_MINIO_ROOT_PASSWORD"
	EnvArtBucket      = "AHP_EXEC_ARTIFACT_BUCKET"
	EnvArtifactParser = "AHP_ARTIFACT_PARSER"

	// DefaultArtifactBucket is used only when no bucket is configured; it is a
	// bucket name, not a secret. Callers in isolated runs set their own.
	DefaultArtifactBucket = "ahp-exec-artifacts"
)

// Config wires one execution-plane executor. All fields are validated by New.
type Config struct {
	// Object storage for real artifacts. Endpoint is a URL (host parsed from
	// it) and TLS is decided by that scheme alone -- there is no separate switch,
	// so a caller cannot ask for TLS over an http:// endpoint.
	S3Endpoint    string
	S3Region      string
	S3AccessKey   string
	S3SecretKey   string
	Bucket        string
	ParserCommand artifacts.ParserCommand

	// Model. When nil the executor resolves a native-Qwen secret from Vault and
	// builds the OpenAI-compatible client itself. Supplying Model is reserved
	// for client-conformance tests (a deterministic fake turn); it is NOT a
	// production path and never silently substitutes a non-Qwen provider.
	Model core.ModelClient

	// MaxSteps bounds the model/tool loop for one turn.
	MaxSteps int
}

// New builds a production executor from an explicit config plus the shared
// Postgres pool. It returns an error (fail closed) if object storage or the
// model path is misconfigured; it never installs a fake executor.
func New(ctx context.Context, pool *pgxpool.Pool, cfg Config) (*Executor, error) {
	if pool == nil {
		return nil, errors.New("executor: nil pool")
	}
	if cfg.Bucket == "" {
		return nil, errors.New("executor: artifact bucket required")
	}
	model := cfg.Model
	if model == nil {
		secret, err := secretstore.QwenFromEnv(ctx)
		if err != nil {
			if errors.Is(err, secretstore.ErrNotConfigured) {
				return nil, errors.New("executor: native-Qwen runtime secret not configured (fail closed; no fallback provider)")
			}
			return nil, err
		}
		qm, err := NewQwenModel(ctx, secret)
		if err != nil {
			return nil, err
		}
		model = qm
	}
	if cfg.MaxSteps <= 0 {
		cfg.MaxSteps = 16
	}

	plane, err := NewArtifactPlane(ctx, pool, ArtifactPlaneConfig{
		S3Endpoint: cfg.S3Endpoint, S3Region: cfg.S3Region,
		S3AccessKey: cfg.S3AccessKey, S3SecretKey: cfg.S3SecretKey,
		Bucket: cfg.Bucket, ParserCommand: cfg.ParserCommand,
	})
	if err != nil {
		return nil, err
	}
	return &Executor{
		store:    plane.Store,
		minio:    plane.client,
		bucket:   plane.bucket,
		model:    model,
		tools:    plane.Tools,
		maxSteps: cfg.MaxSteps,
	}, nil
}

// NewFromEnv builds a production executor entirely from the process
// environment plus the shared pool. The native-Qwen credential is read only
// from the scoped Vault runtime secret; its absence fails closed.
func NewFromEnv(ctx context.Context, pool *pgxpool.Pool) (*Executor, error) {
	cfg := Config{
		S3Endpoint:    os.Getenv(EnvS3Endpoint),
		S3Region:      os.Getenv(EnvS3Region),
		S3AccessKey:   os.Getenv(EnvS3AccessKey),
		S3SecretKey:   os.Getenv(EnvS3SecretKey),
		Bucket:        firstNonEmpty(os.Getenv(EnvArtBucket), DefaultArtifactBucket),
		ParserCommand: artifacts.ParserCommand{Path: os.Getenv(EnvArtifactParser)},
		MaxSteps:      envInt("AHP_EXEC_MAX_STEPS", 16),
	}
	return New(ctx, pool, cfg)
}

// ObjectStorageConfigured reports whether this process has the object-storage
// configuration a real artifact needs. It exists so a plane can say out loud
// that it cannot serve an artifact-bearing skill, instead of accepting a task
// and answering COMPLETED with nothing to download.
func ObjectStorageConfigured() bool { return os.Getenv(EnvS3Endpoint) != "" }

// ArtifactPlaneFromEnv builds just the sealed-artifact store and its tool, from
// the same environment the direct executor reads. Used by the Model Gateway
// bridge so the default wiring can seal real artifacts too, and by tests that
// need to read an artifact back through the same store that wrote it.
func ArtifactPlaneFromEnv(ctx context.Context, pool *pgxpool.Pool) (*ArtifactPlane, error) {
	return NewArtifactPlane(ctx, pool, ArtifactPlaneConfig{
		S3Endpoint:    os.Getenv(EnvS3Endpoint),
		S3Region:      os.Getenv(EnvS3Region),
		S3AccessKey:   os.Getenv(EnvS3AccessKey),
		S3SecretKey:   os.Getenv(EnvS3SecretKey),
		Bucket:        firstNonEmpty(os.Getenv(EnvArtBucket), DefaultArtifactBucket),
		ParserCommand: artifacts.ParserCommand{Path: os.Getenv(EnvArtifactParser)},
	})
}

func parseEndpoint(raw string) (host string, secure bool, err error) {
	if raw == "" {
		return "", false, errors.New("executor: object storage endpoint not configured")
	}
	u, perr := url.Parse(raw)
	if perr != nil || u.Host == "" {
		return "", false, errors.New("executor: object storage endpoint is not a valid URL")
	}
	return u.Host, strings.EqualFold(u.Scheme, "https"), nil
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func envInt(name string, def int) int {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}
