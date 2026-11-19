package executor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/atrest/objectcipher"
	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// ArtifactToolName is the tool a model calls to seal a real artifact for the
// turn it is running. It is exported because the tool is no longer private to
// this executor: any plane that drives a core.Runtime loop -- including the
// Model Gateway bridge, which is what the execution role actually mounts --
// has to be able to offer the same side effect, or the plane cannot deliver the
// artifact its own Agent Card advertises.
const ArtifactToolName = artifactToolName

// Turn carries per-attempt identity into the tool layer, which the core loop
// does not thread through. Exported so a bridge that runs the same loop can
// attribute a sealed artifact to the tenant scope and task that own it -- an
// artifact written under an empty scope is either a failure or, worse, a leak.
type Turn struct {
	Scope  contracts.Scope
	TaskID string
	// ContextID is the State thread / A2A context the turn belongs to. Sealing
	// without it is refused: the artifact would leave the deletion cascade.
	ContextID string
}

// WithTurn attaches the attempt identity that artifact writes are bound to.
func WithTurn(ctx context.Context, t Turn) context.Context {
	return withTurn(ctx, turnCtx{scope: t.Scope, taskID: t.TaskID, contextID: t.ContextID})
}

// SealedArtifact is the payload the artifact tool returns to the model, and the
// record the plane turns into an a2a.Artifact. Exported for the same reason as
// the tool: every plane must report the seal the same way, sha256 included.
type SealedArtifact struct {
	ArtifactID string `json:"artifact_id"`
	SHA256     string `json:"sha256"`
	Size       int64  `json:"size"`
	MIME       string `json:"mime"`
	Name       string `json:"name"`
}

// ParseSealedArtifact decodes one emitted tool step. ok=false means the content
// is not a seal (an error step, or a different tool), never "empty artifact".
func ParseSealedArtifact(content json.RawMessage) (SealedArtifact, bool) {
	var s SealedArtifact
	if len(content) == 0 || json.Unmarshal(content, &s) != nil {
		return SealedArtifact{}, false
	}
	if s.ArtifactID == "" || s.SHA256 == "" {
		return SealedArtifact{}, false
	}
	return s, true
}

// ArtifactsFromSeals renders sealed artifacts as the A2A objects a task carries.
// The sha256 is the store's verified seal, not a claim by the model.
func ArtifactsFromSeals(seals []SealedArtifact) []*a2a.Artifact {
	if len(seals) == 0 {
		return nil
	}
	out := make([]*a2a.Artifact, 0, len(seals))
	for _, ar := range seals {
		out = append(out, &a2a.Artifact{
			ID:   a2a.ArtifactID(ar.ArtifactID),
			Name: ar.Name,
			Metadata: map[string]any{
				"sha256": ar.SHA256,
				"size":   ar.Size,
				"mime":   ar.MIME,
			},
			Parts: a2a.ContentParts{a2a.NewTextPart(fmt.Sprintf(
				"sealed artifact %q sha256=%s size=%d", ar.Name, ar.SHA256, ar.Size))},
		})
	}
	return out
}

// Seal writes content under scope into object storage and returns the seal the
// store verified (PUT bytes == sealed sha256). It is the one implementation of
// "this turn produced an artifact", used both by the model-callable
// write_artifact tool and by a plane that exports its own turn result. Idempotent
// on (taskID, sha256), so a retried attempt with identical bytes replays the
// same upload row instead of minting a second artifact.
//
// The taskID is part of the idempotency key, not decoration: without it two
// tasks that sealed the same bytes would collide onto one artifact and one of
// them would read the other's object.
//
// The binding is not optional either. PDF rev3.2 makes deletion a cascade by
// context/subject/tenant, and a row written without context_id is reachable
// only by its id -- exactly the orphan the cascade never catches. An unbound
// Seal is therefore refused fail-closed (contracts.ErrInvalid) instead of
// silently written: half a deletion obligation is worse than a visible failure.
func (p *ArtifactPlane) Seal(ctx context.Context, scope contracts.Scope, b artifacts.Binding, name, mime, content string) (SealedArtifact, error) {
	data := []byte(content)
	if len(data) == 0 {
		return SealedArtifact{}, contracts.NewError(contracts.ErrInvalid, "executor: artifact content is empty")
	}
	if p == nil {
		return SealedArtifact{}, contracts.NewError(contracts.ErrInvalid,
			"executor: artifact seal refused: no artifact plane is mounted")
	}
	if err := scope.Validate(); err != nil {
		return SealedArtifact{}, err
	}
	if strings.TrimSpace(b.ContextID) == "" {
		return SealedArtifact{}, contracts.NewError(contracts.ErrInvalid,
			"executor: artifact seal refused: empty context binding would put the artifact outside the context deletion cascade")
	}
	if strings.TrimSpace(b.TaskID) == "" {
		return SealedArtifact{}, contracts.NewError(contracts.ErrInvalid,
			"executor: artifact seal refused: empty task binding would collide the idempotency key across tasks")
	}
	if p.client == nil {
		return SealedArtifact{}, errors.New("executor: nil object client")
	}
	sum := sha256.Sum256(data)
	sha := hex.EncodeToString(sum[:])
	if strings.TrimSpace(name) == "" {
		name = "artifact.txt"
	}
	if strings.TrimSpace(mime) == "" {
		mime = "text/plain"
	}
	key := b.TaskID + "/" + sha
	up, err := p.Store.BeginUploadWithBinding(ctx, scope, b, key, artifacts.UploadRequest{
		Name: name, MIME: mime, SHA256: sha, Size: int64(len(data)),
	})
	if err != nil {
		return SealedArtifact{}, err
	}
	if _, err := p.client.PutObject(ctx, p.bucket, up.Object, bytes.NewReader(data),
		int64(len(data)), minio.PutObjectOptions{ContentType: mime}); err != nil {
		return SealedArtifact{}, fmt.Errorf("executor: artifact PUT: %w", err)
	}
	ref, err := p.Store.Seal(ctx, scope, up.ID)
	if err != nil {
		return SealedArtifact{}, err
	}
	return SealedArtifact{ArtifactID: ref.ID, SHA256: ref.SHA256, Size: ref.Size, MIME: ref.MIME, Name: ref.Name}, nil
}

// ArtifactPlaneConfig is the object-storage side of a turn.
type ArtifactPlaneConfig struct {
	S3Endpoint  string
	S3Region    string
	S3AccessKey string
	S3SecretKey string
	Bucket      string
	// ParserCommand is an operator-configured sandboxed extraction executable.
	// Empty keeps extractable artifacts pending and unavailable for download.
	ParserCommand artifacts.ParserCommand
	// ObjectCipher, when non-nil, mounts at-rest encryption deterministically
	// (embedders/tests). When nil, the plane resolves the mount from the
	// operator environment via objectcipher.FromEnv, exactly as a product role.
	ObjectCipher *objectcipher.Cipher
}

// ArtifactPlane is the sealed-artifact store plus the tool registry that offers
// it to a model. Reused by the direct executor and by the Model Gateway bridge
// so that "this plane can produce an artifact" is one implementation, not two.
type ArtifactPlane struct {
	Store  *artifacts.Store
	Tools  *core.ToolRegistry
	client *minio.Client
	bucket string
}

// Bucket is the object-storage bucket the plane seals into.
func (p *ArtifactPlane) Bucket() string { return p.bucket }

// NewArtifactPlane builds the store, verifies the schema and registers the
// write_artifact tool. It fails closed on a missing bucket or an unusable
// endpoint: a plane that advertises an artifact it cannot seal is worse than one
// that does not mount the tool at all.
func NewArtifactPlane(ctx context.Context, pool *pgxpool.Pool, cfg ArtifactPlaneConfig) (*ArtifactPlane, error) {
	if pool == nil {
		return nil, errors.New("executor: nil pool")
	}
	if cfg.Bucket == "" {
		return nil, errors.New("executor: artifact bucket required")
	}
	endpoint, secure, err := parseEndpoint(cfg.S3Endpoint)
	if err != nil {
		return nil, err
	}
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.S3AccessKey, cfg.S3SecretKey, ""),
		Region: cfg.S3Region,
		Secure: secure,
	})
	if err != nil {
		return nil, err
	}
	// The artifacts store enforces tenant isolation and label admission on every
	// read; this Authorize hook is the execution plane's own gate.
	artCipher := cfg.ObjectCipher
	if artCipher == nil {
		artCipher, err = objectcipher.FromEnv(ctx, client)
		if err != nil {
			return nil, err
		}
	}
	store, err := artifacts.New(pool, client, cfg.Bucket, func(_ context.Context, scope contracts.Scope, _, _ string) error {
		return scope.Validate()
	}, artifacts.WithObjectCipher(artCipher), artifacts.WithParserCommand(cfg.ParserCommand))
	if err != nil {
		return nil, err
	}
	if err := store.Migrate(ctx); err != nil {
		return nil, err
	}
	// The tool holds the plane, and the plane holds the tool. That is deliberate:
	// the model-callable path and the plane-exports-its-own-result path must be
	// the SAME write, or a plane can pass an admission test through one door and
	// serve the other differently.
	plane := &ArtifactPlane{Store: store, client: client, bucket: cfg.Bucket}
	plane.Tools = core.NewToolRegistry(&guardedExecutor{
		inner: &artifactTool{plane: plane}, guard: artifactGuardian{}})
	plane.Tools.Register(core.ToolDef{
		Name:        artifactToolName,
		Description: "Persist a UTF-8 text artifact produced by this turn to tenant-scoped object storage. Returns its immutable sha256-sealed id.",
		Parameters:  json.RawMessage(artifactToolSchema),
	})
	return plane, nil
}
