package executor

import (
	"context"
	"encoding/json"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// artifactToolName is the single tool the canonical executor offers the model.
const artifactToolName = "write_artifact"

const artifactToolSchema = `{"type":"object","additionalProperties":false,"properties":{` +
	`"name":{"type":"string","description":"safe file name"},` +
	`"mime":{"type":"string","description":"media type, e.g. text/plain"},` +
	`"content":{"type":"string","description":"UTF-8 artifact body"}},"required":["name","mime","content"]}`

type artifactArgs struct {
	Name    string `json:"name"`
	MIME    string `json:"mime"`
	Content string `json:"content"`
}

// artifactTool is the model-callable door onto the plane's one write path. It
// keeps no storage handle of its own: the tool and a plane that exports its own
// turn result go through ArtifactPlane.Seal, so "sealed" means the same thing on
// both doors -- same idempotency key, same store verification, same sha256.
type artifactTool struct {
	plane *ArtifactPlane
}

func (a *artifactTool) Execute(ctx context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
	var aa artifactArgs
	if err := json.Unmarshal(args, &aa); err != nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "artifact args: "+err.Error())
	}
	turn := turnFromCtx(ctx)
	seal, err := a.plane.Seal(ctx, turn.scope,
		artifacts.Binding{ContextID: turn.contextID, TaskID: turn.taskID},
		aa.Name, aa.MIME, aa.Content)
	if err != nil {
		return nil, err
	}
	out, _ := json.Marshal(artifactResult{
		ArtifactID: seal.ArtifactID, SHA256: seal.SHA256, Size: seal.Size, MIME: seal.MIME, Name: seal.Name,
	})
	return out, nil
}

// artifactResult is the tool payload returned to the model and parsed back by
// the executor to build the a2a.Artifact.
type artifactResult struct {
	ArtifactID string `json:"artifact_id"`
	SHA256     string `json:"sha256"`
	Size       int64  `json:"size"`
	MIME       string `json:"mime"`
	Name       string `json:"name"`
}
