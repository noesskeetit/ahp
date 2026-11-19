package core

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"unicode/utf8"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
	artifacts "github.com/ahp-platform/ahp/sdk/go/artifacts"
)

const outputInlineBytes = 64 << 10
const outputPreviewBytes = 4 << 10
const maxOutputChunkBytes = 32 << 10
const ToolReadOutputChunk = "read_output_chunk"

// OutputArtifacts is implemented by the shared Artifacts owner API client.
// Small outputs do not require an artifact service; large outputs fail closed
// when no client has been wired.
type OutputArtifacts interface {
	Put(context.Context, artifacts.PutRequest) (artifacts.ArtifactRef, error)
	Get(context.Context, string) (artifacts.ArtifactRef, error)
	ReadRange(context.Context, string, int64, int64) ([]byte, error)
}

type externalOutput struct {
	Artifact  artifacts.ArtifactRef `json:"artifact"`
	Preview   string                `json:"preview"`
	Truncated bool                  `json:"truncated"`
	Reader    string                `json:"reader"`
}

func (r *Runtime) outputClient(req *space.CoreRequest) OutputArtifacts {
	if a, ok := r.cfg.Artifacts.(*artifacts.Client); ok {
		return a.WithTokenSource(func(context.Context) (string, error) { return req.TaskToken, nil })
	}
	return r.cfg.Artifacts
}
func outputScopeMatches(ref artifacts.ArtifactRef, req *space.CoreRequest) bool {
	ctx := req.ContextID
	if ctx == "" {
		ctx = req.ThreadID
	}
	return ref.Tenant == req.Scope.Tenant && ref.Deployment == req.Scope.Deployment && ref.Subject == req.Scope.Subject && ref.ContextID == ctx
}
func (r *Runtime) externalizeOutput(ctx context.Context, req *space.CoreRequest, kind string, raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) <= outputInlineBytes {
		return raw, nil
	}
	a := r.outputClient(req)
	if a == nil {
		return nil, fmt.Errorf("large output requires Artifacts owner client")
	}
	sum := sha256.Sum256(raw)
	sha := hex.EncodeToString(sum[:])
	key, _ := contracts.Digest([]string{"core-output-v1", req.ContextID, req.RunID, kind, sha})
	ref, err := a.Put(ctx, artifacts.PutRequest{Key: key, Name: kind + ".json", MIME: "application/octet-stream", Data: raw})
	if err != nil {
		return nil, err
	}
	if !outputScopeMatches(ref, req) || ref.TaskID == "" || ref.Version == "" || ref.ID == "" || ref.Size != int64(len(raw)) || ref.SHA256 != sha {
		return nil, fmt.Errorf("output artifact integrity or task binding mismatch")
	}
	preview := raw
	if len(preview) > outputPreviewBytes {
		preview = preview[:outputPreviewBytes]
	}
	for len(preview) > 0 && !utf8.Valid(preview) {
		preview = preview[:len(preview)-1]
	}
	return json.Marshal(externalOutput{Artifact: ref, Preview: string(preview), Truncated: true, Reader: ToolReadOutputChunk})
}
func readOutputDef() ToolDef {
	return ToolDef{Name: ToolReadOutputChunk, Description: "Read a bounded byte range of a large output artifact using its exact id and version. Data is base64 encoded; offset and length are bytes (length at most 32768).", Parameters: json.RawMessage(`{"type":"object","properties":{"id":{"type":"string"},"version":{"type":"string"},"offset":{"type":"integer","minimum":0},"length":{"type":"integer","minimum":1,"maximum":32768}},"required":["id","version","offset","length"],"additionalProperties":false}`)}
}
func (r *Runtime) readOutputChunk(ctx context.Context, req *space.CoreRequest, args json.RawMessage) (json.RawMessage, error) {
	var in struct {
		ID      string `json:"id"`
		Version string `json:"version"`
		Offset  int64  `json:"offset"`
		Length  int64  `json:"length"`
	}
	if err := json.Unmarshal(args, &in); err != nil || in.ID == "" || in.Version == "" || in.Offset < 0 || in.Length <= 0 || in.Length > maxOutputChunkBytes {
		return nil, contracts.NewError(contracts.ErrInvalid, "invalid output chunk request")
	}
	a := r.outputClient(req)
	if a == nil {
		return nil, contracts.NewError(contracts.ErrForbidden, "artifact reader unavailable")
	}
	ref, err := a.Get(ctx, in.ID)
	if err != nil {
		return nil, err
	}
	if ref.ID != in.ID || ref.Version != in.Version || !outputScopeMatches(ref, req) {
		return nil, contracts.NewError(contracts.ErrForbidden, "output artifact scope or version mismatch")
	}
	if in.Offset >= ref.Size {
		return nil, contracts.NewError(contracts.ErrInvalid, "output offset outside artifact")
	}
	data, err := a.ReadRange(ctx, in.ID, in.Offset, in.Length)
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{"id": ref.ID, "version": ref.Version, "offset": in.Offset, "data": base64.StdEncoding.EncodeToString(data), "encoding": "base64", "next_offset": in.Offset + int64(len(data)), "eof": in.Offset+int64(len(data)) == ref.Size})
}
