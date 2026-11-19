package state

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
	artifacts "github.com/ahp-platform/ahp/sdk/go/artifacts"
)

// The threshold leaves room for base64 and State's 1MiB HTTP request limit.
const inlinePayloadBytes = 256 << 10
const inlineBatchBytes = 512 << 10
const payloadMagic = "\x00AHP-PAYLOAD-v1\n"
const metadataPayloadKey = "$ahp_checkpoint_payload_v1" // legacy user key, never a wire discriminator
const artifactPayloadEncoding = "ahp-artifact-v1"

// checkpointArtifact is an immutable projection. In particular, current grant
// labels returned as EffectiveLabels must never enter State's retry fingerprint.
type checkpointArtifact struct {
	ID         string     `json:"id"`
	Tenant     string     `json:"tenant"`
	Deployment string     `json:"deployment"`
	Subject    string     `json:"subject"`
	ContextID  string     `json:"context_id"`
	TaskID     string     `json:"task_id"`
	Version    string     `json:"version"`
	SHA256     string     `json:"sha256"`
	Size       int64      `json:"size"`
	Labels     labels.Set `json:"labels"`
}

func (c *Client) payloadClient() (*artifacts.Client, error) {
	if c.artifacts != nil {
		return c.artifacts.WithTokenSource(artifacts.TokenSource(c.token)), nil
	}
	return artifacts.New(artifacts.Config{BaseURL: c.base, HTTPClient: c.http, TokenSource: artifacts.TokenSource(c.token)})
}

// spill stores opaque serialized bytes. The serializer and its type tag are
// deliberately outside this layer, so old checkpoints keep their exact format.
func (c *Client) spill(ctx context.Context, runID string, data []byte, force bool) ([]byte, error) {
	if len(data) <= inlinePayloadBytes && !force {
		return data, nil
	}
	if int64(len(data)) > artifacts.MaxSize {
		return nil, newInvalid("checkpoint payload exceeds artifact limit")
	}
	a, err := c.payloadClient()
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	key, _ := contracts.Digest([]string{"checkpoint-v1", runID, digest})
	ref, err := a.Put(ctx, artifacts.PutRequest{Key: key, Name: "checkpoint.bin", MIME: "application/octet-stream", Data: data})
	if err != nil {
		return nil, err
	}
	if ref.SHA256 != digest || ref.Size != int64(len(data)) || ref.TaskID == "" || ref.ContextID == "" {
		return nil, newInvalid("artifact checkpoint binding or integrity mismatch")
	}
	raw, err := json.Marshal(checkpointArtifact{ID: ref.ID, Tenant: ref.Tenant, Deployment: ref.Deployment, Subject: ref.Subject, ContextID: ref.ContextID, TaskID: ref.TaskID, Version: ref.Version, SHA256: ref.SHA256, Size: ref.Size, Labels: ref.Labels})
	if err != nil {
		return nil, err
	}
	return raw, nil
}

// An absent marker is always legacy opaque bytes, regardless of their content.
func (c *Client) hydrate(ctx context.Context, data []byte, encoding ...string) ([]byte, error) {
	codec := ""
	if len(encoding) > 0 {
		codec = encoding[0]
	}
	if codec == "" {
		return data, nil
	}
	if codec != artifactPayloadEncoding {
		return nil, newInvalid("unsupported checkpoint payload encoding")
	}
	var expected artifacts.ArtifactRef
	if err := json.Unmarshal(data, &expected); err != nil || expected.ID == "" || expected.Version == "" || expected.ContextID == "" || expected.TaskID == "" || expected.Size < 0 || expected.Size > artifacts.MaxSize {
		return nil, newInvalid("invalid checkpoint artifact reference")
	}
	a, err := c.payloadClient()
	if err != nil {
		return nil, err
	}
	actual, err := a.Get(ctx, expected.ID)
	if err != nil {
		return nil, err
	}
	// Re-read authority on every hydration, including after restart/revocation.
	// Effective labels may be tightened by the owner; immutable source labels,
	// binding and bytes must still match the recorded descriptor.
	if actual.ID != expected.ID || actual.Version != expected.Version || actual.SHA256 != expected.SHA256 || actual.Size != expected.Size || actual.Tenant != expected.Tenant || actual.Deployment != expected.Deployment || actual.Subject != expected.Subject || actual.ContextID != expected.ContextID || actual.TaskID != expected.TaskID || !reflect.DeepEqual(actual.Labels, expected.Labels) {
		return nil, newInvalid("checkpoint artifact descriptor mismatch")
	}
	data, err = a.Download(ctx, expected.ID)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(data)
	if int64(len(data)) != expected.Size || hex.EncodeToString(sum[:]) != expected.SHA256 {
		return nil, newInvalid("checkpoint artifact integrity mismatch")
	}
	return data, nil
}

func (c *Client) spillCheckpoint(ctx context.Context, runID string, cp Checkpoint) (Checkpoint, error) {
	return c.spillCheckpointBudget(ctx, runID, cp, inlineBatchBytes)
}
func (c *Client) spillCheckpointBudget(ctx context.Context, runID string, cp Checkpoint, budget int) (Checkpoint, error) {
	cp.Parts = append([]CheckpointPart(nil), cp.Parts...)
	total := len(cp.Metadata)
	for _, p := range cp.Parts {
		total += len(p.Data)
	}
	force := total > budget
	for i, p := range cp.Parts {
		if p.Data == nil {
			continue
		}
		data, err := c.spill(ctx, runID, p.Data, force && len(p.Data) > 1024)
		if err != nil {
			return Checkpoint{}, err
		}
		cp.Parts[i].Data = data
		cp.Parts[i].Encoding = payloadEncoding(p.Data, force && len(p.Data) > 1024)
	}
	cp.MetadataEncoding = ""
	if len(cp.Metadata) > inlinePayloadBytes || (force && len(cp.Metadata) > 1024) {
		data, err := c.spill(ctx, runID, cp.Metadata, true)
		if err != nil {
			return Checkpoint{}, err
		}
		cp.Metadata = data
		cp.MetadataEncoding = artifactPayloadEncoding
	}

	return cp, nil
}
func (c *Client) hydrateCheckpoint(ctx context.Context, cp Checkpoint) (Checkpoint, error) {
	if cp.MetadataEncoding != "" {
		data, err := c.hydrate(ctx, cp.Metadata, cp.MetadataEncoding)
		if err != nil {
			return Checkpoint{}, err
		}
		cp.Metadata = data
		cp.MetadataEncoding = ""
	}
	cp.Parts = append([]CheckpointPart(nil), cp.Parts...)
	for i, p := range cp.Parts {
		if p.Data != nil {
			data, err := c.hydrate(ctx, p.Data, p.Encoding)
			if err != nil {
				return Checkpoint{}, err
			}
			cp.Parts[i].Data = data
		}
		cp.Parts[i].Encoding = ""
	}
	return cp, nil
}
func (c *Client) spillWrites(ctx context.Context, runID string, writes []PendingWrite) ([]PendingWrite, error) {
	return c.spillWritesBudget(ctx, runID, writes, inlineBatchBytes)
}
func (c *Client) spillWritesBudget(ctx context.Context, runID string, writes []PendingWrite, budget int) ([]PendingWrite, error) {
	out := append([]PendingWrite(nil), writes...)
	total := 0
	for _, w := range writes {
		total += len(w.Data)
	}
	for i, w := range writes {
		data, err := c.spill(ctx, runID, w.Data, total > budget && len(w.Data) > 1024)
		if err != nil {
			return nil, fmt.Errorf("pending write payload: %w", err)
		}
		out[i].Data = data
		out[i].Encoding = payloadEncoding(w.Data, total > budget && len(w.Data) > 1024)
	}
	return out, nil
}

func payloadEncoding(data []byte, force bool) string {
	if len(data) > inlinePayloadBytes || force {
		return artifactPayloadEncoding
	}
	return ""
}
