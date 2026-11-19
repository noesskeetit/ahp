package state

import (
	"context"
	"encoding/json"
	"github.com/ahp-platform/ahp/pkg/contracts"
	artifacts "github.com/ahp-platform/ahp/sdk/go/artifacts"
)

// AdminClient has a distinct administrative credential and URL namespace.
// The server requires a human control-plane credential bound to mTLS and IAM;
// an ordinary task token is rejected even when it has state capability.
type AdminClient struct{ client *Client }
type AdminTarget struct {
	Deployment string `json:"deployment"`
	Subject    string `json:"subject"`
}

func NewAdmin(cfg Config) (*AdminClient, error) {
	c, err := New(cfg)
	if err != nil {
		return nil, err
	}
	c.prefix = "/v1/state-admin/"
	return &AdminClient{c}, nil
}
func (a *AdminClient) ExportThread(ctx context.Context, target AdminTarget, threadID string) (ControlExport, error) {
	var out ControlExport
	in := struct {
		Target AdminTarget `json:"target"`
		Input  any         `json:"input"`
	}{target, struct {
		ThreadID string `json:"thread_id"`
	}{threadID}}
	err := a.client.callTask(ctx, "ExportThread", in, &out)
	return out, err
}

// RehomeExport converts only explicitly encoded artifact refs via owner Copy.
// Legacy opaque bytes remain byte-identical. Source and recipient clients use
// independently current task credentials. No artifact bytes pass through SDK
// upload, which would erase provenance and accumulated security labels.
func (c *Client) RehomeExport(ctx context.Context, source *Client, exp ControlExport, target artifacts.TransferTarget, key string) (ControlExport, error) {
	if source == nil || key == "" || target.ContextID == "" || target.TaskID == "" || target.Deployment == "" {
		return ControlExport{}, newInvalid("source client, stable recovery key and full target required")
	}
	raw, err := json.Marshal(exp)
	if err != nil {
		return ControlExport{}, err
	}
	var out ControlExport
	if err = json.Unmarshal(raw, &out); err != nil {
		return out, err
	}
	from, err := source.payloadClient()
	if err != nil {
		return out, err
	}
	to, err := c.payloadClient()
	if err != nil {
		return out, err
	}
	rewrite := func(data []byte, encoding string) ([]byte, error) {
		if encoding == "" {
			return data, nil
		}
		if encoding != artifactPayloadEncoding {
			return nil, newInvalid("unsupported checkpoint encoding")
		}
		var ref artifacts.ArtifactRef
		if err := json.Unmarshal(data, &ref); err != nil {
			return nil, newInvalid("invalid checkpoint descriptor")
		}
		grant, err := from.TransferGrant(ctx, ref, target)
		if err != nil {
			return nil, err
		}
		if grant.ArtifactID != ref.ID || grant.Version != ref.Version || grant.SHA256 != ref.SHA256 {
			return nil, newInvalid("source transfer grant does not match checkpoint digest")
		}
		op, _ := contracts.Digest([]string{"rehome-v1", key, ref.ID, ref.Version, ref.SHA256})
		copied, err := to.CopyFromTransferGrant(ctx, grant.Grant, op)
		if err != nil {
			return nil, err
		}
		if copied.Tenant != ref.Tenant || copied.Subject != ref.Subject || copied.SHA256 != ref.SHA256 || copied.Size != ref.Size || copied.ContextID != target.ContextID || copied.TaskID != target.TaskID || copied.Deployment != target.Deployment {
			return nil, newInvalid("owner transfer result binding mismatch")
		}
		return json.Marshal(checkpointArtifact{ID: copied.ID, Tenant: copied.Tenant, Deployment: copied.Deployment, Subject: copied.Subject, ContextID: copied.ContextID, TaskID: copied.TaskID, Version: copied.Version, SHA256: copied.SHA256, Size: copied.Size, Labels: copied.Labels})
	}
	for i := range out.Checkpoints {
		cp := &out.Checkpoints[i]
		cp.Metadata, err = rewrite(cp.Metadata, cp.MetadataEncoding)
		if err != nil {
			return ControlExport{}, err
		}
		for j := range cp.Parts {
			p := &cp.Parts[j]
			p.Data, err = rewrite(p.Data, p.Encoding)
			if err != nil {
				return ControlExport{}, err
			}
		}
	}
	for i := range out.Writes {
		for j := range out.Writes[i].Writes {
			w := &out.Writes[i].Writes[j]
			w.Data, err = rewrite(w.Data, w.Encoding)
			if err != nil {
				return ControlExport{}, err
			}
		}
	}
	return out, nil
}

// ImportThreadTo imports a prepared snapshot into the current task's context.
// RehomeExport must first relocate explicitly encoded foreign-context refs.
func (c *Client) ImportThreadTo(ctx context.Context, exp ControlExport, contextID string) (Thread, error) {
	var out Thread
	err := c.callTask(ctx, "ImportThread", struct {
		Export    ControlExport `json:"export"`
		ContextID string        `json:"context_id"`
	}{exp, contextID}, &out)
	return out, err
}
