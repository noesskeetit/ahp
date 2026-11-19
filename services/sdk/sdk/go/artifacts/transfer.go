package artifacts

import (
	"context"
	"time"
)

// TransferTarget identifies the recipient task; tenant/subject cannot change.
type TransferTarget struct {
	Deployment string `json:"target_deployment"`
	ContextID  string `json:"target_context"`
	TaskID     string `json:"target_task"`
}
type TransferGrant struct {
	Grant      string    `json:"grant"`
	ArtifactID string    `json:"artifact_id"`
	Version    string    `json:"version"`
	SHA256     string    `json:"sha256"`
	ExpiresAt  time.Time `json:"expires_at"`
}

// TransferGrant delegates one immutable source to a specific current target.
func (c *Client) TransferGrant(ctx context.Context, ref ArtifactRef, target TransferTarget) (TransferGrant, error) {
	var out TransferGrant
	in := struct {
		ID      string `json:"id"`
		Version string `json:"version"`
		TransferTarget
	}{ref.ID, ref.Version, target}
	err := c.call(ctx, "TransferGrant", in, &out, true)
	return out, err
}

// CopyFromTransferGrant preserves provenance and taints the target context.
// Repeating the SAME key reconciles an ambiguous dispatch; never retry with a
// fresh key unless the operator has accepted a genuinely new copy attempt.
func (c *Client) CopyFromTransferGrant(ctx context.Context, grant, key string) (ArtifactRef, error) {
	var out ArtifactRef
	err := c.call(ctx, "CopyFromTransferGrant", struct {
		Grant string `json:"grant"`
		Key   string `json:"key"`
	}{grant, key}, &out, true)
	return out, err
}
