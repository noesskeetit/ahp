//go:build ahp_data

package main

import (
	"context"
	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
	"github.com/ahp-platform/ahp/pkg/transport"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"net/url"
	"strconv"
	"time"
)

func configureArtifactTransfer(store *artifacts.Store, ids *identity.Store) error {
	u, err := url.Parse(envOr("AHP_S3_ENDPOINT", ""))
	if err != nil || u.Host == "" {
		return contracts.NewError(contracts.ErrInvalid, "transfer requires S3 endpoint")
	}
	copier, err := artifacts.NewTransferCopier(u.Host, minio.Options{Creds: credentials.NewStaticV4(envOr("AHP_MINIO_ROOT_USER", ""), envOr("AHP_MINIO_ROOT_PASSWORD", ""), ""), Secure: u.Scheme == "https", Region: envOr("AHP_S3_REGION", "")})
	if err != nil {
		return err
	}
	maxBytes, err := strconv.ParseInt(envOr("AHP_CHECKPOINT_TRANSFER_MAX_BYTES", "67108864"), 10, 64)
	if err != nil || maxBytes <= 0 || maxBytes > artifacts.MaxSize {
		return contracts.NewError(contracts.ErrInvalid, "transfer byte bound must be within 1..67108864")
	}
	timeout, err := time.ParseDuration(envOr("AHP_CHECKPOINT_TRANSFER_TIMEOUT", "60s"))
	if err != nil || timeout <= 0 || timeout > time.Minute {
		return contracts.NewError(contracts.ErrInvalid, "transfer timeout must be positive and at most 60s")
	}
	artifacts.WithTransferAuthority(identityTransferAuthority{ids}, artifacts.TransferLimits{MaxBytes: maxBytes, Timeout: timeout})(store)
	artifacts.WithTransferCopier(copier)(store)
	return nil
}

// identityTransferAuthority uses Identity's public owner methods; no other
// service queries grant tables or turns a stale ticket into current authority.
type identityTransferAuthority struct{ store *identity.Store }

func (a identityTransferAuthority) Evaluate(ctx context.Context, in artifacts.TransferIdentity) (context.Context, artifacts.TransferIdentity, error) {
	g, err := a.store.Evaluate(ctx, in.Scope, in.Binding.TaskID, "artifacts")
	if err != nil {
		return ctx, artifacts.TransferIdentity{}, err
	}
	if in.GrantID == "" || in.GrantID != g.ID || in.Binding.ContextID != g.Context || (in.Epoch != 0 && in.Epoch != g.Epoch) {
		return ctx, artifacts.TransferIdentity{}, contracts.NewError(contracts.ErrForbidden, "checkpoint transfer grant binding changed")
	}
	in.Epoch = g.Epoch
	in.Labels = g.Labels
	in.ExpiresAt = g.ExpiresAt
	claims := auth.Claims{Kind: auth.KindTask, TokenUse: auth.TokenUse, Tenant: g.Tenant, Deployment: g.Deployment, Subject: g.Subject, Task: g.Task, Context: g.Context, Grant: g.ID, Root: g.Root, Actors: g.Actors, Depth: g.Depth}
	// Evaluate proves the live grant; Authorize additionally applies current
	// resource IAM. Reconstruct only from the owner-returned grant, never from
	// caller-supplied claims or a token stored inside the transfer receipt.
	if err := a.store.Authorize(ctx, claims, "artifacts"); err != nil {
		return ctx, artifacts.TransferIdentity{}, err
	}
	ctx = transport.WithPrincipal(ctx, transport.Principal{Scope: in.Scope, Claims: claims})
	return artifacts.WithCallerContext(ctx, g.Context), in, nil
}
func (a identityTransferAuthority) Accumulate(ctx context.Context, in artifacts.TransferIdentity, set labels.Set) error {
	if _, _, err := a.Evaluate(ctx, in); err != nil {
		return err
	}
	_, err := a.store.AddLabels(ctx, in.Scope, in.Binding.TaskID, set)
	return err
}
