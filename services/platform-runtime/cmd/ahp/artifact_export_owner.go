//go:build ahp_data

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
	artifactsdk "github.com/ahp-platform/ahp/sdk/go/artifacts"
)

// dataExportOwner reuses the mounted Artifacts store and its bounded pool.
// Its only caller is Compute carrying the middleware-verified task principal.
type dataExportOwner struct {
	store     *artifacts.Store
	authorize transport.Authorize
}

func (o *dataExportOwner) principal(ctx context.Context) (context.Context, transport.Principal, error) {
	p, ok := transport.FromContext(ctx)
	if !ok || p.Claims.Kind != auth.KindTask || p.Claims.Scope() != p.Scope || p.Claims.Task == "" || p.Claims.Context == "" || p.Claims.ExpiresAt <= time.Now().Unix() || o.authorize == nil {
		return ctx, p, contracts.NewError(contracts.ErrForbidden, "current verified export task required")
	}
	if err := o.authorize(ctx, p.Claims, "artifacts"); err != nil {
		return ctx, p, err
	}
	return artifacts.WithCallerContext(ctx, p.Claims.Context), p, nil
}

func (o *dataExportOwner) BeginUpload(ctx context.Context, in artifactsdk.UploadRequest) (artifactsdk.Upload, error) {
	ctx, p, err := o.principal(ctx)
	if err != nil {
		return artifactsdk.Upload{}, err
	}
	up, err := o.store.BeginUploadWithBinding(ctx, p.Scope, artifacts.Binding{TaskID: p.Claims.Task, ContextID: p.Claims.Context}, in.Key, artifacts.UploadRequest{Name: in.Name, MIME: in.MIME, SHA256: in.SHA256, Size: in.Size, Labels: in.Labels})
	return artifactsdk.Upload{ID: up.ID, PresignedURL: up.PresignedURL, Object: up.Object, ExpectedSHA: up.ExpectedSHA, ExpectedSize: up.ExpectedSize, Headers: up.Headers, ExpiresAt: up.ExpiresAt}, err
}

func (o *dataExportOwner) Upload(ctx context.Context, up artifactsdk.Upload, data []byte) error {
	ctx, _, err := o.principal(ctx)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	u, err := url.Parse(up.PresignedURL)
	if err != nil || u.Host == "" || u.User != nil || (u.Scheme != "https" && u.Scheme != "http") || int64(len(data)) != up.ExpectedSize || len(data) == 0 || len(data) > artifacts.MaxSize || hex.EncodeToString(sum[:]) != up.ExpectedSHA {
		return contracts.NewError(contracts.ErrInvalid, "invalid owner-issued upload")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, up.PresignedURL, bytes.NewReader(data))
	if err != nil {
		return err
	}
	for key, value := range up.Headers {
		switch strings.ToLower(key) {
		case "authorization", "cookie", "host", "proxy-authorization":
			return contracts.NewError(contracts.ErrInvalid, "credential header in blob upload")
		case "content-length":
			if value != fmt.Sprint(len(data)) {
				return contracts.NewError(contracts.ErrInvalid, "upload size mismatch")
			}
		default:
			req.Header.Set(key, value)
		}
	}
	// Presigned capability only: no bearer, client certificate, cookie jar or redirect.
	hc := &http.Client{Timeout: 45 * time.Second, Transport: &http.Transport{Proxy: http.ProxyFromEnvironment}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer hc.CloseIdleConnections()
	resp, err := hc.Do(req)
	if err != nil {
		return contracts.NewError(contracts.ErrUnavailable, "artifact blob upload failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return contracts.NewError(contracts.ErrUnavailable, "artifact blob upload refused")
	}
	return nil
}

func (o *dataExportOwner) Seal(ctx context.Context, id string) (artifactsdk.ArtifactRef, error) {
	ctx, p, err := o.principal(ctx)
	if err != nil {
		return artifactsdk.ArtifactRef{}, err
	}
	ref, err := o.store.Seal(ctx, p.Scope, id)
	return publicExportRef(ref), err
}

func (o *dataExportOwner) Get(ctx context.Context, id string) (artifactsdk.ArtifactRef, error) {
	ctx, p, err := o.principal(ctx)
	if err != nil {
		return artifactsdk.ArtifactRef{}, err
	}
	ref, err := o.store.Get(ctx, p.Scope, id)
	return publicExportRef(ref), err
}

func publicExportRef(r artifacts.ArtifactRef) artifactsdk.ArtifactRef {
	return artifactsdk.ArtifactRef{ID: r.ID, Tenant: r.Tenant, Deployment: r.Deployment, Subject: r.Subject, ContextID: r.ContextID, TaskID: r.TaskID, Version: r.Version, SHA256: r.SHA256, Size: r.Size, Name: r.Name, MIME: r.MIME, Labels: r.Labels, EffectiveLabels: r.EffectiveLabels}
}
