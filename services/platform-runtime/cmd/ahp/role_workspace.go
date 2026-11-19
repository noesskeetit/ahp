//go:build ahp_data

package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/atrest/objectcipher"
	"github.com/ahp-platform/ahp/internal/workspace"
	"github.com/ahp-platform/ahp/internal/workspace/workspaceapi"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// workspaceMountPrefix mirrors the route the Workspace API itself cuts off the
// request path (internal/workspace/workspaceapi/api.go:
// strings.CutPrefix(r.URL.Path, "/v1/workspace/")). It is a literal here for the
// same reason the compute prefix is one: the route table of the aggregate must be
// readable without importing the tier, and role_workspace_test.go pins this string
// against the tier source so the two can never drift apart silently.
const (
	workspaceMountPrefix = "/v1/workspace/"
	// envEnableWorkspace is the operator's opt-in. Unset means the Workspace face
	// is NOT mounted at all, so every composition that predates this file answers
	// exactly as before and no workspace/artifact migration is attempted by a
	// process that never asked for it.
	envEnableWorkspace = "AHP_ENABLE_WORKSPACE"
)

// workspaceRequested reports whether the operator asked for the Workspace face in
// this process. Only the literal "1" turns it on: a typo must not half-mount a
// face that accepts file uploads.
func workspaceRequested() bool { return envOr(envEnableWorkspace, "0") == "1" }

// buildWorkspaceHandler wires the Workspace API for in-process mounting.
//
// Why this exists: the whole measured user chain is "upload a file into the
// project -> run a turn over it -> download the artifact" (PDF rev 3.2, and
// root's six-block order section 5 names upload as the second integration gap).
// The Workspace store, its HTTP facade and its integrity checks all exist and
// pass against real PostgreSQL + object storage - but until this file the ONLY
// thing that ever mounted workspaceapi was the standalone cmd/compute-host
// binary. The live aggregate therefore answered /v1/workspace/ from the SPA
// fallback: the upload leg did not exist on the user origin at all.
//
// Object storage is not fabricated: with no AHP_S3_ENDPOINT the mount fails
// closed with a named reason instead of mounting a face whose writes would land
// nowhere. The artifact store is built the same way the data plane builds its
// own, and the workspace membership check is wired INTO it, so an object of one
// workspace cannot be reached through another workspace's membership.
func (s *server) buildWorkspaceHandler(ctx context.Context) (http.Handler, error) {
	if s.pool == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "ahp: the workspace plane needs a database pool")
	}
	raw := envOr("AHP_S3_ENDPOINT", "")
	if raw == "" {
		return nil, errors.New("ahp: the workspace plane needs AHP_S3_ENDPOINT (an object store), refusing to mount an upload face that cannot store bytes")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("ahp: bad AHP_S3_ENDPOINT: %w", err)
	}
	client, err := minio.New(u.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(envOr("AHP_MINIO_ROOT_USER", ""), envOr("AHP_MINIO_ROOT_PASSWORD", ""), ""),
		Secure: u.Scheme == "https",
		Region: envOr("AHP_S3_REGION", ""),
	})
	if err != nil {
		return nil, err
	}
	bucket := envOr("AHP_S3_WORKSPACE_BUCKET", "")
	if bucket == "" {
		bucket = envOr("AHP_S3_BUCKET", "")
	}
	if bucket == "" {
		return nil, errors.New("ahp: the workspace plane needs AHP_S3_BUCKET (or AHP_S3_WORKSPACE_BUCKET)")
	}
	// Actions the workspace tier itself performs on objects. Anything else,
	// including a forged action name, is denied before it reaches an object.
	allowedActions := map[string]bool{
		"upload": true, "seal": true, "read": true, "delete": true,
		"publish": true, "derive": true, "retain": true, "hold": true, "purge": true,
	}
	allow := func(_ context.Context, scope contracts.Scope, action, _ string) error {
		if err := scope.Validate(); err != nil {
			return err
		}
		if !allowedActions[action] {
			return contracts.NewError(contracts.ErrForbidden, "ahp: workspace artifact action "+action+" is not permitted")
		}
		return nil
	}
	var wsRef *workspace.Store
	artCipher, cipherErr := objectcipher.FromEnv(ctx, client)
	if cipherErr != nil {
		return nil, fmt.Errorf("ahp: workspace at-rest: %w", cipherErr)
	}
	art, err := artifacts.New(s.pool, client, bucket, allow,
		artifacts.WithObjectCipher(artCipher),
		artifacts.WithWorkspaceAccess(func(ctx context.Context, scope contracts.Scope, ws string) error {
			return wsRef.IsMember(ctx, scope, ws)
		}))
	if err != nil {
		return nil, fmt.Errorf("ahp: workspace artifact store: %w", err)
	}
	if err := art.Migrate(ctx); err != nil {
		return nil, fmt.Errorf("ahp: workspace artifacts migrate: %w", err)
	}
	ws, err := workspace.New(s.pool, art, allow)
	if err != nil {
		return nil, fmt.Errorf("ahp: workspace store: %w", err)
	}
	wsRef = ws
	s.attachWorkspaceIAM(ws)
	if err := ws.Migrate(ctx); err != nil {
		return nil, fmt.Errorf("ahp: workspace migrate: %w", err)
	}

	verifier, _ := devIdentityAuth()
	authorize := dataLiveAuthority(s.identityStore())

	handler, err := workspaceapi.New(ws, verifier, authorize)
	if err != nil {
		return nil, err
	}
	s.logger.Info("workspace plane mounted", "prefix", workspaceMountPrefix, "bucket", bucket)
	return handler, nil
}

// mountWorkspacePlane routes the Workspace API onto mux.
func mountWorkspacePlane(mux *http.ServeMux, handler http.Handler) {
	mux.Handle(workspaceMountPrefix, handler)
}

// tryMountWorkspace co-locates the Workspace face on a shared mux. It is a no-op
// (not an error) when the operator did not ask for it, so the "all" role stays
// exactly what it was for every composition that predates this file.
func (s *server) tryMountWorkspace(ctx context.Context, mux *http.ServeMux) error {
	if !workspaceRequested() {
		return nil
	}
	handler, err := s.buildWorkspaceHandler(ctx)
	if err != nil {
		return err
	}
	mountWorkspacePlane(mux, handler)
	return nil
}
