//go:build ahp_data

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/artifactsapi"
	"github.com/ahp-platform/ahp/internal/atrest/objectcipher"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/workspace"
	"github.com/ahp-platform/ahp/internal/workspace/workspaceapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	aot "github.com/ahp-platform/ahp/pkg/otel"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// pendingIndexStore is the one fact about the artifact migration this mount
// needs to report. *artifacts.Store satisfies it.
type pendingIndexStore interface{ IdempotencyIndexPending() bool }

// warnIdempotencyIndexPending makes a data plane that came up WITHOUT its
// durable idempotency index visible at start, and returns whether that was the
// case.
//
// The index can only be pending when duplicate rows produced before the
// constraint exist: Migrate stays green so the planes mount instead of taking the
// front down (DEF-STAND-1), and upload idempotency is held by the per-key
// advisory lock in the meantime. Serving from that state is safe; not knowing it
// is not, because a green start would otherwise read as "index installed". The
// index returns on the next start after the duplicates are read out.
func warnIdempotencyIndexPending(log *slog.Logger, store pendingIndexStore) bool {
	if store == nil || !store.IdempotencyIndexPending() {
		return false
	}
	if log != nil {
		log.Warn("artifacts idempotency index pending: pre-constraint duplicate rows exist; "+
			"upload idempotency is held by the per-key advisory lock; the index returns on the "+
			"next start after the duplicates are read out",
			"guarantee", "advisory_lock", "restores_on", "restart_after_duplicate_readout")
	}
	return true
}

// serveData mounts the data plane on its own address: the artifact store + API
// AND the workspace store + API (project file context). Authorization is not
// permissive: each HTTP face demands a task token bound to the mTLS workload
// and its own capability ("artifacts" / "workspace"); the artifact store's
// authorizer allowlists the actions the artifact tier performs, and the
// workspace store re-checks membership and the owner/editor/viewer tier on
// every call. Gated behind the ahp_data build tag so the default build and every
// other role compile without internal/artifacts, internal/workspace or minio-go.
func (s *server) serveData(ctx context.Context, addr string) error {
	artifactsHandler, workspaceHandler, err := s.buildDataHandler(ctx)
	if err != nil {
		return err
	}
	mux := s.baseMux()
	mountDataHandlers(mux, artifactsHandler, workspaceHandler)
	return s.runServer(ctx, "data", addr, aot.HTTPMiddleware("data")(mux))
}

// tryMountData co-locates the data plane (artifact + workspace faces) on a
// shared mux for the "all" role.
func (s *server) tryMountData(ctx context.Context, mux *http.ServeMux) error {
	artifactsHandler, workspaceHandler, err := s.buildDataHandler(ctx)
	if err != nil {
		return err
	}
	mountDataHandlers(mux, artifactsHandler, workspaceHandler)
	return nil
}

// mountDataHandlers registers the data-plane faces on a mux. It is separated
// from construction so the prefix routing (the thing that makes
// /v1/workspace/... reach the workspace package instead of falling through to
// the SPA's "route not found") is exercised directly by an offline test that
// never touches the database or the object store.
func mountDataHandlers(mux *http.ServeMux, artifactsHandler, workspaceHandler http.Handler) {
	mux.Handle("/v1/artifacts/", artifactsHandler)
	mux.Handle("/v1/workspace/", workspaceHandler)
}

// buildDataHandler constructs both planes, runs their idempotent migrations,
// installs the artifact deletion cascade, and returns the two HTTP faces.
func (s *server) buildDataHandler(ctx context.Context) (http.Handler, http.Handler, error) {
	// One authority over the durable planes, mounted twice on purpose: the sweep
	// asks it before collecting (W-1), and the deletion cascade asks it before
	// tombstoning (DEF-H24-15). Same object, same queries, one place to audit.
	// State writers can occupy every slot while waiting for an artifact pin.
	// Owner writes and read-only authority queries therefore have bounded,
	// independent pools, avoiding a State->Artifacts->State pool wait cycle.
	dataPool, readPool, closePools, err := checkpointOwnerPools(ctx, s.pool)
	if err != nil {
		return nil, nil, err
	}
	keepPools := false
	defer func() {
		if !keepPools {
			closePools()
		}
	}()
	liveness := newStateLiveness(readPool)
	reader := state.New(readPool)
	ids := s.identityStoreFor(readPool)
	dataOwner := &server{pool: dataPool, logger: s.logger, iam: s.iam}
	artStore, wsStore, verifier, err := dataOwner.newDataPlanes(ctx, artifacts.WithReferenceKeeper(liveness), artifacts.WithGrantLabels(dataArtifactGrantLabels(ids)), artifacts.WithCheckpointSettlement(reader.CheckpointOperationSettled))
	if err != nil {
		return nil, nil, err
	}
	if err := configureArtifactTransfer(artStore, ids); err != nil {
		return nil, nil, err
	}
	if err := artStore.Migrate(ctx); err != nil {
		return nil, nil, err
	}
	// A green migration can still leave the durable idempotency index uninstalled
	// when pre-constraint duplicate rows exist. Say so once at start rather than
	// let the mount read as fully guaranteed.
	warnIdempotencyIndexPending(s.logger, artStore)
	if err := wsStore.Migrate(ctx); err != nil {
		return nil, nil, err
	}
	// Mounting the data plane is what makes the deletion cascade possible in
	// this process: State's DeleteThread/DeleteSubject then tombstone the deleted
	// context's artifacts through this same store, in the artifact plane's own
	// semantics (tombstone, legal hold, idempotency).
	s.cascade = artifactStoreCascade{store: artStore}
	if s.stateOwner != nil {
		s.stateOwner.SetCheckpointArtifacts(checkpointArtifactOwner{Store: artStore, identity: ids})
	}
	edges, audience := workspaceUserEdgeConfig()
	if len(edges) > 0 && s.logger != nil {
		s.logger.Info("workspace face will verify user credentials",
			"user_audience", audience, "sanctioned_edges", strings.Join(edges, ","))
	}
	artHandler, wsHandler, err := dataHandlersWithPolicy(artStore, wsStore, verifier, edges, audience, dataLiveAuthority(ids),
		artifacts.WithLiveness(liveness),
		artifacts.WithReferenceScan(liveness))
	if err != nil {
		return nil, nil, err
	}
	s.artifactExportOwner = &dataExportOwner{store: artStore, authorize: ids.Authorize}
	keepPools = true
	context.AfterFunc(ctx, closePools)
	return artHandler, wsHandler, nil
}

// newDataPlanes constructs the artifact and workspace stores plus the identity
// verifier, WITHOUT running migrations. Splitting construction from migration
// lets an offline test build and mount the real handlers while the database and
// object store stay unreachable (construction alone opens no connection: pgx
// connects lazily and minio dials only on object I/O).
func (s *server) newDataPlanes(ctx context.Context, opts ...artifacts.Option) (*artifacts.Store, *workspace.Store, *auth.Verifier, error) {
	raw := envOr("AHP_S3_ENDPOINT", "")
	if raw == "" {
		return nil, nil, nil, errors.New("ahp: data role requires AHP_S3_ENDPOINT")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return nil, nil, nil, fmt.Errorf("ahp: bad AHP_S3_ENDPOINT")
	}
	client, err := minio.New(u.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(envOr("AHP_MINIO_ROOT_USER", ""), envOr("AHP_MINIO_ROOT_PASSWORD", ""), ""),
		Secure: u.Scheme == "https",
		// The region is pinned from the environment instead of being discovered
		// with a GetBucketLocation round trip. Without a pin every presign first
		// dials the object store, so an S3 endpoint that is configured but not
		// yet answering would make even an offline-legal presign fail; with the
		// pin the signature is computed locally and only real object I/O needs
		// the store. Empty keeps the default (discovery) behaviour.
		Region: envOr("AHP_S3_REGION", ""),
	})
	if err != nil {
		return nil, nil, nil, err
	}
	bucket := envOr("AHP_S3_BUCKET", "")
	if bucket == "" {
		return nil, nil, nil, errors.New("ahp: data role requires AHP_S3_BUCKET")
	}

	// Fine-grained authorization inside the artifact tier: every object operation
	// is admitted per scope and per action. Only the actions the artifact tier
	// itself performs are allowed; anything else (including a forged action name)
	// is denied before it can reach an object.
	//
	// "pin" and "restore" are admitted because the durable-retention plane is
	// something a workload owns, not something it may delegate: both actions are
	// bound to the VERIFIED scope inside the store (a pin row is written under
	// scope.Tenant and a restore re-opens only an artifact the caller's tenant,
	// subject and label ceiling already admit), so admitting them adds no reach
	// beyond what "delete" and "retain" already give the same credential. What
	// stays refused is the sweep itself: "gc" is deliberately absent, because a
	// request handler must never be able to ask this store to collect bytes
	// (cmd/ahp/mount_gc.go runs the collector on its own single-action store).
	artifactActions := map[string]bool{
		"upload": true, "seal": true, "read": true, "delete": true, "publish": true,
		"derive": true, "retain": true, "hold": true, "purge": true,
		"pin": true, "restore": true,
		// A conversation may admit one more of the SAME subject's conversations
		// to read its own artifacts (GrantContext). The store requires the
		// caller to be that conversation, ownership, untainted labels and an
		// idempotency key; the artifact's binding context is never rewritten.
		"grant": true, "revoke": true,
	}
	artAuthorize := func(_ context.Context, scope contracts.Scope, action, _ string) error {
		if err := scope.Validate(); err != nil {
			return err
		}
		if !artifactActions[action] {
			return contracts.NewError(contracts.ErrForbidden,
				"ahp: artifact action "+action+" is not permitted")
		}
		return nil
	}

	// The workspace store owns membership; the artifact store has none, so a read
	// of a workspace-restricted artifact is admitted only through the workspace
	// store's membership hook. A captured pointer breaks the construction cycle
	// (the artifact store needs workspace.IsMember; the workspace store needs the
	// artifact store).
	if publicEndpoint := envOr("AHP_S3_UPLOAD_ENDPOINT", ""); publicEndpoint != "" {
		publicURL, err := url.Parse(publicEndpoint)
		if err != nil || publicURL.Host == "" || publicURL.User != nil || publicURL.RawQuery != "" || publicURL.Fragment != "" || (publicURL.Path != "" && publicURL.Path != "/") || (publicURL.Scheme != "http" && publicURL.Scheme != "https") {
			return nil, nil, nil, errors.New("ahp: invalid upload endpoint")
		}
		publicClient, err := minio.New(publicURL.Host, &minio.Options{Creds: credentials.NewStaticV4(envOr("AHP_MINIO_ROOT_USER", ""), envOr("AHP_MINIO_ROOT_PASSWORD", ""), ""), Secure: publicURL.Scheme == "https", Region: envOr("AHP_S3_REGION", "us-east-1")})
		if err != nil {
			return nil, nil, nil, err
		}
		opts = append(opts, artifacts.WithUploadClient(publicClient))
	}
	var wsStore *workspace.Store
	if parserPath := envOr("AHP_ARTIFACT_PARSER", ""); parserPath != "" {
		opts = append(opts, artifacts.WithParserCommand(artifacts.ParserCommand{Path: parserPath}))
	}
	artCipher, cipherErr := objectcipher.FromEnv(ctx, client)
	if cipherErr != nil {
		return nil, nil, nil, cipherErr
	}
	if artCipher != nil {
		opts = append(opts, artifacts.WithObjectCipher(artCipher))
	}
	artStore, err := artifacts.New(s.pool, client, bucket, artAuthorize,
		append([]artifacts.Option{artifacts.WithWorkspaceAccess(func(ctx context.Context, scope contracts.Scope, w string) error {
			if wsStore == nil {
				return contracts.NewError(contracts.ErrForbidden,
					"ahp: workspace membership check is not ready")
			}
			return wsStore.IsMember(ctx, scope, w)
		})}, opts...)...)
	if err != nil {
		return nil, nil, nil, err
	}

	// Fine-grained per-call authorize seam inside the workspace tier. It is a
	// coarse allowlist of the actions the workspace store itself performs; the
	// owner/editor/viewer decision is made by the store against the persisted
	// members.role, so the two layers compose and no action reaches the store
	// unexamined.
	workspaceActions := map[string]bool{
		"create": true, "read": true, "write": true, "purge": true,
		"candidate:create": true, "candidate:list": true, "candidate:write": true,
		"candidate:check": true, "candidate:reject": true, "publish": true,
		"member:add": true, "member:remove": true, "member:list": true,
	}
	wsStore, err = workspace.New(s.pool, artStore,
		func(_ context.Context, scope contracts.Scope, action, _ string) error {
			if err := scope.Validate(); err != nil {
				return err
			}
			if !workspaceActions[action] {
				return contracts.NewError(contracts.ErrForbidden,
					"ahp: workspace action "+action+" is not permitted")
			}
			return nil
		})
	if err != nil {
		return nil, nil, nil, err
	}

	verifier, _ := devIdentityAuth()
	s.attachWorkspaceIAM(wsStore)
	return artStore, wsStore, verifier, nil
}

// dataHandlers builds the two HTTP faces from already-constructed stores. The
// coarse gate at each face mirrors the artifact plane: a task token (never a
// human or service token), the plane's own capability, and a complete
// tenant/deployment/subject scope. The transport has already pinned the token to
// the mTLS certificate workload; the fine membership/rights decision lives in
// the stores.
func dataHandlers(artStore *artifacts.Store, wsStore *workspace.Store, verifier *auth.Verifier,
	gcOpts ...artifacts.GCOption) (http.Handler, http.Handler, error) {
	return dataHandlersWithUserEdges(artStore, wsStore, verifier, nil, "", gcOpts...)
}

// workspaceUserEdgeConfig reads the one decision that lets a data plane verify
// end-user credentials: which deployments are allowed to carry them, plus the
// audience identity puts on the credential a browser holds. Unset, or set to
// anything that resolves to no deployment at all, leaves the workspace face
// exactly as it has always been -- task tokens only -- so a plane that nobody
// configured cannot accidentally start trusting a forwarded bearer.
func workspaceUserEdgeConfig() (edges []string, audience string) {
	audience = envOr("AHP_WORKSPACE_USER_AUDIENCE", "ahp-browser")
	for _, raw := range strings.Split(envOr("AHP_WORKSPACE_USER_EDGE_DEPLOYMENTS", ""), ",") {
		if dep := strings.TrimSpace(raw); dep != "" {
			edges = append(edges, dep)
		}
	}
	if len(edges) == 0 {
		return nil, ""
	}
	return edges, audience
}

// dataHandlersWithUserEdges is dataHandlers with the browser route the director
// ruled at 20261101T154234Z item 2 wired in: the user edge forwards the project
// operations with the user's own bearer, so THIS plane verifies it. edges names
// the deployments sanctioned to carry a user credential, and the check runs on
// the deployment the transport proved from the client certificate -- never from
// a header and never from the token, because a browser credential carries no
// deployment at all.
func dataHandlersWithUserEdges(artStore *artifacts.Store, wsStore *workspace.Store, verifier *auth.Verifier,
	edges []string, userAudience string, gcOpts ...artifacts.GCOption) (http.Handler, http.Handler, error) {
	return dataHandlersWithPolicy(artStore, wsStore, verifier, edges, userAudience, nil, gcOpts...)
}

func dataHandlersWithPolicy(artStore *artifacts.Store, wsStore *workspace.Store, verifier *auth.Verifier,
	edges []string, userAudience string, live transport.Authorize, gcOpts ...artifacts.GCOption) (http.Handler, http.Handler, error) {
	artifactsAllow := func(ctx context.Context, claims auth.Claims, got string) error {
		if got != "artifacts" {
			return contracts.NewError(contracts.ErrForbidden,
				"ahp: unexpected capability "+got)
		}
		if claims.Kind != auth.KindTask {
			return contracts.NewError(contracts.ErrForbidden,
				"ahp: a task token is required for the artifact plane")
		}
		if live != nil {
			return live(ctx, claims, got)
		}
		return claims.Scope().Validate()
	}
	// The sweep this facade carries asks the durable planes before it removes
	// anything: without a probe only registered pins protect a live reference,
	// which is the fail-closed default and not a production answer.
	artifactsHandler, err := artifactsapi.New(artStore, verifier, artifactsAllow, gcOpts...)
	if err != nil {
		return nil, nil, err
	}

	userEdges := func(deployment string) bool {
		for _, edge := range edges {
			if edge == deployment {
				return true
			}
		}
		return false
	}
	workspaceAllow := func(ctx context.Context, claims auth.Claims, got string) error {
		if got != "workspace" {
			return contracts.NewError(contracts.ErrForbidden,
				"ahp: unexpected capability "+got)
		}
		switch claims.Kind {
		case auth.KindTask:
		case auth.KindHuman:
			// A user credential is accepted only on a plane that was configured to
			// verify it, and only from a workload named as a user edge. The edge
			// gets no say over whose files it reads: the subject comes from the
			// credential, and the store re-checks membership and tier per call.
			if len(edges) == 0 {
				return contracts.NewError(contracts.ErrForbidden,
					"ahp: the workspace plane does not accept user credentials on this deployment")
			}
			if !userEdges(claims.Deployment) {
				return contracts.NewError(contracts.ErrForbidden,
					"ahp: this workload is not a sanctioned user edge")
			}
		default:
			return contracts.NewError(contracts.ErrForbidden,
				"ahp: a task token is required for the workspace plane")
		}
		if live != nil {
			return live(ctx, claims, got)
		}
		return claims.Scope().Validate()
	}
	var workspaceHandler http.Handler
	if userAudience != "" {
		workspaceHandler, err = workspaceapi.NewAcceptingUsers(wsStore, verifier, workspaceAllow, userAudience)
	} else {
		workspaceHandler, err = workspaceapi.New(wsStore, verifier, workspaceAllow)
	}
	if err != nil {
		return nil, nil, err
	}
	return artifactsHandler, workspaceHandler, nil
}

// artifactStoreCascade is the in-process half of the deletion cascade: the
// composed topology runs State and the artifact plane in one process, so the
// composition root reaches the artifact plane through its own store and never
// through State's schema.
type artifactStoreCascade struct {
	store *artifacts.Store
}

func (c artifactStoreCascade) cascadeThread(ctx context.Context, scope contracts.Scope,
	contextIDs, taskIDs []string) (cascadeCounters, error) {
	res, err := c.store.DeleteByThread(ctx, scope, contextIDs, taskIDs)
	return cascadeCounters{
		Tombstoned:     res.Tombstoned,
		Held:           res.Held,
		Referenced:     res.Referenced,
		AlreadyDeleted: res.AlreadyDeleted,
	}, err
}

func (c artifactStoreCascade) cascadeSubject(ctx context.Context, scope contracts.Scope) (cascadeCounters, error) {
	res, err := c.store.DeleteBySubject(ctx, scope)
	return cascadeCounters{
		Tombstoned:     res.Tombstoned,
		Held:           res.Held,
		Referenced:     res.Referenced,
		AlreadyDeleted: res.AlreadyDeleted,
	}, err
}

func (c artifactStoreCascade) subjectScopes(ctx context.Context, scope contracts.Scope) ([]contracts.Scope, error) {
	return c.store.SubjectScopes(ctx, scope)
}

func (c artifactStoreCascade) fenceSubject(ctx context.Context, scope contracts.Scope) error {
	return c.store.FenceSubject(ctx, scope)
}

func (c artifactStoreCascade) fenceTenant(ctx context.Context, scope contracts.Scope) error {
	return c.store.FenceTenant(ctx, scope)
}
func (c artifactStoreCascade) tenantScopes(ctx context.Context, scope contracts.Scope) ([]contracts.Scope, error) {
	return c.store.TenantScopes(ctx, scope)
}
