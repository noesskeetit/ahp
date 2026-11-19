//go:build ahp_data

package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/memory"
	"github.com/ahp-platform/ahp/internal/memory/embed"
	"github.com/ahp-platform/ahp/internal/memory/memoryapi"
	"github.com/ahp-platform/ahp/internal/workspace"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// Hindsight is the default personal/project backend. The explicit legacy
// branch retains upstream embedding policy, live grant ceiling and read labels.
func (s *server) tryMountMemory(ctx context.Context, mux *http.ServeMux,
	verifier *auth.Verifier, authorize transport.Authorize, authorityMode string, grants GrantLabels) error {
	backend := envOr("AHP_MEMORY_BACKEND", "hindsight")
	if backend == "hindsight" {
		if envOr("AHP_ENABLE_SHARED_MEMORY", "0") == "1" {
			if authorityMode != authorityIdentityMode || grants == nil {
				return fmt.Errorf("supplemental Memory requires live Identity labels")
			}
			// Stage both mounts before publishing either route. Migrate the PDF
			// store first so the native erasure owner discovers these records.
			combined := http.NewServeMux()
			if err := s.tryMountPDFMemory(ctx, combined, verifier, authorize, authorityMode, grants, sharedMemoryPrefix); err != nil {
				return err
			}
			if err := s.tryMountHindsightMemory(ctx, combined, verifier, authorize, authorityMode, grants); err != nil {
				return err
			}
			mux.Handle("/v1/memory/", combined)
			return nil
		}
		return s.tryMountHindsightMemory(ctx, mux, verifier, authorize, authorityMode, grants)
	}
	if backend != "legacy" {
		return fmt.Errorf("unknown AHP_MEMORY_BACKEND")
	}
	return s.tryMountPDFMemory(ctx, mux, verifier, authorize, authorityMode, grants, "/v1/memory/")
}

const sharedMemoryPrefix = "/v1/memory/shared/"

// The supplemental owner keeps its own PDF records; it never reinterprets or
// broadens a native personal/private-project Hindsight bank.
func (s *server) tryMountPDFMemory(ctx context.Context, mux *http.ServeMux,
	verifier *auth.Verifier, authorize transport.Authorize, authorityMode string, grants GrantLabels, prefix string) error {
	artStore, wsStore, _, err := s.newDataPlanes(ctx)
	if err != nil {
		return fmt.Errorf("memory: %w", err)
	}
	// Idempotent migrations of the schemas this plane consults on every call:
	// artifacts (evidence verification) and workspace (live membership). Both
	// are the owning package's own Migrate, so the memory mount never creates
	// a table another package owns.
	if err := artStore.Migrate(ctx); err != nil {
		return fmt.Errorf("memory: artifacts migrate: %w", err)
	}
	if err := wsStore.Migrate(ctx); err != nil {
		return fmt.Errorf("memory: workspace migrate: %w", err)
	}
	// The live label ceiling (PDF: a record's visibility is never wider than the
	// writing task's ceiling) is a fact Identity owns: the accumulated label
	// journal of the context behind the presented task credential is read back
	// into grant.Labels on every check (internal/identity/journal.go: "the
	// journal row of a context is the single source of truth a grant's Labels is
	// read from"). When this front delegates the run credential to Identity the
	// mount wires that journal as the ceiling, and a grant that cannot be read
	// fails the write closed.
	//
	// In the dev authority mode there IS no live grant -- State mints its own
	// claims and nothing names a journal row -- so no honest ceiling exists and
	// none is faked: the plane mounts with the ceiling absent and says so in the
	// log, which is a declared gap rather than a silent one.
	var opts []memory.Option
	ceiling := "absent (dev authority: no live grant to read labels from)"
	if authorityMode == authorityIdentityMode && grants != nil {
		writer, ok := grants.(GrantLabelWriter)
		if !ok {
			return fmt.Errorf("memory: identity read-label sink is required")
		}
		opts = append(opts, memory.WithLabels(memoryCeilingFromGrants(grants)), memory.WithReadLabels(memoryReadLabelsFromGrants(writer)))
		ceiling = "identity context label journal (grant.Labels)"
	}
	embedOpts, embedderDesc, err := memoryEmbedderFromEnv()
	if err != nil {
		return fmt.Errorf("memory: %w", err)
	}
	if len(embedOpts) > 0 && (authorityMode != authorityIdentityMode || grants == nil) {
		return fmt.Errorf("memory: hybrid search requires live Identity labels and read-label sink")
	}
	opts = append(opts, embedOpts...)
	if prefix == sharedMemoryPrefix {
		// New shared scopes use the same canonical resource IDs as Workspace
		// and the Identity label journal, not the legacy display-name namespace.
		opts = append(opts, memory.WithWorkspaces(wsStore.ListWorkspaceIDs))
	}
	store, err := newMemoryStore(s.pool, artStore, wsStore, authorize, opts...)
	if err != nil {
		return fmt.Errorf("memory: %w", err)
	}
	if err := store.Migrate(ctx); err != nil {
		return fmt.Errorf("memory: migrate: %w", err)
	}
	handler, err := memoryapi.New(store, verifier, authorize)
	if err != nil {
		return fmt.Errorf("memory: handler: %w", err)
	}
	if prefix == "/v1/memory/" {
		s.cascade = withMemoryDeletion(s.cascade, store, wsStore)
		mountMemoryHandlers(mux, handler)
	} else {
		mux.Handle(prefix, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			copy := r.Clone(r.Context())
			copy.URL.Path = "/v1/memory/" + strings.TrimPrefix(r.URL.Path, prefix)
			copy.URL.RawPath = ""
			handler.ServeHTTP(w, copy)
		}))
	}
	s.logger.Info("memory plane mounted",
		"prefix", prefix,
		"workspaces", "workspace.Store.ListWorkspaces",
		"label_ceiling", ceiling,
		"embedder", embedderDesc)
	return nil
}

// memoryEmbedderFromEnv validates the operator's explicit search mode and
// binds the HTTP backend to its tenant/label policy before the face is mounted.
func memoryEmbedderFromEnv() ([]memory.Option, string, error) {
	mode := strings.TrimSpace(os.Getenv("AHP_MEMORY_SEARCH_MODE"))
	if mode != "" && mode != "hybrid" && mode != "lexical" {
		return nil, "", fmt.Errorf("memory: AHP_MEMORY_SEARCH_MODE must be hybrid or lexical")
	}
	if mode == "lexical" {
		if strings.TrimSpace(os.Getenv(embed.EnvBaseURL)) != "" {
			return nil, "", fmt.Errorf("memory: lexical mode conflicts with an embedding endpoint")
		}
		return nil, "absent (explicit lexical search only)", nil
	}
	backend, err := embed.FromEnv()
	if err != nil {
		return nil, "", err
	}
	if backend == nil {
		return nil, "", fmt.Errorf("memory: semantic search requires an embedding endpoint; set AHP_MEMORY_SEARCH_MODE=lexical explicitly for degraded operation")
	}
	if strings.TrimSpace(os.Getenv(embed.EnvRevision)) == "" {
		return nil, "", fmt.Errorf("memory: AHP_MEMORY_EMBEDDING_REVISION is required to version the configured embedding space")
	}
	policy, err := embed.PolicyFromEnv(backend)
	if err != nil {
		return nil, "", err
	}
	return []memory.Option{memory.WithEmbedding(backend), memory.WithEmbeddingPolicy(policy)}, backend.Describe(), nil
}

// newMemoryStore wires the Memory store with the composition's real
// collaborators. It is separated from the migration-running mount so an offline
// test can build and mount the real handler while the database and object store
// stay unreachable (pgx connects lazily, minio dials only on object I/O), the
// same split the data plane uses.
// The workspace-membership hook is installed here rather than passed in: it is
// what makes a workspace-scoped memory readable BY LIVE MEMBERSHIP, and a
// caller that forgot it would silently narrow the admissible set to nothing.
// Keeping it inside the constructor removes that footgun; callers add only
// optional collaborators (the label ceiling).
func newMemoryStore(pool *pgxpool.Pool, artStore *artifacts.Store,
	wsStore *workspace.Store, authorize transport.Authorize, opts ...memory.Option) (*memory.Store, error) {
	return memory.New(pool, artStore, memoryStoreAuthorize(authorize), nil,
		append([]memory.Option{memory.WithWorkspaces(wsStore.ListWorkspaces)}, opts...)...)
}

// memoryCeilingFromGrants turns that reader into the store's Ceiling hook.
//
// It is fail-closed by construction: the ceiling is only ever answered for the
// verified principal of the exact scope the store is writing for, and any
// failure to read the journal is returned as an error, which makes Propose
// refuse rather than store a record with no ceiling. An empty label set is a
// legitimate answer -- it means the journal accumulated nothing -- and is
// distinct from "could not read", which never relaxes a write.
func memoryCeilingFromGrants(grants GrantLabels) memory.Ceiling {
	return func(ctx context.Context, scope contracts.Scope) (labels.Set, error) {
		if err := scope.Validate(); err != nil {
			return labels.Set{}, err
		}
		p, ok := transport.FromContext(ctx)
		if !ok {
			return labels.Set{}, contracts.NewError(contracts.ErrForbidden,
				"ahp: memory ceiling requires a verified principal")
		}
		if p.Scope != scope {
			return labels.Set{}, contracts.NewError(contracts.ErrForbidden,
				"ahp: memory ceiling scope does not match the verified principal")
		}
		return grants.GrantLabelSet(ctx, scope, p.Claims.Task, memoryPlaneCapability)
	}
}

// mountMemoryHandlers registers the Memory face on a mux. Registration is its
// own function so the prefix routing -- the thing that decides whether
// /v1/memory/Read reaches the memory package or falls through to the SPA's
// "route not found" -- is exercised directly without a database.
func mountMemoryHandlers(mux *http.ServeMux, handler http.Handler) {
	mux.Handle("/v1/memory/", handler)
}

// memoryStoreActions is the closed set of actions the Memory package performs
// through its store-level authorize seam: the lifecycle calls in
// internal/memory/lifecycle.go plus nothing else. A forged or unexpected
// action name is refused before it reaches any capability lookup.
//
// It is DERIVED from memory.Vocabulary rather than restated here. The same list
// is what infra/memory/capability-roles.sql publishes and what provisioning
// validates; a literal list in this file was DEF-MEM-6's drift vector -- the
// code could gain an action no operator could name, or keep naming one the code
// had dropped.
var memoryStoreActions = func() map[string]bool {
	set := make(map[string]bool, len(memory.Vocabulary)-1)
	for _, c := range memory.Vocabulary {
		if c == memory.CapabilityGateway {
			continue // operation receipts use a separately checked exact resource
		}
		set[c] = true
	}
	return set
}()

// memoryStoreAuthorize adapts the front's transport.Authorize (which sees the
// verified claims) to the store's scope-shaped seam, so the fine checks the
// package delegates -- review capability, label narrowing, deletion, subject
// erasure -- are answered by the SAME authority that guards every other plane
// (Identity's live grant in a composed front) instead of a local allowlist that
// would grant the reviewer decision to any memory-capable token.
//
// It fails closed three times over: the action must belong to the closed set,
// the request must carry a verified principal, and that principal's scope must
// be exactly the scope the store is acting on -- so a store call that lost its
// caller can never borrow someone else's capabilities.
func memoryStoreAuthorize(authorize transport.Authorize) memory.Authorize {
	return func(ctx context.Context, scope contracts.Scope, action, resource string) error {
		if err := scope.Validate(); err != nil {
			return err
		}
		operationAuthority := action == memory.CapabilityGateway && resource == "memory:operation"
		if !memoryStoreActions[action] && !operationAuthority {
			return contracts.NewError(contracts.ErrForbidden,
				"ahp: memory action "+action+" is not permitted")
		}
		if permitsDeletion(ctx, scope) && action == memory.CapabilityDeleteSubject && (resource == "subject:"+scope.Subject || resource == "tenant:"+scope.Tenant) {
			return nil
		}
		if authorize == nil {
			return contracts.NewError(contracts.ErrForbidden,
				"ahp: memory plane has no authorization callback")
		}
		p, ok := transport.FromContext(ctx)
		if !ok {
			return contracts.NewError(contracts.ErrForbidden,
				"ahp: memory action requires a verified principal")
		}
		if p.Scope != scope {
			return contracts.NewError(contracts.ErrForbidden,
				"ahp: memory action scope does not match the verified principal")
		}
		return authorize(ctx, p.Claims, action)
	}
}

// memoryReadLabelsFromGrants is the trusted service-to-Identity edge. The
// caller supplies neither task identity nor labels; both come from server state.
func memoryReadLabelsFromGrants(grants GrantLabelWriter) memory.ReadLabels {
	return func(ctx context.Context, scope contracts.Scope, set labels.Set) error {
		p, ok := transport.FromContext(ctx)
		if !ok || p.Scope != scope {
			return contracts.NewError(contracts.ErrForbidden, "memory: read labels require a verified matching principal")
		}
		return grants.AddGrantLabels(ctx, scope, p.Claims.Task, set)
	}
}
