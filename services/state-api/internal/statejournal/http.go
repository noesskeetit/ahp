package statejournal

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// NewHTTP mounts the journal operations implemented by the compact store.
func NewHTTP(store *Store, verifier *auth.Verifier, authorize transport.Authorize) (http.Handler, error) {
	return newHTTP(store, verifier, authorize, nil, false)
}

// NewHTTPWithToolLeases issues proof of lease ownership for the fenced Gateway.
// Use a signing key dedicated to the State API rather than the task issuer.
func NewHTTPWithToolLeases(store *Store, verifier *auth.Verifier, authorize transport.Authorize, signer *auth.Signer) (http.Handler, error) {
	if signer == nil {
		return nil, invalidHTTP("journal tool leases require a signing key")
	}
	return newHTTP(store, verifier, authorize, signer, false)
}

// NewHTTPWithDotsFIFO explicitly enables the additional task-scoped Dots
// admission and claim routes. An optional signer issues fenced Tool Gateway
// proof tied to the same verified task that owns the claimed run.
func NewHTTPWithDotsFIFO(store *Store, verifier *auth.Verifier, authorize transport.Authorize, signer *auth.Signer) (http.Handler, error) {
	return newHTTP(store, verifier, authorize, signer, true)
}

func newHTTP(store *Store, verifier *auth.Verifier, authorize transport.Authorize, signer *auth.Signer, dotsFIFO bool) (http.Handler, error) {
	if store == nil || verifier == nil || authorize == nil {
		return nil, invalidHTTP("journal handler requires storage, verification and authorization")
	}
	leaseProof := func(caller transport.Principal, session string, lease Lease) (string, error) {
		if signer == nil {
			return "", nil
		}
		return signer.SignToolLease(caller.Claims, session, lease.Epoch, lease.ExpiresAt)
	}
	leaseResponse := func(caller transport.Principal, session string, lease Lease, control *journal.TurnControlDelta, err error) (any, error) {
		if err != nil {
			return nil, err
		}
		proof, err := leaseProof(caller, session, lease)
		if err != nil {
			return nil, err
		}
		return struct {
			Lease
			Control *journal.TurnControlDelta `json:"control,omitempty"`
			Proof   string                    `json:"tool_lease_proof,omitempty"`
		}{lease, control, proof}, nil
	}
	mux := http.NewServeMux()
	if store.s3 != nil {
		mux.HandleFunc("POST /v1/state/sessions/{session}/blobs:stage", jsonRouteLimit(6<<20, func(r *http.Request, caller transport.Principal, req stageBlobRequest) (any, error) {
			return store.StageBlob(r.Context(), caller.Scope, r.PathValue("session"), authorizedLease(caller, req.Epoch), req.Data)
		}))
		mux.HandleFunc("POST /v1/state/sessions/{session}/blobs:read", jsonRoute(func(r *http.Request, caller transport.Principal, req BlobRef) (any, error) {
			data, err := store.ReadBlob(r.Context(), caller.Scope, r.PathValue("session"), req)
			return blobReadResponse{Data: data}, err
		}))
	}
	mux.HandleFunc("POST /v1/state/sessions:enqueue", jsonRoute(func(r *http.Request, caller transport.Principal, req EnqueueRequest) (any, error) {
		if req.ExecutionMode != "" || req.DeliveryMode != "" || req.TargetTurnSeq != 0 {
			if err := authorize(r.Context(), caller.Claims, "state.coordinate"); err != nil {
				return nil, err
			}
		}
		return store.EnqueueTurn(r.Context(), caller.Scope, req)
	}))
	mux.HandleFunc("POST /v1/state/sessions/{session}/vm:claim", jsonRoute(func(r *http.Request, caller transport.Principal, req journal.VMTurnClaimRequest) (any, error) {
		if err := authorize(r.Context(), caller.Claims, "state.vm_claim"); err != nil {
			return nil, err
		}
		if caller.Claims.Task != req.ExecutionID {
			return nil, contracts.NewError(contracts.ErrForbidden, "VM execution does not match authenticated task")
		}
		turn, err := store.ClaimVMTurn(r.Context(), caller.Scope, r.PathValue("session"), req, duration(req.TTLMilliseconds))
		if err != nil {
			return nil, err
		}
		proof, err := leaseProof(caller, turn.SessionID, turn.Lease)
		return struct {
			TurnLease
			Proof string `json:"tool_lease_proof,omitempty"`
		}{turn, proof}, err
	}))
	mux.HandleFunc("POST /v1/state/sessions/{session}/vm:requeue-fenced", jsonRoute(func(r *http.Request, caller transport.Principal, req journal.FenceRequeueRequest) (any, error) {
		if err := authorize(r.Context(), caller.Claims, "state.coordinate"); err != nil {
			return nil, err
		}
		return struct{}{}, store.RequeueFencedVMTurn(r.Context(), caller.Scope, r.PathValue("session"), req)
	}))
	mux.HandleFunc("POST /v1/state/sessions/{session}/vm:cancel", jsonRoute(func(r *http.Request, caller transport.Principal, req struct {
		TurnSeq int64 `json:"turn_seq,omitempty"`
	}) (any, error) {
		if err := authorize(r.Context(), caller.Claims, "state.coordinate"); err != nil {
			return nil, err
		}
		return store.CancelTurn(r.Context(), caller.Scope, r.PathValue("session"), req.TurnSeq)
	}))
	mux.HandleFunc("GET /internal/v1/state/vm-reconcile", readRoute(func(r *http.Request, caller transport.Principal) (any, error) {
		if err := authorize(r.Context(), caller.Claims, "state.coordinate"); err != nil {
			return nil, err
		}
		query := r.URL.Query()
		var cursor *journal.VMReconcileCursor
		after, id := query.Get("after"), query.Get("after_session")
		if after != "" || id != "" {
			at, err := time.Parse(time.RFC3339Nano, after)
			if err != nil || id == "" {
				return nil, invalidHTTP("invalid VM reconcile cursor")
			}
			cursor = &journal.VMReconcileCursor{At: at, SessionID: id}
		}
		return store.ListRecoverableVMPage(r.Context(), caller.Scope, query.Get("status"), cursor, 64)
	}))
	if dotsFIFO {
		mux.HandleFunc("POST /v1/state/sessions/dots:enqueue", jsonRoute(func(r *http.Request, caller transport.Principal, req journal.DotsEnqueueRequest) (any, error) {
			return store.EnqueueDotsTurn(r.Context(), caller.Scope, caller.Claims.Task, req)
		}))
		mux.HandleFunc("POST /v1/state/sessions/dots:claim-turn", jsonRoute(func(r *http.Request, caller transport.Principal, req claimTurnRequest) (any, error) {
			turn, err := store.ClaimDotsTurn(r.Context(), caller.Scope, req.SessionID, caller.Claims.Task, duration(req.TTL))
			if err != nil {
				return nil, err
			}
			proof, err := leaseProof(caller, turn.SessionID, turn.Lease)
			return struct {
				journal.DotsTurnLease
				Proof string `json:"tool_lease_proof,omitempty"`
			}{turn, proof}, err
		}))
	}
	mux.HandleFunc("POST /v1/state/sessions:claim-turn", jsonRoute(func(r *http.Request, caller transport.Principal, req claimTurnRequest) (any, error) {
		turn, err := store.ClaimTurn(r.Context(), caller.Scope, req.SessionID, caller.Claims.Task, duration(req.TTL))
		if err != nil {
			return nil, err
		}
		proof, err := leaseProof(caller, turn.SessionID, turn.Lease)
		return struct {
			TurnLease
			Proof string `json:"tool_lease_proof,omitempty"`
		}{turn, proof}, err
	}))
	mux.HandleFunc("GET /v1/state/sessions/{session}", readRoute(func(r *http.Request, caller transport.Principal) (any, error) {
		if r.URL.Query().Get("view") == "resume" {
			return store.InspectSession(r.Context(), caller.Scope, r.PathValue("session"))
		}
		return store.GetTurnStatus(r.Context(), caller.Scope, r.PathValue("session"))
	}))
	mux.HandleFunc("GET /v1/state/sessions", readRoute(func(r *http.Request, caller transport.Principal) (any, error) {
		limit, err := queryInt(r, "limit", 100)
		if err != nil || limit < 1 || limit > 1000 {
			return nil, invalidHTTP("invalid limit")
		}
		query := r.URL.Query()
		if query.Get("view") == "catalog" {
			offset, err := queryInt(r, "offset", 0)
			if err != nil || offset < 0 || offset > 10000 {
				return nil, invalidHTTP("invalid catalog offset")
			}
			var since *time.Time
			if value := query.Get("updated_after"); value != "" {
				parsed, err := time.Parse(time.RFC3339Nano, value)
				if err != nil {
					return nil, invalidHTTP("invalid updated_after timestamp")
				}
				since = &parsed
			}
			return store.ListCatalog(r.Context(), caller.Scope, CatalogQuery{
				Harness: query.Get("harness"), AppID: query.Get("app_id"), AgentID: query.Get("agent_id"),
				UserID: query.Get("user_id"), Status: query.Get("status"), Search: query.Get("search_query"),
				Cursor: query.Get("cursor"), UpdatedAfter: since, Limit: int(limit), Offset: int(offset),
				Label: query.Get("label"), SpawnedBy: query.Get("spawned_by"), AgentIDs: query["configured_agent_id"],
			})
		}
		return store.ListSessions(r.Context(), caller.Scope, query.Get("harness"), query.Get("cursor"), int(limit))
	}))
	mux.HandleFunc("POST /v1/state/sessions:acquire", jsonRoute(func(r *http.Request, caller transport.Principal, req acquireRequest) (any, error) {
		if req.ReadOnly {
			return store.InspectSession(r.Context(), caller.Scope, req.ID)
		}
		ttl := duration(req.TTL)
		if ttl == 0 {
			return nil, invalidHTTP("invalid lease duration")
		}
		lease, err := store.AcquireOrCreateSession(r.Context(), caller.Scope, req.ID,
			req.Harness, req.FormatVersion, caller.Claims.Task, ttl)
		if err != nil {
			return nil, err
		}
		proof, err := leaseProof(caller, req.ID, lease)
		if err != nil {
			return nil, err
		}
		state, err := store.InspectSession(r.Context(), caller.Scope, req.ID)
		return struct {
			Lease
			ResumeState
			Proof string `json:"tool_lease_proof,omitempty"`
		}{lease, state, proof}, err
	}))
	mux.HandleFunc("POST /v1/state/sessions/{action}", jsonRoute(func(r *http.Request, caller transport.Principal, req heartbeatRequest) (any, error) {
		session, ok := strings.CutSuffix(r.PathValue("action"), ":heartbeat")
		if !ok {
			return nil, contracts.NewError(contracts.ErrNotFound, "unknown session action")
		}
		lease := authorizedLease(caller, req.Epoch)
		if req.Release {
			status := req.Status
			if status == "" {
				status = SessionIdle
			}
			return struct{}{}, store.ReleaseLeaseTo(r.Context(), caller.Scope, session, lease, status)
		}
		if req.Status != "" {
			return nil, invalidHTTP("session disposition requires release")
		}
		renewed, control, err := store.HeartbeatLeaseWithControl(r.Context(), caller.Scope, session, lease, duration(req.TTL))
		return leaseResponse(caller, session, renewed, control, err)
	}))
	mux.HandleFunc("POST /v1/state/sessions/{session}/events:append", jsonRouteLimit(3<<20, func(r *http.Request, caller transport.Principal, req appendRequest) (any, error) {
		shared := false
		for _, kv := range req.KV {
			if kv.ScopeType != "session" {
				if err := authorize(r.Context(), caller.Claims, "state.shared"); err != nil {
					return nil, err
				}
				shared = true
				break
			}
		}
		return store.AppendEvents(r.Context(), caller.Scope, r.PathValue("session"), authorizedLease(caller, req.Epoch), shared, req.AppendRequest)
	}))
	mux.HandleFunc("GET /v1/state/sessions/{session}/events", readRoute(func(r *http.Request, caller transport.Principal) (any, error) {
		after, err := queryInt(r, "after_seq", 0)
		if err != nil {
			return nil, err
		}
		limit, err := queryInt(r, "limit", 100)
		if err != nil || limit < 1 || limit > 1000 {
			return nil, invalidHTTP("invalid limit")
		}
		query := r.URL.Query()
		filters := EventFilters{
			Search: query.Get("q"), BranchLeaf: query.Get("branch_leaf_id"),
		}
		if raw := query.Get("after_ts"); raw != "" {
			stamp, err := time.Parse(time.RFC3339Nano, raw)
			if err != nil {
				return nil, invalidHTTP("invalid after_ts")
			}
			filters.AfterTime = &stamp
		}
		if raw := query.Get("active_only"); raw != "" {
			active, err := strconv.ParseBool(raw)
			if err != nil {
				return nil, invalidHTTP("invalid active_only")
			}
			filters.ActiveOnly = active
		}
		if raw := query.Get("num_recent"); raw != "" {
			recent, err := queryInt(r, "num_recent", 0)
			if err != nil || recent < 1 || recent > 1000 {
				return nil, invalidHTTP("invalid num_recent")
			}
			limit, filters.Recent = recent, true
		}
		namespace := query.Get("state_namespace")
		if namespace != "" {
			if err := authorize(r.Context(), caller.Claims, "state.shared"); err != nil {
				return nil, err
			}
		}
		return store.filteredEvents(r.Context(), caller.Scope, r.PathValue("session"), after, int(limit), namespace, filters)
	}))
	mux.HandleFunc("POST /v1/state/sessions/{session}/checkpoints", jsonRouteLimit(3<<20, func(r *http.Request, caller transport.Principal, req checkpointRequest) (any, error) {
		return store.CommitCheckpoint(r.Context(), caller.Scope, r.PathValue("session"), authorizedLease(caller, req.Epoch), req.CheckpointCommit)
	}))
	mux.HandleFunc("GET /v1/state/sessions/{session}/checkpoints", readRoute(func(r *http.Request, caller transport.Principal) (any, error) {
		query := r.URL.Query()
		if query.Get("list") == "true" {
			limit, err := queryInt(r, "limit", 100)
			if err != nil || limit < 1 || limit > 1000 {
				return nil, invalidHTTP("invalid limit")
			}
			if query.Get("include_tuples") == "true" {
				return store.ListCheckpointTuples(r.Context(), caller.Scope, r.PathValue("session"),
					query.Get("checkpoint_ns"), query.Get("before"), int(limit),
					json.RawMessage(query.Get("metadata")), query.Get("include_unindexed") == "true")
			}
			ids, err := store.ListCheckpointIDs(r.Context(), caller.Scope, r.PathValue("session"),
				query.Get("checkpoint_ns"), query.Get("before"), int(limit))
			return struct {
				IDs []string `json:"checkpoint_ids"`
			}{IDs: ids}, err
		}
		return store.GetCheckpoint(r.Context(), caller.Scope, r.PathValue("session"), query.Get("checkpoint_ns"), query.Get("checkpoint_id"))
	}))
	mux.HandleFunc("GET /v1/state/sessions/{session}/chat-history", readRoute(func(r *http.Request, caller transport.Principal) (any, error) {
		query := r.URL.Query()
		return store.GetChatHistory(r.Context(), caller.Scope, r.PathValue("session"), query.Get("checkpoint_ns"), query.Get("checkpoint_id"))
	}))
	mux.HandleFunc("DELETE /v1/state/sessions/{session}", readRoute(func(r *http.Request, caller transport.Principal) (any, error) {
		return struct{}{}, store.DeleteSession(r.Context(), caller.Scope, r.PathValue("session"))
	}))
	mux.HandleFunc("PATCH /v1/state/sessions/{session}/legal-hold", jsonRoute(func(r *http.Request, caller transport.Principal, req holdRequest) (any, error) {
		if err := authorize(r.Context(), caller.Claims, "state.retention"); err != nil {
			return nil, err
		}
		if req.Held == nil {
			return nil, invalidHTTP("legal hold flag is required")
		}
		return struct{}{}, store.SetLegalHold(r.Context(), caller.Scope, r.PathValue("session"), *req.Held)
	}))
	mux.HandleFunc("POST /v1/state/sessions/{session}/checkpoints/{checkpoint}/writes", jsonRouteLimit(3<<20, func(r *http.Request, caller transport.Principal, req pendingWritesRequest) (any, error) {
		return struct{}{}, store.PutTurnWrites(r.Context(), caller.Scope, r.PathValue("session"),
			authorizedLease(caller, req.Epoch), req.Namespace, r.PathValue("checkpoint"), req.Writes, req.TurnSeq)
	}))
	mutateIntentRoute := jsonRoute(func(r *http.Request, caller transport.Principal, req intentRequest) (any, error) {
		return struct{}{}, store.MutateToolIntent(r.Context(), caller.Scope, req.Session, authorizedLease(caller, req.Epoch), req.Transition)
	})
	mux.HandleFunc("POST /v1/state/aux:mutate", mutateIntentRoute)
	mux.HandleFunc("POST /v1/state/intents:mutate", mutateIntentRoute)
	mux.HandleFunc("POST /v1/state/kv:mutate", jsonRouteLimit(3<<20, func(r *http.Request, caller transport.Principal, req kvRequest) (any, error) {
		shared := false
		for _, mutation := range req.KV {
			if mutation.ScopeType != "session" {
				if err := authorize(r.Context(), caller.Claims, "state.shared"); err != nil {
					return nil, err
				}
				shared = true
				break
			}
		}
		return store.MutateKV(r.Context(), caller.Scope, req.Session, authorizedLease(caller, req.Epoch), shared,
			KVBatch{TurnSeq: req.TurnSeq, Key: req.Key, ExpectedSeq: req.ExpectedSeq, KV: req.KV, Catalog: req.Catalog})
	}))
	mux.HandleFunc("GET /v1/state/kv", readRoute(func(r *http.Request, caller transport.Principal) (any, error) {
		query := r.URL.Query()
		scopeType := query.Get("scope_type")
		if scopeType != "session" {
			if err := authorize(r.Context(), caller.Claims, "state.shared"); err != nil {
				return nil, err
			}
		}
		limit, err := queryInt(r, "limit", 100)
		if err != nil || limit < 1 || limit > 1000 {
			return nil, invalidHTTP("invalid KV limit")
		}
		offset, err := queryInt(r, "offset", 0)
		if err != nil || offset < 0 || offset > 10000 {
			return nil, invalidHTTP("invalid KV offset")
		}
		keys := query["key"]
		if len(keys) == 1 && keys[0] == "" {
			// Preserve the legacy single empty query parameter as no filter.
			keys = nil
		}
		return store.ListKV(r.Context(), caller.Scope, KVQuery{
			Session: query.Get("session_id"), ScopeType: scopeType, Namespace: query.Get("namespace"), Keys: keys,
			Prefix: query.Get("prefix") == "true", Limit: int(limit), Offset: int(offset),
		})
	}))
	mux.HandleFunc("POST /v1/state/sessions/{session}/tools:begin-standalone", jsonRoute(func(r *http.Request, caller transport.Principal, req beginStandaloneRequest) (any, error) {
		return store.BeginStandaloneTool(r.Context(), caller.Scope, r.PathValue("session"), authorizedLease(caller, req.Epoch), req.StandaloneToolRequest)
	}))
	mux.HandleFunc("GET /v1/state/aux:mutate", readRoute(func(r *http.Request, caller transport.Principal) (any, error) {
		query := r.URL.Query()
		if namespace := query.Get("state_namespace"); namespace != "" {
			if err := authorize(r.Context(), caller.Claims, "state.shared"); err != nil {
				return nil, err
			}
			return store.GetUserState(r.Context(), caller.Scope, namespace)
		}
		return store.GetToolIntent(r.Context(), caller.Scope, query.Get("session_id"), query.Get("call_id"))
	}))
	return transport.Task(verifier, "ahp", "state", authorize)(mux), nil
}

type acquireRequest struct {
	ID            string `json:"session_id"`
	Harness       string `json:"harness_type"`
	FormatVersion int    `json:"format_version"`
	TTL           int64  `json:"ttl_ms,omitempty"`
	ReadOnly      bool   `json:"read_only,omitempty"`
}

type stageBlobRequest struct {
	Epoch int64  `json:"lease_epoch"`
	Data  []byte `json:"data"`
}

type holdRequest struct {
	Held *bool `json:"held"`
}

type blobReadResponse struct {
	Data []byte `json:"data"`
}

type claimTurnRequest struct {
	SessionID string `json:"session_id,omitempty"`
	TTL       int64  `json:"ttl_ms,omitempty"`
}

type heartbeatRequest struct {
	Epoch   int64  `json:"lease_epoch"`
	TTL     int64  `json:"ttl_ms,omitempty"`
	Release bool   `json:"release,omitempty"`
	Status  string `json:"status,omitempty"`
}

type appendRequest struct {
	Epoch int64 `json:"lease_epoch"`
	AppendRequest
}

type checkpointRequest struct {
	Epoch int64 `json:"lease_epoch"`
	CheckpointCommit
}

type pendingWritesRequest struct {
	TurnSeq   int64          `json:"turn_seq,omitempty"`
	Epoch     int64          `json:"lease_epoch"`
	Namespace string         `json:"namespace"`
	Writes    []PendingWrite `json:"writes"`
}

type kvRequest struct {
	TurnSeq     int64                  `json:"turn_seq,omitempty"`
	Catalog     *OpenClawCatalogUpdate `json:"catalog,omitempty"`
	Session     string                 `json:"session_id"`
	Epoch       int64                  `json:"lease_epoch"`
	Key         string                 `json:"key"`
	ExpectedSeq int64                  `json:"expected_seq"`
	KV          []KVMutation           `json:"kv"`
}

type intentRequest struct {
	Session    string           `json:"session_id"`
	Epoch      int64            `json:"lease_epoch"`
	Transition IntentTransition `json:"transition"`
}

type beginStandaloneRequest struct {
	Epoch int64 `json:"lease_epoch"`
	StandaloneToolRequest
}

func authorizedLease(caller transport.Principal, epoch int64) Lease {
	return Lease{Worker: caller.Claims.Task, Epoch: epoch}
}

func duration(ms int64) time.Duration {
	switch {
	case ms == 0:
		return 30 * time.Second
	case ms < 0 || ms > int64((15*time.Minute)/time.Millisecond):
		return 0
	default:
		return time.Duration(ms) * time.Millisecond
	}
}

func invalidHTTP(message string) error {
	return contracts.NewError(contracts.ErrInvalid, message)
}

func queryInt(r *http.Request, name string, fallback int64) (int64, error) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, invalidHTTP("invalid " + name)
	}
	return value, nil
}

type reader func(*http.Request, transport.Principal) (any, error)

func readRoute(handler reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		caller, ok := transport.FromContext(r.Context())
		if !ok {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "journal route requires verified identity"))
			return
		}
		result, err := handler(r, caller)
		if err != nil {
			transport.WriteError(w, r, err)
			return
		}
		transport.WriteJSON(w, http.StatusOK, result)
	}
}

func jsonRoute[T any](handler func(*http.Request, transport.Principal, T) (any, error)) http.HandlerFunc {
	return jsonRouteLimit(transport.DefaultMaxBodyBytes, handler)
}

func jsonRouteLimit[T any](maxBytes int64, handler func(*http.Request, transport.Principal, T) (any, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var request T
		if err := transport.ReadJSON(w, r, &request, maxBytes); err != nil {
			transport.WriteError(w, r, err)
			return
		}
		readRoute(func(r *http.Request, caller transport.Principal) (any, error) {
			return handler(r, caller, request)
		})(w, r)
	}
}
