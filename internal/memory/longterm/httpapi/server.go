package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/ahp-platform/ahp/internal/memory/longterm"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

type Server struct{ Service *memory.Service }

func New(service *memory.Service) http.Handler {
	s := &Server{service}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, r *http.Request) { respond(w, 200, map[string]bool{"live": true}, nil) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := service.Store.Ping(r.Context()); err != nil {
			// DEF-MEM-HTTP-CODE-1: the readiness refusal is exactly the family
			// contracts.ErrUnavailable documents (a dependency is not answering),
			// so it carries the canonical code, not a face-local literal.
			respond(w, 503, nil, contracts.NewError(contracts.ErrUnavailable, "database is unavailable"))
			return
		}
		respond(w, 200, map[string]bool{"ready": true}, nil)
	})
	mux.HandleFunc("POST /v1/memory/remember", s.remember)
	mux.HandleFunc("POST /v1/memory/bind", s.bindProject)
	mux.HandleFunc("POST /v1/memory/search", s.search)
	mux.HandleFunc("GET /v1/memory/items", s.list)
	mux.HandleFunc("POST /v1/memory/context", s.context)
	mux.HandleFunc("GET /v1/memory/settings", s.settings)
	mux.HandleFunc("GET /v1/memory/status", s.status)
	mux.HandleFunc("PATCH /v1/memory/settings", s.updateSettings)
	mux.HandleFunc("POST /v1/memory/clear", s.clear)
	mux.HandleFunc("GET /v1/memory/operations/{id}", s.operation)
	mux.HandleFunc("POST /v1/memory/operations/{id}/retry", s.retry)
	mux.HandleFunc("PATCH /v1/memory/items/{ref}", s.correct)
	mux.HandleFunc("POST /v1/memory/items/{ref}/invalidate", s.invalidate)
	mux.HandleFunc("POST /internal/v1/turns", s.turn)
	mux.HandleFunc("POST /v1/memory/runtime/turns", s.turn)
	mux.HandleFunc("POST /v1/memory/runtime/flush", s.flush)
	mux.HandleFunc("POST /v1/memory/runtime/runs", s.run)
	mux.HandleFunc("DELETE /v1/memory/projects/{id}", s.deleteProject)
	mux.HandleFunc("POST /internal/v1/sessions/flush", s.flush)
	mux.HandleFunc("POST /internal/v1/runs", s.run)
	mux.HandleFunc("DELETE /internal/v1/projects/{id}", s.deleteProject)
	return mux
}
func decode(w http.ResponseWriter, r *http.Request, into any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(into); err != nil {
		return memory.Fail(400, "invalid_json", "invalid, oversized or unknown JSON fields")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return memory.Fail(400, "invalid_json", "exactly one JSON object is required")
	}
	return nil
}
func respond(w http.ResponseWriter, status int, value any, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if err != nil {
		code, message := "internal_error", "request failed"
		status = 500
		var typed *memory.Error
		if errors.As(err, &typed) {
			status = typed.Status
			code = typed.Code
			message = typed.Message
		} else if errors.Is(err, memory.ErrMissing) {
			// decision 2026-10-31-http-surface-no-false-200: every HTTP 404 of the
			// platform carries the canonical code; the internal layer already
			// returns contracts.ErrNotFound for the same fact (internal/memory).
			status = 404
			code = contracts.ErrNotFound
			message = "resource not found"
		} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			status = 503
			code = contracts.ErrUnavailable
			message = "request did not complete"
		} else if code := contracts.Code(err); code != "" {
			// A contracts error reached this face (for example the canonical
			// readiness refusal below): answer with the canonical code and the
			// status the platform maps for that code family.
			status = contractStatus(code)
			code2 := code
			message = err.Error()
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": code2, "message": message}})
			return
		}
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": code, "message": message}})
		return
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

// contractStatus maps a canonical pkg/contracts code to the HTTP status the
// platform reserves for its family (pkg/transport/json.go WriteError is the
// reference mapping; this face keeps its own envelope shape on purpose).
func contractStatus(code string) int {
	switch code {
	case contracts.ErrInvalid, contracts.ErrKeyReuse:
		return http.StatusBadRequest
	case contracts.ErrForbidden:
		return http.StatusForbidden
	case contracts.ErrNotFound:
		return http.StatusNotFound
	case contracts.ErrConflict, contracts.ErrStaleEpoch, contracts.ErrLeaseLost:
		return http.StatusConflict
	case contracts.ErrUnavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}
func (s *Server) identity(r *http.Request, request memory.AccessRequest) (memory.Identity, error) {
	if len(request.ProjectID) > 200 || strings.ContainsRune(request.ProjectID, 0) {
		return memory.Identity{}, memory.Fail(400, "invalid_project", "project reference exceeds configured bounds")
	}
	for i := range request.Sources {
		request.Sources[i].Text = ""
	}
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		return memory.Identity{}, memory.Fail(401, "authentication_required", "Bearer token required")
	}
	id, err := s.Service.Access.Authorize(r.Context(), strings.TrimPrefix(auth, "Bearer "), request)
	if err != nil {
		return id, err
	}
	return id, id.Validate(s.Service.Now())
}
func scopes(scope string) []memory.Scope {
	if scope == "both" {
		return []memory.Scope{memory.Personal, memory.Project}
	}
	return []memory.Scope{memory.Scope(scope)}
}
func manage(id memory.Identity) error {
	if id.ActorKind != "console" && id.ActorKind != "platform" {
		return memory.Fail(403, "console_required", "settings and cleanup require a console or platform identity")
	}
	return nil
}
func (s *Server) remember(w http.ResponseWriter, r *http.Request) {
	var req memory.RememberRequest
	if err := decode(w, r, &req); err != nil {
		respond(w, 0, nil, err)
		return
	}
	proofs := []memory.Source{}
	for _, ref := range req.SourceRefs {
		proofs = append(proofs, memory.Source{Ref: ref, AllowedScopes: []memory.Scope{req.Target}})
	}
	id, err := s.identity(r, memory.AccessRequest{Action: "memory:write", ProjectID: req.ProjectID, Scopes: []memory.Scope{req.Target}, Sources: proofs})
	if err != nil {
		respond(w, 0, nil, err)
		return
	}
	out, err := s.Service.Remember(r.Context(), id, r.Header.Get("Idempotency-Key"), req)
	respond(w, 202, out, err)
}
func (s *Server) bindProject(w http.ResponseWriter, r *http.Request) {
	var req memory.BindRequest
	if err := decode(w, r, &req); err != nil {
		respond(w, 0, nil, err)
		return
	}
	id, err := s.identity(r, memory.AccessRequest{Action: "memory:bind", ProjectID: req.ProjectID, Scopes: []memory.Scope{memory.Project}})
	if err != nil {
		respond(w, 0, nil, err)
		return
	}
	out, err := s.Service.BindProject(r.Context(), id, r.Header.Get("Idempotency-Key"), req)
	respond(w, 200, out, err)
}
func (s *Server) search(w http.ResponseWriter, r *http.Request) {
	var req memory.SearchRequest
	if err := decode(w, r, &req); err != nil {
		respond(w, 0, nil, err)
		return
	}
	id, err := s.identity(r, memory.AccessRequest{Action: "memory:read", ProjectID: req.ProjectID, Scopes: scopes(req.Scope)})
	if err != nil {
		respond(w, 0, nil, err)
		return
	}
	out, err := s.Service.Search(r.Context(), id, req)
	respond(w, 200, out, err)
}
func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	scope := memory.Scope(q.Get("scope"))
	project := q.Get("project_id")
	limit, offset := 100, 0
	var err error
	if v := q.Get("limit"); v != "" {
		limit, err = strconv.Atoi(v)
		if err != nil {
			respond(w, 0, nil, memory.Fail(400, "invalid_page", "invalid limit"))
			return
		}
	}
	if v := q.Get("offset"); v != "" {
		offset, err = strconv.Atoi(v)
		if err != nil {
			respond(w, 0, nil, memory.Fail(400, "invalid_page", "invalid offset"))
			return
		}
	}
	id, err := s.identity(r, memory.AccessRequest{Action: "memory:read", ProjectID: project, Scopes: []memory.Scope{scope}})
	if err != nil {
		respond(w, 0, nil, err)
		return
	}
	out, err := s.Service.List(r.Context(), id, scope, project, limit, offset)
	respond(w, 200, out, err)
}
func (s *Server) context(w http.ResponseWriter, r *http.Request) {
	var req memory.ContextRequest
	if err := decode(w, r, &req); err != nil {
		respond(w, 0, nil, err)
		return
	}
	requested := []memory.Scope{memory.Project}
	if req.IncludePersonal {
		requested = append(requested, memory.Personal)
	}
	id, err := s.identity(r, memory.AccessRequest{Action: "memory:read", ProjectID: req.ProjectID, Scopes: requested})
	if err != nil {
		respond(w, 0, nil, err)
		return
	}
	settings, err := s.Service.Settings(r.Context(), id, req.ProjectID)
	if err != nil {
		respond(w, 0, nil, err)
		return
	}
	result := memory.SearchResult{Facts: []memory.Fact{}}
	if req.IncludePersonal && settings.PersonalEnabled && settings.UsePersonal {
		part, err := s.Service.Search(r.Context(), id, memory.SearchRequest{ProjectID: req.ProjectID, Scope: "personal", Query: "Stable user preferences, communication language, response format and working style"})
		if err != nil {
			respond(w, 0, nil, err)
			return
		}
		result.Facts = append(result.Facts, part.Facts...)
		result.Degraded = append(result.Degraded, part.Degraded...)
	}
	if settings.ProjectEnabled {
		part, err := s.Service.Search(r.Context(), id, memory.SearchRequest{ProjectID: req.ProjectID, Scope: "project", Query: req.Query})
		if err != nil {
			respond(w, 0, nil, err)
			return
		}
		result.Facts = append(result.Facts, part.Facts...)
		result.Degraded = append(result.Degraded, part.Degraded...)
	}
	respond(w, 200, result, nil)
}
func (s *Server) settings(w http.ResponseWriter, r *http.Request) {
	project := r.URL.Query().Get("project_id")
	id, err := s.identity(r, memory.AccessRequest{Action: "memory:read", ProjectID: project})
	if err != nil {
		respond(w, 0, nil, err)
		return
	}
	out, err := s.Service.Settings(r.Context(), id, project)
	respond(w, 200, out, err)
}
func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	project := r.URL.Query().Get("project_id")
	scope := memory.Scope(r.URL.Query().Get("scope"))
	id, err := s.identity(r, memory.AccessRequest{Action: "memory:read", ProjectID: project, Scopes: []memory.Scope{scope}})
	if err != nil {
		respond(w, 0, nil, err)
		return
	}
	out, err := s.Service.Status(r.Context(), id, scope, project)
	respond(w, 200, out, err)
}
func (s *Server) updateSettings(w http.ResponseWriter, r *http.Request) {
	var req memory.SettingsRequest
	if err := decode(w, r, &req); err != nil {
		respond(w, 0, nil, err)
		return
	}
	id, err := s.identity(r, memory.AccessRequest{Action: "memory:manage", ProjectID: req.ProjectID})
	if err == nil {
		err = manage(id)
	}
	if err != nil {
		respond(w, 0, nil, err)
		return
	}
	out, err := s.Service.UpdateSettings(r.Context(), id, r.Header.Get("Idempotency-Key"), req)
	respond(w, 200, out, err)
}
func (s *Server) clear(w http.ResponseWriter, r *http.Request) {
	var req memory.ClearRequest
	if err := decode(w, r, &req); err != nil {
		respond(w, 0, nil, err)
		return
	}
	id, err := s.identity(r, memory.AccessRequest{Action: "memory:clear", ProjectID: req.ProjectID, Scopes: []memory.Scope{req.Scope}})
	if err == nil {
		err = manage(id)
	}
	if err != nil {
		respond(w, 0, nil, err)
		return
	}
	out, err := s.Service.Clear(r.Context(), id, r.Header.Get("Idempotency-Key"), req)
	respond(w, 202, out, err)
}
func (s *Server) operation(w http.ResponseWriter, r *http.Request) {
	project := r.URL.Query().Get("project_id")
	scope := memory.Scope(r.URL.Query().Get("scope"))
	if !scope.Valid() {
		respond(w, 0, nil, memory.Fail(400, "invalid_scope", "scope is required"))
		return
	}
	id, err := s.identity(r, memory.AccessRequest{Action: "memory:read", ProjectID: project, Scopes: []memory.Scope{scope}})
	if err != nil {
		respond(w, 0, nil, err)
		return
	}
	out, err := s.Service.GetOperation(r.Context(), id, r.PathValue("id"), project, scope)
	respond(w, 200, out, err)
}
func (s *Server) retry(w http.ResponseWriter, r *http.Request) {
	var req memory.ClearRequest
	if err := decode(w, r, &req); err != nil {
		respond(w, 0, nil, err)
		return
	}
	id, err := s.identity(r, memory.AccessRequest{Action: "memory:manage", ProjectID: req.ProjectID, Scopes: []memory.Scope{req.Scope}})
	if err == nil {
		err = manage(id)
	}
	if err != nil {
		respond(w, 0, nil, err)
		return
	}
	out, err := s.Service.Retry(r.Context(), id, r.Header.Get("Idempotency-Key"), r.PathValue("id"), req.ProjectID, req.Scope)
	respond(w, 202, out, err)
}
func (s *Server) correct(w http.ResponseWriter, r *http.Request)    { s.curate(w, r, false) }
func (s *Server) invalidate(w http.ResponseWriter, r *http.Request) { s.curate(w, r, true) }
func (s *Server) curate(w http.ResponseWriter, r *http.Request, invalidate bool) {
	var req memory.CorrectRequest
	if err := decode(w, r, &req); err != nil {
		respond(w, 0, nil, err)
		return
	}
	id, err := s.identity(r, memory.AccessRequest{Action: "memory:write", ProjectID: req.ProjectID, Scopes: []memory.Scope{req.Scope}})
	if err != nil {
		respond(w, 0, nil, err)
		return
	}
	out, err := s.Service.Correct(r.Context(), id, r.Header.Get("Idempotency-Key"), r.PathValue("ref"), req, invalidate)
	respond(w, 202, out, err)
}
func (s *Server) turn(w http.ResponseWriter, r *http.Request) {
	var req memory.TurnRequest
	if err := decode(w, r, &req); err != nil {
		respond(w, 0, nil, err)
		return
	}
	original, _ := json.Marshal(req)
	sum := sha256.Sum256(original)
	req.IntentDigest = hex.EncodeToString(sum[:])
	proofs := append([]memory.Source(nil), req.Sources...)
	requested := []memory.Scope{}
	for _, source := range proofs {
		requested = append(requested, source.AllowedScopes...)
	}
	id, err := s.identity(r, memory.AccessRequest{Action: "runtime:turn", ProjectID: req.ProjectID, Scopes: requested, Sources: proofs})
	if err != nil {
		respond(w, 0, nil, err)
		return
	}
	if policy, ok := s.Service.Access.(interface {
		FilterSources(context.Context, memory.Identity, []memory.Source) ([]memory.Source, error)
	}); ok {
		req.Sources, err = policy.FilterSources(r.Context(), id, req.Sources)
		if err != nil {
			respond(w, 0, nil, err)
			return
		}
	}
	out, err := s.Service.Ingest(r.Context(), id, r.Header.Get("Idempotency-Key"), req)
	respond(w, 202, out, err)
}
func (s *Server) flush(w http.ResponseWriter, r *http.Request) {
	var req memory.FlushRequest
	if err := decode(w, r, &req); err != nil {
		respond(w, 0, nil, err)
		return
	}
	id, err := s.identity(r, memory.AccessRequest{Action: "runtime:flush", ProjectID: req.ProjectID})
	if err != nil {
		respond(w, 0, nil, err)
		return
	}
	out, err := s.Service.Flush(r.Context(), id, r.Header.Get("Idempotency-Key"), req)
	respond(w, 202, out, err)
}
func (s *Server) run(w http.ResponseWriter, r *http.Request) {
	var req memory.RunRequest
	if err := decode(w, r, &req); err != nil {
		respond(w, 0, nil, err)
		return
	}
	id, err := s.identity(r, memory.AccessRequest{Action: "runtime:run", ProjectID: req.ProjectID})
	if err != nil {
		respond(w, 0, nil, err)
		return
	}
	out, err := s.Service.RunEvent(r.Context(), id, r.Header.Get("Idempotency-Key"), req)
	respond(w, 202, out, err)
}
func (s *Server) deleteProject(w http.ResponseWriter, r *http.Request) {
	project := r.PathValue("id")
	id, err := s.identity(r, memory.AccessRequest{Action: "project:delete", ProjectID: project})
	if err == nil && id.ActorKind != "platform" {
		err = memory.Fail(403, "platform_required", "project deletion requires a platform event")
	}
	if err != nil {
		respond(w, 0, nil, err)
		return
	}
	out, err := s.Service.DeleteProject(r.Context(), id, r.Header.Get("Idempotency-Key"), project)
	respond(w, 202, out, err)
}
