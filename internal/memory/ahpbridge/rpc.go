package ahpbridge

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/ahp-platform/ahp/internal/memory/longterm"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// Browser RPCs preserve Space's existing POST-only proxy and delegate to the
// same REST implementation as machine clients, under the verified principal.
type rpcRequest struct {
	ProjectID       string       `json:"project_id,omitempty"`
	Scope           memory.Scope `json:"scope,omitempty"`
	Target          memory.Scope `json:"target,omitempty"`
	Query           string       `json:"query,omitempty"`
	Text            string       `json:"text,omitempty"`
	Reason          string       `json:"reason,omitempty"`
	Ref             string       `json:"memory_ref,omitempty"`
	ID              string       `json:"id,omitempty"`
	SourceRefs      []string     `json:"source_refs,omitempty"`
	OpKey           string       `json:"op_key,omitempty"`
	PersonalEnabled *bool        `json:"personal_enabled,omitempty"`
	ProjectEnabled  *bool        `json:"project_enabled,omitempty"`
	UsePersonal     *bool        `json:"use_personal,omitempty"`
	IdleDays        *int         `json:"idle_days,omitempty"`
}

func withRPC(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		op := strings.TrimPrefix(r.URL.Path, "/v1/memory/")
		allowed := map[string]bool{"Remember": true, "Search": true, "Settings": true, "UpdateSettings": true, "Status": true, "Operation": true, "Clear": true, "Correct": true, "Invalidate": true}
		if r.Method != "POST" || !allowed[op] {
			next.ServeHTTP(w, r)
			return
		}
		var req rpcRequest
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&req); err != nil {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "invalid memory RPC body"))
			return
		}
		if err := decoder.Decode(&struct{}{}); err != io.EOF {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "one memory RPC body required"))
			return
		}
		if req.Scope == "" {
			req.Scope = memory.Personal
		}
		method, path := http.MethodPost, "/v1/memory/"
		var payload any
		q := url.Values{"scope": {string(req.Scope)}, "project_id": {req.ProjectID}}
		switch op {
		case "Remember":
			p, _ := transport.FromContext(r.Context())
			if len(req.SourceRefs) == 0 && p.Claims.CtxScope == auth.CtxScopeAuthority {
				req.SourceRefs = []string{"console://memory/command/" + req.OpKey}
			}
			payload = memory.RememberRequest{ProjectID: req.ProjectID, Target: req.Target, Text: req.Text, SourceRefs: req.SourceRefs}
			path += "remember"
		case "Search":
			if strings.TrimSpace(req.Query) == "" {
				method = http.MethodGet
				path += "items"
			} else {
				path += "search"
				payload = memory.SearchRequest{ProjectID: req.ProjectID, Scope: string(req.Scope), Query: req.Query}
			}
		case "Settings":
			method = http.MethodGet
			path += "settings"
		case "Status":
			method = http.MethodGet
			path += "status"
		case "Operation":
			method = http.MethodGet
			path += "operations/" + url.PathEscape(req.ID)
		case "UpdateSettings":
			method = http.MethodPatch
			path += "settings"
			payload = memory.SettingsRequest{ProjectID: req.ProjectID, PersonalEnabled: req.PersonalEnabled, ProjectEnabled: req.ProjectEnabled, UsePersonal: req.UsePersonal, IdleDays: req.IdleDays}
		case "Clear":
			path += "clear"
			payload = memory.ClearRequest{ProjectID: req.ProjectID, Scope: req.Scope}
		case "Correct", "Invalidate":
			path += "items/" + url.PathEscape(req.Ref)
			method = http.MethodPatch
			if op == "Invalidate" {
				path += "/invalidate"
				method = http.MethodPost
			}
			payload = memory.CorrectRequest{ProjectID: req.ProjectID, Scope: req.Scope, Text: req.Text, Reason: req.Reason}
		}
		copy := r.Clone(r.Context())
		copy.Method = method
		copy.URL.Path = path
		copy.URL.RawPath = ""
		copy.URL.RawQuery = q.Encode()
		if req.OpKey != "" {
			if given := copy.Header.Get("Idempotency-Key"); given != "" && given != req.OpKey {
				transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "conflicting operation keys"))
				return
			}
			copy.Header.Set("Idempotency-Key", req.OpKey)
		}
		if payload != nil {
			body, err := json.Marshal(payload)
			if err != nil {
				transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "invalid memory operation"))
				return
			}
			copy.Body = io.NopCloser(bytes.NewReader(body))
			copy.ContentLength = int64(len(body))
		} else {
			copy.Body = http.NoBody
			copy.ContentLength = 0
		}
		copy.Header.Set("Content-Type", "application/json")
		next.ServeHTTP(w, copy)
	})
}
