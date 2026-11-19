package identityapi

import (
	"net/http"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

type issueTaskTokenBody struct {
	TTL string `json:"ttl,omitempty"`
}

// issueTaskToken is deliberately workload-only: Authorization headers do not
// authenticate a subject, including expired bearers. Only Workload's verified
// peer certificate selects tenant/azp. The request cannot choose an audience.
func (h *api) issueTaskToken(w http.ResponseWriter, r *http.Request) {
	p, ok := transport.FromContext(r.Context())
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "identityapi: no workload principal"))
		return
	}
	var body *issueTaskTokenBody
	if r.ContentLength != 0 {
		if !h.readBody(w, r, &body) {
			return
		}
		if body == nil {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "identityapi: an object is required"))
			return
		}
	}
	var ttl time.Duration
	if body != nil && body.TTL != "" {
		var err error
		ttl, err = time.ParseDuration(body.TTL)
		if err != nil || ttl <= 0 || ttl > identity.MaxTokenTTL {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "identityapi: ttl must be positive and at most 15 minutes"))
			return
		}
	}
	token, err := h.cfg.Store.IssueTaskToken(r.Context(), p.Scope, r.PathValue("task"), h.cfg.Signer, h.cfg.Audience, ttl)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]string{"token": token})
}
