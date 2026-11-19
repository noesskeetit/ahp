package toolsapi

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"

	"github.com/ahp-platform/ahp/internal/tools"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// ConnectCapability is distinct from risky-action approval: only the owner connects.
const ConnectCapability = "tools:connect"

func oauthCookie(state string) string {
	sum := sha256.Sum256([]byte(state))
	return "ahp_oauth_" + hex.EncodeToString(sum[:16])
}

func (h *Handler) oauth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	p, ok := transport.FromContext(r.Context())
	if !ok || p.Claims.Kind != auth.KindHuman {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "oauth: human session required"))
		return
	}
	m, e := h.svc.PersonalOAuth()
	if e != nil {
		transport.WriteError(w, r, e)
		return
	}
	var in struct {
		Connection string `json:"connection"`
		State      string `json:"state"`
		Code       string `json:"code"`
		Error      string `json:"error"`
	}
	if e = transport.ReadJSON(w, r, &in, 16<<10); e != nil {
		transport.WriteError(w, r, e)
		return
	}
	switch r.URL.Path {
	case "/v1/tools/OAuthConnections":
		out, e := m.Connections(r.Context(), p.Claims)
		if e != nil {
			transport.WriteError(w, r, e)
			return
		}
		transport.WriteJSON(w, http.StatusOK, map[string]any{"connections": out})
		return
	case "/v1/tools/OAuthBegin":
		out, e := m.Begin(r.Context(), p.Claims, in.Connection)
		if e != nil {
			transport.WriteError(w, r, e)
			return
		}
		http.SetCookie(w, &http.Cookie{Name: oauthCookie(out.State), Value: out.Nonce, Path: "/", HttpOnly: true, Secure: out.SecureCookie, SameSite: http.SameSiteLaxMode, MaxAge: 900})
		transport.WriteJSON(w, http.StatusOK, out)
		return
	case "/v1/tools/OAuthCallback":
		cookie, err := r.Cookie(oauthCookie(in.State))
		if err != nil {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "oauth: browser binding missing"))
			return
		}
		e = m.Complete(r.Context(), p.Claims, tools.OAuthCallback{State: in.State, Code: in.Code, Error: in.Error, Nonce: cookie.Value})
		if e == nil {
			http.SetCookie(w, &http.Cookie{Name: cookie.Name, Value: "", Path: "/", HttpOnly: true, Secure: cookie.Secure, SameSite: http.SameSiteLaxMode, MaxAge: -1})
		}
	case "/v1/tools/OAuthRevoke":
		e = m.Revoke(r.Context(), p.Claims, in.Connection)
	default:
		e = contracts.NewError(contracts.ErrNotFound, "oauth: unknown operation")
	}
	if e != nil {
		transport.WriteError(w, r, e)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
