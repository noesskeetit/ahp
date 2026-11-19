package artifactsapi

// Resolve is the read-only digest→(id, exact version) lookup that the human edge
// needs while the download contract stays version-exact. It is a read: it sits behind
// the same task-token gate as Get, answers from the caller's verified tenant only, and
// returns the identical NOT_FOUND whether the digest is unknown or merely not the
// caller's to see. It writes nothing, issues no grant and returns no object key.

import (
	"net/http"

	"github.com/ahp-platform/ahp/pkg/transport"
)

type resolveRequest struct {
	SHA256 string `json:"sha256"`
}

func (h *Handler) resolve(w http.ResponseWriter, r *http.Request) {
	ctx, scope, err := readRequest(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in resolveRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	versions, err := h.store.ResolveByDigest(ctx, scope, in.SHA256)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, map[string]any{"sha256": in.SHA256, "versions": versions})
}
