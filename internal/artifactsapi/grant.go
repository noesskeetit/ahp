package artifactsapi

// DownloadGrant and OpenDownloadGrant are the two halves of the named Artifacts
// operation of PDF rev3.2 §6 (стр.1582) as applied to рис. 7 ("скачивание через
// прокси: текущие права, точная версия"):
//
//	DownloadGrant      issue a short-lived, use-counted grant for ONE exact
//	                 sealed version, after re-running the current rights check;
//	OpenDownloadGrant  redeem one grant and stream the bytes through this
//	                 process, honouring Range. Never a redirect, never a
//	                 presigned URL.
//
// Both are machine-surface routes: they sit behind the same coarse gate as the
// rest of the plane (mTLS workload certificate plus a task token carrying the
// "artifacts" capability). A browser is never given either credential - the
// browser talks to the human edge, and the human edge's proxy (Space) calls
// these two routes with its own workload identity. That is what keeps стр.743
// ("только токен задачи", "azp сверяется с mTLS") true while a person still
// gets their file.

import (
	"errors"
	"net/http"
	"time"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

type downloadGrantRequest struct {
	ID      string `json:"id"`
	Version string `json:"version"`
	// Recipient may only name the calling subject; empty means that subject.
	Recipient string `json:"recipient,omitempty"`
	// TTLSeconds is the requested lifetime. Unset gets the default; over the
	// ceiling is cut to the ceiling; under the floor is refused.
	TTLSeconds int64 `json:"ttl_seconds,omitempty"`
	// Uses is the number of consumptions allowed; unset means one.
	Uses int `json:"uses,omitempty"`
}

type openGrantRequest struct {
	Grant string `json:"grant"`
}

// downloadGrant issues a grant. The rights decision is made now, inside the
// store, from the VERIFIED claims of this request: tenant, the grant capability,
// the conversation gate of the caller's context claim, the label ceiling of the
// version, and lifecycle (a tombstone or a legal hold is refused). Nothing here
// recomputes those rules.
func (h *Handler) downloadGrant(w http.ResponseWriter, r *http.Request) {
	ctx, scope, err := readRequest(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in downloadGrantRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	grant, err := h.store.MintDownloadGrant(ctx, scope, in.ID, artifacts.GrantRequest{
		Version:   in.Version,
		Recipient: in.Recipient,
		TTL:       time.Duration(in.TTLSeconds) * time.Second,
		Uses:      in.Uses,
	})
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, grant)
}

// openDownloadGrant redeems a grant. It answers with the same headers and the
// same bytes Download answers with for that version, because it must be
// indistinguishable at the byte level: the proxy in рис. 7 is a pipe, not a
// second implementation.
//
// The caller's context claim is deliberately not consulted here. The grant is
// the authority: it was issued inside one conversation, and it names the
// recipient subject whose label ceiling and lifecycle state are re-checked on
// every redemption.
func (h *Handler) openDownloadGrant(w http.ResponseWriter, r *http.Request) {
	scope, err := principal(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in openGrantRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}

	w.Header().Set("Accept-Ranges", "bytes")
	offset, count, partial, ok := parseRange(r.Header.Get("Range"))
	if !ok {
		// Unparseable or unsupported Range: per RFC, ignore and serve full.
		partial = false
	}

	rc, ref, err := h.store.OpenGrantedRange(r.Context(), scope, in.Grant, offset, count)
	if err != nil {
		if errors.Is(err, artifacts.ErrRangeNotSatisfiable) {
			writeRangeNotSatisfiable(w, err)
			return
		}
		transport.WriteError(w, r, err)
		return
	}
	writeRangeBody(w, rc, ref, offset, count, partial)
}
