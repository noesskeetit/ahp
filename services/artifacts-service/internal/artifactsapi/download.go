package artifactsapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// download streams the exact sealed object version and honours a single
// HTTP byte range. It never returns a presigned URL: the bytes are proxied
// through the store, which re-runs the full label/authorization gate.
func (h *Handler) download(w http.ResponseWriter, r *http.Request) {
	ctx, scope, err := readRequest(r)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	var in idRequest
	if err := transport.ReadJSON(w, r, &in, maxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}

	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("ETag", quoteETag(in.ID))

	offset, count, partial, ok := parseRange(r.Header.Get("Range"))
	if !ok {
		// Unparseable or unsupported Range: per RFC, ignore and serve full.
		partial = false
	}

	rc, ref, err := h.store.OpenRange(ctx, scope, in.ID, offset, count)
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

// writeRangeBody streams the opened range and sets the ETag to the digest of
// the exact sealed version. Download and OpenDownloadGrant share it so both
// surfaces answer byte-for-byte the same way for the same version.
func writeRangeBody(w http.ResponseWriter, rc io.ReadCloser, ref artifacts.ArtifactRef,
	offset, count int64, partial bool) {
	defer func() { _ = rc.Close() }()

	w.Header().Set("ETag", quoteETag(ref.SHA256))
	w.Header().Set("Content-Type", "application/octet-stream")

	if partial {
		// end is absolute: an open-ended range (count == 0) runs to the last byte
		// of the object. Adding offset to size double-counted it and promised
		// offset+size bytes (measured: bytes=12- on 28B answered "bytes 12-39/28",
		// CL=28, sent 16 - the client waited for 12 bytes that do not exist).
		end := ref.Size - 1
		if count > 0 && offset+count-1 < end {
			end = offset + count - 1
		}
		w.Header().Set("Content-Range", "bytes "+
			strconv.FormatInt(offset, 10)+"-"+strconv.FormatInt(end, 10)+
			"/"+strconv.FormatInt(ref.Size, 10))
		w.Header().Set("Content-Length", strconv.FormatInt(end-offset+1, 10))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.Header().Set("Content-Length", strconv.FormatInt(ref.Size, 10))
		w.WriteHeader(http.StatusOK)
	}
	_, _ = io.Copy(w, rc)
}

// parseRange parses a single "bytes=start-end" or "bytes=start-" range.
// It returns offset, count (0 == to the end), whether a range was present,
// and whether the value was understood. Suffix ranges ("bytes=-N") are not
// understood and reported as not-ok.
func parseRange(v string) (offset, count int64, present, ok bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, 0, false, false
	}
	spec, found := strings.CutPrefix(v, "bytes=")
	if !found || strings.Contains(spec, ",") {
		return 0, 0, true, false
	}
	startStr, endStr, found := strings.Cut(spec, "-")
	if !found {
		return 0, 0, true, false
	}
	if startStr == "" {
		return 0, 0, true, false // suffix form not supported
	}
	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil || start < 0 {
		return 0, 0, true, false
	}
	if endStr == "" {
		return start, 0, true, true
	}
	end, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil || end < start {
		return 0, 0, true, false
	}
	return start, end - start + 1, true, true
}

// writeRangeNotSatisfiable emits 416 with a contract-shaped body; the generic
// transport mapper would collapse ErrRangeNotSatisfiable (code invalid) to 400.
//
// Content-Range names the complete length, as RFC 9110 requires: a resuming
// client learns the object shrank (or that it overshot) from that number and
// nothing else. Measured on the live stand: a 29 B object asked for
// "bytes=100000-100001" answered "bytes */?", which is not a length and left
// the client unable to recompute a range. The store has just read the size to
// make the 416 decision, so it carries it on the error.
func writeRangeNotSatisfiable(w http.ResponseWriter, err error) {
	contentRange := "bytes */?"
	var rns *artifacts.RangeNotSatisfiableError
	if errors.As(err, &rns) && rns.Size >= 0 {
		contentRange = "bytes */" + strconv.FormatInt(rns.Size, 10)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Range", contentRange)
	w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"code": "range_not_satisfiable", "message": err.Error(),
	})
}

func quoteETag(s string) string {
	if s == "" {
		return `""`
	}
	if strings.HasPrefix(s, `"`) {
		return s
	}
	return `"` + strings.Trim(s, `"`) + `"`
}
