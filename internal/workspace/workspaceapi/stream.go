package workspaceapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/ahp-platform/ahp/internal/artifacts"
)

// parseRange parses a single "bytes=start-end" or "bytes=start-" range,
// mirroring the artifact download semantics. Suffix ranges are not supported.
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
		return 0, 0, true, false
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

func isRangeNotSatisfiable(err error) bool {
	return errors.Is(err, artifacts.ErrRangeNotSatisfiable)
}

func writeRangeNotSatisfiable(w http.ResponseWriter, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Range", "bytes */?")
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
