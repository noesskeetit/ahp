package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/artifactsapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// newArtifactCompatibilityHandler keeps the development compute-host's GET
// download URL while using the same owner API as the composed AHP binary.
// It does not add live grant enforcement to this explicitly permissive host.
func newArtifactCompatibilityHandler(store *artifacts.Store, verifier *auth.Verifier, authorize transport.Authorize) (http.Handler, error) {
	owner, err := artifactsapi.New(store, verifier, authorize)
	if err != nil {
		return nil, err
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			id, ok := strings.CutPrefix(r.URL.Path, "/v1/artifacts/")
			if !ok || id == "" || len(id) > 256 || strings.Contains(id, "/") {
				http.NotFound(w, r)
				return
			}
			body, _ := json.Marshal(struct {
				ID string `json:"id"`
			}{ID: id})
			// Clone preserves the original TLS principal, Authorization, context
			// and Range. Owner middleware verifies them before installing the
			// caller context and running the normal Download admission checks.
			r = r.Clone(r.Context())
			r.Method = http.MethodPost
			r.URL.Path = "/v1/artifacts/Download"
			r.URL.RawPath, r.URL.RawQuery = "", ""
			r.RequestURI = r.URL.RequestURI()
			r.Header.Set("Content-Type", "application/json")
			r.Body = io.NopCloser(bytes.NewReader(body))
			r.ContentLength = int64(len(body))
		}
		// The legacy multipart /exports route is deliberately not mounted.
		owner.ServeHTTP(w, r)
	}), nil
}
