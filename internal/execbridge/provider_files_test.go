package execbridge

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/transport"
)

func TestProviderFilesExplicitlyUnsupportedWithoutDriver(t *testing.T) {
	b, err := New(context.Background(), dbPool(t), Config{Inference: &fakeInference{text: "unchanged", usage: modelUsage()}})
	if err != nil {
		t.Fatal(err)
	}
	scope := uniqueScope()
	for _, op := range []string{"CopyFile", "GetFile", "DeleteFile"} {
		r := httptest.NewRequest(http.MethodPost, "/v1/model/"+op, bytes.NewBufferString(`{"key":"copy","artifact_id":"sealed"}`))
		claims := auth.Claims{Kind: auth.KindTask, Tenant: scope.Tenant, Subject: scope.Subject, Deployment: scope.Deployment, Task: "task", Context: "context"}
		r = r.WithContext(transport.WithPrincipal(r.Context(), transport.Principal{Scope: scope, Claims: claims}))
		w := httptest.NewRecorder()
		b.serveModel(w, r)
		if w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), "provider_files_not_supported") {
			t.Fatalf("%s: status=%d body=%s", op, w.Code, w.Body.String())
		}
	}
}
