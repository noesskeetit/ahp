package artifactsapi

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
)

// Real Chromium + HTTP BFF metadata + mTLS Artifact API + PG/MinIO. The task
// credential stays in the Go server; only PUT capabilities cross to Chromium.
func TestBrowserDirectUploadRealMinIO(t *testing.T) {
	module := os.Getenv("AHP_PLAYWRIGHT_MODULE")
	if module == "" {
		t.Skip("requires explicit Playwright module")
	}
	e := newEnv(t)
	token := e.signTask(t, alice)
	proxy, err := space.NewUploadProxy(space.UploadProxyConfig{ArtifactsURL: e.srv.URL, Client: e.srv.Client()})
	if err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile(filepath.Join("..", "..", "web", "aifactory", "artifact-upload.js"))
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	ui := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			w.Header().Set("Content-Type", "text/html")
			w.Write([]byte(`<script src="/artifact-upload.js"></script>`))
		case "/artifact-upload.js":
			w.Header().Set("Content-Type", "text/javascript")
			w.Write(source)
		case space.UIUploadPath:
			calls++
			if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") || r.ContentLength > 4096 {
				t.Error("browser sent file bytes through BFF")
			}
			proxy.ServeHTTP(w, r.WithContext(space.WithScopedTask(r.Context(), token)))
		default:
			http.NotFound(w, r)
		}
	}))
	defer ui.Close()
	cmd := exec.Command("node", "testdata/direct-upload-browser.mjs", ui.URL, module)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("browser upload failed: %v (no credentials logged)", err)
	}
	var report struct {
		ID     string `json:"id"`
		SHA256 string `json:"sha256"`
		Puts   int    `json:"puts"`
		Bytes  int    `json:"bytes"`
	}
	if err := json.Unmarshal(out, &report); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || report.Puts != 1 || report.Bytes == 0 {
		t.Fatalf("wrong upload counts metadata=%d puts=%d bytes=%d", calls, report.Puts, report.Bytes)
	}
	status, body, _ := e.downloadRaw(t, token, mustJSON(t, map[string]any{"id": report.ID}), "")
	sum := sha256.Sum256([]byte(body))
	if status != 200 || len(body) != report.Bytes || hex.EncodeToString(sum[:]) != report.SHA256 {
		t.Fatal("uploaded object differs from browser file")
	}
	status, _, _ = e.downloadRaw(t, e.signTask(t, mallory), mustJSON(t, map[string]any{"id": report.ID}), "")
	if status == 200 {
		t.Fatal("foreign user downloaded browser upload")
	}
}
