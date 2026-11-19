package web

import (
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// uiGet serves one request through Handler and returns status, content type and body.
func uiGet(t *testing.T, h http.Handler, method, path string) (int, string, string) {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("read body of %s %s: %v", method, path, err)
	}
	return rec.Result().StatusCode, rec.Result().Header.Get("Content-Type"), string(body)
}

// TestAIFactoryFlagServesTheChosenPrototype is the GREEN leg of the operator switch: with
// AHP_UI_AIFACTORY=1 the same binary serves the AI Factory prototype the customer picked,
// including the assets it actually loads.
func TestAIFactoryFlagServesTheChosenPrototype(t *testing.T) {
	t.Setenv(UIAIFactoryEnv, "1")
	h := Handler()

	status, _, index := uiGet(t, h, http.MethodGet, "/")
	if status != http.StatusOK {
		t.Fatalf("aifactory root status=%d", status)
	}
	if !strings.Contains(index, "AI Factory") {
		t.Fatalf("aifactory root is not the chosen prototype (title marker missing)")
	}

	for _, path := range []string{"/app.js", "/styles.css", "/assets/icons/icon-main-menu.svg"} {
		if status, _, body := uiGet(t, h, http.MethodGet, path); status != http.StatusOK || body == "" {
			t.Fatalf("aifactory asset %s: status=%d len=%d", path, status, len(body))
		}
	}

	// A client-side route inside the UI boundary still gets the shell, not a 404.
	if status, _, body := uiGet(t, h, http.MethodGet, "/stacks"); status != http.StatusOK || !strings.Contains(body, "AI Factory") {
		t.Fatalf("aifactory SPA route /stacks: status=%d", status)
	}
}

// TestAIFactoryFlagOffKeepsAcceptedUI pins what the director required: with no flag (or any
// value that is not exactly "1") the served bytes are the accepted web/static, so existing
// legs cannot drift onto the new front by accident.
func TestAIFactoryFlagOffKeepsAcceptedUI(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, req)
	defaultBody := rec.Body.String()

	for _, value := range []string{"", "0", "true", "yes", " on ", "1x"} {
		t.Setenv(UIAIFactoryEnv, value)
		rec := httptest.NewRecorder()
		Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		if got := rec.Body.String(); got != defaultBody {
			t.Fatalf("AHP_UI_AIFACTORY=%q changed the served UI; only the exact value 1 may", value)
		}
	}
	if !strings.Contains(defaultBody, "Agents Space") {
		t.Fatal("the default UI is no longer the accepted web/static shell")
	}
	if strings.Contains(defaultBody, "AI Factory") {
		t.Fatal("the default UI carries the aifactory shell: the two trees were merged")
	}
}

// TestAIFactoryKeepsTheAPIBoundary covers the shared serving policy for the new tree: a
// reserved namespace that no handler implements is answered as an API, never as HTML 200.
func TestAIFactoryKeepsTheAPIBoundary(t *testing.T) {
	t.Setenv(UIAIFactoryEnv, "1")
	h := Handler()

	for _, c := range []struct{ method, path string }{
		{http.MethodGet, "/v1/definitely-not-a-route"},
		{http.MethodGet, "/u-ahp/v1/definitely-not-a-route"},
		{http.MethodPost, "/v1/definitely-not-a-route"},
		{http.MethodGet, "/.well-known/agent-card.json"},
	} {
		status, ctype, body := uiGet(t, h, c.method, c.path)
		if status != http.StatusNotFound {
			t.Fatalf("%s %s: status=%d want 404", c.method, c.path, status)
		}
		if !strings.Contains(ctype, "application/json") || !strings.Contains(body, "NOT_FOUND") {
			t.Fatalf("%s %s answered %s %q: the shell leaked on an API path", c.method, c.path, ctype, body)
		}
	}
}

// TestAIFactoryDiskOverrideStillWins pins precedence with the deployment knob: a disk
// directory beats both embedded trees, so an operator can serve a built SPA without a rebuild.
func TestAIFactoryDiskOverrideStillWins(t *testing.T) {
	dir := t.TempDir()
	const marker = "<html><body>disk-built ui</body></html>"
	if err := os.WriteFile(filepath.Join(dir, "index.html"), []byte(marker), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(StaticDirEnv, dir)
	t.Setenv(UIAIFactoryEnv, "1")

	if _, _, body := uiGet(t, Handler(), http.MethodGet, "/"); body != marker {
		t.Fatalf("AHP_WEB_STATIC_DIR must win over the embedded aifactory UI")
	}
}

// TestEmbeddedAIFactoryMatchesTheRepositoryTree fails loudly when someone adds a browser
// file to web/aifactory without adding it to the embed list: a prototype that serves a
// half-imported front is worse than one that fails to build.
func TestEmbeddedAIFactoryMatchesTheRepositoryTree(t *testing.T) {
	const servedRoot = "aifactory"
	embedded, err := fs.Sub(aifactoryFS, servedRoot)
	if err != nil {
		t.Fatal(err)
	}

	notEmbeddedOnPurpose := map[string]bool{
		"README.md":    true,
		"design-qa.md": true,
	}

	var checked int
	err = filepath.WalkDir(servedRoot, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(servedRoot, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if notEmbeddedOnPurpose[rel] || strings.HasPrefix(rel, "evidence/") {
			return nil
		}
		want, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		got, err := fs.ReadFile(embedded, rel)
		if err != nil {
			t.Errorf("web/aifactory/%s exists on disk but is not embedded: add it to the //go:embed list", rel)
			return nil
		}
		if string(got) != string(want) {
			t.Errorf("web/aifactory/%s differs between disk and the embedded copy", rel)
		}
		checked++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if checked < 10 {
		t.Fatalf("only %d aifactory files were checked; the import is incomplete", checked)
	}
}
