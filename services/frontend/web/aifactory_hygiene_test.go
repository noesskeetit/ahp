package web

import (
	"io/fs"
	"strings"
	"testing"
)

// The prototype was imported from a downloaded mock whose whole premise was "a demo that
// needs no platform": it kept state in localStorage, seeded a fake MODEL_API_KEY, showed a
// Git access-token field and printed api.ahp.example.test endpoints. The customer chose this
// front for the REAL product, and the director's ruling removed those four things from the
// source. This test is why they cannot come back: it reads the bytes that actually ship in
// the binary, so a re-import of the pristine zip fails here instead of shipping.
//
// It is an absence test on purpose (the same shape the evals verdict test uses): the claim is
// "these fields/strings do not exist", which no mock can fake.
func TestAIFactoryShipsNoBrowserStorageNoFakeCredentialsNoFakeEndpoints(t *testing.T) {
	forbidden := []struct {
		why  string
		want string
	}{
		{"the browser must not persist platform state or secrets", "localStorage"},
		{"the browser must not persist platform state or secrets", "sessionStorage"},
		{"a fabricated model credential is a secret-shaped lie", "MODEL_API_KEY"},
		{"a Git access-token field invites a real secret into a mock", "Токен доступа"},
		{"a fabricated endpoint is a success the platform never gave", "api.ahp.example.test"},
		{"a fabricated registry endpoint, same reason", "registry.example.com"},
	}
	names, err := fs.ReadDir(aifactoryFS, "aifactory")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, entry := range names {
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".js") && !strings.HasSuffix(entry.Name(), ".html") {
			continue
		}
		raw, err := fs.ReadFile(aifactoryFS, "aifactory/"+entry.Name())
		if err != nil {
			t.Fatal(err)
		}
		checked++
		text := string(raw)
		for _, f := range forbidden {
			if strings.Contains(text, f.want) {
				t.Errorf("aifactory/%s contains %q: %s", entry.Name(), f.want, f.why)
			}
		}
	}
	if checked < 2 {
		t.Fatalf("only %d browser files embedded: the check silently covers nothing", checked)
	}
}

// TestAIFactoryIndexLoadsTheLiveLayer pins the wiring the real legs depend on: the prototype
// renderer alone makes zero network calls, so the page is only a product client while
// ahp-live.js travels with it and is loaded after the renderer (it wraps the action
// dispatcher and the render pass, both of which must exist first).
func TestAIFactoryIndexLoadsTheLiveLayer(t *testing.T) {
	index, err := fs.ReadFile(aifactoryFS, "aifactory/index.html")
	if err != nil {
		t.Fatal(err)
	}
	body := string(index)
	appAt, liveAt := strings.Index(body, `src="app.js"`), strings.Index(body, `src="ahp-live.js"`)
	if appAt < 0 || liveAt < 0 {
		t.Fatalf("index.html must load app.js and ahp-live.js (app=%d live=%d)", appAt, liveAt)
	}
	if liveAt < appAt {
		t.Error("ahp-live.js is loaded before app.js: the live layer would wrap nothing")
	}
	live, err := fs.ReadFile(aifactoryFS, "aifactory/ahp-live.js")
	if err != nil {
		t.Fatal(err)
	}
	for _, marker := range []string{
		"/u-ahp/v1/auth/login", // F1 вход
		`"/v1/space/" + op`,    // F4/F7 ход и отмена идут через человеческую грань Space
		"AcceptTurn",
		"CancelTurn",
		"/u-ahp/v1/model/Cost",        // F6 стоимость дерева в токенах
		"/u-ahp/v1/artifacts/Resolve", // F5 digest -> артефакт
		"crypto.subtle",               // побайтовая сверка считается здесь, вне полосы
		"action_not_wired",            // не подключено = отказ с кодом, а не тихий успех
	} {
		if !strings.Contains(string(live), marker) {
			t.Errorf("ahp-live.js no longer calls %s: the human surface it proves is gone", marker)
		}
	}
	// The live layer speaks only to human faces. A machine face here would mean the browser
	// was handed a credential it is not allowed to hold.
	for _, forbidden := range []string{"/v1/state/", "/v1/artifacts/BeginUpload", "AS_", "VAULT"} {
		if strings.Contains(string(live), forbidden) {
			t.Errorf("ahp-live.js references %q: browser code must not reach machine faces", forbidden)
		}
	}
}
