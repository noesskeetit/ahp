// Package web serves the Agents Space browser UI. The UI is a thin client of
// the real Space API (/v1/space/*); it is not a mock. Static assets are
// embedded so a single as-runner binary serves both the API and the UI.
package web

import (
	"embed"
	"io/fs"
	"net/http"
	"os"
	"strings"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

//go:embed static
var staticFS embed.FS

// aifactoryFS is the second, opt-in UI: the AI Factory prototype the customer chose as the
// prototype front. It is embedded separately and deliberately narrowly - only what a browser
// loads (index, app, styles, assets) - so design-QA screenshots and the prototype's own notes
// stay documentation on disk instead of bytes in every binary. web/static remains the default:
// with no flag set, Handler serves exactly the bytes it served before this file existed, which
// is what keeps the accepted UI legs meaningful.
//
//go:embed aifactory/index.html aifactory/app.js aifactory/ahp-live.js aifactory/artifact-upload.js aifactory/profile-admin.js aifactory/ahp-shapes.js aifactory/ahp-canonical.js aifactory/styles.css aifactory/assets
var aifactoryFS embed.FS

// reservedPrefixes are machine-facing namespaces that share this origin with the UI.
// A missing path inside one of them is a wrong API call, not a client-side route, so it has
// to be answered as an API (404 + contract code). Answering it with the app shell used to be
// possible and was measured on the live human edge: GET /.well-known/agent-card.json,
// /robots.txt and /api/definitely-not-a-route all returned 200 text/html with the 2678-byte
// index.html. That makes every "the endpoint answered" smoke meaningless (200 for a path nobody
// implemented) and makes an unmounted well-known document look served.
var reservedPrefixes = []string{"/v1/", "/api/", "/a2a/", "/.well-known/", "/healthz", "/metrics", "/u-ahp/", "/debug"}

// Two deliberate extensions of the list in the decision, both strictly stronger (they can never
// hide a route that exists) and both measured on the live human edge:
//
//   * case-insensitive matching. Go routing is case-sensitive, so /API/x and /V1/x reach the
//     catch-all exactly like /api/x and are equally unroutable; before this, /API/definitely-not-a-route
//     answered 200 text/html on the live edge even with the guard in place.
//   * /u-ahp/, the browser authority prefix the same-origin proxy uses for identity. On this
//     deployment no handler lives behind it, and POST /u-ahp/v1/auth/login was measured answering
//     200 text/html with the shell - a false success on an auth path, which is the worst possible
//     thing for a smoke to believe. The same path on the identity edge answers 403 application/json.
//
// If either is unwanted, say so and it comes out; nothing else in this file depends on them.
//
// A third extension, measured rather than guessed: /debug. GET /debug/pprof/ on the runner edge
// (127.0.0.1:18082, byte-identical shell) answered 200 text/html with the whole app shell, because
// the last segment of that path is empty and therefore not asset-shaped. A profiler endpoint that
// is not mounted must say "no such route"; answering it with a page is the same false 200 that made
// endpoint smokes meaningless. No client-side route lives under /debug — the four surfaces are
// /space, /workspace, /dot and /admin — so this can only ever refuse a path that was never a page.

// servesAsUI reports whether a path that has no matching file may receive the app shell.
//
// Only two things qualify, and both are needed: the request is a UI navigation (GET/HEAD — a
// POST to a UI route is not a page view), and the path is neither inside a machine-facing
// namespace nor shaped like an asset. "Asset-shaped" means a dot in the last segment
// (.js, .css, .json, .txt, .png): a browser asking for a missing asset must get a 404 it can
// act on, never an HTML body it will try to parse as a module.
func servesAsUI(r *http.Request) bool {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return false
	}

	path := r.URL.Path
	lowerPath := strings.ToLower(path)

	for _, prefix := range reservedPrefixes {
		if strings.HasSuffix(prefix, "/") {
			if strings.HasPrefix(lowerPath, prefix) {
				return false
			}

			continue
		}

		if lowerPath == prefix || strings.HasPrefix(lowerPath, prefix+"/") {
			return false
		}
	}

	lastSegment := path[strings.LastIndex(path, "/")+1:]

	return !strings.Contains(lastSegment, ".")
}

// StaticDirEnv points the UI at a directory on disk instead of the copy compiled into the binary.
//
// Why it exists: what `//go:embed static` compiles is a hand-written 2 678-byte placeholder page,
// while the built SPA is a 4 623-byte shell plus /assets/ bundles served from a separate origin.
// Without this knob the only way to show the real UI at the Space edge is to recompile the binary
// with a populated web/static (see sync-static.sh), which makes every UI change a Go build. With
// it, a deployment serves the bytes it actually built, and `AHP_WEB_STATIC_DIR` says which bytes
// those are - so "the stand shows a placeholder" stops being invisible.
//
// It fails closed on purpose: a configured directory with no index.html panics instead of quietly
// falling back to the embedded stub, because a stub that is silently substituted for the product is
// exactly the false impression this file exists to prevent.
const StaticDirEnv = "AHP_WEB_STATIC_DIR"

// UIAIFactoryEnv selects the AI Factory prototype as the served UI. Only the exact value "1"
// turns it on; anything else (including "true", "yes", or a typo) keeps the accepted web/static,
// so a mistyped operator flag cannot silently swap the front.
const UIAIFactoryEnv = "AHP_UI_AIFACTORY"

// aifactorySubdir is the tree inside aifactoryFS that Handler serves as root.
const aifactorySubdir = "aifactory"

// Handler returns an http.Handler serving the embedded UI at "/". Unknown paths fall
// back to index.html so client-side routing keeps working - but only within the boundary
// servesAsUI draws. Anything else that has no file behind it is answered as a machine:
// 404 with {"code":"NOT_FOUND"}, the same envelope every other AHP surface uses.
func Handler() http.Handler {
	if dir := strings.TrimSpace(os.Getenv(StaticDirEnv)); dir != "" {
		root := os.DirFS(dir)
		if _, err := fs.Stat(root, "index.html"); err != nil {
			panic("web: " + StaticDirEnv + " points at " + dir + ", which has no index.html: " + err.Error())
		}

		return uiHandler(root)
	}

	if strings.TrimSpace(os.Getenv(UIAIFactoryEnv)) == "1" {
		sub, err := fs.Sub(aifactoryFS, aifactorySubdir)
		if err != nil {
			panic("web: embedded aifactory dir missing: " + err.Error())
		}
		if _, err := fs.Stat(sub, "index.html"); err != nil {
			panic("web: the embedded aifactory UI has no index.html: " + err.Error())
		}

		return uiHandler(sub)
	}

	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic("web: embedded static dir missing: " + err.Error())
	}

	return uiHandler(sub)
}

// uiHandler is the whole serving policy over one file tree, whichever FS it came from. Keeping it
// shared is what makes the contract above true for a disk-served UI as well as the embedded one -
// a deployment knob that quietly skipped the reserved-prefix rule would reopen the false 200.
func uiHandler(root fs.FS) http.Handler {
	fileServer := http.FileServer(http.FS(root))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			if _, statErr := fs.Stat(root, r.URL.Path[1:]); statErr != nil {
				if !servesAsUI(r) {
					transport.WriteJSON(w, http.StatusNotFound, &contracts.Error{
						Code:    contracts.ErrNotFound,
						Message: "no route or asset at " + r.URL.Path,
					})

					return
				}
				// SPA fallback, within the UI boundary only.
				http.ServeFileFS(w, r, root, "index.html")

				return
			}
		}

		fileServer.ServeHTTP(w, r)
	})
}
