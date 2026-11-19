package main

// Single-origin human edge: the browser talks to ONE origin, the composed edge.
//
// Measured before this file existed: the user's SPA came from the edge (:18081)
// while the Space turn stream and the artifact download live on the as-runner
// (:18082). A browser cannot keep a session across two origins it was never
// told about, and the download leg answered 403/404 depending on which origin
// a page happened to call - the same fact the accept-origin probe measures as
// "spa_index + space 404" on the edge and "range_416_shape" on the runner.
//
// The director chose wiring (b) (post 20261101T031844Z-dir-single-origin-b-
// wiring-deputy): the edge PROXIES the user-facing Surface to the runner. The
// rejected alternative was to mount a second Space/Data instance in the edge
// process with AHP_ENABLE_DATA/AHP_ENABLE_EXECUTION - it collides on the
// registry bind and puts a second Space in front of the authority chain, which
// is a power boundary, not a style preference.
//
// So: no second Space instance, no second Data instance, no new listen port.
// The edge forwards method, query, Authorization, Range and body untouched and
// copies the upstream status, headers and body back verbatim - including 403,
// 400, 404 and 416 with Accept-Ranges/Content-Range/Content-Length. A 403 from
// the runner must arrive as a 403: if the edge rewrote it, the "another user
// cannot download this" leg of the acceptance would be measuring the proxy
// instead of the authorization gate.

import (
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
)

// envSingleOrigin names the runner origin to proxy to. Unset means the edge
// mounts nothing: every accepted composition that does not set it answers
// exactly as it did before this file.
const envSingleOrigin = "AHP_SINGLE_ORIGIN_UPSTREAM"

// The two user-facing prefixes the edge forwards, and the root the SPA is
// served from. They are literals, not space.UIDownloadPath, on purpose: the
// untagged edge build must not start importing internal/space (see the mount
// order comment in serveAll about what the untagged binary is allowed to
// link). internal/space/ui_download.go:47 is the source of the download path;
// single_origin_test.go pins all three strings against that file.
const (
	singleOriginSpacePrefix     = "/v1/space/"
	singleOriginSpaceExact      = "/v1/space"
	singleOriginArtifactsPrefix = "/u-ahp/v1/artifacts/"
	singleOriginArtifactsExact  = "/u-ahp/v1/artifacts"
	// singleOriginWorkspacePrefix carries the user's upload-into-a-project call to the origin that
	// owns the Workspace face. The measurement that made this necessary is on the record in two
	// places: on the acceptance contour at 15:01Z the same task token got 200 from the aggregate for
	// workspace/Create + CreateCandidate + UpdateCandidateFiles while the OWNER's own
	// /v1/workspace/GetWorkspace through the human edge answered 404 (the SPA catch-all), and the
	// data plane mounts the face at role_data.go:61. The prefix is pinned against that mount by
	// TestSingleOriginRoutesMatchTheSurfaceTheyProxy, so if the data-plane mount moves, the build
	// fails loudly instead of the route quietly becoming a dead end again.
	singleOriginWorkspacePrefix = "/v1/workspace/"
	singleOriginWorkspaceExact  = "/v1/workspace"
	// The only two artifact operations the edge will exchange a token for. Named
	// individually on purpose: the upload leg of the user chain needs these and
	// nothing else from that face.
	singleOriginArtifactsBeginUpload = "/v1/artifacts/BeginUpload"
	singleOriginArtifactsSeal        = "/v1/artifacts/Seal"
	// singleOriginComputePrefix forwards the Compute API on the same origin.
	// Without it the browser is told a plane exists that it cannot reach from
	// the only origin it speaks to, and every UI leg that watches an isolated
	// command dies on an HTML 404. Pinned against
	// internal/compute/computeapi/api.go by role_compute_test.go.
	singleOriginComputePrefix = "/v1/compute/"
	// singleOriginPlanePrefixes forward the four human plane surfaces an operator
	// front needs: Registry (choose/publish/install), Evals (the verdict a
	// publication needs), Triggers (schedules) and Memory (lifecycle and search).
	// They belong to the RUNNER and not to this process, because only the runner
	// turns a human session into a scoped task token (cmd/as-runner/main.go mounts
	// them, internal/space/ui_planes.go implements them); the edge registers them so
	// a browser keeps ONE origin. Named one by one instead of as /u-ahp/v1/ because
	// this process answers /u-ahp/v1/auth/* itself and must not hand that to a proxy.
	// A deployment whose runner mounts none of these answers with the runner's own
	// API-shaped 404, which the UI shows as unavailable instead of as success.
	singleOriginPlanePrefixes = "/u-ahp/v1/profile/ /u-ahp/v1/registry/ /u-ahp/v1/evals/ /u-ahp/v1/triggers/ /u-ahp/v1/memory/ /u-ahp/v1/compute/ /u-ahp/v1/workspace/ /u-ahp/v1/tools/"
	// singleOriginCostPrefix forwards the read-only human cost face (PDF rev3.2 :347
	// "стоимость задачи"). Measured on a composed pair before this line existed: the RUNNER
	// mounts /u-ahp/v1/model/Cost (internal/space/ui_cost.go) while the edge answers
	// "no route at /u-ahp/v1/model/Cost", because the four plane prefixes above do not cover
	// it and the machine face /v1/model/TreeCost refuses a browser token (audience
	// ahp-browser). So a person on the single origin could not read the cost of their own
	// task at all. Forwarded, not owned: the exchange and the tenant stay the runner's.
	singleOriginCostPrefix = "/u-ahp/v1/model/"
)

// singleOriginPlanePrefixList is singleOriginPlanePrefixes split for registration.
func singleOriginPlanePrefixList() []string {
	return strings.Fields(singleOriginPlanePrefixes)
}

// mountSingleOrigin wires the edge as the single origin a browser sees. It
// returns true when the proxy is live.
//
// browserMounted is required: without the Identity browser face the edge
// cannot answer /u-ahp/v1/auth/* itself, and forwarding that prefix to a runner
// would half-solve login behind a proxy instead of refusing. Fail closed and
// say why.
func mountSingleOrigin(mux *http.ServeMux, raw string, browserMounted bool, logger *slog.Logger) bool {
	return mountSingleOriginEdge(mux, raw, browserMounted, false, false, logger)
}

// mountSingleOriginWithPlane is mountSingleOrigin for a process that may also own
// the data plane on the SAME mux. It keeps the two-planes call shape the data-plane
// tests and callers use and delegates the route table to mountSingleOriginEdge.
func mountSingleOriginWithPlane(mux *http.ServeMux, raw string, browserMounted, dataPlaneMounted bool, logger *slog.Logger) bool {
	return mountSingleOriginEdge(mux, raw, browserMounted, dataPlaneMounted, false, logger)
}

// The data plane obeys the same rule, and it got here first.
//
// The measurement that forced that parameter: an aggregate asked to be the human
// origin and the artifact/workspace plane at once died at startup with
// `panic: pattern "/v1/workspace/" ... conflicts with pattern "/v1/workspace/"`
// (single_origin.go vs role_data.go:61). http.ServeMux refuses two owners of one
// prefix, and it is right to: forwarding a prefix this process already serves
// would send the caller's own face through a token exchange and an HTTP hop to
// somebody else. So when this process owns the data plane the edge registers
// neither the Workspace prefix nor the two artifact upload operations - the
// local mounts answer - and says so in the journal instead of vanishing.
// mountSingleOriginEdge is the edge itself.
//
// computePlaneMounted carries the ownership decision for /v1/compute/ (DEF-COMPUTE-1),
// and the rule is the same one the data plane measurement established: a prefix has
// exactly ONE owner per process, and the owner is the process that holds the store
// behind the route. For Compute that is whichever process mounted computeapi
// (cmd/ahp/role_compute.go): the compute store is a Postgres schema this process
// opens (compute.New(s.pool) + Migrate), the jail base is a directory on this host,
// and the optional Firecracker socket is a local path -- none of which a proxy hop
// to a runner could honour. So when this process mounted the plane the edge
// registers NO handler for the prefix and the local face answers; when it did not,
// the prefix keeps belonging to the runner and is forwarded exactly as before, which
// is what TestSingleOriginStillForwardsComputeWhenItIsNotLocal pins. The alternative
// ownership (the runner serves Compute and this process leaves the prefix alone) is
// still expressible: it is simply AHP_ENABLE_COMPUTE unset here.
//
// Why the decision has to live in this function: http.ServeMux refuses two owners of
// one pattern by panicking at registration, and that is what the stand paid for --
// the composed front with AHP_ENABLE_COMPUTE=1 died at startup with
// `panic: pattern "/v1/compute/" ... conflicts with pattern "/v1/compute/"`, one
// optional plane taking the whole canonical origin down. The panic is a legitimate
// enforcer, so the test pins the rule (TestOnlyOneOwnerOfTheComputePrefix) instead of
// the stand discovering it at boot.
func mountSingleOriginEdge(mux *http.ServeMux, raw string, browserMounted, dataPlaneMounted, computePlaneMounted bool, logger *slog.Logger) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	upstream, err := url.Parse(raw)
	switch {
	case err != nil:
		logger.Warn("single-origin proxy NOT mounted: AHP_SINGLE_ORIGIN_UPSTREAM is unparsable", "err", err)
		return false
	case upstream.Host == "":
		logger.Warn("single-origin proxy NOT mounted: AHP_SINGLE_ORIGIN_UPSTREAM must be an origin, not a path", "value", raw)
		return false
	case upstream.Scheme != "http" && upstream.Scheme != "https":
		logger.Warn("single-origin proxy NOT mounted: AHP_SINGLE_ORIGIN_UPSTREAM scheme must be http or https", "scheme", upstream.Scheme)
		return false
	case upstream.Path != "" && upstream.Path != "/":
		// A path here would be prepended to the caller's path, which silently
		// rewrites every route on the runner. Refuse rather than guess.
		logger.Warn("single-origin proxy NOT mounted: AHP_SINGLE_ORIGIN_UPSTREAM must not carry a path", "path", upstream.Path)
		return false
	case !browserMounted:
		logger.Warn("single-origin proxy NOT mounted: the Identity browser face is absent, so this edge cannot serve the whole user surface (AHP_ENABLE_BROWSER=1 is required)")
		return false
	}

	proxy := &httputil.ReverseProxy{
		// The turn stream is a live stream; buffering it until EOF would hang
		// the frame-by-frame UI (and the artifact_ref_from_stream probe) until
		// the turn finished.
		FlushInterval: -1,
		Director: func(req *http.Request) {
			req.URL.Scheme = upstream.Scheme
			req.URL.Host = upstream.Host
			// Path, RawQuery and the Host header stay exactly as the browser
			// sent them. Path/RawQuery because the runner owns those route
			// contracts; Host because this origin is the ONLY one the browser
			// speaks to - stamping the upstream host here would let any
			// absolute URL the runner builds hand the user a loopback origin
			// (127.0.0.1:18082) that is not theirs. Nothing in the runner's
			// authorization depends on Host (it is bearer token + labels), so
			// forwarding the caller's authority costs no gate.
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			// 502, never 404: an unreachable runner is not "no such artifact",
			// and a client that sees 404 here would cache a missing object.
			logger.Warn("single-origin upstream unreachable", "path", r.URL.Path, "err", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"code":"unavailable","message":"the runner origin is not reachable"}` + "\n"))
		},
	}

	mux.Handle(singleOriginSpaceExact, proxy)
	mux.Handle(singleOriginSpacePrefix, proxy)
	mux.Handle(singleOriginArtifactsExact, proxy)
	mux.Handle(singleOriginArtifactsPrefix, proxy)
	// One owner per prefix (DEF-COMPUTE-1): see the contract on
	// mountSingleOriginEdge. Registering this proxy line unconditionally beside a
	// local compute mount is the panic the stand measured.
	if !computePlaneMounted {
		mux.Handle(singleOriginComputePrefix, proxy)
	}
	for _, prefix := range singleOriginPlanePrefixList() {
		mux.Handle(prefix, proxy)
	}
	mux.Handle(singleOriginCostPrefix, proxy)
	// The Workspace prefix is forwarded, not owned -- and the distinction is
	// measured, not assumed. On the flipped acceptance pair (14:50Z) the runner
	// behind AHP_SINGLE_ORIGIN_UPSTREAM answers /v1/space/AcceptTurn 403 and
	// /u-ahp/v1/artifacts/download 403 (it owns both) but /v1/workspace/GetHead
	// 404, the identical answer it gives an unknown path. The Workspace face
	// lives on the aggregate behind mTLS + a task token, which this proxy cannot
	// present (single_origin_test.go: TestSingleOriginCannotFrontAnMTLSWorkspace
	// Face measures the 502). So forwarding makes the route REACHABLE at the
	// edge and honest about its upstream; it does not by itself put a
	// user-authenticated Workspace face in front of a browser. Until an origin
	// the edge legitimately proxies to owns that face, the upload leg runs
	// against the origin that does (scripts/asws-acceptance-leg.sh) and the edge
	// mirrors a 404 rather than pretending otherwise.
	// Exactly one handler may own the Workspace prefix: http.ServeMux panics on a
	// duplicate pattern (measured), so the choice has to be made here rather than
	// by registering twice and hoping the last one wins.
	workspaceEdge := newWorkspaceEdge(logger)
	switch {
	case dataPlaneMounted:
		logger.Info("single-origin edge: the Workspace prefix and the artifact upload operations belong to this process's own data plane; the edge forwards neither",
			"workspace", singleOriginWorkspacePrefix,
			"artifact_operations", []string{singleOriginArtifactsBeginUpload, singleOriginArtifactsSeal})
	case workspaceEdge != nil:
		workspaceBoundary := workspaceEdge.forBoundary(boundaryWorkspace)
		mux.Handle(singleOriginWorkspaceExact, workspaceBoundary)
		mux.Handle(singleOriginWorkspacePrefix, workspaceBoundary)
		// The named user chain is "upload a file into the project -> run a turn over
		// it -> download the artifact", and staging a file into a project takes two
		// artifact operations first: BeginUpload and Seal. They live on the same
		// aggregate as the Workspace face (measured: 403 there, 404 on the runner),
		// so fronting only /v1/workspace/ would leave the chain broken before it
		// reached a workspace at all. Exactly these two operations are forwarded --
		// not the /v1/artifacts/ prefix -- because the edge has no claim on the rest
		// of that face, and single_origin_test.go pins that every other artifact
		// route still goes to the runner. The pair exchanges under its OWN task
		// prefix and capability: the contract rejects a merged root, so staging a
		// file must never end up holding workspace authority, or the reverse.
		artifactBoundary := workspaceEdge.forBoundary(boundaryArtifactUpload)
		mux.Handle(singleOriginArtifactsBeginUpload, artifactBoundary)
		mux.Handle(singleOriginArtifactsSeal, artifactBoundary)
		logger.Info("single-origin edge: the Workspace prefix and the two artifact upload operations exchange the caller's human token for a task token and forward to the origin that owns them",
			"origin", envOr(envWorkspaceOrigin, ""), "exchange", envOr(envWorkspaceExchange, ""))
	default:
		mux.Handle(singleOriginWorkspaceExact, proxy)
		mux.Handle(singleOriginWorkspacePrefix, proxy)
	}
	mux.Handle("/", spaOnly(proxy))

	// The journal must not claim to forward a prefix this process answers. Before
	// this fix the same line listed /v1/compute/ while the local plane owned it,
	// which is the kind of log an operator trusts once and then cannot debug with.
	forwarded := []string{singleOriginSpacePrefix, singleOriginArtifactsPrefix}
	computeOwnership := "forwarded to the runner origin"
	if computePlaneMounted {
		computeOwnership = "served by this process"
	} else {
		forwarded = append(forwarded, singleOriginComputePrefix)
	}
	forwarded = append(forwarded, singleOriginPlanePrefixList()...)
	forwarded = append(forwarded, singleOriginCostPrefix, singleOriginWorkspacePrefix, "/")

	logger.Info("single-origin edge: Space, artifact download, the human plane surfaces, the Workspace prefix and the SPA are proxied to the runner origin",
		"upstream", upstream.Scheme+"://"+upstream.Host,
		"compute", computeOwnership,
		"prefixes", strings.Join(forwarded, " "))
	return true
}

// singleOriginAPIPrefixes are the machine-facing namespaces that share this one
// origin with the SPA. A path with no handler inside one of them is a wrong API
// call, not a client-side route, and must be answered as an API (404 + the
// contract envelope) rather than with index.html + 200, which every client reads
// as success. This list is deliberately the SAME rule the runner's own SPA
// handler enforces (web/embed.go reservedPrefixes): a single origin has to apply
// a single policy, or the edge and the runner disagree and a miss on one answers
// as a page on the other. It is extended with /u-api/, the browser BFF namespace
// the browser leg actually calls -- GET /u-api/bff-console/v2/customers/me was
// measured coming back 200 text/html with the app shell, the exact false success
// this guard exists to prevent -- and /readyz, which the edge already refused.
//
// It is a strictly-stronger refusal set over what the edge forwards: every prefix
// here is either refused (no handler owns it) or is served by a MORE SPECIFIC
// handler already registered on the mux (a real /u-ahp/v1/* identity face, the
// proxied /v1/space/ and /u-ahp/v1/artifacts/ and /v1/compute/ and /v1/workspace/
// prefixes, the mounted /a2a/* and /v1/* planes), so widening this list can never
// shadow a route that exists -- http.ServeMux hands those to their own handler and
// only the leftovers reach this catch-all. The proxied upstreams are untouched.
var singleOriginAPIPrefixes = []string{
	"/v1/", "/api/", "/u-api/", "/a2a/", "/u-ahp/", "/.well-known/",
	"/healthz", "/readyz", "/metrics", "/debug",
}

// spaOnlyRejectsAsAPI reports whether an otherwise-unowned path belongs to a
// machine-facing namespace and must be answered 404 rather than receive the SPA.
// Matching is case-insensitive because Go routing is case-sensitive and /API/x,
// /V1/x reach the catch-all exactly like their lowercase twins; a differently
// cased machine path is just as unroutable and must not get the app shell. No
// asset-shape heuristic lives here: unlike the runner, which checks its own file
// tree first, the edge is a proxy and cannot know whether /assets/app.js exists
// upstream, so it forwards asset and page paths alike and refuses only namespaces.
func spaOnlyRejectsAsAPI(path string) bool {
	lower := strings.ToLower(path)
	for _, prefix := range singleOriginAPIPrefixes {
		root := strings.TrimSuffix(prefix, "/")
		if lower == root || strings.HasPrefix(lower, root+"/") {
			return true
		}
	}
	return false
}

// spaOnly serves the proxied SPA for page routes and refuses API routes that
// nothing owns. Without it an unknown /v1/... path would come back 200 with
// index.html, which every client on earth reads as a successful API call.
func spaOnly(proxy http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if spaOnlyRejectsAsAPI(r.URL.Path) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"code":"NOT_FOUND","message":"route not found"}` + "\n"))
			return
		}
		proxy.ServeHTTP(w, r)
	})
}
