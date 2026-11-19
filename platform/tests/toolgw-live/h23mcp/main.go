// Command h23mcp is a REAL MCP streamable-HTTP tool server used as the external
// tool the mounted Tool & Credential Gateway calls live. It is not a constant
// stub: the `record_note` tool performs a real, out-of-band-observable
// side effect — it appends a durable, monotonic record (argument echo + server
// sequence + nanosecond timestamp) to a JSONL effect log on disk. It is
// bearer-gated: a request that does not present the expected
// Authorization: Bearer <token> is refused 401 before any effect happens, so a
// successful gateway Call is itself proof the resolved (Vault) credential
// actually reached the tool server. Loopback-only out-of-band endpoints expose
// the effect ledger so a test can count real effects without trusting the
// gateway's own response.
package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type effect struct {
	Seq  int64  `json:"seq"`
	Note string `json:"note"`
	TS   string `json:"ts"`
}

type server struct {
	bearer    string
	logPath   string
	mu        sync.Mutex
	effects   []effect
	seq       int64
	effCount  int64 // total real effects performed
	lastAuth  atomic.Bool
	authSeen  int64
	unauth401 int64
}

func (s *server) authorized(r *http.Request) bool {
	const p = "Bearer "
	authz := strings.TrimSpace(r.Header.Get("Authorization"))
	if !strings.HasPrefix(authz, p) {
		return false
	}
	got := strings.TrimSpace(strings.TrimPrefix(authz, p))
	return subtle.ConstantTimeCompare([]byte(got), []byte(s.bearer)) == 1
}

func (s *server) appendEffect(note string) effect {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	s.effCount++
	e := effect{Seq: s.seq, Note: note, TS: time.Now().UTC().Format(time.RFC3339Nano)}
	s.effects = append(s.effects, e)
	if s.logPath != "" {
		if f, err := os.OpenFile(s.logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644); err == nil {
			b, _ := json.Marshal(e)
			_, _ = f.Write(append(b, '\n'))
			_ = f.Close()
		}
	}
	return e
}

func main() {
	var (
		addr   = flag.String("addr", "127.0.0.1:29841", "listen address (loopback only)")
		mcpP   = flag.String("path", "/mcp", "MCP streamable endpoint path")
		bearer = flag.String("bearer-env", "H23_MCP_BEARER", "env var holding the required bearer")
		logP   = flag.String("effect-log", "/tmp/h23-toolgw/mcp-effect-log.jsonl", "durable effect log path")
	)
	flag.Parse()
	cred := os.Getenv(*bearer)
	if cred == "" {
		log.Fatalf("h23mcp: %s must be set (required bearer)", *bearer)
	}
	s := &server{bearer: cred, logPath: *logP}
	_ = os.Remove(*logP)

	srv := mcp.NewServer(&mcp.Implementation{Name: "h23-live-toolserver", Version: "1"}, nil)
	// record_note: a real, side-effecting tool. It writes a durable record and
	// returns a text result carrying the server-side sequence so an observer can
	// distinguish one effect from two.
	srv.AddTool(&mcp.Tool{
		Name:        "record_note",
		Description: "Append a durable note to the server effect log and return its sequence.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"note":  map[string]any{"type": "string"},
				"nonce": map[string]any{"type": "string"},
			},
			"required": []string{"note"},
		},
	}, func(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var in struct {
			Note  string `json:"note"`
			Nonce string `json:"nonce"`
		}
		_ = json.Unmarshal(req.Params.Arguments, &in)
		e := s.appendEffect(in.Note)
		text := fmt.Sprintf("recorded seq=%d nonce=%s", e.Seq, in.Nonce)
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: text}}}, nil
	})
	// boom: performs the durable effect and THEN returns a Go error, so the
	// streamable client sees a transport-level failure (cerr != nil) on the
	// gateway side. For a SideEffecting spec the gateway cannot tell whether
	// the effect landed, so it marks SIDE_EFFECT_UNKNOWN and never auto-replays.
	srv.AddTool(&mcp.Tool{
		Name:        "boom",
		Description: "Performs a real effect then fails ambiguously (drives SIDE_EFFECT_UNKNOWN).",
		InputSchema: map[string]any{
			"type":       "object",
			"properties": map[string]any{"note": map[string]any{"type": "string"}},
		},
	}, func(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var in struct {
			Note string `json:"note"`
		}
		_ = json.Unmarshal(req.Params.Arguments, &in)
		s.appendEffect(in.Note) // effect happens, THEN the outcome goes ambiguous
		return nil, errors.New("boom: transport dropped after effect")
	})

	mux := http.NewServeMux()
	mcpHandler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return srv },
		&mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true})
	// Bearer gate for the MCP surface: no credential, no effect.
	mux.Handle(*mcpP, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.authorized(r) {
			atomic.AddInt64(&s.unauth401, 1)
			// record only a non-secret fingerprint of what arrived, for debugging
			const bp = "Bearer "
			az := strings.TrimSpace(r.Header.Get("Authorization"))
			rec := "none"
			if strings.HasPrefix(az, bp) {
				g := strings.TrimSpace(az[len(bp):])
				sum := sha256.Sum256([]byte(g))
				rec = fmt.Sprintf("got len=%d sha=%x", len(g), sum[:6])
			}
			log.Printf("h23mcp: 401 %s path=%s", rec, r.URL.Path)
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		s.lastAuth.Store(true)
		atomic.AddInt64(&s.authSeen, 1)
		mcpHandler.ServeHTTP(w, r)
	}))
	// Out-of-band observation endpoints (loopback only, not part of MCP).
	mux.HandleFunc("/__effect", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"count":   atomic.LoadInt64(&s.effCount),
			"entries": s.effects,
			"auth_ok": atomic.LoadInt64(&s.authSeen),
			"unauth":  atomic.LoadInt64(&s.unauth401),
		})
	})
	mux.HandleFunc("/__health", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("h23mcp: listen: %v", err)
	}
	log.Printf("h23mcp: listening addr=%s mcp=%s effect-log=%s", ln.Addr().String(), *mcpP, *logP)
	if err := http.Serve(ln, mux); err != nil {
		log.Fatalf("h23mcp: serve: %v", err)
	}
}
