package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const profileTool = "write"

type effectArgs struct {
	EffectID  string `json:"effect_id"`
	LatencyMS int    `json:"latency_ms"`
	SizeBytes int    `json:"size_bytes"`
	HoldReply bool   `json:"hold_reply"`
}

type effectSnapshot struct {
	Calls        int   `json:"calls"`
	Unique       int   `json:"unique"`
	Duplicates   int   `json:"duplicates"`
	Unauthorized int   `json:"unauthorized"`
	StubP99NS    int64 `json:"stub_p99_ns"`
}

type effectRecorder struct {
	mu           sync.Mutex
	counts       map[string]int
	timings      [4096]int64
	timed        uint64
	calls        int
	unauthorized int
	debugURL     string
}

func (e *effectRecorder) count(id string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.counts[id]
}

func (e *effectRecorder) snapshot() effectSnapshot {
	e.mu.Lock()
	calls, unique, denied := e.calls, len(e.counts), e.unauthorized
	n := min(e.timed, uint64(len(e.timings)))
	samples := append([]int64(nil), e.timings[:n]...)
	e.mu.Unlock()
	slices.Sort(samples)
	var p99 int64
	if len(samples) != 0 {
		p99 = samples[(99*len(samples)+99)/100-1]
	}
	return effectSnapshot{Calls: calls, Unique: unique, Duplicates: calls - unique, Unauthorized: denied, StubP99NS: p99}
}

func (e *effectRecorder) held(ctx context.Context, id string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.debugURL+"/debug/ahp-profile/held", nil)
	if err != nil {
		return err
	}
	query := req.URL.Query()
	query.Set("effect_id", id)
	req.URL.RawQuery = query.Encode()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf("poll committed gateway reply: %w", err)
		}
		res.Body.Close()
		switch res.StatusCode {
		case http.StatusNoContent:
			return nil
		case http.StatusNotFound:
		default:
			return fmt.Errorf("poll committed gateway reply: HTTP %d", res.StatusCode)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

func startProfileMCP(canary string) (*effectRecorder, *http.Server, string, error) {
	e := &effectRecorder{counts: make(map[string]int)}
	server := mcp.NewServer(&mcp.Implementation{Name: "ahp-profile-loopback", Version: "1"}, nil)
	server.AddTool(&mcp.Tool{Name: profileTool, InputSchema: map[string]any{"type": "object"}},
		func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			started := time.Now()
			var args effectArgs
			dec := json.NewDecoder(bytes.NewReader(req.Params.Arguments))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&args); err != nil {
				return nil, errors.New("invalid profiling MCP arguments")
			}
			if args.EffectID == "" || len(args.EffectID) > 256 || args.LatencyMS < 0 || args.LatencyMS > 1000 ||
				args.SizeBytes < 0 || args.SizeBytes > 320<<10 {
				return nil, errors.New("profiling MCP arguments outside fixture bounds")
			}
			e.mu.Lock()
			e.counts[args.EffectID]++
			e.calls++
			e.mu.Unlock()
			processing := time.Since(started)
			if args.LatencyMS != 0 {
				timer := time.NewTimer(time.Duration(args.LatencyMS) * time.Millisecond)
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			ready := time.Now()
			result := &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: strings.Repeat("x", args.SizeBytes)}}}
			elapsed := (processing + time.Since(ready)).Nanoseconds()
			e.mu.Lock()
			e.timings[e.timed%uint64(len(e.timings))] = elapsed
			e.timed++
			e.mu.Unlock()
			return result, nil
		})
	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server },
		&mcp.StreamableHTTPOptions{JSONResponse: true})
	mux := http.NewServeMux()
	mux.Handle("/mcp", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		values := r.Header.Values("Authorization")
		if len(values) != 1 || subtle.ConstantTimeCompare([]byte(values[0]), []byte("Bearer "+canary)) != 1 {
			e.mu.Lock()
			e.unauthorized++
			e.mu.Unlock()
			http.Error(w, "MCP connector credential required", http.StatusUnauthorized)
			return
		}
		handler.ServeHTTP(w, r)
	}))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, "", err
	}
	httpServer := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	go func() { _ = httpServer.Serve(ln) }()
	return e, httpServer, "http://" + ln.Addr().String() + "/mcp", nil
}
