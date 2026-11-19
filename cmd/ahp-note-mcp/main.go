// ahp-note-mcp is a small real MCP note service for operator-owned stand data.
// It durably appends notes, reloads them after restart, and requires the owner's
// bearer for every read/write. Provider/database credentials are never needed.
package main

import (
	"bufio"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type note struct {
	ID        string    `json:"id"`
	Text      string    `json:"text"`
	Nonce     string    `json:"nonce,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}
type notes struct {
	mu    sync.Mutex
	path  string
	items []note
}

func loadNotes(path string) (*notes, error) {
	n := &notes{path: path}
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return n, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	scan := bufio.NewScanner(f)
	scan.Buffer(make([]byte, 4096), 1<<20)
	for scan.Scan() {
		var row note
		if err := json.Unmarshal(scan.Bytes(), &row); err != nil {
			return nil, fmt.Errorf("note ledger is invalid")
		}
		n.items = append(n.items, row)
	}
	return n, scan.Err()
}
func (n *notes) append(text, nonce string) (note, error) {
	if len(text) == 0 || len(text) > 65536 || len(nonce) > 128 {
		return note{}, fmt.Errorf("note or nonce outside allowed bounds")
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if nonce != "" {
		for _, row := range n.items {
			if row.Nonce == nonce {
				if row.Text != text {
					return note{}, fmt.Errorf("nonce already used for different content")
				}
				return row, nil
			}
		}
	}
	row := note{ID: contracts.NewID(), Text: text, Nonce: nonce, CreatedAt: time.Now().UTC()}
	raw, err := json.Marshal(row)
	if err != nil {
		return note{}, err
	}
	f, err := os.OpenFile(n.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return note{}, fmt.Errorf("note ledger unavailable")
	}
	_, err = f.Write(append(raw, '\n'))
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		return note{}, fmt.Errorf("note durability not confirmed")
	}
	n.items = append(n.items, row)
	return row, nil
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "note MCP stopped:", err)
		os.Exit(1)
	}
}
func run() error {
	addr := os.Getenv("AHP_NOTE_MCP_LISTEN")
	if addr == "" {
		addr = "127.0.0.1:30841"
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil || host != "127.0.0.1" {
		return fmt.Errorf("loopback listen required")
	}
	bearer := os.Getenv("AHP_NOTE_MCP_BEARER")
	dir := os.Getenv("AHP_NOTE_MCP_DIR")
	if len(bearer) < 32 || dir == "" {
		return fmt.Errorf("owned bearer and notes directory required")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	ledger, err := loadNotes(filepath.Join(dir, "notes.jsonl"))
	if err != nil {
		return err
	}
	srv := mcp.NewServer(&mcp.Implementation{Name: "ahp-note-mcp", Version: "1.0.0"}, nil)
	srv.AddTool(&mcp.Tool{Name: "record_note", Description: "Durably append a note; optional nonce makes matching retries idempotent.", InputSchema: map[string]any{"type": "object", "properties": map[string]any{"note": map[string]any{"type": "string", "minLength": 1, "maxLength": 65536}, "nonce": map[string]any{"type": "string", "maxLength": 128}}, "required": []string{"note"}, "additionalProperties": false}}, func(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var in struct {
			Note  string `json:"note"`
			Nonce string `json:"nonce"`
		}
		if err := json.Unmarshal(req.Params.Arguments, &in); err != nil {
			return nil, fmt.Errorf("invalid note arguments")
		}
		row, err := ledger.append(in.Note, in.Nonce)
		if err != nil {
			return nil, err
		}
		raw, _ := json.Marshal(row)
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(raw)}}}, nil
	})
	srv.AddTool(&mcp.Tool{Name: "list_notes", Description: "Read the latest 100 notes owned by this credential.", InputSchema: map[string]any{"type": "object", "properties": map[string]any{}, "additionalProperties": false}}, func(_ context.Context, _ *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		ledger.mu.Lock()
		defer ledger.mu.Unlock()
		start := len(ledger.items) - 100
		if start < 0 {
			start = 0
		}
		raw, _ := json.Marshal(ledger.items[start:])
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(raw)}}}, nil
	})
	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return srv }, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true})
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	mux.Handle("/mcp", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || subtle.ConstantTimeCompare([]byte(got), []byte(bearer)) != 1 {
			http.Error(w, "unauthorized", 401)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		handler.ServeHTTP(w, r)
	}))
	server := http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second}
	return server.ListenAndServe()
}
