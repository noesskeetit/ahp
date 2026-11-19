//go:build state_integration

package space_test

import (
	"net/http"
	"strings"
	"sync"
	"time"
)

// Counts only protocol work, not request bodies, tokens or generated text.
// Used to locate the large-receipt recovery delay without changing its budget.
type largeRecoveryRPCs struct {
	mu  sync.Mutex
	ops map[string]largeRecoveryRPC
}
type largeRecoveryRPC struct {
	Calls  int   `json:"calls"`
	Bytes  int64 `json:"bytes"`
	Millis int64 `json:"millis"`
}
type largeRecoveryWriter struct {
	http.ResponseWriter
	bytes int64
}

func (w *largeRecoveryWriter) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.bytes += int64(n)
	return n, err
}
func (w *largeRecoveryWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
func (p *largeRecoveryRPCs) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		count := &largeRecoveryWriter{ResponseWriter: w}
		next.ServeHTTP(count, r)
		op := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.ops == nil {
			p.ops = make(map[string]largeRecoveryRPC)
		}
		s := p.ops[op]
		s.Calls++
		s.Bytes += count.bytes
		s.Millis += time.Since(started).Milliseconds()
		p.ops[op] = s
	})
}
func (p *largeRecoveryRPCs) snapshot() map[string]largeRecoveryRPC {
	p.mu.Lock()
	defer p.mu.Unlock()
	r := make(map[string]largeRecoveryRPC, len(p.ops))
	for k, v := range p.ops {
		r[k] = v
	}
	return r
}
