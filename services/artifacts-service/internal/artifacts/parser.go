package artifacts

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"
	"unicode/utf8"
)

const (
	// MaxExtractBytes bounds both parser input and persisted extracted UTF-8.
	MaxExtractBytes = 2 << 20
	// MaxTextChunkBytes bounds one agent/API read independently of document size.
	MaxTextChunkBytes = 32 << 10
	parserRevision    = "utf8-json-v1"
)

// ParserCommand configures the trusted, sandboxing artifact-parser executable.
// It is not a user-provided shell command. The executable installs its own
// OS sandbox before consuming stdin. Unsupported platforms fail closed.
type ParserCommand struct {
	Path    string
	Timeout time.Duration
}

// WithParserCommand enables extraction on Seal using a dedicated subprocess.
func WithParserCommand(p ParserCommand) Option { return func(s *Store) { s.parser = p } }

func supportedText(mime string) bool {
	switch mime {
	case "text/plain", "text/markdown", "text/csv", "application/json":
		return true
	}
	return false
}

type cappedBuffer struct {
	bytes.Buffer
	limit int
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	if len(p) > b.limit-b.Len() {
		return 0, errors.New("parser output limit")
	}
	return b.Buffer.Write(p)
}

func (p ParserCommand) extract(ctx context.Context, mime string, input io.Reader) (string, error) {
	if p.Path == "" || !filepath.IsAbs(p.Path) {
		return "", errors.New("parser unavailable")
	}
	timeout := p.Timeout
	if timeout <= 0 || timeout > 10*time.Second {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	dir, err := os.MkdirTemp("", "ahp-artifact-parser-")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(dir)
	cmd := exec.CommandContext(ctx, p.Path, mime)
	cmd.Dir = dir
	cmd.Env = []string{"GOMAXPROCS=1", "GOMEMLIMIT=64MiB", "GODEBUG=netdns=go"}
	cmd.Stdin = io.LimitReader(input, MaxExtractBytes+1)
	out := &cappedBuffer{limit: MaxExtractBytes}
	cmd.Stdout = out
	cmd.Stderr = io.Discard
	cmd.WaitDelay = time.Second
	if err := cmd.Run(); err != nil {
		return "", errors.New("parser refused, timed out, or exceeded resource limits")
	}
	if !utf8.Valid(out.Bytes()) || bytes.IndexByte(out.Bytes(), 0) >= 0 {
		return "", errors.New("parser returned invalid text")
	}
	return out.String(), nil
}
