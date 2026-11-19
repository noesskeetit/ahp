// artifact-parser consumes at most 2 MiB on stdin after installing a Linux
// syscall sandbox. It has no credentials, object-store client or file paths.
package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"runtime"
	"runtime/debug"
	"unicode/utf8"
)

const maxBytes = 2 << 20

func main() {
	runtime.GOMAXPROCS(1)
	debug.SetMemoryLimit(64 << 20)
	runtime.GC()
	if len(os.Args) != 2 || sandbox() != nil {
		os.Exit(2)
	}
	data, err := io.ReadAll(io.LimitReader(os.Stdin, maxBytes+1))
	if err != nil || len(data) > maxBytes || !utf8.Valid(data) || bytes.IndexByte(data, 0) >= 0 {
		os.Exit(3)
	}
	switch os.Args[1] {
	case "text/plain", "text/markdown", "text/csv":
	case "application/json":
		if !json.Valid(data) {
			os.Exit(3)
		}
	default:
		os.Exit(4)
	}
	if _, err = os.Stdout.Write(data); err != nil {
		os.Exit(5)
	}
}
