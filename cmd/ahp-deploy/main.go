package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/ahp-platform/ahp/internal/deploycontroller"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func main() {
	var (
		manifestPath = flag.String("manifest", "", "path to deployment manifest (JSON); '-' or empty = stdin")
		outDir       = flag.String("out", "", "write one JSON file per object into this directory")
		apply        = flag.Bool("apply", false, "apply to explicit Kubernetes context after server dry-run and wait for readiness")
		kubeContext  = flag.String("context", "", "explicit kubectl context for apply")
		validateOnly = flag.Bool("validate-only", false, "validate only, print nothing on success")
	)
	flag.Parse()

	raw, err := readManifest(*manifestPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "deploy: cannot read manifest: %v\n", err)
		os.Exit(2)
	}
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		fmt.Fprintf(os.Stderr, "deploy: manifest is not valid JSON: %v\n", err)
		os.Exit(2)
	}

	// Atomic: validate fully BEFORE generating anything. Any error -> refuse,
	// no partial objects (SC-delivery-01).
	if errs := m.Validate(); len(errs) > 0 {
		fmt.Fprintf(os.Stderr, "deploy: manifest INVALID, refusing deploy (%d error(s)), no objects generated:\n", len(errs))
		for _, e := range errs {
			fmt.Fprintf(os.Stderr, "  - %s\n", e)
		}
		os.Exit(1)
	}
	if *validateOnly {
		fmt.Println("manifest valid")
		return
	}

	if errs := m.ValidateRuntime(); len(errs) > 0 {
		fmt.Fprintf(os.Stderr, "deploy: runtime attachment invalid: %v\n", errs)
		os.Exit(1)
	}
	objects := m.Generate()
	if *apply {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if err := deploycontroller.Apply(ctx, *kubeContext, objects); err != nil {
			fmt.Fprintf(os.Stderr, "deploy: apply failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintln(os.Stderr, "deploy: objects applied; deployments ready")
		return
	}
	if *outDir != "" {
		if err := writeObjects(*outDir, objects); err != nil {
			fmt.Fprintf(os.Stderr, "deploy: write failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "deploy: wrote %d objects to %s\n", len(objects), *outDir)
		return
	}
	if err := emit(os.Stdout, objects); err != nil {
		fmt.Fprintf(os.Stderr, "deploy: emit failed: %v\n", err)
		os.Exit(1)
	}
}

func readManifest(path string) ([]byte, error) {
	if path == "" || path == "-" {
		return io.ReadAll(os.Stdin)
	}
	return os.ReadFile(path)
}

// emit writes a `---`-separated stream of JSON documents, which is a valid
// multi-document YAML stream for `kubectl apply -f -`.
func emit(w io.Writer, objects []map[string]any) error {
	for i, o := range objects {
		if i > 0 {
			if _, err := io.WriteString(w, "---\n"); err != nil {
				return err
			}
		}
		b, err := json.MarshalIndent(o, "", "  ")
		if err != nil {
			return err
		}
		if _, err := w.Write(b); err != nil {
			return err
		}
		if _, err := io.WriteString(w, "\n"); err != nil {
			return err
		}
	}
	return nil
}

func writeObjects(dir string, objects []map[string]any) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	for _, o := range objects {
		meta := o["metadata"].(map[string]any)
		kind := strings.ToLower(fmt.Sprint(o["kind"]))
		name := fmt.Sprint(meta["name"])
		b, err := json.MarshalIndent(o, "", "  ")
		if err != nil {
			return err
		}
		file := filepath.Join(dir, fmt.Sprintf("%s-%s.json", kind, name))
		if err := os.WriteFile(file, b, 0o644); err != nil {
			return err
		}
	}
	return nil
}

// helpers

func joinList(xs []string) string { return strings.Join(xs, ",") }
func itoa(n int) string           { return strconv.Itoa(n) }

func (l Limits) String() string {
	return fmt.Sprintf("depth=%d,task_timeout=%s,active_tasks=%d", l.Depth, l.TaskTimeout, l.ActiveTasks)
}
