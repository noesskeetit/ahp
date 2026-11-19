//go:build !ahp_data

package main

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

// TestMemoryMountNotBuiltByDefault pins the default-build half of the gate: an
// untagged binary cannot serve /v1/memory at all, so enabling the plane can
// never be a runtime-only accident and the default composition stays free of
// internal/memory, internal/artifacts and minio-go.
func TestMemoryMountNotBuiltByDefault(t *testing.T) {
	s := &server{}
	err := s.tryMountMemory(context.Background(), http.NewServeMux(), nil, nil, authorityDevMode, nil)
	if !errors.Is(err, errFeatureNotBuilt) {
		t.Fatalf("err = %v, want errFeatureNotBuilt", err)
	}
}
