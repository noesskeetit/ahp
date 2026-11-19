package routing_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ahp-platform/ahp/internal/model/routing"
)

// writeFileAll is a tiny helper kept separate so the test file above stays about
// routing.
func writeFileAll(path, body string) error {
	return os.WriteFile(path, []byte(body), 0o600)
}

// moduleRoot walks up from the package directory to the module root, so the
// copy guard works from any checkout (mine, the canonical one, a CI clone).
func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("wd: %v", err)
	}
	for i := 0; i < 12; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		dir = filepath.Dir(dir)
	}
	t.Fatal("could not find the module root")
	return ""
}

// loadProviderTable reads the config point through the product parser, so the
// guard validates the same bytes the gateway would load.
func loadProviderTable(path string) (*routing.Table, error) {
	return routing.LoadFile(path)
}
