package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The artifact face admits exactly the actions the artifact tier performs on the
// caller's own data, and that set IS the authorization boundary. "pin" and
// "restore" belong to a workload that owns its durable retention (both are bound
// to the verified scope inside the store); "gc" belongs to no request handler at
// all, because the collector runs on its own single-action store
// (cmd/ahp/mount_gc.go). Pinned at the source of the mount so a widening of the
// set has to be a deliberate edit to this gate as well.
func TestArtifactMountAdmitsPinAndRestoreAndRefusesGC(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(".", "role_data.go"))
	if err != nil {
		t.Fatalf("read role_data.go: %v", err)
	}
	src := string(raw)
	const anchor = "artifactActions := map[string]bool{"
	start := strings.Index(src, anchor)
	if start < 0 {
		t.Fatal(`role_data.go no longer builds an artifactActions set: re-read this gate instead of deleting it`)
	}
	block := src[start:]
	if end := regexp.MustCompile(`\n\t}`).FindStringIndex(block); end != nil {
		block = block[:end[0]]
	}
	admitted := map[string]bool{}
	for _, m := range regexp.MustCompile(`"([a-z]+)":\s*true`).FindAllStringSubmatch(block, -1) {
		admitted[m[1]] = true
	}
	// "grant"/"revoke" belong to the conversation that owns the bytes: they let
	// it admit one more of the SAME subject's conversations to read them, under
	// the store's own consent, ownership and label checks. They are not a
	// general read widening and they never move an artifact.
	for _, action := range []string{
		"pin", "restore", "grant", "revoke",
		"upload", "seal", "read", "delete", "retain", "hold", "purge", "publish", "derive",
	} {
		if !admitted[action] {
			t.Fatalf("the mount must admit %q: a workload owns this action on its own data (admitted=%v)", action, admitted)
		}
	}
	for _, refused := range []string{"gc", "sweep", "admin", "all", "authorize", "purge_all"} {
		if admitted[refused] {
			t.Fatalf("the mount must never admit %q: the sweep is not a request-handler action", refused)
		}
	}
}
