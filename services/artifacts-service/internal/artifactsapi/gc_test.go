package artifactsapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// The HTTP shape of the storage GC gate: the sweep, the un-delete, the live
// reference registry and the audit trail are control-plane operations, the
// swept tenant is the caller's own and nowhere else, and the deletion plane
// refuses a credential the policy has not given those actions to.

func seconds(n int64) *int64 { return &n }

// rawObjectReader opens the same bucket the store was given, outside the API, so
// byte-level claims are checked against the object store itself.
func rawObjectReader(t *testing.T) (*minio.Client, string) {
	t.Helper()
	raw := os.Getenv("AHP_S3_ENDPOINT")
	if raw == "" {
		t.Fatal("AHP_S3_ENDPOINT must be set")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		t.Fatalf("bad AHP_S3_ENDPOINT: %v", err)
	}
	client, err := minio.New(u.Host, &minio.Options{
		Creds:  credentials.NewStaticV4(os.Getenv("AHP_MINIO_ROOT_USER"), os.Getenv("AHP_MINIO_ROOT_PASSWORD"), ""),
		Secure: u.Scheme == "https",
	})
	if err != nil {
		t.Fatalf("minio: %v", err)
	}
	return client, bucketForRun(t)
}

// bucketForRun resolves the bucket of the most recently built env. Fixtures
// create one bucket per test and register its name here, so an out-of-band
// check can read the very bucket the store under test was given.
var runBucket string

func bucketForRun(t *testing.T) string {
	t.Helper()
	if runBucket == "" {
		t.Fatal("no bucket registered")
	}
	return runBucket
}

func registerBucket(bucket string) { runBucket = bucket }

func TestHTTPGCRunsPurgeAndAudits(t *testing.T) {
	e := newEnvWithAuthorize(t, nil)
	tok := e.signBound(t, alice, "ctx-gc-1", "task-gc-1")
	id := e.uploadSeal(t, tok, "k-gc-1", "artifact-to-be-purged-by-the-sweep", labels.Set{})

	if code, body := e.call(t, "Delete", tok, mustJSON(t, map[string]any{"id": id, "key": "del-1"})); code != http.StatusOK {
		t.Fatalf("Delete: %d %v", code, body)
	}
	// Retention moved into the past: the control plane said this one may go.
	past := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
	if code, body := e.call(t, "SetRetention", tok,
		mustJSON(t, map[string]any{"id": id, "retain_until": past})); code != http.StatusOK {
		t.Fatalf("SetRetention: %d %v", code, body)
	}
	code, report := e.call(t, "GC", tok, mustJSON(t, map[string]any{
		"key": "http-sweep-1", "staged_grace_seconds": 60, "orphan_grace_seconds": 60,
	}))
	mustStatus(t, code, http.StatusOK, report)
	if intOf(t, report, "purged_tombstoned") != 1 {
		t.Fatalf("sweep report = %v", report)
	}
	if report["tenant"] != e.tenant {
		t.Fatalf("sweep reported a tenant it does not own: %v", report)
	}
	// The row was purged, so the id no longer resolves.
	if code, body := e.call(t, "Get", tok, mustJSON(t, map[string]any{"id": id})); code != http.StatusNotFound {
		t.Fatalf("purged artifact still readable: %d %v", code, body)
	}
	// The audit trail is readable over the same route family, carries the digest,
	// and never the content.
	code, audit := e.call(t, "GCAudit", tok, mustJSON(t, map[string]any{"artifact_id": id}))
	mustStatus(t, code, http.StatusOK, audit)
	raw, _ := json.Marshal(audit)
	if !strings.Contains(string(raw), `"event":"`+`purge`+`"`) {
		t.Fatalf("purge missing from the audit trail: %s", raw)
	}
	if strings.Contains(string(raw), "artifact-to-be-purged") {
		t.Fatalf("audit trail leaked artifact content: %s", raw)
	}
}

func TestHTTPGCSweepIsScopedToCallerTenant(t *testing.T) {
	e := newEnvWithAuthorize(t, nil)
	own := e.signBound(t, alice, "ctx-own", "task-own")
	victim := e.signBound(t, "bob", "ctx-victim", "task-victim")

	mine := e.uploadSeal(t, own, "k-own", "own-tenant-collectable", labels.Set{})
	theirs := e.uploadSeal(t, victim, "k-victim", "other-subject-collectable", labels.Set{})
	if code, body := e.call(t, "Delete", own, mustJSON(t, map[string]any{"id": mine, "key": "d-mine"})); code != http.StatusOK {
		t.Fatalf("Delete mine: %d %v", code, body)
	}
	// The other subject's artifact is deleted by its own subject: the write and
	// delete gates are the subject's, while the sweep is the tenant's control
	// plane acting over the whole tenant.
	if code, body := e.call(t, "Delete", victim, mustJSON(t, map[string]any{"id": theirs, "key": "d-theirs"})); code != http.StatusOK {
		t.Fatalf("Delete theirs: %d %v", code, body)
	}
	// Both tombstones get an elapsed floor, each by the subject that owns it, so
	// the only thing that could still keep a row is a rule of the sweep rather
	// than a number.
	past := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
	for _, tc := range []struct {
		token, id string
	}{{own, mine}, {victim, theirs}} {
		if code, body := e.call(t, "SetRetention", tc.token,
			mustJSON(t, map[string]any{"id": tc.id, "retain_until": past})); code != http.StatusOK {
			t.Fatalf("SetRetention %s: %d %v", tc.id, code, body)
		}
	}

	// A body cannot name a tenant, a subject, or a broader scope: the swept
	// tenant is the verified one, so an attempt to aim the sweep elsewhere is
	// simply unknown fields, which the strict decoder refuses.
	code, body := e.call(t, "GC", own, mustJSON(t, map[string]any{
		"tenant": "someone-else", "staged_grace_seconds": 60}))
	if code == http.StatusOK {
		t.Fatalf("GC accepted a scope field from the body: %v", body)
	}
	code, report := e.call(t, "GC", own, mustJSON(t, map[string]any{"staged_grace_seconds": 60}))
	mustStatus(t, code, http.StatusOK, report)
	if report["tenant"] != e.tenant {
		t.Fatalf("swept tenant = %v, want %v", report["tenant"], e.tenant)
	}
	// Both tombstones belong to the caller's tenant, so both are collected: the
	// boundary that matters here is the tenant, which is the caller's, and the
	// subject is the caller's subject's own data path (both artifacts were
	// written under this workload's identity).
	if intOf(t, report, "purged_tombstoned") != 2 {
		t.Fatalf("a tenant-wide sweep did not cover the whole tenant: %v", report)
	}
	_ = theirs
}

func TestHTTPPinsProtectAndRestoreReopens(t *testing.T) {
	e := newEnvWithAuthorize(t, nil)
	tok := e.signBound(t, alice, "ctx-pin-http", "task-pin-http")
	id := e.uploadSeal(t, tok, "k-pin-http", "bytes-a-live-run-still-needs", labels.Set{})
	if code, body := e.call(t, "Delete", tok, mustJSON(t, map[string]any{"id": id, "key": "del-pin"})); code != http.StatusOK {
		t.Fatalf("Delete: %d %v", code, body)
	}
	past := time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
	if code, body := e.call(t, "SetRetention", tok,
		mustJSON(t, map[string]any{"id": id, "retain_until": past})); code != http.StatusOK {
		t.Fatalf("SetRetention: %v", body)
	}
	if code, body := e.call(t, "PinRef", tok,
		mustJSON(t, map[string]any{"kind": "context", "ref_id": "ctx-pin-http"})); code != http.StatusOK {
		t.Fatalf("PinRef: %d %v", code, body)
	}
	code, pins := e.call(t, "ListPins", tok, mustJSON(t, map[string]any{}))
	mustStatus(t, code, http.StatusOK, pins)
	if list, _ := pins["pins"].([]any); len(list) != 1 {
		t.Fatalf("ListPins = %v", pins)
	}
	code, report := e.call(t, "GC", tok, mustJSON(t, map[string]any{"staged_grace_seconds": 60}))
	mustStatus(t, code, http.StatusOK, report)
	if intOf(t, report, "purged_tombstoned") != 0 || intOf(t, report, "skipped_live_reference") != 1 {
		t.Fatalf("pinned artifact was collected: %v", report)
	}
	// An explicit purge is refused for the same reason.
	if code, body := e.call(t, "Purge", tok, mustJSON(t, map[string]any{"id": id})); code != http.StatusConflict {
		t.Fatalf("Purge of a pinned artifact: %d %v", code, body)
	}
	// Un-delete inside the window: the artifact is readable again with the same
	// digest, and the sweep leaves it alone.
	code, ref := e.call(t, "Restore", tok, mustJSON(t, map[string]any{"id": id}))
	mustStatus(t, code, http.StatusOK, ref)
	if code, body := e.call(t, "Get", tok, mustJSON(t, map[string]any{"id": id})); code != http.StatusOK {
		t.Fatalf("restored artifact unreadable: %d %v", code, body)
	}
	if code, unpin := e.call(t, "UnpinRef", tok,
		mustJSON(t, map[string]any{"kind": "context", "ref_id": "ctx-pin-http"})); code != http.StatusOK {
		t.Fatalf("UnpinRef: %d %v", code, unpin)
	}
	// Re-tombstone and sweep: released and expired, so it goes.
	if code, body := e.call(t, "Delete", tok, mustJSON(t, map[string]any{"id": id, "key": "del-pin-2"})); code != http.StatusOK {
		t.Fatalf("Delete again: %d %v", code, body)
	}
	if code, body := e.call(t, "SetRetention", tok,
		mustJSON(t, map[string]any{"id": id, "retain_until": past})); code != http.StatusOK {
		t.Fatalf("SetRetention again: %d %v", code, body)
	}
	code, report = e.call(t, "GC", tok, mustJSON(t, map[string]any{"staged_grace_seconds": 60}))
	mustStatus(t, code, http.StatusOK, report)
	if intOf(t, report, "purged_tombstoned") != 1 {
		t.Fatalf("released tombstone was not collected: %v", report)
	}
}

func TestHTTPDeletionPlaneIsAdminGated(t *testing.T) {
	// A policy that grants the data-plane actions and refuses the deletion-plane
	// ones: exactly the line the PDF draws ("DeleteSubject, retention, legal
	// hold, Export - только control plane AHP", с. 34).
	deny := func(_ context.Context, _ contracts.Scope, action, _ string) error {
		switch action {
		case "gc", "restore", "pin":
			return contracts.NewError(contracts.ErrForbidden, "policy: control-plane action for this credential")
		}
		return nil
	}
	e := newEnvWithAuthorize(t, deny)
	tok := e.signBound(t, alice, "ctx-admin", "task-admin")
	id := e.uploadSeal(t, tok, "k-admin", "refused-across-the-admin-line", labels.Set{})

	for _, tc := range []struct {
		op   string
		body any
	}{
		{"GC", map[string]any{}},
		{"Restore", map[string]any{"id": id}},
		{"PinRef", map[string]any{"kind": "context", "ref_id": "ctx-admin"}},
		{"UnpinRef", map[string]any{"kind": "context", "ref_id": "ctx-admin"}},
	} {
		code, body := e.call(t, tc.op, tok, mustJSON(t, tc.body))
		if code != http.StatusForbidden {
			t.Fatalf("%s with a non-admin credential: %d %v", tc.op, code, body)
		}
	}
	// The data plane is untouched by the same policy.
	if code, body := e.call(t, "Get", tok, mustJSON(t, map[string]any{"id": id})); code != http.StatusOK {
		t.Fatalf("Get under the same policy: %d %v", code, body)
	}
}

func TestHTTPGCIgnoresForeignTenantBytes(t *testing.T) {
	e := newEnvWithAuthorize(t, nil)
	tok := e.signBound(t, alice, "ctx-fence", "task-fence")
	victimTok := e.signBound(t, "mallory", "ctx-victim-fence", "task-victim-fence")
	victim := e.uploadSeal(t, victimTok, "k-victim-fence", "another-subjects-journey-artifact", labels.Set{})
	code, vref := e.call(t, "Get", victimTok, mustJSON(t, map[string]any{"id": victim}))
	mustStatus(t, code, http.StatusOK, vref)

	garbage := e.uploadSeal(t, tok, "k-fence-garbage", "collectable-tombstone-of-the-caller", labels.Set{})
	if code, body := e.call(t, "Delete", tok, mustJSON(t, map[string]any{"id": garbage, "key": "del-fence"})); code != http.StatusOK {
		t.Fatalf("Delete: %d %v", code, body)
	}
	code, report := e.call(t, "GC", tok, mustJSON(t, map[string]any{"staged_grace_seconds": 60, "orphan_grace_seconds": 60}))
	mustStatus(t, code, http.StatusOK, report)

	// Out of band: the other subject's row and bytes are exactly as they were.
	var object, version, sha string
	err := isolatedPool(t).QueryRow(context.Background(), `
SELECT a.object, coalesce(a.sha256, ''), coalesce(a.version, '')
  FROM ahp_artifacts.artifacts a WHERE a.id = $1`, victim).Scan(&object, &sha, &version)
	if err != nil {
		t.Fatalf("victim row lookup: %v", err)
	}
	client, bucket := rawObjectReader(t)
	obj, err := client.GetObject(context.Background(), bucket, object, minio.GetObjectOptions{VersionID: version})
	if err != nil {
		t.Fatalf("victim object: %v", err)
	}
	buf := make([]byte, 0, 64)
	chunk := make([]byte, 32)
	for {
		n, rerr := obj.Read(chunk)
		buf = append(buf, chunk[:n]...)
		if rerr != nil {
			break
		}
	}
	_ = obj.Close()
	if string(buf) != "another-subjects-journey-artifact" {
		t.Fatalf("victim bytes changed: %q", buf)
	}
	if shaHex("another-subjects-journey-artifact") != sha {
		t.Fatalf("victim digest moved")
	}
	if code, body := e.call(t, "Get", victimTok, mustJSON(t, map[string]any{"id": victim})); code != http.StatusOK {
		t.Fatalf("victim lost its artifact: %d %v", code, body)
	}
}
