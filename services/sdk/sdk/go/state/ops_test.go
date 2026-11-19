package state

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestNewOperations(t *testing.T) {
	m := newTestTLS(t)
	var cap capture
	srv := newServer(t, m, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		cap.record(r, body)
		w.Header().Set("Content-Type", "application/json")
		op := strings.TrimPrefix(r.URL.Path, "/v1/state/")
		switch op {
		case "ListThreads":
			_, _ = w.Write([]byte(`{"threads":[{"id":"t1","context_id":"c","scope":{"tenant":"x","deployment":"d","subject":"s"},"epoch":1,"created_at":"2026-01-01T00:00:00Z"}],"next_cursor":"abc","has_more":false}`))
		case "GetPart":
			_, _ = w.Write([]byte(`{"data":"eHl6"}`))
		case "SearchKV":
			_, _ = w.Write([]byte(`{"items":[{"namespace":["ns"],"key":"k1","value":{"a":1},"version":2,"updated_at":"2026-01-01T00:00:00Z"}]}`))
		case "GetIntent":
			_, _ = w.Write([]byte(`{"node":"n1","call":"c1","kind":"tool","target":"t","status":"completed"}`))
		case "ReconcileIntent":
			_, _ = w.Write([]byte(`{"node":"n1","call":"c1","kind":"tool","target":"t","status":"reconciled"}`))
		case "AppendStream":
			_, _ = w.Write([]byte(`{"id":"42","key":"k","kind":"event","sequence":1}`))
		case "ReadStream":
			_, _ = w.Write([]byte(`{"events":[{"id":"1","key":"k","kind":"e","sequence":1}]}`))
		case "PutBatch":
			_, _ = w.Write([]byte(`{"ok":true}`))
		case "DeleteSubject":
			_, _ = w.Write([]byte(`{"deleted":42}`))
		case "SetLegalhold":
			_, _ = w.Write([]byte(`{"ok":true}`))
		case "RemoveLegalhold":
			_, _ = w.Write([]byte(`{"ok":true}`))
		case "ExportThread":
			_, _ = w.Write([]byte(`{"thread":{"id":"t1","context_id":"c","scope":{"tenant":"x","deployment":"d","subject":"s"},"epoch":1,"created_at":"2026-01-01T00:00:00Z"},"runs":[],"checkpoints":[],"exported_at":"2026-01-01T00:00:00Z"}`))
		case "ImportThread":
			_, _ = w.Write([]byte(`{"id":"t1","context_id":"c","scope":{"tenant":"x","deployment":"d","subject":"s"},"epoch":1,"created_at":"2026-01-01T00:00:00Z"}`))
		default:
			_, _ = w.Write([]byte(`{"ok":true}`))
		}
	})
	c := clientFor(t, m, srv.URL, staticToken("tok"))
	ctx := context.Background()
	fence := Fence{"run-1", "w-1", 3}

	t.Run("ListThreads", func(t *testing.T) {
		threads, next, hasMore, err := c.ListThreads(ctx, "cur", 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(threads) != 1 || next != "abc" || hasMore {
			t.Fatalf("unexpected: %d threads, next=%q, hasMore=%v", len(threads), next, hasMore)
		}
		assertPathBody(t, &cap, "ListThreads", "cur")
	})

	t.Run("GetPart", func(t *testing.T) {
		data, err := c.GetPart(ctx, "thr", "ns", "messages", "v3")
		if err != nil {
			t.Fatal(err)
		}
		if len(data) == 0 {
			t.Fatal("expected non-empty data")
		}
		assertPathBody(t, &cap, "GetPart", "messages")
	})

	t.Run("SearchKV", func(t *testing.T) {
		result, err := c.SearchKV(ctx, []string{"ns"}, "k", map[string]any{"q": "test"}, 5)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Items) != 1 || result.Items[0].Key != "k1" {
			t.Fatalf("unexpected: %+v", result)
		}
		assertPathBody(t, &cap, "SearchKV", "namespace_prefix")
	})

	t.Run("GetIntent", func(t *testing.T) {
		intent, err := c.GetIntent(ctx, "run-1", "n1", "c1")
		if err != nil {
			t.Fatal(err)
		}
		if intent.Status != "completed" {
			t.Fatalf("unexpected: %+v", intent)
		}
		assertPathBody(t, &cap, "GetIntent", "run-1")
	})

	t.Run("ReconcileIntent", func(t *testing.T) {
		intent, err := c.ReconcileIntent(ctx, fence, "n1", "c1", "done", "abc", nil)
		if err != nil {
			t.Fatal(err)
		}
		if intent.Status != "reconciled" {
			t.Fatalf("unexpected: %+v", intent)
		}
		assertPathBody(t, &cap, "ReconcileIntent", "fence")
	})

	t.Run("AppendStream", func(t *testing.T) {
		ev, err := c.AppendStream(ctx, fence, "k1", "event", nil)
		if err != nil {
			t.Fatal(err)
		}
		if ev.ID != "42" || ev.Key != "k" {
			t.Fatalf("unexpected: %+v", ev)
		}
		assertPathBody(t, &cap, "AppendStream", "fence")
	})

	t.Run("ReadStream", func(t *testing.T) {
		events, err := c.ReadStream(ctx, "thr", 0, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(events) != 1 || events[0].ID != "1" {
			t.Fatalf("unexpected: %+v", events)
		}
		assertPathBody(t, &cap, "ReadStream", "thr")
	})

	t.Run("PutBatch", func(t *testing.T) {
		batch := BatchRequest{Key: "k1"}
		if _, err := c.PutBatch(ctx, fence, batch); err != nil {
			t.Fatal(err)
		}
		assertPathBody(t, &cap, "PutBatch", "fence")
	})

	t.Run("DeleteSubject", func(t *testing.T) {
		deleted, err := c.DeleteSubject(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if deleted != 42 {
			t.Fatalf("expected deleted=42, got %d", deleted)
		}
		path, _, body := cap.last(t)
		if path != "/v1/state/DeleteSubject" {
			t.Errorf("path=%q", path)
		}
		if body != "{}" {
			t.Errorf("DeleteSubject body should be empty, got %s", body)
		}
	})

	t.Run("SetLegalhold", func(t *testing.T) {
		if err := c.SetLegalhold(ctx, "thr", "audit"); err != nil {
			t.Fatal(err)
		}
		assertPathBody(t, &cap, "SetLegalhold", "audit")
	})

	t.Run("RemoveLegalhold", func(t *testing.T) {
		if err := c.RemoveLegalhold(ctx, "thr"); err != nil {
			t.Fatal(err)
		}
		assertPathBody(t, &cap, "RemoveLegalhold", "thr")
	})

	t.Run("ExportThread", func(t *testing.T) {
		exp, err := c.ExportThread(ctx, "thr")
		if err != nil {
			t.Fatal(err)
		}
		if exp.Thread.ID != "t1" {
			t.Fatalf("unexpected thread: %+v", exp.Thread)
		}
		assertPathBody(t, &cap, "ExportThread", "thr")
	})

	t.Run("ImportThread", func(t *testing.T) {
		th, err := c.ImportThread(ctx, ControlExport{Thread: Thread{ID: "t1"}})
		if err != nil {
			t.Fatal(err)
		}
		if th.ID != "t1" {
			t.Fatalf("unexpected: %+v", th)
		}
		assertPathBody(t, &cap, "ImportThread", "export")
	})
}

func TestFenceNested(t *testing.T) {
	m := newTestTLS(t)
	var cap capture
	srv := newServer(t, m, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		cap.record(r, body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"checkpoint":{"id":"cp1"}}`))
	})
	c := clientFor(t, m, srv.URL, staticToken("tok"))
	ctx := context.Background()
	fence := Fence{"r1", "w1", 5}

	// Verify nested fence is sent (not flat)
	_, _ = c.PutCheckpoint(ctx, fence, Checkpoint{ID: "cp1"}, "key1")
	_, _, body := cap.last(t)
	if !strings.Contains(body, `"fence"`) {
		t.Errorf("expected nested fence, got: %s", body)
	}
	if strings.Contains(body, `"run_id":"r1"`) && !strings.Contains(body, `"fence"`) {
		t.Errorf("flat fence detected (should be nested): %s", body)
	}
}

func TestNewOperationsValidation(t *testing.T) {
	m := newTestTLS(t)
	srv := newServer(t, m, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	c := clientFor(t, m, srv.URL, staticToken("tok"))
	ctx := context.Background()

	if _, err := c.ReconcileIntent(ctx, Fence{}, "n", "c", "", "", nil); err == nil {
		t.Error("ReconcileIntent should reject invalid fence")
	}
	if _, err := c.PutBatch(ctx, Fence{}, BatchRequest{}); err == nil {
		t.Error("PutBatch should reject invalid fence")
	}
	if _, err := c.GetPart(ctx, "", "", "", ""); err == nil {
		t.Error("GetPart should reject empty channel")
	}
	if _, err := c.ReadStream(ctx, "", 0, 0); err == nil {
		t.Error("ReadStream should reject empty thread_id")
	}
}

func assertPathBody(t *testing.T, cap *capture, op, bodyFragment string) {
	t.Helper()
	path, _, body := cap.last(t)
	if want := "/v1/state/" + op; path != want {
		t.Errorf("path=%q want=%q", path, want)
	}
	if bodyFragment != "" && !strings.Contains(body, bodyFragment) {
		t.Errorf("body missing %q: %s", bodyFragment, body)
	}
}
