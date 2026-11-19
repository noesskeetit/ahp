package integration_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// B133 §9.1 Entry and idempotency: 6 situations at the compute dispatch level.

// Situation 1: Duplicate dispatch — same task_id (key) dispatched twice.
// The second call is idempotent: returns the existing record, no double-execution.
func TestB133Sit1_DuplicateDispatch(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b133-s1-"+contracts.NewID())

	key := "b133-s1-dup"
	cmd := "echo first-run"

	// First dispatch executes normally.
	rec1, err := e.runner.RunCommand(ctx(t), sc, comp.ID, key, cmd, "")
	if err != nil {
		t.Fatalf("first dispatch: %v", err)
	}
	if rec1.Status != compute.CommandSucceeded {
		t.Fatalf("first dispatch: status=%q", rec1.Status)
	}

	// Second dispatch with same key and same body: idempotent replay.
	rec2, err := e.runner.RunCommand(ctx(t), sc, comp.ID, key, cmd, "")
	if err != nil {
		t.Fatalf("duplicate dispatch: %v", err)
	}
	if rec2.Status != compute.CommandSucceeded {
		t.Fatalf("duplicate dispatch: status=%q want succeeded (replayed)", rec2.Status)
	}
	// Same record returned (idempotent).
	if rec2.ID != rec1.ID {
		t.Fatalf("duplicate dispatch: different record id %q vs %q", rec2.ID, rec1.ID)
	}
	t.Log("Sit1: duplicate dispatch — idempotent, same record returned, no re-execution")
}

// Situation 2: Retry after lost ack — client doesn't receive response, retries
// with the same task and body. The second call is idempotent.
func TestB133Sit2_RetryLostAck(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b133-s2-"+contracts.NewID())

	key := "b133-s2-retry"
	cmd := "printf 'ack-lost'"

	// First dispatch: succeeds but client "lost" the response.
	rec1, err := e.runner.RunCommand(ctx(t), sc, comp.ID, key, cmd, "")
	if err != nil {
		t.Fatalf("initial dispatch: %v", err)
	}
	if rec1.Stdout != "ack-lost" {
		t.Fatalf("initial: stdout=%q want 'ack-lost'", rec1.Stdout)
	}

	// Client retries (same key, same body).
	rec2, err := e.runner.RunCommand(ctx(t), sc, comp.ID, key, cmd, "")
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	// Idempotent: same record, same result.
	if rec2.ID != rec1.ID {
		t.Fatalf("retry: different record (%q vs %q)", rec2.ID, rec1.ID)
	}
	if rec2.Stdout != "ack-lost" {
		t.Fatalf("retry: stdout=%q want 'ack-lost'", rec2.Stdout)
	}
	t.Log("Sit2: retry after lost ack — idempotent, same result returned")
}

// Situation 3: Different task, same parameters — two different keys with
// identical command body both execute independently.
func TestB133Sit3_DiffTaskSameParams(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b133-s3-"+contracts.NewID())

	// Two different keys, identical command body.
	rec1, err := e.runner.RunCommand(ctx(t), sc, comp.ID, "b133-s3-key1", "echo same-params", "")
	if err != nil {
		t.Fatalf("task 1: %v", err)
	}
	rec2, err := e.runner.RunCommand(ctx(t), sc, comp.ID, "b133-s3-key2", "echo same-params", "")
	if err != nil {
		t.Fatalf("task 2: %v", err)
	}
	if rec1.Status != compute.CommandSucceeded || rec2.Status != compute.CommandSucceeded {
		t.Fatalf("status: %q / %q", rec1.Status, rec2.Status)
	}
	// Both executed independently (different records).
	if rec1.ID == rec2.ID {
		t.Fatal("different keys produced same record (should be independent)")
	}
	t.Log("Sit3: different task, same params — both execute independently")
}

// Situation 4: Concurrent dispatch to same computer — two tasks with different
// keys dispatch concurrently. Both succeed (serialized by incarnation).
func TestB133Sit4_ConcurrentDispatch(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b133-s4-"+contracts.NewID())

	var wg sync.WaitGroup
	var mu sync.Mutex
	var results []compute.CommandRecord
	var errs []error

	keys := []string{"b133-s4-concurrent-a", "b133-s4-concurrent-b"}
	cmds := []string{"echo concurrent-a", "echo concurrent-b"}

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			rec, err := e.runner.RunCommand(ctx(t), sc, comp.ID, keys[idx], cmds[idx], "")
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, fmt.Errorf("dispatch[%d]: %w", idx, err))
				return
			}
			results = append(results, rec)
		}(i)
	}
	wg.Wait()

	for _, err := range errs {
		t.Fatalf("concurrent dispatch: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("concurrent dispatch: got %d results, want 2", len(results))
	}
	for i, rec := range results {
		if rec.Status != compute.CommandSucceeded {
			t.Fatalf("concurrent dispatch[%d]: status=%q", i, rec.Status)
		}
	}
	t.Log("Sit4: concurrent dispatch to same computer — both succeed, serialized by incarnation")
}

// Situation 5: Dispatch to non-existent computer → NOT_FOUND (404).
func TestB133Sit5_NonExistentComputer(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	_, err := e.runner.RunCommand(ctx(t), sc, "non-existent-computer",
		"b133-s5-nx", "echo test", "")
	if err == nil {
		t.Fatal("dispatch to non-existent computer: expected error, got nil")
	}
	if contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("non-existent computer: code=%s want NOT_FOUND", contracts.Code(err))
	}
	t.Log("Sit5: dispatch to non-existent computer — NOT_FOUND (404)")
}

// Situation 6: Key reuse with different body → KEY_REUSE error (409 equivalent).
// Same idempotency key but different command body is rejected.
func TestB133Sit6_KeyReuseMismatch(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
	}, "b133-s6-"+contracts.NewID())

	key := "b133-s6-reuse"

	// First: execute with body A.
	rec1, err := e.runner.RunCommand(ctx(t), sc, comp.ID, key, "echo body-a", "")
	if err != nil {
		t.Fatalf("first dispatch: %v", err)
	}
	if rec1.Status != compute.CommandSucceeded {
		t.Fatalf("first dispatch: status=%q", rec1.Status)
	}

	// Second: same key, different body → rejected.
	_, err = e.runner.RunCommand(ctx(t), sc, comp.ID, key, "echo body-B", "")
	if err == nil {
		t.Fatal("key reuse with different body: expected error, got nil")
	}
	if contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("key reuse: code=%s want KEY_REUSE", contracts.Code(err))
	}
	t.Log("Sit6: key reuse with different body — rejected (KEY_REUSE / 409)")
}
