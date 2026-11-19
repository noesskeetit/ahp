package compute

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

type exportBindingCase struct {
	Scope              contracts.Scope
	First, Second, Bob string
}

func TestExportBindingColdProcessAndErasure(t *testing.T) {
	s, scope := tenantComputeFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	create := func(sc contracts.Scope, key string) Computer {
		t.Helper()
		c, e := s.CreateComputer(ctx, sc, NewComputerRequest{Class: ClassS, Profile: ProfileEphemeral}, key)
		if e != nil {
			t.Fatal(e)
		}
		return c
	}
	first, second := create(scope, "first"), create(scope, "second")
	bobScope := scope
	bobScope.Subject = "bob"
	bob := create(bobScope, "bob")
	if e := s.bindExportRequest(ctx, scope, "stable-key", first.ID, "task", "context", "out.txt"); e != nil {
		t.Fatal(e)
	}
	config := exportBindingCase{scope, first.ID, second.ID, bob.ID}
	raw, _ := json.Marshal(config)
	path := filepath.Join(t.TempDir(), "binding.json")
	if e := os.WriteFile(path, raw, 0600); e != nil {
		t.Fatal(e)
	}
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestExportBindingColdProcessHelper$", "-test.v")
	child.Env = append(os.Environ(), "AHP_EXPORT_BINDING_CASE="+path)
	if out, e := child.CombinedOutput(); e != nil {
		t.Fatalf("cold process binding: %v\n%s", e, out)
	}
	// Destroying a computer does not release the deployment-wide key.
	if _, e := s.SetComputerStatus(ctx, scope, first.ID, ComputerDestroyed); e != nil {
		t.Fatal(e)
	}
	if e := s.bindExportRequest(ctx, scope, "stable-key", second.ID, "task", "context", "out.txt"); contracts.Code(e) != contracts.ErrKeyReuse {
		t.Fatalf("destroy released key: %v", e)
	}
	if e := s.FenceSubject(ctx, scope.Tenant, scope.Subject); e != nil {
		t.Fatal(e)
	}
	if e := s.bindExportRequest(ctx, scope, "new", second.ID, "task", "context", "out.txt"); contracts.Code(e) != contracts.ErrForbidden {
		t.Fatalf("erased subject accepted binding: %v", e)
	}
	if e := s.DeleteSubject(ctx, scope.Tenant, scope.Subject); e != nil {
		t.Fatal(e)
	}
	var count int
	if e := s.pool.QueryRow(ctx, "SELECT count(*) FROM ahp_compute.export_request_bindings WHERE tenant=$1 AND subject=$2", scope.Tenant, scope.Subject).Scan(&count); e != nil || count != 0 {
		t.Fatalf("subject binding erasure: count=%d err=%v", count, e)
	}
	if e := s.bindExportRequest(ctx, bobScope, "bob-key", bob.ID, "bob-task", "bob-context", "out.txt"); e != nil {
		t.Fatal(e)
	}
	if e := s.FenceTenant(ctx, scope.Tenant); e != nil {
		t.Fatal(e)
	}
	if e := s.DeleteTenant(ctx, scope.Tenant); e != nil {
		t.Fatal(e)
	}
	if e := s.pool.QueryRow(ctx, "SELECT count(*) FROM ahp_compute.export_request_bindings WHERE tenant=$1", scope.Tenant).Scan(&count); e != nil || count != 0 {
		t.Fatalf("tenant binding erasure: count=%d err=%v", count, e)
	}
}
func TestExportBindingColdProcessHelper(t *testing.T) {
	path := os.Getenv("AHP_EXPORT_BINDING_CASE")
	if path == "" {
		return
	}
	raw, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	var c exportBindingCase
	if e = json.Unmarshal(raw, &c); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, e := pgxpool.New(ctx, os.Getenv("AHP_TEST_DATABASE_URL"))
	if e != nil {
		t.Fatal(e)
	}
	defer pool.Close()
	s := New(pool)
	if e = s.bindExportRequest(ctx, c.Scope, "stable-key", c.First, "task", "context", "out.txt"); e != nil {
		t.Fatal("same request cold replay", e)
	}
	if e = s.bindExportRequest(ctx, c.Scope, "stable-key", c.Second, "task", "context", "out.txt"); contracts.Code(e) != contracts.ErrKeyReuse {
		t.Fatal("different computer cold replay", e)
	}
	if e = s.bindExportRequest(ctx, c.Scope, "stable-key", c.First, "different-task", "context", "out.txt"); contracts.Code(e) != contracts.ErrKeyReuse {
		t.Fatal("different task cold replay", e)
	}
	bob := c.Scope
	bob.Subject = "bob"
	if e = s.bindExportRequest(ctx, bob, "stable-key", c.Bob, "task", "context", "out.txt"); contracts.Code(e) != contracts.ErrForbidden {
		t.Fatal("cross-subject cold replay", e)
	}
}
