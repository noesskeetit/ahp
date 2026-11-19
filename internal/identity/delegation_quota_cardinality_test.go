// Package identity: what the active-subtree quota is a counter OF.
//
// Requirement (ahp-agents-space-architecture (2) (1).pdf, rev 3.2, section 9.4
// "Делегирование и мультиагентность", extracted line 3118):
//
//	Агент порождает тысячи подзадач | Лимиты fan-out и активных задач на
//	атомарных счётчиках; сверх лимита — rejected | AHP
//
// The counted thing is ACTIVE TASKS. A task is a NODE of the delegation tree,
// so max_active / limits.active_tasks (cmd/ahp/runtime_authority.go maps the
// deploy manifest field ActiveTasks onto it) is a ceiling over distinct nodes.
// The number of walk-in paths that reach those nodes is an artifact of how the
// count is computed and must never be what a caller is charged for.
//
// D-ID-1 (found by Fermat's independent run,
// reports/mac-director/identity-r4-concurrency-fermat-20261110.md): the quota
// query anchored its recursive walk at "root = $1", which already yields every
// descendant, and then walked parent -> child on top of that, so a node at
// depth d was counted d times. A 2+4+8 tree - 14 live tasks - charged 34.
// Practical consequence: the quota is UNDER-issued, and legitimate delegation
// of a personal Dot is refused with "the root active-subtree limit is
// exhausted" while the tree is nowhere near its ceiling.
package identity

import (
	"fmt"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// seedRootGrantSQL writes one root grant with a generous ceiling. Its context
// journal row must exist first: contexts is referenced by a foreign key.
const seedRootGrantSQL = `
    INSERT INTO ahp_identity.grants_root
        (id, root, parent, tenant, deployment, subject, task, context_id,
         status, epoch, depth, actors, capabilities,
         max_depth, max_fanout, max_active, allow_self, body_digest, expires_at)
    VALUES ($1,$1,'',$2,$3,$4,$5,$6,'active',1,0,'{dep}','{read}',
            16,1024,$7,true,'sha256:seed', clock_timestamp() + interval '1 hour')`

const seedContextSQL = `
    INSERT INTO ahp_identity.contexts (id, tenant, deployment, subject)
    VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`

// seedGrantTree inserts a root plus a balanced tree below it and returns the
// root id and the ids of every level, levels[0] being the children of the root.
//
// The tree is written directly rather than delegated: the scenario is a tree
// that ALREADY exists and one final delegation that must be judged against it,
// and delegating the whole tree would let the defective quota itself decide
// how big the tree gets - the measurement would then depend on the defect.
// Rows satisfy every stored invariant (depth = parent.depth + 1, root is
// carried down, one parent per node), so what follows is a legitimate tree of
// the kind the product itself produces.
func seedGrantTree(t *testing.T, s *Store, scope contracts.Scope, prefix string, levels []int, maxActive int) (string, [][]string) {
	t.Helper()
	ctx := testContext(t)
	rootID := prefix + "-root-" + sanitizeID(contracts.NewID())
	rootCtx := prefix + "-ctx-" + sanitizeID(rootID)
	if _, err := s.pool.Exec(ctx, seedContextSQL,
		rootCtx, scope.Tenant, scope.Deployment, scope.Subject); err != nil {
		t.Fatalf("seed root context journal: %v", err)
	}
	if _, err := s.pool.Exec(ctx, seedRootGrantSQL,
		rootID, scope.Tenant, scope.Deployment, scope.Subject,
		prefix+"-root-task", rootCtx, maxActive); err != nil {
		t.Fatalf("seed root grant: %v", err)
	}

	var byLevel [][]string
	parents := []string{rootID}
	for depth, want := range levels {
		// Distribute the level evenly over the previous level's nodes, so a
		// 2+4+8 request really produces two children of the root, two of each
		// of those, and two of each of the four.
		if want%len(parents) != 0 {
			t.Fatalf("level %d wants %d nodes, not divisible by %d parents", depth+1, want, len(parents))
		}
		per := want / len(parents)
		var created []string
		for _, parent := range parents {
			for k := 0; k < per; k++ {
				id := fmt.Sprintf("%s-d%d-%s", prefix, depth+1, sanitizeID(contracts.NewID()))
				ctxID := prefix + "-ctx-" + sanitizeID(id)
				if _, err := s.pool.Exec(ctx, `
				    INSERT INTO ahp_identity.contexts (id, tenant, deployment, subject)
				    VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`,
					ctxID, scope.Tenant, scope.Deployment, scope.Subject); err != nil {
					t.Fatalf("seed context: %v", err)
				}
				if _, err := s.pool.Exec(ctx, `
				    INSERT INTO ahp_identity.grants_child
				        (id, root, parent, tenant, deployment, subject, task, context_id,
				         status, epoch, depth, actors, capabilities,
				         max_depth, max_fanout, max_active, allow_self, body_digest, expires_at)
				    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'active',1,$9,'{dep}','{read}',
				            16,1024,$10,true,'sha256:seed', clock_timestamp() + interval '1 hour')`,
					id, rootID, parent, scope.Tenant, scope.Deployment, scope.Subject,
					"task-"+sanitizeID(id), ctxID, depth+1, maxActive); err != nil {
					t.Fatalf("seed child grant: %v", err)
				}
				created = append(created, id)
			}
		}
		byLevel = append(byLevel, created)
		parents = created
	}
	return rootID, byLevel
}

// countQuotaBothWays reports what the two candidate semantics count for one
// root: distinct live nodes, and walk-in paths to them (the defective shape,
// quoted verbatim from the query the quota used).
func countQuotaBothWays(t *testing.T, s *Store, rootID string) (nodes, paths int) {
	t.Helper()
	ctx := testContext(t)
	err := s.pool.QueryRow(ctx, `
	    WITH RECURSIVE nodes(id, live) AS (
	        SELECT c.id, (c.status = 'active' AND clock_timestamp() < c.expires_at)
	          FROM ahp_identity.grants_child c WHERE c.root = $1
	        UNION ALL
	        SELECT c.id, (c.status = 'active' AND clock_timestamp() < c.expires_at)
	          FROM ahp_identity.grants_child c JOIN nodes h ON c.parent = h.id
	    ),
	    paths(id, live) AS (
	        SELECT c.id, (c.status = 'active' AND clock_timestamp() < c.expires_at)
	          FROM ahp_identity.grants_child c WHERE c.root = $1
	        UNION ALL
	        SELECT c.id, (c.status = 'active' AND clock_timestamp() < c.expires_at)
	          FROM ahp_identity.grants_child c JOIN paths h ON c.parent = h.id
	    )
	    SELECT
	        (SELECT count(DISTINCT id) FROM nodes WHERE live),
	        (SELECT count(*) FROM paths WHERE live)`, rootID).Scan(&nodes, &paths)
	if err != nil {
		t.Fatalf("count subtree both ways: %v", err)
	}
	return nodes, paths
}

// setGrantCeilings narrows the ceilings stored on one grant, standing in for a
// deploy manifest that carries a tighter limits.active_tasks.
func setGrantCeilings(t *testing.T, s *Store, table, id string, maxActive, maxFanout, maxDepth int) {
	t.Helper()
	ctx := testContext(t)
	if _, err := s.pool.Exec(ctx, `UPDATE ahp_identity.`+table+`
	                      SET max_active = $2, max_fanout = $3, max_depth = $4
	                      WHERE id = $1`, id, maxActive, maxFanout, maxDepth); err != nil {
		t.Fatalf("set ceilings on %s %s: %v", table, id, err)
	}
}

// TestDelegationQuotaCountsActiveNodesNotPaths is the D-ID-1 RED leg.
//
// A legitimate tree of 14 live tasks (2 + 4 + 8) hangs below one root. With
// limits.active_tasks = 14 the ceiling is exactly reached, so one more child
// must be refused; with 15 it is not, so the child must be created. The
// defective path-counting refuses both, because it charges 34.
func TestDelegationQuotaCountsActiveNodesNotPaths(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)

	for _, tc := range []struct {
		name      string
		maxActive int
		wantOK    bool
	}{
		// The exact-count pins: 14 live nodes means the 15th child is one over
		// a ceiling of 14 and inside a ceiling of 15.
		{name: "ceiling exactly reached refuses", maxActive: 14, wantOK: false},
		{name: "ceiling with room admits", maxActive: 15, wantOK: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scope := uniqueScope(t)
			rootID, byLevel := seedGrantTree(t, s, scope, "q1", []int{2, 4, 8}, 1000)
			nodes, paths := countQuotaBothWays(t, s, rootID)
			leaf := byLevel[len(byLevel)-1][0]
			t.Logf("D-ID-1 %s: tree 2+4+8 -> live NODES=%d live PATHS=%d; delegating under max_active=%d from leaf %s",
				tc.name, nodes, paths, tc.maxActive, leaf)
			if nodes != 14 {
				t.Fatalf("substrate wrong: want 14 live nodes, got %d", nodes)
			}
			// Only the delegating parent carries the tested ceiling; every
			// ancestor keeps a generous one so the ancestor rule cannot be
			// what answers here.
			setGrantCeilings(t, s, "grants_child", leaf, tc.maxActive, 8, 4)

			_, err := s.Delegate(ctx, scope, "task-"+sanitizeID(leaf), DelegateRequest{
				Task:         "extra-" + sanitizeID(contracts.NewID()),
				Context:      "ctx-" + sanitizeID(contracts.NewID()),
				Deployment:   scope.Deployment,
				Capabilities: []string{"read"},
				TTL:          time.Hour,
				AllowSelf:    true,
			})
			switch {
			case tc.wantOK && err != nil:
				t.Errorf("D-ID-1: quota refused a legitimate delegation over a tree of %d live tasks with max_active=%d: %v"+
					" (the walk charges %d paths, not %d tasks)", nodes, tc.maxActive, err, paths, nodes)
			case !tc.wantOK && err == nil:
				t.Errorf("D-ID-1: quota admitted an over-ceiling child: %d live tasks + 1 exceeds max_active=%d and was accepted", nodes, tc.maxActive)
			}
			if !tc.wantOK && err != nil {
				assertCode(t, err, contracts.ErrForbidden)
			}
		})
	}
}

// TestLegitDelegationToDepthThreeIsNotRefusedByQuota is the D-ID-1 end-to-end
// leg: a real personal-Dot shaped tree built only through the product API must
// be admitted up to its declared ceiling.
//
// Root: limits active_tasks=16, fan-out 2, depth 4. Breadth-first, two children
// per parent, 14 delegations total. Every one of them is inside every stored
// ceiling, so every one must succeed.
func TestLegitDelegationToDepthThreeIsNotRefusedByQuota(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	scope := uniqueScope(t)

	root, err := s.CreateRoot(ctx, scope, RootRequest{
		Task:         "dot-run-" + sanitizeID(contracts.NewID()),
		Context:      "ctx-dot-" + sanitizeID(contracts.NewID()),
		Deployment:   scope.Deployment,
		Capabilities: []string{"read"},
		TTL:          time.Hour,
		MaxDepth:     4,
		MaxFanout:    2,
		MaxActive:    16,
		AllowSelf:    true,
	})
	if err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}

	live := []Grant{root}
	var refused []string
	for level := 1; level <= 3; level++ {
		var next []Grant
		for _, parent := range live {
			if parent.Depth != level-1 {
				continue
			}
			for k := 0; k < 2; k++ {
				child, err := s.Delegate(ctx, scope, parent.Task, DelegateRequest{
					Task:         fmt.Sprintf("d%d-%s", level, sanitizeID(contracts.NewID())),
					Context:      "ctx-" + sanitizeID(contracts.NewID()),
					Deployment:   scope.Deployment,
					Capabilities: []string{"read"},
					TTL:          time.Hour,
					AllowSelf:    true,
				})
				if err != nil {
					refused = append(refused, fmt.Sprintf("depth %d under %s: %v", level, parent.Task, err))
					continue
				}
				next = append(next, child)
			}
		}
		if len(next) == 0 {
			break
		}
		live = append(live, next...)
	}

	nodes, paths := countQuotaBothWays(t, s, root.ID)
	t.Logf("D-ID-1 e2e: ceiling active_tasks=16 fan-out=2 depth=4 -> admitted %d, live nodes=%d (paths charged by the old walk=%d), refused=%d",
		len(live)-1, nodes, paths, len(refused))
	if len(refused) > 0 {
		t.Errorf("D-ID-1: %d legitimate delegations were refused although the tree holds %d live tasks under a ceiling of 16; charged count was %d paths. First refusal: %s",
			len(refused), nodes, paths, refused[0])
	}
	if nodes != 14 {
		t.Errorf("D-ID-1: expected the tree to reach 14 live tasks inside the ceiling, got %d", nodes)
	}
}
