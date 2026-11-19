package model

import (
	"context"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Tree accounting. The ledger is a tree by construction (Allocate carves a
// child out of a parent), but until now the only way to see the tree was to
// read one node at a time -- which is not an accounting view. A snapshot is
// what the acceptance evidence ("budget before/after a real run") needs: one
// fail-closed read giving every node plus the tree-level totals.
//
// Semantics, fixed here because the numbers are only meaningful with them:
//   - node.spent is that node's accounting view: what was charged against it
//     plus the spent of every descendant that closed into it (Close
//     propagates); reading two nodes and adding them would double count --
//     the tree total therefore comes from the events, not from node sums;
//   - an open child keeps its whole carve-out held on the parent (parent
//     reserved >= child limit), so summing node.reserved across the tree
//     double counts nested holds. The tree totals therefore come from the
//     flat, non-nested facts: every node's spent, and every open
//     (unsettled) reservation amount.
//   - TreeSpent = sum of actual over SETTLED reservations: every real charge
//     is exactly one settle, so this is the one number that cannot
//     double-count propagation; TreeOpenHolds = sum of amount over unsettled
//     reservations; TreeCommitted = TreeSpent + TreeOpenHolds is the honest
//     "money in play" for the whole tree. Admission is bounded atomically;
//     actual provider overruns remain visible and block subsequent admission.

const (
	maxTreeDepth = 64
	maxTreeNodes = 10000
)

// TreeNode is one node of the tree with its depth (root = 0).
type TreeNode struct {
	Budget
	Depth int
}

// TreeTotals are the per-tree aggregates (see semantics above).
type TreeTotals struct {
	Nodes     int
	Spent     int64
	OpenHolds int64
	Committed int64
}

// TreeSnapshot is the full per-node plus per-tree view of one budget tree.
type TreeSnapshot struct {
	RootID string
	Nodes  []TreeNode
	Totals TreeTotals
}

// TreeSnapshot reads the whole tree rooted at rootID inside scope. The root
// must live in scope; descendants are followed within the root's tenant and
// subject (the only axes Allocate permits across). A cycle or a tree beyond
// the depth/node guards fails closed instead of hanging -- tree shape is
// server-set, and a snapshot must not be a way to DoS the ledger.
func (s *BudgetStore) TreeSnapshot(ctx context.Context, scope contracts.Scope, rootID string) (TreeSnapshot, error) {
	root, err := s.Get(ctx, scope, rootID)
	if err != nil {
		return TreeSnapshot{}, err
	}
	snap := TreeSnapshot{RootID: rootID}
	rows, err := s.pool.Query(ctx, `
WITH RECURSIVE tree(id, parent, task, tenant, subject, deployment, key, "limit",
                    reserved, spent, closed, digest, owner, depth, path) AS (
  SELECT b.id, b.parent, b.task, b.tenant, b.subject, b.deployment, b.key, b."limit",
         b.reserved, b.spent, b.closed, b.digest, b.owner, 0, ARRAY[b.id]
    FROM ahp_model.budgets b
   WHERE b.id = $1 AND b.tenant = $2 AND b.subject = $3
  UNION ALL
  SELECT b.id, b.parent, b.task, b.tenant, b.subject, b.deployment, b.key, b."limit",
         b.reserved, b.spent, b.closed, b.digest, b.owner, t.depth + 1, t.path || b.id
    FROM ahp_model.budgets b
    JOIN tree t ON b.parent = t.id
   WHERE b.tenant = $2 AND b.subject = $3
     AND t.depth < $4 AND NOT b.id = ANY(t.path)
)
SELECT t.id, t.parent, t.task, t.tenant, t.subject, t.deployment, t.key, t."limit",
       t.reserved, t.spent, t.closed, t.digest, t.owner, t.depth
  FROM tree t
 ORDER BY t.depth, t.id
 LIMIT $5`, rootID, root.Scope.Tenant, root.Scope.Subject, maxTreeDepth, maxTreeNodes+1)
	if err != nil {
		return TreeSnapshot{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var n TreeNode
		var parent *string
		if err := rows.Scan(&n.ID, &parent, &n.Task, &n.Scope.Tenant, &n.Scope.Subject,
			&n.Scope.Deployment, &n.Key, &n.Limit, &n.Reserved, &n.Spent, &n.Closed,
			&n.digest, &n.Owner, &n.Depth); err != nil {
			return TreeSnapshot{}, err
		}
		if parent != nil {
			n.Parent = *parent
		}
		snap.Nodes = append(snap.Nodes, n)
	}
	if err := rows.Err(); err != nil {
		return TreeSnapshot{}, err
	}
	if len(snap.Nodes) > maxTreeNodes {
		return TreeSnapshot{}, contracts.NewError(contracts.ErrConflict, "budget tree exceeds node guard")
	}
	// Depth guard: the recursive arm stops at maxTreeDepth; if a node sits at
	// the cap with descendants the tree is deeper than the ledger may audit.
	if len(snap.Nodes) > 0 && snap.Nodes[len(snap.Nodes)-1].Depth >= maxTreeDepth {
		var deeper int
		if err := s.pool.QueryRow(ctx,
			`SELECT count(*) FROM ahp_model.budgets WHERE parent = $1`, snap.Nodes[len(snap.Nodes)-1].ID).Scan(&deeper); err != nil {
			return TreeSnapshot{}, err
		}
		if deeper > 0 {
			return TreeSnapshot{}, contracts.NewError(contracts.ErrConflict, "budget tree exceeds depth guard")
		}
	}

	snap.Totals.Nodes = len(snap.Nodes)
	ids := make([]string, 0, len(snap.Nodes))
	for _, n := range snap.Nodes {
		ids = append(ids, n.ID)
	}
	if len(ids) > 0 {
		if err := s.pool.QueryRow(ctx,
			`SELECT COALESCE(sum(actual) FILTER (WHERE settled), 0),
			        COALESCE(sum(amount) FILTER (WHERE NOT settled), 0)
			 FROM ahp_model.budget_reservations WHERE budget_id = ANY($1)`, ids,
		).Scan(&snap.Totals.Spent, &snap.Totals.OpenHolds); err != nil {
			return TreeSnapshot{}, err
		}
	}
	snap.Totals.Committed = snap.Totals.Spent + snap.Totals.OpenHolds

	return snap, nil
}
