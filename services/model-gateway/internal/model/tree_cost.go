package model

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Whole-tree cost. This is the read model the acceptance and the UI need and did
// not have: one question, asked with the task id they already hold, answered with
// what the whole task tree costs -- parent and every delegated child -- plus the
// observation of which model calls produced that charge.
//
// BudgetStore.TreeSnapshot already reads the ledger tree correctly (its totals
// come from events, because node sums double-count both Close propagation and
// nested holds). What was missing here is the three things a caller can actually
// use: resolution by TASK rather than by root budget id, the join with the
// generation ledger (steps and input/output tokens), and a statement of whether
// the charge and the provider's reported usage agree.
//
// Two denominations exist in this ledger and they are never added together:
// a task root is accounted in tokens, a delegated run in model steps. A tree
// therefore reports its unit, and a caller holding one task can get more than one
// tree back.

// Cost units. `unknown` is reported rather than guessed: a budget key this plane
// does not recognise means the accounting view is not understood, and an
// understood-by-assumption number is how a wrong total reaches a report.
const (
	CostUnitTokens  = "tokens"
	CostUnitSteps   = "steps"
	CostUnitUnknown = "unknown"
)

// Budget keys whose denomination is fixed by the writers of this ledger.
const (
	budgetKeyTaskTokenRoot = "execbridge-task-root"
	budgetKeyRunStepRoot   = "treedelegate:run-steps"
)

func unitForBudgetKey(key string) string {
	switch key {
	case budgetKeyTaskTokenRoot:
		return CostUnitTokens
	case budgetKeyRunStepRoot:
		return CostUnitSteps
	default:
		return CostUnitUnknown
	}
}

// CostNode is one node of the tree: its ledger numbers plus what produced them.
type CostNode struct {
	BudgetID string          `json:"budget_id"`
	ParentID string          `json:"parent_budget_id,omitempty"`
	Task     string          `json:"task"`
	Key      string          `json:"budget_key"`
	Unit     string          `json:"unit"`
	Scope    contracts.Scope `json:"scope"`
	Depth    int             `json:"depth"`

	Limit    int64 `json:"limit"`
	Reserved int64 `json:"reserved"`
	Spent    int64 `json:"spent"`
	Closed   bool  `json:"closed"`

	// Generations, TokensIn/Out/Total are observations from ahp_model.generations
	// charged to this node. They are kept beside the ledger numbers, never
	// substituted for them: the ledger is what a limit is enforced against, the
	// generation rows are what the provider said it used.
	Generations   int64 `json:"generations"`
	TokensIn      int64 `json:"tokens_in"`
	TokensOut     int64 `json:"tokens_out"`
	TokensTotal   int64 `json:"tokens_total"`
	SettledActual int64 `json:"settled_actual"`
}

// CostTree is one budget tree with its totals.
type CostTree struct {
	RootID string     `json:"tree_root"`
	Unit   string     `json:"unit"`
	Root   CostNode   `json:"root"`
	Nodes  []CostNode `json:"nodes"`

	Limit int64 `json:"limit"`
	// HeldForChildren is what the root has already carved out for its children and
	// still holds. It belongs in the answer because PDF :1789 makes the root's
	// capacity the thing that runs out: a parent whose remainder is committed to a
	// child is exhausted even though it has spent nothing itself.
	HeldForChildren int64 `json:"held_for_children"`
	// Spent is tree spend from settled reservations (see TreeSnapshot semantics):
	// every real charge is exactly one settle, so this cannot double-count.
	Spent     int64 `json:"spent"`
	OpenHolds int64 `json:"open_holds"`
	Committed int64 `json:"committed"`
	Remaining int64 `json:"remaining"`
	// AvailableToRoot is the remainder a further charge would be admitted
	// against: limit - root.spent - root.held_for_children.
	AvailableToRoot int64 `json:"available_to_root"`
	// Exhausted is the state PDF :1789 makes meaningful: with nothing left, the
	// next charge is refused rather than the limit exceeded.
	Exhausted bool `json:"exhausted"`

	Generations int64 `json:"generations"`
	TokensIn    int64 `json:"tokens_in"`
	TokensOut   int64 `json:"tokens_out"`
	TokensTotal int64 `json:"tokens_total"`

	// Reconciles states whether what the ledger charged agrees with what the
	// provider reported for the same tree. It is the check that turns "a number"
	// into "the same quantity measured at both ends", which is exactly what a
	// verifier comparing A and B needs.
	Reconciles bool     `json:"reconciles"`
	Mismatches []string `json:"mismatches,omitempty"`
}

// TreeCostByTask returns every budget tree this task takes part in.
//
// The task is addressed by the names the writers use: a2a-task:<id> for a token
// root, treedelegate-run:<id> for a step root, and treedelegate-child:<parent>:<id>
// for a delegated child -- a child answers with its whole tree, because a partial
// cost of one node is not what "what did this cost" means.
//
// Scope is the caller's, and it is enforced rather than decorative: the root must
// live in the caller's tenant, and its subject must match when the caller stated
// one. A task belonging to another tenant is reported as absent, not as
// forbidden, so this read cannot be used to probe for a task id.
func (s *BudgetStore) TreeCostByTask(ctx context.Context, scope contracts.Scope, taskID string) ([]CostTree, error) {
	if scope.Tenant == "" {
		return nil, contracts.NewError(contracts.ErrInvalid, "model: tree cost needs a tenant")
	}
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return nil, contracts.NewError(contracts.ErrInvalid, "model: tree cost needs a task id")
	}

	rows, err := s.pool.Query(ctx, `
SELECT id FROM ahp_model.budgets
 WHERE tenant = $1 AND ($2 = '' OR subject = $2)
   AND (task = 'a2a-task:' || $3
     OR task = 'treedelegate-run:' || $3
     OR task = 'treedelegate-child:' || $3
     OR task LIKE 'treedelegate-child:%:' || $3
     OR task = $3)
 UNION SELECT budget_id FROM ahp_model.budget_task_aliases WHERE tenant=$1 AND ($2='' OR subject=$2) AND task='a2a-task:'||$3`, scope.Tenant, scope.Subject, taskID)
	if err != nil {
		return nil, fmt.Errorf("model: tree cost lookup: %w", err)
	}
	var matched []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, fmt.Errorf("model: tree cost lookup scan: %w", err)
		}
		matched = append(matched, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("model: tree cost lookup: %w", err)
	}
	if len(matched) == 0 {
		// Same answer as "does not exist": the caller learns nothing about
		// whether the task exists somewhere else.
		return nil, contracts.NewError(contracts.ErrNotFound, "model: no budget tree for this task in scope")
	}

	roots, err := s.rootsOf(ctx, matched)
	if err != nil {
		return nil, err
	}

	out := make([]CostTree, 0, len(roots))
	for rootID := range roots {
		tree, terr := s.costTree(ctx, scope, rootID)
		if terr != nil {
			return nil, terr
		}
		out = append(out, tree)
	}
	return out, nil
}

// rootsOf walks each matched node up to its root, deduplicating. The walk is
// bounded: tree shape is server-set, and a read model must not be a way to spin.
func (s *BudgetStore) rootsOf(ctx context.Context, ids []string) (map[string]bool, error) {
	roots := make(map[string]bool, len(ids))
	for _, id := range ids {
		cur := id
		for depth := 0; depth <= maxTreeDepth; depth++ {
			var parent string
			err := s.pool.QueryRow(ctx, `SELECT coalesce(parent,'') FROM ahp_model.budgets WHERE id=$1`, cur).Scan(&parent)
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, contracts.NewError(contracts.ErrNotFound, "model: budget node vanished while reading its tree")
			}
			if err != nil {
				return nil, fmt.Errorf("model: tree cost parent walk: %w", err)
			}
			if parent == "" {
				roots[cur] = true
				break
			}
			cur = parent
			if depth == maxTreeDepth {
				return nil, contracts.NewError(contracts.ErrInvalid, "model: budget tree exceeds depth guard")
			}
		}
	}
	return roots, nil
}

// costTree reads one tree's ledger snapshot and joins it with the generations it
// charged.
func (s *BudgetStore) costTree(ctx context.Context, scope contracts.Scope, rootID string) (CostTree, error) {
	snap, err := s.TreeSnapshot(ctx, scope, rootID)
	if err != nil {
		return CostTree{}, err
	}
	ids := make([]string, 0, len(snap.Nodes))
	for _, n := range snap.Nodes {
		ids = append(ids, n.ID)
	}

	genByNode, err := generationTotalsByBudget(ctx, s.pool, ids)
	if err != nil {
		return CostTree{}, err
	}
	settledByNode, err := settledActualByBudget(ctx, s.pool, ids)
	if err != nil {
		return CostTree{}, err
	}

	tree := CostTree{RootID: snap.RootID, Nodes: make([]CostNode, 0, len(snap.Nodes))}
	for _, n := range snap.Nodes {
		cn := CostNode{
			BudgetID: n.ID, ParentID: n.Parent, Task: n.Task, Key: n.Key,
			Unit: unitForBudgetKey(n.Key), Scope: n.Scope, Depth: n.Depth,
			Limit: n.Limit, Reserved: n.Reserved, Spent: n.Spent, Closed: n.Closed,
		}
		if g, ok := genByNode[n.ID]; ok {
			cn.Generations, cn.TokensIn, cn.TokensOut, cn.TokensTotal = g.generations, g.prompt, g.completion, g.total
		}
		cn.SettledActual = settledByNode[n.ID]
		if n.Depth == 0 {
			tree.Root = cn
			tree.Unit = cn.Unit
			tree.Limit = n.Limit
		}
		tree.Generations += cn.Generations
		tree.TokensIn += cn.TokensIn
		tree.TokensOut += cn.TokensOut
		tree.TokensTotal += cn.TokensTotal
		tree.Nodes = append(tree.Nodes, cn)
	}

	tree.Spent = snap.Totals.Spent
	tree.OpenHolds = snap.Totals.OpenHolds
	tree.Committed = snap.Totals.Committed
	if tree.Limit >= tree.Committed {
		tree.Remaining = tree.Limit - tree.Committed
	} else {
		// A provider exceeded its admitted estimate. Keep the real bill visible
		// and report the exhausted ceiling separately from usage.
		tree.Remaining = 0
		tree.Mismatches = append(tree.Mismatches, fmt.Sprintf(
			"committed %d exceeds root limit %d", tree.Committed, tree.Limit))
	}
	tree.HeldForChildren = tree.Root.Reserved
	tree.AvailableToRoot = tree.Limit - tree.Root.Spent - tree.Root.Reserved
	if tree.AvailableToRoot < 0 {
		tree.AvailableToRoot = 0
	}
	// Exhaustion is the root's admission state, not an average: this is the number
	// a further Reserve is checked against, so "exhausted" must mean "the next
	// charge is refused", never "the sum of spend looks large".
	tree.Exhausted = tree.AvailableToRoot == 0
	for _, n := range snap.Nodes {
		if !n.Closed && n.Spent+n.Reserved > n.Limit {
			tree.Exhausted = true
			tree.AvailableToRoot = 0
		}
	}
	tree.Reconciles = tree.checkReconciliation()
	return tree, nil
}

// checkReconciliation compares the charge with the provider's own report.
//
// tokens: the tree's settled charges must equal the sum of the usage the provider
// reported for the generations charged to this tree -- the identity that made the
// first live measurement trustworthy (1427 = 661 + 766).
// steps: every step is one settled reservation, so the number of settled charges
// is the number of steps the tree consumed.
func (t *CostTree) checkReconciliation() bool {
	switch t.Unit {
	case CostUnitTokens:
		if t.Spent != t.TokensTotal {
			t.Mismatches = append(t.Mismatches, fmt.Sprintf(
				"charged %d tokens but generations reported %d", t.Spent, t.TokensTotal))
			return false
		}
		return true
	case CostUnitSteps:
		if t.Spent != t.Generations && t.Generations > 0 {
			// Generations are only an observation here: a step can be charged by a
			// path that does not write a generation row in this plane (a delegated
			// child accounted remotely), so a mismatch is reported, not fatalised.
			t.Mismatches = append(t.Mismatches, fmt.Sprintf(
				"charged %d steps but this plane holds %d generation rows", t.Spent, t.Generations))
		}
		return true
	default:
		t.Mismatches = append(t.Mismatches, "budget key denomination is unknown to this plane, so the charge cannot be reconciled")
		return false
	}
}

type genTotals struct {
	generations, prompt, completion, total int64
}

func generationTotalsByBudget(ctx context.Context, pool *pgxpool.Pool, ids []string) (map[string]genTotals, error) {
	out := map[string]genTotals{}
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := pool.Query(ctx, `
SELECT coalesce(budget_id,''), count(*), coalesce(sum(usage_prompt),0),
       coalesce(sum(usage_completion),0), coalesce(sum(usage_total),0)
  FROM ahp_model.generations
 WHERE budget_id = ANY($1)
 GROUP BY 1`, ids)
	if err != nil {
		return nil, fmt.Errorf("model: tree cost generations: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var g genTotals
		if err := rows.Scan(&id, &g.generations, &g.prompt, &g.completion, &g.total); err != nil {
			return nil, fmt.Errorf("model: tree cost generations scan: %w", err)
		}
		out[id] = g
	}
	return out, rows.Err()
}

func settledActualByBudget(ctx context.Context, pool *pgxpool.Pool, ids []string) (map[string]int64, error) {
	out := map[string]int64{}
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := pool.Query(ctx, `
SELECT budget_id, coalesce(sum(actual),0)
  FROM ahp_model.budget_reservations
 WHERE budget_id = ANY($1) AND settled
 GROUP BY budget_id`, ids)
	if err != nil {
		return nil, fmt.Errorf("model: tree cost settlements: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var sum int64
		if err := rows.Scan(&id, &sum); err != nil {
			return nil, fmt.Errorf("model: tree cost settlements scan: %w", err)
		}
		out[id] = sum
	}
	return out, rows.Err()
}
