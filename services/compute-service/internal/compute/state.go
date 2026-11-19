package compute

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// ErrStaleHash is the typed-patch CAS failure: the file's live hash no longer
// matches the hash the patch was computed against.
const ErrStaleHash = "STALE_HASH"

// StateFile is one file entry in a Computer state revision tree. Hash is the
// lowercase hex SHA-256 of the file content; it is the value a typed patch
// must verify before the revision is appended.
type StateFile struct {
	Hash string `json:"hash"`
	Size int64  `json:"size"`
	Mode int    `json:"mode"`
}

// StateTree maps a workspace-relative path to its file entry.
type StateTree map[string]StateFile

// StateRevision is one immutable revision of a Computer's state. Branch
// "main" is the Computer's live tree; derivative branches fork from main so
// several chats can work in parallel and merge back.
type StateRevision struct {
	ID         string    `json:"id"`
	ComputerID string    `json:"computer_id"`
	Seq        int64     `json:"seq"`
	Branch     string    `json:"branch"`
	ParentID   string    `json:"parent_id,omitempty"`
	BaseSeq    int64     `json:"base_seq"`
	Tree       StateTree `json:"tree"`
	Digest     string    `json:"digest"`
	CreatedAt  time.Time `json:"created_at"`
}

const stateMainBranch = "main"

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func stateTreeDigest(tree StateTree) (string, error) {
	return contracts.Digest(map[string]StateFile(tree))
}

// stateTreeFromJSON decodes the durable jsonb tree column.
func stateTreeFromJSON(raw []byte) (StateTree, error) {
	tree := StateTree{}
	if len(raw) == 0 {
		return tree, nil
	}
	if err := json.Unmarshal(raw, &tree); err != nil {
		return nil, fmt.Errorf("compute: decode state tree: %w", err)
	}
	return tree, nil
}

func scanStateRevision(row scanner) (StateRevision, error) {
	var r StateRevision
	var raw []byte
	err := row.Scan(&r.ID, &r.ComputerID, &r.Seq, &r.Branch, &r.ParentID, &r.BaseSeq, &raw, &r.Digest, &r.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return StateRevision{}, notFound("state revision")
		}
		return StateRevision{}, fmt.Errorf("compute: scan state revision: %w", err)
	}
	r.Tree, err = stateTreeFromJSON(raw)
	if err != nil {
		return StateRevision{}, err
	}
	r.CreatedAt = r.CreatedAt.UTC()
	return r, nil
}

const stateRevisionSelect = `id, computer_id, seq, branch, parent_id, base_seq, tree, digest, created_at`

// EnsureStateMain guarantees the personal Computer has a main state revision
// (seq 0, empty tree) and returns the current main head. Replaying is
// idempotent.
func (s *Store) EnsureStateMain(ctx context.Context, scope contracts.Scope, computerID string) (StateRevision, error) {
	var out StateRevision
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		c, err := s.getComputer(ctx, tx, scope, computerID)
		if err != nil {
			return err
		}
		if c.Profile != ProfilePersonal {
			return contracts.NewError(contracts.ErrForbidden,
				"state revisions apply only to personal computers")
		}
		head, err := scanStateRevision(tx.QueryRow(ctx, `
			SELECT `+stateRevisionSelect+` FROM ahp_compute.state_revisions
			WHERE computer_id = $1 AND branch = $2 ORDER BY seq DESC LIMIT 1`,
			computerID, stateMainBranch))
		if err == nil {
			out = head
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		id := contracts.NewID()
		digest, err := stateTreeDigest(StateTree{})
		if err != nil {
			return err
		}
		out, err = scanStateRevision(tx.QueryRow(ctx, `
			INSERT INTO ahp_compute.state_revisions
				(id, computer_id, seq, branch, parent_id, base_seq, tree, digest)
			VALUES ($1,$2,0,$3,'',0,'{}'::jsonb,$4)
			RETURNING `+stateRevisionSelect, id, computerID, stateMainBranch, digest))
		return err
	})
	return out, err
}

// StateHead returns the head revision of a branch, or NOT_FOUND when the
// branch does not exist.
func (s *Store) StateHead(ctx context.Context, scope contracts.Scope, computerID, branch string) (StateRevision, error) {
	if branch == "" {
		branch = stateMainBranch
	}
	if err := validIdentifier(branch, "branch"); err != nil {
		return StateRevision{}, err
	}
	var out StateRevision
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := s.getComputer(ctx, tx, scope, computerID); err != nil {
			return err
		}
		return scanStateRevisionInto(&out, tx.QueryRow(ctx, `
			SELECT `+stateRevisionSelect+` FROM ahp_compute.state_revisions
			WHERE computer_id = $1 AND branch = $2 ORDER BY seq DESC LIMIT 1`,
			computerID, branch))
	})
	return out, err
}

// ListStateRevisions returns a branch's revisions in ascending seq order.
func (s *Store) ListStateRevisions(ctx context.Context, scope contracts.Scope, computerID, branch string) ([]StateRevision, error) {
	if branch == "" {
		branch = stateMainBranch
	}
	var out []StateRevision
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := s.getComputer(ctx, tx, scope, computerID); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
			SELECT `+stateRevisionSelect+` FROM ahp_compute.state_revisions
			WHERE computer_id = $1 AND branch = $2 ORDER BY seq ASC`, computerID, branch)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			r, err := scanStateRevision(rows)
			if err != nil {
				return err
			}
			out = append(out, r)
		}
		return rows.Err()
	})
	return out, err
}

// CommitStatePatch appends a typed-patch revision to branch: the head must
// still be expectedHeadID (compare-and-swap), the file hash was already
// verified by the caller against both the tree and the live workspace. The
// new revision is the derivative of expectedHeadID.
func (s *Store) CommitStatePatch(ctx context.Context, scope contracts.Scope, computerID, branch, expectedHeadID, path string, f StateFile) (StateRevision, error) {
	if branch == "" {
		branch = stateMainBranch
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	if path == "/" || strings.Contains(path, "..") {
		return StateRevision{}, invalid("state path must be a relative workspace path")
	}
	if f.Hash == "" || f.Size < 0 {
		return StateRevision{}, invalid("state file needs a content hash and size")
	}
	var out StateRevision
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		c, err := s.getComputer(ctx, tx, scope, computerID)
		if err != nil {
			return err
		}
		if c.Profile != ProfilePersonal {
			return contracts.NewError(contracts.ErrForbidden,
				"state patches apply only to personal computers")
		}
		head, err := scanStateRevision(tx.QueryRow(ctx, `
			SELECT `+stateRevisionSelect+` FROM ahp_compute.state_revisions
			WHERE computer_id = $1 AND branch = $2
			ORDER BY seq DESC LIMIT 1 FOR UPDATE`, computerID, branch))
		if err != nil {
			return err
		}
		if head.ID != expectedHeadID {
			return contracts.NewError(ErrStaleHash,
				fmt.Sprintf("state head moved (was %s, now %s); rebase the patch", expectedHeadID, head.ID))
		}
		tree := cloneStateTree(head.Tree)
		tree[path] = f
		digest, err := stateTreeDigest(tree)
		if err != nil {
			return err
		}
		raw, err := json.Marshal(tree)
		if err != nil {
			return err
		}
		return scanStateRevisionInto(&out, tx.QueryRow(ctx, `
			INSERT INTO ahp_compute.state_revisions
				(id, computer_id, seq, branch, parent_id, base_seq, tree, digest)
			VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8)
			RETURNING `+stateRevisionSelect,
			contracts.NewID(), computerID, head.Seq+1, branch, head.ID, head.Seq, raw, digest))
	})
	return out, err
}

// DeriveStateBranch forks branch from the current main head. The fork point
// revision carries the main tree verbatim, so the branch can be diffed against
// its base for an eventual merge. Deriving from a non-main branch is refused:
// derivative branches are the employee's parallel-work mechanism off main.
func (s *Store) DeriveStateBranch(ctx context.Context, scope contracts.Scope, computerID, name string) (StateRevision, error) {
	if err := validIdentifier(name, "branch name"); err != nil {
		return StateRevision{}, err
	}
	if name == stateMainBranch {
		return StateRevision{}, invalid("branch name 'main' is reserved")
	}
	var out StateRevision
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		c, err := s.getComputer(ctx, tx, scope, computerID)
		if err != nil {
			return err
		}
		if c.Profile != ProfilePersonal {
			return contracts.NewError(contracts.ErrForbidden,
				"derivative branches apply only to personal computers")
		}
		var exists bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM ahp_compute.state_revisions
			WHERE computer_id = $1 AND branch = $2)`, computerID, name).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return contracts.NewError(contracts.ErrConflict, "branch already exists")
		}
		head, err := scanStateRevision(tx.QueryRow(ctx, `
			SELECT `+stateRevisionSelect+` FROM ahp_compute.state_revisions
			WHERE computer_id = $1 AND branch = $2
			ORDER BY seq DESC LIMIT 1 FOR UPDATE`, computerID, stateMainBranch))
		if err != nil {
			return err
		}
		raw, err := json.Marshal(head.Tree)
		if err != nil {
			return err
		}
		return scanStateRevisionInto(&out, tx.QueryRow(ctx, `
			INSERT INTO ahp_compute.state_revisions
				(id, computer_id, seq, branch, parent_id, base_seq, tree, digest)
			VALUES ($1,$2,0,$3,$4,$5,$6::jsonb,$7)
			RETURNING `+stateRevisionSelect,
			contracts.NewID(), computerID, name, head.ID, head.Seq, raw, head.Digest))
	})
	return out, err
}

// stateDiff lists the paths that differ between two trees.
func stateDiff(a, b StateTree) []string {
	seen := map[string]struct{}{}
	var out []string
	add := func(p string) {
		if _, dup := seen[p]; dup {
			return
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	for p, fa := range a {
		fb, ok := b[p]
		if !ok || fa.Hash != fb.Hash || fa.Mode != fb.Mode {
			add(p)
		}
	}
	for p, fb := range b {
		fa, ok := a[p]
		if !ok || fa.Hash != fb.Hash {
			add(p)
		}
	}
	sort.Strings(out)
	return out
}

// MergeStateBranch merges a derivative branch into main. The branch and main
// are diffed against the fork point (the branch's seq-0 revision); if either
// side changed the same path the merge is refused with WORKSPACE_CONFLICT so
// the employee reconciles deliberately. A clean merge appends one main
// revision carrying the merged tree.
func (s *Store) MergeStateBranch(ctx context.Context, scope contracts.Scope, computerID, name string) (StateRevision, error) {
	if err := validIdentifier(name, "branch name"); err != nil {
		return StateRevision{}, err
	}
	var out StateRevision
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		c, err := s.getComputer(ctx, tx, scope, computerID)
		if err != nil {
			return err
		}
		if c.Profile != ProfilePersonal {
			return contracts.NewError(contracts.ErrForbidden,
				"branch merges apply only to personal computers")
		}
		fork, err := scanStateRevision(tx.QueryRow(ctx, `
			SELECT `+stateRevisionSelect+` FROM ahp_compute.state_revisions
			WHERE computer_id = $1 AND branch = $2 AND seq = 0`, computerID, name))
		if err != nil {
			return err
		}
		branchHead, err := scanStateRevision(tx.QueryRow(ctx, `
			SELECT `+stateRevisionSelect+` FROM ahp_compute.state_revisions
			WHERE computer_id = $1 AND branch = $2
			ORDER BY seq DESC LIMIT 1 FOR UPDATE`, computerID, name))
		if err != nil {
			return err
		}
		mainHead, err := scanStateRevision(tx.QueryRow(ctx, `
			SELECT `+stateRevisionSelect+` FROM ahp_compute.state_revisions
			WHERE computer_id = $1 AND branch = $2
			ORDER BY seq DESC LIMIT 1 FOR UPDATE`, computerID, stateMainBranch))
		if err != nil {
			return err
		}
		branchChanged := stateDiff(fork.Tree, branchHead.Tree)
		mainChanged := stateDiff(fork.Tree, mainHead.Tree)
		if len(branchChanged) == 0 {
			return contracts.NewError(contracts.ErrConflict, "branch has no changes to merge")
		}
		mainChangedSet := diffSet(mainChanged)
		var conflict []string
		for _, p := range branchChanged {
			if _, ok := mainChangedSet[p]; ok {
				conflict = append(conflict, p)
			}
		}
		if len(conflict) > 0 {
			sort.Strings(conflict)
			return workspaceConflict(
				"merge conflict on paths: " + strings.Join(conflict, ", "))
		}
		merged := cloneStateTree(mainHead.Tree)
		for _, p := range branchChanged {
			f, ok := branchHead.Tree[p]
			if !ok {
				delete(merged, p)
				continue
			}
			merged[p] = f
		}
		digest, err := stateTreeDigest(merged)
		if err != nil {
			return err
		}
		raw, err := json.Marshal(merged)
		if err != nil {
			return err
		}
		err = scanStateRevisionInto(&out, tx.QueryRow(ctx, `
			INSERT INTO ahp_compute.state_revisions
				(id, computer_id, seq, branch, parent_id, base_seq, tree, digest)
			VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8)
			RETURNING `+stateRevisionSelect,
			contracts.NewID(), computerID, mainHead.Seq+1, stateMainBranch, mainHead.ID, mainHead.Seq, raw, digest))
		if err != nil {
			return err
		}
		// A merged branch is consumed: drop its revisions so the fork point
		// cannot be re-merged twice (idempotency of the merge itself).
		if _, err := tx.Exec(ctx, `
			DELETE FROM ahp_compute.state_revisions
			WHERE computer_id = $1 AND branch = $2`, computerID, name); err != nil {
			return err
		}
		return nil
	})
	return out, err
}

func diffSet(paths []string) map[string]struct{} {
	out := make(map[string]struct{}, len(paths))
	for _, p := range paths {
		out[p] = struct{}{}
	}
	return out
}

func cloneStateTree(in StateTree) StateTree {
	out := make(StateTree, len(in))
	for p, f := range in {
		out[p] = f
	}
	return out
}

// scanStateRevisionInto stores a scanned revision into out.
func scanStateRevisionInto(out *StateRevision, row scanner) error {
	r, err := scanStateRevision(row)
	if err != nil {
		return err
	}
	*out = r
	return nil
}
