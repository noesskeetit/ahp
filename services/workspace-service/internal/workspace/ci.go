package workspace

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Trusted CI. The PDF (§3.7 с.37, §7 с.48) makes publication run
// PreparePublish → проверки на итоговом дереве → CommitPublish, and the
// audit's boundary is that a *code* candidate must be proven by a trusted CI
// runner, never by whoever wrote the candidate. This file is that trust
// anchor inside the Workspace plane:
//
//   - a workspace owner registers the subjects that are CI runners for this
//     project (a runner is a separate workload subject, not the author);
//   - a check records its provenance: which task ran it and whether the
//     presenting subject was a registered runner at that moment;
//   - Publish refuses a code/executable candidate whose checks lack that
//     provenance, so a browser flag or the author's own task token cannot
//     merge code ("автор сам собрал" is not a build).
//
// Documents and data keep the built-in integrity path (VerifyCandidate) and
// need no runner: the PDF's trusted-test-runner boundary is about executing
// candidate code, which integrity never does.

// CheckProvenance is what the presenting layer observed about the caller that
// posted a check. The store never trusts the flags on this struct: Trusted is
// derived here from the runner registry, and Task must be non-empty for a
// verdict to carry any provenance at all.
type CheckProvenance struct {
	Task     string
	Root     string
	UserLike bool // the credential was a human/user credential, not a task
}

// CIRunner is one registered CI workload for a workspace.
type CIRunner struct {
	Subject string `json:"subject"`
	AddedBy string `json:"added_by"`
	AddedAt string `json:"added_at"`
}

// RegisterCIRunner designates subject as a CI workload whose verdicts carry
// trusted provenance in this workspace. Owner only. A caller may not register
// their own subject: that would turn every task token they hold into a CI
// verdict injector, which is exactly the self-attestation this gate exists to
// refuse. The runner still needs ordinary membership to reach the candidate;
// registration decides trust, membership decides reach.
func (s *Store) RegisterCIRunner(ctx context.Context, scope contracts.Scope, workspace, subject string) error {
	w, err := s.requireMember(ctx, scope, workspace, "ci:register")
	if err != nil {
		return err
	}
	if subject == "" {
		return contracts.NewError(contracts.ErrInvalid, "runner subject is required")
	}
	if subject == scope.Subject {
		return contracts.NewError(contracts.ErrForbidden, "a caller cannot register their own subject as a trusted CI runner")
	}
	_, err = s.writeExec(ctx, scope,
		`INSERT INTO ahp_workspace.ci_runners (workspace_id, subject, added_by) VALUES ($1,$2,$3)
		 ON CONFLICT (workspace_id, subject) DO NOTHING`,
		w.ID, subject, scope.Subject)
	return err
}

// DeregisterCIRunner removes a runner designation. Checks already recorded
// with trusted provenance keep it: they describe a fact that was true when
// the verdict was posted, and they are digest-bound, so they cannot be reused
// against different bytes.
func (s *Store) DeregisterCIRunner(ctx context.Context, scope contracts.Scope, workspace, subject string) error {
	w, err := s.requireMember(ctx, scope, workspace, "ci:remove")
	if err != nil {
		return err
	}
	tag, err := s.writeExec(ctx, scope,
		`DELETE FROM ahp_workspace.ci_runners WHERE workspace_id=$1 AND subject=$2`,
		w.ID, subject)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return contracts.NewError(contracts.ErrNotFound, "runner is not registered")
	}
	return nil
}

// ListCIRunners returns the registered CI workloads for a workspace.
func (s *Store) ListCIRunners(ctx context.Context, scope contracts.Scope, workspace string) ([]CIRunner, error) {
	w, err := s.requireMember(ctx, scope, workspace, "ci:list")
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx,
		`SELECT subject, added_by, added_at FROM ahp_workspace.ci_runners
		  WHERE workspace_id=$1 ORDER BY subject`, w.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CIRunner
	for rows.Next() {
		var r CIRunner
		var at time.Time
		if err := rows.Scan(&r.Subject, &r.AddedBy, &at); err != nil {
			return nil, err
		}
		r.AddedAt = at.UTC().Format(time.RFC3339)
		out = append(out, r)
	}
	return out, rows.Err()
}

// isTrustedRunner answers whether scope.Subject is a registered CI workload of
// this workspace right now. The read is a fresh query per verdict, so a
// revocation takes effect on the next call.
func (s *Store) isTrustedRunner(ctx context.Context, workspaceID, subject string) (bool, error) {
	var trusted bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM ahp_workspace.ci_runners WHERE workspace_id=$1 AND subject=$2)`,
		workspaceID, subject).Scan(&trusted)
	return trusted, err
}

// SetCheckWithProvenance records a check and derives its provenance. The
// caller-supplied struct only describes the credential; Trusted is decided
// from the registry so a forged "trusted: true" changes nothing.
func (s *Store) SetCheckWithProvenance(ctx context.Context, scope contracts.Scope, candidateID, name, status, detail string, prov CheckProvenance) error {
	cand, err := s.getCandidate(ctx, scope, candidateID, "candidate:check")
	if err != nil {
		return err
	}
	if name == "" {
		return contracts.NewError(contracts.ErrInvalid, "check name is required")
	}
	switch status {
	case "pending", "passed", "failed":
	default:
		return contracts.NewError(contracts.ErrInvalid, "check status must be pending, passed or failed")
	}
	if cand.State != "pending" {
		return contracts.NewError(contracts.ErrConflict, "candidate is no longer pending")
	}
	trusted := false
	if !prov.UserLike && strings.TrimSpace(prov.Task) != "" {
		if trusted, err = s.isTrustedRunner(ctx, cand.WorkspaceID, scope.Subject); err != nil {
			return err
		}
	}
	_, err = s.writeExec(ctx, scope, `
INSERT INTO ahp_workspace.checks (id, candidate_id, name, status, target_digest, detail,
                                  trusted, runner_task, runner_root, runner_subject)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
ON CONFLICT (candidate_id, name) DO UPDATE
   SET status = EXCLUDED.status, target_digest = EXCLUDED.target_digest, detail = EXCLUDED.detail,
       trusted = EXCLUDED.trusted, runner_task = EXCLUDED.runner_task,
       runner_root = EXCLUDED.runner_root, runner_subject = EXCLUDED.runner_subject`,
		contracts.NewID(), candidateID, name, status, cand.Digest, detail,
		trusted, prov.Task, prov.Root, scope.Subject)
	return err
}

// codeishExtensions are file types whose candidate content must be executed or
// interpreted by something before it can be called good. Integrity hashing
// cannot vouch for them, only a runner can.
var codeishExtensions = map[string]bool{
	".go": true, ".py": true, ".sh": true, ".bash": true, ".zsh": true, ".js": true,
	".mjs": true, ".cjs": true, ".ts": true, ".tsx": true, ".jsx": true, ".rs": true,
	".java": true, ".kt": true, ".c": true, ".h": true, ".cc": true, ".cpp": true,
	".hpp": true, ".rb": true, ".php": true, ".pl": true, ".lua": true, ".sql": true,
}

// codeishBasenames are executable build entry points recognised without an
// extension.
var codeishBasenames = map[string]bool{
	"makefile": true, "dockerfile": true, "jenkinsfile": true, "build": true,
}

// requiresTrustedCI reports whether a candidate's file set carries code or
// executable bytes. Executable mode bits are decisive; otherwise the name
// decides. Anything that is neither (plain documents and data) keeps the
// built-in integrity path.
func requiresTrustedCI(files []File) bool {
	for _, f := range files {
		if f.Mode&0111 != 0 {
			return true
		}
		base := f.Path[strings.LastIndex(f.Path, "/")+1:]
		lower := strings.ToLower(base)
		if codeishExtensions[strings.ToLower(extendedSuffix(lower))] || codeishBasenames[lower] {
			return true
		}
	}
	return false
}

// extendedSuffix returns the extension of a lowercased base name, or "" when
// the name has none (a dot-only or dot-starting name is not an extension).
func extendedSuffix(lowerBase string) string {
	i := strings.LastIndex(lowerBase, ".")
	if i <= 0 {
		return ""
	}
	return lowerBase[i:]
}

// checkTrustedCI enforces the merge gate for code candidates: every recorded
// check must have been posted as trusted CI provenance — a registered runner,
// a real task id — and (by checkChecks' own digest rule) bound to exactly the
// bytes being merged. A failed or stale check never reaches this gate.
func (s *Store) checkTrustedCI(ctx context.Context, tx pgx.Tx, candidateID string, files []File) error {
	if !requiresTrustedCI(files) {
		return nil
	}
	var name string
	err := tx.QueryRow(ctx, `
SELECT name FROM ahp_workspace.checks
 WHERE candidate_id = $1 AND (trusted = false OR runner_task = '')
 ORDER BY name LIMIT 1`, candidateID).Scan(&name)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil
	case err != nil:
		return err
	}
	return contracts.NewError(contracts.ErrConflict,
		"check "+name+" lacks trusted CI provenance: code candidates merge only on verdicts from a registered CI runner task")
}
