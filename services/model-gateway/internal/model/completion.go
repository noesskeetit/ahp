package model

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// MaxStreamFrameBytes accommodates worst-case JSON escaping of the full 2 MiB
// text bound plus the terminal envelope. Ledger receipts never carry that blob.
const MaxStreamFrameBytes = 16 << 20
const maxEncodedCompletionBytes = MaxStreamFrameBytes - (64 << 10)

func outputDigest(output string) string {
	sum := sha256.Sum256([]byte(output))
	return hex.EncodeToString(sum[:])
}

// prepareCompletion stores the final provider output before publishing its small
// ledger receipt. The generation is still nonterminal until that receipt commits.
// The existing advisory execution owner and expected streamed output prevent a
// second executor from rewriting this result. Erasure fences forbid late content.
func (s *GenerationStore) prepareCompletion(ctx context.Context, scope contracts.Scope, id, streamed, output, finish string, usage Usage) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		owner, err := operations.New(s.pool, "ahp_model")
		if err != nil {
			return err
		}
		if err = owner.CheckTenantWritable(ctx, tx, scope.Tenant); err != nil {
			return err
		}
		if err = owner.CheckSubjectWritable(ctx, tx, scope.Tenant, scope.Subject); err != nil {
			return err
		}
		if err = s.lockGeneration(ctx, tx, scope, id); err != nil {
			if contracts.Code(err) == contracts.ErrNotFound {
				return contracts.NewError(contracts.ErrConflict, "model: completion content fenced or changed")
			}
			return err
		}
		// This command starts after any waited-for append has committed. The
		// parent lock prevents another append/finalize/delete between this exact
		// logical-output check and the materialization below.
		current, err := s.scanGen(tx.QueryRow(ctx, `SELECT `+genStreamCols+` FROM ahp_model.generations
			WHERE id=$1 AND tenant=$2 AND subject=$3 AND deployment=$4`, id, scope.Tenant, scope.Subject, scope.Deployment))
		if err != nil {
			return err
		}
		if current.CancelRequested || terminal(current.Status) || (current.Output != streamed && current.Output != output) {
			return contracts.NewError(contracts.ErrConflict, "model: completion content fenced or changed")
		}
		tag, err := tx.Exec(ctx, `UPDATE ahp_model.generations SET output=$5,output_from_deltas=false,finish_reason=$6,usage_prompt=$7,usage_completion=$8,usage_total=$9,updated_at=now()
   WHERE id=$1 AND tenant=$2 AND subject=$3 AND deployment=$4
    AND status IN ('PENDING','RUNNING','STREAMING') AND NOT cancel_requested`,
			id, scope.Tenant, scope.Subject, scope.Deployment, output, finish, usage.PromptTokens, usage.CompletionTokens, usage.TotalTokens)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return contracts.NewError(contracts.ErrConflict, "model: completion content fenced or changed")
		}
		return nil
	})
}
