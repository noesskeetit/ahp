package artifacts

import (
	"context"
	"errors"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// indexExtractedText commits search independently of extraction. An index
// failure rolls back only index work and persists an honest failure status.
func (s *Store) indexExtractedText(ctx context.Context, scope contracts.Scope, ref ArtifactRef) error {
	if _, err := s.get(ctx, scope, ref.ID, true); err != nil {
		return err
	}
	if err := s.authorize(ctx, scope, "derive", "artifact:"+ref.ID); err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := checkArtifactWritable(ctx, tx, scope, ref.ID); err != nil {
			return err
		}
		var deleted bool
		if err := tx.QueryRow(ctx, `SELECT deleted FROM ahp_artifacts.artifacts WHERE id=$1 FOR UPDATE`, ref.ID).Scan(&deleted); err != nil {
			return err
		}
		if deleted {
			return contracts.NewError(contracts.ErrNotFound, "artifact not found")
		}
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, ref.Tenant+"|extract|"+ref.SHA256+"|"+ref.MIME); err != nil {
			return err
		}
		var state string
		if err := tx.QueryRow(ctx, `SELECT search_state FROM ahp_artifacts.extraction_cache WHERE tenant=$1 AND sha256=$2 AND mime=$3 AND revision=$4`, ref.Tenant, ref.SHA256, ref.MIME, parserRevision).Scan(&state); err != nil {
			return err
		}
		if state == "ready" {
			return nil
		}
		// Nested transaction is a PostgreSQL savepoint. SQL errors (including
		// resource limits) must be rolled back before writing the durable outcome.
		// Lexemes are normalized before grouping (never by cutting source bytes).
		// 128 * (2047-byte lexeme + metadata) stays well below the 1 MiB vector cap.
		// Overlong lexemes are omitted exactly as PostgreSQL to_tsvector does;
		// original text is always preserved in full.
		indexErr := pgx.BeginFunc(ctx, tx, func(index pgx.Tx) error {
			if _, err := index.Exec(ctx, `SELECT set_config('statement_timeout',CASE WHEN setting::bigint=0 THEN '5000' ELSE least(setting::bigint,5000)::text END,true) FROM pg_settings WHERE name='statement_timeout'`); err != nil {
				return err
			}
			if _, err := index.Exec(ctx, `DELETE FROM ahp_artifacts.extraction_search_chunks WHERE tenant=$1 AND sha256=$2 AND mime=$3 AND revision=$4`, ref.Tenant, ref.SHA256, ref.MIME, parserRevision); err != nil {
				return err
			}
			_, err := index.Exec(ctx, `
WITH words AS (
 SELECT DISTINCT word.lexeme
 FROM ahp_artifacts.extraction_cache source
 CROSS JOIN LATERAL ts_debug('simple',source.text_content) parsed
 CROSS JOIN LATERAL unnest(parsed.lexemes) AS word(lexeme)
 WHERE source.tenant=$1 AND source.sha256=$2 AND source.mime=$3 AND source.revision=$4
 AND octet_length(word.lexeme) BETWEEN 1 AND 2047
), parts AS (
 SELECT lexeme,(row_number() OVER (ORDER BY lexeme COLLATE "C")-1)/128 AS chunk_no FROM words
)
INSERT INTO ahp_artifacts.extraction_search_chunks(tenant,sha256,mime,revision,chunk_no,lexemes)
SELECT $1,$2,$3,$4,chunk_no,array_agg(lexeme ORDER BY lexeme COLLATE "C") FROM parts GROUP BY chunk_no`, ref.Tenant, ref.SHA256, ref.MIME, parserRevision)
			return err
		})
		if errors.Is(indexErr, context.Canceled) || errors.Is(indexErr, context.DeadlineExceeded) {
			return indexErr
		}
		state = "ready"
		detail := ""
		if indexErr != nil {
			state, detail = "failed", "index_failed"
			var pgErr *pgconn.PgError
			if errors.As(indexErr, &pgErr) {
				switch pgErr.Code {
				case "54000":
					detail = "index_limit"
				case "57014":
					detail = "index_timeout"
				}
			}
		}
		if err := s.authorize(ctx, scope, "derive", "artifact:"+ref.ID); err != nil {
			return err
		}
		if _, err := s.admit(ctx, scope, ref.Labels); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE ahp_artifacts.extraction_cache SET search_state=$5,search_detail=$6 WHERE tenant=$1 AND sha256=$2 AND mime=$3 AND revision=$4`, ref.Tenant, ref.SHA256, ref.MIME, parserRevision, state, detail)
		return storageError("record search outcome", err)
	})
}
