package state

import (
	"context"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// WithCommonPayloadObjects enables the research-only immutable payload layout.
// All readers must have this migration before object writers are enabled.
// Wire/native formats stay unchanged. Default writers continue using inline bytes.
func WithCommonPayloadObjects() StoreOption {
	return func(s *Store) { s.commonRecords = true; s.commonPayloadObjects = true }
}

// Objects belong to one thread, never globally across subjects. All old cuts
// remain roots; no history pruning or online GC is implemented by this option.
const commonPayloadDDL = `
CREATE TABLE IF NOT EXISTS ahp_state.common_payloads(
 thread_id text NOT NULL REFERENCES ahp_state.threads(id) ON DELETE CASCADE,
 sha256 text NOT NULL CHECK(sha256 ~ '^[0-9a-f]{64}$'),attributes bytea NOT NULL,
 PRIMARY KEY(thread_id,sha256));
ALTER TABLE ahp_state.common_records ADD COLUMN IF NOT EXISTS attributes_hash text;
DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conname='common_payload_fk' AND conrelid='ahp_state.common_records'::regclass) THEN
 ALTER TABLE ahp_state.common_records ADD CONSTRAINT common_payload_fk
 FOREIGN KEY(thread_id,attributes_hash) REFERENCES ahp_state.common_payloads(thread_id,sha256)
 DEFERRABLE INITIALLY DEFERRED;
 ALTER TABLE ahp_state.common_records ADD CONSTRAINT common_payload_inline_exclusive
 CHECK(attributes_hash IS NULL OR octet_length(attributes)=0);
 END IF;
END $$;`

func (s *Store) putCommonPayloads(ctx context.Context, tx pgx.Tx, thread string, values map[string][]byte) error {
	hashes := make([]string, 0, len(values))
	payloads := make([][]byte, 0, len(values))
	for h, v := range values {
		hashes = append(hashes, h)
		payloads = append(payloads, v)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO ahp_state.common_payloads(thread_id,sha256,attributes)
 SELECT $1,h,v FROM unnest($2::text[],$3::bytea[]) AS w(h,v) ON CONFLICT DO NOTHING`, thread, hashes, payloads); err != nil {
		return err
	}
	// A hash collision or DBA corruption must never receive a successful receipt.
	// Compare exact opaque bytes without parsing/reformatting native JSON.
	var mismatches int64
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM unnest($2::text[],$3::bytea[]) AS w(h,v)
 LEFT JOIN ahp_state.common_payloads p ON p.thread_id=$1 AND p.sha256=w.h
 WHERE p.sha256 IS NULL OR p.attributes<>w.v`, thread, hashes, payloads).Scan(&mismatches); err != nil {
		return err
	}
	if mismatches != 0 {
		return contracts.NewError(contracts.ErrUnavailable, "common payload integrity check failed")
	}
	return nil
}
