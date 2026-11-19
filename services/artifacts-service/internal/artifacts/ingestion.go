package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

const ddlIngestion = `
ALTER TABLE ahp_artifacts.artifacts
 ADD COLUMN IF NOT EXISTS ingestion_state text NOT NULL DEFAULT 'pending',
 ADD COLUMN IF NOT EXISTS ingestion_detail text NOT NULL DEFAULT '';
CREATE TABLE IF NOT EXISTS ahp_artifacts.extraction_cache (
 tenant text NOT NULL,
 sha256 text NOT NULL,
 mime text NOT NULL,
 revision text NOT NULL,
 text_content text NOT NULL CHECK (octet_length(text_content)<=2097152),
 text_sha256 text NOT NULL,
 PRIMARY KEY (tenant,sha256,mime,revision)
);
-- Search is derived independently. Remove only the old derived whole-document
-- vector; retain extracted bytes and rebuild bounded search chunks on Process.
ALTER TABLE ahp_artifacts.extraction_cache
 DROP COLUMN IF EXISTS search_vector,
 ADD COLUMN IF NOT EXISTS search_state text NOT NULL DEFAULT 'pending',
 ADD COLUMN IF NOT EXISTS search_detail text NOT NULL DEFAULT '';
CREATE TABLE IF NOT EXISTS ahp_artifacts.extraction_search_chunks (
 tenant text NOT NULL,
 sha256 text NOT NULL,
 mime text NOT NULL,
 revision text NOT NULL,
 chunk_no bigint NOT NULL CHECK (chunk_no>=0),
 lexemes text[] NOT NULL CHECK (cardinality(lexemes) BETWEEN 1 AND 128),
 search_vector tsvector GENERATED ALWAYS AS (array_to_tsvector(lexemes)) STORED,
 PRIMARY KEY (tenant,sha256,mime,revision,chunk_no),
 FOREIGN KEY (tenant,sha256,mime,revision) REFERENCES ahp_artifacts.extraction_cache(tenant,sha256,mime,revision) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS extraction_chunks_search_idx ON ahp_artifacts.extraction_search_chunks USING gin(search_vector);
`

// Process extracts a sealed immutable version through the configured sandbox.
// Cache admission never replaces current artifact authorization. Failures are
// durable quarantine; indexing failures retain extracted text with an independent
// failed search state. Generic derivation APIs cannot clear ingestion states.
func (s *Store) Process(ctx context.Context, scope contracts.Scope, id string) (Readiness, error) {
	ref, err := s.get(ctx, scope, id, true)
	if err != nil {
		return Readiness{}, err
	}
	if err = s.authorize(ctx, scope, "derive", "artifact:"+id); err != nil {
		return Readiness{}, err
	}
	select {
	case s.parserSlots <- struct{}{}:
		defer func() { <-s.parserSlots }()
	default:
		return Readiness{}, contracts.NewError(contracts.ErrConflict, "artifact parser capacity is busy")
	}
	extracted := false
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := checkArtifactWritable(ctx, tx, scope, id); err != nil {
			return err
		}
		var deleted bool
		if err := tx.QueryRow(ctx, `SELECT deleted FROM ahp_artifacts.artifacts WHERE id=$1 FOR UPDATE`, id).Scan(&deleted); err != nil {
			return err
		}
		if deleted {
			return contracts.NewError(contracts.ErrNotFound, "artifact not found")
		}
		// Lock digest, not only id: simultaneous uploads share one extraction.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, ref.Tenant+"|extract|"+ref.SHA256+"|"+ref.MIME); err != nil {
			return err
		}
		state, detail := "ready", ""
		var text string
		if !supportedText(ref.MIME) {
			state, detail = "unsupported", "unsupported_media_type"
		} else if ref.Size > MaxExtractBytes {
			state, detail = "quarantined", "extraction_size_limit"
		} else {
			err := tx.QueryRow(ctx, `SELECT text_content FROM ahp_artifacts.extraction_cache WHERE tenant=$1 AND sha256=$2 AND mime=$3 AND revision=$4`, ref.Tenant, ref.SHA256, ref.MIME, parserRevision).Scan(&text)
			if errors.Is(err, pgx.ErrNoRows) {
				if s.parser.Path == "" {
					return contracts.NewError(contracts.ErrConflict, "artifact parser is not configured")
				}
				object, err := s.objectPath(ctx, id)
				if err != nil {
					return err
				}
				body, err := s.plaintextReader(ctx, ref.Tenant, object, ref.Version)
				if err != nil {
					return err
				}
				text, err = s.parser.extract(ctx, ref.MIME, body)
				body.Close()
				if err != nil {
					state, detail = "quarantined", "parser_refused"
				} else {
					sum := sha256.Sum256([]byte(text))
					_, err = tx.Exec(ctx, `INSERT INTO ahp_artifacts.extraction_cache(tenant,sha256,mime,revision,text_content,text_sha256) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, ref.Tenant, ref.SHA256, ref.MIME, parserRevision, text, hex.EncodeToString(sum[:]))
					if err != nil {
						return err
					}
				}
			} else if err != nil {
				return err
			}
		}
		// Recheck current authority after expensive work and before committing.
		if err := s.authorize(ctx, scope, "derive", "artifact:"+id); err != nil {
			return err
		}
		if _, err := s.admit(ctx, scope, ref.Labels); err != nil {
			return err
		}
		extracted = state == "ready"
		_, err := tx.Exec(ctx, `UPDATE ahp_artifacts.artifacts SET ingestion_state=$2,ingestion_detail=$3 WHERE id=$1 AND NOT deleted`, id, state, detail)
		return err
	})
	if err != nil {
		return Readiness{}, storageError("process artifact", err)
	}
	// Extraction commits before indexing: index rollback cannot discard text.
	if extracted {
		if err := s.indexExtractedText(ctx, scope, ref); err != nil {
			return Readiness{}, err
		}
	}
	return s.Readiness(ctx, scope, id)
}

func (s *Store) processSealed(ctx context.Context, scope contracts.Scope, ref ArtifactRef) (ArtifactRef, error) {
	if !supportedText(ref.MIME) {
		_, err := s.pool.Exec(ctx, `UPDATE ahp_artifacts.artifacts SET ingestion_state='unsupported',ingestion_detail='unsupported_media_type' WHERE id=$1 AND NOT deleted`, ref.ID)
		return ref, storageError("record unsupported extraction", err)
	}
	if s.parser.Path != "" {
		if _, err := s.Process(ctx, scope, ref.ID); err != nil {
			return ArtifactRef{}, err
		}
	}
	return ref, nil
}

// TextChunk is bounded UTF-8 text. Offset and NextOffset count bytes; SHA256
// describes the complete extraction, while Version binds the original object.
type TextChunk struct {
	ID         string `json:"id"`
	Version    string `json:"version"`
	SHA256     string `json:"sha256"`
	Text       string `json:"text"`
	Offset     int64  `json:"offset"`
	NextOffset int64  `json:"next_offset"`
	TotalBytes int64  `json:"total_bytes"`
	EOF        bool   `json:"eof"`
}

// ReadTextChunk checks current authority and exact version on every read.
// contextBudget is a byte budget (a conservative bound on token count).
func (s *Store) ReadTextChunk(ctx context.Context, scope contracts.Scope, id, version string, offset int64, limit, contextBudget int) (TextChunk, error) {
	if version == "" || offset < 0 || offset > MaxExtractBytes || limit <= 0 || limit > MaxTextChunkBytes || contextBudget <= 0 || contextBudget > MaxTextChunkBytes {
		return TextChunk{}, contracts.NewError(contracts.ErrInvalid, "exact version and bounded text range required")
	}
	ref, err := s.get(ctx, scope, id, true)
	if err != nil {
		return TextChunk{}, err
	}
	if ref.Version != version {
		return TextChunk{}, contracts.NewError(contracts.ErrConflict, "artifact version mismatch")
	}
	if limit > contextBudget {
		limit = contextBudget
	}
	var state, chunk, digest string
	var total int64
	// PostgreSQL slices bytes; no entire document enters the agent/API process.
	var raw []byte
	err = s.pool.QueryRow(ctx, `SELECT a.ingestion_state,substring(convert_to(c.text_content,'UTF8') from $5::integer+1 for $6::integer),octet_length(c.text_content),c.text_sha256
 FROM ahp_artifacts.artifacts a JOIN ahp_artifacts.extraction_cache c ON c.tenant=a.tenant AND c.sha256=a.sha256 AND c.mime=a.mime AND c.revision=$4
 WHERE a.id=$1 AND a.tenant=$2 AND a.version=$3 AND NOT a.deleted`, id, scope.Tenant, version, parserRevision, offset, limit).Scan(&state, &raw, &total, &digest)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && state != "ready") {
		return TextChunk{}, contracts.NewError(contracts.ErrConflict, "extraction is not ready")
	}
	if err != nil {
		return TextChunk{}, storageError("read extracted text", err)
	}
	if offset > total {
		return TextChunk{}, ErrRangeNotSatisfiable
	}
	if len(raw) > 0 && !utf8.RuneStart(raw[0]) {
		return TextChunk{}, contracts.NewError(contracts.ErrInvalid, "offset is not a UTF-8 boundary")
	}
	for len(raw) > 0 && !utf8.Valid(raw) {
		raw = raw[:len(raw)-1]
	}
	if len(raw) == 0 && offset < total {
		return TextChunk{}, contracts.NewError(contracts.ErrInvalid, "budget cannot fit next UTF-8 character")
	}
	chunk = string(raw)
	return TextChunk{ID: id, Version: version, SHA256: digest, Text: chunk, Offset: offset, NextOffset: offset + int64(len(raw)), TotalBytes: total, EOF: offset+int64(len(raw)) == total}, nil
}

// SearchText matches all normalized query lexemes across the indexed chunks of
// one authorized artifact, including terms that occur in different chunks.
// Results contain a reference only; text is consumed through ReadTextChunk.
func (s *Store) SearchText(ctx context.Context, scope contracts.Scope, id, version, query string) (bool, error) {
	if len(query) == 0 || len(query) > 1024 || strings.TrimSpace(query) == "" || strings.ContainsRune(query, 0) {
		return false, contracts.NewError(contracts.ErrInvalid, "search query must be 1..1024 bytes")
	}
	ref, err := s.get(ctx, scope, id, true)
	if err != nil {
		return false, err
	}
	if version == "" || version != ref.Version {
		return false, contracts.NewError(contracts.ErrConflict, "artifact version mismatch")
	}
	var matched bool
	err = s.pool.QueryRow(ctx, `SELECT numnode(plainto_tsquery('simple',$5))>0 AND NOT EXISTS (
 SELECT 1 FROM unnest(tsvector_to_array(to_tsvector('simple',$5))) AS query(lexeme)
 WHERE NOT EXISTS (
  SELECT 1 FROM ahp_artifacts.extraction_search_chunks part
  WHERE part.tenant=c.tenant AND part.sha256=c.sha256 AND part.mime=c.mime AND part.revision=c.revision
   AND part.search_vector @@ quote_literal(query.lexeme)::tsquery))
 FROM ahp_artifacts.extraction_cache c JOIN ahp_artifacts.artifacts a ON a.tenant=c.tenant AND a.sha256=c.sha256 AND a.mime=c.mime
 WHERE a.id=$1 AND a.tenant=$2 AND a.version=$3 AND c.revision=$4 AND a.ingestion_state='ready' AND c.search_state='ready' AND NOT a.deleted`, id, scope.Tenant, version, parserRevision, query).Scan(&matched)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, contracts.NewError(contracts.ErrConflict, "search is not ready")
	}
	return matched, storageError("search extracted text", err)
}

// checkContent gates every raw byte delivery, including grant redemption.
func (s *Store) checkContent(ctx context.Context, id string) error {
	var state, mime string
	if err := s.pool.QueryRow(ctx, `SELECT ingestion_state,mime FROM ahp_artifacts.artifacts WHERE id=$1 AND NOT deleted`, id).Scan(&state, &mime); err != nil {
		return contracts.NewError(contracts.ErrNotFound, "artifact not found")
	}
	if state == "quarantined" || (supportedText(mime) && state != "ready") {
		return contracts.NewError(contracts.ErrForbidden, "artifact is quarantined or awaiting extraction")
	}
	return nil
}
