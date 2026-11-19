package statejournal

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/jackc/pgx/v5"
)

const catalogPromptNamespace = "openclaw:catalog-prompts"

type OpenClawCatalogUpdate = journal.OpenClawCatalogUpdate

func validateCatalogUpdate(u OpenClawCatalogUpdate) error {
	if (u.AgentID != "" || u.Key != "") && (!validID(u.AgentID) || strings.Contains(u.AgentID, ":") ||
		!validID(u.Key) || !strings.HasPrefix(u.Key, "agent:"+u.AgentID+":")) {
		return contracts.NewError(contracts.ErrInvalid, "invalid OpenClaw catalog identity")
	}
	if u.Title != nil && (len(*u.Title) > 512 || strings.ContainsRune(*u.Title, 0)) ||
		u.Prompt != nil && len(*u.Prompt) > maxInlineBytes {
		return contracts.NewError(contracts.ErrInvalid, "OpenClaw catalog title or prompt is too large or invalid")
	}
	if len(u.Summary) != 0 {
		var object map[string]json.RawMessage
		if len(u.Summary) > 4096 || json.Unmarshal(u.Summary, &object) != nil || object == nil {
			return contracts.NewError(contracts.ErrInvalid, "OpenClaw catalog summary must be a JSON object up to 4 KiB")
		}
	}
	return nil
}

func updateOpenClawCatalog(ctx context.Context, tx pgx.Tx, scope contracts.Scope, session string, u OpenClawCatalogUpdate) error {
	var promptRef *string
	if u.Prompt != nil {
		digest := ""
		if *u.Prompt != "" {
			// Immutable content under the authenticated user's deployment scope.
			// Different sessions can reuse the bytes without sharing chat history.
			err := tx.QueryRow(ctx, `
				WITH data AS (SELECT $4::bytea AS payload, encode(sha256($4::bytea),'hex') AS digest),
				inserted AS (INSERT INTO ahp_journal.scoped_kv
					(tenant_id,deployment_id,scope_type,scope_owner,namespace,key,version,inline_payload)
					SELECT $1,$2,'user',$3,$5,digest,1,payload FROM data
					ON CONFLICT DO NOTHING)
				SELECT digest FROM data`, scope.Tenant, scope.Deployment, scope.Subject, []byte(*u.Prompt), catalogPromptNamespace).Scan(&digest)
			if err != nil {
				return err
			}
		}
		promptRef = &digest
	}
	var summary any
	if len(u.Summary) != 0 {
		summary = u.Summary
	}
	tag, err := tx.Exec(ctx, `
		UPDATE ahp_journal.sessions
		SET agent_id=COALESCE(NULLIF($3,''),agent_id),native_key=COALESCE(NULLIF($4,''),native_key),
		    title=COALESCE($5,title),prompt_ref=CASE WHEN $6::text IS NULL THEN prompt_ref ELSE NULLIF($6,'') END,
		    summary_metadata=COALESCE($7::jsonb,summary_metadata)
		WHERE tenant_id=$1 AND session_id=$2 AND harness_type='openclaw'
		  AND ($7::jsonb IS NULL OR octet_length(($7::jsonb)::text)<=4096)`,
		scope.Tenant, session, u.AgentID, u.Key, u.Title, promptRef, summary)
	if err == nil && tag.RowsAffected() != 1 {
		return contracts.NewError(contracts.ErrInvalid, "catalog update requires an OpenClaw session and a summary up to 4 KiB")
	}
	return err
}

type CatalogQuery struct {
	Harness, AppID, AgentID, UserID, Status, Search, Cursor string
	Label, SpawnedBy                                        string
	AgentIDs                                                []string
	UpdatedAfter                                            *time.Time
	Limit, Offset                                           int
}

type CatalogPage = journal.CatalogPage

type CatalogSession = journal.CatalogSession

func (s *Store) ListCatalog(ctx context.Context, scope contracts.Scope, q CatalogQuery) (CatalogPage, error) {
	page := CatalogPage{Sessions: []CatalogSession{}}
	if err := scope.Validate(); err != nil {
		return page, err
	}
	if !validID(q.Harness) || q.Limit < 1 || q.Limit > 100 || q.Offset < 0 || q.Offset > 10000 ||
		(q.Offset != 0 && q.Cursor != "") || len(q.Search) > 256 || strings.ContainsRune(q.Search, 0) ||
		len(q.Label) > 512 || strings.ContainsRune(q.Label, 0) || (q.SpawnedBy != "" && !validID(q.SpawnedBy)) || len(q.AgentIDs) > 256 {
		return page, contracts.NewError(contracts.ErrInvalid, "invalid catalog page")
	}
	for _, value := range []string{q.AppID, q.AgentID, q.UserID} {
		if value != "" && !validID(value) {
			return page, contracts.NewError(contracts.ErrInvalid, "invalid catalog filter")
		}
	}
	for _, agent := range q.AgentIDs {
		if !validID(agent) {
			return page, contracts.NewError(contracts.ErrInvalid, "invalid configured agent filter")
		}
	}
	switch q.Status {
	case "", "ACTIVE", "IDLE", "QUEUED", "WAITING_INPUT", "INTERRUPTED":
	default:
		return page, contracts.NewError(contracts.ErrInvalid, "invalid catalog status")
	}
	var before *time.Time
	var beforeID string
	if q.Cursor != "" {
		if len(q.Cursor) > 400 {
			return page, contracts.NewError(contracts.ErrInvalid, "invalid catalog cursor")
		}
		decoded, err := base64.RawURLEncoding.DecodeString(q.Cursor)
		stamp, id, found := strings.Cut(string(decoded), "\n")
		parsed, timestampErr := time.Parse(time.RFC3339Nano, stamp)
		if err != nil || timestampErr != nil || !found || !validID(id) {
			return page, contracts.NewError(contracts.ErrInvalid, "invalid catalog cursor")
		}
		before, beforeID = &parsed, id
	}
	if q.AppID != "" && q.AppID != scope.Deployment || q.UserID != "" && q.UserID != scope.Subject {
		return page, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT session_id,COALESCE(agent_id,''),COALESCE(native_key,''),COALESCE(title,''),status,
		       updated_at,created_at,head_seq,leaf_entry_id,COALESCE(prompt_ref,''),summary_metadata
		FROM ahp_journal.sessions
		WHERE tenant_id=$1 AND deployment_id=$2 AND subject_id=$3 AND harness_type=$4 AND status<>'DELETED'
		  AND ($5='' OR agent_id=$5) AND ($6='' OR status=$6)
		  AND ($7::timestamptz IS NULL OR updated_at>$7)
		  AND ($8='' OR strpos(lower(concat_ws(' ',title,native_key,session_id,summary_metadata->>'label')),lower($8))>0)
		  AND ($9::timestamptz IS NULL OR (updated_at,session_id)<($9,$10))
		  AND ($13::text[] IS NULL OR agent_id=ANY($13))
		  AND ($14='' OR summary_metadata->>'label'=$14)
		  AND ($15='' OR summary_metadata->>'spawned_by'=$15)
		ORDER BY updated_at DESC,session_id DESC LIMIT $11 OFFSET $12`,
		scope.Tenant, scope.Deployment, scope.Subject, q.Harness, q.AgentID, q.Status,
		q.UpdatedAfter, q.Search, before, beforeID, q.Limit+1, q.Offset, q.AgentIDs, q.Label, q.SpawnedBy)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	for rows.Next() {
		var row CatalogSession
		if err := rows.Scan(&row.ID, &row.AgentID, &row.Key, &row.Title, &row.Status,
			&row.UpdatedAt, &row.CreatedAt, &row.HeadSeq, &row.Leaf, &row.PromptRef, &row.Summary); err != nil {
			return CatalogPage{}, err
		}
		if len(page.Sessions) == q.Limit {
			last := page.Sessions[q.Limit-1]
			page.NextCursor = base64.RawURLEncoding.EncodeToString([]byte(last.UpdatedAt.UTC().Format(time.RFC3339Nano) + "\n" + last.ID))
			break
		}
		page.Sessions = append(page.Sessions, row)
	}
	return page, rows.Err()
}
