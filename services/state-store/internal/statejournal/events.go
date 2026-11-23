package statejournal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/trace"
)

const maxInlineBytes = 256 << 10
const metadataChangeType = "ahp.state.mutation"

type Payload = journal.Payload

type BlobRef = journal.BlobRef

type EventInput = journal.EventInput

type KVMutation = journal.KVMutation

type PreparedIntent = journal.PreparedIntent

type SHA256Digest = journal.SHA256Digest

type AppendRequest = journal.AppendRequest
type EventPatch = journal.EventPatch

type PointerUpdate = journal.PointerUpdate

type AppendReceipt = journal.AppendReceipt

type Event = journal.Event

// AppendEvents publishes a contiguous batch, shared KV and pre-dispatch intents
// in one transaction. shared grants access to user/app KV and comes from policy.
func (s *Store) AppendEvents(ctx context.Context, scope contracts.Scope, session string, lease Lease, shared bool, request AppendRequest) (AppendReceipt, error) {
	if err := scope.Validate(); err != nil {
		return AppendReceipt{}, err
	}
	if err := validateAppend(session, lease, shared, request); err != nil {
		return AppendReceipt{}, err
	}
	var digestInput any = request
	if request.FinishTurn != "" {
		digestInput = struct {
			Request AppendRequest
			Worker  string
			Epoch   int64
		}{request, lease.Worker, lease.Epoch}
	}
	data, err := json.Marshal(digestInput)
	if err != nil {
		return AppendReceipt{}, err
	}
	digest := sha256.Sum256(data)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return AppendReceipt{}, err
	}
	defer tx.Rollback(ctx)
	head, turn, lockErr := lockSessionState(ctx, tx, scope, session, lease)
	if lockErr != nil && (request.FinishTurn == "" || contracts.Code(lockErr) != contracts.ErrStaleEpoch) {
		return AppendReceipt{}, lockErr
	}
	var receipt AppendReceipt
	var stored []byte
	var savedControl *journal.TurnControlDelta
	err = tx.QueryRow(ctx, `
		SELECT seq, result_head_seq, request_digest, turn_receipt FROM ahp_journal.session_events
		WHERE tenant_id=$1 AND session_id=$2 AND operation_key=$3`,
		scope.Tenant, session, request.Key).Scan(&receipt.FirstSeq, &receipt.HeadSeq, &stored, &savedControl)
	switch {
	case err == nil && slices.Equal(stored, digest[:]):
		receipt.Control = savedControl
		if savedControl != nil && savedControl.NextTurn != nil &&
			(lockErr != nil || turn.Seq != savedControl.NextTurn.InputSeq) {
			// Replaying the result is safe. The old receipt must not grant
			// permission to run a followup that has moved on.
			receipt.Control = nil
		}
		if receipt.Control == nil && lockErr == nil && request.TurnSeq == turn.Seq {
			receipt.Control, err = peekControl(ctx, tx, scope, session, turn)
		}
		return receipt, err
	case lockErr != nil:
		// Only a matching terminal receipt may be read after release.
		return AppendReceipt{}, lockErr
	case err == nil:
		return AppendReceipt{}, contracts.NewError(contracts.ErrKeyReuse, "append key has a different request digest")
	case !errors.Is(err, pgx.ErrNoRows):
		return AppendReceipt{}, err
	case head != request.ExpectedSeq:
		return AppendReceipt{}, contracts.NewError(contracts.ErrConflict, "session head has changed")
	}

	if err := turn.validate(request.TurnSeq, request.SteerAck); err != nil {
		return AppendReceipt{}, err
	}
	if err := applySteerAck(ctx, tx, scope, session, &turn, request.SteerAck); err != nil {
		return AppendReceipt{}, err
	}
	if err := s.pinBlobs(ctx, tx, scope, session, appendBlobs(request)); err != nil {
		return AppendReceipt{}, err
	}
	events := request.Events
	if len(events) == 0 {
		events = []EventInput{{
			ID: "state-mutation-" + hex.EncodeToString(digest[:]), Type: metadataChangeType,
			Role: "system", Codec: "json", Payload: Payload{Inline: []byte("{}")},
		}}
	}
	receipt = AppendReceipt{FirstSeq: head + 1, HeadSeq: head + int64(len(events))}
	for index, event := range events {
		event.TurnSeq = request.TurnSeq
		var key any
		var hash []byte
		var result any
		if index == 0 {
			key, hash, result = request.Key, digest[:], receipt.HeadSeq
		}
		if err := insertEvent(ctx, tx, scope.Tenant, session, receipt.FirstSeq+int64(index), event, key, hash, result); err != nil {
			return AppendReceipt{}, err
		}
	}
	if len(request.EventPatches) > 0 {
		patches, err := json.Marshal(request.EventPatches)
		if err != nil {
			return AppendReceipt{}, err
		}
		tag, err := tx.Exec(ctx, `
			UPDATE ahp_journal.session_events e
			SET is_active=coalesce(p.is_active,e.is_active), is_compacted=coalesce(p.is_compacted,e.is_compacted)
			FROM jsonb_to_recordset($3::jsonb) p(seq bigint, is_active boolean, is_compacted boolean)
			WHERE e.tenant_id=$1 AND e.session_id=$2 AND e.seq=p.seq
				  AND (e.turn_seq IS NULL OR e.event_type NOT IN ('turn.completed','turn.steer','turn.followup'))`, scope.Tenant, session, patches)
		if err != nil {
			return AppendReceipt{}, err
		}
		if tag.RowsAffected() != int64(len(request.EventPatches)) {
			return AppendReceipt{}, contracts.NewError(contracts.ErrInvalid, "event patches must match distinct existing events")
		}
	}
	if err := applyKV(ctx, tx, scope, session, request.KV); err != nil {
		return AppendReceipt{}, err
	}
	if err := prepareIntents(ctx, tx, scope.Tenant, session, lease.Epoch, request.Intents); err != nil {
		return AppendReceipt{}, err
	}
	if err := commitOutcomes(ctx, tx, scope.Tenant, session, request.ToolOutcomes); err != nil {
		return AppendReceipt{}, err
	}
	if err := updateHead(ctx, tx, scope.Tenant, session, receipt.HeadSeq, request.Pointers); err != nil {
		return AppendReceipt{}, err
	}
	if request.Catalog != nil {
		if err := updateCatalog(ctx, tx, scope, session, *request.Catalog); err != nil {
			return AppendReceipt{}, err
		}
	}
	if request.FinishTurn != "" {
		if turn.Mode == journal.ExecutionVMManaged {
			receipt.Control, err = finishManagedTurn(ctx, tx, scope, session, request.FinishTurn, lease, receipt.HeadSeq, turn)
			receipt.HeadSeq++
			if err == nil {
				_, err = tx.Exec(ctx, `UPDATE ahp_journal.session_events
					SET result_head_seq=$4,turn_receipt=$5 WHERE tenant_id=$1 AND session_id=$2 AND seq=$3`,
					scope.Tenant, session, receipt.FirstSeq, receipt.HeadSeq, receipt.Control)
			}
		} else {
			err = finishTurn(ctx, tx, scope, session, request.FinishTurn)
		}
	} else {
		receipt.Control, err = peekControl(ctx, tx, scope, session, turn)
	}
	if err != nil {
		return AppendReceipt{}, err
	}

	if turn.Mode == journal.ExecutionVMManaged {
		if err := notifyJournal(ctx, tx, scope, session); err != nil {
			return AppendReceipt{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return AppendReceipt{}, err
	}
	return receipt, nil
}

func applyKV(ctx context.Context, tx pgx.Tx, scope contracts.Scope, session string, changes []KVMutation) error {
	mutations := slices.Clone(changes)
	slices.SortStableFunc(mutations, func(a, b KVMutation) int {
		for _, fields := range [][2]string{{a.ScopeType, b.ScopeType}, {a.Namespace, b.Namespace}, {a.Key, b.Key}} {
			if order := strings.Compare(fields[0], fields[1]); order != 0 {
				return order
			}
		}
		return 0
	})
	for _, mutation := range mutations {
		if err := putKV(ctx, tx, scope, session, mutation); err != nil {
			return err
		}
	}
	return nil
}

func insertEvent(ctx context.Context, tx pgx.Tx, tenant, session string, seq int64, event EventInput, key any, digest []byte, result any) error {
	var traceID, spanID string
	if span := trace.SpanContextFromContext(ctx); span.IsValid() {
		traceID, spanID = span.TraceID().String(), span.SpanID().String()
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO ahp_journal.session_events
			(tenant_id,session_id,seq,event_uid,parent_entry_id,event_type,role,codec,
			 inline_payload,blob_ref,operation_key,request_digest,result_head_seq,content_text,otel_trace_id,otel_span_id,turn_seq)
		SELECT $1,$2,$3,$4,nullif($5,''),$6,$7,$8,$9,$10,$11,$12,$13,nullif($14,''),nullif($15,''),nullif($16,''),nullif($17::bigint,0)
		WHERE $5='' OR EXISTS (
			SELECT 1 FROM ahp_journal.session_events WHERE tenant_id=$1 AND session_id=$2 AND event_uid=$5)`,
		tenant, session, seq, event.ID, event.ParentID, event.Type, event.Role, event.Codec,
		event.Payload.Inline, blobValue(event.Payload), key, digest, result, event.Text, traceID, spanID, event.TurnSeq)
	if err == nil && tag.RowsAffected() != 1 {
		return contracts.NewError(contracts.ErrInvalid, "event parent must be an earlier row in the same session")
	}
	return classifyWrite(err)
}

func updateHead(ctx context.Context, tx pgx.Tx, tenant, session string, head int64, update *PointerUpdate) error {
	var leaf, parent *string
	if update != nil {
		leaf, parent = update.Leaf, update.AppendParent
	}
	tag, err := tx.Exec(ctx, `
		UPDATE ahp_journal.sessions
		SET head_seq=$3, updated_at=clock_timestamp(),
		    leaf_entry_id=CASE WHEN $4::text IS NULL THEN leaf_entry_id ELSE nullif($4,'') END,
		    append_parent_id=CASE WHEN $5::text IS NULL THEN append_parent_id ELSE nullif($5,'') END
		WHERE tenant_id=$1 AND session_id=$2
		  AND (nullif($4,'') IS NULL OR EXISTS (
		    SELECT 1 FROM ahp_journal.session_events e WHERE e.tenant_id=$1 AND e.session_id=$2 AND e.event_uid=$4))
		  AND (nullif($5,'') IS NULL OR EXISTS (
		    SELECT 1 FROM ahp_journal.session_events e WHERE e.tenant_id=$1 AND e.session_id=$2 AND e.event_uid=$5))`,
		tenant, session, head, leaf, parent)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return contracts.NewError(contracts.ErrInvalid, "session pointer must refer to an event in the same session")
	}
	return nil
}

func putKV(ctx context.Context, tx pgx.Tx, scope contracts.Scope, session string, change KVMutation) error {
	owner := session
	switch change.ScopeType {
	case "user", "store":
		owner = scope.Subject
	case "app":
		owner = scope.Deployment
	}
	var tag pgconn.CommandTag
	var err error
	if change.Delete {
		// Keep the revision across deletion so an older writer cannot update
		// a deleted and re-created key using a previously valid CAS version.
		change.Payload = Payload{Inline: []byte{}}
	}
	if change.ExpectedVersion == 0 {
		tag, err = tx.Exec(ctx, `
			INSERT INTO ahp_journal.scoped_kv
				(tenant_id, deployment_id, scope_type, scope_owner, namespace, key, version, inline_payload, blob_ref, deleted)
			VALUES ($1,$2,$3,$4,$5,$6,1,$7,$8,$9) ON CONFLICT DO NOTHING`, scope.Tenant,
			scope.Deployment, change.ScopeType, owner, change.Namespace, change.Key,
			change.Payload.Inline, blobValue(change.Payload), change.Delete)
	} else {
		tag, err = tx.Exec(ctx, `
			UPDATE ahp_journal.scoped_kv SET version=version+1, inline_payload=$7, blob_ref=$8,
			    deleted=$10, updated_at=clock_timestamp()
			WHERE tenant_id=$1 AND deployment_id=$2 AND scope_type=$3 AND scope_owner=$4
			  AND namespace=$5 AND key=$6 AND version=$9`, scope.Tenant,
			scope.Deployment, change.ScopeType, owner, change.Namespace, change.Key,
			change.Payload.Inline, blobValue(change.Payload), change.ExpectedVersion, change.Delete)
	}
	if err != nil {
		return classifyWrite(err)
	}
	if tag.RowsAffected() != 1 {
		return contracts.NewError(contracts.ErrConflict, "KV revision has changed")
	}
	return nil
}

func validateAppend(session string, lease Lease, shared bool, request AppendRequest) error {
	if !validID(session) || !validID(lease.Worker) || lease.Epoch <= 0 || !validID(request.Key) ||
		request.ExpectedSeq < 0 || len(request.Events) > 256 ||
		len(request.KV) > 256 || len(request.Intents) > 256 || len(request.EventPatches) > 4096 {
		return contracts.NewError(contracts.ErrInvalid, "invalid session fence or append batch")
	}
	if len(request.Events) == 0 && len(request.KV) == 0 && len(request.EventPatches) == 0 &&
		len(request.Intents) == 0 && len(request.ToolOutcomes) == 0 && request.Pointers == nil &&
		request.Catalog == nil && request.FinishTurn == "" && request.SteerAck == nil {
		return contracts.NewError(contracts.ErrInvalid, "append batch needs an event or a state mutation")
	}
	if request.FinishTurn != "" && request.FinishTurn != SessionIdle && request.FinishTurn != SessionWaitingInput {
		return contracts.NewError(contracts.ErrInvalid, "invalid turn disposition")
	}
	inlineSize := 0
	for _, event := range request.Events {
		if !validID(event.ID) || event.ID == event.ParentID || !validID(event.Type) ||
			event.Type == chatPatchType || event.Type == metadataChangeType ||
			(request.TurnSeq != 0 && (event.Type == "turn.completed" || event.Type == "turn.input" || event.Type == "turn.steer" || event.Type == "turn.followup")) ||
			!validID(event.Role) || !validID(event.Codec) || !validPayload(event.Payload) ||
			len(event.Text) > 32<<10 || strings.IndexByte(event.Text, 0) >= 0 ||
			(event.Text != "" && event.Role != "user" && event.Role != "assistant" && event.Role != "compaction") {
			return contracts.NewError(contracts.ErrInvalid, "invalid event")
		}
		inlineSize += len(event.Payload.Inline)
	}
	for _, patch := range request.EventPatches {
		if patch.Seq <= 0 || patch.Seq > request.ExpectedSeq || (patch.Active == nil && patch.Compacted == nil) {
			return contracts.NewError(contracts.ErrInvalid, "invalid event visibility patch")
		}
	}
	for _, change := range request.KV {
		if change.Namespace == catalogPromptNamespace || change.Namespace == kvReceiptNamespace {
			return contracts.NewError(contracts.ErrInvalid, "reserved journal KV namespace")
		}
		switch change.ScopeType {
		case "session":
		case "user", "app", "store":
			if !shared {
				return contracts.NewError(contracts.ErrForbidden, "shared KV requires a grant")
			}
			if change.Payload.Blob != nil {
				return contracts.NewError(contracts.ErrInvalid, "shared KV requires an inline payload")
			}
		default:
			return contracts.NewError(contracts.ErrInvalid, "unknown KV scope")
		}
		if !validID(change.Namespace) || !validID(change.Key) || change.ExpectedVersion < 0 ||
			(!change.Delete && !validPayload(change.Payload)) ||
			(change.Delete && (change.Payload.Inline != nil || change.Payload.Blob != nil)) {
			return contracts.NewError(contracts.ErrInvalid, "invalid KV mutation")
		}
		inlineSize += len(change.Payload.Inline)
	}
	for _, outcome := range request.ToolOutcomes {
		if outcome.Result != nil {
			inlineSize += len(outcome.Result.Inline)
		}
	}
	if inlineSize > 2<<20 {
		return contracts.NewError(contracts.ErrInvalid, "append batch exceeds 2 MiB of inline payload")
	}
	if err := validateIntents(request.Intents); err != nil {
		return err
	}
	if request.Catalog != nil {
		if err := validateCatalogUpdate(*request.Catalog); err != nil {
			return err
		}
	}
	return validateOutcomes(request.ToolOutcomes)
}

func validateIntents(intents []PreparedIntent) error {
	if len(intents) > 256 {
		return contracts.NewError(contracts.ErrInvalid, "intent batch exceeds 256 items")
	}
	for _, intent := range intents {
		if !validID(intent.CallID) || !validID(intent.ToolName) || !validID(intent.IdempotencyKey) || intent.ArgsDigest == (SHA256Digest{}) {
			return contracts.NewError(contracts.ErrInvalid, "invalid intent")
		}
	}
	return nil
}

func prepareIntents(ctx context.Context, tx pgx.Tx, tenant, session string, epoch int64, intents []PreparedIntent) error {
	for _, intent := range intents {
		_, err := tx.Exec(ctx, `
			INSERT INTO ahp_journal.tool_intents
				(tenant_id, session_id, call_id, tool_name, args_digest, idempotency_key, status, prepared_epoch)
			VALUES ($1,$2,$3,$4,$5,$6,'PREPARED',$7)`, tenant, session,
			intent.CallID, intent.ToolName, intent.ArgsDigest[:], intent.IdempotencyKey, epoch)
		if err != nil {
			return classifyWrite(err)
		}
	}
	return nil
}

type EventPage = journal.EventPage

// GetEvents reads a page and its durable head from one database snapshot.
func (s *Store) GetEvents(ctx context.Context, scope contracts.Scope, session string, after int64, limit int) (EventPage, error) {
	return s.getEvents(ctx, scope, session, after, limit, "")
}

type EventFilters struct {
	AfterTime  *time.Time
	ActiveOnly bool
	Search     string
	BranchLeaf string
	Recent     bool
}

// getEvents can include scoped state from the same read snapshot. The caller
// must check state.shared before supplying a namespace.
func (s *Store) getEvents(ctx context.Context, scope contracts.Scope, session string, after int64, limit int, stateNamespace string) (EventPage, error) {
	return s.filteredEvents(ctx, scope, session, after, limit, stateNamespace, EventFilters{})
}

func (s *Store) filteredEvents(ctx context.Context, scope contracts.Scope, session string, after int64, limit int, stateNamespace string, filters EventFilters) (EventPage, error) {
	return s.readEvents(ctx, scope, session, after, limit, stateNamespace, filters, false)
}

func (s *Store) readEvents(ctx context.Context, scope contracts.Scope, session string, after int64, limit int, stateNamespace string, filters EventFilters, vmManagedOnly bool) (EventPage, error) {
	var page EventPage
	if err := scope.Validate(); err != nil {
		return page, err
	}
	if !validID(session) || after < 0 || limit < 1 || limit > 1000 ||
		len(filters.Search) > 512 || strings.IndexByte(filters.Search, 0) >= 0 ||
		(filters.BranchLeaf != "" && !validID(filters.BranchLeaf)) {
		return page, contracts.NewError(contracts.ErrInvalid, "invalid event page")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return page, err
	}
	defer tx.Rollback(ctx)
	err = tx.QueryRow(ctx, `
		SELECT head_seq, leaf_entry_id, append_parent_id, lease_epoch FROM ahp_journal.sessions
		WHERE tenant_id=$1 AND session_id=$2 AND deployment_id=$3 AND subject_id=$4
		  AND status <> 'DELETED' AND (NOT $5::boolean OR execution_mode='vm_managed')`,
		scope.Tenant, session, scope.Deployment, scope.Subject, vmManagedOnly).
		Scan(&page.HeadSeq, &page.Leaf, &page.AppendParent, &page.Epoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return page, contracts.NewError(contracts.ErrNotFound, "session not found")
	}
	if err != nil {
		return page, err
	}
	rows, err := queryEventRows(ctx, tx, scope.Tenant, session, after, limit, filters)
	if err != nil {
		return page, err
	}
	page.Events, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (Event, error) {
		var event Event
		err := row.Scan(&event.Seq, &event.ID, &event.ParentID, &event.Type, &event.Role,
			&event.Codec, &event.Payload.Inline, &event.Payload.Blob, &event.Text,
			&event.Active, &event.Compacted, &event.TraceID, &event.SpanID, &event.TurnSeq)
		return event, err
	})
	if err != nil {
		return page, err
	}
	if filters.Recent {
		slices.Reverse(page.Events)
	}
	if stateNamespace != "" {
		page.State, err = readKV(ctx, tx, scope, session, stateNamespace, "")
		if err != nil {
			return page, err
		}
	}
	return page, tx.Commit(ctx)
}

func queryEventRows(ctx context.Context, tx pgx.Tx, tenant, session string, after int64, limit int, filters EventFilters) (pgx.Rows, error) {
	query := `
		SELECT seq, event_uid, coalesce(parent_entry_id,''), event_type, role, codec, inline_payload, blob_ref,
			coalesce(content_text,''), is_active, is_compacted, coalesce(otel_trace_id,''), coalesce(otel_span_id,''), coalesce(turn_seq,0)
		FROM ahp_journal.session_events
		WHERE tenant_id=$1 AND session_id=$2 AND seq>$3`
	args := []any{tenant, session, after, limit}
	condition := func(clause string, value any) {
		args = append(args, value)
		query += " AND " + fmt.Sprintf(clause, len(args))
	}
	if filters.AfterTime != nil {
		condition("created_at > $%d", filters.AfterTime)
	}
	if filters.ActiveOnly {
		query += " AND is_active"
	}
	if filters.Search != "" {
		condition("role IN ('user','assistant','compaction') AND search_tsv @@ plainto_tsquery('simple', $%d)", filters.Search)
	}
	if filters.BranchLeaf != "" {
		args = append(args, filters.BranchLeaf)
		query = fmt.Sprintf(`
			WITH RECURSIVE branch(event_uid, parent_entry_id) AS (
				SELECT event_uid, parent_entry_id FROM ahp_journal.session_events
				WHERE tenant_id=$1 AND session_id=$2 AND event_uid=$%d
				UNION ALL
				SELECT e.event_uid, e.parent_entry_id FROM ahp_journal.session_events e
				JOIN branch b ON e.event_uid=b.parent_entry_id WHERE e.tenant_id=$1 AND e.session_id=$2
			)`, len(args)) + query + " AND event_uid IN (SELECT event_uid FROM branch)"
	}
	query += " ORDER BY seq"
	if filters.Recent {
		query += " DESC"
	}
	return tx.Query(ctx, query+" LIMIT $4", args...)
}

func validPayload(payload Payload) bool {
	if payload.Blob == nil {
		return payload.Inline != nil && len(payload.Inline) <= maxInlineBytes
	}
	hash, err := hex.DecodeString(payload.Blob.SHA256)
	return payload.Inline == nil && payload.Blob.SizeBytes > maxInlineBytes &&
		validID(payload.Blob.ArtifactID) && validID(payload.Blob.Version) &&
		payload.Blob.URI == "" && payload.Blob.Size == 0 && err == nil && len(hash) == sha256.Size
}

func blobValue(payload Payload) any {
	if payload.Blob == nil {
		return nil
	}
	return payload.Blob
}

func classifyWrite(err error) error {
	var failure *pgconn.PgError
	if errors.As(err, &failure) {
		switch failure.Code {
		case "23505":
			return contracts.NewError(contracts.ErrConflict, "journal identifier is already in use")
		case "23503", "23514":
			return contracts.NewError(contracts.ErrInvalid, "journal reference or payload is invalid")
		}
	}
	return err
}
