package statejournal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"strings"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/jackc/pgx/v5"
)

const chatPatchType = "langgraph.chat.patch"

type ChatMessage = journal.ChatMessage

type ChatDelta = journal.ChatDelta

func encodeChatDelta(codec string, delta *ChatDelta) ([]byte, error) {
	if delta == nil {
		return nil, nil
	}
	bad := contracts.NewError(contracts.ErrInvalid, "invalid LangGraph chat delta")
	if codec != langGraphCodec {
		return nil, bad
	}
	validMessageID := func(id string) bool {
		return id != "" && len(id) <= 256 && !strings.ContainsRune(id, 0)
	}
	touched := make(map[string]bool)
	for _, id := range delta.Remove {
		if !validMessageID(id) || touched[id] {
			return nil, bad
		}
		touched[id] = true
	}
	for _, msg := range delta.Upserts {
		if !validMessageID(msg.ID) || touched[msg.ID] || !validID(msg.Role) || !json.Valid(msg.Content) {
			return nil, bad
		}
		touched[msg.ID] = true
	}
	ordered := make(map[string]bool)
	for _, id := range delta.Order {
		if !validMessageID(id) || ordered[id] {
			return nil, bad
		}
		ordered[id] = true
	}
	raw, err := json.Marshal(delta)
	if err != nil || len(raw) > maxInlineBytes {
		return nil, bad
	}
	return raw, nil
}

func chatEventID(namespace, checkpointID string) string {
	digest := sha256.Sum256([]byte(namespace + "\x00" + checkpointID))
	return "lg:chat:" + hex.EncodeToString(digest[:])
}

func appendChatDelta(ctx context.Context, tx pgx.Tx, scope contracts.Scope, session string,
	cp Checkpoint, head int64, eventID string, raw []byte) error {
	var parent string
	if cp.ParentID != "" {
		err := tx.QueryRow(ctx, `
			SELECT COALESCE(chat_event_id,'') FROM ahp_journal.checkpoints
			WHERE tenant_id=$1 AND session_id=$2 AND checkpoint_ns=$3 AND checkpoint_id=$4`,
			scope.Tenant, session, cp.Namespace, cp.ParentID).Scan(&parent)
		if err != nil {
			return err
		}
	}
	if err := insertEvent(ctx, tx, scope.Tenant, session, head+1, EventInput{
		ID: eventID, ParentID: parent, Type: chatPatchType, Role: "system", Codec: "json",
		Payload: Payload{Inline: raw},
	}, nil, nil, nil); err != nil {
		return err
	}
	return updateHead(ctx, tx, scope.Tenant, session, head+1, nil)
}

type ChatHistory = journal.ChatHistory

func (s *Store) GetChatHistory(ctx context.Context, scope contracts.Scope, session, namespace, checkpointID string) (ChatHistory, error) {
	history := ChatHistory{Messages: []ChatMessage{}}
	if err := scope.Validate(); err != nil {
		return history, err
	}
	if !validID(session) || namespace != "" && !validID(namespace) || checkpointID != "" && !validID(checkpointID) {
		return history, contracts.NewError(contracts.ErrInvalid, "invalid chat history checkpoint")
	}
	rows, err := s.pool.Query(ctx, `
		WITH RECURSIVE selected AS (
			SELECT c.checkpoint_id, c.chat_event_id
			FROM ahp_journal.checkpoints c JOIN ahp_journal.sessions t USING (tenant_id,session_id)
			WHERE c.tenant_id=$1 AND c.session_id=$2 AND t.deployment_id=$3 AND t.subject_id=$4
			  AND t.status<>'DELETED' AND c.checkpoint_ns=$5 AND ($6='' OR c.checkpoint_id=$6)
			ORDER BY c.checkpoint_id DESC LIMIT 1
		), chain AS (
			SELECT e.seq,e.parent_entry_id,e.inline_payload
			FROM selected c JOIN ahp_journal.session_events e ON e.event_uid=c.chat_event_id
			WHERE e.tenant_id=$1 AND e.session_id=$2 AND e.event_type=$7
			UNION ALL
			SELECT e.seq,e.parent_entry_id,e.inline_payload
			FROM chain child JOIN ahp_journal.session_events e ON e.event_uid=child.parent_entry_id
			WHERE e.tenant_id=$1 AND e.session_id=$2 AND e.event_type=$7 AND e.seq<child.seq
		)
		SELECT c.checkpoint_id,e.inline_payload FROM selected c CROSS JOIN chain e ORDER BY e.seq`,
		scope.Tenant, session, scope.Deployment, scope.Subject, namespace, checkpointID, chatPatchType)
	if err != nil {
		return history, err
	}
	defer rows.Close()
	messages := make(map[string]ChatMessage)
	var order []string
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&history.CheckpointID, &raw); err != nil {
			return ChatHistory{}, err
		}
		var patch ChatDelta
		if err := json.Unmarshal(raw, &patch); err != nil {
			return ChatHistory{}, err
		}
		for _, id := range patch.Remove {
			delete(messages, id)
		}
		if len(patch.Remove) != 0 {
			order = slices.DeleteFunc(order, func(id string) bool {
				_, found := messages[id]
				return !found
			})
		}
		for _, message := range patch.Upserts {
			if _, exists := messages[message.ID]; !exists {
				order = append(order, message.ID)
			}
			messages[message.ID] = message
		}
		if len(patch.Order) != 0 {
			order = patch.Order
		}
	}
	if err := rows.Err(); err != nil {
		return ChatHistory{}, err
	}
	if history.CheckpointID == "" {
		return history, contracts.NewError(contracts.ErrNotFound, "chat history unavailable at this checkpoint")
	}
	if len(order) != len(messages) {
		return ChatHistory{}, contracts.NewError(contracts.ErrUnavailable, "stored chat order is invalid")
	}
	for _, id := range order {
		message, ok := messages[id]
		if !ok {
			return ChatHistory{}, contracts.NewError(contracts.ErrUnavailable, "stored chat order is invalid")
		}
		history.Messages = append(history.Messages, message)
	}
	return history, nil
}
