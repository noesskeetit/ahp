package memory

import (
	"context"
	"encoding/json"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

const (
	DecisionApprove = "approve"
	DecisionReject  = "reject"
)
const (
	labelAction         = CapabilityLabel
	deleteAction        = CapabilityDelete
	deleteSubjectAction = CapabilityDeleteSubject
)

// Review activates or rejects a proposal atomically with its operation key.
// The optional argument preserves Go source compatibility; an omitted key is
// refused, just like a missing op_key on the HTTP mutation.
func (s *Store) Review(ctx context.Context, scope contracts.Scope, id, decision string, keys ...string) (Memory, error) {
	key := firstKey(keys)
	if err := scope.Validate(); err != nil {
		return Memory{}, err
	}
	if err := requireOpKey(key); err != nil {
		return Memory{}, err
	}
	if !idRe.MatchString(id) {
		return Memory{}, contracts.NewError(contracts.ErrNotFound, "memory: not found")
	}
	if decision != DecisionApprove && decision != DecisionReject {
		return Memory{}, contracts.NewError(contracts.ErrInvalid, "memory: decision must be approve or reject")
	}
	if err := s.authorize(ctx, scope, CapabilityReview, "memory:"+id); err != nil {
		return Memory{}, err
	}
	digest, err := mutationDigest("review", id, decision)
	if err != nil {
		return Memory{}, err
	}
	tx, prior, _, replay, err := s.beginMutation(ctx, scope, key, digest)
	if err != nil {
		return Memory{}, err
	}
	defer tx.Rollback(ctx)
	if err = denyErasedTenant(ctx, tx, scope.Tenant); err != nil {
		return Memory{}, err
	}
	if replay {
		// No owning mutation is needed on replay. Release its transaction
		// before the source read/Identity journal call uses other owners.
		if err := tx.Rollback(ctx); err != nil {
			return Memory{}, storageError("release replay", err)
		}
		return s.Get(ctx, scope, prior)
	}
	m, err := readMutationMemory(ctx, tx, scope.Tenant, id)
	if err != nil {
		return Memory{}, err
	}
	ws, err := s.callerWorkspaces(ctx, scope)
	if err != nil {
		return Memory{}, err
	}
	if !s.callerCanRead(ctx, m, scope, ws) {
		return Memory{}, contracts.NewError(contracts.ErrNotFound, "memory: not found")
	}
	if m.State != StateProposed {
		return Memory{}, contracts.NewError(contracts.ErrConflict, "memory: only a proposed record can be reviewed")
	}
	target := StateRejected
	if decision == DecisionApprove {
		target = StateActive
		if m.Supersedes != "" {
			previous, err := readMutationMemory(ctx, tx, scope.Tenant, m.Supersedes)
			if err != nil {
				return Memory{}, err
			}
			if !s.callerCanRead(ctx, previous, scope, ws) {
				return Memory{}, contracts.NewError(contracts.ErrNotFound, "memory: superseded record not found")
			}
			if !sameMemoryNamespace(previous, m) {
				return Memory{}, contracts.NewError(contracts.ErrForbidden, "memory: correction cannot change its source namespace")
			}
			if previous.State != StateActive {
				return Memory{}, contracts.NewError(contracts.ErrConflict, "memory: correction source was already replaced")
			}
			// The source may have narrowed since the proposal was written. Its
			// current labels follow the correction in this same transaction.
			merged := labels.Merge(m.Labels, previous.Labels)
			doc, err := json.Marshal(merged)
			if err != nil {
				return Memory{}, err
			}
			if _, err = tx.Exec(ctx, `UPDATE ahp_memory.memories SET labels=$3::jsonb WHERE tenant=$1 AND id=$2`, scope.Tenant, m.ID, doc); err != nil {
				return Memory{}, storageError("correction labels", err)
			}
			if _, err = tx.Exec(ctx, `UPDATE ahp_memory.memories SET state='superseded',updated_at=now() WHERE tenant=$1 AND id=$2 AND state='active'`, scope.Tenant, m.Supersedes); err != nil {
				return Memory{}, storageError("review supersede", err)
			}
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE ahp_memory.memories SET state=$3,updated_at=now() WHERE tenant=$1 AND id=$2`, scope.Tenant, id, target); err != nil {
		return Memory{}, storageError("review apply", err)
	}
	m, err = readMutationMemory(ctx, tx, scope.Tenant, id)
	if err != nil {
		return Memory{}, err
	}
	if err = finishMutation(ctx, tx, scope, key, id, 0); err != nil {
		return Memory{}, err
	}
	// The mutation has committed, but its existing source is only emitted
	// after current read ACL/lifecycle checks and a successful label journal.
	return s.Get(ctx, scope, id)
}

// AddLabels locks the current record before merging. Concurrent narrowing
// cannot overwrite restrictions introduced by another completed mutation.
func (s *Store) AddLabels(ctx context.Context, scope contracts.Scope, id, opKey string, add labels.Set) (Memory, error) {
	if err := scope.Validate(); err != nil {
		return Memory{}, err
	}
	if err := requireOpKey(opKey); err != nil {
		return Memory{}, err
	}
	if !idRe.MatchString(id) {
		return Memory{}, contracts.NewError(contracts.ErrNotFound, "memory: not found")
	}
	if err := s.authorize(ctx, scope, labelAction, "memory:"+id); err != nil {
		return Memory{}, err
	}
	add = labels.Merge(labels.Set{}, add)
	digest, err := mutationDigest("add_labels", id, add)
	if err != nil {
		return Memory{}, err
	}
	tx, prior, _, replay, err := s.beginMutation(ctx, scope, opKey, digest)
	if err != nil {
		return Memory{}, err
	}
	defer tx.Rollback(ctx)
	if err = denyErasedTenant(ctx, tx, scope.Tenant); err != nil {
		return Memory{}, err
	}
	if replay {
		// No owning mutation is needed on replay. Release its transaction
		// before the source read/Identity journal call uses other owners.
		if err := tx.Rollback(ctx); err != nil {
			return Memory{}, storageError("release replay", err)
		}
		return s.Get(ctx, scope, prior)
	}
	m, err := readMutationMemory(ctx, tx, scope.Tenant, id)
	if err != nil {
		return Memory{}, err
	}
	if m.State == StateDeleted {
		return Memory{}, contracts.NewError(contracts.ErrNotFound, "memory: not found")
	}
	ws, err := s.callerWorkspaces(ctx, scope)
	if err != nil {
		return Memory{}, err
	}
	if !s.callerCanRead(ctx, m, scope, ws) {
		return Memory{}, contracts.NewError(contracts.ErrNotFound, "memory: not found")
	}
	merged := labels.Merge(m.Labels, add)
	if err := labels.CheckWrite(merged, labels.Audience{Kind: labels.KindTenant}); contracts.Code(err) == contracts.ErrInvalid {
		return Memory{}, err
	}
	doc, err := json.Marshal(merged)
	if err != nil {
		return Memory{}, err
	}
	if _, err = tx.Exec(ctx, `UPDATE ahp_memory.memories SET labels=$3::jsonb,updated_at=now() WHERE tenant=$1 AND id=$2`, scope.Tenant, id, doc); err != nil {
		return Memory{}, storageError("add labels", err)
	}
	m, err = readMutationMemory(ctx, tx, scope.Tenant, id)
	if err != nil {
		return Memory{}, err
	}
	if err = finishMutation(ctx, tx, scope, opKey, id, 0); err != nil {
		return Memory{}, err
	}
	// The mutation has committed, but its existing source is only emitted
	// after current read ACL/lifecycle checks and a successful label journal.
	return s.Get(ctx, scope, id)
}

func (s *Store) Delete(ctx context.Context, scope contracts.Scope, id, opKey string) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if err := requireOpKey(opKey); err != nil {
		return err
	}
	if !idRe.MatchString(id) {
		return contracts.NewError(contracts.ErrNotFound, "memory: not found")
	}
	if err := s.authorize(ctx, scope, deleteAction, "memory:"+id); err != nil {
		return err
	}
	digest, err := mutationDigest("delete", id, nil)
	if err != nil {
		return err
	}
	tx, _, _, replay, err := s.beginMutation(ctx, scope, opKey, digest)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if replay {
		return nil
	}
	ws, err := s.callerWorkspaces(ctx, scope)
	if err != nil {
		return err
	}
	m, err := readMutationMemory(ctx, tx, scope.Tenant, id)
	if err != nil {
		return err
	}
	if m.State != StateDeleted && !s.callerCanRead(ctx, m, scope, ws) {
		return contracts.NewError(contracts.ErrNotFound, "memory: not found")
	}
	if _, err = tx.Exec(ctx, `UPDATE ahp_memory.memories SET state='deleted',text='',embedding=NULL,evidence='[]'::jsonb,provenance='{}'::jsonb,source_kind='',updated_at=now() WHERE tenant=$1 AND id=$2 AND state<>'deleted'`, scope.Tenant, id); err != nil {
		return storageError("delete memory", err)
	}
	return finishMutation(ctx, tx, scope, opKey, id, 0)
}

// AuthorizeSubjectDeletion lets a durable coordinator check the Memory grant
// before another owner commits its deletion. It performs no mutation.
func (s *Store) AuthorizeSubjectDeletion(ctx context.Context, scope contracts.Scope, target string) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if !idRe.MatchString(target) {
		return contracts.NewError(contracts.ErrInvalid, "memory: delete-subject target is malformed")
	}
	return s.authorize(ctx, scope, deleteSubjectAction, "subject:"+target)
}

func (s *Store) DeleteSubject(ctx context.Context, scope contracts.Scope, target string, keys ...string) (int, error) {
	key := firstKey(keys)
	if err := scope.Validate(); err != nil {
		return 0, err
	}
	if err := requireOpKey(key); err != nil {
		return 0, err
	}
	if !idRe.MatchString(target) {
		return 0, contracts.NewError(contracts.ErrInvalid, "memory: delete-subject target is malformed")
	}
	if err := s.authorize(ctx, scope, deleteSubjectAction, "subject:"+target); err != nil {
		return 0, err
	}
	digest, err := mutationDigest("delete_subject", target, nil)
	if err != nil {
		return 0, err
	}
	tx, _, count, replay, err := s.beginMutation(ctx, scope, key, digest)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	if replay {
		return int(count), nil
	}
	if err = lockSubjectMutation(ctx, tx, scope.Tenant, target); err != nil {
		return 0, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO ahp_memory.erased_subjects VALUES($1,$2) ON CONFLICT DO NOTHING`, scope.Tenant, target); err != nil {
		return 0, err
	}
	tag, err := tx.Exec(ctx, `UPDATE ahp_memory.memories SET state='deleted',text='',embedding=NULL,evidence='[]'::jsonb,provenance='{}'::jsonb,source_kind='',updated_at=now() WHERE tenant=$1 AND subject=$2 AND state<>'deleted'`, scope.Tenant, target)
	if err != nil {
		return 0, storageError("delete subject", err)
	}
	if err = finishMutation(ctx, tx, scope, key, "", tag.RowsAffected()); err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func firstKey(keys []string) string {
	if len(keys) == 1 {
		return keys[0]
	}
	return ""
}
func requireOpKey(key string) error {
	if key == "" {
		return contracts.NewError(contracts.ErrInvalid, "memory: op_key is required for every mutation")
	}
	if !idRe.MatchString(key) {
		return contracts.NewError(contracts.ErrInvalid, "memory: op_key is malformed")
	}
	return nil
}
