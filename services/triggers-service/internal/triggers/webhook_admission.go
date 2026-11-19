package triggers

import (
	"context"
	"errors"
	"fmt"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

type webhookDelivery struct {
	sequence int64
	digest   string
}

func (s *Service) webhookDelivery(ctx context.Context, trig Trigger, messageID string) (webhookDelivery, bool, error) {
	var d webhookDelivery
	err := s.pool.QueryRow(ctx, fmt.Sprintf(`SELECT sequence, body_digest FROM %s.webhook_deliveries WHERE trigger_id=$1 AND message_id=$2`, quoteIdent(s.schema)), trig.TriggerID, messageID).Scan(&d.sequence, &d.digest)
	if errors.Is(err, pgx.ErrNoRows) {
		return d, false, nil
	}
	return d, err == nil, err
}

// Admission is durable before an effect is sent. The row lock serializes all
// replicas; a missing acknowledgement never returns a reserved slot to MaxFires.
func (s *Service) admitWebhook(ctx context.Context, trig Trigger, messageID, digest string) (webhookDelivery, error) {
	var delivery webhookDelivery
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.ledger.CheckTenantWritable(ctx, tx, trig.Tenant); err != nil {
			return err
		}
		if err := s.ledger.CheckSubjectWritable(ctx, tx, trig.Tenant, trig.Subject); err != nil {
			return err
		}
		current, err := scanTrigger(tx.QueryRow(ctx, fmt.Sprintf(`SELECT %s FROM %s.triggers WHERE trigger_id=$1 AND tenant=$2 AND deployment=$3 AND subject=$4 FOR UPDATE`, triggerSelect, quoteIdent(s.schema)), trig.TriggerID, trig.Tenant, trig.Deployment, trig.Subject))
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.NewError(contracts.ErrNotFound, "triggers: webhook not found")
		}
		if err != nil {
			return err
		}
		if current.Status == StatusRevoked {
			return contracts.NewError(contracts.ErrForbidden, "triggers: webhook revoked")
		}
		err = tx.QueryRow(ctx, fmt.Sprintf(`SELECT sequence, body_digest FROM %s.webhook_deliveries WHERE trigger_id=$1 AND message_id=$2`, quoteIdent(s.schema)), trig.TriggerID, messageID).Scan(&delivery.sequence, &delivery.digest)
		if err == nil {
			if delivery.digest != digest {
				return contracts.NewError(contracts.ErrKeyReuse, "triggers: message_id reused with a different delivery body")
			}
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if current.Status != StatusActive || current.MaxFires != nil && current.FiredCount >= *current.MaxFires {
			return contracts.NewError(contracts.ErrForbidden, "triggers: webhook exhausted or inactive")
		}
		delivery = webhookDelivery{sequence: current.FiredCount + 1, digest: digest}
		if _, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s.webhook_deliveries(trigger_id,message_id,body_digest,sequence) VALUES($1,$2,$3,$4)`, quoteIdent(s.schema)), trig.TriggerID, messageID, digest, delivery.sequence); err != nil {
			return err
		}
		status := StatusActive
		if current.MaxFires != nil && delivery.sequence >= *current.MaxFires {
			status = StatusExhausted
		}
		_, err = tx.Exec(ctx, fmt.Sprintf(`UPDATE %s.triggers SET fired_count=$2,status=$3,version=version+1,updated_at=now() WHERE trigger_id=$1`, quoteIdent(s.schema)), trig.TriggerID, delivery.sequence, status)
		return err
	})
	return delivery, err
}
