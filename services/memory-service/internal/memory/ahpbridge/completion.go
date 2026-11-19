package ahpbridge

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/ahp-platform/ahp/internal/memory/longterm"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

type Completion struct {
	ID, ThreadID, ProjectID string
	Sequence                int64
	CreatedAt               time.Time
	Claims                  auth.Claims
	Sources                 []memory.Source
}

// ConsumeCompletion accepts only a trusted State outbox projection. Its claims
// were snapshotted by State from the verified releasing principal; they are
// not parsed from an unsigned client body or a model tool argument.
func ConsumeCompletion(ctx context.Context, service *memory.Service, access *Access, event Completion) error {
	if event.ID == "" || event.ThreadID == "" || event.Claims.Scope().Validate() != nil {
		return contracts.NewError(contracts.ErrInvalid, "invalid completion projection")
	}
	scopes := []memory.Scope{memory.Personal}
	if event.ProjectID != "" {
		scopes = append(scopes, memory.Project)
	}
	ctx = context.WithValue(ctx, stateCompletionKey{}, true)
	ctx = transport.WithPrincipal(ctx, transport.Principal{Claims: event.Claims, Scope: event.Claims.Scope()})
	id, err := access.Authorize(ctx, "", memory.AccessRequest{Action: "runtime:turn", ProjectID: event.ProjectID, Scopes: scopes, Sources: event.Sources})
	if err != nil {
		return err
	}
	req := memory.TurnRequest{ProjectID: event.ProjectID, SessionID: event.ThreadID, Sequence: event.Sequence, Sources: event.Sources, Sanitized: true}
	// A short remaining grant shortens buffering, never extends authority.
	req.Flush = id.ExpiresAt.Sub(event.CreatedAt) < service.Config.PersonalWait+time.Minute
	data, _ := json.Marshal(req)
	sum := sha256.Sum256(data)
	req.IntentDigest = hex.EncodeToString(sum[:])
	req.Sources, err = access.FilterSources(ctx, id, req.Sources)
	if err != nil {
		return err
	}
	_, err = service.Ingest(ctx, id, "state-completion:"+event.ID, req)
	return err
}
