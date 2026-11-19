package state

import (
	"context"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

type EvidenceFrame struct {
	StreamEvent
	CreatedAt time.Time
}
type RunEvidence struct {
	Run       Run
	Thread    Thread
	StartedAt time.Time
	Frames    []EvidenceFrame
	Truncated bool
}

// ReadRunEvidence is State's bounded, scope-checked projection for the trusted
// completion consumer. Other services never query State tables themselves.
func (s *Store) ReadRunEvidence(ctx context.Context, scope contracts.Scope, id string, limit int) (RunEvidence, error) {
	var out RunEvidence
	if limit < 1 || limit > 1000 {
		return out, invalid("invalid evidence limit")
	}
	var err error
	out.Run, err = s.GetRun(ctx, scope, id)
	if err != nil {
		return out, err
	}
	out.Thread, err = s.GetThread(ctx, scope, out.Run.ThreadID)
	if err != nil {
		return out, err
	}
	if err := s.pool.QueryRow(ctx, `SELECT created_at FROM ahp_state.runs WHERE id=$1`, id).Scan(&out.StartedAt); err != nil {
		return out, err
	}
	rows, err := s.pool.Query(ctx, `SELECT id,thread_id,key,kind,sequence,payload,created_at FROM
 (SELECT id,thread_id,key,kind,sequence,payload,created_at FROM ahp_state.stream_events WHERE thread_id=$1 AND run_id=$2 AND kind IN ('model','tool') ORDER BY sequence DESC LIMIT $3) tail ORDER BY sequence`, out.Thread.ID, id, limit+1)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var frame EvidenceFrame
		if err := rows.Scan(&frame.ID, &frame.ThreadID, &frame.Key, &frame.Kind, &frame.Sequence, &frame.Payload, &frame.CreatedAt); err != nil {
			return out, err
		}
		out.Frames = append(out.Frames, frame)
	}
	if err := rows.Err(); err != nil {
		return out, err
	}
	if len(out.Frames) > limit {
		out.Truncated = true
		out.Frames = out.Frames[len(out.Frames)-limit:]
	}
	return out, nil
}
