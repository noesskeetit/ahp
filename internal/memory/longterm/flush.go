package memory

import "context"

type FlushRequest struct {
	ProjectID string `json:"project_id"`
	SessionID string `json:"session_id"`
}

func (s *Service) Flush(ctx context.Context, id Identity, key string, req FlushRequest) (map[string]any, error) {
	var out map[string]any
	if req.ProjectID == "" || req.SessionID == "" || (id.ActorKind != "runtime" && id.ActorKind != "platform") {
		return nil, Fail(403, "runtime_required", "trusted runtime and session references are required")
	}
	err := s.mutate(ctx, id, key, "flush", req, func(tx Tx) (any, error) {
		cs, err := list[Cursor](tx, "cursor")
		if err != nil {
			return nil, err
		}
		for _, c := range cs {
			if c.ProjectID == req.ProjectID && c.SessionID == req.SessionID {
				c.Flush = true
				if err := tx.Put("cursor", c.ID, c); err != nil {
					return nil, err
				}
			}
		}
		return map[string]any{"accepted": true}, nil
	}, &out)
	return out, err
}
