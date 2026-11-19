package space

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

const (
	maxTerminalCheckpointEntries = 64
	maxTerminalCheckpointBytes   = 8 << 20
)

// Completed turns keep their local projection. Only the bodies already
// confirmed by State occupy this bounded window. Access is under Coordinator.mu.
type terminalCheckpointCache struct {
	order list.List
	bytes int
}

type terminalCheckpointEntry struct {
	threadID   string
	turnID     string
	checkpoint *Checkpoint
	size       int
}

func (c *Coordinator) rememberTerminalCheckpoint(turn *Turn) {
	cp := turn.Checkpoint
	if cp == nil || !cp.runtimeSnapshot || cp.payloadCold {
		return
	}
	cache := &c.terminalCheckpoints
	size := cap(cp.Metadata) + cap(cp.Messages) + cap(cp.State)
	cache.order.PushBack(terminalCheckpointEntry{turn.ThreadID, turn.ID, cp, size})
	cache.bytes += size
	for cache.order.Len() > maxTerminalCheckpointEntries || cache.bytes > maxTerminalCheckpointBytes {
		entry := cache.remove(cache.order.Front())
		old := c.turns[entry.turnID]
		if old == nil || old.Checkpoint != entry.checkpoint || !old.Status.IsTerminal() {
			continue
		}
		header := *entry.checkpoint
		header.Metadata, header.Messages, header.State = nil, nil, nil
		header.payloadCold = true
		old.Checkpoint = &header
	}
}

func (c *terminalCheckpointCache) remove(element *list.Element) terminalCheckpointEntry {
	entry := c.order.Remove(element).(terminalCheckpointEntry)
	c.bytes -= entry.size
	return entry
}

func (c *terminalCheckpointCache) forgetThread(threadID string) {
	for element := c.order.Front(); element != nil; {
		next := element.Next()
		if element.Value.(terminalCheckpointEntry).threadID == threadID {
			c.remove(element)
		}
		element = next
	}
}

// Load the exact snapshot after the HTTP caller's ownership has been checked.
// The fetched body lives only for this read. Loading the latest in a thread
// would substitute a younger turn's checkpoint.
func (c *Coordinator) loadTerminalCheckpoint(ctx context.Context, token string, turn Turn) (Turn, error) {
	if turn.Checkpoint == nil || !turn.Checkpoint.payloadCold {
		return turn, nil
	}
	header := *turn.Checkpoint
	cp, err := c.state.getCheckpoint(ctx, token, turn.ThreadID, header.Namespace, header.ID)
	if err != nil {
		return Turn{}, fmt.Errorf("read saved turn checkpoint: %w", err)
	}
	var state struct {
		State    json.RawMessage `json:"state"`
		Messages json.RawMessage `json:"messages"`
	}
	if err := json.Unmarshal(cp.Metadata, &state); err != nil {
		return Turn{}, fmt.Errorf("decode saved turn checkpoint: %w", err)
	}
	header.Metadata, header.State, header.Messages = cp.Metadata, state.State, state.Messages
	header.payloadCold = false
	turn.Checkpoint = &header
	return turn, nil
}

func (h *Handler) loadTurnCheckpoint(r *http.Request, turn Turn) (Turn, error) {
	if turn.Checkpoint == nil || !turn.Checkpoint.payloadCold {
		return turn, nil
	}
	token, err := h.stateToken(r)
	if err != nil {
		return Turn{}, err
	}
	return h.coordinator.loadTerminalCheckpoint(r.Context(), token, turn)
}

func (c *Coordinator) loadTurnCheckpoints(ctx context.Context, token string, turns []Turn) error {
	for i, turn := range turns {
		full, err := c.loadTerminalCheckpoint(ctx, token, turn)
		if err != nil {
			return err
		}
		turns[i] = full
	}
	return nil
}
