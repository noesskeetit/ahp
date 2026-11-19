package execbridge

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// modelStreamFrame is NDJSON, one durable provider fragment per frame. Terminal
// appears only after every fragment through generation.delta_sequence is sent.
type modelStreamFrame struct {
	Type         string             `json:"type"`
	GenerationID string             `json:"generation_id,omitempty"`
	Delta        *model.StreamDelta `json:"delta,omitempty"`
	Generation   *model.Generation  `json:"generation,omitempty"`
	Code         string             `json:"code,omitempty"`
}

func (b *Bridge) streamModel(w http.ResponseWriter, r *http.Request, g model.Generation, after int64, authorize transport.Authorize) {
	if after > g.DeltaSequence {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "model: cursor beyond generation"))
		return
	}
	p, _ := transport.FromContext(r.Context())
	requestCtx, requestStop := context.WithTimeout(r.Context(), 10*time.Minute)
	defer requestStop()
	r = r.WithContext(requestCtx)
	executionError := make(chan error, 1)
	streamAttempt := contracts.NewID()
	// The HTTP subscription is not the generation owner. A disconnected reader
	// may resume this same key, but cannot cancel or redispatch the inference.
	owner, stop := context.WithTimeout(context.WithoutCancel(r.Context()), 10*time.Minute)
	go func() {
		defer stop()
		if authorize != nil {
			go func() {
				tick := time.NewTicker(500 * time.Millisecond)
				defer tick.Stop()
				for {
					select {
					case <-owner.Done():
						return
					case <-tick.C:
						check, cancel := context.WithTimeout(owner, 2*time.Second)
						err := authorize(check, p.Claims, "model")
						if p.Claims.ExpiresAt > 0 && time.Now().Unix() >= p.Claims.ExpiresAt {
							err = context.DeadlineExceeded
						}
						if err != nil {
							cause := "authorization_error"
							if p.Claims.ExpiresAt > 0 && time.Now().Unix() >= p.Claims.ExpiresAt {
								cause = "token_expired"
							}
							attrs := model.InspectExecutionError(check, "owner_authorize", "authorization", err).LogAttrs()
							attrs = append(attrs, slog.String("generation_id", g.ID), slog.String("task_id", g.Task),
								slog.String("stream_attempt_id", streamAttempt), slog.String("cause", cause))
							slog.LogAttrs(check, slog.LevelWarn, "model.owner_cancelled", attrs...)
						}
						cancel()
						if err != nil {
							stop()
							return
						}
					}
				}
			}()
		}
		runObservedModelExecution(owner, p.Scope, g.ID, streamAttempt, b.svc.Execute, executionError)
	}()
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	controller := http.NewResponseController(w)
	encoder := json.NewEncoder(w)
	send := func(frame modelStreamFrame) bool {
		// Initial HTTP verification does not make a long-lived subscription
		// valid beyond exp, including observers that never own inference.
		if frame.Type != "error" && p.Claims.ExpiresAt > 0 && time.Now().Unix() >= p.Claims.ExpiresAt {
			return false
		}
		// A stalled subscriber must not keep a request or database work alive forever.
		_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
		if encoder.Encode(frame) != nil {
			return false
		}
		return controller.Flush() == nil
	}
	if !send(modelStreamFrame{Type: "accepted", GenerationID: g.ID}) {
		return
	}
	ticker := time.NewTicker(40 * time.Millisecond)
	defer ticker.Stop()
	authAt := time.Now()
	sentAt := time.Now()
	for {
		if r.Context().Err() != nil {
			return
		}
		if p.Claims.ExpiresAt > 0 && time.Now().Unix() >= p.Claims.ExpiresAt {
			send(modelStreamFrame{Type: "error", Code: "FORBIDDEN"})
			return
		}
		select {
		case <-executionError:
			send(modelStreamFrame{Type: "error", Code: "UNAVAILABLE"})
			return
		default:
		}
		if time.Since(sentAt) >= 500*time.Millisecond {
			if !send(modelStreamFrame{Type: "heartbeat"}) {
				return
			}
			sentAt = time.Now()
		}
		if authorize != nil && time.Since(authAt) >= 500*time.Millisecond {
			check, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			err := authorize(check, p.Claims, "model")
			cancel()
			authAt = time.Now()
			if err != nil {
				send(modelStreamFrame{Type: "error", Code: "FORBIDDEN"})
				return
			}
		}
		current, err := b.gens.GetStreamSnapshot(r.Context(), p.Scope, g.ID)
		if err != nil {
			send(modelStreamFrame{Type: "error", Code: string(contracts.Code(err))})
			return
		}
		deltas, err := b.gens.ReadDeltas(r.Context(), p.Scope, g.ID, after, 256)
		if err != nil {
			send(modelStreamFrame{Type: "error", Code: string(contracts.Code(err))})
			return
		}
		for _, delta := range deltas {
			if !send(modelStreamFrame{Type: "delta", Delta: &delta}) {
				return
			}
			after = delta.Sequence
		}
		if after >= current.DeltaSequence && current.Status != model.GenPending && current.Status != model.GenRunning && current.Status != model.GenStreaming {
			current.Request = nil
			send(modelStreamFrame{Type: "terminal", Generation: &current})
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
		}
	}
}
