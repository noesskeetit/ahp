package model

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/sdk/go/internal/clienthttp"
	"iter"
)

type Config = clienthttp.Config
type Error = clienthttp.Error
type Client struct{ wire *clienthttp.Client }

func New(cfg Config) (*Client, error) {
	w, e := clienthttp.New(cfg)
	if e != nil {
		return nil, e
	}
	return &Client{w}, nil
}
func validate(r Request) error {
	if e := clienthttp.Required(r.Key); e != nil {
		return e
	}
	if len(r.Messages) == 0 || r.After < 0 {
		return contracts.NewError(contracts.ErrInvalid, "messages and nonnegative cursor required")
	}
	return nil
}

// Submit executes once; recover a lost response by submitting the same request/key.
func (c *Client) Submit(ctx context.Context, r Request) (Generation, error) {
	var out Generation
	if e := validate(r); e != nil {
		return out, e
	}
	e := c.wire.Call(ctx, "/v1/model/SubmitModel", r, &out, true)
	return out, e
}
func (c *Client) Get(ctx context.Context, id string) (Generation, error) {
	var out Generation
	if e := clienthttp.Required(id); e != nil {
		return out, e
	}
	e := c.wire.Call(ctx, "/v1/model/GetModel", map[string]string{"id": id}, &out, false)
	return out, e
}
func (c *Client) Cancel(ctx context.Context, id string) (Generation, error) {
	var out Generation
	if e := clienthttp.Required(id); e != nil {
		return out, e
	}
	e := c.wire.Call(ctx, "/v1/model/CancelModel", map[string]string{"id": id}, &out, true)
	return out, e
}

// Stream yields durable NDJSON records. Resume explicitly with identical request/key
// and After equal to the last consumed Delta.Sequence. Stopping iteration closes
// only the subscription; Cancel is the separate durable cancellation operation.
func (c *Client) Stream(ctx context.Context, r Request) iter.Seq2[Frame, error] {
	return func(yield func(Frame, error) bool) {
		if e := validate(r); e != nil {
			yield(Frame{}, e)
			return
		}
		id := ""
		cursor := r.After
		terminal := false
		stopped := errors.New("reader stopped")
		invalid := func() error {
			return &Error{Code: "INVALID_RESPONSE", Message: "invalid model stream identity or cursor", OutcomeUnknown: true}
		}
		e := c.wire.Lines(ctx, "/v1/model/StreamModel", r, false, func(raw []byte) error {
			var f Frame
			if json.Unmarshal(raw, &f) != nil {
				return invalid()
			}
			switch f.Type {
			case "accepted":
				if id != "" || f.GenerationID == "" {
					return invalid()
				}
				id = f.GenerationID
			case "delta":
				if f.Delta == nil || id == "" || f.Delta.GenerationID != id || f.Delta.Sequence != cursor+1 {
					return invalid()
				}
				cursor = f.Delta.Sequence
			case "terminal":
				if f.Generation == nil || id == "" || f.Generation.ID != id || f.Generation.DeltaSequence != cursor {
					return invalid()
				}
				terminal = true
			case "heartbeat":
			case "error":
				return &Error{Code: f.Code, Message: "model stream refused", OutcomeUnknown: true}
			default:
				return invalid()
			}
			if !yield(f, nil) || terminal {
				return stopped
			}
			return nil
		})
		if errors.Is(e, stopped) {
			return
		}
		if e == nil && !terminal {
			e = &Error{Code: "UNKNOWN", Message: "stream ended before terminal; resume same key and cursor", OutcomeUnknown: true}
		}
		if e != nil {
			yield(Frame{}, e)
		}
	}
}
