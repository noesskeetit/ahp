package execbridge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// RemoteModel gives Agents Space the same model boundary as custom runtimes.
// The only credential it sends is the current run's task token.
type RemoteModel struct {
	origin, model string
	client        *http.Client
}

func NewRemoteModel(origin, name string, client *http.Client) (*RemoteModel, error) {
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" || u.Scheme != "https" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || client == nil {
		return nil, fmt.Errorf("model gateway requires an HTTPS origin and workload mTLS client")
	}
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &RemoteModel{strings.TrimRight(origin, "/"), name, &copyClient}, nil
}

func (c *RemoteModel) Generate(ctx context.Context, req *core.ModelRequest) (*core.ModelResponse, error) {
	scope, token, ok := core.RunScopeFromContext(ctx)
	binding, bound := core.RunBindingFromContext(ctx)
	if !ok || token == "" || !bound || binding.TaskID == "" {
		return nil, fmt.Errorf("model gateway requires the active run credential and durable binding")
	}
	if err := scope.Validate(); err != nil {
		return nil, fmt.Errorf("model gateway requires a valid run scope: %w", err)
	}
	// Including the canonical request distinguishes a resumed checkpoint's next
	// logical call from an earlier call whose local loop index was also zero.
	digest, err := contracts.Digest([]any{binding.TaskID, req.Step, req.Attempt, req.Messages, req.Tools, req.MaxTokens, c.model})
	if err != nil {
		return nil, err
	}
	body := map[string]any{"key": "core-" + digest, "messages": gatewayMessages(req.Messages), "tools": gatewayTools(req.Tools), "max_tokens": req.MaxTokens}
	if c.model != "" {
		body["model"] = c.model
	}
	gen, err := c.call(ctx, token, "SubmitModel", body)
	if err != nil {
		return nil, err
	}
	for gen.Status == model.GenPending || gen.Status == model.GenRunning || gen.Status == model.GenStreaming {
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		gen, err = c.call(ctx, token, "GetModel", map[string]string{"id": gen.ID})
		if err != nil {
			return nil, err
		}
	}
	return generationResponse(gen)
}

func (c *RemoteModel) call(ctx context.Context, token, op string, input any) (model.Generation, error) {
	data, err := json.Marshal(input)
	if err != nil {
		return model.Generation{}, err
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, c.origin+"/v1/model/"+op, bytes.NewReader(data))
	if err != nil {
		return model.Generation{}, err
	}
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+token)
	response, err := c.client.Do(r)
	if err != nil {
		return model.Generation{}, fmt.Errorf("model gateway transport failed; retry with the same logical key: %w", err)
	}
	defer response.Body.Close()
	data, err = io.ReadAll(io.LimitReader(response.Body, 8<<20))
	if err != nil {
		return model.Generation{}, err
	}
	if response.StatusCode != http.StatusOK {
		// Same rule as the streaming path: the body carries the code, the stable
		// reason and the message naming the refusing check. The old rendering
		// read Code/Reason only and never the "message" field, so the check that
		// refused a unary call was lost the same way.
		return model.Generation{}, refusalError("model gateway refused "+op, response.StatusCode, bytes.NewReader(data))
	}
	var gen model.Generation
	if err := json.Unmarshal(data, &gen); err != nil {
		return gen, fmt.Errorf("model gateway returned an invalid generation")
	}
	return gen, nil
}
