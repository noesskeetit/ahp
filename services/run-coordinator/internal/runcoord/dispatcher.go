package runcoord

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type HTTPDispatcher struct {
	BaseURL     string
	Client      *http.Client
	TokenSource func(context.Context) (string, error)
}

func (d HTTPDispatcher) EnsureRunning(ctx context.Context, execution Execution) error {
	return d.call(ctx, ":ensure", execution)
}

func (d HTTPDispatcher) Fence(ctx context.Context, execution Execution) error {
	return d.call(ctx, ":fence", execution)
}

func (d HTTPDispatcher) call(ctx context.Context, action string, execution Execution) error {
	base, err := url.Parse(d.BaseURL)
	if err != nil || base.Scheme != "https" || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		return fmt.Errorf("Runtime endpoint must be an HTTPS base URL")
	}
	payload, err := json.Marshal(execution)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(base.String(), "/")+"/internal/v1/executions"+action, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if d.TokenSource != nil {
		token, err := d.TokenSource(ctx)
		if err != nil {
			return fmt.Errorf("read runtime dispatch token: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := d.Client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 4096))
	if res.StatusCode != http.StatusOK && !(action == ":ensure" &&
		(res.StatusCode == http.StatusAccepted || res.StatusCode == http.StatusNoContent)) {
		return fmt.Errorf("Runtime %s returned HTTP %d", action, res.StatusCode)
	}
	return nil
}
