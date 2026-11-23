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
	BaseURL string
	Client  *http.Client
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
	res, err := d.Client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 4096))
	// In particular 202 is not evidence that fencing has completed.
	if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusNoContent && !(action == ":ensure" && res.StatusCode == http.StatusAccepted) {
		return fmt.Errorf("Runtime %s returned HTTP %d", action, res.StatusCode)
	}
	return nil
}
