package space

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// DotConfigPinner is the owner API boundary; the runner has no Registry database.
type DotConfigPinner interface {
	Pin(context.Context, string, string, string) (*AgentConfigRef, error)
}

type profileClient struct {
	origin string
	client *http.Client
}

// NewProfileClient configures Registry's workload-authenticated owner API.
func NewProfileClient(origin string, client *http.Client) (DotConfigPinner, error) {
	owner, err := newRegistryOwnerClient(origin, client)
	if err != nil {
		return nil, err
	}
	return owner, nil
}

func newRegistryOwnerClient(origin string, client *http.Client) (*profileClient, error) {
	origin, err := planeOrigin("AS_REGISTRY_URL", origin)
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(origin, "https://") || client == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "profile owner API requires HTTPS and workload client")
	}
	copy := *client
	copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &profileClient{origin, &copy}, nil
}
func (p *profileClient) Pin(ctx context.Context, token, key, contextID string) (*AgentConfigRef, error) {
	data, _ := json.Marshal(map[string]string{"key": key, "context_id": contextID})
	r, err := http.NewRequestWithContext(ctx, "POST", p.origin+"/v1/registry/PinDotConfig", bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	setServiceRequest(r, token, "application/json")
	response, err := p.client.Do(r)
	if err != nil {
		return nil, contracts.NewError(contracts.ErrUnavailable, "profile owner API unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, profileRefusal(response)
	}
	var out struct {
		Snapshot *AgentConfigRef `json:"snapshot"`
	}
	if err = json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&out); err != nil || out.Snapshot == nil || out.Snapshot.Dot == nil {
		return nil, contracts.NewError(contracts.ErrUnavailable, "profile owner API returned invalid snapshot")
	}
	return out.Snapshot, nil
}

// profileRefusal keeps the CLASS the owner API chose for a refusal of PinDotConfig.
//
// `artifactsRefusal` already carries a refusal that speaks the contracts vocabulary as a
// contracts error, which is what lets the AcceptTurn caller answer it with the right status.
// What it cannot express on its own is a dependency outage stated without a contracts
// envelope -- Registry behind a gateway answers 502/503/504 with an HTML body, and
// `codeForStatus` deliberately knows no server-side class. Left alone, such an outage reached
// `transport.WriteError` as a value it cannot classify and the browser was told
// `500 INTERNAL / internal server error` about a dependency that had plainly said "temporarily
// unavailable" -- the same collapse measured on the live stand for the envelope case.
//
// Only the dependency-outage class is mapped, and only on this owner path: Artifacts' download
// proxy keeps answering upstream verdicts verbatim (`writeRefusal`), and an upstream INTERNAL
// is NOT relabelled as a dependency outage -- a bug upstream stays an internal error here
// rather than being laundered into something retryable.
func profileRefusal(response *http.Response) error {
	refusal := artifactsRefusal(response)
	if contracts.Code(refusal) != "" {
		return refusal
	}
	var stated *upstreamRefusal
	if !errors.As(refusal, &stated) {
		return refusal
	}
	switch response.StatusCode {
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		message := stated.message
		if message == "" {
			message = fmt.Sprintf("profile owner API refused the pin with HTTP %d", response.StatusCode)
		}
		if stated.reason != "" {
			return contracts.NewReasoned(contracts.ErrUnavailable, stated.reason, message)
		}
		return contracts.NewError(contracts.ErrUnavailable, message)
	default:
		return refusal
	}
}

// DotRuntimeConfig is the immutable owner response, carried in State's run payload.
type DotRuntimeConfig struct {
	Instructions string `json:"instructions"`
	ModelRoute   *struct {
		Route    string          `json:"route"`
		Revision int64           `json:"revision"`
		Document json.RawMessage `json:"document"`
	} `json:"model_route,omitempty"`
	Skills []struct {
		SkillID  string          `json:"skill_id"`
		Revision int64           `json:"revision"`
		Digest   string          `json:"digest"`
		Bundle   json.RawMessage `json:"bundle"`
	} `json:"skills"`
	MaxSteps       int      `json:"max_steps"`
	AllowedTools   []string `json:"allowed_tools"`
	ComputerPolicy string   `json:"computer_policy"`
}
