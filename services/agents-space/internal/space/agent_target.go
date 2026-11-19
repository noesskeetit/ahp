package space

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// AgentTargetRef is server-owned routing provenance, not the agent's state or
// credentials. Registry remains the owner of the full pinned config and runtime.
type AgentTargetRef struct {
	InstallationID string `json:"installation_id"`
	AgentID        string `json:"agent_id"`
	VersionID      string `json:"version_id"`
	Digest         string `json:"digest"`
	Environment    string `json:"environment,omitempty"`
	ConfigRevision int64  `json:"config_revision"`
	PinID          string `json:"pin_id"`
	StateCompat    string `json:"state_compat"`
	StatePartition string `json:"state_partition"`
}

type AgentTargetPinner interface {
	PinAgentTurn(context.Context, string, string) (*AgentTargetRef, error)
}

func NewAgentTargetClient(origin string, client *http.Client) (AgentTargetPinner, error) {
	owner, err := newRegistryOwnerClient(origin, client)
	if err != nil {
		return nil, err
	}
	return owner, nil
}

func (p *profileClient) PinAgentTurn(ctx context.Context, token, installation string) (*AgentTargetRef, error) {
	if strings.TrimSpace(token) == "" || strings.TrimSpace(installation) == "" {
		return nil, contracts.NewError(contracts.ErrInvalid, "agent target requires a run token and installation")
	}
	data, _ := json.Marshal(map[string]string{"installation_id": installation})
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, p.origin+"/v1/registry/PinAgentTurn", bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	setServiceRequest(r, token, "application/json")
	response, err := p.client.Do(r)
	if err != nil {
		return nil, contracts.NewError(contracts.ErrUnavailable, "agent target owner API unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, profileRefusal(response)
	}
	data, err = io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil || len(data) > 1<<20 {
		return nil, contracts.NewError(contracts.ErrUnavailable, "agent target owner response exceeded its bound")
	}
	var out struct {
		Target *struct {
			AgentTargetRef
			Task     string `json:"task"`
			Manifest struct {
				State struct {
					Compat    string `json:"compat"`
					Partition string `json:"partition"`
				} `json:"state"`
			} `json:"manifest"`
		} `json:"target"`
	}
	if json.Unmarshal(data, &out) != nil || out.Target == nil {
		return nil, contracts.NewError(contracts.ErrUnavailable, "agent target owner returned an invalid snapshot")
	}
	target := out.Target.AgentTargetRef
	target.PinID = out.Target.Task
	target.StateCompat = out.Target.Manifest.State.Compat
	target.StatePartition = out.Target.Manifest.State.Partition
	digest, decodeErr := hex.DecodeString(strings.TrimPrefix(target.Digest, "sha256:"))
	if target.InstallationID != installation || target.AgentID == "" || target.VersionID == "" || target.PinID == "" || !strings.HasPrefix(target.Digest, "sha256:") || decodeErr != nil || len(digest) != 32 || target.StateCompat == "" || target.StatePartition == "" {
		return nil, contracts.NewError(contracts.ErrUnavailable, "agent target owner returned an incomplete or mismatched snapshot")
	}
	return &target, nil
}

// The installation belongs to the conversation; the version/config pin belongs
// to an individual turn. Omitting the installation continues the same executor.
func conversationExecutorBinding(runs []runResponse, requested string) (string, error) {
	installation := ""
	found := false
	for _, run := range runs {
		var p turnRunPayload
		if json.Unmarshal(run.Payload, &p) != nil {
			return "", contracts.NewError(contracts.ErrConflict, "space: conversation contains an invalid executor payload")
		}
		if p.Type != "turn" {
			continue
		}
		if !found {
			installation = p.AgentInstallationID
			found = true
		} else if installation != p.AgentInstallationID {
			return "", contracts.NewError(contracts.ErrConflict, "space: conversation has inconsistent executors")
		}
	}
	if found {
		if requested != "" && requested != installation {
			return "", contracts.NewError(contracts.ErrConflict, "space: start a new conversation to change its agent")
		}
		return installation, nil
	}
	return requested, nil
}

func compatibleConversationAgentState(runs []runResponse, target *AgentTargetRef) error {
	for _, run := range runs {
		var p turnRunPayload
		if json.Unmarshal(run.Payload, &p) != nil {
			return contracts.NewError(contracts.ErrConflict, "space: unreadable conversation executor")
		}
		if p.Type != "turn" {
			continue
		}
		if p.AgentTarget == nil || p.AgentTarget.InstallationID != target.InstallationID || p.AgentTarget.StateCompat != target.StateCompat || p.AgentTarget.StatePartition != target.StatePartition {
			return contracts.NewError(contracts.ErrConflict, "space: selected agent version cannot resume this conversation state; start a new conversation or migrate it explicitly")
		}
	}
	return nil
}
