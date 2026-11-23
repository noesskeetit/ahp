package tools

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// LeaseProof is the current State-issued right to use a tool in a VM session.
type LeaseProof struct {
	SessionID string `json:"session_id"`
	Epoch     int64  `json:"lease_epoch"`
	Proof     string `json:"tool_lease_proof"`
}

func (l LeaseProof) validate() error {
	if l.SessionID == "" || l.Epoch <= 0 || l.Proof == "" {
		return invalid("current tool lease proof is required")
	}
	return nil
}

// CallWithLease makes one leased attempt. Recovery uses FenceAndLookup.
func (c *Client) CallWithLease(ctx context.Context, key, tool string, args json.RawMessage, lease LeaseProof) (Operation, error) {
	var out Operation
	if err := lease.validate(); err != nil {
		return out, err
	}
	if key == "" || tool == "" || !json.Valid(args) {
		return out, invalid("key, tool and JSON arguments are required")
	}
	err := c.wire.Call(ctx, "/v1/tools/Call", struct {
		LeaseProof
		Key  string          `json:"key"`
		Tool string          `json:"tool"`
		Args json.RawMessage `json:"arguments"`
	}{lease, key, tool, args}, &out, true)
	return out, err
}

const (
	LookupCompleted      = "COMPLETED"
	LookupInFlight       = "IN_FLIGHT"
	LookupSafeToRetry    = "SAFE_TO_RETRY"
	LookupOutcomeUnknown = "OUTCOME_UNKNOWN"
)

type FencedLookup struct {
	Status  string      `json:"status"`
	Receipt *ToolResult `json:"receipt,omitempty"`
}

// FenceAndLookup advances the Gateway's fence and asks for the operation's
// durable outcome. A missing route or ownership error must never permit retry.
func (c *Client) FenceAndLookup(ctx context.Context, key string, lease LeaseProof) (FencedLookup, error) {
	if err := lease.validate(); err != nil {
		return FencedLookup{}, err
	}
	if key == "" {
		return FencedLookup{}, invalid("key is required")
	}
	var out FencedLookup
	status, err := c.wire.CallWithStatus(ctx, "/v1/tools/FenceAndLookup", struct {
		LeaseProof
		Key string `json:"key"`
	}{lease, key}, &out, true)
	if err != nil {
		var api *Error
		if errors.As(err, &api) {
			switch {
			case api.Status == http.StatusNotFound && api.Code == LookupSafeToRetry:
				return FencedLookup{Status: LookupSafeToRetry}, nil
			case api.Status == http.StatusConflict && api.Code == contracts.ErrConflict && api.Reason == LookupOutcomeUnknown:
				return FencedLookup{Status: LookupOutcomeUnknown}, nil
			}
		}
		return FencedLookup{}, err
	}
	switch {
	case status == http.StatusOK && out.Status == LookupCompleted && out.Receipt != nil && out.Receipt.Status == "SUCCEEDED" && out.Receipt.Key == key:
		if err := out.Receipt.validate(); err != nil {
			return FencedLookup{}, err
		}
		return out, nil
	case status == http.StatusAccepted && out.Status == LookupInFlight && out.Receipt == nil:
		return out, nil
	default:
		return FencedLookup{}, unknown("invalid fenced lookup response")
	}
}
