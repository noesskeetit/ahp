// Package sdk implements the AHP platform contract for custom agents.
//
// It provides the building blocks a containerised agent needs to integrate with
// AHP: A2A task lifecycle, lease management, LangGraph-compatible checkpointing,
// operation-key intent recording, and KV state — all routed through the State API.
//
// Wire security follows the State API contract: scope is derived from the mTLS
// certificate and bound task token, never from request bodies.
package sdk

import (
	"fmt"
	"time"

	sdkstate "github.com/ahp-platform/ahp/sdk/go/state"
)

const (
	// DefaultHeartbeatInterval is the recommended interval between heartbeats.
	// Must be significantly shorter than DefaultLeaseTTL.
	DefaultHeartbeatInterval = 5 * time.Second
	// DefaultLeaseTTL is the recommended lease duration.
	DefaultLeaseTTL = 30 * time.Second
	// DefaultMaxRetries is the maximum number of retries for transient errors.
	DefaultMaxRetries = 0
)

// Config configures an SDK instance.
type Config struct {
	// StateClient is a pre-configured State API client (carries mTLS + token source).
	StateClient *sdkstate.Client
	// Worker is the identity of this process/replica; used in lease operations.
	Worker string
	// LeaseTTL is the duration requested in ClaimRun. Defaults to DefaultLeaseTTL.
	LeaseTTL time.Duration
	// HeartbeatInterval is how often Heartbeat is sent. Defaults to DefaultHeartbeatInterval.
	HeartbeatInterval time.Duration
}

// Validate checks required fields and applies defaults.
func (c *Config) Validate() error {
	if c.StateClient == nil {
		return fmt.Errorf("sdk: Config.StateClient is required")
	}
	if c.Worker == "" {
		return fmt.Errorf("sdk: Config.Worker must not be empty")
	}
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = DefaultLeaseTTL
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if c.HeartbeatInterval >= c.LeaseTTL {
		return fmt.Errorf("sdk: HeartbeatInterval must be less than LeaseTTL")
	}
	return nil
}

// LeaseTTLMS returns LeaseTTL in milliseconds for wire use.
func (c *Config) LeaseTTLMS() int64 {
	return c.LeaseTTL.Milliseconds()
}
