package main

import (
	"fmt"
	"github.com/ahp-platform/ahp/internal/state"
)

// Native common storage is opt-in. Invalid modes fail before migration or
// listening; disabling a reader does not downgrade an already versioned DB.
func loadStateStoreOptions() ([]state.StoreOption, error) {
	options := stateStoreOptions()
	if path := envOr("AHP_STATE_KV_NAMESPACE_POLICIES_FILE", ""); path != "" {
		var policies []state.KVNamespacePolicy
		if e := readOperatorJSON(path, &policies); e != nil {
			return nil, fmt.Errorf("reviewed State namespace policy: %w", e)
		}
		option, e := state.WithKVNamespacePolicies(policies)
		if e != nil {
			return nil, e
		}
		options = append(options, option)
	}
	if path := envOr("AHP_STATE_COMMON_RETENTION_PROFILES_FILE", ""); path != "" {
		if !commonStateConfigured() {
			return nil, fmt.Errorf("common retention requires snapshot or interval mode")
		}
		var profiles []string
		if e := readOperatorJSON(path, &profiles); e != nil {
			return nil, fmt.Errorf("reviewed retention profiles: %w", e)
		}
		option, e := state.WithCommonRetention(profiles)
		if e != nil {
			return nil, e
		}
		options = append(options, option)
	}
	if appendMode := envOr("AHP_STATE_COMMON_APPEND", ""); appendMode != "" && appendMode != "off" {
		if appendMode != "on" || envOr("AHP_STATE_COMMON_MODE", "") != "interval" {
			return nil, fmt.Errorf("AHP_STATE_COMMON_APPEND=on requires interval mode")
		}
		options = append(options, state.WithCommonIncrementalAppend())
	}
	switch mode := envOr("AHP_STATE_COMMON_MODE", ""); mode {
	case "", "off":
		return options, nil
	case "snapshot":
		return append(options, state.WithCommonRecords(), state.WithCommonPayloadObjects(), state.WithCommonArtifactReferences()), nil
	case "interval":
		return append(options, state.WithCommonRecordVersions(), state.WithCommonArtifactReferences()), nil
	default:
		return nil, fmt.Errorf("AHP_STATE_COMMON_MODE must be off, snapshot or interval")
	}
}

func commonStateConfigured() bool {
	mode := envOr("AHP_STATE_COMMON_MODE", "")
	return mode == "snapshot" || mode == "interval"
}
