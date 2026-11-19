package space

import (
	"encoding/json"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func conversationProject(runs []runResponse, requested string) (string, error) {
	project, _, err := conversationMemoryBinding(runs, requested, "")
	return project, err
}

// Blank is the historical personal/private-project profile. Shared is always
// explicit and durable; adding members never silently changes the backend.
func normalizeProjectMemoryProfile(profile string) (string, error) {
	switch profile {
	case "", "native":
		return "", nil
	case "shared":
		return "shared", nil
	default:
		return "", contracts.NewError(contracts.ErrInvalid, "space: unknown project memory profile")
	}
}

func conversationMemoryBinding(runs []runResponse, requested, requestedProfile string) (string, string, error) {
	profile, err := normalizeProjectMemoryProfile(requestedProfile)
	if err != nil {
		return "", "", err
	}
	project := ""
	storedProfile := ""
	found := false
	for _, run := range runs {
		var p turnRunPayload
		if err := json.Unmarshal(run.Payload, &p); err != nil {
			return "", "", contracts.NewError(contracts.ErrConflict, "space: conversation contains an invalid run payload")
		}
		if p.Type != "turn" {
			continue
		}
		currentProfile, err := normalizeProjectMemoryProfile(p.ProjectMemoryProfile)
		if err != nil || currentProfile == "shared" && p.ProjectID == "" {
			return "", "", contracts.NewError(contracts.ErrConflict, "space: conversation contains an invalid memory profile")
		}
		if !found {
			project = p.ProjectID
			storedProfile = currentProfile
			found = true
		} else if project != p.ProjectID || storedProfile != currentProfile {
			return "", "", contracts.NewError(contracts.ErrConflict, "space: conversation has inconsistent project memory bindings")
		}
	}
	if found && requested != "" && requested != project {
		return "", "", contracts.NewError(contracts.ErrConflict, "space: start a new conversation to change its project")
	}
	if found && requestedProfile != "" && profile != storedProfile {
		return "", "", contracts.NewError(contracts.ErrConflict, "space: start a new conversation to change its memory profile")
	}
	if found {
		return project, storedProfile, nil
	}
	if profile == "shared" && requested == "" {
		return "", "", contracts.NewError(contracts.ErrInvalid, "space: shared memory requires a project")
	}
	return requested, profile, nil
}
