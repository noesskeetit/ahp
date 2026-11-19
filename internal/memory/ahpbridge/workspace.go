package ahpbridge

import (
	"github.com/ahp-platform/ahp/pkg/contracts"
)

type Member struct {
	Subject string
	Role    string
}

func RequirePrivateWorkspace(subject string, members []Member) error {
	if len(members) != 1 || members[0].Subject != subject || members[0].Role != "owner" {
		return contracts.NewError(contracts.ErrForbidden, "project memory requires a Workspace with exactly one member who is its owner")
	}
	return nil
}
