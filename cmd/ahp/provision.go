package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/memory"
)

// validateProvisionPermissions refuses a Memory capability that does not exist.
//
// DEF-MEM-6: the plane authorises "memory" at its face and "memory:review",
// "memory:label", "memory:delete", "memory:delete-subject" inside the store, but
// provisioning accepted ANY string. An operator who typed "memory:revieww" got a
// successful start, a useless grant, and a 403 much later that named neither the
// typo nor the missing capability. Refusing here changes nothing about who is
// allowed to do what -- it only makes a non-existent capability impossible to
// provision, which is what makes the published vocabulary trustworthy.
//
// The check is scoped to the Memory namespace on purpose. Capabilities of other
// planes are not Memory's to classify, and a global allowlist invented here
// would be a new, undocumented authority of its own.
func validateProvisionPermissions(perms []string) error {
	for _, p := range perms {
		if !strings.Contains(p, memory.CapabilityGateway) {
			continue // outside the Memory namespace: not ours to judge
		}
		if memory.IsCapability(p) {
			continue
		}
		return fmt.Errorf("provision: %q is not a Memory capability; AHP_PROVISION_USERS_JSON accepts exactly these: %s",
			p, strings.Join(memory.Vocabulary, ", "))
	}
	return nil
}

// provisionUser is the wire shape for the browser-user provisioning role. It is
// supplied ONLY through the process environment (AHP_PROVISION_USERS_JSON) so a
// demo password never lands in argv, a committed file or the logs.
type provisionUser struct {
	ID          string   `json:"id"`
	Tenant      string   `json:"tenant"`
	Subject     string   `json:"subject"`
	DisplayName string   `json:"display_name"`
	Email       string   `json:"email"`
	Username    string   `json:"username"`
	Password    string   `json:"password"`
	Roles       []string `json:"roles"`
	Permissions []string `json:"permissions"`
}

// provisionBrowserUsers seeds the browser human accounts the isolated demo logs
// in as. It is a one-shot administrative role, never a served surface, so it
// cannot be reached over HTTP and cannot self-register (matching the browser
// login design).
func (s *server) provisionBrowserUsers(ctx context.Context) error {
	raw := envOr("AHP_PROVISION_USERS_JSON", "")
	if raw == "" {
		return fmt.Errorf("provision: AHP_PROVISION_USERS_JSON is empty")
	}
	var users []provisionUser
	if err := json.Unmarshal([]byte(raw), &users); err != nil {
		return fmt.Errorf("provision: decode users: %w", err)
	}
	if len(users) == 0 {
		return fmt.Errorf("provision: no users supplied")
	}
	store := identity.New(s.pool)
	if err := store.Migrate(ctx); err != nil {
		return fmt.Errorf("provision: identity migrate: %w", err)
	}
	for _, u := range users {
		if err := validateProvisionPermissions(u.Permissions); err != nil {
			return fmt.Errorf("provision: user %q: %w", u.Username, err)
		}
		bu := identity.BrowserUser{
			UserID:      u.ID,
			Tenant:      u.Tenant,
			Subject:     u.Subject,
			DisplayName: u.DisplayName,
			Email:       u.Email,
			Roles:       u.Roles,
			Permissions: u.Permissions,
		}
		if err := store.SeedBrowserUser(ctx, bu, u.Username, u.Password, u.Roles, u.Permissions); err != nil {
			return fmt.Errorf("provision: seed user %q: %w", u.Username, err)
		}
		// Never log the password.
		s.logger.Info("provision: seeded browser user", "username", u.Username, "tenant", u.Tenant, "subject", u.Subject)
	}
	s.logger.Info("provision: complete", "count", len(users))
	return nil
}
