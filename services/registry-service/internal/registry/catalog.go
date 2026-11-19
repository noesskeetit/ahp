package registry

import (
	"context"
	"encoding/json"
	"regexp"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// rolePattern bounds one catalog role name.
var rolePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,63}$`)

// CatalogEntry is one employee-facing catalog card.
//
// The card must show whether the agent keeps data between users (PDF rev 3.2,
// c. 42 "Карточка агента показывает, хранит ли он данные между пользователями";
// shared spaces are declared in the manifest and are a review item), and the
// card exists only because an administrator published the agent, with a role
// audience (c. 42: "Публикация агента для сотрудников -- отдельное решение
// администратора tenant с видимостью по ролям").
type CatalogEntry struct {
	Installation string   `json:"installation_id"`
	AgentID      string   `json:"agent_id"`
	System       string   `json:"system"`
	Kind         string   `json:"kind"`
	Environment  string   `json:"environment"`
	VersionID    string   `json:"version_id"`
	Digest       string   `json:"digest"`
	Audience     string   `json:"audience"`
	Visibility   string   `json:"visibility"`
	SharesState  bool     `json:"shares_between_users"`
	Roles        []string `json:"roles"`
	Published    bool     `json:"published"`
}

// PublishWithRoles publishes an installation into the tenant catalog with the
// role audience employees must hold to see it. Publishing is a tenant
// administrator's decision, separate from installing (PDF rev 3.2, c. 42).
// Only a prod installation can be published: the schema rejects it otherwise.
func (s *Store) PublishWithRoles(ctx context.Context, tenant, installationID string, roles []string) error {
	if err := requireID("tenant", tenant); err != nil {
		return err
	}
	if err := requireID("installation id", installationID); err != nil {
		return err
	}
	if len(roles) == 0 || len(roles) > 64 {
		return contracts.NewError(contracts.ErrInvalid, "a catalog audience needs 1..64 roles")
	}
	seen := map[string]bool{}
	for _, role := range roles {
		if !rolePattern.MatchString(role) {
			return contracts.NewError(contracts.ErrInvalid, "catalog role must be a short identifier: "+role)
		}
		if seen[role] {
			return contracts.NewError(contracts.ErrInvalid, "duplicate catalog role: "+role)
		}
		seen[role] = true
	}
	doc, err := json.Marshal(roles)
	if err != nil {
		return contracts.NewError(contracts.ErrInvalid, "catalog roles cannot be encoded")
	}
	rolesDoc := json.RawMessage(doc)
	return s.setCatalogPublication(ctx, tenant, installationID, true, &rolesDoc)
}

// CatalogCards lists the catalog cards of one tenant. callerRoles filters the
// cards for an employee: a card with an empty audience is visible to every
// employee, otherwise the employee must hold one of its roles. An EMPTY
// callerRoles list is the administrator's view (every published card), which is
// how the tenant admin reviews the catalog. Other tenants' rows never appear.
func (s *Store) CatalogCards(ctx context.Context, tenant string, callerRoles []string) ([]CatalogEntry, error) {
	if err := requireID("tenant", tenant); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
SELECT i.id, i.agent_id, i.environment, i.version_id, i.published, i.audience_roles::text,
       v.digest, v.manifest::text
FROM ahp_registry.installations i JOIN ahp_registry.versions v ON v.id = i.version_id
WHERE i.published AND i.tenant = $1
ORDER BY i.agent_id, i.environment, i.id`, tenant)
	if err != nil {
		return nil, storageError("read catalog", err)
	}
	defer rows.Close()

	wanted := map[string]bool{}
	for _, r := range callerRoles {
		wanted[r] = true
	}
	adminView := len(callerRoles) == 0

	out := make([]CatalogEntry, 0, 8)
	for rows.Next() {
		var id, agentID, environment, versionID, digest, rolesDoc, manifestDoc string
		var published bool
		if err := rows.Scan(&id, &agentID, &environment, &versionID, &published, &rolesDoc, &digest, &manifestDoc); err != nil {
			return nil, storageError("scan catalog", err)
		}
		var manifest Manifest
		if err := json.Unmarshal([]byte(manifestDoc), &manifest); err != nil {
			return nil, contracts.NewError(contracts.ErrConflict, "stored manifest is unreadable")
		}
		var roles []string
		if rolesDoc != "" && rolesDoc != "null" {
			if err := json.Unmarshal([]byte(rolesDoc), &roles); err != nil {
				return nil, contracts.NewError(contracts.ErrConflict, "stored catalog roles are unreadable")
			}
		}
		if !adminView && len(roles) > 0 {
			visible := false
			for _, r := range roles {
				if wanted[r] {
					visible = true
					break
				}
			}
			if !visible {
				continue
			}
		}
		out = append(out, CatalogEntry{
			Installation: id,
			AgentID:      agentID,
			System:       manifest.System,
			Kind:         manifest.Kind,
			Environment:  environment,
			VersionID:    versionID,
			Digest:       digest,
			Audience:     manifest.Audience,
			Visibility:   manifest.Visibility,
			SharesState:  len(manifest.State.Shared) > 0,
			Roles:        roles,
			Published:    published,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, storageError("read catalog rows", err)
	}
	return out, nil
}

func (s *Store) setCatalogPublication(ctx context.Context, tenant, installationID string, visible bool, roles *json.RawMessage) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if err := checkTenantWritable(ctx, tx, tenant); err != nil {
			return err
		}
		inst, err := lockInstallation(ctx, tx, tenant, installationID)
		if err != nil {
			return err
		}
		if visible {
			// Say it with a typed error before the schema's
			// CHECK (NOT published OR environment = 'prod') fires: the catalog is
			// the plane employees reach, and a dev/stage installation must never
			// answer for a real tenant. The constraint stays as the backstop.
			if inst.Environment != EnvironmentProd {
				return contracts.NewError(contracts.ErrConflict,
					"only a prod installation may serve tenant users; "+inst.Environment+" stays out of the catalog")
			}
			version, err := getVersion(ctx, tx, inst.VersionID)
			if err != nil {
				return err
			}
			if err := s.requireEvalVerdict(ctx, version.Digest); err != nil {
				return err
			}
		}
		if roles == nil {
			_, err = tx.Exec(ctx, `UPDATE ahp_registry.installations SET published=$1,updated_at=now() WHERE tenant=$2 AND id=$3`, visible, tenant, installationID)
		} else {
			_, err = tx.Exec(ctx, `UPDATE ahp_registry.installations SET published=$1,audience_roles=$2::jsonb,updated_at=now() WHERE tenant=$3 AND id=$4`, visible, *roles, tenant, installationID)
		}
		if err != nil {
			return storageError("publish installation", err)
		}
		return nil
	})
}
