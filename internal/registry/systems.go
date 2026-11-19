package registry

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// SystemMember pins one agent of a system to one immutable version.
type SystemMember struct {
	AgentID   string `json:"agent_id"`
	VersionID string `json:"version_id"`
}

// SystemRelease is a pinned set of member versions of one agent system. A
// release is what makes "the system was rolled out and can be rolled back as a
// whole" meaningful (PDF rev 3.2, c. 44: "Релиз системы закрепляет версии
// участников, выкладка и откат идут целиком").
type SystemRelease struct {
	ID        string         `json:"id"`
	SystemID  string         `json:"system_id"`
	Digest    string         `json:"digest"`
	Members   []SystemMember `json:"members"`
	CreatedAt time.Time      `json:"created_at"`
}

// SystemDeployment is one whole-system rollout fact: which release of which
// system is (or was) live in one tenant and environment.
type SystemDeployment struct {
	ID            string            `json:"id"`
	Tenant        string            `json:"tenant"`
	SystemID      string            `json:"system_id"`
	ReleaseID     string            `json:"release_id"`
	Environment   string            `json:"environment"`
	Installations map[string]string `json:"installations"`
	Action        string            `json:"action"`
	CreatedAt     time.Time         `json:"created_at"`
}

// RegisterSystem claims an agent system for a publisher. A system groups
// several deployments of ONE publisher (PDF rev 3.2, c. 44).
func (s *Store) RegisterSystem(ctx context.Context, publisherID, systemID string) error {
	if err := requireID("system id", systemID); err != nil {
		return err
	}
	if !publisherPattern.MatchString(publisherID) {
		return invalid("publisher id must be a lowercase identifier of at most 128 bytes")
	}
	tag, err := s.pool.Exec(ctx, `
INSERT INTO ahp_registry.agent_systems (system_id, publisher_id) VALUES ($1, $2)
ON CONFLICT DO NOTHING`, systemID, publisherID)
	if err != nil {
		return storageError("register system", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var owner string
	if err := s.pool.QueryRow(ctx,
		`SELECT publisher_id FROM ahp_registry.agent_systems WHERE system_id = $1`, systemID).Scan(&owner); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.NewError(contracts.ErrNotFound, "system not found")
		}
		return storageError("read system", err)
	}
	if owner != publisherID {
		return contracts.NewError(contracts.ErrConflict, "system belongs to another publisher")
	}
	return nil
}

// GetSystem loads the publisher of one agent system.
func (s *Store) GetSystem(ctx context.Context, systemID string) (string, error) {
	if err := requireID("system id", systemID); err != nil {
		return "", err
	}
	var owner string
	err := s.pool.QueryRow(ctx, `SELECT publisher_id FROM ahp_registry.agent_systems WHERE system_id = $1`, systemID).Scan(&owner)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", contracts.NewError(contracts.ErrNotFound, "system not found")
		}
		return "", storageError("read system", err)
	}
	return owner, nil
}

// PublishSystemRelease pins member versions into an immutable release.
//
// Every member must be a version this publisher published whose manifest
// declares this very system, so a release cannot smuggle in a foreign or
// mislabelled agent. Repeating the same member set is idempotent.
func (s *Store) PublishSystemRelease(ctx context.Context, publisherID, systemID string, members []SystemMember) (SystemRelease, error) {
	if err := requireID("system id", systemID); err != nil {
		return SystemRelease{}, err
	}
	if len(members) == 0 {
		return SystemRelease{}, invalid("a system release needs at least one member")
	}
	if len(members) > 64 {
		return SystemRelease{}, invalid("a system release may not exceed 64 members")
	}
	owner, err := s.GetSystem(ctx, systemID)
	if err != nil {
		return SystemRelease{}, err
	}
	if owner != publisherID {
		return SystemRelease{}, contracts.NewError(contracts.ErrNotFound, "system is not published by this publisher")
	}
	normalized := append([]SystemMember(nil), members...)
	sort.Slice(normalized, func(i, j int) bool { return normalized[i].AgentID < normalized[j].AgentID })
	seen := map[string]bool{}
	for _, m := range normalized {
		if err := requireID("agent id", m.AgentID); err != nil {
			return SystemRelease{}, err
		}
		if err := requireID("version id", m.VersionID); err != nil {
			return SystemRelease{}, err
		}
		if seen[m.AgentID] {
			return SystemRelease{}, invalid("an agent may appear in a release only once")
		}
		seen[m.AgentID] = true
	}
	digest, err := contracts.Digest(map[string]any{"system": systemID, "members": normalized})
	if err != nil {
		return SystemRelease{}, invalid("release members cannot be canonicalised")
	}
	doc, err := json.Marshal(normalized)
	if err != nil {
		return SystemRelease{}, invalid("release members cannot be encoded")
	}

	var out SystemRelease
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		// Validate members under the same lock the release insert takes, so a
		// version cannot be re-pointed between check and pin.
		for _, m := range normalized {
			version, verr := lockVersion(ctx, tx, m.VersionID)
			if verr != nil {
				return verr
			}
			if version.AgentID != m.AgentID {
				return contracts.NewError(contracts.ErrConflict,
					"version "+m.VersionID+" belongs to a different agent")
			}
			publisher, perr := s.publisherOf(ctx, tx, m.AgentID)
			if perr != nil {
				return perr
			}
			if publisher != publisherID {
				return contracts.NewError(contracts.ErrForbidden,
					"member "+m.AgentID+" belongs to another publisher")
			}
			if version.Manifest.System != systemID {
				return contracts.NewError(contracts.ErrConflict,
					"member "+m.AgentID+" declares a different system")
			}
		}
		var id string
		var created time.Time
		err := tx.QueryRow(ctx, `
SELECT id, created_at FROM ahp_registry.system_releases WHERE system_id = $1 AND digest = $2`,
			systemID, digest).Scan(&id, &created)
		if err == nil {
			out = SystemRelease{ID: id, SystemID: systemID, Digest: digest,
				Members: normalized, CreatedAt: created.UTC()}
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return storageError("read system release", err)
		}
		id = contracts.NewID()
		if _, err := tx.Exec(ctx, `
INSERT INTO ahp_registry.system_releases (id, system_id, digest, members) VALUES ($1, $2, $3, $4::jsonb)`,
			id, systemID, digest, doc); err != nil {
			if isUniqueViolation(err) {
				return contracts.NewError(contracts.ErrConflict, "release already exists for this member set")
			}
			return storageError("store system release", err)
		}
		out = SystemRelease{ID: id, SystemID: systemID, Digest: digest, Members: normalized}
		return nil
	})
	if err != nil {
		return SystemRelease{}, err
	}
	return out, nil
}

// GetSystemRelease loads one release.
func (s *Store) GetSystemRelease(ctx context.Context, releaseID string) (SystemRelease, error) {
	if err := requireID("release id", releaseID); err != nil {
		return SystemRelease{}, err
	}
	var (
		rel SystemRelease
		doc []byte
		ts  time.Time
	)
	err := s.pool.QueryRow(ctx, `
SELECT id, system_id, digest, members, created_at FROM ahp_registry.system_releases WHERE id = $1`, releaseID).
		Scan(&rel.ID, &rel.SystemID, &rel.Digest, &doc, &ts)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return SystemRelease{}, contracts.NewError(contracts.ErrNotFound, "system release not found")
		}
		return SystemRelease{}, storageError("read system release", err)
	}
	if err := json.Unmarshal(doc, &rel.Members); err != nil {
		return SystemRelease{}, contracts.NewError(contracts.ErrConflict, "stored release is unreadable")
	}
	rel.CreatedAt = ts.UTC()
	return rel, nil
}

// ListSystemReleases lists releases of one system, newest first.
func (s *Store) ListSystemReleases(ctx context.Context, systemID string, limit int) ([]SystemRelease, error) {
	if err := requireID("system id", systemID); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `
SELECT id FROM ahp_registry.system_releases WHERE system_id = $1 ORDER BY created_at DESC, id LIMIT $2`,
		systemID, limit)
	if err != nil {
		return nil, storageError("list system releases", err)
	}
	ids := make([]string, 0, 8)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, storageError("list system releases scan", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, storageError("list system releases rows", err)
	}
	out := make([]SystemRelease, 0, len(ids))
	for _, id := range ids {
		rel, err := s.GetSystemRelease(ctx, id)
		if err != nil {
			return nil, err
		}
		out = append(out, rel)
	}
	return out, nil
}

// DeploySystem rolls one release out into a tenant and environment as a single
// transaction: every member is installed or promoted, or nothing is (PDF rev
// 3.2, c. 44: "выкладка и откат идут целиком"). An installation that is already
// at the pinned version is left untouched.
func (s *Store) DeploySystem(ctx context.Context, tenant, systemID, releaseID, environment string) (SystemDeployment, error) {
	return s.rollSystem(ctx, tenant, systemID, releaseID, environment, "deploy")
}

// RollbackSystem returns a tenant's environment to the release that was live
// before the current one, again as one transaction across all members.
func (s *Store) RollbackSystem(ctx context.Context, tenant, systemID, environment string) (SystemDeployment, error) {
	if err := requireID("tenant", tenant); err != nil {
		return SystemDeployment{}, err
	}
	if err := requireID("system id", systemID); err != nil {
		return SystemDeployment{}, err
	}
	if err := requireEnvironment(environment); err != nil {
		return SystemDeployment{}, err
	}
	var out SystemDeployment
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if err := checkTenantWritable(ctx, tx, tenant); err != nil {
			return err
		}
		var currentRelease, previousRelease string
		rows, err := tx.Query(ctx, `
SELECT release_id FROM ahp_registry.system_deployments
WHERE tenant = $1 AND system_id = $2 AND environment = $3
ORDER BY created_at DESC, id DESC LIMIT 2`, tenant, systemID, environment)
		if err != nil {
			return storageError("read system rollout history", err)
		}
		found := 0
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return storageError("read system rollout history scan", err)
			}
			found++
			if found == 1 {
				currentRelease = id
			} else {
				previousRelease = id
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return storageError("read system rollout history rows", err)
		}
		if previousRelease == "" {
			return contracts.NewError(contracts.ErrNotFound,
				"no previous system release to roll back to (current release: "+currentRelease+")")
		}
		_ = currentRelease
		rel, err := releaseInTx(ctx, tx, previousRelease)
		if err != nil {
			return err
		}
		installed, err := s.applyRelease(ctx, tx, tenant, rel, environment, "rollback")
		if err != nil {
			return err
		}
		out, err = recordSystemDeployment(ctx, tx, tenant, systemID, previousRelease, environment, installed, "rollback")
		return err
	})
	if err != nil {
		return SystemDeployment{}, err
	}
	return out, nil
}

func (s *Store) rollSystem(ctx context.Context, tenant, systemID, releaseID, environment, action string) (SystemDeployment, error) {
	if err := requireID("tenant", tenant); err != nil {
		return SystemDeployment{}, err
	}
	if err := requireID("system id", systemID); err != nil {
		return SystemDeployment{}, err
	}
	if err := requireID("release id", releaseID); err != nil {
		return SystemDeployment{}, err
	}
	if err := requireEnvironment(environment); err != nil {
		return SystemDeployment{}, err
	}
	var out SystemDeployment
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if err := checkTenantWritable(ctx, tx, tenant); err != nil {
			return err
		}
		rel, err := releaseInTx(ctx, tx, releaseID)
		if err != nil {
			return err
		}
		if rel.SystemID != systemID {
			return contracts.NewError(contracts.ErrConflict, "release belongs to a different system")
		}
		installed, err := s.applyRelease(ctx, tx, tenant, rel, environment, action)
		if err != nil {
			return err
		}
		out, err = recordSystemDeployment(ctx, tx, tenant, systemID, releaseID, environment, installed, action)
		return err
	})
	if err != nil {
		return SystemDeployment{}, err
	}
	return out, nil
}

// applyRelease installs or promotes every member of a release inside the given
// transaction. It returns agentID -> installationID.
func (s *Store) applyRelease(ctx context.Context, tx pgx.Tx, tenant string, rel SystemRelease, environment, action string) (map[string]string, error) {
	installed := map[string]string{}
	for _, m := range rel.Members {
		version, err := lockVersion(ctx, tx, m.VersionID)
		if err != nil {
			return nil, err
		}
		if err := requireInstallationAllowed(version.Manifest, tenant); err != nil {
			return nil, err
		}
		// A whole-system deploy promotes every member at once, so it carries the
		// same evaluation precondition as a single promotion: the member version
		// must be fresh under the current eval policy. Rollback -- moving back to
		// what already ran -- keeps the accepted escape-hatch semantics and is
		// not re-gated.
		if action != "rollback" {
			if err := s.requireEvalVerdict(ctx, version.Digest); err != nil {
				return nil, err
			}
		}
		var instID, currentVersion, grantedDoc string
		var granted Capabilities
		err = tx.QueryRow(ctx, `
SELECT id, version_id, capabilities::text FROM ahp_registry.installations
WHERE tenant = $1 AND agent_id = $2 AND environment = $3`, tenant, m.AgentID, environment).
			Scan(&instID, &currentVersion, &grantedDoc)
		if err == nil {
			if uerr := json.Unmarshal([]byte(grantedDoc), &granted); uerr != nil {
				return nil, contracts.NewError(contracts.ErrConflict, "stored capabilities are unreadable")
			}
		}
		switch {
		case err == nil:
			if currentVersion == m.VersionID {
				installed[m.AgentID] = instID
				continue
			}
			if err := requireGrantedSubset(version.Manifest.Capabilities, granted); err != nil {
				return nil, contracts.NewError(contracts.ErrConflict,
					"granted capabilities exceed the "+action+" target ceiling for "+m.AgentID+": "+err.Error())
			}
			if _, err := tx.Exec(ctx, `
UPDATE ahp_registry.installations SET version_id = $1, updated_at = now() WHERE tenant = $2 AND id = $3`,
				m.VersionID, tenant, instID); err != nil {
				return nil, storageError("apply release promotion", err)
			}
			if err := recordDeployment(ctx, tx, tenant, instID, m.AgentID, environment, version, actionToDeployment(action), rel.ID); err != nil {
				return nil, err
			}
			installed[m.AgentID] = instID
		case errors.Is(err, pgx.ErrNoRows):
			granted = version.Manifest.Capabilities
			doc, merr := json.Marshal(granted)
			if merr != nil {
				return nil, invalid("capabilities cannot be encoded")
			}
			instID = contracts.NewID()
			if _, err := tx.Exec(ctx, `
INSERT INTO ahp_registry.installations
    (id, tenant, agent_id, deployment, environment, version_id, capabilities, published, idem_key, request_digest)
VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, false, $8, $9)`,
				instID, tenant, m.AgentID, m.AgentID, environment, m.VersionID, doc,
				"system:"+rel.ID+":"+m.AgentID, rel.Digest); err != nil {
				if isUniqueViolation(err) {
					return nil, contracts.NewError(contracts.ErrConflict,
						"installation slot for "+m.AgentID+" is taken")
				}
				return nil, storageError("apply release install", err)
			}
			if err := recordDeployment(ctx, tx, tenant, instID, m.AgentID, environment, version, actionToDeployment(action), rel.ID); err != nil {
				return nil, err
			}
			installed[m.AgentID] = instID
		default:
			return nil, storageError("read installation slot", err)
		}
	}
	return installed, nil
}

func actionToDeployment(action string) string {
	if action == "rollback" {
		return "rollback"
	}
	return "promote"
}

func releaseInTx(ctx context.Context, tx pgx.Tx, releaseID string) (SystemRelease, error) {
	var (
		rel SystemRelease
		doc []byte
		ts  time.Time
	)
	err := tx.QueryRow(ctx, `
SELECT id, system_id, digest, members, created_at FROM ahp_registry.system_releases WHERE id = $1`, releaseID).
		Scan(&rel.ID, &rel.SystemID, &rel.Digest, &doc, &ts)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return SystemRelease{}, contracts.NewError(contracts.ErrNotFound, "system release not found")
		}
		return SystemRelease{}, storageError("read system release", err)
	}
	if err := json.Unmarshal(doc, &rel.Members); err != nil {
		return SystemRelease{}, contracts.NewError(contracts.ErrConflict, "stored release is unreadable")
	}
	rel.CreatedAt = ts.UTC()
	return rel, nil
}

func recordSystemDeployment(ctx context.Context, tx pgx.Tx, tenant, systemID, releaseID, environment string, installed map[string]string, action string) (SystemDeployment, error) {
	doc, err := json.Marshal(installed)
	if err != nil {
		return SystemDeployment{}, invalid("installation map cannot be encoded")
	}
	out := SystemDeployment{ID: contracts.NewID(), Tenant: tenant, SystemID: systemID,
		ReleaseID: releaseID, Environment: environment, Installations: installed, Action: action}
	if _, err := tx.Exec(ctx, `
INSERT INTO ahp_registry.system_deployments (id, tenant, system_id, release_id, environment, installations, action)
VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)`,
		out.ID, tenant, systemID, releaseID, environment, doc, action); err != nil {
		return SystemDeployment{}, storageError("record system deployment", err)
	}
	return out, nil
}

// CurrentSystemDeployment reports which release of a system is live in one
// tenant and environment, with the installations it pinned.
func (s *Store) CurrentSystemDeployment(ctx context.Context, tenant, systemID, environment string) (SystemDeployment, error) {
	if err := requireID("tenant", tenant); err != nil {
		return SystemDeployment{}, err
	}
	if err := requireID("system id", systemID); err != nil {
		return SystemDeployment{}, err
	}
	var (
		out SystemDeployment
		doc []byte
		ts  time.Time
	)
	err := s.pool.QueryRow(ctx, `
SELECT id, tenant, system_id, release_id, environment, installations, action, created_at
FROM ahp_registry.system_deployments
WHERE tenant = $1 AND system_id = $2 AND environment = $3
ORDER BY created_at DESC, id DESC LIMIT 1`, tenant, systemID, environment).
		Scan(&out.ID, &out.Tenant, &out.SystemID, &out.ReleaseID, &out.Environment, &doc, &out.Action, &ts)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return SystemDeployment{}, contracts.NewError(contracts.ErrNotFound, "system is not deployed here")
		}
		return SystemDeployment{}, storageError("read system deployment", err)
	}
	if err := json.Unmarshal(doc, &out.Installations); err != nil {
		return SystemDeployment{}, contracts.NewError(contracts.ErrConflict, "stored system deployment is unreadable")
	}
	out.CreatedAt = ts.UTC()
	return out, nil
}
