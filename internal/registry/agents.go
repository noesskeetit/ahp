package registry

import (
	"context"
	"errors"
	"regexp"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// agentPattern bounds an agent identifier the same way a manifest system does.
var agentPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._:@-]{0,127}$`)

// publisherPattern bounds a publisher identifier (a team or an external
// developer identity, never a secret).
var publisherPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._:@-]{0,127}$`)

// Agent is the publisher-facing record of one artifact: who published it.
// Nothing in this struct can reveal a consuming tenant, an installation, or
// any state inside an installation.
type Agent struct {
	AgentID     string `json:"agent_id"`
	PublisherID string `json:"publisher_id"`
}

// RegisterAgent binds an agent to its publisher. An agent and every version of
// it belong to one publisher (PDF rev 3.2, c. 42 "Установка. Агент и его версии
// принадлежат издателю -- команде или внешнему разработчику"). Repeating the
// call with the same publisher is idempotent; a different publisher claiming an
// existing agent is a conflict, never a takeover.
func (s *Store) RegisterAgent(ctx context.Context, publisherID, agentID string) (Agent, error) {
	if err := requireID("agent id", agentID); err != nil {
		return Agent{}, err
	}
	if !publisherPattern.MatchString(publisherID) {
		return Agent{}, invalid("publisher id must be a lowercase identifier of at most 128 bytes")
	}
	tag, err := s.pool.Exec(ctx, `
INSERT INTO ahp_registry.agents (agent_id, publisher_id) VALUES ($1, $2)
ON CONFLICT DO NOTHING`, agentID, publisherID)
	if err != nil {
		return Agent{}, storageError("register agent", err)
	}
	if tag.RowsAffected() == 1 {
		return Agent{AgentID: agentID, PublisherID: publisherID}, nil
	}
	existing, err := s.GetAgent(ctx, agentID)
	if err != nil {
		return Agent{}, err
	}
	if existing.PublisherID != publisherID {
		return Agent{}, contracts.NewError(contracts.ErrConflict,
			"agent belongs to another publisher")
	}
	return existing, nil
}

// GetAgent loads the publisher record of one agent.
func (s *Store) GetAgent(ctx context.Context, agentID string) (Agent, error) {
	if err := requireID("agent id", agentID); err != nil {
		return Agent{}, err
	}
	var a Agent
	err := s.pool.QueryRow(ctx,
		`SELECT agent_id, publisher_id FROM ahp_registry.agents WHERE agent_id = $1`, agentID).
		Scan(&a.AgentID, &a.PublisherID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Agent{}, contracts.NewError(contracts.ErrNotFound, "agent is not registered to a publisher")
		}
		return Agent{}, storageError("read agent", err)
	}
	return a, nil
}

// publisherOf resolves the publisher of an agent, or NOT_FOUND when the agent
// has never been claimed. PutVersion requires this: publishing is what a
// publisher does, and an unclaimed agent has nobody entitled to publish it.
func (s *Store) publisherOf(ctx context.Context, tx pgx.Tx, agentID string) (string, error) {
	var publisher string
	q := `SELECT publisher_id FROM ahp_registry.agents WHERE agent_id = $1`
	var row pgx.Row
	if tx == nil {
		row = s.pool.QueryRow(ctx, q, agentID)
	} else {
		row = tx.QueryRow(ctx, q, agentID)
	}
	if err := row.Scan(&publisher); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", contracts.NewError(contracts.ErrNotFound,
				"agent is not registered to a publisher: register it before publishing a version")
		}
		return "", storageError("read agent publisher", err)
	}
	return publisher, nil
}

// ListVersions lists the versions one publisher published for one agent, newest
// first. A publisher that does not own the agent sees nothing (NOT_FOUND), so
// version lists are not a way to enumerate other publishers' artifacts.
func (s *Store) ListVersions(ctx context.Context, publisherID, agentID string, limit int) ([]Version, error) {
	if err := requireID("agent id", agentID); err != nil {
		return nil, err
	}
	owner, err := s.GetAgent(ctx, agentID)
	if err != nil {
		return nil, err
	}
	if owner.PublisherID != publisherID {
		return nil, contracts.NewError(contracts.ErrNotFound, "agent is not published by this publisher")
	}
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `
SELECT id, agent_id, digest, manifest, created_at FROM ahp_registry.versions
WHERE agent_id = $1 ORDER BY created_at DESC, id LIMIT $2`, agentID, limit)
	if err != nil {
		return nil, storageError("list versions", err)
	}
	defer rows.Close()
	out := make([]Version, 0, 8)
	for rows.Next() {
		v, serr := scanVersion(rows)
		if serr != nil {
			return nil, serr
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// PublisherAggregates is the ONLY thing a publisher may learn about the
// installations of its agent: counts, and only from tenants that opted in
// (PDF rev 3.2, c. 42: "Издатель не видит состояние и содержимое установки --
// только агрегаты и оценки, которые разрешил потребитель"). No tenant id, no
// deployment, no capabilities, no configuration, no task ids.
type PublisherAggregates struct {
	AgentID          string         `json:"agent_id"`
	Installations    int            `json:"installations"`
	Environments     map[string]int `json:"environments"`
	PinnedTasks      int            `json:"pinned_tasks"`
	DisclosingTenant int            `json:"-"`
}

// SetPublisherDisclosure is the consumer tenant's opt-in to sharing anonymous
// aggregates of one installation with the publisher. It is a consumer decision
// by construction: the row is addressed by tenant, so a publisher cannot
// disclose their own installation view (PDF rev 3.2, c. 42, same clause).
func (s *Store) SetPublisherDisclosure(ctx context.Context, tenant, installationID string, allow bool) error {
	if err := requireID("tenant", tenant); err != nil {
		return err
	}
	if err := requireID("installation id", installationID); err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `
UPDATE ahp_registry.installations SET publisher_disclosure = $1, updated_at = now()
WHERE tenant = $2 AND id = $3`, allow, tenant, installationID)
	if err != nil {
		return storageError("set publisher disclosure", err)
	}
	if tag.RowsAffected() == 0 {
		return contracts.NewError(contracts.ErrNotFound, "installation not found")
	}
	return nil
}

// PublisherAggregates returns the disclosed-only aggregate view for a publisher.
func (s *Store) PublisherAggregates(ctx context.Context, publisherID, agentID string) (PublisherAggregates, error) {
	owner, err := s.GetAgent(ctx, agentID)
	if err != nil {
		return PublisherAggregates{}, err
	}
	if owner.PublisherID != publisherID {
		return PublisherAggregates{}, contracts.NewError(contracts.ErrNotFound, "agent is not published by this publisher")
	}
	out := PublisherAggregates{AgentID: agentID, Environments: map[string]int{}}
	rows, err := s.pool.Query(ctx, `
SELECT i.environment, count(*) FILTER (WHERE i.publisher_disclosure), count(p.task_id)
FROM ahp_registry.installations i
LEFT JOIN ahp_registry.task_pins p ON p.installation_id = i.id AND i.publisher_disclosure
WHERE i.agent_id = $1
GROUP BY i.environment`, agentID)
	if err != nil {
		return PublisherAggregates{}, storageError("publisher aggregates", err)
	}
	defer rows.Close()
	disclosed := 0
	for rows.Next() {
		var env string
		var installs, pins int
		if err := rows.Scan(&env, &installs, &pins); err != nil {
			return PublisherAggregates{}, storageError("publisher aggregates scan", err)
		}
		if installs > 0 {
			out.Environments[env] = installs
			disclosed += installs
			out.PinnedTasks += pins
		}
	}
	if err := rows.Err(); err != nil {
		return PublisherAggregates{}, storageError("publisher aggregates rows", err)
	}
	out.Installations = disclosed
	return out, nil
}

// SetPublisherDisclosureForAgent opts every installation of one agent in or out
// at once, for tenants that prefer a single switch. Scoped to the caller tenant.
func (s *Store) SetPublisherDisclosureForAgent(ctx context.Context, tenant, agentID string, allow bool) (int, error) {
	if err := requireID("tenant", tenant); err != nil {
		return 0, err
	}
	if err := requireID("agent id", agentID); err != nil {
		return 0, err
	}
	tag, err := s.pool.Exec(ctx, `
UPDATE ahp_registry.installations SET publisher_disclosure = $1, updated_at = now()
WHERE tenant = $2 AND agent_id = $3`, allow, tenant, agentID)
	if err != nil {
		return 0, storageError("set publisher disclosure", err)
	}
	return int(tag.RowsAffected()), nil
}
