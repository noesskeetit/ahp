package registry

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// Skill levels. A skill is available to a caller holding at least that level
// (PDF rev 3.2, c. 36: "Навык -- Ревизия бандла в Registry; уровни admin,
// workspace, user").
const (
	SkillLevelUser      = "user"
	SkillLevelWorkspace = "workspace"
	SkillLevelAdmin     = "admin"
)

// maxSkillBundleBytes bounds a stored bundle: a skill is a bundle of
// instructions and resources, not a file store.
const maxSkillBundleBytes = 1 << 20

// Skill is one immutable revision of a skill bundle.
type Skill struct {
	ID          string          `json:"id"`
	SkillID     string          `json:"skill_id"`
	PublisherID string          `json:"publisher_id"`
	Revision    int64           `json:"revision"`
	Level       string          `json:"level"`
	Bundle      json.RawMessage `json:"bundle"`
	Digest      string          `json:"digest"`
	CreatedAt   time.Time       `json:"created_at"`
}

// skillRank orders the levels so gating is one comparison.
func skillRank(level string) (int, bool) {
	switch level {
	case SkillLevelUser:
		return 1, true
	case SkillLevelWorkspace:
		return 2, true
	case SkillLevelAdmin:
		return 3, true
	default:
		return 0, false
	}
}

// PutSkill publishes the next immutable revision of a skill bundle. Repeating
// byte-identical content is an idempotent replay of the same revision; new
// content gets revision+1. A skill is published by a publisher, exactly like a
// manifest version.
func (s *Store) PutSkill(ctx context.Context, publisherID, skillID, level string, bundle json.RawMessage) (Skill, error) {
	return s.putSkill(ctx, publisherID, skillID, level, bundle, nil, contracts.Scope{})
}

func (s *Store) putSkill(ctx context.Context, publisherID, skillID, level string, bundle json.RawMessage, expected *int64, scope contracts.Scope) (Skill, error) {
	if err := requireID("skill id", skillID); err != nil {
		return Skill{}, err
	}
	if !publisherPattern.MatchString(publisherID) {
		return Skill{}, invalid("publisher id must be a lowercase identifier of at most 128 bytes")
	}
	if _, ok := skillRank(level); !ok {
		return Skill{}, invalid("skill level must be admin, workspace or user")
	}
	if len(bundle) == 0 || len(bundle) > maxSkillBundleBytes {
		return Skill{}, invalid("skill bundle must be non-empty and at most 1 MiB")
	}
	var probe map[string]any
	if err := json.Unmarshal(bundle, &probe); err != nil {
		return Skill{}, invalid("skill bundle must be a JSON object")
	}
	digest, err := contracts.Digest(map[string]any{"skill": skillID, "level": level, "bundle": probe})
	if err != nil {
		return Skill{}, invalid("skill bundle cannot be canonicalised")
	}

	var out Skill
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "skill/"+skillID); err != nil {
			return err
		}
		if expected != nil {
			if *expected < 0 {
				return invalid("expected_revision cannot be negative")
			}
			if err := checkTenantWritable(ctx, tx, scope.Tenant); err != nil {
				return err
			}
			if err := checkSubjectWritable(ctx, tx, scope.Tenant, scope.Subject); err != nil {
				return err
			}
			var rev int64
			if err := tx.QueryRow(ctx, `SELECT COALESCE(max(revision),0) FROM ahp_registry.skill_revisions WHERE skill_id=$1`, skillID).Scan(&rev); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO ahp_registry.skill_owners(publisher_id,tenant,subject) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, publisherID, scope.Tenant, func() string {
				if publisherID == skillOwner(scope.Tenant, "") {
					return ""
				}
				return scope.Subject
			}()); err != nil {
				return err
			}
			if rev != *expected {
				return contracts.NewError(contracts.ErrConflict, "skill revision changed; reload before saving")
			}
		}

		var existingID string
		var revision int64
		var created time.Time
		var pub, lvl string
		var doc []byte
		err := tx.QueryRow(ctx, `
SELECT id, revision, level, publisher_id, bundle::text, created_at FROM ahp_registry.skill_revisions
WHERE skill_id = $1 AND digest = $2`, skillID, digest).
			Scan(&existingID, &revision, &lvl, &pub, &doc, &created)
		if err == nil {
			if pub != publisherID {
				return contracts.NewError(contracts.ErrConflict, "skill belongs to another publisher")
			}
			out = Skill{ID: existingID, SkillID: skillID, PublisherID: pub, Revision: revision,
				Level: lvl, Bundle: json.RawMessage(doc), Digest: digest, CreatedAt: created.UTC()}
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return storageError("read skill revision", err)
		}
		// Who owns this skill? A skill is a single publisher's artifact.
		var owner string
		err = tx.QueryRow(ctx, `
SELECT publisher_id FROM ahp_registry.skill_revisions WHERE skill_id = $1 LIMIT 1 FOR SHARE`, skillID).Scan(&owner)
		switch {
		case err == nil && owner != publisherID:
			return contracts.NewError(contracts.ErrForbidden, "skill belongs to another publisher")
		case err != nil && !errors.Is(err, pgx.ErrNoRows):
			return storageError("read skill owner", err)
		}
		var maxRev int64
		if err := tx.QueryRow(ctx, `
SELECT COALESCE(max(revision), 0) FROM ahp_registry.skill_revisions WHERE skill_id = $1`, skillID).Scan(&maxRev); err != nil {
			return storageError("read skill revisions", err)
		}
		out = Skill{ID: contracts.NewID(), SkillID: skillID, PublisherID: publisherID,
			Revision: maxRev + 1, Level: level, Bundle: append(json.RawMessage(nil), bundle...), Digest: digest}
		if _, err := tx.Exec(ctx, `
INSERT INTO ahp_registry.skill_revisions (id, skill_id, publisher_id, revision, level, bundle, digest)
VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)`,
			out.ID, skillID, publisherID, out.Revision, level, string(bundle), digest); err != nil {
			if isUniqueViolation(err) {
				return contracts.NewError(contracts.ErrConflict, "skill revision changed concurrently, retry")
			}
			return storageError("store skill revision", err)
		}
		return nil
	})
	if err != nil {
		return Skill{}, err
	}
	return out, nil
}

// GetSkill loads one revision. revision 0 means the latest revision.
func (s *Store) GetSkill(ctx context.Context, skillID string, revision int64) (Skill, error) {
	if err := requireID("skill id", skillID); err != nil {
		return Skill{}, err
	}
	q := `SELECT id, skill_id, publisher_id, revision, level, bundle::text, digest, created_at
	      FROM ahp_registry.skill_revisions WHERE skill_id = $1 AND ($2 = 0 OR revision = $2)
	      ORDER BY revision DESC LIMIT 1`
	return scanSkill(s.pool.QueryRow(ctx, q, skillID, revision))
}

// SkillFor returns the newest revision of a skill a caller at callerLevel may
// receive. A skill above the caller's level is NOT_FOUND, not FORBIDDEN: the
// Registry never confirms that a higher-scope skill exists.
func (s *Store) SkillFor(ctx context.Context, skillID, callerLevel string, revision int64) (Skill, error) {
	rank, ok := skillRank(callerLevel)
	if !ok {
		return Skill{}, invalid("caller level must be admin, workspace or user")
	}
	skill, err := s.GetSkill(ctx, skillID, revision)
	if err != nil {
		return Skill{}, err
	}
	skillRankValue, _ := skillRank(skill.Level)
	if skillRankValue > rank {
		return Skill{}, contracts.NewError(contracts.ErrNotFound, "skill revision not found at this level")
	}
	return skill, nil
}

// ListSkills returns the newest revision of every skill the caller level may
// see, without bundle bodies.
func (s *Store) ListSkills(ctx context.Context, callerLevel string) ([]Skill, error) {
	rank, ok := skillRank(callerLevel)
	if !ok {
		return nil, invalid("caller level must be admin, workspace or user")
	}
	rows, err := s.pool.Query(ctx, `
SELECT DISTINCT ON (skill_id) id, skill_id, publisher_id, revision, level, ''::text, digest, created_at
FROM ahp_registry.skill_revisions
WHERE CASE level WHEN 'user' THEN 1 WHEN 'workspace' THEN 2 ELSE 3 END <= $1
ORDER BY skill_id, revision DESC`, rank)
	if err != nil {
		return nil, storageError("list skills", err)
	}
	defer rows.Close()
	out := make([]Skill, 0, 8)
	for rows.Next() {
		skill, serr := scanSkill(rows)
		if serr != nil {
			return nil, serr
		}
		skill.Bundle = nil
		out = append(out, skill)
	}
	return out, rows.Err()
}

func scanSkill(row pgx.Row) (Skill, error) {
	var (
		skill   Skill
		doc     string
		created time.Time
	)
	err := row.Scan(&skill.ID, &skill.SkillID, &skill.PublisherID, &skill.Revision,
		&skill.Level, &doc, &skill.Digest, &created)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Skill{}, contracts.NewError(contracts.ErrNotFound, "skill revision not found")
		}
		return Skill{}, storageError("read skill revision", err)
	}
	if doc != "" {
		skill.Bundle = json.RawMessage(doc)
	}
	skill.CreatedAt = created.UTC()
	return skill, nil
}
