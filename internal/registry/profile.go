package registry

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

const DotAgentID = "agents-space"
const profileDDL = `
CREATE TABLE IF NOT EXISTS ahp_registry.model_route_generations (
 tenant text NOT NULL, route text NOT NULL, revision bigint NOT NULL, PRIMARY KEY(tenant,route));
CREATE TABLE IF NOT EXISTS ahp_registry.skill_owners (publisher_id text PRIMARY KEY,tenant text NOT NULL,subject text NOT NULL);
CREATE TABLE IF NOT EXISTS ahp_registry.profiles (
 tenant text NOT NULL, subject text NOT NULL, revision bigint NOT NULL, data jsonb NOT NULL,
 PRIMARY KEY(tenant,subject));
CREATE TABLE IF NOT EXISTS ahp_registry.dot_pins (
 tenant text NOT NULL, subject text NOT NULL, deployment text NOT NULL, operation_key text NOT NULL,
 context_id text NOT NULL, snapshot jsonb NOT NULL,
 PRIMARY KEY(tenant,subject,deployment,operation_key));`

// Profile is a private revisioned overlay; ownership is supplied by verified scope.
type Profile struct {
	Revision int64           `json:"revision"`
	Data     json.RawMessage `json:"data"`
}

type dotOverlay struct {
	Instructions   string   `json:"instructions,omitempty"`
	ModelRoute     string   `json:"model_route,omitempty"`
	Skills         []string `json:"skills,omitempty"`
	PersonalSkills []string `json:"personal_skills,omitempty"`
	MaxSteps       int      `json:"max_steps,omitempty"`
	ComputerPolicy string   `json:"computer_policy,omitempty"`
}

type dotPolicy struct {
	AllowedModelRoutes        []string `json:"allowed_model_routes"`
	AllowedSkills             []string `json:"allowed_skills"`
	AllowedTools              []string `json:"allowed_tools"`
	AllowedComputerPolicies   []string `json:"allowed_computer_policies"`
	AllowPersonalInstructions bool     `json:"allow_personal_instructions"`
	AllowPersonalSkills       bool     `json:"allow_personal_skills"`
}

type dotTenantConfig struct {
	dotOverlay
	Policy dotPolicy `json:"policy"`
}

// DotSnapshot contains all configuration bytes the runner needs after restart.
// Model and skill revisions are resolved once, never via a mutable latest pointer.
type DotSnapshot struct {
	Revision          int64            `json:"revision"`
	ProfileRevision   int64            `json:"profile_revision"`
	DeploymentVersion string           `json:"deployment_version,omitempty"`
	Dot               DotRuntimeConfig `json:"dot"`
}

type DotRuntimeConfig struct {
	Instructions   string      `json:"instructions"`
	ModelRoute     *ModelRoute `json:"model_route,omitempty"`
	Skills         []Skill     `json:"skills"`
	MaxSteps       int         `json:"max_steps"`
	AllowedTools   []string    `json:"allowed_tools"`
	ComputerPolicy string      `json:"computer_policy"`
}

func strictDocument(data []byte, out any) error {
	if len(data) == 0 || len(data) > 64<<10 {
		return invalid("configuration must be non-empty and within 64 KiB")
	}
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return invalid("configuration must be an object")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return invalid("configuration contains unknown fields or invalid values")
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return invalid("configuration must contain one object")
	}
	return nil
}

func validateDotConfig(data json.RawMessage) error {
	var c dotTenantConfig
	if err := strictDocument(data, &c); err != nil {
		return err
	}
	if c.MaxSteps < 1 || c.MaxSteps > 256 {
		return invalid("tenant max_steps must be between 1 and 256")
	}
	if len(c.Instructions) > 16384 {
		return invalid("instructions exceed 16 KiB")
	}
	if c.ModelRoute == "" || !containsString(c.Policy.AllowedModelRoutes, c.ModelRoute) {
		return invalid("default route is outside tenant policy")
	}
	if c.ComputerPolicy == "" {
		c.ComputerPolicy = "disabled"
	}
	if c.ComputerPolicy != "disabled" && !containsString(c.Policy.AllowedComputerPolicies, c.ComputerPolicy) {
		return invalid("default Computer policy is outside tenant policy")
	}
	for _, p := range c.Policy.AllowedComputerPolicies {
		if p != "disabled" && p != "ephemeral" && p != "personal" {
			return invalid("unknown Computer policy")
		}
	}
	for _, id := range c.Skills {
		if !containsString(c.Policy.AllowedSkills, id) {
			return invalid("default skill is outside tenant policy")
		}
	}
	if len(c.PersonalSkills) > 0 {
		return invalid("tenant config cannot name personal skills")
	}
	return nil
}

func readProfile(ctx context.Context, tx pgx.Tx, tenant, subject string) (Profile, error) {
	p := Profile{Data: json.RawMessage(`{}`)}
	err := tx.QueryRow(ctx, `SELECT revision,data FROM ahp_registry.profiles WHERE tenant=$1 AND subject=$2`, tenant, subject).Scan(&p.Revision, &p.Data)
	if errors.Is(err, pgx.ErrNoRows) {
		return p, nil
	}
	return p, storageError("read profile", err)
}

// GetProfile returns only the caller's private overlay; an absent overlay is revision zero.
func (s *Store) GetProfile(ctx context.Context, scope contracts.Scope) (Profile, error) {
	if err := scope.Validate(); err != nil {
		return Profile{}, err
	}
	var p Profile
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var err error
		p, err = readProfile(ctx, tx, scope.Tenant, scope.Subject)
		return err
	})
	return p, err
}

func resolveDot(ctx context.Context, tx pgx.Tx, scope contracts.Scope, p Profile) (DotSnapshot, error) {
	c, err := getConfig(ctx, tx, scope.Tenant, DotAgentID)
	if contracts.Code(err) == contracts.ErrNotFound {
		return DotSnapshot{}, contracts.NewError(contracts.ErrUnavailable, "Dot tenant configuration is missing: administrator must save agents-space AgentConfig before starting a run")
	} else if err != nil {
		return DotSnapshot{}, err
	}
	if err := validateDotConfig(c.Data); err != nil {
		return DotSnapshot{}, err
	}
	var tenant dotTenantConfig
	if err := strictDocument(c.Data, &tenant); err != nil {
		return DotSnapshot{}, err
	}
	var user dotOverlay
	if err := strictDocument(p.Data, &user); err != nil {
		return DotSnapshot{}, err
	}
	if len(user.Instructions) > 16384 || user.MaxSteps < 0 || len(user.Skills) > 32 || len(user.PersonalSkills) > 32 {
		return DotSnapshot{}, invalid("profile exceeds configured bounds")
	}
	deny := func(message string) (DotSnapshot, error) {
		return DotSnapshot{}, contracts.NewError(contracts.ErrForbidden, message)
	}
	if user.Instructions != "" && !tenant.Policy.AllowPersonalInstructions {
		return deny("personal instructions disabled by tenant")
	}
	if user.MaxSteps > tenant.MaxSteps {
		return deny("profile cannot increase tenant step budget")
	}
	if user.ModelRoute != "" && !containsString(tenant.Policy.AllowedModelRoutes, user.ModelRoute) {
		return deny("model route is outside tenant policy")
	}
	if user.ComputerPolicy != "" && !containsString(tenant.Policy.AllowedComputerPolicies, user.ComputerPolicy) {
		return deny("Computer policy is outside tenant policy")
	}
	effective := tenant.dotOverlay
	if user.Instructions != "" {
		effective.Instructions += "\n\nPersonal instructions:\n" + user.Instructions
	}
	if user.ModelRoute != "" {
		effective.ModelRoute = user.ModelRoute
	}
	if user.MaxSteps > 0 {
		effective.MaxSteps = user.MaxSteps
	}
	if user.ComputerPolicy != "" {
		effective.ComputerPolicy = user.ComputerPolicy
	}
	if effective.ComputerPolicy == "" {
		effective.ComputerPolicy = "disabled"
	}
	if user.Skills != nil {
		effective.Skills = user.Skills
	}
	if len(user.PersonalSkills) > 0 && !tenant.Policy.AllowPersonalSkills {
		return deny("personal skills disabled by tenant")
	}
	out := DotSnapshot{Revision: c.Revision, ProfileRevision: p.Revision, Dot: DotRuntimeConfig{Instructions: effective.Instructions, MaxSteps: effective.MaxSteps, ComputerPolicy: effective.ComputerPolicy, AllowedTools: append([]string{}, tenant.Policy.AllowedTools...), Skills: []Skill{}}}
	if effective.ModelRoute != "" {
		route, err := resolveRouteInTx(ctx, tx, scope.Tenant, effective.ModelRoute)
		if err != nil {
			return out, err
		}
		out.Dot.ModelRoute = &route
	}
	for _, id := range effective.Skills {
		if !containsString(tenant.Policy.AllowedSkills, id) {
			return deny("skill is outside tenant policy")
		}
		skill, err := scopedSkillInTx(ctx, tx, scope.Tenant, "", id, 0)
		if err != nil {
			return out, err
		}
		if skill.Level != SkillLevelUser {
			return out, contracts.NewError(contracts.ErrNotFound, "Dot skill is not available at user level")
		}
		out.Dot.Skills = append(out.Dot.Skills, skill)
	}
	for _, id := range user.PersonalSkills {
		skill, err := scopedSkillInTx(ctx, tx, scope.Tenant, scope.Subject, id, 0)
		if err != nil {
			return out, err
		}
		if skill.Level != SkillLevelUser {
			return out, contracts.NewError(contracts.ErrNotFound, "Dot skill is not available at user level")
		}
		out.Dot.Skills = append(out.Dot.Skills, skill)
	}
	doc, err := json.Marshal(out)
	if err != nil {
		return out, err
	}
	if len(doc) > 256<<10 {
		return out, invalid("selected skills exceed the 256 KiB runtime configuration budget")
	}
	return out, nil
}

// PutProfile CAS-writes an allowlisted overlay after validating against current tenant policy.
func (s *Store) PutProfile(ctx context.Context, scope contracts.Scope, revision int64, data json.RawMessage) (Profile, error) {
	if err := scope.Validate(); err != nil {
		return Profile{}, err
	}
	if revision < 0 {
		return Profile{}, invalid("expected_revision cannot be negative")
	}
	var shape dotOverlay
	if err := strictDocument(data, &shape); err != nil {
		return Profile{}, err
	}
	var out Profile
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if err := checkTenantWritable(ctx, tx, scope.Tenant); err != nil {
			return err
		}
		if err := checkSubjectWritable(ctx, tx, scope.Tenant, scope.Subject); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "dot-profile/"+scope.Tenant+"/"+scope.Subject); err != nil {
			return err
		}
		old, err := readProfile(ctx, tx, scope.Tenant, scope.Subject)
		if err != nil {
			return err
		}
		if old.Revision != revision {
			return contracts.NewError(contracts.ErrConflict, "profile revision changed; reload before saving")
		}
		out = Profile{Revision: revision + 1, Data: data}
		if _, err := resolveDot(ctx, tx, scope, out); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO ahp_registry.profiles(tenant,subject,revision,data) VALUES($1,$2,$3,$4) ON CONFLICT(tenant,subject) DO UPDATE SET revision=excluded.revision,data=excluded.data`, scope.Tenant, scope.Subject, out.Revision, data)
		return storageError("write profile", err)
	})
	return out, err
}

// PinDotConfig atomically captures profile, tenant policy, skills and model route for
// one stable AcceptTurn key. A retry returns the original bytes even after edits.
func (s *Store) PinDotConfig(ctx context.Context, scope contracts.Scope, key, contextID string) (DotSnapshot, error) {
	if err := scope.Validate(); err != nil {
		return DotSnapshot{}, err
	}
	for k, v := range map[string]string{"key": key, "context": contextID} {
		if err := requireID(k, v); err != nil {
			return DotSnapshot{}, err
		}
	}
	var out DotSnapshot
	err := s.withDotPinSnapshot(ctx, scope, key, func(tx pgx.Tx) error {
		if err := checkTenantWritable(ctx, tx, scope.Tenant); err != nil {
			return err
		}
		if err := checkSubjectWritable(ctx, tx, scope.Tenant, scope.Subject); err != nil {
			return err
		}
		var doc []byte
		var oldContext string
		err := tx.QueryRow(ctx, `SELECT context_id,snapshot FROM ahp_registry.dot_pins WHERE tenant=$1 AND subject=$2 AND deployment=$3 AND operation_key=$4`, scope.Tenant, scope.Subject, scope.Deployment, key).Scan(&oldContext, &doc)
		if err == nil {
			if oldContext != contextID {
				return contracts.NewError(contracts.ErrConflict, "run key belongs to another conversation")
			}
			return json.Unmarshal(doc, &out)
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return storageError("read Dot pin", err)
		}
		p, err := readProfile(ctx, tx, scope.Tenant, scope.Subject)
		if err != nil {
			return err
		}
		out, err = resolveDot(ctx, tx, scope, p)
		if err != nil {
			return err
		}
		doc, err = json.Marshal(out)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO ahp_registry.dot_pins(tenant,subject,deployment,operation_key,context_id,snapshot) VALUES($1,$2,$3,$4,$5,$6)`, scope.Tenant, scope.Subject, scope.Deployment, key, contextID, doc)
		return storageError("pin Dot config", err)
	})
	return out, err
}

// withDotPinSnapshot acquires erasure and operation locks before establishing a
// repeatable-read snapshot. Waiting inside RR would retain a stale view of a
// concurrently committed pin or erasure fence. One connection also works with a
// single-connection pool; uncertain commit outcomes are never retried here.
func (s *Store) withDotPinSnapshot(ctx context.Context, scope contracts.Scope, key string, fn func(pgx.Tx) error) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return storageError("acquire Dot pin connection", err)
	}
	locks := []struct{ key, acquire, release string }{
		{tenantFenceKey(scope.Tenant), "pg_advisory_lock_shared", "pg_advisory_unlock_shared"},
		{"ahp_registry/subject-erasure/" + scope.Tenant + "/" + scope.Subject, "pg_advisory_lock_shared", "pg_advisory_unlock_shared"},
		{"dot-pin/" + scope.Tenant + "/" + scope.Subject + "/" + scope.Deployment + "/" + key, "pg_advisory_lock", "pg_advisory_unlock"},
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if conn.Conn().IsClosed() || conn.Conn().PgConn().TxStatus() != 'I' {
			_ = conn.Hijack().Close(cleanup)
			return
		}
		// Also unlock a possibly acquired lock whose cancelled response was lost.
		// Never return a connection with uncertain session locks to the pool.
		for i := len(locks) - 1; i >= 0; i-- {
			if _, err := conn.Exec(cleanup, "SELECT "+locks[i].release+"(hashtextextended($1,0))", locks[i].key); err != nil {
				_ = conn.Hijack().Close(cleanup)
				return
			}
		}
		conn.Release()
	}()
	for _, lock := range locks {
		if _, err := conn.Exec(ctx, "SELECT "+lock.acquire+"(hashtextextended($1,0))", lock.key); err != nil {
			return storageError("lock Dot pin", err)
		}
	}
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return storageError("begin Dot pin", err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	if err := fn(tx); err != nil {
		return err
	}
	return storageError("commit Dot pin", tx.Commit(ctx))
}

func skillOwner(tenant, subject string) string {
	h := sha256.Sum256([]byte(tenant + "\x00" + subject))
	return "dot-" + hex.EncodeToString(h[:16])
}
func scopedSkillID(tenant, subject, id string) string { return skillOwner(tenant, subject) + "." + id }
func scopedSkillInTx(ctx context.Context, tx pgx.Tx, tenant, subject, id string, revision int64) (Skill, error) {
	skill, err := scanSkill(tx.QueryRow(ctx, `SELECT id,skill_id,publisher_id,revision,level,bundle::text,digest,created_at FROM ahp_registry.skill_revisions WHERE skill_id=$1 AND ($2=0 OR revision=$2) ORDER BY revision DESC LIMIT 1`, scopedSkillID(tenant, subject, id), revision))
	skill.SkillID = id
	return skill, err
}

// ListScopedSkills reads the tenant catalog or the caller's own catalog only.
func (s *Store) ListScopedSkills(ctx context.Context, tenant, subject string) ([]Skill, error) {
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT ON(skill_id) id,skill_id,publisher_id,revision,level,''::text,digest,created_at FROM ahp_registry.skill_revisions WHERE publisher_id=$1 ORDER BY skill_id,revision DESC`, skillOwner(tenant, subject))
	if err != nil {
		return nil, storageError("list scoped skills", err)
	}
	defer rows.Close()
	out := []Skill{}
	for rows.Next() {
		skill, err := scanSkill(rows)
		if err != nil {
			return nil, err
		}
		skill.SkillID = strings.TrimPrefix(skill.SkillID, skillOwner(tenant, subject)+".")
		out = append(out, skill)
	}
	return out, rows.Err()
}

// GetScopedSkill reads an exact immutable revision in the caller's catalog.
func (s *Store) GetScopedSkill(ctx context.Context, tenant, subject, id string, revision int64) (Skill, error) {
	var out Skill
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		var err error
		out, err = scopedSkillInTx(ctx, tx, tenant, subject, id, revision)
		return err
	})
	return out, err
}

// PutScopedSkill publishes instructions under tenant/subject ownership with CAS.
func (s *Store) PutScopedSkill(ctx context.Context, scope contracts.Scope, personal bool, id, level string, revision int64, bundle json.RawMessage) (Skill, error) {
	if err := scope.Validate(); err != nil {
		return Skill{}, err
	}
	if !routePattern.MatchString(id) {
		return Skill{}, invalid("skill id must be a lowercase identifier of at most 64 bytes")
	}
	var document struct {
		Instructions string `json:"instructions"`
	}
	if err := strictDocument(bundle, &document); err != nil {
		return Skill{}, err
	}
	if strings.TrimSpace(document.Instructions) == "" || len(document.Instructions) > 16384 {
		return Skill{}, invalid("skill instructions required, maximum 16 KiB")
	}
	subject := ""
	if personal {
		subject = scope.Subject
		level = SkillLevelUser
	}
	// The existing primitive owns immutable revisions and digest calculation.
	out, err := s.putSkill(ctx, skillOwner(scope.Tenant, subject), scopedSkillID(scope.Tenant, subject, id), level, bundle, &revision, scope)
	out.SkillID = id
	return out, err
}

// DeleteModelRoute removes a current route by revision; pinned runs keep their copy.
func (s *Store) DeleteModelRoute(ctx context.Context, tenant, route string, revision int64) error {
	if err := requireID("tenant", tenant); err != nil {
		return err
	}
	if !routePattern.MatchString(route) {
		return invalid("model route must be a lowercase identifier of at most 64 bytes")
	}
	if revision < 1 {
		return invalid("positive expected_revision required")
	}
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if err := checkTenantWritable(ctx, tx, tenant); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "ahp_registry/route/"+tenant+"/"+route); err != nil {
			return storageError("lock route", err)
		}
		var removed int64
		err := tx.QueryRow(ctx, `DELETE FROM ahp_registry.model_routes WHERE tenant=$1 AND route=$2 AND revision=$3 RETURNING revision`, tenant, route, revision).Scan(&removed)
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.NewError(contracts.ErrConflict, fmt.Sprintf("route %s changed or no longer exists", route))
		}
		if err != nil {
			return storageError("delete route", err)
		}
		// Keep the generation so a stale editor cannot match a recreated route.
		_, err = tx.Exec(ctx, `INSERT INTO ahp_registry.model_route_generations(tenant,route,revision) VALUES($1,$2,$3)
ON CONFLICT(tenant,route) DO UPDATE SET revision=GREATEST(ahp_registry.model_route_generations.revision,EXCLUDED.revision)`, tenant, route, removed)
		if err != nil {
			return storageError("preserve route generation", err)
		}
		return nil
	})
}
