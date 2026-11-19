package memory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ProjectBatch     int
	PersonalBatch    int
	ProjectWait      time.Duration
	PersonalWait     time.Duration
	Lease            time.Duration
	Poll             time.Duration
	PurgeGrace       time.Duration
	AuditInterval    time.Duration
	PayloadRetention time.Duration
	MaxAttempts      int
	MaxPending       int
}

func DefaultConfig() Config {
	return Config{5, 20, 2 * time.Minute, 10 * time.Minute, 90 * time.Second, 5 * time.Second, 2 * time.Minute, time.Hour, 24 * time.Hour, 8, 256}
}

type Service struct {
	Store  Store
	Engine Engine
	Access Access
	Config Config
	Now    func() time.Time
}

func New(store Store, engine Engine, access Access) *Service {
	return &Service{Store: store, Engine: engine, Access: access, Config: DefaultConfig(), Now: func() time.Time { return time.Now().UTC() }}
}

func digest(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func mustRecord[T any](tx Tx, kind, id string, fallback T) (T, error) {
	var v T
	err := tx.Get(kind, id, &v)
	if errors.Is(err, ErrMissing) {
		return fallback, nil
	}
	return v, err
}
func (s *Service) user(tx Tx) (User, error) {
	return mustRecord(tx, "user", "settings", User{Enabled: true})
}
func (s *Service) project(tx Tx, id string) (ProjectState, error) {
	return mustRecord(tx, "project", id, ProjectState{ID: id, Enabled: true, UsePersonal: true, IdleDays: 30, LastActivity: s.Now()})
}
func (s *Service) update(ctx context.Context, owner string, fn func(Tx) error) error {
	return s.Store.Within(ctx, owner, func(tx Tx) error {
		var user User
		if err := tx.Get("user", "settings", &user); errors.Is(err, ErrMissing) {
			if err := tx.Put("user", "settings", User{Enabled: true}); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		if err := fn(tx); err != nil {
			return err
		}
		return s.schedule(tx)
	})
}
func (s *Service) mutate(ctx context.Context, id Identity, key, action string, request any, fn func(Tx) (any, error), out any) error {
	if err := id.Validate(s.Now()); err != nil {
		return err
	}
	if len(key) < 1 || len(key) > 200 {
		return Fail(400, "idempotency_key_required", "Idempotency-Key must contain 1 to 200 characters")
	}
	hash := digest(request)
	recordID := digest([]string{id.ActorID, action, key})
	return s.updateIdentity(ctx, id, func(tx Tx) error {
		fenced, err := subjectFenced(tx)
		if err != nil {
			return err
		}
		if fenced {
			return Fail(410, "subject_deleted", "memory subject has been erased")
		}
		var previous Idempotency
		err = tx.Get("idempotency", recordID, &previous)
		if err == nil {
			if previous.Hash != hash {
				return Fail(409, "idempotency_conflict", "key already used for a different request")
			}
			return json.Unmarshal(previous.Response, out)
		}
		if !errors.Is(err, ErrMissing) {
			return err
		}
		response, err := fn(tx)
		if err != nil {
			return err
		}
		b, err := json.Marshal(response)
		if err != nil {
			return err
		}
		if err := tx.Put("idempotency", recordID, Idempotency{hash, b}); err != nil {
			return err
		}
		return json.Unmarshal(b, out)
	})
}
func (s *Service) activeBank(tx Tx, scope Scope, project string, create bool) (Bank, error) {
	banks, err := list[Bank](tx, "bank")
	if err != nil {
		return Bank{}, err
	}
	if scope == Personal {
		project = ""
	}
	generation := 0
	var floor time.Time
	for _, b := range banks {
		if b.Scope == scope && b.ProjectID == project {
			if b.ClosedAt.After(floor) {
				floor = b.ClosedAt
			}
			if b.State == "active" {
				return b, nil
			}
			if b.Generation > generation {
				generation = b.Generation
			}
		}
	}
	if !create {
		return Bank{}, ErrMissing
	}
	b := Bank{ID: newID(), Scope: scope, ProjectID: project, Generation: generation + 1, State: "active", CreatedAt: s.Now(), SourceFloor: floor}
	return b, tx.Put("bank", b.ID, b)
}
func (s *Service) enabled(tx Tx, scope Scope, projectID string) (bool, error) {
	u, err := s.user(tx)
	if err != nil {
		return false, err
	}
	if projectID == "" {
		return scope == Personal && u.Enabled, nil
	}
	p, err := s.project(tx, projectID)
	if err != nil {
		return false, err
	}
	if p.Deleted {
		return false, nil
	}
	if scope == Personal {
		return u.Enabled && p.UsePersonal, nil
	}
	return p.Enabled, nil
}
func (s *Service) liveRun(tx Tx, project string) (bool, error) {
	runs, err := list[Run](tx, "run")
	if err != nil {
		return false, err
	}
	for _, r := range runs {
		if r.ProjectID == project && !r.Finished && r.Until.After(s.Now()) {
			return true, nil
		}
	}
	return false, nil
}
func (s *Service) expire(tx Tx, p ProjectState) error {
	if p.Deleted || s.Now().Before(p.LastActivity.Add(time.Duration(p.IdleDays)*24*time.Hour)) {
		return nil
	}
	live, err := s.liveRun(tx, p.ID)
	if err != nil {
		return err
	}
	if live {
		return nil
	}
	return s.closeBanksAt(tx, Project, p.ID, p.LastActivity.Add(time.Duration(p.IdleDays)*24*time.Hour))
}
func (s *Service) closeBanks(tx Tx, scope Scope, project string) error {
	return s.closeBanksAt(tx, scope, project, s.Now())
}
func (s *Service) closeBanksAt(tx Tx, scope Scope, project string, closedAt time.Time) error {
	banks, err := list[Bank](tx, "bank")
	if err != nil {
		return err
	}
	for _, b := range banks {
		if b.State != "active" || b.Scope != scope || (scope == Project && b.ProjectID != project) {
			continue
		}
		b.State = "deleting"
		b.ClosedAt = closedAt
		b.NextPurge = s.Now().Add(s.Config.PurgeGrace)
		b.Revision++
		if err := tx.Put("bank", b.ID, b); err != nil {
			return err
		}
		cursors, err := list[Cursor](tx, "cursor")
		if err != nil {
			return err
		}
		for _, c := range cursors {
			if c.BankID == b.ID {
				c.Pending = nil
				c.Flush = false
				if err := tx.Put("cursor", c.ID, c); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
func (s *Service) touch(tx Tx, project string) (ProjectState, error) {
	p, err := s.project(tx, project)
	if err != nil {
		return p, err
	}
	if p.Deleted {
		return p, Fail(410, "project_deleted", "project has been deleted")
	}
	if err := s.expire(tx, p); err != nil {
		return p, err
	}
	p.LastActivity = s.Now()
	return p, tx.Put("project", project, p)
}

func validateSources(sources []Source, now time.Time) error {
	if len(sources) == 0 || len(sources) > 100 {
		return Fail(400, "invalid_sources", "provide 1 to 100 sources")
	}
	total := 0
	for _, source := range sources {
		total += len(source.Text)
		if source.Ref == "" || len(source.Ref) > 1000 || strings.TrimSpace(source.Text) == "" || len(source.Text) > 65536 || source.At.IsZero() || source.At.After(now.Add(time.Minute)) {
			return Fail(400, "invalid_source", "source requires a reference, bounded text and a valid original timestamp")
		}
		if source.Role != "user" && source.Role != "assistant" && source.Role != "tool" {
			return Fail(400, "invalid_source_role", "only user, assistant and tool evidence is accepted")
		}
		for _, scope := range source.AllowedScopes {
			if !scope.Valid() {
				return Fail(400, "invalid_scope", "unknown source scope")
			}
		}
		if len(source.Labels) > 100 {
			return Fail(400, "invalid_labels", "too many source labels")
		}
	}
	if total > 128*1024 {
		return Fail(413, "source_limit", "a completed turn may contain at most 128 KiB of selected source text")
	}
	return nil
}
func allows(source Source, scope Scope) bool {
	for _, v := range source.AllowedScopes {
		if v == scope {
			return true
		}
	}
	return false
}

func (s *Service) Ingest(ctx context.Context, id Identity, key string, req TurnRequest) (map[string]any, error) {
	var result map[string]any
	if id.ActorKind != "runtime" && id.ActorKind != "platform" {
		return nil, Fail(403, "runtime_required", "turn events require a trusted runtime identity")
	}
	if req.SessionID == "" || req.Sequence < 1 || !req.Sanitized {
		return nil, Fail(400, "invalid_turn", "project, session, positive sequence and sanitized source attestation are required")
	}
	if err := validateSources(req.Sources, s.Now()); err != nil {
		return nil, err
	}
	intentDigest := req.IntentDigest
	if intentDigest == "" {
		intentDigest = digest(req)
	}
	err := s.mutate(ctx, id, key, "turn", intentDigest, func(tx Tx) (any, error) {
		turnID := digest([]any{req.ProjectID, req.SessionID, req.Sequence})
		var previous Turn
		if err := tx.Get("turn", turnID, &previous); err == nil {
			if previous.Hash != intentDigest {
				return nil, Fail(409, "turn_conflict", "turn sequence already has different content")
			}
			return map[string]any{"accepted": true, "turn_id": turnID}, nil
		} else if !errors.Is(err, ErrMissing) {
			return nil, err
		}
		if req.ProjectID != "" {
			if _, err := s.touch(tx, req.ProjectID); err != nil {
				return nil, err
			}
		}
		turn := Turn{ID: turnID, Hash: intentDigest, Request: req, Identity: id, ReceivedAt: s.Now()}
		queued := false
		if err := tx.Put("turn", turnID, turn); err != nil {
			return nil, err
		}
		for _, scope := range []Scope{Personal, Project} {
			enabled, err := s.enabled(tx, scope, req.ProjectID)
			if err != nil {
				return nil, err
			}
			if !enabled {
				continue
			}
			eligible := false
			for _, source := range req.Sources {
				if allows(source, scope) {
					eligible = true
					break
				}
			}
			if !eligible {
				continue
			}
			bank, err := s.activeBank(tx, scope, req.ProjectID, true)
			if err != nil {
				return nil, err
			}
			floor, err := s.sourceFloor(tx, bank, req.ProjectID)
			if err != nil {
				return nil, err
			}
			eligible = false
			for _, source := range req.Sources {
				if allows(source, scope) && !source.At.Before(floor) {
					eligible = true
				}
			}
			if !eligible {
				continue
			}
			cursorID := digest([]string{bank.ID, req.ProjectID, req.SessionID})
			c, err := mustRecord(tx, "cursor", cursorID, Cursor{ID: cursorID, BankID: bank.ID, ProjectID: req.ProjectID, SessionID: req.SessionID})
			if err != nil {
				return nil, err
			}
			if req.Sequence <= c.LastSequence {
				return nil, Fail(409, "out_of_order", "runtime must deliver completed turns in increasing sequence order")
			}
			if len(c.Pending) >= s.Config.MaxPending {
				return nil, Fail(429, "queue_limit", "pending memory window is full")
			}
			if len(c.Pending) == 0 {
				c.PendingSince = s.Now()
			}
			c.Pending = append(c.Pending, turnID)
			queued = true
			c.LastSequence = req.Sequence
			c.Flush = c.Flush || req.Flush
			if err := tx.Put("cursor", c.ID, c); err != nil {
				return nil, err
			}
		}
		if !queued {
			turn.Request.Sources = nil
			if err := tx.Put("turn", turnID, turn); err != nil {
				return nil, err
			}
		}
		return map[string]any{"accepted": true, "turn_id": turnID}, nil
	}, &result)
	return result, err
}

func (s *Service) Remember(ctx context.Context, id Identity, key string, req RememberRequest) (Operation, error) {
	var out Operation
	if !req.Target.Valid() || strings.TrimSpace(req.Text) == "" || len(req.Text) > 65536 || len(req.SourceRefs) == 0 || len(req.SourceRefs) > 100 || (req.Target == Project && req.ProjectID == "") {
		return out, Fail(400, "invalid_memory", "target, bounded text and source references are required")
	}
	for _, ref := range req.SourceRefs {
		if strings.TrimSpace(ref) == "" || len(ref) > 1000 {
			return out, Fail(400, "invalid_source", "invalid source reference")
		}
	}
	err := s.mutate(ctx, id, key, "remember", req, func(tx Tx) (any, error) {
		if req.ProjectID != "" {
			if _, err := s.touch(tx, req.ProjectID); err != nil {
				return nil, err
			}
		}
		on, err := s.enabled(tx, req.Target, req.ProjectID)
		if err != nil {
			return nil, err
		}
		if !on {
			return nil, Fail(409, "memory_disabled", "requested memory is disabled")
		}
		bank, err := s.activeBank(tx, req.Target, req.ProjectID, true)
		if err != nil {
			return nil, err
		}
		bank.Labels = unique(append(bank.Labels, id.Labels...))
		bank.Revision++
		if err := tx.Put("bank", bank.ID, bank); err != nil {
			return nil, err
		}
		refs, _ := json.Marshal(req.SourceRefs)
		job := Job{ID: newID(), Kind: "retain", BankID: bank.ID, Scope: req.Target, ProjectID: req.ProjectID, Identity: id, Content: req.Text, Metadata: map[string]string{"source_refs": string(refs), "mission_version": MissionVersion}, State: "queued", CreatedAt: s.Now(), NextAt: s.Now()}
		for _, ref := range req.SourceRefs {
			job.Sources = append(job.Sources, Source{Ref: ref, AllowedScopes: []Scope{req.Target}})
		}
		if err := tx.Put("job", job.ID, job); err != nil {
			return nil, err
		}
		return operation(job), nil
	}, &out)
	return out, err
}
func operation(j Job) Operation {
	out := Operation{ID: j.ID, State: j.State, Scope: j.Scope, Error: j.Error}
	if j.Kind == "retain" {
		if raw, ok := j.Metadata["facts_committed"]; ok {
			if n, err := strconv.Atoi(raw); err == nil {
				out.FactsCommitted = &n
			}
		}
	}
	return out
}
func (s *Service) GetOperation(ctx context.Context, id Identity, jobID, projectID string, scope Scope) (Operation, error) {
	var out Operation
	err := s.updateIdentity(ctx, id, func(tx Tx) error {
		var j Job
		if err := tx.Get("job", jobID, &j); err != nil {
			return err
		}
		if j.ProjectID != projectID || j.Scope != scope {
			return Fail(403, "wrong_project", "operation does not belong to the requested area")
		}
		out = operation(j)
		return nil
	})
	return out, err
}
func (s *Service) Settings(ctx context.Context, id Identity, project string) (Settings, error) {
	var out Settings
	err := s.updateIdentity(ctx, id, func(tx Tx) error {
		u, err := s.user(tx)
		if err != nil {
			return err
		}
		p, err := s.project(tx, project)
		if err != nil {
			return err
		}
		if p.Deleted {
			return Fail(410, "project_deleted", "project has been deleted")
		}
		out = Settings{u.Enabled, p.Enabled, p.UsePersonal, p.IdleDays, p.LastActivity}
		return nil
	})
	return out, err
}
func (s *Service) UpdateSettings(ctx context.Context, id Identity, key string, req SettingsRequest) (Settings, error) {
	var out Settings
	if req.IdleDays != nil && (*req.IdleDays < 1 || *req.IdleDays > 36500) {
		return out, Fail(400, "invalid_idle_days", "idle_days must be between 1 and 36500")
	}
	if req.ProjectID == "" && (req.ProjectEnabled != nil || req.UsePersonal != nil || req.IdleDays != nil) {
		return out, Fail(400, "project_required", "project settings require project_id")
	}
	if req.ProjectID != "" && req.PersonalEnabled != nil {
		return out, Fail(400, "global_setting", "personal_enabled is a global user setting")
	}
	err := s.mutate(ctx, id, key, "settings", req, func(tx Tx) (any, error) {
		u, err := s.user(tx)
		if err != nil {
			return nil, err
		}
		p, err := s.project(tx, req.ProjectID)
		if err != nil {
			return nil, err
		}
		if p.Deleted {
			return nil, Fail(410, "project_deleted", "project has been deleted")
		}
		changedGlobal := req.PersonalEnabled != nil && *req.PersonalEnabled != u.Enabled
		changedProject := req.ProjectEnabled != nil && *req.ProjectEnabled != p.Enabled
		changedPersonal := req.UsePersonal != nil && *req.UsePersonal != p.UsePersonal
		if changedGlobal {
			u.Enabled = *req.PersonalEnabled
			u.Since = s.Now()
			if err := tx.Put("user", "settings", u); err != nil {
				return nil, err
			}
		}
		if changedProject {
			p.Enabled = *req.ProjectEnabled
			p.ProjectSince = s.Now()
		}
		if changedPersonal {
			p.UsePersonal = *req.UsePersonal
			p.PersonalSince = s.Now()
		}
		if req.IdleDays != nil {
			p.IdleDays = *req.IdleDays
		}
		if req.ProjectID != "" {
			if err := tx.Put("project", p.ID, p); err != nil {
				return nil, err
			}
		}
		// Discard pending material on either transition; enabling never backfills.
		cs, err := list[Cursor](tx, "cursor")
		if err != nil {
			return nil, err
		}
		for _, c := range cs {
			var b Bank
			if err := tx.Get("bank", c.BankID, &b); err != nil {
				return nil, err
			}
			changed := (changedGlobal && b.Scope == Personal) || (c.ProjectID == req.ProjectID && ((changedPersonal && b.Scope == Personal) || (changedProject && b.Scope == Project)))
			if changed {
				c.Pending = nil
				c.Flush = false
				if err := tx.Put("cursor", c.ID, c); err != nil {
					return nil, err
				}
			}
		}
		return Settings{u.Enabled, p.Enabled, p.UsePersonal, p.IdleDays, p.LastActivity}, nil
	}, &out)
	return out, err
}
func (s *Service) Clear(ctx context.Context, id Identity, key string, req ClearRequest) (map[string]any, error) {
	var out map[string]any
	if !req.Scope.Valid() || (req.Scope == Project && req.ProjectID == "") {
		return nil, Fail(400, "invalid_scope", "select personal or a current project")
	}
	err := s.mutate(ctx, id, key, "clear", req, func(tx Tx) (any, error) {
		if req.ProjectID != "" {
			if _, err := s.touch(tx, req.ProjectID); err != nil {
				return nil, err
			}
		}
		if err := s.closeBanks(tx, req.Scope, req.ProjectID); err != nil {
			return nil, err
		}
		return map[string]any{"state": "deleting"}, nil
	}, &out)
	return out, err
}
func (s *Service) DeleteProject(ctx context.Context, id Identity, key, project string) (map[string]any, error) {
	var out map[string]any
	if project == "" {
		return nil, Fail(400, "project_required", "project_id is required")
	}
	err := s.mutate(ctx, id, key, "delete_project", project, func(tx Tx) (any, error) {
		p, err := s.project(tx, project)
		if err != nil {
			return nil, err
		}
		p.Deleted = true
		if err := tx.Put("project", project, p); err != nil {
			return nil, err
		}
		if err := s.closeBanks(tx, Project, project); err != nil {
			return nil, err
		}
		return map[string]any{"state": "deleting"}, nil
	}, &out)
	return out, err
}
func (s *Service) RunEvent(ctx context.Context, id Identity, key string, req RunRequest) (map[string]any, error) {
	var out map[string]any
	if req.ProjectID == "" || req.RunID == "" || (id.ActorKind != "runtime" && id.ActorKind != "platform") {
		return nil, Fail(403, "runtime_required", "trusted runtime and project/run references are required")
	}
	err := s.mutate(ctx, id, key, "run", req, func(tx Tx) (any, error) {
		if _, err := s.touch(tx, req.ProjectID); err != nil {
			return nil, err
		}
		runID := digest([]string{req.ProjectID, req.RunID})
		r, err := mustRecord(tx, "run", runID, Run{ID: runID, ProjectID: req.ProjectID})
		if err != nil {
			return nil, err
		}
		if !r.Finished {
			r.Finished = req.Finished
			r.Until = s.Now().Add(5 * time.Minute)
			if id.ExpiresAt.Before(r.Until) {
				r.Until = id.ExpiresAt
			}
		}
		return map[string]any{"accepted": true}, tx.Put("run", r.ID, r)
	}, &out)
	return out, err
}

func (s *Service) snapshot(tx Tx, scope Scope, project string) (Bank, error) {
	if fenced, err := subjectFenced(tx); err != nil {
		return Bank{}, err
	} else if fenced {
		return Bank{}, Fail(410, "subject_deleted", "memory subject has been erased")
	}
	if project != "" {
		p, err := s.project(tx, project)
		if err != nil {
			return Bank{}, err
		}
		if p.Deleted {
			return Bank{}, Fail(410, "project_deleted", "project has been deleted")
		}
		if err := s.expire(tx, p); err != nil {
			return Bank{}, err
		}
	}
	on, err := s.enabled(tx, scope, project)
	if err != nil {
		return Bank{}, err
	}
	if !on {
		return Bank{}, Fail(409, "memory_disabled", "requested memory is disabled")
	}
	return s.activeBank(tx, scope, project, false)
}
func (s *Service) Search(ctx context.Context, id Identity, req SearchRequest) (SearchResult, error) {
	scopes := []Scope{Scope(req.Scope)}
	if req.Scope == "both" {
		scopes = []Scope{Personal, Project}
	}
	if req.Query == "" || len(req.Query) > 8192 || (req.Scope != "both" && !Scope(req.Scope).Valid()) || ((req.Scope == "both" || req.Scope == string(Project)) && req.ProjectID == "") {
		return SearchResult{}, Fail(400, "invalid_search", "query and valid scope/current project are required")
	}
	result := SearchResult{Facts: []Fact{}}
	banks := []Bank{}
	err := s.updateIdentity(ctx, id, func(tx Tx) error {
		for _, scope := range scopes {
			b, err := s.snapshot(tx, scope, req.ProjectID)
			if errors.Is(err, ErrMissing) {
				continue
			}
			if err != nil {
				return err
			}
			banks = append(banks, b)
		}
		return nil
	})
	if err != nil {
		return result, err
	}
	labels := []string{}
	for _, b := range banks {
		budget := 2400
		if b.Scope == Personal {
			budget = 800
		}
		facts, err := s.Engine.Recall(ctx, b, req.Query, budget)
		if err != nil {
			var domain *Error
			if errors.As(err, &domain) && domain.Status < 500 {
				return result, err
			}
			result.Degraded = append(result.Degraded, b.Scope)
			continue
		}
		valid := false
		err = s.updateIdentity(ctx, id, func(tx Tx) error {
			latest, err := s.snapshot(tx, b.Scope, req.ProjectID)
			if errors.Is(err, ErrMissing) {
				return nil
			}
			if err != nil {
				return err
			}
			valid = latest.ID == b.ID && latest.Revision == b.Revision
			return nil
		})
		if err != nil {
			return SearchResult{}, err
		}
		if !valid {
			continue
		}
		labels = append(labels, b.Labels...)
		for _, f := range facts {
			result.Facts = append(result.Facts, Fact{Ref: b.ID + "." + f.ID, Scope: b.Scope, Text: f.Text, Type: f.Type, MentionedAt: f.MentionedAt, Metadata: f.Metadata})
		}
	}
	if len(result.Facts) > 0 {
		if err := s.Access.Bind(ctx, id, unique(labels)); err != nil {
			return SearchResult{}, err
		}
	}
	return result, nil
}
func unique(in []string) []string {
	m := map[string]bool{}
	for _, v := range in {
		m[v] = true
	}
	out := make([]string, 0, len(m))
	for v := range m {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func (s *Service) Correct(ctx context.Context, id Identity, key, ref string, req CorrectRequest, invalidate bool) (Operation, error) {
	var out Operation
	if !req.Scope.Valid() {
		return out, Fail(400, "invalid_scope", "scope is required")
	}
	parts := strings.Split(ref, ".")
	if len(parts) != 2 || len(parts[0]) != 36 || len(parts[1]) != 36 {
		return out, Fail(400, "invalid_memory_ref", "invalid memory reference")
	}
	if !invalidate && (strings.TrimSpace(req.Text) == "" || len(req.Text) > 65536) {
		return out, Fail(400, "invalid_correction", "bounded replacement text is required")
	}
	request := struct {
		Ref        string
		Body       CorrectRequest
		Invalidate bool
	}{ref, req, invalidate}
	err := s.mutate(ctx, id, key, "correct", request, func(tx Tx) (any, error) {
		var b Bank
		if err := tx.Get("bank", parts[0], &b); err != nil {
			return nil, err
		}
		if b.Scope != req.Scope {
			return nil, Fail(403, "wrong_scope", "memory does not belong to the requested area")
		}
		if b.Scope == Project && b.ProjectID != req.ProjectID {
			return nil, Fail(403, "wrong_project", "memory does not belong to the current project")
		}
		current, err := s.snapshot(tx, b.Scope, req.ProjectID)
		if err != nil {
			return nil, err
		}
		if current.ID != b.ID {
			return nil, Fail(410, "memory_deleted", "memory generation has been cleared")
		}
		if req.ProjectID != "" {
			if _, err := s.touch(tx, req.ProjectID); err != nil {
				return nil, err
			}
		}
		kind := "correct"
		if invalidate {
			kind = "invalidate"
		}
		j := Job{ID: newID(), Kind: kind, BankID: b.ID, Scope: b.Scope, ProjectID: req.ProjectID, Identity: id, MemoryID: parts[1], Content: req.Text, Reason: req.Reason, State: "queued", CreatedAt: s.Now(), NextAt: s.Now()}
		b.Labels = unique(append(b.Labels, id.Labels...))
		b.Revision++
		if err := tx.Put("bank", b.ID, b); err != nil {
			return nil, err
		}
		if err := tx.Put("job", j.ID, j); err != nil {
			return nil, err
		}
		return operation(j), nil
	}, &out)
	return out, err
}

func (s *Service) Retry(ctx context.Context, id Identity, key, jobID, projectID string, scope Scope) (Operation, error) {
	var out Operation
	err := s.mutate(ctx, id, key, "retry", []string{jobID, projectID, string(scope)}, func(tx Tx) (any, error) {
		var j Job
		if err := tx.Get("job", jobID, &j); err != nil {
			return nil, err
		}
		if j.ProjectID != projectID || j.Scope != scope {
			return nil, Fail(403, "wrong_project", "operation does not belong to the requested area")
		}
		if j.State != "failed" {
			return nil, Fail(409, "not_failed", "only a failed operation can be retried")
		}
		if j.PayloadExpired {
			return nil, Fail(410, "payload_expired", "retry payload expired; submit a new authorized source intent")
		}
		b, err := s.snapshot(tx, j.Scope, j.ProjectID)
		if err != nil {
			return nil, err
		}
		if b.ID != j.BankID {
			return nil, Fail(410, "memory_deleted", "old generations cannot be retried")
		}
		// A new operation UUID is needed if an upstream operation definitively failed.
		j.ID = newID()
		j.Identity = id
		j.State = "queued"
		j.Error = ""
		j.Attempts = 0
		j.Failures = 0
		j.NextAt = s.Now()
		j.LeaseUntil = time.Time{}
		j.LeaseToken = ""
		j.FinishedAt = time.Time{}
		if err := tx.Put("job", j.ID, j); err != nil {
			return nil, err
		}
		if j.CursorID != "" {
			var c Cursor
			if err := tx.Get("cursor", j.CursorID, &c); err != nil {
				return nil, err
			}
			c.JobID = j.ID
			if err := tx.Put("cursor", c.ID, c); err != nil {
				return nil, err
			}
		}
		return operation(j), nil
	}, &out)
	return out, err
}
