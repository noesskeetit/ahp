package memory

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// Search request/response types. The isolation scope always comes from the
// verified task token, never the body.

// SearchRequest is the body of a Search. Query may be empty (then ranking is
// recency-neutral and the caller simply enumerates admissible active records).
// Kind optionally narrows to a single scope; Limit caps returned hits.
type SearchRequest struct {
	Query     string `json:"query,omitempty"`
	Kind      string `json:"kind,omitempty"`
	Limit     int    `json:"limit,omitempty"`
	Workspace string `json:"workspace,omitempty"`
}

// SearchHit is one readable memory together with its fusion rank score.
type SearchHit struct {
	Memory Memory  `json:"memory"`
	Score  float64 `json:"score"`
}

// SearchResult is the ranked, rights-rechecked result set.
type SearchResult struct {
	Hits []SearchHit `json:"hits"`
	Mode string      `json:"mode"`
}

const (
	searchCandLimit = 200
	searchDefaultN  = 10
	searchMaxN      = 50
	rrfK            = 60.0
)

// getRaw reads a memory by (tenant, id) with no audience check. It is internal:
// callers must run the emission check before returning a record externally.
func (s *Store) getRaw(ctx context.Context, tenant, id string) (Memory, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+columns+` FROM ahp_memory.memories WHERE tenant=$1 AND id=$2 AND NOT EXISTS(SELECT 1 FROM ahp_memory.erased_tenants WHERE tenant=$1)`, tenant, id)
	m, err := scanMemory(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Memory{}, contracts.NewError(contracts.ErrNotFound, "memory: not found")
	}
	if err != nil {
		return Memory{}, storageError("get memory", err)
	}
	return m, nil
}

// Get reads a single memory by id if — and only if — the caller may read it.
// An unreadable id and a non-existent id both return NOT_FOUND so the store is
// not an existence oracle.
func (s *Store) Get(ctx context.Context, scope contracts.Scope, id string) (Memory, error) {
	return s.getChecked(ctx, scope, id, "")
}

// GetInWorkspace checks the selected project before emitting either data or
// source labels. A caller's access to another project cannot widen this read.
func (s *Store) GetInWorkspace(ctx context.Context, scope contracts.Scope, id, workspace string) (Memory, error) {
	if workspace == "" || !idRe.MatchString(workspace) {
		return Memory{}, contracts.NewError(contracts.ErrInvalid, "memory: workspace is required")
	}
	return s.getChecked(ctx, scope, id, workspace)
}

func (s *Store) getChecked(ctx context.Context, scope contracts.Scope, id, workspace string) (Memory, error) {
	if err := scope.Validate(); err != nil {
		return Memory{}, err
	}
	if !idRe.MatchString(id) {
		return Memory{}, contracts.NewError(contracts.ErrNotFound, "memory: not found")
	}
	m, err := s.getRaw(ctx, scope.Tenant, id)
	if err != nil {
		return Memory{}, err
	}
	if workspace != "" && (m.Kind != KindWorkspace || m.Workspace != workspace) {
		return Memory{}, contracts.NewError(contracts.ErrNotFound, "memory: not found")
	}
	if m.State == StateDeleted || (m.TTL != nil && !m.TTL.After(time.Now())) {
		return Memory{}, contracts.NewError(contracts.ErrNotFound, "memory: not found")
	}
	ws, err := s.callerWorkspaces(ctx, scope)
	if err != nil {
		return Memory{}, err
	}
	if !s.callerCanRead(ctx, m, scope, ws) {
		return Memory{}, contracts.NewError(contracts.ErrNotFound, "memory: not found")
	}
	// Active facts are ordinary reads. An author may inspect their own
	// pending proposal; other inactive records require an explicit reviewer
	// capability in addition to (never instead of) the source visibility ACL.
	if m.State != StateActive && !(m.State == StateProposed && m.Subject == scope.Subject) {
		if err := s.authorize(ctx, scope, CapabilityReview, "memory:"+id); err != nil {
			return Memory{}, contracts.NewError(contracts.ErrNotFound, "memory: not found")
		}
	}
	if s.readLabels != nil {
		if err := s.readLabels(ctx, scope, m.Labels); err != nil {
			return Memory{}, err
		}
	}

	return m, nil
}

// Search runs the hybrid (lexical + pgvector) pipeline: first the admissible
// set is narrowed (tenant, owner, live workspace membership, discoverable
// state, live TTL), then lexical and vector candidates are fused with RRF, then
// every emitted hit has its rights re-checked. The index never grants read on
// the source.
func (s *Store) Search(ctx context.Context, scope contracts.Scope, req SearchRequest) (SearchResult, error) {
	if err := scope.Validate(); err != nil {
		return SearchResult{}, err
	}
	if req.Kind != "" {
		if req.Kind != KindUser && req.Kind != KindWorkspace && req.Kind != KindAgent {
			return SearchResult{}, contracts.NewError(contracts.ErrInvalid, "memory: search kind is malformed")
		}
	}
	if req.Workspace != "" {
		if !idRe.MatchString(req.Workspace) || req.Kind != "" && req.Kind != KindWorkspace {
			return SearchResult{}, contracts.NewError(contracts.ErrInvalid, "memory: workspace filter requires workspace scope")
		}
		req.Kind = KindWorkspace
	}
	n := req.Limit
	switch {
	case n <= 0:
		n = searchDefaultN
	case n > searchMaxN:
		n = searchMaxN
	}

	callerWS, err := s.callerWorkspaces(ctx, scope)
	if err != nil {
		return SearchResult{}, err
	}
	if callerWS == nil {
		callerWS = []string{}
	}
	if req.Workspace != "" {
		member := false
		for _, workspace := range callerWS {
			if workspace == req.Workspace {
				member = true
				break
			}
		}
		if !member {
			return SearchResult{}, contracts.NewError(contracts.ErrForbidden, "memory: current workspace membership is required")
		}
		// Narrow the owner-side corpus before lexical/vector/graph candidates,
		// ranking statistics and label emission. Never filter a broad result in
		// the agent after the other projects' labels have already tainted it.
		callerWS = []string{req.Workspace}
	}

	// RRF fusion over ordered candidate lists.
	type cand struct {
		id    string
		score float64
	}
	scores := map[string]float64{}

	mode := "lexical_graph"
	if s.embedder != nil {
		mode = "hybrid"
	}
	query := strings.TrimSpace(req.Query)
	if len(query) > maxTextLen {
		return SearchResult{}, contracts.NewError(contracts.ErrInvalid, "memory: search query exceeds size limit")
	}
	if query != "" {
		// BM25 statistics are computed only over the admissible corpus.
		lexIDs, lexErr := s.lexCandidates(ctx, scope, callerWS, req.Kind, query)
		if lexErr != nil {
			return SearchResult{}, lexErr
		}
		for i, id := range lexIDs {
			scores[id] += 1.0 / (rrfK + float64(i+1))
		}
		// Vector signal (pgvector cosine) when an embedder is wired.
		if s.embedder != nil {
			ceiling, err := s.liveCeiling(ctx, scope)
			if err != nil {
				return SearchResult{}, err
			}
			vec, eerr := s.embed(ctx, scope, query, ceiling)
			if eerr != nil {
				return SearchResult{}, eerr
			}
			if err := s.checkEmbedding(vec); err != nil {
				return SearchResult{}, err
			}
			vecIDs, verr := s.vectorCandidates(ctx, scope, callerWS, req.Kind, serializeVector(vec))
			if verr != nil {
				return SearchResult{}, verr
			}
			for i, id := range vecIDs {
				scores[id] += 1.0 / (rrfK + float64(i+1))
			}
		}
		// One-hop graph over evidence artifacts and explicit supersession links.
		seeds := make([]string, 0, len(scores))
		for id := range scores {
			seeds = append(seeds, id)
		}
		if len(seeds) > 0 {
			ids, err := s.graphCandidates(ctx, scope, callerWS, req.Kind, seeds)
			if err != nil {
				return SearchResult{}, err
			}
			for i, id := range ids {
				scores[id] += 1.0 / (rrfK + float64(i+1))
			}
		}
	} else {
		mode = "browse"
		// No query text: enumerate admissible active records, recency ordered.
		ids, eerr := s.recentCandidates(ctx, scope, callerWS, req.Kind, searchCandLimit)
		if eerr != nil {
			return SearchResult{}, eerr
		}
		for i, id := range ids {
			scores[id] += 1.0 / (rrfK + float64(i+1))
		}
	}

	if len(scores) == 0 {
		return SearchResult{Hits: []SearchHit{}, Mode: mode}, nil
	}

	cands := make([]cand, 0, len(scores))
	for id, sc := range scores {
		cands = append(cands, cand{id: id, score: sc})
	}
	sort.SliceStable(cands, func(i, j int) bool {
		if cands[i].score != cands[j].score {
			return cands[i].score > cands[j].score
		}
		return cands[i].id < cands[j].id
	})

	hits := make([]SearchHit, 0, n)
	for _, c := range cands {
		if len(hits) >= n {
			break
		}
		m, gerr := s.getRaw(ctx, scope.Tenant, c.id)
		if gerr != nil {
			if contracts.Code(gerr) == contracts.ErrNotFound {
				continue
			}
			return SearchResult{}, gerr
		}
		if m.State != StateActive || (m.TTL != nil && !m.TTL.After(time.Now())) {
			continue
		}
		if req.Workspace != "" && (m.Kind != KindWorkspace || m.Workspace != req.Workspace) {
			continue
		}
		liveWS, err := s.callerWorkspaces(ctx, scope)
		if err != nil {
			return SearchResult{}, err
		}
		if !s.callerCanRead(ctx, m, scope, liveWS) {
			continue // rights re-check at emission
		}
		hits = append(hits, SearchHit{Memory: m, Score: c.score})
	}
	if hits == nil {
		hits = []SearchHit{}
	}
	if s.readLabels != nil {
		var set labels.Set
		for _, hit := range hits {
			set = labels.Merge(set, hit.Memory.Labels)
		}
		if len(hits) > 0 {
			if err := s.readLabels(ctx, scope, set); err != nil {
				return SearchResult{}, err
			}
		}
	}
	return SearchResult{Hits: hits, Mode: mode}, nil
}

// admissible is the shared coarse right-to-read narrowing: tenant + live TTL +
// (own user records | active agent records | active workspace records the caller
// still belongs to). It is defense-in-depth; the per-record emission check is
// authoritative.
func (s *Store) admissible(scope contracts.Scope, callerWS []string, kind string) (string, []any) {
	if callerWS == nil {
		callerWS = []string{}
	}
	args := []any{scope.Tenant, scope.Subject, callerWS, scope.Deployment}
	where := `tenant=$1 AND state='active' AND (ttl IS NULL OR ttl > now())
 AND NOT EXISTS (SELECT 1 FROM ahp_memory.erased_tenants et WHERE et.tenant=$1)
 AND ((kind='user' AND subject=$2) OR (kind='agent' AND deployment=$4)
      OR (kind='workspace' AND workspace=ANY($3)))
 AND NOT COALESCE(NULLIF(labels->'taints','null'::jsonb),'[]'::jsonb) ? 'secret'
 AND (CASE
  WHEN jsonb_array_length(COALESCE(NULLIF(labels->'subjects','null'::jsonb),'[]'::jsonb))>0
   THEN labels->'subjects' = jsonb_build_array($2::text)
  WHEN jsonb_array_length(COALESCE(NULLIF(labels->'workspaces','null'::jsonb),'[]'::jsonb))>0
   THEN EXISTS (SELECT 1 FROM jsonb_array_elements_text(labels->'workspaces') w WHERE w=ANY($3))
  ELSE true END)`
	if kind != "" {
		args = append(args, kind)
		where += " AND kind=$5"
	}
	return where, args
}

// BM25 uses k1=1.2 and b=0.75, with document frequencies and lengths from the
// authorized corpus. Hidden documents cannot affect either rank or candidate budget.
func (s *Store) lexCandidates(ctx context.Context, scope contracts.Scope, callerWS []string, kind, query string) ([]string, error) {
	where, args := s.admissible(scope, callerWS, kind)
	args = append(args, query)
	q := `WITH allowed AS MATERIALIZED (SELECT id,search FROM ahp_memory.memories WHERE ` + where + `),
 terms AS (SELECT unnest(tsvector_to_array(to_tsvector('simple',$` + itoa(len(args)) + `))) term),
 tokens AS MATERIALIZED (SELECT id,lexeme,cardinality(positions)::float8 tf FROM allowed CROSS JOIN LATERAL unnest(search)),
 lengths AS (SELECT id,sum(tf) dl FROM tokens GROUP BY id),
 stats AS (SELECT count(*)::float8 n,avg(dl) avgdl FROM lengths),
 matched AS (SELECT tokens.* FROM tokens JOIN terms ON lexeme=term),
 freq AS (SELECT lexeme,count(*)::float8 df FROM matched GROUP BY lexeme)
 SELECT m.id FROM matched m JOIN freq f USING(lexeme) JOIN lengths l USING(id) CROSS JOIN stats s
 GROUP BY m.id
 ORDER BY sum(ln(1+(s.n-f.df+0.5)/(f.df+0.5)) * m.tf*2.2/(m.tf+1.2*(0.25+0.75*l.dl/s.avgdl))) DESC,m.id
 LIMIT ` + itoa(searchCandLimit)
	return s.queryIDs(ctx, q, args, "BM25 candidates")
}

// The graph is derived from canonical provenance, not a second permission store.
// Only visible endpoints participate; deletion, TTL and label changes invalidate
// their edges in the same statement as candidate selection.
func (s *Store) graphCandidates(ctx context.Context, scope contracts.Scope, callerWS []string, kind string, seeds []string) ([]string, error) {
	where, args := s.admissible(scope, callerWS, kind)
	args = append(args, seeds)
	q := `WITH allowed AS MATERIALIZED (SELECT id,supersedes,evidence FROM ahp_memory.memories WHERE ` + where + `),
 edges AS (SELECT id,artifact FROM allowed CROSS JOIN LATERAL jsonb_array_elements(evidence) ev
   CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(NULLIF(ev->'artifacts','null'::jsonb),'[]'::jsonb)) artifact),
 neighbors AS (
 SELECT a.id,b.id seed FROM edges a JOIN edges b USING(artifact) WHERE a.id<>b.id
 UNION SELECT a.id,b.id FROM allowed a JOIN allowed b ON a.supersedes=b.id OR b.supersedes=a.id)
 SELECT id FROM neighbors WHERE seed=ANY($` + itoa(len(args)) + `) GROUP BY id ORDER BY count(*) DESC,id LIMIT ` + itoa(searchCandLimit)
	return s.queryIDs(ctx, q, args, "graph candidates")
}

func (s *Store) vectorCandidates(ctx context.Context, scope contracts.Scope, callerWS []string, kind, vec string) ([]string, error) {
	where, args := s.admissible(scope, callerWS, kind)
	vIdx := len(args) + 1
	args = append(args, vec, s.embeddingSpace)
	q := `SELECT id FROM ahp_memory.memories
	      WHERE ` + where + ` AND embedding IS NOT NULL AND embedding_space=$` + itoa(vIdx+1) + `
	      ORDER BY embedding <=> $` + itoa(vIdx) + `::vector
	      LIMIT ` + itoa(searchCandLimit)
	return s.queryIDs(ctx, q, args, "vector candidates")
}

func (s *Store) recentCandidates(ctx context.Context, scope contracts.Scope, callerWS []string, kind string, limit int) ([]string, error) {
	where, args := s.admissible(scope, callerWS, kind)
	q := `SELECT id FROM ahp_memory.memories
	      WHERE ` + where + `
	      ORDER BY created_at DESC
	      LIMIT ` + itoa(limit)
	return s.queryIDs(ctx, q, args, "recent candidates")
}

func (s *Store) queryIDs(ctx context.Context, q string, args []any, op string) ([]string, error) {
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, storageError(op, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, storageError(op+" scan", err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		return nil, storageError(op+" rows", err)
	}
	return out, nil
}

// callerCanRead is the authoritative emission gate. A caller may read a record
// iff its labels admit one of: a private audience for that caller, a workspace
// audience for a workspace the caller still belongs to, or a tenant audience.
// This is evaluated per record, so a private fact downgraded into a workspace
// rubric (labels carry Subjects:[writer]) is invisible to every other member.
func (s *Store) callerCanRead(ctx context.Context, m Memory, scope contracts.Scope, callerWS []string) bool {
	_ = ctx
	if m.Kind == KindAgent && m.Deployment != scope.Deployment {
		return false
	}
	if m.Kind == KindUser && m.Subject != scope.Subject {
		return false
	}
	if m.Kind == KindWorkspace {
		member := false
		for _, w := range callerWS {
			if w == m.Workspace {
				member = true
			}
		}
		if !member {
			return false
		}
	}
	if hasSecret(m.Labels.Taints) {
		return false
	}
	// The record's restrictive class is derived explicitly rather than probed
	// through CheckWrite's Private audience: CheckWrite admits a workspace-
	// restricted Set into any private audience (IAM verifies source access
	// separately), so probing it here would let any caller read a workspace
	// memory by direct Get. We therefore classify by the labels themselves.
	subjects := distinctStrings(m.Labels.Subjects)
	workspaces := distinctStrings(m.Labels.Workspaces)
	switch {
	case len(subjects) == 1:
		// private_to_user: only the sole subject may read, even a co-member.
		return subjects[0] == scope.Subject
	case len(subjects) > 1:
		// Restricted to multiple subjects: no single caller may read.
		return false
	case len(workspaces) > 0:
		// Workspace-restricted (not private-downgraded): a live member of one
		// of the recorded workspaces may read; membership is re-checked live.
		for _, ws := range callerWS {
			for _, w := range workspaces {
				if ws != "" && ws == w {
					return true
				}
			}
		}
		return false
	default:
		// No subject/workspace restriction: tenant-wide (agent / shared). An
		// external taint is not auto-readable into a trusted sink.
		return labels.CheckWrite(m.Labels, labels.Audience{Kind: labels.KindTenant, AllowExternal: true}) == nil
	}
}

// itoa keeps the dynamic query builders terse.
func itoa(n int) string { return strconv.Itoa(n) }
