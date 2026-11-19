package memory

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// maxTextLen bounds a single memory record's text. Memory records are notes,
// not blobs; oversized text is rejected rather than silently truncated.
const maxTextLen = 1 << 14 // 16 KiB

// dedupCosineThreshold is the cosine similarity at or above which an existing
// record is reported as a near-duplicate of a new proposal.
const dedupCosineThreshold = 0.92

// DedupLimit caps how many near-duplicate hints Propose returns.
const DedupLimit = 8

// ProposeRequest carries the body of a Propose call. The isolation scope is
// never part of the body; it comes from the verified task token.
type ProposeRequest struct {
	Kind           string         `json:"kind"`
	Workspace      string         `json:"workspace,omitempty"`
	Text           string         `json:"text"`
	OpKey          string         `json:"op_key"`
	SourceKind     string         `json:"source_kind,omitempty"`
	Evidence       []Evidence     `json:"evidence,omitempty"`
	Supersedes     string         `json:"supersedes,omitempty"`
	Provenance     map[string]any `json:"provenance,omitempty"`
	ReviewRequired bool           `json:"review_required,omitempty"`
	TTL            *time.Time     `json:"ttl,omitempty"`
}

// SimilarHit is one existing record that may duplicate a new proposal.
type SimilarHit struct {
	ID        string  `json:"id"`
	Kind      string  `json:"kind"`
	Workspace string  `json:"workspace,omitempty"`
	State     string  `json:"state"`
	Score     float64 `json:"score"`
	Exact     bool    `json:"exact"`
}

// ProposeResult is the outcome of a Propose: the stored (proposed) record plus
// de-duplication hints. Propose never lands a record as active.
type ProposeResult struct {
	Memory  Memory       `json:"memory"`
	Similar []SimilarHit `json:"similar,omitempty"`
	Replay  bool         `json:"replay,omitempty"`
}

// fingerprint is the normalized argument set whose digest enforces the
// operation-key contract: the same key with a different fingerprint is a
// KEY_REUSE_MISMATCH, the same key with the same fingerprint is a replay.
type fingerprint struct {
	Op             string         `json:"op"`
	Kind           string         `json:"kind"`
	Workspace      string         `json:"workspace"`
	Text           string         `json:"text"`
	SourceKind     string         `json:"source_kind"`
	Evidence       []Evidence     `json:"evidence,omitempty"`
	Supersedes     string         `json:"supersedes,omitempty"`
	Provenance     map[string]any `json:"provenance,omitempty"`
	ReviewRequired bool           `json:"review_required,omitempty"`
	TTL            *time.Time     `json:"ttl,omitempty"`
}

// Propose writes a new memory as a proposal through the second-brain gates:
// operation-key idempotency, Secret Firewall, the ceiling-derived visibility
// ceiling, evidence/source-kind requirements for shared scopes, and an
// artifact-publication check for evidence artifacts. A record's visibility is
// never wider than the writing task's live ceiling.
func (s *Store) Propose(ctx context.Context, scope contracts.Scope, req ProposeRequest) (ProposeResult, error) {
	if err := scope.Validate(); err != nil {
		return ProposeResult{}, err
	}
	kind, workspace, err := normalizeKindScope(req.Kind, req.Workspace)
	if err != nil {
		return ProposeResult{}, err
	}
	if req.OpKey == "" {
		return ProposeResult{}, contracts.NewError(contracts.ErrInvalid, "memory: op_key is required for every mutation")
	}
	if !idRe.MatchString(req.OpKey) {
		return ProposeResult{}, contracts.NewError(contracts.ErrInvalid, "memory: op_key is malformed")
	}
	text := req.Text
	if text == "" {
		return ProposeResult{}, contracts.NewError(contracts.ErrInvalid, "memory: text must not be empty")
	}
	if len(text) > maxTextLen {
		return ProposeResult{}, contracts.NewError(contracts.ErrInvalid, "memory: text exceeds the size limit")
	}
	if req.Supersedes != "" && !idRe.MatchString(req.Supersedes) {
		return ProposeResult{}, contracts.NewError(contracts.ErrInvalid, "memory: supersedes id is malformed")
	}

	fp := fingerprint{Op: "propose", Kind: kind, Workspace: workspace, Text: text, SourceKind: req.SourceKind, Evidence: req.Evidence, Supersedes: req.Supersedes, Provenance: req.Provenance, ReviewRequired: req.ReviewRequired, TTL: req.TTL}
	opDigest, err := contracts.Digest(fp)
	if err != nil {
		return ProposeResult{}, err
	}
	textDigest, err := contracts.Digest(text)
	if err != nil {
		return ProposeResult{}, err
	}
	// Source labels describe sensitivity, not current project membership. A
	// revoked member may still carry a valid task grant and old workspace
	// labels; neither authorizes creating a new proposal in that workspace.
	if kind == KindWorkspace {
		memberships, err := s.callerWorkspaces(ctx, scope)
		if err != nil {
			return ProposeResult{}, err
		}
		member := false
		for _, current := range memberships {
			if current == workspace {
				member = true
				break
			}
		}
		if !member {
			return ProposeResult{}, contracts.NewError(contracts.ErrForbidden, "memory: current workspace membership is required")
		}
	}

	// Secret Firewall: a secret in the live ceiling or in any evidence taint
	// means the fact may not enter memory at all (CheckWrite denies every sink
	// for secret data), so refuse before storing.
	ceiling, err := s.liveCeiling(ctx, scope)
	if err != nil {
		return ProposeResult{}, err
	}
	if hasSecret(ceiling.Taints) {
		return ProposeResult{}, contracts.NewError(contracts.ErrForbidden, "memory: secret-tainted source may not be stored")
	}
	for _, ev := range req.Evidence {
		if hasSecret(ev.Taints) {
			return ProposeResult{}, contracts.NewError(contracts.ErrForbidden, "memory: secret-tainted evidence may not be stored")
		}
	}
	if replay, found, err := s.proposalReplay(ctx, scope, req.OpKey, opDigest); found || err != nil {
		return replay, err
	}
	if req.Supersedes != "" {
		previous, err := s.Get(ctx, scope, req.Supersedes)
		if err != nil {
			return ProposeResult{}, err
		}
		if !sameMemoryNamespace(previous, Memory{Kind: kind, Workspace: workspace, Subject: scope.Subject, Deployment: scope.Deployment}) {
			return ProposeResult{}, contracts.NewError(contracts.ErrForbidden, "memory: correction cannot change its source namespace")
		}
		if previous.State != StateActive {
			return ProposeResult{}, contracts.NewError(contracts.ErrConflict, "memory: correction requires an active source")
		}
		ceiling = labels.Merge(ceiling, previous.Labels)
	}

	recordLabels, err := s.deriveLabels(ctx, scope, kind, workspace, ceiling)
	if err != nil {
		return ProposeResult{}, err
	}

	// Evidence gate: a record that leaves its author (workspace/agent scopes)
	// must cite a source kind and at least one evidence entry.
	if kind != KindUser {
		if err := requireEvidence(req); err != nil {
			return ProposeResult{}, err
		}
	}

	// Evidence-artifact publication check: an evidence artifact may not be
	// laundered into a memory whose audience is wider than the artifact allows.
	sink := audienceByKind(kind, workspace)
	if isPrivateToSubj(recordLabels, scope.Subject) {
		sink = labels.Audience{Kind: labels.KindPrivate, Subject: scope.Subject}
	}
	if evArtifacts := evidenceArtifactIDs(req.Evidence); len(evArtifacts) > 0 {
		if s.art == nil {
			return ProposeResult{}, contracts.NewError(contracts.ErrForbidden, "memory: evidence artifacts cannot be verified (no artifact store)")
		}
		if err := s.art.CheckPublication(ctx, scope, evArtifacts, sink); err != nil {
			return ProposeResult{}, err
		}
	}

	// review_required: explicitly requested, or forced by an external/untrusted
	// taint on the ceiling or evidence.
	reviewRequired := req.ReviewRequired || hasExternalTaintAny(ceiling.Taints) || evidenceHasExternal(req.Evidence)

	var embedding []float32
	if s.embedder != nil {
		embedding, err = s.embed(ctx, scope, text, labels.Merge(ceiling, recordLabels))
		if err != nil {
			return ProposeResult{}, err
		}
		if err := s.checkEmbedding(embedding); err != nil {
			return ProposeResult{}, err
		}
	}

	// De-duplication hints computed before the insert so they are reported even
	// on the (idempotent) replay path is skipped; here they belong to a new write.
	similar, err := s.findSimilar(ctx, scope, kind, workspace, textDigest, embedding)
	if err != nil {
		return ProposeResult{}, err
	}

	created, replay, err := s.insertProposal(ctx, scope, kind, workspace, text, textDigest, embedding, recordLabels, req, opDigest, reviewRequired)
	if err != nil {
		return ProposeResult{}, err
	}
	if replay {
		// Replay reads an existing source, whose labels may have narrowed
		// since the original write. Recheck it and taint this reading task.
		current, err := s.Get(ctx, scope, created.ID)
		if err != nil {
			return ProposeResult{}, err
		}
		return ProposeResult{Memory: current, Replay: true}, nil
	}
	return ProposeResult{Memory: created, Similar: similar}, nil
}

// insertProposal writes the proposal row, applying operation-key idempotency.
// On a unique-key collision it compares fingerprints: an identical fingerprint
// is a replay of the same operation, a different one is a key reuse error.
func (s *Store) insertProposal(
	ctx context.Context,
	scope contracts.Scope,
	kind, workspace, text, textDigest string,
	embedding []float32,
	recordLabels labels.Set,
	req ProposeRequest,
	opDigest string,
	reviewRequired bool,
) (Memory, bool, error) {
	id := contracts.NewID()
	evDoc, err := json.Marshal(nonNilEvidence(req.Evidence))
	if err != nil {
		return Memory{}, false, contracts.NewError(contracts.ErrInvalid, "memory: evidence is not serialisable")
	}
	prov := req.Provenance
	if prov == nil {
		prov = map[string]any{}
	}
	provDoc, err := json.Marshal(prov)
	if err != nil {
		return Memory{}, false, contracts.NewError(contracts.ErrInvalid, "memory: provenance is not serialisable")
	}
	labelDoc, err := json.Marshal(recordLabels)
	if err != nil {
		return Memory{}, false, contracts.NewError(contracts.ErrInvalid, "memory: labels are not serialisable")
	}
	var vecArg any
	if embedding != nil {
		vecArg = serializeVector(embedding)
	}

	tx, prior, _, replay, err := s.beginMutation(ctx, scope, req.OpKey, opDigest)
	if err != nil {
		return Memory{}, false, err
	}
	defer tx.Rollback(ctx)
	if err = denyErasedTenant(ctx, tx, scope.Tenant); err != nil {
		return Memory{}, false, err
	}
	if replay {
		existing, err := readMutationMemory(ctx, tx, scope.Tenant, prior)
		return existing, true, err
	}
	if err = lockSubjectMutation(ctx, tx, scope.Tenant, scope.Subject); err != nil {
		return Memory{}, false, err
	}
	var erased bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_memory.erased_subjects WHERE tenant=$1 AND subject=$2)`, scope.Tenant, scope.Subject).Scan(&erased); err != nil {
		return Memory{}, false, err
	}
	if erased {
		return Memory{}, false, contracts.NewError(contracts.ErrForbidden, "memory: subject was erased")
	}
	_, err = tx.Exec(ctx, `
INSERT INTO ahp_memory.memories
    (id, tenant, deployment, kind, workspace, subject, text, text_digest, embedding,
     state, op_key, op_digest, source_kind, evidence, supersedes, review_required,
     provenance, labels, ttl, embedding_space)
VALUES
    ($1,$2,$3,$4,$5,$6,$7,$8,$9::vector,
     'proposed',$10,$11,$12,$13::jsonb,$14,$15,$16::jsonb,$17::jsonb,$18,$19)`,
		id, scope.Tenant, scope.Deployment, kind, workspace, scope.Subject, text, textDigest, vecArg,
		req.OpKey, opDigest, req.SourceKind, evDoc, req.Supersedes, reviewRequired,
		provDoc, labelDoc, req.TTL, s.embeddingSpace)
	if err != nil {
		return Memory{}, false, storageError("insert memory proposal", err)
	}
	created, err := readMutationMemory(ctx, tx, scope.Tenant, id)
	if err != nil {
		return Memory{}, false, err
	}
	if err = finishMutation(ctx, tx, scope, req.OpKey, id, 0); err != nil {
		return Memory{}, false, err
	}
	return created, false, nil
}

// deriveLabels returns the stored visibility labels for a record, which are the
// narrowest of the kind's default audience and the writing task's live ceiling.
// A private ceiling may never enter scope agent; into a workspace scope it is
// downgraded to private_to_user (the PDF's "private source yields private_to_user
// even inside a project category").
func (s *Store) deriveLabels(ctx context.Context, scope contracts.Scope, kind, workspace string, ceiling labels.Set) (labels.Set, error) {
	private := privateCeiling(ceiling)
	switch kind {
	case KindAgent:
		// Agent scope is shared beyond any one workspace. Validate the
		// complete ceiling before storing it: absence of Subjects does not
		// authorize dropping workspace restrictions accumulated by a read.
		if err := labels.CheckWrite(ceiling, labels.Audience{Kind: labels.KindTenant}); err != nil {
			return labels.Set{}, err
		}
		return labels.Merge(ceiling), nil
	case KindWorkspace:
		if private {
			// Downgrade: private_to_user even in the project rubric. Refuse if
			// the ceiling is private to someone other than the writer.
			if err := labels.CheckWrite(ceiling, labels.Audience{Kind: labels.KindPrivate, Subject: scope.Subject}); err != nil {
				return labels.Set{}, contracts.NewError(contracts.ErrForbidden,
					"memory: a private ceiling may not be written under this subject")
			}
			return labels.Merge(labels.Set{Subjects: []string{scope.Subject}}, labels.Set{Taints: ceiling.Taints}), nil
		}
		if err := labels.CheckWrite(ceiling, labels.Audience{Kind: labels.KindWorkspace, Workspace: workspace}); err != nil {
			return labels.Set{}, err
		}
		return labels.Merge(labels.Set{Workspaces: []string{workspace}}, labels.Set{Taints: ceiling.Taints}), nil
	default: // KindUser
		// A user-scope record is always private to its subject.
		if err := labels.CheckWrite(ceiling, labels.Audience{Kind: labels.KindPrivate, Subject: scope.Subject}); err != nil {
			return labels.Set{}, err
		}
		return labels.Merge(labels.Set{Subjects: []string{scope.Subject}}, labels.Set{Taints: ceiling.Taints}), nil
	}
}

// normalizeKindScope validates the kind and the per-kind workspace constraint,
// returning the canonical kind and workspace (empty for non-workspace scopes).
func normalizeKindScope(kind, workspace string) (string, string, error) {
	switch kind {
	case KindUser:
		if workspace != "" {
			return "", "", contracts.NewError(contracts.ErrInvalid, "memory: user scope must not carry a workspace")
		}
		return KindUser, "", nil
	case KindWorkspace:
		if workspace == "" {
			return "", "", contracts.NewError(contracts.ErrInvalid, "memory: workspace scope requires a workspace")
		}
		if !idRe.MatchString(workspace) {
			return "", "", contracts.NewError(contracts.ErrInvalid, "memory: workspace name is malformed")
		}
		return KindWorkspace, workspace, nil
	case KindAgent:
		if workspace != "" {
			return "", "", contracts.NewError(contracts.ErrInvalid, "memory: agent scope must not carry a workspace")
		}
		return KindAgent, "", nil
	default:
		return "", "", contracts.NewError(contracts.ErrInvalid, "memory: unknown kind "+kind)
	}
}

func requireEvidence(req ProposeRequest) error {
	if len(req.Evidence) == 0 {
		return contracts.NewError(contracts.ErrForbidden, "memory: shared scopes require evidence")
	}
	for _, ev := range req.Evidence {
		if ev.SourceKind == "" {
			return contracts.NewError(contracts.ErrForbidden, "memory: evidence requires a source_kind")
		}
	}
	return nil
}

func evidenceArtifactIDs(evs []Evidence) []string {
	var out []string
	seen := map[string]struct{}{}
	for _, ev := range evs {
		for _, a := range ev.Artifacts {
			if a == "" {
				continue
			}
			if _, dup := seen[a]; dup {
				continue
			}
			seen[a] = struct{}{}
			out = append(out, a)
		}
	}
	return out
}

func evidenceHasExternal(evs []Evidence) bool {
	for _, ev := range evs {
		if hasExternalTaintAny(ev.Taints) {
			return true
		}
	}
	return false
}

func hasExternalTaintAny(taints []string) bool {
	for _, t := range taints {
		if t == labels.TaintExternal || t == labels.TaintUntrustedExternal {
			return true
		}
	}
	return false
}

func nonNilEvidence(evs []Evidence) []Evidence {
	if evs == nil {
		return []Evidence{}
	}
	return evs
}

// findSimilar reports existing active/proposed records that duplicate the new
// text: exact text-digest matches first, then near-duplicates by cosine.
func (s *Store) findSimilar(ctx context.Context, scope contracts.Scope, kind, workspace, textDigest string, embedding []float32) ([]SimilarHit, error) {
	ws, err := s.callerWorkspaces(ctx, scope)
	if err != nil {
		return nil, err
	}
	where, args := s.admissible(scope, ws, kind)
	// A writer can see their own pending duplicate, but nobody else's proposal.
	where = strings.Replace(where, "state='active'", "(state='active' OR (state='proposed' AND subject=$2))", 1)
	args = append(args, workspace, textDigest)
	w, d := len(args)-1, len(args)
	score := "0::float8"
	near := "false"
	if embedding != nil {
		args = append(args, serializeVector(embedding), s.embeddingSpace)
		v, space := len(args)-1, len(args)
		score = "(1-(embedding <=> $" + itoa(v) + "::vector))::float8"
		near = "(embedding IS NOT NULL AND embedding_space=$" + itoa(space) + " AND " + score + ">=0.92)"
	}
	q := `SELECT id,kind,workspace,state,CASE WHEN text_digest=$` + itoa(d) + ` THEN 1::float8 ELSE ` + score + ` END,text_digest=$` + itoa(d) + `
 FROM ahp_memory.memories WHERE ` + where + ` AND workspace=$` + itoa(w) + ` AND (text_digest=$` + itoa(d) + ` OR ` + near + `)
 ORDER BY (text_digest=$` + itoa(d) + `) DESC,5 DESC,id LIMIT ` + itoa(DedupLimit)
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, storageError("dedup candidates", err)
	}
	defer rows.Close()
	var out []SimilarHit
	for rows.Next() {
		var h SimilarHit
		if err := rows.Scan(&h.ID, &h.Kind, &h.Workspace, &h.State, &h.Score, &h.Exact); err != nil {
			return nil, storageError("dedup scan", err)
		}
		out = append(out, h)
	}
	if err := rows.Err(); err != nil {
		return nil, storageError("dedup rows", err)
	}
	// The candidate list is not authority: refresh membership and source labels.
	var visible []SimilarHit
	for _, h := range out {
		m, err := s.getRaw(ctx, scope.Tenant, h.ID)
		if err != nil {
			return nil, err
		}
		ws, err := s.callerWorkspaces(ctx, scope)
		if err != nil {
			return nil, err
		}
		if (m.State == StateActive || (m.State == StateProposed && m.Subject == scope.Subject)) &&
			(m.TTL == nil || m.TTL.After(time.Now())) && s.callerCanRead(ctx, m, scope, ws) {
			visible = append(visible, h)
		}
	}
	return visible, nil
}

// isPrivateToSubj reports whether a record's labels restrict it to exactly this
// subject (a private_to_user record). Such a record may only be checked against
// a private audience for that subject during the evidence-publication gate.
func isPrivateToSubj(set labels.Set, subject string) bool {
	if len(set.Subjects) == 0 {
		return false
	}
	for _, s := range set.Subjects {
		if s != subject {
			return false
		}
	}
	return true
}
