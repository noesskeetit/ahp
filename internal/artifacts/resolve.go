package artifacts

// ResolveByDigest answers the one question the human edge cannot answer for itself:
// a person knows the digest of the file they were shown (the run's frame names it)
// but not the artifact id and the sealed version that the download route demands
// ("текущие права, точная версия"). PDF rev 3.2 keeps the download contract
// version-exact on purpose, so the missing piece is a read-only lookup, not a
// weaker download.
//
// Two rules decide the shape of this file:
//
//   - the answer comes from the SAME read gate as a read of the artifact itself
//     (Store.load with the label-denial mask on), so "the digest exists but you may
//     not see it" and "no such digest" are the same answer. A lookup that answered
//     FORBIDDEN for one and NOT_FOUND for the other would turn the human edge into
//     an existence oracle over the whole tenant;
//   - the tenant comes from the verified scope, never from the request. There is no
//     tenant parameter to pass, so there is no way to ask about somebody else's.

import (
	"context"
	"regexp"
	"sort"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// ReasonDigestRequired is this operation's own refusal: a lookup without a digest is
// a malformed question, not a search that found nothing.
const ReasonDigestRequired = "digest_required"

// digestPattern is a lowercase hex sha256 — the same shape the seal writes into
// sha256 and the same shape a run frame carries.
var digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// maxResolveCandidates bounds one lookup. A digest shared by more artifacts than
// this inside one tenant is an anomaly worth refusing over an unbounded read, and
// refusing here cannot hide an artifact: the caller can still read it by id.
const maxResolveCandidates = 64

// ResolvedVersion is one readable, sealed version of one artifact carrying the
// requested digest. Only what a download needs travels: id, version, size, name,
// mime. No labels, no object key, no bucket, no stored content.
type ResolvedVersion struct {
	ArtifactID string `json:"artifact_id"`
	Version    string `json:"version"`
	SHA256     string `json:"sha256"`
	Size       int64  `json:"size"`
	Name       string `json:"name"`
	MIME       string `json:"mime"`
}

// ResolveByDigest returns every sealed version in the caller's tenant that carries
// digest AND that the caller's current rights admit. It refuses with the same
// NOT_FOUND a missing id gets when nothing qualifies.
func (s *Store) ResolveByDigest(ctx context.Context, scope contracts.Scope, digest string) ([]ResolvedVersion, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if digest == "" {
		return nil, contracts.NewReasoned(contracts.ErrInvalid, ReasonDigestRequired, "artifacts: sha256 is required")
	}
	if !digestPattern.MatchString(digest) {
		return nil, contracts.NewError(contracts.ErrInvalid, "artifacts: sha256 is malformed")
	}
	const q = `SELECT id FROM ahp_artifacts.artifacts
	           WHERE tenant = $1 AND sha256 = $2 AND NOT deleted
	           ORDER BY id LIMIT $3`
	rows, err := s.pool.Query(ctx, q, scope.Tenant, digest, maxResolveCandidates)
	if err != nil {
		return nil, storageError("resolve artifacts by digest", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, storageError("resolve artifact id", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, storageError("iterate digest candidates", err)
	}

	// Each candidate goes through the ordinary read gate, denial mask included. A
	// refusal of ANY kind (labels, conversation scope, tombstone written concurrently)
	// drops that candidate; it never becomes an answer of its own. Dropping instead of
	// propagating is what keeps this lookup from being an existence oracle: a caller not
	// admitted to one candidate of their own tenant must get the same bytes they would
	// get for a digest nobody has, never a FORBIDDEN that confirms the digest is real.
	out := make([]ResolvedVersion, 0, len(ids))
	for _, id := range ids {
		ref, err := s.load(ctx, scope, id, true, true)
		if err != nil {
			continue
		}
		out = append(out, ResolvedVersion{
			ArtifactID: ref.ID, Version: ref.Version, SHA256: ref.SHA256,
			Size: ref.Size, Name: ref.Name, MIME: ref.MIME,
		})
	}
	if len(out) == 0 {
		// Byte-for-byte the answer a missing id gives, so the two are not
		// distinguishable from outside.
		return nil, contracts.NewError(contracts.ErrNotFound, "artifact not found")
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ArtifactID != out[j].ArtifactID {
			return out[i].ArtifactID < out[j].ArtifactID
		}
		return out[i].Version < out[j].Version
	})
	return out, nil
}
