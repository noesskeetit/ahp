package state

import (
	"context"
	"encoding/json"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"strings"
)

type CommonRelationDelta struct {
	Source         string   `json:"source"`
	Role           string   `json:"role"`
	Mode           string   `json:"mode"`
	ExpectedLength *int     `json:"expected_length,omitempty"`
	Targets        []string `json:"targets,omitempty"`
}
type CommonDelta struct {
	Format    string                  `json:"format"`
	Profile   string                  `json:"profile"`
	Root      string                  `json:"root"`
	Put       map[string]CommonRecord `json:"put,omitempty"`
	Delete    []string                `json:"delete,omitempty"`
	Relations []CommonRelationDelta   `json:"relations,omitempty"`
}

func (d *CommonDelta) UnmarshalJSON(raw []byte) error {
	fields, err := commonObject(raw, []string{"format", "profile", "root", "put", "delete", "relations"})
	if err != nil {
		return err
	}
	type plain CommonDelta
	var value plain
	if err = json.Unmarshal(raw, &value); err != nil {
		return err
	}
	if p, ok := fields["put"]; ok {
		records, e := commonObject(p, nil)
		if e != nil {
			return e
		}
		for _, body := range records {
			if _, e = commonObject(body, []string{"kind", "native_id", "sequence", "owner_hint", "attributes", "relations"}); e != nil {
				return e
			}
		}
	}
	if v, ok := fields["relations"]; ok {
		var changes []json.RawMessage
		if err = json.Unmarshal(v, &changes); err != nil {
			return err
		}
		for _, change := range changes {
			if _, err = commonObject(change, []string{"source", "role", "mode", "expected_length", "targets"}); err != nil {
				return err
			}
		}
	}
	*d = CommonDelta(value)
	return d.Validate()
}
func (d CommonDelta) Validate() error {
	if d.Format != CommonFormat {
		return invalid("unsupported common delta format")
	}
	if err := validIdentifier(d.Profile, "delta profile"); err != nil {
		return err
	}
	if err := validIdentifier(d.Root, "delta root"); err != nil {
		return err
	}
	if len(d.Put)+len(d.Delete) > 50000 || len(d.Relations) > 50000 {
		return invalid("common delta operation count outside limit")
	}
	for id, row := range d.Put {
		if row.Relations == nil || len(row.Relations) != 0 {
			return invalid("delta put contains metadata only; relations are separate")
		}
		if err := (CommonDocument{Format: d.Format, Profile: d.Profile, Root: id, Records: map[string]CommonRecord{id: row}}).Validate(); err != nil {
			return err
		}
	}
	seen := map[string]bool{}
	for _, id := range d.Delete {
		if err := validIdentifier(id, "delta delete id"); err != nil {
			return err
		}
		if _, put := d.Put[id]; put || seen[id] {
			return invalid("duplicate or contradictory delta delete")
		}
		seen[id] = true
	}
	edgeCount := 0
	seenRoles := map[commonRoleKey]bool{}
	for _, r := range d.Relations {
		if err := validIdentifier(r.Source, "delta source"); err != nil {
			return err
		}
		if err := validIdentifier(r.Role, "delta role"); err != nil {
			return err
		}
		key := commonRoleKey{r.Source, r.Role}
		if seenRoles[key] {
			return invalid("duplicate delta relation edit")
		}
		seenRoles[key] = true
		if r.Mode != "replace" && r.Mode != "append" && r.Mode != "delete" {
			return invalid("unknown delta relation mode")
		}
		if r.Mode == "append" {
			if r.ExpectedLength == nil || *r.ExpectedLength < 0 {
				return invalid("append requires expected_length")
			}
		} else if r.ExpectedLength != nil {
			return invalid("expected_length is append-only")
		}
		if r.Mode == "delete" && len(r.Targets) != 0 {
			return invalid("delete relation cannot carry targets")
		}
		for _, id := range r.Targets {
			if err := validIdentifier(id, "delta target"); err != nil {
				return err
			}
		}
		edgeCount += len(r.Targets)
		if edgeCount > 200000 {
			return invalid("common delta edge count outside limit")
		}
	}
	b, err := json.Marshal(d)
	if err != nil || len(b) > MaxCommonDocumentBytes {
		return invalid("common delta exceeds wire limit")
	}
	return nil
}
func cloneCommonDocument(doc *CommonDocument) CommonDocument {
	out := CommonDocument{Records: map[string]CommonRecord{}}
	if doc == nil {
		return out
	}
	out.Format = doc.Format
	out.Profile = doc.Profile
	out.Root = doc.Root
	for id, row := range doc.Records {
		row.NativeID = append(json.RawMessage(nil), row.NativeID...)
		row.Sequence = append(json.RawMessage(nil), row.Sequence...)
		row.OwnerHint = append(json.RawMessage(nil), row.OwnerHint...)
		row.Attributes = append(json.RawMessage(nil), row.Attributes...)
		links := map[string][]string{}
		for role, refs := range row.Relations {
			links[role] = append([]string{}, refs...)
		}
		row.Relations = links
		out.Records[id] = row
	}
	return out
}
func applyCommonDelta(before *CommonDocument, delta CommonDelta) (CommonDocument, error) {
	if before != nil && (before.Format != delta.Format || before.Profile != delta.Profile) {
		return CommonDocument{}, invalid("delta requires the same native profile")
	}
	out := cloneCommonDocument(before)
	out.Format = delta.Format
	out.Profile = delta.Profile
	out.Root = delta.Root
	for id, row := range delta.Put {
		links := map[string][]string{}
		if old, ok := out.Records[id]; ok {
			links = old.Relations
		}
		row.Relations = links
		out.Records[id] = row
	}
	for _, change := range delta.Relations {
		row, ok := out.Records[change.Source]
		if !ok {
			return CommonDocument{}, invalid("delta relation source does not exist")
		}
		switch change.Mode {
		case "delete":
			if _, ok := row.Relations[change.Role]; !ok {
				return CommonDocument{}, invalid("delta delete role does not exist")
			}
			delete(row.Relations, change.Role)
		case "replace":
			row.Relations[change.Role] = append([]string{}, change.Targets...)
		case "append":
			refs, exists := row.Relations[change.Role]
			if !exists || len(refs) != *change.ExpectedLength {
				return CommonDocument{}, contracts.NewError(contracts.ErrConflict, "delta append length differs")
			}
			row.Relations[change.Role] = append(refs, change.Targets...)
		}
		out.Records[change.Source] = row
	}
	for _, id := range delta.Delete {
		if _, ok := out.Records[id]; !ok {
			return CommonDocument{}, invalid("delta delete record does not exist")
		}
		delete(out.Records, id)
	}
	if err := out.Validate(); err != nil {
		return CommonDocument{}, err
	}
	return out, nil
}

// Prepare the publication attempt before entering the SQL transaction; validate
// markers against the reconstructed full graph before any rows are published.
func (s *Store) commonDeltaArtifactContext(ctx context.Context, scope contracts.Scope, req CommonCommit) (context.Context, error) {
	if len(req.ArtifactReferences) == 0 && !strings.HasSuffix(req.Delta.Profile, CommonArtifactProfileSuffix) {
		return ctx, nil
	}
	if !s.commonArtifactReferences || s.checkpointArtifacts == nil {
		return ctx, contracts.NewError(contracts.ErrUnavailable, "common artifact reference owner is not configured")
	}
	if len(req.ArtifactReferences) == 0 {
		return ctx, nil
	}
	return context.WithValue(ctx, checkpointAttemptKey{}, &checkpointAttempt{id: contracts.NewID(), scope: scope}), nil
}
