package memory

import (
	"context"
	"errors"
)

type Page struct {
	Facts  []Fact `json:"facts"`
	Total  int    `json:"total"`
	Limit  int    `json:"limit"`
	Offset int    `json:"offset"`
}
type ListEngine interface {
	List(context.Context, Bank, int, int) ([]NativeFact, int, error)
}

func (s *Service) List(ctx context.Context, id Identity, scope Scope, project string, limit, offset int) (Page, error) {
	out := Page{Facts: []Fact{}, Limit: limit, Offset: offset}
	if !scope.Valid() || (scope == Project && project == "") || limit < 1 || limit > 100 || offset < 0 || offset > 100000 {
		return out, Fail(400, "invalid_page", "invalid memory scope or page")
	}
	var b Bank
	err := s.updateIdentity(ctx, id, func(tx Tx) error {
		var err error
		b, err = s.snapshot(tx, scope, project)
		if errors.Is(err, ErrMissing) {
			return nil
		}
		return err
	})
	if err != nil {
		return out, err
	}
	if b.ID == "" {
		return out, nil
	}
	engine, ok := s.Engine.(ListEngine)
	if !ok {
		return out, Fail(503, "listing_unavailable", "memory listing is unavailable")
	}
	facts, total, err := engine.List(ctx, b, limit, offset)
	if err != nil {
		return out, err
	}
	valid := false
	err = s.updateIdentity(ctx, id, func(tx Tx) error {
		current, err := s.snapshot(tx, scope, project)
		if errors.Is(err, ErrMissing) {
			return nil
		}
		if err != nil {
			return err
		}
		valid = current.ID == b.ID && current.Revision == b.Revision
		return nil
	})
	if err != nil {
		return out, err
	}
	if !valid {
		return out, nil
	}
	if len(facts) > 0 {
		if err := s.Access.Bind(ctx, id, b.Labels); err != nil {
			return Page{}, err
		}
	}
	for _, f := range facts {
		out.Facts = append(out.Facts, Fact{Ref: b.ID + "." + f.ID, Scope: scope, Text: f.Text, Type: f.Type, MentionedAt: f.MentionedAt, Metadata: f.Metadata})
	}
	out.Total = total
	return out, nil
}
