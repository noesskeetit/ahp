package memory

import "time"

func (s *Service) sourceFloor(tx Tx, b Bank, project string) (time.Time, error) {
	floor := b.SourceFloor
	if b.Scope == Personal {
		u, err := s.user(tx)
		if err != nil {
			return floor, err
		}
		if u.Since.After(floor) {
			floor = u.Since
		}
	}
	if project != "" {
		p, err := s.project(tx, project)
		if err != nil {
			return floor, err
		}
		since := p.ProjectSince
		if b.Scope == Personal {
			since = p.PersonalSince
		}
		if since.After(floor) {
			floor = since
		}
	}
	return floor, nil
}
