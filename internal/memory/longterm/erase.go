package memory

import (
	"context"
	"errors"
)

func subjectFenced(tx Tx) (bool, error) {
	var fenced bool
	err := tx.Get("fence", "subject", &fenced)
	if errors.Is(err, ErrMissing) {
		return false, nil
	}
	return fenced, err
}

// EraseSubject is a control-plane API, never an LLM tool. The caller must
// authorize the exact administrative target before entering this method.
// pending stays true until every closed Hindsight bank has been processed.
func (s *Service) EraseSubject(ctx context.Context, tenant, user string) (pending bool, errorCount int, err error) {
	if tenant == "" || user == "" {
		return false, 0, Fail(400, "invalid_owner", "tenant and subject required")
	}
	err = s.update(ctx, (Identity{TenantID: tenant, UserID: user}).Owner(), func(tx Tx) error {
		if err := tx.Put("fence", "subject", true); err != nil {
			return err
		}
		if err := tx.Put("user", "settings", User{Enabled: false, Since: s.Now()}); err != nil {
			return err
		}
		projects, err := list[ProjectState](tx, "project")
		if err != nil {
			return err
		}
		for _, p := range projects {
			p.Deleted = true
			if err := tx.Put("project", p.ID, p); err != nil {
				return err
			}
			if err := s.closeBanks(tx, Project, p.ID); err != nil {
				return err
			}
		}
		if err := s.closeBanks(tx, Personal, ""); err != nil {
			return err
		}
		banks, err := list[Bank](tx, "bank")
		if err != nil {
			return err
		}
		for _, b := range banks {
			if b.State != "deleted" {
				pending = true
				errorCount++
			}
		}
		if !pending {
			if err := tx.ForgetIdentity(); err != nil {
				return err
			}
			for _, kind := range []string{"project", "cursor", "turn", "binding", "idempotency", "run", "job"} {
				if err := tx.DeleteKind(kind); err != nil {
					return err
				}
			}
			for _, bank := range banks {
				bank.ProjectID = ""
				bank.Labels = nil
				if err := tx.Put("bank", bank.ID, bank); err != nil {
					return err
				}
			}
		}
		return nil
	})
	return
}
