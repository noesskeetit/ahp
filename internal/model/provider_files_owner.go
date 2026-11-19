package model

import (
	"context"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// WithProviderFiles joins copies to the existing Model owner cascade.
func (s *Service) WithProviderFiles(files *FileService) *Service { s.files = files; return s }

func (s *Service) deleteProviderFiles(ctx context.Context, tenant string) error {
	if s.files != nil {
		return s.files.DeleteTenant(ctx, tenant)
	}
	var exists bool
	if err := s.gens.pool.QueryRow(ctx, `SELECT to_regclass('ahp_model.provider_files') IS NOT NULL`).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return nil
	}
	var pending bool
	if err := s.gens.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_model.provider_files WHERE tenant=$1 AND status<>'DELETED')`, tenant).Scan(&pending); err != nil {
		return err
	}
	if pending {
		return contracts.NewReasoned(contracts.ErrUnavailable, "tenant_model_files_pending", "model: file cleanup owner is not configured")
	}
	_, err := s.gens.pool.Exec(ctx, `DELETE FROM ahp_model.provider_files WHERE tenant=$1`, tenant)
	return err
}

// DeleteSubjectFiles is the trusted service-control hook. Missing runtime
// configuration must not erase the inventory of still-existing remote copies.
func (s *Service) DeleteSubjectFiles(ctx context.Context, tenant, subject string) error {
	if s.files != nil {
		return s.files.DeleteSubject(ctx, tenant, subject)
	}
	var exists bool
	if err := s.gens.pool.QueryRow(ctx, `SELECT to_regclass('ahp_model.provider_files') IS NOT NULL`).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return nil
	}
	var pending bool
	if err := s.gens.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_model.provider_files WHERE tenant=$1 AND subject=$2 AND status<>'DELETED')`, tenant, subject).Scan(&pending); err != nil {
		return err
	}
	if pending {
		return contracts.NewReasoned(contracts.ErrUnavailable, "subject_model_files_pending", "model: file cleanup owner is not configured")
	}
	_, err := s.gens.pool.Exec(ctx, `DELETE FROM ahp_model.provider_files WHERE tenant=$1 AND subject=$2 AND status='DELETED'`, tenant, subject)
	return err
}
