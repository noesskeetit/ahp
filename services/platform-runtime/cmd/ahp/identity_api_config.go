package main

import "github.com/ahp-platform/ahp/internal/identityapi"

func (s *server) configureIdentityAPIAuthority(cfg *identityapi.Config) {
	if s.iam == nil {
		return
	}
	cfg.RequireExactWorkloadIDs = true
	cfg.ExchangerWorkloadIDs = envListOr("AHP_IDENTITY_EXCHANGER_SPIFFE_IDS")
	cfg.DelegatorWorkloadIDs = envListOr("AHP_IDENTITY_DELEGATOR_SPIFFE_IDS")
	cfg.ControlPlaneWorkloadIDs = envListOr("AHP_IDENTITY_CONTROL_SPIFFE_IDS")
}
