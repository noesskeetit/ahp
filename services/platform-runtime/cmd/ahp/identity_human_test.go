package main

import (
	"context"
	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/stateapi"
	"github.com/ahp-platform/ahp/internal/tools/toolsapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/transport"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http"
	"os"
	"strings"
	"testing"
)

func TestSyntheticHumanFacadesHonorResourceRevocation(t *testing.T) {
	ctx := context.Background()
	if os.Getenv("AHP_TEST_DATABASE_URL") == "" {
		t.Fatal("real PostgreSQL required")
	}
	pool, err := pgxpool.New(ctx, os.Getenv("AHP_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	policy := identity.NewSyntheticResourcePolicy(pool)
	ids := identity.New(pool, identity.WithResourceEvaluator(policy))
	if err = ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	caps := []string{"workspace", toolsapi.ApproveCapability, toolsapi.ConnectCapability, stateapi.AdminCapability, stateapi.TenantDeleteCapability}
	u := identity.BrowserUser{UserID: "human-" + randHexTest(), Tenant: "human-iam-" + randHexTest(), Subject: "alice"}
	if err = ids.SeedBrowserUser(ctx, u, "login-"+randHexTest(), "synthetic-test-password", nil, caps); err != nil {
		t.Fatal(err)
	}
	session, err := ids.StartBrowserSession(ctx, u.UserID)
	if err != nil {
		t.Fatal(err)
	}
	jti := "human-" + randHexTest()
	if err = ids.BindBrowserToken(ctx, jti, session); err != nil {
		t.Fatal(err)
	}
	c := auth.Claims{Kind: auth.KindHuman, Tenant: u.Tenant, Subject: u.Subject, Deployment: "edge", JTI: jti}
	for _, cap := range caps {
		t.Run(cap, func(t *testing.T) {
			var authorize transport.Authorize
			switch cap {
			case "workspace":
				authorize = dataLiveAuthority(ids)
			case toolsapi.ApproveCapability, toolsapi.ConnectCapability:
				authorize = toolsHumanAuthorize(ids, nil)
			default:
				authorize = stateAdminAuthority(ids, []string{"edge"})
			}
			for _, resource := range []identity.Resource{{Kind: "tenant", ID: u.Tenant}, {Kind: "deployment", ID: "edge"}} {
				if _, err = policy.PutRule(ctx, identity.ResourceRule{Tenant: u.Tenant, Subject: u.Subject, Resource: resource, Action: cap, Allowed: true}, 0); err != nil {
					t.Fatal(err)
				}
			}
			if err = authorize(ctx, c, cap); err != nil {
				t.Fatal("authorized human was refused")
			}
			rule := identity.ResourceRule{Tenant: u.Tenant, Subject: u.Subject, Resource: identity.Resource{Kind: "deployment", ID: "edge"}, Action: cap, Allowed: false}
			if _, err = policy.PutRule(ctx, rule, 1); err != nil {
				t.Fatal(err)
			}
			if err = authorize(ctx, c, cap); err == nil {
				t.Fatal("old human claims bypassed live resource revocation")
			}
			// Existing local profile intentionally has no resource evaluator.
			local := identity.New(pool)
			var localAuthorize transport.Authorize
			switch cap {
			case "workspace":
				localAuthorize = dataLiveAuthority(local)
			case toolsapi.ApproveCapability, toolsapi.ConnectCapability:
				localAuthorize = toolsHumanAuthorize(local, nil)
			default:
				localAuthorize = stateAdminAuthority(local, []string{"edge"})
			}
			if err = localAuthorize(ctx, c, cap); err != nil {
				t.Fatal("local profile compatibility changed")
			}
		})
	}
}

func TestSyntheticToolsCannotSelectLegacyHumanAuthority(t *testing.T) {
	s := &server{iam: identity.ResourceEvaluatorFunc(func(context.Context, identity.ResourceRequest) error { return nil })}
	err := s.tryMountTools(context.Background(), http.NewServeMux(), nil, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "live human resource authority") {
		t.Fatal("synthetic tools accepted legacy human authorization")
	}
}
