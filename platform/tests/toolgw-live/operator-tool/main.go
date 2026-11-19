// Command operator-tool is harness tooling for the live Tool Gateway leg: it
// performs the OPERATOR-plane side of DEF-TGW-1 durable evidence through the
// product's own audited path (tools.Service.GrantOperatorPolicy /
// RevokeOperatorPolicy). That path is intentionally not an HTTP route -- a task
// bearer must never widen its own side-effect authority -- so an acceptance
// stand exercises it exactly like an operator would: out of band, against the
// stand database, with an audit row per change.
//
// It performs NO tool call and resolves NO credential; the credential resolver
// and connector map are inert placeholders required only by tools.New.
//
// Environment (secrets are never printed):
//
//	AHP_TOOLS_DB_URL  required -- stand tools database DSN
//	AHP_TOOLS_SCHEMA  optional -- defaults to the mount default ahp_tools_all
//	AHP_TOOLGW_OP_TENANT     required (e.g. djourney)
//	AHP_TOOLGW_OP_CONNECTION required (e.g. h23mcp)
//	AHP_TOOLGW_OP_TOOLS      required -- comma-separated aliases
//	AHP_TOOLGW_OP_ACTOR      optional audit actor (default harness-operator)
//	AHP_TOOLGW_OP_REVOKE=1   revoke instead of grant
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/ahp-platform/ahp/internal/tools"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

type inertResolver struct{}

func (inertResolver) Resolve(context.Context, contracts.Scope, string) (tools.Credential, error) {
	return tools.Credential{}, fmt.Errorf("operator-tool: credentials are never resolved here")
}

func envRequired(key string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		fmt.Fprintln(os.Stderr, "operator-tool: "+key+" is required")
		os.Exit(2)
	}
	return v
}

func main() {
	ctx := context.Background()
	schema := strings.TrimSpace(os.Getenv("AHP_TOOLS_SCHEMA"))
	if schema == "" {
		schema = "ahp_tools_all"
	}
	tenant := envRequired("AHP_TOOLGW_OP_TENANT")
	conn := envRequired("AHP_TOOLGW_OP_CONNECTION")
	aliases := strings.Split(envRequired("AHP_TOOLGW_OP_TOOLS"), ",")
	actor := strings.TrimSpace(os.Getenv("AHP_TOOLGW_OP_ACTOR"))
	if actor == "" {
		actor = "harness-operator"
	}
	pool, err := pgxpool.New(ctx, envRequired("AHP_TOOLS_DB_URL"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "operator-tool: pool:", err)
		os.Exit(2)
	}
	defer pool.Close()
	cfg := tools.Config{
		Guardian:    tools.NewPolicyGuardian("operator-tool"),
		Credentials: inertResolver{},
		Connections: map[string]tools.Connector{"operator-tool": func(context.Context, tools.Credential) (tools.MCPClient, error) {
			return nil, fmt.Errorf("operator-tool: never connects")
		}},
		Tools: map[string]tools.ToolSpec{"operator-tool": {Name: "operator-tool", Connection: "operator-tool"}},
	}
	svc, err := tools.New(ctx, pool, schema, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "operator-tool: service:", err)
		os.Exit(1)
	}
	scope := contracts.Scope{Tenant: tenant, Deployment: "operator", Subject: actor}
	if os.Getenv("AHP_TOOLGW_OP_COUNT") == "1" {
		var n int
		if err := pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.tool_policy WHERE tenant=$1 AND connection=$2`, schema), tenant, conn).Scan(&n); err != nil {
			fmt.Fprintln(os.Stderr, "operator-tool: count:", err)
			os.Exit(1)
		}
		fmt.Printf("operator-tool: policy_rows=%d\n", n)
		return
	}
	revoke := os.Getenv("AHP_TOOLGW_OP_REVOKE") == "1" || os.Getenv("AHP_TOOLGW_OP_ACTION") == "revoke"
	for _, raw := range aliases {
		alias := strings.TrimSpace(raw)
		if alias == "" {
			continue
		}
		var err error
		if revoke {
			err = svc.RevokeOperatorPolicy(ctx, scope, conn, alias, actor)
		} else {
			err = svc.GrantOperatorPolicy(ctx, scope, conn, alias, actor)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "operator-tool: alias %s: %v\n", alias, err)
			os.Exit(1)
		}
		fmt.Printf("operator-tool: ok alias=%s revoke=%v\n", alias, revoke)
	}
}
