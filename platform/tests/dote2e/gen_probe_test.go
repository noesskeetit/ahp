package dote2e_test

import (
	"context"
	"testing"
)

func TestGenerationRowsOfProbeKeys(t *testing.T) {
	stand := loadStand(t)
	rows, err := stand.Pool.Query(context.Background(),
		`SELECT key, status, coalesce(finish_reason,''), coalesce(task,''), coalesce(left(error,160),''), model, coalesce(provider,'')
		   FROM ahp_model.generations WHERE tenant=$1 ORDER BY created_at DESC LIMIT 5`, stand.Scope["tenant"])
	if err != nil {
		t.Fatalf("query generations: %v", err)
	}
	defer rows.Close()
	var found []map[string]any
	for rows.Next() {
		var key, status, finish, code, message, model, provider string
		if err := rows.Scan(&key, &status, &finish, &code, &message, &model, &provider); err != nil {
			t.Fatalf("scan: %v", err)
		}
		found = append(found, map[string]any{"key": key, "status": status, "finish": finish,
			"task_prefix": code[:10], "model": model, "provider": provider, "error": message})
	}
	var budgets int
	if err := stand.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ahp_model.budgets WHERE tenant=$1`, stand.Scope["tenant"]).Scan(&budgets); err != nil {
		t.Fatalf("count budgets: %v", err)
	}
	var aliases int
	if err := stand.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ahp_model.budget_task_aliases WHERE tenant=$1`, stand.Scope["tenant"]).Scan(&aliases); err != nil {
		t.Fatalf("count aliases: %v", err)
	}

	var grants int
	if err := stand.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ahp_identity.grants_root WHERE tenant=$1`, stand.Scope["tenant"]).Scan(&grants); err != nil {
		t.Fatalf("count root grants: %v", err)
	}
	stand.record(t, "model-plane-probe", "PASS", map[string]any{
		"generations": found, "budget_rows": budgets, "budget_aliases": aliases, "root_grants": grants})
}
