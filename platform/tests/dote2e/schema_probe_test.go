package dote2e_test

// A durable lane diagnostic: the standalone (custom-agent) path spends against
// the Model Gateway's task budget, so the shape of the ahp_model schema is part
// of the evidence. It prints table and column names only - no values.

import (
	"context"
	"testing"
)

func TestModelPlaneSchemaIsMigrated(t *testing.T) {
	stand := loadStand(t)
	rows, err := stand.Pool.Query(context.Background(),
		`SELECT table_schema, table_name FROM information_schema.tables
		   WHERE table_schema IN ('ahp_model','ahp_exec') ORDER BY 1,2`)
	if err != nil {
		t.Fatalf("read the model/exec schema: %v", err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var schema, table string
		if err := rows.Scan(&schema, &table); err != nil {
			t.Fatalf("scan: %v", err)
		}
		tables = append(tables, schema+"."+table)
	}
	// record classifies named check objects, not top-level metadata. This is
	// a schema inventory only; it does not prove a completed model turn.
	inventory := map[string]any{"status": "PASS", "tables": tables, "count": len(tables)}
	checks := map[string]any{"schema_inventory": inventory}
	columns := map[string][]string{}
	for _, name := range tables {
		var schema, table string
		for i := range name {
			if name[i] == '.' {
				schema, table = name[:i], name[i+1:]
				break
			}
		}
		crows, err := stand.Pool.Query(context.Background(),
			`SELECT column_name FROM information_schema.columns WHERE table_schema=$1 AND table_name=$2 ORDER BY 1`,
			schema, table)
		if err != nil {
			continue
		}
		for crows.Next() {
			var column string
			crows.Scan(&column)
			columns[name] = append(columns[name], column)
		}
		crows.Close()
	}
	inventory["columns"] = columns
	if len(tables) == 0 {
		inventory["status"] = "FAIL"
		stand.record(t, "model-schema-probe", "FAIL", checks)
		t.Fatal("the Model Gateway schema is not migrated on the lane stand")
	}
	stand.record(t, "model-schema-probe", "PASS", checks)
}
