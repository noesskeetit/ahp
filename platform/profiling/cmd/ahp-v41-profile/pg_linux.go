package main

import (
	"context"
	"fmt"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts/journal"
)

func seedSessions(ctx context.Context, s *stand, count int) error {
	tx, err := s.PG.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	// Small dormant records pressure the real indexes and query plans. Active
	// fixture lanes use a separate 100 KiB of context below.
	for target := range s.SubjectTokens {
		scope := s.Scope(target)
		for n := target; n < count; n += len(s.SubjectTokens) {
			session := journal.ScopedSessionID(scope, fmt.Sprintf("dormant-%d", n))
			if _, err := tx.Exec(ctx, `
                INSERT INTO ahp_journal.sessions
                (tenant_id,deployment_id,subject_id,session_id,harness_type,format_version,status,head_seq,head_checkpoint_id,execution_mode)
                VALUES ($1,$2,$3,$4,'go',1,'IDLE',25,'baseline','vm_managed')`, scope.Tenant, scope.Deployment, scope.Subject, session); err != nil {
				return err
			}
		}
		_, err = tx.Exec(ctx, `
            INSERT INTO ahp_journal.session_events
            (tenant_id,session_id,seq,event_uid,event_type,role,codec,inline_payload)
            SELECT t.tenant_id,t.session_id,n,'fixture-'||n,'bench.history','system','json',convert_to('{"prior":true}','UTF8')
            FROM ahp_journal.sessions t CROSS JOIN generate_series(1,25) n
            WHERE t.tenant_id=$1 AND t.head_checkpoint_id='baseline'`, scope.Tenant)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
            INSERT INTO ahp_journal.checkpoints
            (tenant_id,session_id,checkpoint_ns,checkpoint_id,snapshot_seq,metadata,channel_versions,codec,inline_payload,operation_key,request_digest)
            SELECT tenant_id,session_id,'','baseline',25,'{}','{}','json',convert_to('{}','UTF8'),'fixture-seed',decode(repeat('00',32),'hex')
            FROM ahp_journal.sessions WHERE tenant_id=$1 AND head_checkpoint_id='baseline'`, scope.Tenant)
		if err != nil {
			return err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	for _, table := range []string{"sessions", "session_events", "checkpoints"} {
		if _, err := s.PG.Exec(ctx, "VACUUM (ANALYZE) ahp_journal."+table); err != nil {
			return err
		}
	}
	return nil
}

func (s *stand) seedLane(ctx context.Context, target int, conv string) error {
	scope := s.Scope(target)
	session := journal.ScopedSessionID(scope, conv)
	tx, err := s.PG.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `
        INSERT INTO ahp_journal.sessions
        (tenant_id,deployment_id,subject_id,session_id,harness_type,format_version,status,head_seq,execution_mode)
        VALUES ($1,$2,$3,$4,'go',1,'IDLE',25,'vm_managed')`, scope.Tenant, scope.Deployment, scope.Subject, session)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
        INSERT INTO ahp_journal.session_events
        (tenant_id,session_id,seq,event_uid,event_type,role,codec,inline_payload)
        SELECT $1,$2,n,'bench-history-'||n,'bench.history','system','json',
        convert_to(json_build_object('id',n,'text',repeat(md5($2 || n::text),128))::text,'UTF8')
        FROM generate_series(1,25) n`, scope.Tenant, session)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type pgCounters struct {
	Calls      int64   `json:"calls"`
	ExecMillis float64 `json:"exec_ms"`
	WALBytes   float64 `json:"wal_bytes"`
}

func snapshotPG(ctx context.Context, s *stand) (map[string]pgCounters, error) {
	rows, err := s.PG.Query(ctx, `
      SELECT d.datname,sum(p.calls)::bigint,sum(p.total_exec_time),sum(p.wal_bytes)::float8
      FROM pg_stat_statements p JOIN pg_database d ON d.oid=p.dbid
      GROUP BY d.datname ORDER BY d.datname`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]pgCounters{}
	for rows.Next() {
		var db string
		var n pgCounters
		if err := rows.Scan(&db, &n.Calls, &n.ExecMillis, &n.WALBytes); err != nil {
			return nil, err
		}
		for role, name := range s.DatabaseNames {
			if name == db {
				out[role] = n
			}
		}
	}
	return out, rows.Err()
}

func finalLiveState(s *stand) map[string]int64 {
	out := map[string]int64{}
	ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	for _, row := range []struct{ key, sql string }{
		{"queued", `SELECT count(*) FROM ahp_journal.sessions WHERE status='QUEUED'`},
		{"active", `SELECT count(*) FROM ahp_journal.sessions WHERE status='ACTIVE'`},
		{"expired_active", `SELECT count(*) FROM ahp_journal.sessions WHERE status='ACTIVE' AND lease_expires_at<=now()`},
		{"live_tool_intents", `SELECT count(*) FROM ahp_journal.tool_intents WHERE status IN ('DISPATCHED','PREPARED')`},
		{"dispatched_tool_intents", `SELECT count(*) FROM ahp_journal.tool_intents WHERE status='DISPATCHED'`},
	} {
		var n int64
		if err := s.PG.QueryRow(ctx, row.sql).Scan(&n); err == nil {
			out[row.key] = n
		}
	}
	return out
}
