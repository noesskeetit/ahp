#!/usr/bin/env node
// scripts/h22/j4-verify.mjs -- INDEPENDENT read-only verifier for the J4
// delegation-chain evidence (agent A delegates a tool call to deployment B).
//
// It is deliberately NOT a harness: it never starts, restarts or re-pins the
// stand, and it writes nothing unless the caller explicitly passes --control
// (the one mutating probe, off by default). It takes the task id of the run and
// re-derives every claim out-of-band from B's Postgres, B's object storage and
// B's public A2A face, then prints PASS / FAIL per assertion.
//
// Honesty rule: an assertion whose inputs are missing is FAIL, never SKIP-as-ok
// and never green by default. Exit code is 0 only when all six assertions pass.
//
// Usage:
//   node scripts/h22/j4-verify.mjs --task <B-task-id> [--parent <A-task-id>]
//     [--tools <h22tools>] [--pg-b <dsn>] [--pg-a <dsn>] [--schema-b <s>] [--schema-a <s>]
//     (A-side is read from EITHER plane: the gateway tasks table when deployment A
//      runs one, otherwise the as-runner run plane -- <schema>.runs / .stream_events /
//      artifacts -- which is where a native A run actually journals.)
//     [--log-a <path>] [--minio-bucket <b>] [--creds-file <path>]
//     [--b-url https://127.0.0.1:29643 --cert-dir <dir> --token <t>]
//     [--foreign-cert-dir <dir> --foreign-token <t> | --foreign-token-file <path>]
//     [--expect-budget N] [--deployment-budget N]
//     [--control]                    (MUTATING: sends one control SendMessage to B)
//     [--control-task <id>]          (READ-ONLY: re-reads an existing control task on B)
//
// Token hygiene: prefer --foreign-token-file / --token-file so no credential ever
// enters argv. Both accept a file whose first line is the bearer token.
import { execFileSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import https from 'node:https';

const NL = String.fromCharCode(10);
const RED = (s) => String(s).replace(/[A-Za-z0-9+/._-]{24,}/g, '<red>');

function parseArgs(argv) {
  const o = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith('--')) continue;
    const k = a.slice(2);
    if (i + 1 < argv.length && !argv[i + 1].startsWith('--')) o[k] = argv[++i];
    else o[k] = true;
  }
  return o;
}
const A = parseArgs(process.argv.slice(2));
const TOOLS = A.tools || '/dev/shm/h22-j4/bin/h22tools';
const PG_B = A['pg-b'] || 'postgres://ahp@127.0.0.1:15432/ahp_hive_h10sb?sslmode=disable';
const PG_A = A['pg-a'] || '';
const TASK = A.task || '';
const DEPLOYMENT_BUDGET_DEFAULT = Number(A['deployment-budget'] || 8);

const results = [];
function verdict(id, title, v, evidence) {
  results.push({ id, title, verdict: v, evidence });
  console.log('[' + v + '] ' + id + ' ' + title);
  for (const line of String(evidence).split(NL)) if (line.trim()) console.log('        ' + line);
}
function now() { return new Date().toISOString(); }

function pg(dsn, sql) {
  try {
    return { out: execFileSync(TOOLS, ['pg', dsn, sql], { encoding: 'utf8', timeout: 30000 }) };
  } catch (e) { return { err: RED(e.message).split(NL)[0] }; }
}
function rowsOf(out) {
  const lines = String(out || '').split(NL);
  const cols = (lines.find((l) => l.startsWith('COL' + '\t')) || '').split('\t').slice(1);
  return lines.filter((l) => l.startsWith('ROW' + '\t')).map((l) => {
    const vals = l.split('\t').slice(1);
    const o = {};
    cols.forEach((c, i) => (o[c] = vals[i]));
    return o;
  });
}
function tokOf(optKey) {
  const f = A[optKey + '-file'];
  if (f) return readFileSync(f, 'utf8').trim().split(NL)[0];
  return A[optKey] || '';
}

function discoverSchema(dsn, tag) {
  const r = pg(dsn, "SELECT table_schema FROM information_schema.tables WHERE table_name='tasks' INTERSECT SELECT table_schema FROM information_schema.tables WHERE table_name='operations' ORDER BY 1");
  if (r.err) return { err: r.err };
  const schemas = rowsOf(r.out).map((x) => x.table_schema);
  console.log('        discovered gateway schema(s) on ' + tag + ': ' + JSON.stringify(schemas));
  return { schemas };
}

// readARunPlane reads deployment A's own journal for the delegating run, out-of-band,
// from whichever plane A actually persists to. A native as-runner has no gateway tasks
// table at all: the run lives in <schema>.runs, its steps in <schema>.stream_events
// (one event per model turn / tool call) and the subject it acted as in the artifact
// journal. Nothing here is inferred from what the B face chose to report.
function readARunPlane(dsn, runId) {
  const d = pg(dsn, "SELECT table_schema FROM information_schema.tables WHERE table_name='runs' ORDER BY 1");
  if (d.err) return { err: 'runs-plane discovery failed: ' + d.err };
  const s = (rowsOf(d.out).map((x) => x.table_schema).filter((x) => x !== 'public')[0]) || '';
  if (!s) return { err: 'no schema with a runs table on A' };
  const q = pg(dsn, "SELECT id,thread_id,task_id,status,sequence,created_at,updated_at,payload::text AS payload FROM " + s + ".runs WHERE id='" + runId + "'");
  if (q.err) return { err: s + '.runs unreadable: ' + q.err };
  const row = rowsOf(q.out)[0] || null;
  if (!row) return { err: 'no row for the delegating run in ' + s + '.runs' };
  const out = { schema: s, row, steps: null, maxSteps: null, subj: null, kinds: null, sources: [] };
  // DEF-A6: the terminal reason of a turn is journaled on the run row's payload
  // (internal/state/close_reason.go RunPayloadCloseReason). Read it in SQL: this
  // reader prints jsonb as a Go map, which is not parseable here.
  const cr = pg(dsn, "SELECT coalesce(payload->>'close_reason','') AS cr FROM " + s + ".runs WHERE id='" + runId + "'");
  const crRow = rowsOf(cr.out)[0];
  out.closeReason = crRow ? String(crRow.cr || '') : '';
  if (out.closeReason) out.sources.push(s + '.runs payload close_reason');
  const ev = pg(dsn, "SELECT kind, count(*)::int AS n FROM " + s + ".stream_events WHERE run_id='" + runId + "' GROUP BY kind ORDER BY kind");
  if (!ev.err) {
    const c = {};
    for (const e of rowsOf(ev.out)) c[e.kind] = Number(e.n);
    out.kinds = c;
    if (c.model !== undefined) { out.steps = c.model; out.sources.push(s + '.stream_events kind=model'); }
  }
  const ms = /max_steps\D{0,4}(\d+)/.exec(row.payload || '');
  if (ms) { out.maxSteps = Number(ms[1]); out.sources.push(s + '.runs payload max_steps'); }
  const ad = pg(dsn, "SELECT table_schema FROM information_schema.tables WHERE table_name='artifacts' ORDER BY 1");
  const asch = (rowsOf(ad.out || '').map((x) => x.table_schema)[0]) || '';
  if (asch) {
    const sq = pg(dsn, "SELECT tenant,subject,count(*)::int AS n FROM " + asch + ".artifacts WHERE task_id='" + runId + "' GROUP BY tenant,subject ORDER BY 1");
    out.subj = rowsOf(sq.out)[0] || null;
    if (out.subj) out.sources.push(asch + '.artifacts subject/tenant journaled by A itself');
  }
  out.terminal = /^(done|failed|error|cancelled|completed)$/i.test(row.status || '');
  return out;
}

async function main() {
  console.log('j4-verify t=' + now() + ' task=' + (TASK || '<missing>'));
  if (!TASK) { console.log('FATAL: --task is required'); process.exit(2); }
  if (!existsSync(TOOLS)) { console.log('FATAL: out-of-band tool not found: ' + TOOLS + ' (rebuild scripts/h22tools)'); process.exit(2); }
  console.log('--- environment probe (read-only)');

  const schemaB = A['schema-b'] || (discoverSchema(PG_B, 'B').schemas || [])[0] || '';
  if (!schemaB) {
    verdict('A1', 'B.tasks parent_task and principal', 'FAIL', 'cannot read B gateway schema: pass --schema-b, DSN=' + RED(PG_B));
    for (const [id, t] of [['A2', 'budget source/effective'], ['A3', 'no double spend'], ['A4', 'foreign tenant denied'], ['A5', 'out-of-band sha256 == recorded'], ['A6', 'close_reason and steps on both sides']]) verdict(id, t, 'FAIL', 'not evaluated: B is not readable');
    finish();
    return;
  }
  const qb = pg(PG_B, "SELECT task_id,tenant,deployment,subject,caller,parent_task,context_id,state,attempts,metadata::text AS metadata FROM " + schemaB + ".tasks WHERE task_id='" + TASK + "'");
  const brow = rowsOf(qb.out)[0];

  // ---- A1: parent linkage + principal identity -------------------------------
  let arow = null;
  const schemaA = A['schema-a'] || '';
  if (A.parent && PG_A) {
    const sa = schemaA || (discoverSchema(PG_A, 'A').schemas || [])[0] || '';
    if (!sa) console.log('        A-side: this DB has no gateway schema (tasks+operations) at all, so deployment A journaled no task here');
    if (sa) {
      const qa = pg(PG_A, "SELECT task_id,tenant,deployment,subject,caller,state,metadata::text AS metadata FROM " + sa + ".tasks WHERE task_id='" + A.parent + "'");
      arow = rowsOf(qa.out)[0] || null;
      if (!arow && qa.err) console.log('        A-side read error: ' + qa.err);
    }
  }
  // No gateway task row on A (the normal case for a native as-runner): fall back to
  // A's own run plane, which is where the delegating run is actually journaled.
  let aRun = null;
  if (!arow && A.parent && PG_A) {
    aRun = readARunPlane(PG_A, A.parent);
    if (aRun.err) console.log('        A-side run-plane probe: ' + aRun.err);
    else console.log('        A-side run-plane row found in ' + aRun.schema + '.runs (steps=' + fmt(aRun.steps) + ', kinds=' + JSON.stringify(aRun.kinds) + ')');
  }
  if (!brow) {
    verdict('A1', 'B.tasks parent_task and principal', 'FAIL', 'no row for task in ' + schemaB + '.tasks on B: ' + RED(qb.err || 'query returned 0 rows'));
  } else {
    const metaB = safeJSON(brow.metadata);
    const parent = brow.parent_task || (metaB.ahp_parent_task || '');
    const wantParent = A.parent || '';
    const parentOk = parent && (!wantParent || parent === wantParent);
    const aSubj = arow
      ? { tenant: arow.tenant, subject: arow.subject, src: 'A gateway tasks row' }
      : (aRun && aRun.subj ? { tenant: aRun.subj.tenant, subject: aRun.subj.subject, src: 'A artifact journal (run plane)' } : null);
    const principalOk = !!aSubj && brow.subject === aSubj.subject && brow.tenant === aSubj.tenant;
    const runLinkOk = aRun && aRun.row ? (brow.parent_task === aRun.row.id && brow.caller === aRun.row.task_id) : null;
    verdict('A1', 'B.tasks parent_task and principal', parentOk && principalOk && runLinkOk !== false ? 'PASS' : 'FAIL',
      ['B row: tenant=' + brow.tenant + ' deployment=' + brow.deployment + ' subject=' + brow.subject + ' caller=' + brow.caller + ' state=' + brow.state,
       'B parent_task=' + (brow.parent_task || '(empty)') + ' metadata.ahp_parent_task=' + (metaB.ahp_parent_task || '(empty)'),
       arow ? 'A row: subject=' + arow.subject + ' tenant=' + arow.tenant + ' state=' + arow.state : 'no A gateway task row',
       aRun && aRun.row
         ? 'A run row (' + aRun.schema + '.runs, read out-of-band): id=' + aRun.row.id + ' task_id=' + aRun.row.task_id + ' status=' + aRun.row.status + ' thread=' + aRun.row.thread_id
         : (aRun ? 'A run-plane read failed: ' + aRun.err : 'A-side row NOT read (needs --parent and --pg-a): principal equality unverifiable'),
       runLinkOk === null ? 'run linkage not evaluated (no A run row)' : 'run linkage: B.parent_task==A.run.id && B.caller==A.run.task_id => ' + runLinkOk,
       aSubj ? 'A-subject source: ' + aSubj.src + ' -> ' + aSubj.tenant + '/' + aSubj.subject : 'A-subject not derivable out-of-band',
       'checks: parent_present=' + !!parent + ' parent_matches=' + (!wantParent || parent === wantParent) + ' subject_equals_A=' + principalOk].join(NL));
  }

  const metaB = brow ? safeJSON(brow.metadata) : {};

  // ---- A2: budget came from the caller and equals A residual ------------------
  const src = metaB.step_budget_source || '';
  const eff = metaB.step_budget_effective === undefined ? null : Number(metaB.step_budget_effective);
  const expect = A['expect-budget'] === undefined ? null : Number(A['expect-budget']);
  const aMeta = arow ? safeJSON(arow.metadata) : {};
  const aBudget = aMeta.max_steps !== undefined ? Number(aMeta.max_steps)
    : (aRun && aRun.maxSteps !== null ? aRun.maxSteps : null);
  let a2ok = !!src && src !== '';
  const a2notes = ['B metadata: step_budget_source=' + (src || '(absent)') + ' step_budget_effective=' + (eff === null ? '(absent)' : eff) + ' max_steps=' + (metaB.max_steps === undefined ? '(absent)' : metaB.max_steps)];
  if (!src) a2ok = false;
  if (src === 'caller') {
    if (eff === null) { a2ok = false; a2notes.push('FAIL: source=caller but no effective number recorded'); }
    else if (eff === DEPLOYMENT_BUDGET_DEFAULT && !expect) { a2ok = false; a2notes.push('SUSPECT: effective equals the deployment default (' + DEPLOYMENT_BUDGET_DEFAULT + '), so "caller" is not distinguished from a default run'); }
    if (expect !== null) { a2ok = a2ok && eff === expect; a2notes.push('residual check: effective=' + eff + ' expected=' + expect + ' => ' + (eff === expect ? 'match' : 'MISMATCH')); }
    else if (aBudget !== null) { a2ok = a2ok && eff <= aBudget; a2notes.push('residual check against A.max_steps=' + aBudget + ': ' + (eff <= aBudget ? 'plausible' : 'larger than A budget')); }
    else { a2ok = false; a2notes.push('FAIL: residual not verified (pass --expect-budget N or --pg-a so A budget is read)'); }
  } else if (src) {
    a2notes.push('source is ' + src + ', expected caller for a delegated call');
  }
  if (aRun && aRun.row) {
    a2notes.push('A-side budget read out-of-band from ' + aRun.schema + '.runs payload max_steps=' + fmt(aRun.maxSteps)
      + ': the forwarded number equals A OWN per-attempt bound (adapters/emil/adapter.go RunAuthority.MaxSteps), '
      + 'NOT A bound-minus-steps-consumed; whether it is a residual after A consumed steps is a separate claim (see A3).');
  }
  verdict('A2', 'step_budget_source=caller and effective equals A residual', a2ok ? 'PASS' : 'FAIL', a2notes.join(NL));

  // ---- A2b: control call without max_steps must report source=deployment -----
  if (A.control) {
    const r = await sendControl();
    verdict('A2b', 'control call without max_steps reports source=deployment', r.ok ? 'PASS' : 'FAIL', r.evidence);
  } else if (A['control-task']) {
    const cTitle = 'control (read-only re-read of an existing task) reports source=deployment';
    const cq = pg(PG_B, "SELECT tenant,subject,state,caller,created_at,metadata::text AS metadata FROM " + schemaB + ".tasks WHERE task_id='" + A['control-task'] + "'");
    const crow = rowsOf(cq.out)[0];
    if (!crow) {
      verdict('A2b', cTitle, 'FAIL', 'control task ' + A['control-task'] + ' not found on B: ' + RED(cq.err || 'query returned 0 rows'));
    } else {
      const cm = safeJSON(crow.metadata);
      const csrc = cm.step_budget_source || '';
      const ceff = cm.step_budget_effective === undefined ? null : Number(cm.step_budget_effective);
      const samePrincipal = brow ? (crow.tenant === brow.tenant && crow.subject === brow.subject) : false;
      const ok = csrc === 'deployment' && (ceff === null || ceff === DEPLOYMENT_BUDGET_DEFAULT) && samePrincipal;
      verdict('A2b', cTitle, ok ? 'PASS' : 'FAIL', [
        'control task=' + A['control-task'] + ' state=' + crow.state + ' caller=' + (crow.caller || '(empty)') + ' created_at=' + (crow.created_at || '?')
          + ' (provenance of the control is shown so a reader can see whether it belongs to this run or to an earlier probe)',
        'control metadata: step_budget_source=' + (csrc || '(absent)') + ' step_budget_effective=' + (ceff === null ? '(absent)' : ceff) + ' (expected source=deployment with ' + DEPLOYMENT_BUDGET_DEFAULT + ')',
        'same principal as tested task: control=' + crow.tenant + '/' + crow.subject + ' tested=' + (brow ? brow.tenant + '/' + brow.subject : '(no B row)') + ' => ' + samePrincipal,
        'mode: read-only re-read of a task that already exists on B; no request was sent, nothing was created',
      ].join(NL));
    }
  } else {
    verdict('A2b', 'control call without max_steps reports source=deployment', 'FAIL', 'not fired: --control is required because it creates one task on B (mutating). Refusing to guess.');
  }

  // ---- A3: no double spend ---------------------------------------------------
  const stepsB = metaB.steps_consumed === undefined ? null : Number(metaB.steps_consumed);
  let stepsA = aMeta.steps_consumed === undefined ? null : Number(aMeta.steps_consumed);
  let aSrc = arow ? 'A.tasks.metadata' : 'none';
  if (stepsA === null && A['log-a']) {
    const g = harvestLog(A['log-a'], A.parent || '');
    if (g.steps !== null) { stepsA = g.steps; }
    if (aBudget === null && g.maxSteps !== null) { /* keep */ }
    if (g.steps !== null) aSrc = A['log-a'];
  }
  if (stepsA === null && aRun && aRun.row && typeof aRun.steps === 'number') {
    stepsA = aRun.steps;
    aSrc = ((aRun.sources || []).find((s) => s.includes('stream_events')) || 'A run plane');
  }
  const budgetForSum = aBudget !== null ? aBudget : expect;
  const a3ok = stepsA !== null && stepsB !== null && budgetForSum !== null && stepsA + stepsB <= budgetForSum;
  verdict('A3', 'steps_consumed(A)+steps_consumed(B) <= A budget', a3ok ? 'PASS' : 'FAIL',
    ['A steps=' + fmt(stepsA) + ' (source: ' + aSrc + ')', 'B steps=' + fmt(stepsB), 'A budget=' + fmt(budgetForSum) + ' (source: ' + (aBudget !== null ? 'A.metadata.max_steps' : expect !== null ? '--expect-budget' : 'none') + ')',
     a3ok ? 'sum ' + stepsA + '+' + stepsB + '=' + (stepsA + stepsB) + ' <= ' + budgetForSum
       : 'sum ' + fmt(stepsA) + '+' + fmt(stepsB) + '=' + (stepsA !== null && stepsB !== null ? stepsA + stepsB : '?') + ' vs A budget ' + fmt(budgetForSum)
         + ' -> bound NOT satisfied' + (aRun && aRun.kinds ? ' (A events journaled: ' + JSON.stringify(aRun.kinds) + ')' : '')
         + '. Note: no shared spend ledger is consulted by either side, so budget is a per-deployment per-attempt bound propagated by value, not a consumable balance.'].join(NL));

  // ---- A4: foreign tenant cannot see the task --------------------------------
  const foreignTok = tokOf('foreign-token');
  if (!A['b-url'] || !foreignTok || !A['foreign-cert-dir']) {
    verdict('A4', 'foreign tenant GetTask -> TASK_NOT_FOUND/403', 'FAIL', 'not probed: needs --b-url, --foreign-cert-dir, --foreign-token(-file). Refusing to assume isolation.');
  } else {
    const g = await rpc(A['b-url'], 'GetTask', { id: TASK }, { certDir: A['foreign-cert-dir'], token: foreignTok });
    const denied = g.status === 404 || g.status === 403 || (g.json && g.json.error && (g.json.error.code === -32001 || /not.?found/i.test(JSON.stringify(g.json.error))));
    verdict('A4', 'foreign tenant GetTask -> TASK_NOT_FOUND/403', denied ? 'PASS' : 'FAIL', 'status=' + g.status + ' body=' + RED(JSON.stringify(g.json || g.err || '')).slice(0, 220));
  }

  // ---- A5: out-of-band sha256(GET) == recorded -------------------------------
  const artQ = pg(PG_B, "SELECT id,object,expected_sha,sha256,size,version,sealed_at,deleted FROM ahp_artifacts.artifacts WHERE task_id='" + TASK + "' ORDER BY created_at");
  const arts = rowsOf(artQ.out);
  if (!arts.length) {
    verdict('A5', 'out-of-band sha256(GET) == recorded', 'FAIL', 'no rows in ahp_artifacts.artifacts for task on B: ' + RED(artQ.err || '0 rows'));
  } else if (!A['creds-file']) {
    verdict('A5', 'out-of-band sha256(GET) == recorded', 'FAIL', 'recorded rows found (' + arts.length + ') but no credentials provided: pass --creds-file <path with "user pass"> to verify bytes out-of-band');
  } else {
    const [mu, mp] = readFileSync(A['creds-file'], 'utf8').trim().split(/\s+/);
    const env = { ...process.env, AHP_S3_ENDPOINT: A['s3-endpoint'] || 'http://127.0.0.1:16997', AHP_S3_REGION: A['s3-region'] || 'us-east-1', AHP_MINIO_ROOT_USER: mu, AHP_MINIO_ROOT_PASSWORD: mp, AHP_EXEC_ARTIFACT_BUCKET: A['minio-bucket'] || 'ahp-h10-chain-b' };
    const lines = [];
    let all = true;
    for (const a of arts) {
      let got = '';
      try {
        const out = execFileSync(TOOLS, ['minio-sha', a.object], { env, encoding: 'utf8', timeout: 30000 });
        const m = /sha256=([0-9a-f]{64})/.exec(out); got = m ? m[1] : '';
      } catch (e) { lines.push('GET failed for ' + a.object + ': ' + RED(e.message).split(NL)[0]); all = false; continue; }
      const rec = a.sha256 || a.expected_sha;
      const ok = got && got === rec && (!a.sha256 || a.sha256 === a.expected_sha);
      if (!ok) all = false;
      lines.push((ok ? 'ok  ' : 'BAD ') + 'artifact=' + a.id.slice(0, 12) + ' recorded=' + (a.sha256 || '(unsealed)').slice(0, 16) + ' expected=' + a.expected_sha.slice(0, 16) + ' got=' + got.slice(0, 16) + ' size=' + (a.size || '?'));
    }
    verdict('A5', 'out-of-band sha256(GET) == recorded', all && arts.length > 0 ? 'PASS' : 'FAIL', lines.join(NL));
  }

  // ---- A6: close_reason / steps on both sides --------------------------------
  const bClose = metaB.close_reason || '';
  const bSteps = stepsB;
  let aClose = aMeta.close_reason || '';
  let aSteps = stepsA;
  if (A['log-a']) { const g = harvestLog(A['log-a'], A.parent || ''); if (!aClose && g.closeReason) aClose = g.closeReason; if (aSteps === null && g.steps !== null) aSteps = g.steps; }
  if (!aClose && aRun && aRun.closeReason) { aClose = aRun.closeReason; }
  const a6ok = bClose === 'model_finished' && bSteps !== null && bSteps >= 1
    && !!aClose && aSteps !== null && aSteps >= 1 && aClose === bClose;
  verdict('A6', 'a terminal reason is journaled on BOTH planes, equal, and matches the outcome', a6ok ? 'PASS' : 'FAIL',
    ['B: close_reason=' + (bClose || '(absent)') + ' steps_consumed=' + fmt(bSteps),
     'A: close_reason=' + (aClose || '(absent)') + ' steps_consumed=' + fmt(aSteps) + (aSrc !== 'none' ? ' (A steps source: ' + aSrc + ')' : ' (no A step counter found)')
       + (aRun && aRun.row ? ' | A run-plane terminal marker: ' + aRun.schema + '.runs.status=' + aRun.row.status + (aRun.terminal ? ' (terminal)' : ' (NOT terminal)') + ' -- read from runs.payload->>close_reason (DEF-A6)' : ''),
     a6ok ? 'both planes carry the same word for the same outcome, and both consumed steps'
       : 'strict reading: a missing or unequal reason on either plane is a FAIL. Spec anchor is "ход завершён с причиной" (rev32-paged.txt:3345-3347), so the value must EXIST and must match the outcome -- A6b proves it is not a constant.'].join(NL));

  // ---- A6b: differential -- a run that did NOT end with a model verdict -------
  // Without this, a build that journals the literal 'model_finished' on every
  // terminal path passes A6 by construction. Internal analogue: Ampere's
  // TestCanceledRunDoesNotClaimModelFinished.
  if (A['cancel-task']) {
    const cid = A['cancel-task'];
    const bTitle = 'differential: a non-model terminal outcome does not report model_finished';
    let ev2 = [];
    let ok = false;
    // Discover A's run plane independently of whether A also has a gateway tasks
    // row: a deployment may journal both, and the differential must see that plane.
    let aRow = null;
    if (PG_A) {
      const rp = readARunPlane(PG_A, cid);
      if (rp.err) ev2.push('A run plane: ' + rp.err);
      else if (rp.row) { aRow = { status: rp.row.status, cr: rp.closeReason || '' }; ev2.push('A run plane discovered in ' + rp.schema + '.runs'); }
    } else ev2.push('A run plane: no --pg-a supplied, only the B plane is readable');
    const bQ = pg(PG_B, "SELECT state, coalesce(metadata->>'close_reason','') AS cr FROM " + schemaB + ".tasks WHERE task_id='" + cid + "'");
    const bRow = rowsOf(bQ.out || '')[0];
    if (!aRow && !bRow) {
      ev2.push('no row for ' + cid + ' on either plane (A run plane was probed; B tasks: ' + RED(bQ.err || '0 rows') + ')');
    } else {
      if (aRow) ev2.push('A run ' + cid + ': status=' + aRow.status + ' close_reason=' + (aRow.cr || '(absent)'));
      if (bRow) ev2.push('B task ' + cid + ': state=' + bRow.state + ' close_reason=' + (bRow.cr || '(absent)'));
      const reason = (aRow && aRow.cr) ? aRow.cr : (bRow ? (bRow.cr || '') : '');
      const terminalWord = aRow ? String(aRow.status) : String(bRow.state || '');
      const modelOutcome = /model_finished/.test(terminalWord) || /COMPLETED/.test(terminalWord);
      const present = !!reason;
      const notFaked = reason !== 'model_finished';
      ok = present && (modelOutcome ? true : notFaked);
      ev2.push('checks: reason_present=' + present + ' outcome_is_model_verdict=' + modelOutcome + ' reason_not_model_finished=' + notFaked
        + ' => ' + (ok ? 'the reason tracks the outcome' : 'either no reason is journaled, or a non-model outcome claims model_finished (substitution)'));
    }
    verdict('A6b', bTitle, ok ? 'PASS' : 'FAIL', ev2.join(NL));
  } else {
    verdict('A6b', 'differential: a non-model terminal outcome does not report model_finished', 'FAIL',
      'not probed: pass --cancel-task <run-or-task-id> of a canceled/failed run (read-only re-read, nothing is created). Without this differential A6 can be satisfied by a constant.');
  }

  finish();
}

function finish() {
  const pass = results.filter((r) => r.verdict === 'PASS').length;
  const fail = results.filter((r) => r.verdict === 'FAIL').length;
  console.log('--- SUMMARY ' + JSON.stringify({ task: TASK, pass, fail, total: results.length }));
  console.log('J4-VERIFY: ' + (fail === 0 && pass === results.length ? 'PASS' : 'FAIL') + ' (' + pass + ' pass / ' + fail + ' fail)');
  console.log(JSON.stringify({ summary: results.map((r) => ({ id: r.id, verdict: r.verdict })) }));
  process.exitCode = fail === 0 && pass === results.length ? 0 : 1;
}

function safeJSON(s) { try { return JSON.parse(s || '{}') || {}; } catch { return {}; } }
function fmt(v) { return v === null || v === undefined ? '(unknown)' : String(v); }

function harvestLog(path, taskId) {
  const out = { steps: null, closeReason: '', maxSteps: null };
  try {
    const txt = readFileSync(path, 'utf8');
    const lines = txt.split(NL).filter((l) => (taskId ? l.includes(taskId) : true) && /close_reason|steps_consumed/.test(l));
    for (const l of lines) {
      const s = /steps_consumed[=":]+(\d+)/.exec(l); if (s) out.steps = Number(s[1]);
      const c = /close_reason[=":]+(\w+)/.exec(l); if (c) out.closeReason = c[1];
      const b = /step_budget[=":]+(\d+)/.exec(l); if (b) out.maxSteps = Number(b[1]);
    }
  } catch { /* absent log is reported by the assertion itself */ }
  return out;
}

function rpc(base, method, params, o) {
  const u = new URL(base);
  const body = JSON.stringify({ jsonrpc: '2.0', id: Date.now() % 1e9, method, params });
  const p = o.certDir;
  const opts = {
    host: u.hostname, port: Number(u.port || 443), method: 'POST', path: u.pathname || '/a2a/public',
    ca: readFileSync(p + '/ca.crt'), cert: readFileSync(p + '/client.crt'), key: readFileSync(p + '/client.key'),
    servername: 'localhost', rejectUnauthorized: false,
    headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body), authorization: 'Bearer ' + o.token },
    timeout: 15000,
  };
  return new Promise((res) => {
    const r = https.request(opts, (resp) => {
      let t = ''; resp.on('data', (d) => (t += d));
      resp.on('end', () => { let j = null; try { j = t ? JSON.parse(t) : null; } catch { } res({ status: resp.statusCode, json: j }); });
    });
    r.on('error', (e) => res({ status: 0, json: null, err: e.message }));
    r.on('timeout', () => { r.destroy(); res({ status: 0, json: null, err: 'timeout' }); });
    r.write(body); r.end();
  });
}

async function sendControl() {
  if (!A.control || !A['b-url'] || !A['cert-dir'] || !tokOf('token')) {
    return { ok: false, evidence: '--control also needs --b-url, --cert-dir, --token (the A principal)' };
  }
  const nonce = Date.now().toString(36);
  const sent = await rpc(A['b-url'], 'SendMessage', {
    message: { role: 'user', kind: 'message', messageId: 'h22-ctl-' + nonce, parts: [{ kind: 'text', text: 'Reply with the word ok. Do not call tools.' }] },
    configuration: { blocking: true },
  }, { certDir: A['cert-dir'], token: tokOf('token') });
  const t = sent.json && sent.json.result && (sent.json.result.task || sent.json.result);
  const id = t && t.id;
  if (!id) return { ok: false, evidence: 'control send failed: status=' + sent.status + ' ' + RED(JSON.stringify(sent.json || sent.err)).slice(0, 200) };
  const q = pg(PG_B, "SELECT metadata::text AS metadata FROM " + (A['schema-b'] || 'ahp_exec') + ".tasks WHERE task_id='" + id + "'");
  const row = rowsOf(q.out)[0]; const m = row ? safeJSON(row.metadata) : {};
  const ok = m.step_budget_source === 'deployment';
  return { ok, evidence: 'control task=' + id + ' step_budget_source=' + (m.step_budget_source || '(absent)') + ' effective=' + (m.step_budget_effective === undefined ? '(absent)' : m.step_budget_effective) + ' expected source=deployment with ' + DEPLOYMENT_BUDGET_DEFAULT };
}

main().catch((e) => { console.log('EXCEPTION ' + RED(e.stack || e.message).slice(0, 400)); process.exit(2); });
