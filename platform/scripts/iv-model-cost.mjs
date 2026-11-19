// Live leg for the Model Gateway cost read face (PDF rev3.2 :347 "стоимость
// задачи"), run against an ALREADY STARTED front -- it starts nothing, restarts
// nothing and writes nothing.
//
//   AHP_DEPLOYED_URL=https://127.0.0.1:29612 \
//   AHP_CERT_DIR=<stand's infra/certs, holding ca/{ca.crt,ca.key}> \
//   AHP_MODEL_COST_CREDS_FILE=<0600 provisioned users> \
//     node scripts/iv-model-cost.mjs [task_id]
//
// Credentials are CANONICAL, which is the whole point of this leg. A token minted
// locally by scripts/tokengen is a dev artifact; the cost face is guarded by
// transport.Task, which verifies a bearer against the published Identity keys and
// binds it to the mTLS certificate's workload -- so a dev-minted token is refused
// here ("identity: token carries an empty actor chain") and a leg built on one
// would prove nothing about the product. This leg therefore obtains its token
// through the supported flow, by invoking the canonical provider in-process:
//   login -> session cookie -> /u-ahp/v1/auth/token -> /v1/identity/control/exchange
// (tests/toolgw-live/scoped-token-provider.mjs). The flow is not duplicated here:
// duplication is how a leg drifts away from the product.
//
// The call certificate MUST carry the workload the exchange bound the token to
// (tenant of the provisioned human + the exchanger's deployment), because
// transport.Task compares the certificate's SPIFFE workload with the token's
// claims in both directions. Certificates are issued into AHP_CERT_OUT by
// infra/gen-client.sh against the TARGET stand's CA; nothing is written into the
// stand's own certificate directory.
//
// No credential substitution: if the creds file lacks a provisioned human for the
// requested tenant, the provider refuses and this leg FAILS. It never falls back
// to a locally minted token or to another tenant's human.
//
// Exit codes are deliberate, because "the face is not mounted on this build" and
// "the face is broken" must not look the same in a board:
//   0  the face answered, and every assertion held
//   3  the target build does not mount the face (AHP_ENABLE_MODEL_COST is unset)
//   1  a real failure: wrong status, wrong shape, or an isolation leak
//
// Nothing here prints a token, a password, or a response body beyond a short code.

import { execFileSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import https from 'node:https';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { PROJECT_ROOT } from '../tests/helpers/ahp-client.mjs';

const BASE = process.env.AHP_DEPLOYED_URL || 'https://127.0.0.1:29612';
const CERT_SRC = process.env.AHP_CERT_DIR || join(PROJECT_ROOT, 'infra', 'certs');
const CERT_OUT = process.env.AHP_CERT_OUT || join(tmpdir(), 'ahp-iv-model-cost-certs');
const CA_DIR = process.env.AHP_MODEL_COST_CA_DIR || join(CERT_SRC, 'ca');
const CA_CRT = join(CA_DIR, 'ca.crt');
const CREDS_FILE = process.env.AHP_MODEL_COST_CREDS_FILE || process.env.AHP_TOOLGW_CREDS_FILE || '';
const PROVIDER = join(PROJECT_ROOT, 'tests', 'toolgw-live', 'scoped-token-provider.mjs');
const GEN_CLIENT = join(PROJECT_ROOT, 'infra', 'gen-client.sh');

const TENANT = process.env.AHP_MODEL_COST_TENANT || 'djourney';
const FOREIGN_TENANT = process.env.AHP_MODEL_COST_FOREIGN_TENANT || 't-foreign';
const EXCH_DEP = process.env.AHP_MODEL_COST_EXCHANGER_DEPLOYMENT || 'space';
// "state" is what the face authorizes under; the rest are the accepted defaults
// of the canonical flow, kept so the exchange is the same one every other live
// leg performs.
const CAPS = process.env.AHP_MODEL_COST_CAPS || 'space:access,memory,tools,state';
const taskID = process.argv[2] || '';

const results = [];
const ok = (msg, detail = '') => results.push(['PASS', msg, detail]);
const bad = (msg, detail = '') => results.push(['FAIL', msg, detail]);
const finish = (code) => {
  let failed = 0;
  for (const [v, m, d] of results) {
    if (v === 'FAIL') failed++;
    console.error(`${v} ${m}${d ? ' :: ' + d : ''}`);
  }
  console.error(`SUMMARY pass=${results.length - failed} fail=${failed}`);
  process.exit(failed > 0 ? 1 : code);
};

function bail(step, detail) {
  console.error(`leg:${step}:${String(detail).slice(0, 200)}`);
  process.exit(1);
}

if (!existsSync(CA_CRT)) bail('env', `stand CA missing at ${CA_CRT} (set AHP_CERT_DIR)`);
if (!existsSync(PROVIDER)) bail('env', `canonical token provider missing at ${PROVIDER}`);
if (!CREDS_FILE || !existsSync(CREDS_FILE)) {
  bail('env', 'AHP_MODEL_COST_CREDS_FILE unset or missing: refusing to run without provisioned humans (no silent substitution)');
}

// scopedToken runs the canonical provider for one tenant and returns its token.
// The provider prints exactly one JWT on stdout and diagnostics on stderr, and it
// refuses to substitute a human from another tenant unless a mismatch mode is set
// explicitly -- this leg never sets it.
function scopedToken(tenant, caps = CAPS, { soft = false } = {}) {
  let out;
  try {
    out = execFileSync('node', [PROVIDER], {
      env: {
        ...process.env,
        AHP_DEPLOYED_URL: BASE,
        AHP_DEPLOYED_CA: CA_CRT,
        AHP_CA_DIR: CA_DIR,
        AHP_TOOLGW_KIND: 'task',
        AHP_TOOLGW_TENANT: tenant,
        AHP_TOOLGW_CREDS_FILE: CREDS_FILE,
        AHP_TOOLGW_CAPS: caps,
        AHP_TOOLGW_EXCHANGER_DEPLOYMENT: EXCH_DEP,
        AHP_TOOLGW_GEN_CLIENT: GEN_CLIENT,
      },
      maxBuffer: 1 << 20,
    }).toString().trim();
  } catch (e) {
    const detail = `tenant=${tenant} ${String(e.stderr || e.message).slice(0, 200)}`;
    if (soft) return null; // a negative leg treats "the exchange itself refused" as evidence
    bail('provider', detail);
  }
  if (!out || out.split('.').length !== 3) bail('provider', `tenant=${tenant} provider did not print a token`);
  return out;
}

// claimsOf decodes a JWT payload for an assertion about WHICH identity the leg
// actually holds. Signature verification is the front's job, not this leg's;
// nothing secret is printed -- only the identity labels.
function claimsOf(tok) {
  try {
    return JSON.parse(Buffer.from(tok.split('.')[1], 'base64url').toString('utf8'));
  } catch {
    return {};
  }
}

// clientFor issues (or reuses) a client certificate bound to this exact
// tenant/deployment, because the front enforces cert<->token binding.
function clientFor(tenant, deployment) {
  const out = join(CERT_OUT, `client-${tenant}-${deployment}`);
  if (!existsSync(join(out, 'client.crt'))) {
    execFileSync('bash', [GEN_CLIENT, tenant, deployment, out], {
      cwd: PROJECT_ROOT,
      stdio: 'ignore',
      env: { ...process.env, AHP_CA_DIR: CA_DIR },
    });
  }
  return { cert: join(out, 'client.crt'), key: join(out, 'client.key') };
}

function call(path, body, { token = '', cert = null, key = null, method = 'POST', timeoutMs = 20000 } = {}) {
  const payload = method === 'GET' ? '' : JSON.stringify(body ?? {});
  const u = new URL(BASE);
  const opts = {
    hostname: u.hostname,
    port: u.port || 443,
    path,
    method,
    agent: false,
    ca: readFileSync(CA_CRT),
    servername: 'localhost',
    rejectUnauthorized: true,
    headers: method === 'GET'
      ? {}
      : { 'content-type': 'application/json', 'content-length': Buffer.byteLength(payload) },
    timeout: timeoutMs,
  };
  if (token) opts.headers.authorization = `Bearer ${token}`;
  if (cert) {
    opts.cert = readFileSync(cert);
    opts.key = readFileSync(key);
  }
  return new Promise((resolve, reject) => {
    const req = https.request(opts, (res) => {
      let buf = '';
      res.on('data', (c) => { buf += c; });
      res.on('end', () => resolve({ status: res.statusCode, body: buf }));
    });
    req.on('error', reject);
    req.on('timeout', () => req.destroy(new Error('timed out')));
    if (payload) req.write(payload);
    req.end();
  });
}

// The canonical exchange binds the task token to the exchanger's own workload, so
// the call certificate must present that same workload for both tenants.
const home = { tenant: TENANT, token: scopedToken(TENANT) };
const foreign = { tenant: FOREIGN_TENANT, token: scopedToken(FOREIGN_TENANT) };
home.cert = clientFor(home.tenant, EXCH_DEP);
foreign.cert = clientFor(foreign.tenant, EXCH_DEP);

// The canonical claims name the workload deployment 'azp' and the credential kind
// 'token_kind' (pkg/auth); a leg that asserted bare 'deployment'/'kind' would read
// its own absence as a product failure.
for (const who of [home, foreign]) {
  const cl = claimsOf(who.token);
  const dep = cl.azp || cl.deployment || '';
  const kind = cl.token_kind || cl.kind || '';
  const subject = cl.sub || cl.subject || '';
  if (cl.tenant !== who.tenant) bad('token is not bound to the tenant the leg asked for', `want=${who.tenant} got=${cl.tenant || 'none'}`);
  else if (dep !== EXCH_DEP) bad('token workload does not match the call certificate', `want=${EXCH_DEP} got=${dep || 'none'}`);
  else if (!subject) bad('token carries no subject');
  else if (kind !== 'task') bad('cost face is a task surface: the token must be a task token', `kind=${kind || 'none'}`);
  else ok('canonical scoped task token obtained', `tenant=${cl.tenant} workload=${dep} subject=${subject} actor_chain=${Array.isArray(cl.act) ? cl.act.length : 0}`);
}

const health = await call('/healthz', undefined, { cert: home.cert.cert, key: home.cert.key, method: 'GET' });
if (health.status !== 200) {
  bad(`front unreachable at ${BASE}`, `healthz=${health.status}`);
  finish(1);
}
ok('target front reachable over mTLS', `${BASE}`);

const ask = (who, tid) => call('/v1/model/TreeCost', { task_id: tid },
  { token: who.token, cert: who.cert.cert, key: who.cert.key });

// A task id that exists nowhere: the honest default probe.
const probeID = taskID || `h21-cost-leg-${Date.now()}`;
const probe = await ask(home, probeID);

if (probe.status === 404 && /page not found/i.test(probe.body)) {
  results.length = 0;
  ok('front reachable', BASE);
  bad('TreeCost is not mounted on this build',
    'the mux has no /v1/model/ prefix: start the front with AHP_ENABLE_MODEL_COST=1 and rerun (exit 3 means "not mounted", not "broken")');
  finish(3);
}
if (probe.status === 401 || probe.status === 403) {
  bad('the canonical scoped token was refused by the cost face',
    `status=${probe.status} body=${probe.body.slice(0, 200).replace(/\s+/g, ' ')}`);
  finish(1);
}
if (!taskID) {
  if (probe.status === 404) ok('absent task answers 404 from the ledger, not a stub',
    probe.body.slice(0, 120).replace(/\s+/g, ' '));
  else bad('absent task must answer 404', `status=${probe.status} body=${probe.body.slice(0, 160)}`);
}

// The guard must be the real one: no bearer means no request, even holding a
// valid workload certificate.
const noTok = await call('/v1/model/TreeCost', { task_id: probeID },
  { cert: home.cert.cert, key: home.cert.key });
if (noTok.status === 403 && /bearer token/i.test(noTok.body)) ok('workload certificate alone is not enough (bearer required)');
else bad('certificate-only request must be refused as unauthenticated', `status=${noTok.status} body=${noTok.body.slice(0, 140)}`);

// And the reverse: a valid token without its certificate must not survive mTLS.
try {
  const noCert = await call('/v1/model/TreeCost', { task_id: probeID }, { token: home.token });
  bad('token without a workload certificate was not refused at the transport', `status=${noCert.status}`);
} catch (e) {
  if (/certificate/i.test(String(e.message))) ok('token without a workload certificate dies at mTLS (binding is enforced below the handler)');
  else bad('unexpected transport error', String(e.message).slice(0, 120));
}

// Isolation with a REAL foreign principal: a provisioned human of another tenant,
// exchanged through the same supported flow, so the denial is a cross-tenant fact
// rather than an artifact of a locally minted token.
const asForeign = await ask(foreign, probeID);
if (asForeign.status === 404) ok('foreign principal gets the absent-task answer (no existence signal)',
  asForeign.body.slice(0, 120).replace(/\s+/g, ' '));
else bad('foreign principal must get 404', `status=${asForeign.status} body=${asForeign.body.slice(0, 140)}`);

// The capability the face authorizes under has to be enforced by the grant, not
// assumed by the route: ask with a token exchanged WITHOUT "state" and the answer
// must be a denial that names the capability. If the exchange itself refuses to
// mint such a token, that is the same proof from the other side.
const weaker = scopedToken(home.tenant, CAPS.split(',').filter((c) => c.trim() !== 'state').join(','), { soft: true });
if (weaker === null) {
  ok('a grant without the cost capability cannot even be exchanged');
} else {
  const noCap = await call('/v1/model/TreeCost', { task_id: probeID },
    { token: weaker, cert: home.cert.cert, key: home.cert.key });
  if (noCap.status === 403 && /capability/i.test(noCap.body)) {
    ok('grant without the cost capability is refused, naming the capability',
      noCap.body.slice(0, 120).replace(/\s+/g, ' '));
  } else {
    bad('a grant lacking the cost capability must be refused',
      `status=${noCap.status} body=${noCap.body.slice(0, 140)}`);
  }
}

// Positive path, only when a real task is offered: the figure must be addressed to
// that task, carry its as-of time, declare its units, and agree with the ledger.
if (taskID) {
  const mine = await ask(home, taskID);
  if (mine.status !== 200) {
    bad('TreeCost must answer 200 for a task this tenant spent on',
      `task=${taskID} status=${mine.status} body=${mine.body.slice(0, 200).replace(/\s+/g, ' ')}`);
  } else {
    let out = {};
    try { out = JSON.parse(mine.body); } catch { bad('answer is not JSON', mine.body.slice(0, 160)); }
    if (out.task_id !== taskID) bad('answer is not about the asked task', `got=${String(out.task_id).slice(0, 40)}`);
    else ok('answer is addressed to the asked task_id');
    if (!Array.isArray(out.trees) || out.trees.length === 0) bad('trees empty for a task with recorded spend', `n=${(out.trees || []).length}`);
    else {
      const trees = out.trees;
      const sum = (f) => trees.reduce((a, t) => a + (Number(t[f]) || 0), 0);
      ok('trees present', `n=${trees.length} roots=${trees.map((t) => String(t.tree_root || '?').slice(0, 40)).join('|')}`);
      ok('tree figures returned',
        `steps=${sum('steps')} child_steps=${sum('child_steps')} tokens_in=${sum('tokens_in')} tokens_out=${sum('tokens_out')} cost_units=${sum('cost_units')}`);
      for (const t of trees) {
        if (t.reconciles === false || (Array.isArray(t.mismatches) && t.mismatches.length)) {
          bad('ledger and generations disagree', `root=${String(t.tree_root).slice(0, 40)} mismatches=${JSON.stringify(t.mismatches || []).slice(0, 120)}`);
        }
      }
    }
    if (!Array.isArray(out.units)) bad('units missing: without them a reader may add tokens and steps together');
    else ok('units declared', out.units.join(',') || 'none');
    if (!out.generated_at) bad('generated_at missing: a cost figure with no as-of time is not reproducible');
    else ok('answer carries an as-of time', out.generated_at);
  }

  // The same id, asked by the foreign principal, must answer as absent -- while
  // the owner sees the numbers above. This is the isolation assertion that matters.
  const fKnown = await ask(foreign, taskID);
  if (fKnown.status === 404) ok('foreign principal cannot read this tenant\'s cost tree (404)');
  else bad('foreign principal must not read another tenant\'s tree', `status=${fKnown.status} body=${fKnown.body.slice(0, 140)}`);
}

if (!taskID) {
  // Say plainly what this run did NOT exercise: without a task id there is no
  // figure to reconcile, so the 200-with-numbers path stays unproven by THIS run.
  // The precondition is out of this leg's hands: the ledger must hold a tree under
  // the tenant this token is bound to (check ahp_model.budgets out of band).
  console.error('NOTE positive path not exercised: no task_id argument, so no figure was reconciled with the ledger');
}
finish(0);
