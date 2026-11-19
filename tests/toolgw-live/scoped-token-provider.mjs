// scoped-token-provider.mjs -- harness provider of REAL Identity credentials for
// TestToolGatewayLive (AHP_TOOLGW_TOKEN_SOURCE=external). It is the bridge the
// director's contract specifies: only the supported flow is used --
//   login -> session cookie -> /u-ahp/v1/auth/token -> /v1/identity/control/exchange
// -- exactly the flow helper20 used for the memory live leg (the flow logic is a
// faithful adaptation of /tmp/h20mem/identity-token.mjs, kept in-repo so the leg
// is reproducible without /tmp archaeology).
//
// Contract (tests/registry-triggers/toolgw_live_test.go externalToken):
//   in : AHP_TOOLGW_KIND=task|human, AHP_TOOLGW_TENANT, AHP_TOOLGW_DEPLOYMENT,
//        AHP_TOOLGW_SUBJECT (advisory: Identity derives identity from the human
//        session and the exchanger certificate, headers/bodies never override it),
//        AHP_DEPLOYED_URL, AHP_DEPLOYED_CA (file or dir), AHP_CA_DIR,
//        AHP_TOOLGW_CREDS_FILE (0600 JSON array of provisioned users),
//        AHP_TOOLGW_CAPS (default space:access,memory,tools),
//        AHP_TOOLGW_EXCHANGER_DEPLOYMENT (default space),
//        AHP_TOOLGW_ALLOW_TENANT_MISMATCH=1 (documented refusal-boundary mode),
//        AHP_TOOLGW_GEN_CLIENT (path to infra/gen-client.sh).
//   out: exactly one raw JWT on stdout; diagnostics on stderr only; tokens are
//        NEVER printed.
import https from 'node:https';
import { readFileSync, writeFileSync, existsSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFileSync } from 'node:child_process';

const die = (step, detail) => {
  process.stderr.write(`provider:${step}:${String(detail).slice(0, 200)}\n`);
  process.exit(1);
};

const url = new URL(process.env.AHP_DEPLOYED_URL || die('env', 'AHP_DEPLOYED_URL unset'));
const HOST = url.hostname;
const PORT = Number(url.port || 443);
let CA = process.env.AHP_DEPLOYED_CA || '';
if (CA && !CA.endsWith('.crt') && !CA.endsWith('.pem')) CA = join(CA, 'ca.crt');
if (!CA || !existsSync(CA)) die('env', 'AHP_DEPLOYED_CA (ca.crt) missing');
const KIND = process.env.AHP_TOOLGW_KIND || die('env', 'AHP_TOOLGW_KIND unset');
const TENANT = process.env.AHP_TOOLGW_TENANT || '';
const CREDS_FILE = process.env.AHP_TOOLGW_CREDS_FILE || die('env', 'AHP_TOOLGW_CREDS_FILE unset');
const CAPS = (process.env.AHP_TOOLGW_CAPS || 'space:access,memory,tools').split(',');
const EXCH_DEP = process.env.AHP_TOOLGW_EXCHANGER_DEPLOYMENT || 'space';
const GEN_CLIENT = process.env.AHP_TOOLGW_GEN_CLIENT || die('env', 'AHP_TOOLGW_GEN_CLIENT unset');

let users;
try { users = JSON.parse(readFileSync(CREDS_FILE, 'utf8')); } catch (e) { die('creds', e.message); }
if (!Array.isArray(users)) users = [users];
let user = users.find((u) => !TENANT || u.tenant === TENANT);
if (!user) {
  if (process.env.AHP_TOOLGW_ALLOW_TENANT_MISMATCH !== '1') {
    die('creds', `no provisioned human for tenant ${TENANT}; refusing (no silent substitution)`);
  }
  user = users[0]; // documented refusal-boundary mode: the request CANNOT be
  // authorized as this tenant; the leg asserts the refusal, never the success.
}

function req({ path, body, tok, cookie, certDir }) {
  return new Promise((resolve, reject) => {
    const payload = body === undefined ? '' : JSON.stringify(body);
    const headers = { 'content-type': 'application/json' };
    if (payload) headers['content-length'] = Buffer.byteLength(payload);
    if (tok) headers['authorization'] = 'Bearer ' + tok;
    if (cookie) headers['cookie'] = cookie;
    if (path.includes('/auth/')) headers['X-AHP-Session'] = '1';
    const opts = { host: HOST, port: PORT, path, method: 'POST', ca: readFileSync(CA), headers, servername: 'localhost' };
    if (certDir && existsSync(join(certDir, 'client.crt'))) {
      opts.cert = readFileSync(join(certDir, 'client.crt'));
      opts.key = readFileSync(join(certDir, 'client.key'));
    }
    const r = https.request(opts, (res) => {
      let d = '';
      res.on('data', (c) => (d += c));
      res.on('end', () => {
        let j = null; try { j = JSON.parse(d); } catch { j = { raw: d.slice(0, 120) }; }
        resolve({ status: res.statusCode, json: j, setCookie: res.headers['set-cookie'] });
      });
    });
    r.on('error', reject);
    if (payload) r.write(payload);
    r.end();
  });
}

// exchanger workload certificate for the human's tenant (deployment must be on
// the stand's exchanger allowlist); minted under the STAND CA, cached per run.
const certDir = mkdtempSync(join(tmpdir(), 'h23-exch-'));
// Build-hygiene (ENOSPC incident 2026-11-09): workload certs+keys minted here must
// not linger in the system temp dir after this provider exits (any exit path).
const cleanCertDir = () => { try { rmSync(certDir, { recursive: true, force: true }); } catch {} };
process.on('exit', cleanCertDir);
process.on('SIGINT', () => { cleanCertDir(); process.exit(130); });
process.on('SIGTERM', () => { cleanCertDir(); process.exit(143); });
try {
  execFileSync('bash', [GEN_CLIENT, user.tenant, EXCH_DEP, certDir], { stdio: 'ignore' });
} catch (e) { die('gen_client', e.message); }

let r = await req({ path: '/u-ahp/v1/auth/login', body: { username: user.username, password: user.password }, certDir });
if (r.status !== 200) die('login', r.status + ' ' + (r.json?.code || ''));
const cookie = (r.setCookie || []).map((c) => c.split(';')[0]).join('; ');

r = await req({ path: '/u-ahp/v1/auth/token', body: {}, cookie, certDir });
if (r.status !== 200) die('auth_token', r.status + ' ' + (r.json?.code || ''));
const human = r.json.access_token || r.json.token;
if (!human) die('auth_token', 'shape');

if (KIND === 'human') { process.stdout.write(human); process.exit(0); }
if (KIND !== 'task') die('env', 'AHP_TOOLGW_KIND must be task|human');

r = await req({
  path: '/v1/identity/control/exchange', certDir,
  body: { human_token: human, task: 'h23toolgw-' + Date.now() + '-' + Math.floor(Math.random() * 1e6), capabilities: CAPS },
});
if (r.status !== 200) die('exchange', r.status + ' ' + (r.json?.code || ''));
const scoped = r.json.token;
if (!scoped) die('exchange', 'shape');
process.stdout.write(scoped);
