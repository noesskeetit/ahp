#!/usr/bin/env node
// identity-task-token.mjs — the standard (штатный) path from a browser human login
// to a SCOPED TASK TOKEN, so a live leg stops hand-rolling credentials.
//
//   1. POST /u-ahp/v1/auth/login      {username,password}  -> HttpOnly session cookie
//   2. POST /u-ahp/v1/auth/token      cookie + x-ahp-session -> human access token (aud=ahp-browser)
//   3. POST /v1/identity/control/exchange  {human_token,task[,context,capabilities]}
//      presented with THIS process's own workload mTLS certificate, whose deployment
//      must be named by the server's AHP_IDENTITY_EXCHANGER_DEPLOYMENTS.
//      -> {token, task}: the task the token is ACTUALLY bound to (narrowed when the
//      grant was attenuated — frozen Tier-2 contract).
//
// Output: one JSON object on stdout: {token, task, headers:{Authorization}, subject,
// tenant, origin, human_expires_in}. The task token is a bearer credential: callers
// must not log stdout. Passwords and human tokens are never printed (only a sha256
// prefix of the human token, for correlation).
//
// Config (env, or the flags below):
//   AHP_ORIGIN            default http://127.0.0.1:29612
//   AHP_ACCEPTANCE_USERS_FILE   REQUIRED. Path to the 0600 JSON array the operator
//                        provisioned administratively (there is no product endpoint that
//                        creates humans): [{username,password,subject,tenant,...}].
//                        This file is the ONLY credential source by design - passing a
//                        password in argv or in a committed env line is how demo creds end
//                        up in shell history and journals.
//   AHP_TEST_USER_INDEX  which entry to use (default 0)
//   AHP_TLS_CLIENT_CERT / AHP_TLS_CLIENT_KEY / AHP_TLS_CA
//                         required for https origins: this process's workload identity
//   AHP_UPSTREAM_SERVERNAME  TLS server name (default: origin hostname)
// Flags: --origin --user-index --task --context --capabilities a,b,c --quiet
//
// Exit codes: 0 ok · 2 configuration · 3 login · 4 token · 5 exchange refused · 6 transport
import http from 'node:http';
import https from 'node:https';
import fs from 'node:fs';
import crypto from 'node:crypto';
import { pathToFileURL } from 'node:url';


// serverNameFor decides the TLS SNI/server name. Node refuses an IP literal as
// servername, and an https origin is frequently 127.0.0.1, so an IP host must get
// no server name at all (verification then falls to the certificate's IP SAN) while
// a named origin can be overridden by AHP_UPSTREAM_SERVERNAME (the workload name the
// stand put in its own certificate).
export function serverNameFor(host, override) {
  if (override) return override;
  if (!host) return undefined;
  const ip = host.match(/^\[?(\d{1,3}(\.\d{1,3}){3}|[0-9a-fA-F:.]+)\]?$/);
  if (ip && (ip[1].includes('.') || ip[1].includes(':'))) return undefined;
  return host;
}

export function parseArgs(argv, env = process.env) {
  const cfg = {
    origin: env.AHP_ORIGIN || 'http://127.0.0.1:29612',
    usersFile: env.AHP_ACCEPTANCE_USERS_FILE || '',
    userIndex: Number(env.AHP_TEST_USER_INDEX || 0),
    task: '', context: '', capabilities: null, quiet: false, redact: false,
    cert: env.AHP_TLS_CLIENT_CERT || '', key: env.AHP_TLS_CLIENT_KEY || '', ca: env.AHP_TLS_CA || '',
    servername: env.AHP_UPSTREAM_SERVERNAME || '',
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--origin') cfg.origin = argv[++i];
    else if (a === '--users-file') cfg.usersFile = argv[++i];
    else if (a === '--user-index') cfg.userIndex = Number(argv[++i]);
    else if (a === '--task') cfg.task = argv[++i];
    else if (a === '--context') cfg.context = argv[++i];
    else if (a === '--capabilities') cfg.capabilities = argv[++i].split(',').map(s => s.trim()).filter(Boolean);
    else if (a === '--quiet') cfg.quiet = true;
    else if (a === '--redact') cfg.redact = true;
    else throw new Error('unknown flag ' + a);
  }
  return cfg;
}

// One request, cookies handled by us so the session never leaks into a global jar.
function request(origin, path, { method = 'POST', body, headers = {}, agent = null } = {}) {
  return new Promise((resolve, reject) => {
    const u = new URL(origin);
    const lib = u.protocol === 'https:' ? https : http;
    const payload = body === undefined ? null : Buffer.from(JSON.stringify(body));
    const h = { ...headers };
    if (payload) { h['content-type'] = 'application/json'; h['content-length'] = String(payload.length); }
    const req = lib.request({
      protocol: u.protocol, hostname: u.hostname, port: u.port || (u.protocol === 'https:' ? 443 : 80),
      path, method, headers: h, agent,
      servername: u.protocol === 'https:' ? serverNameFor(u.hostname, process.env.AHP_UPSTREAM_SERVERNAME) : undefined,
      rejectUnauthorized: process.env.AHP_TLS_SKIP_VERIFY === '1' ? false : true,
    }, res => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({
        status: res.statusCode, headers: res.headers,
        setCookie: (res.headers['set-cookie'] || []).map(c => c.split(';')[0]).join('; '),
        text: Buffer.concat(chunks).toString('utf8'),
      }));
    });
    req.on('error', reject);
    if (payload) req.write(payload);
    req.end();
  });
}

function fail(stage, res) {
  let code = null, message = null, reason = null;
  try { const j = JSON.parse(res.text); code = j.code || null; message = j.message || null; reason = j.reason || j.details?.reason || null; } catch (_) { message = res.text.slice(0, 200); }
  process.stdout.write(JSON.stringify({ ok: false, stage, status: res.status, code, reason, message }) + '\n');
  process.exit(stage === 'exchange' ? 5 : stage === 'token' ? 4 : 3);
}

export async function main(argv = process.argv.slice(2), env = process.env) {
  const cfg = parseArgs(argv, env);
  if (!cfg.usersFile) {
    process.stdout.write(JSON.stringify({ ok: false, stage: 'config', message: 'AHP_ACCEPTANCE_USERS_FILE must point at the 0600 file the operator provisioned humans into (the only credential source)' }) + '\n');
    process.exit(2);
  }
  // Check the file is not world/group readable before touching its contents: a
  // credential source that anyone can read is a defect this tool refuses to work with.
  const st = fs.statSync(cfg.usersFile);
  if (st.mode & 0o077) {
    process.stdout.write(JSON.stringify({ ok: false, stage: 'config', message: 'credential file must be 0600 (group/other bits set)', path_mode: (st.mode & 0o777).toString(8) }) + '\n');
    process.exit(2);
  }
  let users;
  try { users = JSON.parse(fs.readFileSync(cfg.usersFile, 'utf8')); }
  catch (e) {
    process.stdout.write(JSON.stringify({ ok: false, stage: 'config', message: 'cannot read users file: ' + String(e.message).slice(0, 120) }) + '\n');
    process.exit(2);
  }
  if (!Array.isArray(users) || users.length === 0) {
    process.stdout.write(JSON.stringify({ ok: false, stage: 'config', message: 'users file must hold a non-empty JSON array' }) + '\n');
    process.exit(2);
  }
  const user = users[cfg.userIndex];
  if (!user || !user.username || !user.password) {
    process.stdout.write(JSON.stringify({ ok: false, stage: 'config', message: 'entry at AHP_TEST_USER_INDEX lacks username/password' }) + '\n');
    process.exit(2);
  }
  const task = cfg.task || `identity-task-token-${new Date().toISOString().replace(/[-:.]/g, '').slice(0, 17)}Z`;
  let agent = null;
  if (cfg.origin.startsWith('https://')) {
    if (!cfg.cert || !cfg.key || !cfg.ca) {
      process.stdout.write(JSON.stringify({ ok: false, stage: 'config', message: 'https origin needs AHP_TLS_CLIENT_CERT/KEY/CA (workload identity: infra/gen-client.sh)' }) + '\n');
      process.exit(2);
    }
    agent = new https.Agent({
      cert: fs.readFileSync(cfg.cert), key: fs.readFileSync(cfg.key), ca: fs.readFileSync(cfg.ca),
      servername: serverNameFor(new URL(cfg.origin).hostname, cfg.servername), keepAlive: false,
    });
  }
  const send = (path, body, headers) => request(cfg.origin, path, { body, headers: { 'x-ahp-session': '1', ...(headers || {}) }, agent });
  try {
    const login = await send('/u-ahp/v1/auth/login', { username: user.username, password: user.password });
    if (login.status !== 200) fail('login', login);
    if (!login.setCookie) fail('login', { status: 200, text: 'login returned no session cookie', headers: {} });
    const cookie = login.setCookie;
    // /auth/token is a cookie-exchange: the session cookie is the credential, no body
    // and no bearer header travel here (matching the frozen browser contract).
    const tok = await request(cfg.origin, '/u-ahp/v1/auth/token', {
      headers: { 'x-ahp-session': '1', cookie }, agent,
    });
    if (tok.status !== 200) fail('token', tok);
    const human = JSON.parse(tok.text);
    if (!human.access_token) fail('token', { status: tok.status, text: 'no access_token', headers: {} });
    const exBody = { human_token: human.access_token, task };
    if (cfg.context) exBody.context = cfg.context;
    if (cfg.capabilities) exBody.capabilities = cfg.capabilities;
    // The exchange is a control-plane call: it carries NO session cookie and NO human
    // bearer header — the human credential travels in the body, the workload identity
    // travels in the TLS certificate.
    const ex = await request(cfg.origin, '/v1/identity/control/exchange', { body: exBody, agent });
    if (ex.status !== 200) fail('exchange', ex);
    const scoped = JSON.parse(ex.text);
    process.stdout.write(JSON.stringify({
      ok: true, origin: cfg.origin, subject: human.subject || user.subject || null,
      tenant: human.tenant || user.tenant || null,
      task: scoped.task, requested_task: task,
      // --redact keeps the run log safe: a bearer credential must not end up in a
      // transcript by accident. Without it the token is printed, because piping it
      // to the next call is the point of this tool.
      token: cfg.redact ? `[redacted len=${scoped.token.length}]` : scoped.token,
      headers: { Authorization: cfg.redact ? 'Bearer [redacted]' : `Bearer ${scoped.token}` },
      human_expires_in: human.expires_in ?? null,
      human_token_fp: crypto.createHash('sha256').update(human.access_token).digest('hex').slice(0, 12),
    }) + '\n');
  } catch (e) {
    process.stdout.write(JSON.stringify({ ok: false, stage: 'transport', message: String(e.message || e).slice(0, 200) }) + '\n');
    process.exit(6);
  }
}

// Compare via pathToFileURL: a repo path containing spaces percent-encodes in
// import.meta.url, so a string prefix comparison would silently never run.
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) main();
