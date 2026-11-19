// Contract test for scripts/identity-task-token.mjs against a fixture origin that
// enforces the same wire rules the identity plane does: HttpOnly cookie session,
// cookie-only bearer refusal, and an exchange that answers only to a named exchanger
// deployment. The real product path is proven separately against a live stand; this
// test pins the tool's own behaviour (cookie handling, refusal surfacing, output).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { spawn } from 'node:child_process';
import { serverNameFor } from './identity-task-token.mjs';
import { mkdtempSync, writeFileSync, chmodSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';

const TOOL = path.join(import.meta.dirname, 'identity-task-token.mjs');
const USERS = [{ username: 'probe-owner', password: 'p-1234567890', subject: 'probe-owner', tenant: 't1' }];

// The tool reads humans from a 0600 file (the only credential source), so the fixture
// writes one instead of passing credentials through env.
const dir = mkdtempSync(tmpdir() + '/h24-creds-');
// Build-hygiene (ENOSPC incident 2026-11-09): the credential fixture must never
// outlive the test process, on success or on failure.
const cleanCredsDir = () => { try { rmSync(dir, { recursive: true, force: true }); } catch {} };
process.on('exit', cleanCredsDir);
process.on('SIGINT', () => { cleanCredsDir(); process.exit(130); });
process.on('SIGTERM', () => { cleanCredsDir(); process.exit(143); });
const usersFile = dir + '/users.json';
writeFileSync(usersFile, JSON.stringify(USERS)); chmodSync(usersFile, 0o600);

function startFixture({ exchanger = true, permissionDenied = false } = {}) {
  const srv = http.createServer((req, res) => {
    let body = '';
    req.on('data', c => body += c);
    req.on('end', () => {
      const send = (code, obj, headers = {}) => { res.writeHead(code, { 'content-type': 'application/json', ...headers }); res.end(JSON.stringify(obj)); };
      if (req.url === '/u-ahp/v1/auth/login') {
        const b = JSON.parse(body || '{}');
        if (b.username !== USERS[0].username || b.password !== USERS[0].password) return send(401, { code: 'UNAUTHENTICATED', message: 'invalid credentials' });
        return send(200, { ok: true }, { 'set-cookie': 'ahp_browser_session=sess-1; HttpOnly; SameSite=Strict; Path=/' });
      }
      if (req.url === '/u-ahp/v1/auth/token') {
        if (!/ahp_browser_session=sess-1/.test(req.headers.cookie || '') || req.headers['x-ahp-session'] !== '1') return send(401, { code: 'UNAUTHENTICATED', message: 'no session' });
        return send(200, { access_token: 'human-token-abc', token_type: 'Bearer', expires_in: 600, subject: 'probe-owner', tenant: 't1' });
      }
      if (req.url === '/v1/identity/control/exchange') {
        if (req.headers.cookie) return send(400, { code: 'INVALID', message: 'exchange must not carry a browser cookie' });
        const b = JSON.parse(body || '{}');
        if (b.human_token !== 'human-token-abc') return send(403, { code: 'FORBIDDEN', reason: 'token_use_invalid', message: 'a live browser human token is required' });
        if (!exchanger) return send(403, { code: 'FORBIDDEN', reason: 'azp_mismatch', message: 'deployment is not an authorized exchanger' });
        if (permissionDenied) return send(403, { code: 'FORBIDDEN', reason: 'exchange_permission_missing', message: 'subject lacks the exchange entitlement' });
        if (!b.task) return send(400, { code: 'INVALID', message: 'human_token and task are required' });
        return send(200, { token: 'scoped-task-token-xyz', task: 'narrowed-task' });
      }
      send(404, { code: 'NOT_FOUND' });
    });
  });
  return new Promise(r => srv.listen(0, '127.0.0.1', () => r({ srv, port: srv.address().port })));
}

function run(env, args = []) {
  return new Promise(resolve => {
    const p = spawn(process.execPath, [TOOL, ...args], { env: { ...process.env, ...env }, stdio: ['ignore', 'pipe', 'pipe'] });
    let out = '', err = '';
    p.stdout.on('data', c => out += c); p.stderr.on('data', c => err += c);
    p.on('close', code => resolve({ code, out, err }));
  });
}

test('happy path yields the token the server bound the task to', async () => {
  const { srv, port } = await startFixture();
  try {
    const r = await run({ AHP_ORIGIN: `http://127.0.0.1:${port}`, AHP_ACCEPTANCE_USERS_FILE: usersFile, AHP_TASK_IGNORE: '1' }, ['--task', 'task-a']);
    assert.equal(r.code, 0, r.out + r.err);
    const j = JSON.parse(r.out);
    assert.equal(j.ok, true);
    assert.equal(j.token, 'scoped-task-token-xyz');
    // the tool must report the task the token is ACTUALLY bound to, not the one asked for
    assert.equal(j.task, 'narrowed-task');
    assert.equal(j.requested_task, 'task-a');
    assert.equal(j.headers.Authorization, 'Bearer scoped-task-token-xyz');
    assert.ok(j.human_token_fp && j.human_token_fp.length === 12);
    assert.ok(!r.out.includes(USERS[0].password), 'password leaked to stdout');
    assert.ok(!r.out.includes('human-token-abc'), 'human token leaked to stdout');
  } finally { srv.close(); }
});

test('refusal keeps the machine-readable exchange reason', async () => {
  const { srv, port } = await startFixture({ exchanger: false });
  try {
    const r = await run({ AHP_ORIGIN: `http://127.0.0.1:${port}`, AHP_ACCEPTANCE_USERS_FILE: usersFile }, ['--task', 'task-b']);
    assert.equal(r.code, 5);
    const j = JSON.parse(r.out);
    assert.equal(j.stage, 'exchange');
    assert.equal(j.reason, 'azp_mismatch');
  } finally { srv.close(); }
});

test('bad credentials stop at login and never print the password', async () => {
  const { srv, port } = await startFixture();
  const badUsersFile = dir + '/bad.json';
  writeFileSync(badUsersFile, JSON.stringify([{ ...USERS[0], password: 'wrong-password-here' }])); chmodSync(badUsersFile, 0o600);
  try {
    const r = await run({ AHP_ORIGIN: `http://127.0.0.1:${port}`, AHP_ACCEPTANCE_USERS_FILE: badUsersFile });
    assert.equal(r.code, 3);
    assert.ok(!r.out.includes('wrong-password-here'));
  } finally { srv.close(); }
});

test('https origin without a workload certificate fails as configuration, not as a 200', async () => {
  const r = await run({ AHP_ORIGIN: 'https://127.0.0.1:1', AHP_ACCEPTANCE_USERS_FILE: usersFile });
  assert.equal(r.code, 2);
  assert.equal(JSON.parse(r.out).stage, 'config');
});

test('TLS server name: IP origin gets no SNI, named origin keeps it, override wins', () => {
  assert.equal(serverNameFor('127.0.0.1', ''), undefined);
  assert.equal(serverNameFor('::1', ''), undefined);
  assert.equal(serverNameFor('ahp-front.internal', ''), 'ahp-front.internal');
  assert.equal(serverNameFor('127.0.0.1', 'ahp-composed-backend'), 'ahp-composed-backend');
});

test('a credential source that is not 0600 is refused before its contents are read', async () => {
  const loose = dir + '/loose.json';
  writeFileSync(loose, JSON.stringify(USERS)); chmodSync(loose, 0o644);
  const r = await run({ AHP_ORIGIN: 'http://127.0.0.1:1', AHP_ACCEPTANCE_USERS_FILE: loose });
  assert.equal(r.code, 2);
  const j = JSON.parse(r.out);
  assert.equal(j.stage, 'config');
  assert.match(j.message, /0600/);
  assert.ok(!r.out.includes(USERS[0].password));
});

test('no credential source at all is a configuration failure, not a silent empty run', async () => {
  const r = await run({ AHP_ORIGIN: 'http://127.0.0.1:1', AHP_ACCEPTANCE_USERS_FILE: '' });
  assert.equal(r.code, 2);
  assert.match(JSON.parse(r.out).message, /AHP_ACCEPTANCE_USERS_FILE/);
});
