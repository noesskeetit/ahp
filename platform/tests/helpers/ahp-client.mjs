// Shared mTLS + task-token client for delivery e2e/chaos/load tests.
//
// This helper is the out-of-process twin of the server's devAuth: it derives the
// SAME Ed25519 dev key from a deterministic seed, builds the tokengen binary to
// mint KindTask tokens, spawns the ahp service over real mTLS, and issues
// POST /v1/state/<Op> requests with a client certificate carrying a SPIFFE
// workload identity (and, for task routes, a bound bearer token).
//
// SECURITY: development-only. The dev seed is derived (never committed), secrets
// are never printed, and every spawned process is pointed at the block-isolated
// database. Do not reuse outside local testing.

import { execFileSync, spawn } from 'node:child_process';
import { createHash, X509Certificate } from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
import https from 'node:https';
import http from 'node:http';
import { dirname, isAbsolute, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { buildEnv } from '../../scripts/ahp-env.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));
export const PROJECT_ROOT = resolve(HERE, '..', '..');

// Deterministic dev seed. sha256 of a fixed marker, hex-encoded (64 hex chars).
// Shared with the server via AHP_DEV_JWT_SEED so both derive the identical key.
export function devSeedHex() {
  return createHash('sha256').update('ahp-hive-delivery-dev-jwt-seed-v1').digest('hex');
}

// Go build environment (repo-uniform cache locations).
const GO_ENV = {
  GOCACHE: '/tmp/ahp-gocache',
  GOMODCACHE: '/tmp/ahp-modcache',
  GOPATH: '/tmp/ahp-gopath',
};

// Certificate locations are overridable so an acceptance run can be reproduced from a
// checkout that never generated material: infra/certs is git-ignored, so a fresh clone
// has neither a CA nor a private key, and the only thing it can legitimately do is
// PRESENT ALREADY-ISSUED client certs + the stand's public CA. AHP_CLIENT_CERT_DIR does
// that; it never relaxes what the server checks (see clientIdentity below).
export const CERT_DIR = process.env.AHP_CERT_DIR
  ? resolve(process.env.AHP_CERT_DIR)
  : join(PROJECT_ROOT, 'infra', 'certs');
export const CLIENT_CERT_DIR = process.env.AHP_CLIENT_CERT_DIR
  ? resolve(process.env.AHP_CLIENT_CERT_DIR)
  : '';

// Trust anchor for the stand under test: AHP_DEPLOYED_CA (file) > AHP_DEPLOYED_CA_DIR >
// this checkout's dev CA.
export function caFile() {
  if (process.env.AHP_DEPLOYED_CA) return process.env.AHP_DEPLOYED_CA;
  if (process.env.AHP_DEPLOYED_CA_DIR) return join(process.env.AHP_DEPLOYED_CA_DIR, 'ca.crt');
  return join(CERT_DIR, 'ca', 'ca.crt');
}

// Can this environment mint NEW client certs? Minting needs the CA private key, which
// is deliberately not shared with acceptance checkouts. Callers must report legs that
// require a fresh identity as SKIPPED rather than reusing the pinned cert and calling
// it a cross-tenant proof.
export function canMint() {
  const dir = process.env.AHP_CA_DIR ? resolve(process.env.AHP_CA_DIR) : join(CERT_DIR, 'ca');
  const crt = process.env.AHP_CA_FILE || join(dir, 'ca.crt');
  const key = process.env.AHP_CA_KEY || join(dir, 'ca.key');
  return existsSync(crt) && existsSync(key);
}

// The workload identity a client certificate actually carries. A pinned external cert
// is only usable for the identity it was issued to; tokens must be minted to match, so
// reusing it cannot widen anyone's scope.
export function clientIdentity(dir) {
  const file = join(dir, 'client.crt');
  if (!existsSync(file)) throw new Error(`no client.crt under ${dir}`);
  const san = new X509Certificate(readFileSync(file)).subjectAltName || '';
  const m = san.match(/URI:spiffe:\/\/ahp\/tenant\/([^/,\s]+)\/deployment\/([^,\s]+)/);
  if (!m) throw new Error(`${file} carries no spiffe://ahp/tenant/<t>/deployment/<d> SAN`);
  return { tenant: m[1], deployment: m[2] };
}

export function certPaths(dir = 'client') {
  // Resolution order matters for the negative legs: an absolute dir is used verbatim
  // (genClient returns the pinned dir itself), the bare default means "whatever identity
  // this run is allowed to speak with", and any OTHER name must resolve to ITS OWN
  // freshly minted cert. Falling back to the pinned cert here would silently turn a
  // foreign-tenant leg into a same-identity request.
  const base = isAbsolute(dir) ? dir : (dir === 'client' && CLIENT_CERT_DIR ? CLIENT_CERT_DIR : join(CERT_DIR, dir));
  return {
    cert: join(base, 'client.crt'),
    key: join(base, 'client.key'),
    ca: caFile(),
    serverCert: join(CERT_DIR, 'server', 'server.crt'),
    serverKey: join(CERT_DIR, 'server', 'server.key'),
  };
}

// Build bin/ahp if missing (idempotent). Returns absolute path.
export function ensureAhp() {
  const bin = join(PROJECT_ROOT, 'bin', 'ahp');
  if (existsSync(bin)) return bin;
  execFileSync('go', ['build', '-o', 'bin/ahp', './cmd/ahp'], {
    cwd: PROJECT_ROOT,
    env: { ...process.env, ...GO_ENV },
    stdio: 'inherit',
  });
  return bin;
}

// Build bin/tokengen if missing (idempotent). Returns absolute path.
export function ensureTokengen() {
  const bin = join(PROJECT_ROOT, 'bin', 'tokengen');
  if (existsSync(bin)) return bin;
  execFileSync('go', ['build', '-o', 'bin/tokengen', './scripts/tokengen'], {
    cwd: PROJECT_ROOT,
    env: { ...process.env, ...GO_ENV },
    stdio: 'inherit',
  });
  return bin;
}

// Mint a KindTask (or human) token via the tokengen binary. Returns token string.
export function tokengen(args = {}) {
  const bin = ensureTokengen();
  const argv = [];
  const push = (flag, val) => {
    if (val !== undefined && val !== null && val !== '') argv.push(`-${flag}`, String(val));
  };
  push('kid', args.kid ?? 'dev-1');
  push('tenant', args.tenant ?? 'dev');
  push('deployment', args.deployment ?? 'dev');
  push('subject', args.subject ?? 'dev');
  push('task', args.task ?? 'task-1');
  push('root', args.root);
  push('ctx', args.ctx ?? 'dev');
  push('grant', args.grant ?? 'dev');
  push('ttl', args.ttl ?? 600);
  push('audience', args.audience ?? 'ahp');
  push('kind', args.kind ?? 'task');
  const out = execFileSync(bin, argv, {
    cwd: PROJECT_ROOT,
    env: { ...process.env, ...GO_ENV, AHP_DEV_JWT_SEED: args.seed ?? devSeedHex() },
    encoding: 'utf8',
  });
  return out.trim();
}

// Spawn the ahp service with the given port/role over real mTLS, pointed at the
// block-isolated DB. Returns { proc, kill, env }. Secrets are never printed.
export async function spawnService(opts = {}) {
  const bin = ensureAhp();
  const env = await buildEnv(PROJECT_ROOT);
  const port = opts.port ?? 19099;
  const seed = opts.seed ?? devSeedHex();
  const svcEnv = {
    ...env,
    ...GO_ENV,
    AHP_ROLE: opts.role ?? 'all',
    AHP_LISTEN: `:${port}`,
    AHP_DEV_JWT_SEED: seed,
    AHP_TLS_CERT: join(CERT_DIR, 'server', 'server.crt'),
    AHP_TLS_KEY: join(CERT_DIR, 'server', 'server.key'),
    AHP_TLS_CA: join(CERT_DIR, 'ca', 'ca.crt'),
    OTEL_EXPORTER_OTLP_ENDPOINT: '',
    AHP_VERBOSE: opts.verbose ? '1' : '',
  };
  if (opts.tls === false) {
    delete svcEnv.AHP_TLS_CERT;
    delete svcEnv.AHP_TLS_KEY;
    delete svcEnv.AHP_TLS_CA;
  }
  const proc = spawn(bin, [], {
    cwd: PROJECT_ROOT,
    env: svcEnv,
    stdio: opts.verbose ? 'inherit' : ['ignore', 'pipe', 'pipe'],
    detached: false,
  });
  const logs = [];
  if (!opts.verbose && proc.stderr) {
    proc.stderr.on('data', (d) => logs.push(String(d)));
    proc.stdout.on('data', (d) => logs.push(String(d)));
  }
  const kill = () => {
    try {
      proc.kill('SIGTERM');
    } catch {
      /* already gone */
    }
  };
  return {
    proc,
    port,
    seed,
    kill,
    logs: () => logs.join(''),
  };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Poll /readyz until 200 or timeout. Returns true on ready, throws otherwise.
export async function pollReady(port, { tls = true, timeoutMs = 20000 } = {}) {
  const deadline = Date.now() + timeoutMs;
  const tlsOpts = tls
    ? {
        ca: readFileSync(join(CERT_DIR, 'ca', 'ca.crt')),
        cert: readFileSync(join(CERT_DIR, 'client', 'client.crt')),
        key: readFileSync(join(CERT_DIR, 'client', 'client.key')),
        servername: 'localhost',
        rejectUnauthorized: true,
      }
    : {};
  while (Date.now() < deadline) {
    const ok = await new Promise((res) => {
      const mod = tls ? https : http;
      const req = mod.get(
        { host: '127.0.0.1', port, path: '/readyz', ...tlsOpts, timeout: 1500 },
        (r) => {
          r.resume();
          res(r.statusCode === 200);
        },
      );
      req.on('error', () => res(false));
      req.on('timeout', () => {
        req.destroy();
        res(false);
      });
    });
    if (ok) return true;
    await sleep(250);
  }
  throw new Error(`service on port ${port} not ready within ${timeoutMs}ms`);
}

// One mTLS State API call. Returns { status, text, json }.
export function mState(op, body, opts = {}) {
  const port = opts.port ?? 19099;
  const p = certPaths(opts.certDir ?? 'client');
  const headers = { 'content-type': 'application/json' };
  if (opts.token) headers.authorization = `Bearer ${opts.token}`;
  const payload = body === undefined ? '{}' : JSON.stringify(body);
  const options = {
    host: '127.0.0.1',
    port,
    method: 'POST',
    path: `/v1/state/${op}`,
    ca: readFileSync(p.ca),
    cert: readFileSync(p.cert),
    key: readFileSync(p.key),
    servername: 'localhost',
    rejectUnauthorized: true,
    headers,
    timeout: 15000,
  };
  return new Promise((res, rej) => {
    const req = https.request(options, (r) => {
      let text = '';
      r.on('data', (d) => (text += d));
      r.on('end', () => {
        let json = null;
        try {
          json = text ? JSON.parse(text) : null;
        } catch {
          /* leave json null */
        }
        res({ status: r.statusCode, text, json });
      });
    });
    req.on('error', rej);
    req.on('timeout', () => req.destroy(new Error('mState timeout')));
    req.write(payload);
    req.end();
  });
}

// Plain HTTP GET (for /debug/stats, /healthz over cleartext when tls=false).
export function httpGet(port, path, { tls = false } = {}) {
  const mod = tls ? https : http;
  const opts = { host: '127.0.0.1', port, path, timeout: 5000 };
  if (tls) {
    opts.ca = readFileSync(join(CERT_DIR, 'ca', 'ca.crt'));
    opts.cert = readFileSync(join(CERT_DIR, 'client', 'client.crt'));
    opts.key = readFileSync(join(CERT_DIR, 'client', 'client.key'));
    opts.servername = 'localhost';
    opts.rejectUnauthorized = true;
  }
  return new Promise((res, rej) => {
    const req = mod.get(opts, (r) => {
      let text = '';
      r.on('data', (d) => (text += d));
      r.on('end', () => res({ status: r.statusCode, text }));
    });
    req.on('error', rej);
    req.on('timeout', () => req.destroy(new Error('httpGet timeout')));
  });
}

// Assert a condition, throwing a descriptive error.
export function assert(cond, msg) {
  if (!cond) throw new Error(`assertion failed: ${msg}`);
}

// Issue (idempotently) a client cert for an arbitrary workload identity and
// return its cert-dir name. Each test uses a unique tenant/deployment so that
// ClaimRun (scoped by tenant+deployment only) can never claim a neighbour test's
// runs in the shared isolated DB.
export function genClient(tenant, deployment) {
  const dir = `client-${tenant}-${deployment}`;
  const out = join(CERT_DIR, dir);
  // A pinned acceptance cert is reused for ITS OWN identity only. Any other identity
  // still has to be minted, because pretending a foreign tenant can be reached with the
  // victim-adjacent pinned cert would quietly turn an isolation leg into a no-op.
  if (CLIENT_CERT_DIR && existsSync(join(CLIENT_CERT_DIR, 'client.crt'))) {
    const id = clientIdentity(CLIENT_CERT_DIR);
    if (id.tenant === tenant && id.deployment === deployment) return CLIENT_CERT_DIR;
    if (!canMint()) {
      throw new Error(`MINT_UNAVAILABLE: needs a fresh ${tenant}/${deployment} identity, but no CA key is present and ` +
        `AHP_CLIENT_CERT_DIR is pinned to ${id.tenant}/${id.deployment}. Provide AHP_CA_DIR/AHP_CA_KEY, or expect this leg to skip.`);
    }
  }
  if (!canMint()) {
    throw new Error(`MINT_UNAVAILABLE: no CA material for client certs. Run infra/generate-certs.sh in this checkout, ` +
      `point AHP_CA_DIR at the stand CA (needs ca.key to mint), or set AHP_CLIENT_CERT_DIR to issued client certs.`);
  }
  if (!existsSync(join(out, 'client.crt'))) {
    execFileSync('bash', [join(PROJECT_ROOT, 'infra', 'gen-client.sh'), tenant, deployment, out], {
      cwd: PROJECT_ROOT,
      stdio: 'inherit',
    });
  }
  return dir;
}

// A unique, SPIFFE-segment-safe deployment id for per-test isolation.
export function uniqueDeployment(prefix = 'd') {
  return prefix + Date.now().toString(36) + Math.random().toString(36).slice(2, 6);
}
