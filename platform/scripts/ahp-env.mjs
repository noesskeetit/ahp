#!/usr/bin/env node
// Shared environment loader for delivery tooling.
//
// Responsibilities:
//   * Load the (git-ignored) .env without clobbering explicit process overrides.
//   * Force every database-facing variable to the block-isolated database
//     (default ahp_hive_delivery). The DB *name* is always rewritten to the
//     allowlisted isolated name; credentials/host/port/query from the source
//     DSN are preserved so the shared .env role (which has access to the
//     isolated DB) can be reused. Credentials are never logged.
//
// The launcher provisions DATABASE_URL/AHP_DB_URL/PG* with the isolated DB name
// but without a usable password; the shared .env carries the only working
// credentials. We therefore derive ONE canonical DSN from the source that has
// credentials, isolate its DB name, and propagate it to every consumer so no
// child can ever reach the shared ahp_go database.
//
// Exposed: parseEnvFile, loadProjectEnv, isolateDatabase, buildEnv, DEFAULT_DB.
// The .env file itself (often a shared symlink) is never modified.

import { existsSync } from 'node:fs';
import { readFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

export const DEFAULT_DB = 'ahp_hive_delivery';
// Launcher safety allowlist for isolated block databases.
const DB_ALLOWLIST = /^ahp_hive_[a-z][a-z0-9_]{0,23}$/;

export function parseEnvFile(text) {
  const out = {};
  for (const raw of text.split(/\r?\n/)) {
    const line = raw.trim();
    if (!line || line.startsWith('#')) continue;
    const eq = line.indexOf('=');
    if (eq <= 0) continue;
    let value = line.slice(eq + 1).trim();
    const quoted =
      (value.startsWith('"') && value.endsWith('"') && value.length >= 2) ||
      (value.startsWith("'") && value.endsWith("'") && value.length >= 2);
    if (quoted) value = value.slice(1, -1);
    out[line.slice(0, eq).trim()] = value;
  }
  return out;
}

// Merge .env into a copy of process.env; existing process values win.
export async function loadProjectEnv(projectRoot) {
  const env = { ...process.env };
  const envPath = resolve(projectRoot, '.env');
  if (existsSync(envPath)) {
    const parsed = parseEnvFile(await readFile(envPath, 'utf8'));
    for (const [k, v] of Object.entries(parsed)) {
      if (env[k] === undefined || env[k] === '') env[k] = v;
    }
  }
  return env;
}

// Rewrite the database name segment of a postgres/postgresql URL, keeping the
// scheme, credentials, host, port and query string untouched.
export function rewriteDbName(url, dbName) {
  if (!url) return url;
  const u = new URL(url);
  u.pathname = '/' + dbName;
  return u.toString();
}

function dsnHasCredentials(url) {
  if (!url) return false;
  try {
    const u = new URL(url);
    return !!(u.username && u.password);
  } catch {
    return false;
  }
}

// Point every database-facing variable at the isolated block DB, using a single
// canonical DSN that carries working credentials. Throws if the target DB name
// is not on the launcher safety allowlist.
export function isolateDatabase(env, dbName = process.env.AHP_DB_NAME || DEFAULT_DB) {
  if (!DB_ALLOWLIST.test(dbName)) {
    throw new Error(`refusing to isolate to non-allowlisted database name: ${dbName}`);
  }
  const out = { ...env };
  // Prefer a source DSN that actually has credentials so the canonical works.
  const candidates = [
    out.AHP_TEST_DATABASE_URL,
    out.AHP_DATABASE_URL,
    out.DATABASE_URL,
    out.AHP_DB_URL,
  ];
  const credentialed = candidates.find(dsnHasCredentials);
  const fallback = candidates.find(Boolean);
  const source = credentialed || fallback;
  if (!source) {
    out.AHP_DB_NAME = dbName;
    return out;
  }
  const canonical = rewriteDbName(source, dbName);
  // Always set the service + integration-test variables.
  out.AHP_TEST_DATABASE_URL = canonical;
  out.AHP_DATABASE_URL = canonical;
  // Overwrite generic vars only if they were already present, so children that
  // read DATABASE_URL/AHP_DB_URL also land on the isolated DB.
  if (out.DATABASE_URL !== undefined) out.DATABASE_URL = canonical;
  if (out.AHP_DB_URL !== undefined) out.AHP_DB_URL = canonical;
  // Keep libpq-style PG* consistent with the canonical DSN (if it has creds).
  try {
    const u = new URL(canonical);
    if (u.hostname) out.PGHOST = u.hostname;
    if (u.port) out.PGPORT = u.port;
    if (u.username) out.PGUSER = decodeURIComponent(u.username);
    if (u.password) out.PGPASSWORD = decodeURIComponent(u.password);
    out.PGDATABASE = dbName;
  } catch {
    /* leave PG* untouched if canonical is not a URL */
  }
  out.AHP_DB_NAME = dbName;
  return out;
}

// Full pipeline: load .env then isolate. Returns a ready-to-spawn env object.
export async function buildEnv(projectRoot, opts = {}) {
  const env = await loadProjectEnv(projectRoot);
  return isolateDatabase(env, opts.dbName);
}

// Quote a value for safe use in a POSIX shell `export KEY=...` line.
// Wraps in single quotes and escapes embedded single quotes as '\''.
export function shellSingleQuote(v) {
  return "'" + String(v).split("'").join("'\\''") + "'";
}

// CLI:
//   node scripts/ahp-env.mjs            -> print masked DB targets (no secrets)
//   node scripts/ahp-env.mjs --export   -> print shell `export` lines for the
//                                          isolated env so a shell can `eval`
//                                          them. Values may contain the DB
//                                          password; they are written to stdout
//                                          for eval only and must not be echoed
//                                          to a transcript by the caller.
if (process.argv[1] && process.argv[1].endsWith('ahp-env.mjs')) {
  const here = dirname(fileURLToPath(import.meta.url));
  const env = await buildEnv(resolve(here, '..'));
  const mask = (u) => {
    if (!u) return '(unset)';
    try {
      const x = new URL(u);
      if (x.password) x.password = '***';
      return x.toString();
    } catch {
      return '(unparseable)';
    }
  };
  if (process.argv.includes('--export')) {
    const keys = [
      'AHP_DATABASE_URL',
      'AHP_TEST_DATABASE_URL',
      'DATABASE_URL',
      'AHP_DB_NAME',
      'AHP_API_URL',
    ];
    for (const k of keys) {
      if (env[k]) process.stdout.write(`export ${k}=${shellSingleQuote(env[k])}\n`);
    }
    process.exit(0);
  }
  console.log('AHP_DB_NAME           =', env.AHP_DB_NAME);
  console.log('AHP_TEST_DATABASE_URL =', mask(env.AHP_TEST_DATABASE_URL));
  console.log('AHP_DATABASE_URL      =', mask(env.AHP_DATABASE_URL));
  console.log('DATABASE_URL          =', mask(env.DATABASE_URL));
}
