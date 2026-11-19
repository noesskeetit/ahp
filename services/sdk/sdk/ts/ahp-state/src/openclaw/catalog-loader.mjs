// Preload only in a dedicated Gateway process configured for one AHP scope.
import {readFileSync, readdirSync, realpathSync} from 'node:fs';
import {join} from 'node:path';
import {pathToFileURL} from 'node:url';
import * as nodeModule from 'node:module';
import {CompactJournalClient} from '../client.mjs';
import {bindOpenClawGatewayCatalog} from './catalog-gateway.mjs';

const bridge = new URL('./catalog-gateway.mjs', import.meta.url).href;
const handler = `"sessions.list": async ({ params, respond, context }) => {
    if (!params || typeof params !== "object" || Array.isArray(params)) {
      if (!assertValidParams(params, validateSessionsListParams, "sessions.list", respond)) return;
    }
    const {cursor, appId, userId, status, updatedAfter, ...nativeParams} = params;
    if (!assertValidParams(nativeParams, validateSessionsListParams, "sessions.list", respond)) return;
    if ([cursor, appId, userId, status, updatedAfter].some(x => x !== undefined && typeof x !== "string")) {
      respond(false, void 0, errorShape(ErrorCodes.INVALID_REQUEST, "Invalid State API catalog filter"));
      return;
    }
    try {
      const cfg = context.getRuntimeConfig();
      const defaultAgent = resolveDefaultAgentId(cfg);
      const result = await listJournalSessions({ ...nativeParams, cursor, appId, userId, status, updatedAfter },
        listAgentIds(cfg), defaultAgent, (key) => hasTrackedActiveSessionRun({
          context, requestedKey: key, canonicalKey: key, defaultAgentId: defaultAgent
        }));
      respond(true, result, void 0);
    } catch (error) {
      const code = error.status === 400 || error instanceof RangeError ? ErrorCodes.INVALID_REQUEST : ErrorCodes.UNAVAILABLE;
      respond(false, void 0, errorShape(code, "AHP State API could not list sessions"));
    }
  },
`;

const hooks = [
  {
    name: 'sessions.list',
    module: /^export\s*\{[^}]*\bsessionsHandlers\b[^}]*\}/m,
    point: /(^[\t ]*)"sessions\.list":\s*async\s*\(\{\s*params,\s*respond,\s*context\s*\}\)\s*=>\s*\{[\s\S]*?^\1\},(?=\r?\n[\t ]*"sessions\.)/gm,
    native: /\bloadCombinedSessionStoreForGateway\b/,
    prefix: `import {listJournalSessions} from ${JSON.stringify(bridge)};\n`,
    patched: handler.trimEnd(),
  },
  {
    name: 'startup migration preflight',
    module: /^export\s*\{[^}]*\bensureConfigReady\b[^}]*\}/m,
    point: /\bconst\s+shouldConsiderStateMigration\s*=\s*shouldMigrateStateFromPath\(commandPath\);/g,
    patched: 'const shouldConsiderStateMigration = commandPath[0] !== "gateway" && shouldMigrateStateFromPath(commandPath);',
  },
  {
    name: 'startup session migration',
    module: /^export\s*\{[^}]*\brunStartupSessionMigration\b[^}]*\}/m,
    point: /\basync\s+function\s+runStartupSessionMigration\(params\)\s*\{\s*await\s+runSessionStartupMigration\(params\);\s*\}/g,
    patched: 'async function runStartupSessionMigration() {}',
  },
  {
    name: 'restart recovery',
    module: /^export\s*\{[^}]*\brecoverStartupOrphanedMainSessions\b[^}]*\brecoverRestartAbortedMainSessions\b[^}]*\}/m,
    point: /^async\s+function\s+resolveRestartRecoveryStorePaths\(params\)\s*\{[\s\S]*?^\}/gm,
    native: /\bresolveAllAgentSessionStoreTargetsSync\b/,
    patched: 'async function resolveRestartRecoveryStorePaths() { return []; }',
  },
  {
    name: 'health',
    module: /^export\s*\{[^}]*\bhealthCommand\b[^}]*\bgetHealthSnapshot\b[^}]*\}/m,
    point: /^const\s+buildSessionSummary\s*=\s*async\s*\(storePath,\s*agentId\)\s*=>\s*\{[\s\S]*?^\};/gm,
    native: /\blistSessionEntries\b/,
    prefix: `import {journalSessionHealth} from ${JSON.stringify(bridge)};\n`,
    patched: 'const buildSessionSummary = (_storePath, agentId) => journalSessionHealth(agentId);',
  },
];

export function prepareOpenClawGatewayPatches(root) {
  const dist = join(root, 'dist');
  const located = new Set();
  const modules = new Map();
  for (const dirent of readdirSync(dist, {withFileTypes: true})) {
    if (!dirent.isFile() || !dirent.name.endsWith('.js')) continue;
    const url = pathToFileURL(realpathSync(join(dist, dirent.name))).href;
    const source = readFileSync(new URL(url), 'utf8');
    for (const hook of hooks) {
      if (!hook.module.test(source)) continue;
      const sites = [...source.matchAll(hook.point)].filter(match => !hook.native || hook.native.test(match[0]));
      if (sites.length !== 1 || located.has(hook.name)) {
        throw new Error(`OpenClaw catalog hook '${hook.name}' must match exactly once`);
      }
      const [site] = sites;
      located.add(hook.name);
      if (!modules.has(url)) modules.set(url, {source, edits: []});
      modules.get(url).edits.push({hook, site});
    }
  }
  const missing = hooks.filter(hook => !located.has(hook.name)).map(hook => hook.name);
  if (missing.length) throw new Error(`Missing OpenClaw catalog hooks: ${missing.join(', ')}`);
  return new Map([...modules].map(([url, {source, edits}]) => {
    let patched = source;
    for (const {hook, site} of edits.sort((a, b) => b.site.index - a.site.index)) {
      patched = patched.slice(0, site.index) + hook.patched + patched.slice(site.index + site[0].length);
    }
    return [url, edits.map(({hook}) => hook.prefix ?? '').join('') + patched];
  }));
}

/** Register every catalog hook before importing the dedicated Gateway. */
export function registerOpenClawGatewayCatalog({installDir, handshake}) {
  if (!installDir || !handshake) {
    throw new Error('OpenClaw catalog requires an installation and a State API handshake');
  }
  if (!nodeModule.registerHooks) throw new Error('OpenClaw Gateway catalog preload requires Node.js >=22.15');
  const patches = prepareOpenClawGatewayPatches(installDir);
  const client = new CompactJournalClient(handshake);
  let unbind;
  try {
    unbind = bindOpenClawGatewayCatalog(client);
    const registration = nodeModule.registerHooks({
      load(url, context, nextLoad) {
        const source = patches.get(url);
        if (source !== undefined) return {format: 'module', source, shortCircuit: true};
        return nextLoad(url, context);
      },
    });
    return {deregister() {
      registration.deregister();
      unbind();
      client.close();
    }};
  } catch (error) {
    unbind?.();
    client.close();
    throw error;
  }
}
