// Load only into the dedicated, single-scope OpenClaw Gateway process.
// The user's installed OpenClaw tree remains immutable.
import {createHash} from 'node:crypto';
import {readFileSync, realpathSync} from 'node:fs';
import {registerHooks} from 'node:module';
import {join} from 'node:path';
import {pathToFileURL} from 'node:url';

const root = process.env.AHP_OPENCLAW_INSTALL_DIR;
if (!root || !process.env.AHP_OPENCLAW_CATALOG_HANDSHAKE) {
  throw new Error('OpenClaw catalog requires a pinned install and a State API handshake');
}
const patches = new Map();
function patchModule(name, expected, transform) {
  const file = realpathSync(join(root, 'dist', name));
  const original = readFileSync(file, 'utf8');
  if (createHash('sha256').update(original).digest('hex') !== expected) {
    throw new Error(`OpenClaw catalog patch expects 2026.6.11 e085fa1: ${name}`);
  }
  const source = transform(original);
  if (source === original) throw new Error(`OpenClaw catalog patch did not match: ${name}`);
  patches.set(pathToFileURL(file).href, source);
}
const anchor = '"sessions.list": async ({ params, respond, context }) => {';
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
patchModule('sessions-DC378bJ-.js', '9ffb9461527683d7ad518e1ddffa9cdd6479324fafbdca5cc30ef51b47565b1f', original => {
  const start = original.indexOf(anchor);
  const end = original.indexOf('\t"sessions.cleanup":', start);
  if (start < 0 || end <= start) throw new Error('Missing native sessions.list source');
  return `import {listJournalSessions} from ${JSON.stringify(bridge)};\n` +
    original.slice(0, start) + handler + original.slice(end);
});

// The journal owner recovers leases. On this Gateway the legacy startup
// migration and orphan marking would open the old catalog before the first RPC.
patchModule('config-guard-Fjle7Zc-.js', 'd99318a9ca4826a0c865308398aa800b174ebb3c6acceca223135337ef856189',
  original => original.replace('const shouldConsiderStateMigration = shouldMigrateStateFromPath(commandPath);',
    'const shouldConsiderStateMigration = commandPath[0] !== "gateway" && shouldMigrateStateFromPath(commandPath);'));
patchModule('server-startup-session-migration-BYlQqjOO.js', '214413016794fef00178cd923432dd12b4f9f961bfc8157b87db7730ac8e920e',
  original => original.replace('await runSessionStartupMigration(params);', 'return;'));
patchModule('main-session-restart-recovery-x6V6ZCmn.js', '657b523eee8ac93e19b5d064e69f0424b24759dff687be396300b4f7b2f92e82',
  original => original.replace(
    /async function resolveRestartRecoveryStorePaths\(params\) \{[\s\S]*?\n\}/,
    'async function resolveRestartRecoveryStorePaths() { return []; }'));
patchModule('health-CYZzuqvJ.js', '7a56b872deec036d573e019b1703475442ba81e7a24a678b02b5e161309b4fd7', original => {
  const start = original.indexOf('const buildSessionSummary =');
  const end = original.indexOf('function buildPluginHealthSummary(', start);
  if (start < 0 || end <= start) throw new Error('Missing native health session summary');
  return `import {journalSessionHealth} from ${JSON.stringify(bridge)};\n` +
    original.slice(0, start) + 'const buildSessionSummary = (_storePath, agentId) => journalSessionHealth(agentId);\n' +
    original.slice(end);
});

registerHooks({
  load(url, context, nextLoad) {
    const source = patches.get(url);
    if (source !== undefined) return {format: 'module', source, shortCircuit: true};
    return nextLoad(url, context);
  },
});
