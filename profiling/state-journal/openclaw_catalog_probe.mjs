import assert from 'node:assert/strict';
import {execFile as execCallback, spawn} from 'node:child_process';
import {createHash} from 'node:crypto';
import {once} from 'node:events';
import fs from 'node:fs';
import {createServer} from 'node:net';
import {join} from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';
import {pathToFileURL} from 'node:url';
import {promisify} from 'node:util';
import {CompactJournalClient, inlineJSON} from '../../adapters/journal/compact-client.mjs';
import {CompactOpenClawStorage} from '../../adapters/openclaw/compact-storage.mjs';

const client = new CompactJournalClient(process.env.AHP_STATE_HANDSHAKE);
const foreign = new CompactJournalClient(process.env.AHP_STATE_FOREIGN_HANDSHAKE);
const base = '/v1/state/sessions';
const prompt = 'shared-skill-instruction '.repeat(1000);
const promptHash = createHash('sha256').update(prompt).digest('hex');

async function publish(id, {agent = 'main', title = 'Catalog result', connector = client} = {}) {
  const {epoch} = await connector.request('POST', base + ':acquire', {body: {
    session_id: id, harness_type: 'openclaw', format_version: 1, ttl_ms: 30000,
  }});
  const path = `${base}/${id}`;
  const entry = id + ':message';
  const payload = {lease_epoch: epoch, key: entry, expected_seq: 0, events: [
    {id: entry, type: 'openclaw.entry', role: 'user', codec: 'json', payload: inlineJSON({text: title})},
  ], pointers: {leaf_entry_id: entry}, catalog: {
    agent_id: agent, native_key: `agent:${agent}:${id}`, title,
    summary_metadata: {last_message_preview: 'saved preview'}, prompt,
  }};
  await assert.rejects(connector.request('POST', path + '/events:append', {body: {
    ...payload, catalog: {...payload.catalog, summary_metadata: {preview: 'x'.repeat(4097)}},
  }}), {status: 400});
  const receipt = await connector.request('POST', path + '/events:append', {body: payload});
  assert.deepEqual(receipt, {first_seq: 1, head_seq: 1});
  assert.deepEqual(await connector.request('POST', path + '/events:append', {body: payload}), receipt);
  await connector.request('POST', path + ':heartbeat', {body: {lease_epoch: epoch, release: true}});
}

const list = (options = {}) => client.request('GET', base, {query: {
  view: 'catalog', harness: 'openclaw', limit: 1, ...options,
}});

try {
  await publish('z-older');
  await publish('a-newer');
  await publish('different-agent', {agent: 'other'});
  await publish('foreign-session', {connector: foreign});
  await publish('literal-percent', {title: '100% sample'});
  const first = await list({agent_id: 'main', search_query: 'catalog', status: 'IDLE'});
  assert.equal(first.sessions.length, 1);
  assert.equal(first.sessions[0].session_id, 'a-newer');
  assert.equal(first.sessions[0].head_seq, 1);
  assert.equal(first.sessions[0].leaf_entry_id, 'a-newer:message');
  assert.equal(first.sessions[0].native_key, 'agent:main:a-newer');
  assert.equal(first.sessions[0].prompt_ref, promptHash);
  assert.deepEqual(first.sessions[0].summary_metadata, {last_message_preview: 'saved preview'});
  assert.ok(first.next_cursor);
  const second = await list({agent_id: 'main', search_query: 'catalog', status: 'IDLE', cursor: first.next_cursor});
  assert.deepEqual(second.sessions.map(row => row.session_id), ['z-older']);
  assert.equal(second.next_cursor, undefined);
  assert.deepEqual((await list({search_query: '%'})).sessions.map(row => row.session_id), ['literal-percent']);
  assert.deepEqual((await list({search_query: 'foreign-session'})).sessions, []);
  assert.deepEqual((await list({app_id: 'foreign-app'})).sessions, []);
  assert.deepEqual((await list({user_id: 'foreign-reader'})).sessions, []);
  assert.deepEqual((await list({updated_after: new Date(Date.now() + 60000).toISOString()})).sessions, []);
  await assert.rejects(list({cursor: 'invalid-cursor'}), {status: 400});
  console.log(JSON.stringify({first, second, isolated: true, replayed: true}));
  if (process.env.AHP_OPENCLAW_INSTALL_DIR) await verifyNativeGateway();
} finally {
  client.close();
  foreign.close();
}

async function verifyNativeGateway() {
  const root = process.env.AHP_OPENCLAW_INSTALL_DIR;
  const core = await import(pathToFileURL(join(root, 'dist/plugin-sdk/agent-core.js')).href);
  const openclaw = join(root, 'openclaw.mjs');
  const exec = promisify(execCallback);
  const dir = fs.mkdtempSync('/tmp/ahp-openclaw-native-list-');
  const state = join(dir, 'state');
  const sessionDir = join(state, 'agents/main/sessions');
  const store = join(sessionDir, 'sessions.json');
  const trace = join(dir, 'file-io.log');
  fs.mkdirSync(sessionDir, {recursive: true});
  fs.mkdirSync(join(dir, 'workspace'));
  const diskEntries = Object.fromEntries(Array.from({length: 1407}, (_, index) => [
    `agent:main:disk-${index}`, {sessionId: `disk-${index}`, updatedAt: Date.now() - index,
      skillsSnapshot: {prompt, skills: [], version: 1}},
  ]));
  fs.writeFileSync(store, JSON.stringify(diskEntries));
  const seededDiskBytes = fs.statSync(store).size;
  fs.chmodSync(store, 0o444);
  fs.chmodSync(sessionDir, 0o555);

  // They are materialized by the native SessionStorage adapter via mTLS.
  const extra = 1403;
  await Promise.all(Array.from({length: 8}, async (_, worker) => {
    for (let index = worker; index < extra; index += 8) {
      const id = `native-${String(index).padStart(4, '0')}`;
      const storage = await new CompactOpenClawStorage(core, client, id, {heartbeat: false,
        catalog: {agent_id: 'main', native_key: `agent:main:${id}`, title: 'Native session ' + id, prompt},
      }).open();
      await storage.close();
    }
  }));
  const server = createServer();
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const port = server.address().port;
  await promisify(server.close.bind(server))();
  const config = join(dir, 'openclaw.json');
  fs.writeFileSync(config, JSON.stringify({
    gateway: {mode: 'local', bind: 'loopback', auth: {mode: 'none'}, controlUi: {enabled: false}, port},
    discovery: {mdns: {mode: 'off'}}, cron: {enabled: false}, plugins: {enabled: false},
    logging: {file: join(dir, 'openclaw.log')}, update: {checkOnStart: false},
    agents: {defaults: {workspace: join(dir, 'workspace'), heartbeat: {every: '0m'}}}, session: {store},
  }));
  const tracer = join(dir, 'trace.cjs');
  fs.writeFileSync(tracer, `
const fs = require('node:fs');
const append = fs.appendFileSync;
for (const method of ['readFileSync','openSync','writeFileSync','renameSync','unlinkSync']) {
  const original = fs[method];
  fs[method] = function(...args) {
    if (args.some(x => typeof x === 'string' && (x === process.env.AHP_TRACE_STORE ||
        x.startsWith(process.env.AHP_TRACE_STORE + '.')))) {
      append(process.env.AHP_TRACE_LOG, JSON.stringify({method, stack: new Error().stack.split('\\n').slice(2,8)}) + '\\n');
    }
    return original.apply(this, args);
  };
}
require('node:module').syncBuiltinESMExports();
`);
  const env = {
    PATH: process.env.PATH, OPENCLAW_STATE_DIR: state, OPENCLAW_CONFIG_PATH: config,
    AHP_OPENCLAW_INSTALL_DIR: root,
    OPENCLAW_SKIP_CHANNELS: '1', OPENCLAW_SKIP_STARTUP_MODEL_PREWARM: '1',
    OPENCLAW_SESSION_CACHE_TTL_MS: '0',
  };
  const version = (await exec(process.execPath, [openclaw, '--version'], {env, timeout: 10000})).stdout.trim();
  assert.match(version, /2026\.6\.11 \(e085fa1\)/);
  const loader = new URL('../../adapters/openclaw/catalog-loader.mjs', import.meta.url).href;
  const measurements = [];
  // A fresh Gateway process for each initial page size.
  for (const limit of [1, 20]) {
    const logPath = join(dir, `gateway-${limit}.log`);
    const openedLog = fs.openSync(logPath, 'w', 0o600);
    const gateway = spawn(process.execPath, [openclaw, 'gateway', 'run', '--bind', 'loopback', '--port', String(port)], {
      env: {...env, NODE_OPTIONS: `--require=${tracer} --import=${loader}`,
        AHP_OPENCLAW_CATALOG_HANDSHAKE: process.env.AHP_STATE_HANDSHAKE,
        AHP_TRACE_STORE: store, AHP_TRACE_LOG: trace},
      stdio: ['ignore', openedLog, openedLog],
    });
    fs.closeSync(openedLog);
    const exited = once(gateway, 'exit');
    const operationCount = () => fs.existsSync(trace) ? fs.readFileSync(trace, 'utf8').trim().split('\n').length : 0;
    const rpc = async params => {
      const start = performance.now();
      const before = operationCount();
      const {stdout} = await exec(process.execPath, [openclaw, 'gateway', 'call', 'sessions.list',
        '--params', JSON.stringify(params), '--timeout', '8000', '--json'],
      {env: {...env, OPENCLAW_STATE_DIR: join(dir, 'operator')}, timeout: 20000});
      const elapsedMs = performance.now() - start;
      const io = operationCount() - before;
      const response = JSON.parse(stdout);
      assert.equal(response.path, 'ahp:state');
      assert.equal(io, 0, 'Gateway must avoid filesystem I/O on the monolithic catalog during list');
      return {response, elapsedMs, io};
    };
    try {
      for (let attempt = 0; attempt < 100 && !fs.readFileSync(logPath, 'utf8').includes('[gateway] ready'); attempt++) {
        assert.equal(gateway.exitCode, null, fs.readFileSync(logPath, 'utf8'));
        await delay(150);
      }
      assert.match(fs.readFileSync(logPath, 'utf8'), /\[gateway\] ready/);
      const {response, elapsedMs, io} = await rpc({limit, includeDerivedTitles: true});
      const expected = await list({limit});
      const ids = response.sessions.map(row => row.sessionId);
      assert.deepEqual(ids, expected.sessions.map(row => row.session_id));
      assert.equal(ids.length, limit);
      assert.equal(response.nextCursor, expected.next_cursor);
      assert.ok(response.sessions.every(row => row.title && row.promptRef === promptHash));
      const next = await rpc({limit, cursor: response.nextCursor});
      assert.equal(next.response.sessions.length, limit);
      assert.equal(next.response.sessions.some(row => ids.includes(row.sessionId)), false);
      const agent = await rpc({limit, agentId: 'other', search: 'catalog', appId: 'dots', userId: 'reader',
        status: 'IDLE', includeLastMessage: true});
      assert.deepEqual(agent.response.sessions.map(row => row.sessionId), ['different-agent']);
      assert.equal(agent.response.sessions[0].lastMessagePreview, 'saved preview');
      const foreignApp = await rpc({limit, appId: 'foreign-app'});
      assert.deepEqual(foreignApp.response.sessions, []);
      assert.equal(fs.statSync(store).size, seededDiskBytes);
      measurements.push({limit, rows: ids.length, io, cliWallMs: Number(elapsedMs.toFixed(3)),
        responseBytes: Buffer.byteLength(JSON.stringify(response)), ids});
    } finally {
      gateway.kill('SIGTERM');
      await Promise.race([exited, delay(5000, null, {ref: false})]);
      if (gateway.exitCode === null && gateway.signalCode === null) {
        gateway.kill('SIGKILL');
        await exited;
      }
    }
  }
  assert.equal(fs.existsSync(trace), false, 'Gateway must avoid legacy catalog I/O during cold startup and list');
  console.log(JSON.stringify({version, seededSessions: 1407, seededDiskBytes, mode: '0555',
    source: 'State API over mTLS/JWT', startupIO: 0, measurements, localEvidence: dir}));
}
