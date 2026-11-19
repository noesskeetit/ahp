/** Reproduce the two-crash window with the pinned native OpenClaw Agent. */
import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {closeSync, existsSync, fsyncSync, mkdirSync, openSync, readFileSync, writeFileSync} from 'node:fs';
import {createServer} from 'node:http';
import {join} from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';
import {fileURLToPath} from 'node:url';
import {createHash, randomUUID} from 'node:crypto';

import {CompactJournalClient, JournalHTTPError, readInlineJSON} from '../../adapters/journal/compact-client.mjs';
import {CompactOpenClawStorage, OpenClawOutcomeUnknown, attachCompactOpenClawAgent} from '../../adapters/openclaw/compact-storage.mjs';

const entry = fileURLToPath(import.meta.url);
const path = process.env.OPENCLAW_AGENT_CORE || '/opt/homebrew/lib/node_modules/openclaw/dist/plugin-sdk/agent-core.js';
const core = await import(path);
const effectTools = ['reserve', 'charge'];

function parked() {
  return new Promise(() => setInterval(() => {}, 60_000));
}

function messagesWithReceipts(request) {
  const results = new Set();
  for (const message of request.messages ?? []) {
    if (message.role !== 'tool') continue;
    const text = typeof message.content === 'string' ? message.content : JSON.stringify(message.content);
    if (text.includes('written')) results.add('write');
    for (const tool of ['reserve', 'charge']) {
      if (text.includes(`fixture-${tool}-receipt`)) results.add(tool);
    }
  }
  return results;
}

function reply(response, tool) {
  const base = {id: 'compact-oc', object: 'chat.completion.chunk',
    created: Math.floor(Date.now() / 1000), model: 'compact-fixture'};
  const delta = tool ? {role: 'assistant', tool_calls: [{
    index: 0, id: tool + '-1', type: 'function', function: {name: tool, arguments: '{"amount":7}'},
  }]} : {role: 'assistant', content: 'DONE'};
  response.writeHead(200, {'Content-Type': 'text/event-stream'});
  for (const data of [
    {...base, choices: [{index: 0, delta, finish_reason: null}]},
    {...base, choices: [{index: 0, delta: {}, finish_reason: tool ? 'tool_calls' : 'stop'}],
      usage: {prompt_tokens: 50, completion_tokens: 10, total_tokens: 60}},
  ]) response.write('data: ' + JSON.stringify(data) + '\n\n');
  response.end('data: [DONE]\n\n');
}

async function modelFixture() {
  const state = {phase: 'source', logs: [], invalid: null};
  const server = createServer(async (request, response) => {
    try {
      let body = '';
      for await (const chunk of request) body += chunk;
      const results = messagesWithReceipts(JSON.parse(body));
      state.logs.push({phase: state.phase, receipts: [...results]});
      switch (state.phase) {
        case 'source':
          if (results.has('reserve')) return;
          return reply(response, 'reserve');
        case 'resume':
          assert(results.has('reserve'));
          if (results.has('charge')) return;
          return reply(response, 'charge');
        case 'finish':
          assert(results.has('reserve') && results.has('charge'));
          return reply(response, null);
        case 'unknown':
        case 'uncertain-reply':
          assert.equal(state.logs.filter(log => log.phase === state.phase).length, 1);
          return reply(response, 'reserve');
        case 'gateway-prepared':
        case 'gateway-dispatched':
        case 'gateway-known':
        case 'gateway-unknown':
        case 'gateway-detached':
          assert.equal(state.logs.filter(log => log.phase === state.phase).length, 1);
          return reply(response, 'write');
        case 'gateway-resume':
          assert(results.has('write'));
          return reply(response, null);
        default:
          throw new Error('OpenClaw sent a model request despite an unknown outcome');
      }
    } catch (error) {
      state.invalid = error;
      response.writeHead(500);
      response.end();
    }
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  return {state, server, url: `http://127.0.0.1:${server.address().port}/v1`};
}

function ledger(dir) {
  const data = readFileSync(join(dir, 'effect.jsonl'), 'utf8').trim();
  return data ? data.split('\n').map(line => JSON.parse(line)) : [];
}

async function child(phase, id, dir, url) {
  const throughGateway = phase.startsWith('gateway-');
  const recovering = phase === 'gateway-resume' || phase === 'gateway-held';
  const client = new CompactJournalClient(recovering ? join(dir, 'replacement.json') : process.env.AHP_STATE_HANDSHAKE);
  const configured = throughGateway ? ['write'] : effectTools;
  const storage = recovering ? await openAfterCrash(client, id, configured)
    : new CompactOpenClawStorage(core, client, id, {ttlMs: 6000, heartbeat: false, effectTools: configured});
  const requests = [];
  const gateway = recovering ? async (operation, body) => {
    requests.push([operation, body.key]);
    return await client.requestTool(operation, body);
  } : undefined;
  try {
    if (!recovering) await storage.open();
    if (phase === 'gateway-prepared' || phase === 'gateway-detached') {
      storage.dispatchTool = async callID => {
        const {entry: recorded} = await storage.trackedCall(callID);
        const digest = createHash('sha256').update(JSON.stringify([recorded.id, callID])).digest('hex');
        if (phase === 'gateway-detached') await new core.Session(storage).moveTo(recorded.parentId);
        writeFileSync(join(dir, 'tool-started'), 'oc:tool:' + digest);
        await parked();
      };
    }
    const tools = configured.map(name => ({
      name, label: name, description: 'Append a deterministic ledger record',
      parameters: {type: 'object', properties: {amount: {type: 'number'}}, required: ['amount']},
      async execute(call, args, _signal, _onUpdate, bridge) {
        assert.equal(call, name + '-1');
        assert.equal(args.amount, 7);
        assert(bridge.idempotencyKey.startsWith('oc:tool:'));
        if (throughGateway) {
          if (recovering) throw new Error('The native continuation attempted to dispatch an extra tool');
          if (phase !== 'gateway-dispatched') {
            try {
              const op = await client.requestTool('Call', {key: bridge.idempotencyKey, tool: name, arguments: args});
              assert.equal(phase, 'gateway-unknown', 'the Gateway reply was not lost');
              assert.equal(op.status, 'SIDE_EFFECT_UNKNOWN');
            } catch (error) {
              if (phase !== 'gateway-known' || error.code !== 'TRANSPORT' || !error.outcomeUnknown) throw error;
            }
          }
          writeFileSync(join(dir, 'tool-started'), bridge.idempotencyKey);
          await parked();
        }
        const fd = openSync(join(dir, 'effect.jsonl'), 'a');
        try {
          writeFileSync(fd, JSON.stringify({name, call, key: bridge.idempotencyKey, pid: process.pid}) + '\n');
          fsyncSync(fd);
        } finally { closeSync(fd); }
        if (phase === 'unknown') {
          writeFileSync(join(dir, 'tool-started'), 'effect durable');
          await parked();
        }
        if (phase === 'uncertain-reply') throw new Error('Fixture disconnected after recording the effect');
        return {content: [{type: 'text', text: `fixture-${name}-receipt`}], details: {amount: args.amount}};
      },
    }));
    const model = {
      id: 'compact-fixture', name: 'Compact fixture', provider: 'compact-fixture',
      api: 'openai-completions', baseUrl: url, reasoning: false, input: ['text'],
      cost: {input: 0, output: 0, cacheRead: 0, cacheWrite: 0},
      contextWindow: 32768, maxTokens: 4000,
    };
    let host;
    try {
      host = await attachCompactOpenClawAgent(core, storage, {
        initialState: {systemPrompt: 'Complete the deterministic fixture', tools, model, thinkingLevel: 'off'},
        getApiKey: async () => 'local-fixture', gateway,
      });
      if (phase === 'gateway-held') {
        await host.continue();
        throw new Error('The unknown outcome did not stop the native continuation');
      }
    } catch (error) {
      if (phase !== 'gateway-held' || !(error instanceof OpenClawOutcomeUnknown)) throw error;
      return {phase, epoch: storage.epoch, held: true, requests};
    } finally {
      if (phase === 'gateway-held') host?.unsubscribe();
    }
    try {
      if (phase === 'source' || phase === 'unknown' || (throughGateway && !recovering)) {
        await host.prompt('Run the fixture');
      } else if (phase === 'uncertain-reply') {
        await assert.rejects(host.prompt('Run the fixture'), error => error instanceof OpenClawOutcomeUnknown);
        return {phase, epoch: storage.epoch, held: true};
      }
      else if (phase === 'held') {
        await assert.rejects(host.continue(), error => error instanceof OpenClawOutcomeUnknown);
        return {phase, epoch: storage.epoch, held: true};
      } else await host.continue();
      if (phase !== 'finish' && phase !== 'gateway-resume') {
        throw new Error('Agent exited before failpoint: ' + host.agent.state.errorMessage);
      }
      const context = await host.session.buildContext();
      assert(context.messages.at(-1).content.some(item => item.type === 'text' && item.text === 'DONE'));
      return {phase, epoch: storage.epoch, messages: context.messages.length, pid: process.pid,
        ...(throughGateway && {requests})};
    } finally {
      host.unsubscribe();
    }
  } finally {
    await storage.close();
    client.close();
  }
}

async function worker(phase, id, dir, fixture, predicate) {
  fixture.state.phase = phase;
  const log = join(dir, phase + '.log');
  const fd = openSync(log, 'w');
  const proc = spawn(process.execPath, [entry, 'child', phase, id, dir, fixture.url],
    {stdio: ['ignore', fd, fd]});
  closeSync(fd);
  const exited = once(proc, 'exit');
  try {
    if (predicate) {
      for (let i = 0; !predicate(); i++) {
        if (fixture.state.invalid) throw fixture.state.invalid;
        if (proc.exitCode !== null || proc.signalCode !== null) throw new Error(readFileSync(log, 'utf8'));
        if (i >= 400) throw new Error('Timeout waiting for native OpenClaw checkpoint boundary');
        await delay(25);
      }
      proc.kill('SIGKILL');
      assert.deepEqual(await exited, [null, 'SIGKILL']);
      assert(!fixture.state.invalid, fixture.state.invalid);
      return;
    }
    const [code, signal] = await exited;
    if (code !== 0 || signal) throw new Error(readFileSync(log, 'utf8'));
    return JSON.parse(readFileSync(log, 'utf8').trim().split('\n').at(-1));
  } finally {
    if (proc.exitCode === null && proc.signalCode === null) {
      proc.kill('SIGKILL');
      await exited;
    }
  }
}

async function openAfterCrash(client, id, configured = effectTools) {
  for (let i = 0; ; i++) {
    try {
      return await new CompactOpenClawStorage(core, client, id, {effectTools: configured}).open();
    } catch (error) {
      if (error.code !== 'CONFLICT' || i >= 120) throw error;
      await delay(100);
    }
  }
}

async function checkCold(client, id, key, wantStatus) {
  const storage = await new CompactOpenClawStorage(core, client, id).open({readOnly: true});
  const entries = await storage.getEntries();
  const intent = await storage.intent(key);
  assert.equal(intent.status, wantStatus);
  return entries.filter(entry => entry.message?.role === 'toolResult').length;
}

async function treeTrial(client) {
  const id = 'oc-tree-' + randomUUID();
  const storage = await new CompactOpenClawStorage(core, client, id).open();
  const session = new core.Session(storage);
  const message = text => ({role: 'user', content: [{type: 'text', text}], timestamp: Date.now()});
  const root = await session.appendMessage(message('root'));
  const original = await session.appendMessage(message('original'));
  await session.moveTo(root);
  let lost = false, attempts = 0;
  const originalRequest = client.request.bind(client);
  client.request = async (method, route, options) => {
    const result = await originalRequest(method, route, options);
    if (method === 'POST' && route.endsWith('/events:append')) {
      attempts++;
      if (!lost) {
        lost = true;
        throw new JournalHTTPError(0, 'TRANSPORT', true);
      }
    }
    return result;
  };
  let alternate;
  try {
    alternate = await session.appendMessage(message('alternate'));
  } finally {
    client.request = originalRequest;
    await storage.close();
  }
  assert(lost && attempts === 2);
  const cold = await new CompactOpenClawStorage(core, client, id).open({readOnly: true});
  assert.equal(await cold.getLeafId(), alternate);
  assert(await cold.getEntry(original));
  assert.equal((await cold.getEntries()).filter(item => item.id === alternate).length, 1);
  const context = await new core.Session(cold).buildContext();
  assert.deepEqual(context.messages.map(msg => msg.content[0].text), ['root', 'alternate']);
  return {scenario: 'branches', alternate, preservedOriginal: original, lostCommitAckReplay: true};
}

async function gatewayTrial(directory, scenario) {
  const id = 'oc-gateway-' + randomUUID();
  const sourcePath = process.env.AHP_STATE_HANDSHAKE;
  const replacementPath = join(directory, 'replacement.json');
  const handshake = JSON.parse(readFileSync(sourcePath, 'utf8'));
  writeFileSync(replacementPath, JSON.stringify({...handshake, task_token: handshake.replacement_token}), {mode: 0o600});
  const original = new CompactJournalClient(sourcePath);
  const client = new CompactJournalClient(replacementPath);
  const fixture = await modelFixture();
  try {
    const marker = join(directory, 'tool-started');
    await worker(scenario, id, directory, fixture, () =>
      existsSync(marker) && readFileSync(marker, 'utf8').startsWith('oc:tool:'));
    const key = readFileSync(marker, 'utf8');
    const prepared = scenario === 'gateway-prepared';
    const unknown = scenario === 'gateway-unknown';
    const detached = scenario === 'gateway-detached';
    const missing = prepared || scenario === 'gateway-dispatched';
    assert.equal(await checkCold(client, id, key, prepared || detached ? 'PREPARED' : 'DISPATCHED'), 0);

    const phase = unknown || detached ? 'gateway-held' : 'gateway-resume';
    const resumed = await worker(phase, id, directory, fixture);
    let operations = [];
    if (unknown) operations = ['GetResult', 'Get'];
    else if (missing) operations = ['GetResult', 'Call', 'GetResult'];
    else if (!detached) operations = ['GetResult'];
    assert.equal(resumed.epoch, 2);
    assert.deepEqual(resumed.requests, operations.map(operation => [operation, key]));
    const modelRequests = fixture.state.logs.filter(log => log.phase === phase).length;
    assert.equal(modelRequests, unknown || detached ? 0 : 1);
    assert.equal(fixture.state.invalid, null);
    await assert.rejects(original.request('POST', `/v1/state/sessions/${id}:heartbeat`,
      {body: {lease_epoch: 1, ttl_ms: 1000}}), error => error.code === 'STALE_EPOCH');

    const cold = await new CompactOpenClawStorage(core, client, id).open({readOnly: true});
    const entries = await cold.getEntries();
    if (detached) {
      assert.equal((await cold.intent(key)).status, 'PREPARED');
      assert.equal(entries.filter(entry => entry.message?.role === 'toolResult').length, 0);
      assert.equal((await new core.Session(cold).buildContext()).messages.at(-1).role, 'user');
      return {scenario, source_signal: 'SIGKILL', new_epoch: resumed.epoch, stale_epoch_rejected: true,
        gateway_calls: operations, result_events: 0, status: 'PREPARED', model_requests: modelRequests};
    }
    const [result] = entries.filter(entry => entry.message?.role === 'toolResult');
    assert.equal(entries.filter(entry => entry.message?.role === 'toolResult').length, 1);
    assert.equal(result.message.toolCallId, 'write-1');
    const restoredIntent = await cold.intent(key);
    assert.equal(restoredIntent.status, unknown ? 'OUTCOME_UNKNOWN' : 'COMPLETED');
    assert.equal(restoredIntent.gateway_operation_id, key);
    if (unknown) {
      assert.deepEqual(result.message.content, [{type: 'text', text: 'TOOL_OUTCOME_UNKNOWN'}]);
      assert.equal(result.message.isError, true);
      assert.equal(restoredIntent.result, undefined);
    } else {
      const receipt = await client.requestTool('GetResult', {key});
      assert.deepEqual(result.message.content, receipt.payload.content);
      assert.deepEqual(readInlineJSON(restoredIntent.result), result.message);
    }
    return {scenario, source_signal: 'SIGKILL', new_epoch: resumed.epoch, stale_epoch_rejected: true,
      same_key: true, gateway_calls: operations, result_events: 1, status: restoredIntent.status, model_requests: modelRequests};
  } finally {
    fixture.server.closeAllConnections();
    await new Promise(resolve => fixture.server.close(resolve));
    original.close();
    client.close();
  }
}

async function trials(out) {
  const client = new CompactJournalClient(process.env.AHP_STATE_HANDSHAKE);
  const fixture = await modelFixture();
  try {
    const twice = join(out, 'double-crash');
    mkdirSync(twice);
    const id = 'oc-double-' + randomUUID();
    await worker('source', id, twice, fixture, () => fixture.state.logs.some(log =>
      log.phase === 'source' && log.receipts.includes('reserve')));
    const [reserve] = ledger(twice);
    assert.equal(await checkCold(client, id, reserve.key, 'COMPLETED'), 1);
    const secondLease = await openAfterCrash(client, id);
    assert.equal(secondLease.epoch, 2);
    await secondLease.close();

    await worker('resume', id, twice, fixture, () => fixture.state.logs.some(log =>
      log.phase === 'resume' && log.receipts.includes('charge')));
    const effects = ledger(twice);
    assert.deepEqual(effects.map(item => item.name), ['reserve', 'charge']);
    assert.equal(await checkCold(client, id, effects[1].key, 'COMPLETED'), 2);
    const thirdLease = await openAfterCrash(client, id);
    assert.equal(thirdLease.epoch, 4);
    await assert.rejects(client.request('POST', thirdLease.path + ':heartbeat',
      {body: {lease_epoch: 3, ttl_ms: 1000}}), error => error.code === 'STALE_EPOCH');
    await thirdLease.close();
    const final = await worker('finish', id, twice, fixture);
    assert.equal(final.epoch, 5);
    assert.deepEqual(ledger(twice), effects);
    console.log(JSON.stringify({scenario: 'double-crash', signals: ['SIGKILL', 'SIGKILL'],
      receipts: 2, effects: effects.map(item => item.name), oldEpochRejected: true, newProcess: final}));

    const unknown = join(out, 'unknown');
    mkdirSync(unknown);
    const unresolvedID = 'oc-unknown-' + randomUUID();
    await worker('unknown', unresolvedID, unknown, fixture, () => existsSync(join(unknown, 'tool-started')));
    const unresolved = ledger(unknown);
    assert.equal(unresolved.length, 1);
    assert.equal(await checkCold(client, unresolvedID, unresolved[0].key, 'DISPATCHED'), 0);
    const replacement = await openAfterCrash(client, unresolvedID);
    await replacement.close();
    const held = await worker('held', unresolvedID, unknown, fixture);
    assert.equal(held.held, true);
    assert(!fixture.state.logs.some(log => log.phase === 'held'));
    assert.deepEqual(ledger(unknown), unresolved);
    assert.equal(await checkCold(client, unresolvedID, unresolved[0].key, 'OUTCOME_UNKNOWN'), 0);
    console.log(JSON.stringify({scenario: 'unknown-tool-outcome', held: true, effects: 1, modelRequests: 0}));

    const uncertain = join(out, 'uncertain-reply');
    mkdirSync(uncertain);
    const uncertainID = 'oc-uncertain-' + randomUUID();
    const current = await worker('uncertain-reply', uncertainID, uncertain, fixture);
    const uncertainEffects = ledger(uncertain);
    assert(current.held);
    assert.equal(uncertainEffects.length, 1);
    assert.equal(await checkCold(client, uncertainID, uncertainEffects[0].key, 'OUTCOME_UNKNOWN'), 1);
    const modelRequests = fixture.state.logs.filter(log => log.phase === 'uncertain-reply').length;
    assert.equal(modelRequests, 1);
    assert(!fixture.state.invalid, fixture.state.invalid);
    console.log(JSON.stringify({scenario: 'live-uncertain-tool-outcome', held: true, effects: 1, modelRequests}));
    console.log(JSON.stringify(await treeTrial(client)));
  } finally {
    fixture.server.closeAllConnections();
    await new Promise(resolve => fixture.server.close(resolve));
    client.close();
  }
}

const [mode, phase, id, dir, url] = process.argv.slice(2);
if (mode === 'child') console.log(JSON.stringify(await child(phase, id, dir, url)));
else if (phase?.startsWith('gateway-')) console.log(JSON.stringify(await gatewayTrial(mode, phase)));
else await trials(mode);
