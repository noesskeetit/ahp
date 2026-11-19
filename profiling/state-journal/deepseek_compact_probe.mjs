import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {
  closeSync, existsSync, fsyncSync, mkdirSync, openSync, readFileSync, writeFileSync,
} from 'node:fs';
import {join} from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';
import {fileURLToPath} from 'node:url';
import {createHash, randomUUID} from 'node:crypto';

import {Context} from '@deepseek-ai/cordis';
import SessionStore, {SessionId, TOOL_OUTCOME_UNKNOWN} from '@deepseek-ai/dsh-session';
import AgentRegistry from '@deepseek-ai/dsh-agent';
import AgentLoop from '@deepseek-ai/dsh-agent-loop';
import LlmRuntime, {createUserMessage, LlmAdapter, ToolCallId} from '@deepseek-ai/dsh-llm';
import ToolRuntime, {defineContentToolFixture} from '@deepseek-ai/dsh-tools';
import SystemPrompt from '@deepseek-ai/dsh-system-prompt';
import Projections from '@deepseek-ai/dsh-session-projection';
import * as checkpointPolicy from '@deepseek-ai/dsh-session-checkpoint-policy';

import Backend from '../../adapters/deepseek/compact-session-persistence.mjs';
import {CompactJournalClient} from '../../adapters/journal/compact-client.mjs';

const entry = fileURLToPath(import.meta.url);
const TOOL = 'write';
const nativeCall = ToolCallId('mock-call');
const options = {agentOptions: {provider: 'compact-fixture', model: 'compact-fixture'}};

function parked() {
  return new Promise(() => { setInterval(() => {}, 60_000); });
}

async function context(client, ttlMs, heartbeat, gateway) {
  const ctx = new Context();
  for (const plugin of [SessionStore, AgentRegistry, LlmRuntime, SystemPrompt, ToolRuntime, Projections]) {
    await ctx.plugin(plugin);
  }
  await ctx.plugin(Backend, {client, ttlMs, heartbeat, effectTools: [TOOL], gateway});
  await ctx.plugin(checkpointPolicy);
  await ctx.plugin(AgentLoop, {agents: [], maxParallelToolCalls: 1});
  return ctx;
}

function effectFile(directory) {
  return join(directory, 'effect.jsonl');
}

async function source(id, directory, mode) {
  const client = new CompactJournalClient(process.env.AHP_STATE_HANDSHAKE);
  const ctx = await context(client, 5000, false);
  let requests = 0;
  class Model extends LlmAdapter {
    async *stream() {
      if (++requests > 1) {
        assert.equal(mode, 'completed');
        writeFileSync(join(directory, 'marker'), 'next-model-after-result');
        await parked();
        return;
      }
      yield {type: 'block-start', index: 0, blockType: 'tool-call'};
      yield {type: 'block-end', index: 0, block: {
        type: 'tool-call', id: nativeCall, name: TOOL, arguments: '{}',
      }};
      yield {type: 'finish', reason: {kind: 'tool-calls'}};
    }
  }
  ctx.llm.registerAdapter(['compact-fixture'], new Model());
  ctx.tools.register(defineContentToolFixture({
    name: TOOL, description: 'append a deterministic external probe record', parameters: {},
    async execute(_args, exec) {
      assert.equal(exec.callId, nativeCall);
      if (mode === 'gateway-prepared') {
        const call = exec.agent.session.snapshotEvents().findLast(event => event.type === 'tool/call');
        assert.equal(call.data.callId, nativeCall);
        const digest = createHash('sha256').update(JSON.stringify([ctx.sessionPersistence.sessionId(id), nativeCall]))
          .digest('hex');
        writeFileSync(join(directory, 'marker'), `dsh-tool:${call.seq}:${digest}`);
        await parked();
      }
      const key = await ctx.sessionPersistence.dispatchTool(id, exec.callId);
      if (mode.startsWith('gateway-')) {
        try {
          const result = await client.requestTool('Call', {key, tool: TOOL, arguments: {}});
          assert.equal(mode, 'gateway-unknown', 'the Tool Gateway reply was not lost');
          assert.equal(result.status, 'SIDE_EFFECT_UNKNOWN');
        } catch (error) {
          if (mode !== 'gateway-known' || error.code !== 'TRANSPORT' || !error.outcomeUnknown) throw error;
        }
        writeFileSync(join(directory, 'marker'), key);
        await parked();
      }
      const fd = openSync(effectFile(directory), 'a');
      try {
        writeFileSync(fd, JSON.stringify({key, call_id: exec.callId, pid: process.pid}) + '\n');
        fsyncSync(fd);
      } finally {
        closeSync(fd);
      }
      if (mode === 'unknown') {
        writeFileSync(join(directory, 'marker'), 'effect-without-result');
        await parked();
      }
      return [{type: 'text', text: 'fixture-receipt'}];
    },
  }));
  const keepalive = setInterval(() => {}, 1000);
  try {
    const handle = await ctx.agents.create({sessionId: id, ...options});
    handle.agent.followup(createUserMessage({content: [{type: 'text', text: 'run'}], source: {kind: 'user'}}));
    await handle.agent.whenIdle();
    const events = handle.agent.session.snapshotEvents();
    throw new Error(`Agent stopped before reaching the failpoint: ${JSON.stringify(events.slice(-6))}`);
  } finally {
    clearInterval(keepalive);
    await ctx.fiber.dispose();
    client.close();
  }
}

async function trial(client, directory, mode) {
  const throughGateway = mode.startsWith('gateway-');
  const id = SessionId(`compact-ds-${mode}-${randomUUID()}`);
  mkdirSync(directory);
  const log = openSync(join(directory, 'child.log'), 'w');
  const child = spawn(process.execPath, [entry, 'child', id, directory, mode],
    {stdio: ['ignore', log, log]});
  closeSync(log);
  const exited = once(child, 'exit');
  let exitCode, signal;
  try {
    const expected = mode === 'unknown' ? 'effect-without-result' : 'next-model-after-result';
    for (let attempt = 0; ; attempt++) {
      if (child.exitCode !== null || child.signalCode !== null) {
        throw new Error(readFileSync(join(directory, 'child.log'), 'utf8'));
      }
      const marker = join(directory, 'marker');
      if (existsSync(marker)) {
        const value = readFileSync(marker, 'utf8');
        if (throughGateway ? value.startsWith('dsh-tool:') : value === expected) break;
      }
      if (attempt === 400) throw new Error('Native checkpoint failpoint was not reached');
      await delay(25);
    }
    child.kill('SIGKILL');
    [exitCode, signal] = await exited;
  } finally {
    if (child.exitCode === null && child.signalCode === null) {
      child.kill('SIGKILL');
      await exited;
    }
  }
  assert.equal(exitCode, null);
  assert.equal(signal, 'SIGKILL');

  const requests = [];
  const gateway = throughGateway ? async (operation, body) => {
    requests.push([operation, body.key]);
    return await client.requestTool(operation, body);
  } : undefined;
  const ctx = await context(client, 30000, true, gateway);
  let resumed;
  try {
    const handle = await ctx.sessionPersistence.open(id, 'read');
    const {events: before} = await handle.read();
    await handle.close();
    assert(before.some(event => event.type === 'tool/call' && event.data.callId === nativeCall));
    const external = throughGateway ? {key: readFileSync(join(directory, 'marker'), 'utf8')}
      : JSON.parse(readFileSync(effectFile(directory), 'utf8').trim());
    const original = await ctx.sessionPersistence.getIntent(id, external.key);
    const expectedBefore = mode === 'completed' ? 'COMPLETED'
      : mode === 'gateway-prepared' ? 'PREPARED' : 'DISPATCHED';
    assert.equal(original.status, expectedBefore);
    assert.equal(before.some(event => event.type === 'tool/result'), mode === 'completed');
    class ColdModel extends LlmAdapter {
      async *stream() {
        throw new Error('Resume unexpectedly dispatched a new request');
      }
    }
    ctx.llm.registerAdapter(['compact-fixture'], new ColdModel());
    if (mode === 'unknown') {
      for (let attempt = 0; ; attempt++) {
        try {
          await ctx.agents.resume({resumeSessionId: id, ...options});
          throw new Error('DeepSeek resumed an unresolved external effect');
        } catch (error) {
          if (error.name === 'SessionAlreadyOwnedError' && attempt < 100) {
            await delay(100);
            continue;
          }
          assert.equal(error.code, TOOL_OUTCOME_UNKNOWN);
          break;
        }
      }
      assert.equal((await ctx.sessionPersistence.getIntent(id, external.key)).status, 'OUTCOME_UNKNOWN');
      assert.equal(readFileSync(effectFile(directory), 'utf8').trim().split('\n').length, 1);
      return {mode, source_signal: 'SIGKILL', held_before_model: true, effects: 1};
    }
    for (let attempt = 0; ; attempt++) {
      try {
        resumed = await ctx.agents.resume({resumeSessionId: id, ...options});
        break;
      } catch (error) {
        if (error.name !== 'SessionAlreadyOwnedError' || attempt >= 100) throw error;
        await delay(100);
      }
    }
    await resumed.agent.whenIdle();
    await ctx.sessions.flush(resumed.agent.session);
    const writer = ctx.sessionPersistence.writers.get(id);
    assert.equal(writer.epoch, 2);
    await assert.rejects(client.request('POST', ctx.sessionPersistence.path(id) + ':heartbeat',
      {body: {lease_epoch: 1, ttl_ms: 1000}}), error => error.code === 'STALE_EPOCH');
    const previous = await ctx.sessionPersistence.getIntent(id, external.key);
    const unknown = mode.endsWith('unknown');
    assert.equal(previous.status, unknown ? 'OUTCOME_UNKNOWN' : 'COMPLETED');
    const reader = await ctx.sessionPersistence.open(id, 'read');
    const {events} = await reader.read();
    await reader.close();
    const results = events.filter(event => event.type === 'tool/result');
    assert.equal(results.length, 1);
    if (unknown) assert.equal(results[0].data.error.code, TOOL_OUTCOME_UNKNOWN);
    if (throughGateway) {
      const expectedCalls = mode === 'gateway-known' ? ['GetResult'] : unknown
        ? ['GetResult', 'Get'] : ['GetResult', 'Call', 'GetResult'];
      assert.deepEqual(requests, expectedCalls.map(operation => [operation, external.key]));
      if (!unknown) {
        const receipt = await client.requestTool('GetResult', {key: external.key});
        assert.deepEqual(results[0].data.message.content, receipt.payload.content);
        assert.deepEqual(JSON.parse(Buffer.from(previous.result.inline, 'base64')), results[0].data.message);
      }
    } else {
      assert.equal(readFileSync(effectFile(directory), 'utf8').trim().split('\n').length, 1);
    }
    const listed = await ctx.sessionPersistence.list();
    assert(listed.some(item => item.header.id === id));
    return {mode, signal, epoch: writer.epoch, before: before.length, after: events.length,
      native_checkpoint_policy: true, intent: previous.status,
      ...(throughGateway ? {gateway_calls: requests.map(row => row[0]), result_events: results.length}
        : {effects: 1})};
  } finally {
    await resumed?.dispose();
    await ctx.fiber.dispose();
  }
}

const [mode, first, second, third] = process.argv.slice(2);
if (mode === 'child') {
  await source(SessionId(first), second, third);
} else {
  let handoff = process.env.AHP_STATE_HANDSHAKE;
  if (first?.startsWith('gateway-')) {
    const connection = JSON.parse(readFileSync(handoff, 'utf8'));
    handoff = join(mode, 'replacement.json');
    writeFileSync(handoff, JSON.stringify({...connection, task_token: connection.replacement_token}), {mode: 0o600});
  }
  const client = new CompactJournalClient(handoff);
  try {
    for (const kind of first ? [first] : ['unknown', 'completed']) {
      const result = await trial(client, join(mode, kind), kind);
      console.log(JSON.stringify(result));
    }
  } finally {
    client.close();
  }
}
