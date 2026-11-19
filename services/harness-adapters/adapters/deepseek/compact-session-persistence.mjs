import {createHash} from 'node:crypto';
import {isDeepStrictEqual} from 'node:util';
import {
  SessionPersistence, SessionPersistenceRevision, SessionAlreadyExistsError,
  SessionAlreadyOwnedError, SessionPersistenceNotFoundError, SessionReadOnlyError,
  SessionHandleClosedError, SessionOwnershipLostError, SessionPersistenceCorruptionError,
  materializeCreateHeader, materializeAppendBatch, assertContiguous, assertStoredId,
  assertVersion, validateStoredEvents,
} from '@deepseek-ai/dsh-session-persistence';
import {ToolCallRecovery, TOOL_OUTCOME_UNKNOWN} from '@deepseek-ai/dsh-session';
import {createToolResultMessage} from '@deepseek-ai/dsh-llm';
import {beginStandaloneTool, loadJournalJSON, storeJournalJSON} from '../journal/payload.mjs';
import {settleGatewayCall} from '../journal/tool-gateway.mjs';

const API = '/v1/state';
const HEADER = 'dsh.header';
const EVENT = 'dsh.event';
const maxBatchBytes = 900_000;
const roles = new Map([
  ['user/message', 'user'], ['assistant/message', 'assistant'],
  ['tool/call', 'tool_call'], ['tool/result', 'tool_result'],
]);

function nonnegative(value) {
  return Number.isSafeInteger(value) && value >= 0;
}

function row(id, type, role, payload) {
  return {id, type, role, codec: 'json-dsh-v4', payload};
}

class CompactSessionHandle {
  constructor(backend, state, access) {
    this.backend = backend;
    this.state = state;
    this.access = access;
    this.id = state.id;
    this.header = state.header;
    this.inheritedEventCount = state.inheritedEventCount;
    this.tail = Promise.resolve();
    this.routed = [];
    this.closed = false;
    this.lost = false;
  }

  route(event) {
    if (!this.closed) this.routed.push(event);
  }

  async drainRouted() {
    do {
      const count = this.routed.length;
      const batch = materializeAppendBatch(this.routed.slice(0, count));
      const suffix = [];
      for (const event of batch) {
        if (event.seq >= this.state.log.length) suffix.push(event);
        else if (!isDeepStrictEqual(this.state.log[event.seq], event)) {
          throw new SessionPersistenceCorruptionError('Changed routed DeepSeek event');
        }
      }
      assertContiguous(this.id, suffix, this.state.log.length);
      this.state.log.push(...suffix);
      this.routed.splice(0, count);
      await this.backend.publish(this);
    } while (this.routed.length);
  }

  ensure(operation, writing = false) {
    if (this.closed) throw new SessionHandleClosedError(this.id, operation);
    if (writing && this.access !== 'write') throw new SessionReadOnlyError(this.id, operation);
    if (writing && this.lost) throw new SessionOwnershipLostError(this.id);
  }

  serialize(fn) {
    const work = this.tail.then(fn);
    this.tail = work.catch(() => {});
    return work;
  }

  async read(offset = 0, length) {
    this.ensure('read');
    if (!nonnegative(offset) || (length !== undefined && !nonnegative(length))) {
      throw new RangeError('Expected a nonnegative offset and length');
    }
    if (this.access === 'read') {
      const owner = this.backend.writers.get(this.id);
      const fresh = owner?.state ?? await this.backend.load(this.id);
      if (fresh.log.length >= this.state.log.length) this.state = fresh;
    }
    return {eventState: 'detached', events: structuredClone(this.state.log.slice(
      offset, length === undefined ? undefined : offset + length))};
  }

  append(events) {
    this.ensure('append', true);
    const batch = materializeAppendBatch(events);
    return this.serialize(async () => {
      if (this.lost) throw new SessionOwnershipLostError(this.id);
      assertContiguous(this.id, batch, this.state.log.length);
      this.state.log.push(...batch);
    });
  }

  flush() {
    this.ensure('flush', true);
    return this.serialize(() => this.drainRouted());
  }

  close() {
    if (this.closing) return this.closing;
    this.closed = true;
    if (this.access === 'read') return Promise.resolve();
    clearInterval(this.timer);
    this.closing = this.serialize(async () => {
      try {
        if (!this.lost) await this.drainRouted();
      } finally {
        await this.backend.release(this);
      }
    });
    return this.closing;
  }

  async [Symbol.asyncDispose]() {
    await this.close();
  }
}

export default class CompactSessionPersistence extends SessionPersistence {
  constructor(ctx, {client, prefix = 'dsh-', ttlMs = 30000, heartbeat = true, effectTools = [], gateway}) {
    super(ctx);
    this.client = client;
    this.prefix = prefix;
    this.ttlMs = ttlMs;
    this.heartbeat = heartbeat;
    this.effectTools = new Set(effectTools);
    this.gateway = gateway;
    this.writers = new Map();
    ctx.on('session/event', (session, event) => this.writers.get(session.id)?.route(event));
    ctx.on('session/flush', session => this.writers.get(session.id)?.flush());
    ctx.on('session/disposed', session => this.writers.get(session.id)?.close().catch(error => {
      ctx.logger.warn(`DeepSeek journal close failed: ${error.name}`);
    }));
    ctx.effect(() => () => this.dispose());
  }

  sessionId(id) {
    return this.prefix + id;
  }

  path(id) {
    return `${API}/sessions/${encodeURIComponent(this.sessionId(id))}`;
  }

  toolKey(state, event) {
    const identity = state.effectKeyVersion === 2
      ? JSON.stringify([this.sessionId(state.id), event.data.callId])
      : String(event.data.callId);
    const digest = createHash('sha256').update(identity).digest('hex');
    return `dsh-tool:${event.seq}:${digest}`;
  }

  async acquire(id) {
    try {
      return await this.client.request('POST', API + '/sessions:acquire', {body: {
        session_id: this.sessionId(id), harness_type: 'deepseek', format_version: 4,
        ttl_ms: this.ttlMs,
      }});
    } catch (error) {
      if (error.code === 'CONFLICT') throw new SessionAlreadyOwnedError(id);
      throw error;
    }
  }

  async load(id) {
    let page;
    try {
      page = await this.client.request('GET', this.path(id) + '/events', {query: {limit: 100}});
    } catch (error) {
      if (error.code === 'NOT_FOUND') throw new SessionPersistenceNotFoundError(id);
      throw error;
    }
    const head = page.head_seq;
    if (head === 0) throw new SessionPersistenceNotFoundError(id);
    const rows = [...page.events];
    while (rows.at(-1).seq < head) {
      page = await this.client.request('GET', this.path(id) + '/events', {
        query: {limit: 100, after_seq: rows.at(-1).seq},
      });
      if (!page.events?.length) throw new SessionPersistenceCorruptionError('Missing journal page');
      rows.push(...page.events.filter(item => item.seq <= head));
    }
    if (rows[0].type !== HEADER) throw new SessionPersistenceCorruptionError('Missing DeepSeek header');
    const {header: native, inheritedEventCount, effectKeyVersion = 1} = await loadJournalJSON(this.client, this.path(id), rows[0].payload);
    if (effectKeyVersion !== 1 && effectKeyVersion !== 2) {
      throw new SessionPersistenceCorruptionError('Unknown DeepSeek tool-key format');
    }
    const header = materializeCreateHeader(native);
    assertStoredId(id, header);
    assertVersion(header);
    const log = [];
    for (const item of rows.slice(1)) {
      if (item.type !== EVENT) throw new SessionPersistenceCorruptionError('Unexpected DeepSeek journal record');
      log.push(await loadJournalJSON(this.client, this.path(id), item.payload));
    }
    validateStoredEvents(header, log);
    assertContiguous(id, log, 0);
    if (!nonnegative(inheritedEventCount) || inheritedEventCount > log.length) {
      throw new SessionPersistenceCorruptionError('Invalid DeepSeek inherited prefix');
    }
    return {id, header, inheritedEventCount, effectKeyVersion, log, head, acked: log.length};
  }

  track(state, lease) {
    const handle = new CompactSessionHandle(this, state, 'write');
    handle.epoch = lease.epoch;
    this.writers.set(state.id, handle);
    if (this.heartbeat) {
      handle.timer = setInterval(() => handle.serialize(() => this.renew(handle)).catch(error => {
        if (error instanceof SessionOwnershipLostError) clearInterval(handle.timer);
      }), Math.max(100, this.ttlMs / 3));
      handle.timer.unref();
    }
    return handle;
  }

  async create(input, options) {
    options?.signal?.throwIfAborted();
    const header = materializeCreateHeader(input);
    assertStoredId(input.id, header);
    assertVersion(header);
    const id = header.id;
    const inheritedEventCount = options?.inheritedEventCount ?? 0;
    if (!nonnegative(inheritedEventCount) ||
        (header.isSeeded ? options?.inheritedEventCount === undefined : inheritedEventCount !== 0)) {
      throw new TypeError('isSeeded and inheritedEventCount must agree');
    }
    if (this.writers.has(id) || await this.stat(id)) throw new SessionAlreadyExistsError(id);
    let lease;
    try {
      lease = await this.acquire(id);
    } catch (error) {
      if (error instanceof SessionAlreadyOwnedError) throw new SessionAlreadyExistsError(id);
      throw error;
    }
    const handle = this.track({id, header, inheritedEventCount, effectKeyVersion: 2, log: [], head: 0, acked: 0}, lease);
    try {
      if (await this.remoteExists(id)) throw new SessionAlreadyExistsError(id);
      return handle;
    } catch (error) {
      await this.release(handle);
      throw error;
    }
  }

  async remoteExists(id) {
    try {
      await this.load(id);
      return true;
    } catch (error) {
      if (error instanceof SessionPersistenceNotFoundError) return false;
      throw error;
    }
  }

  async open(id, access, options) {
    options?.signal?.throwIfAborted();
    const writer = this.writers.get(id);
    if (access === 'read') return new CompactSessionHandle(this, writer?.state ?? await this.load(id), access);
    if (access !== 'write') throw new TypeError('Expected read or write access');
    if (writer) throw new SessionAlreadyOwnedError(id);
    await this.load(id);
    const lease = await this.acquire(id);
    const handle = this.track({id}, lease);
    try {
      handle.state = await this.load(id);
      handle.header = handle.state.header;
      handle.inheritedEventCount = handle.state.inheritedEventCount;
      if (this.gateway && handle.state.effectKeyVersion === 2) {
        await handle.serialize(() => this.recoverGatewayEffects(handle));
      } else if (!this.gateway && this.effectTools.size > 0) {
        await handle.serialize(() => this.recoverStandaloneEffects(handle));
      }
      return handle;
    } catch (error) {
      await this.release(handle);
      throw error;
    }
  }

  async renew(handle) {
    if (handle.lost) throw new SessionOwnershipLostError(handle.id);
    try {
      await this.client.request('POST', this.path(handle.id) + ':heartbeat', {body: {
        lease_epoch: handle.epoch, ttl_ms: this.ttlMs,
      }});
    } catch (error) {
      if (error.code === 'STALE_EPOCH') {
        handle.lost = true;
        throw new SessionOwnershipLostError(handle.id);
      }
      throw error;
    }
  }

  async makeBatch(state, epoch) {
    const storePayload = value => storeJournalJSON(this.client, this.path(state.id), epoch, value);
    const rows = state.head === 0 ? [row('dsh:header', HEADER, 'system', await storePayload({
      header: state.header, inheritedEventCount: state.inheritedEventCount,
      effectKeyVersion: state.effectKeyVersion,
    }))] : [];
    let size = JSON.stringify(rows).length;
    let end = state.acked;
    while (end < state.log.length && rows.length < 256) {
      const event = state.log[end];
      const role = roles.get(event.type) ?? 'system';
      const item = row(`dsh:event:${event.seq}`, EVENT, role, await storePayload(event));
      size += JSON.stringify(item).length + 1;
      if (size > maxBatchBytes) break;
      rows.push(item);
      end++;
    }
    if (state.head === 0 && end < state.inheritedEventCount) {
      throw new RangeError('Inherited DeepSeek prefix must fit the first journal transaction');
    }
    const request = {lease_epoch: epoch, key: `dsh:append:${state.head}`,
      expected_seq: state.head, events: rows};
    const intents = [];
    const outcomes = [];
    for (const event of state.log.slice(state.acked, end)) {
      if (event.type === 'tool/call' && this.effectTools.has(event.data.name)) {
        const key = this.toolKey(state, event);
        intents.push({call_id: key, tool_name: event.data.name, idempotency_key: key,
          args_digest: [...createHash('sha256').update(event.data.arguments).digest()]});
      }
      if (event.type !== 'tool/result' || event.surfaceOp !== 'append') continue;
      const call = state.log[event.sourceEventSeqs?.[0]];
      if (call?.type !== 'tool/call' || !this.effectTools.has(call.data.name)) continue;
      // A tool that was never dispatched can be closed natively without granting
      // permission to retry the prepared external action.
      if (call.seq >= state.acked) continue;
      const key = this.toolKey(state, call);
      const previous = await this.getIntent(state.id, key);
      if (previous.status !== 'DISPATCHED') continue;
      const unknown = event.data.error?.code === TOOL_OUTCOME_UNKNOWN;
      outcomes.push({call_id: key, gateway_operation_id: previous.gateway_operation_id,
        status: unknown ? 'OUTCOME_UNKNOWN' : 'COMPLETED',
        ...(!unknown && {result: await storePayload(event.data.message)})});
    }
    if (intents.length > 0) request.intents = intents;
    if (outcomes.length > 0) request.tool_outcomes = outcomes;
    return {end, request};
  }

  async publish(handle) {
    const state = handle.state;
    await this.renew(handle);
    while (state.pending || state.head === 0 || state.log.length > state.acked) {
      state.pending ??= await this.makeBatch(state, handle.epoch);
      const {request, end} = state.pending;
      const receipt = await this.client.request('POST', this.path(state.id) + '/events:append',
        {body: request});
      if (receipt.head_seq !== state.head + request.events.length || receipt.first_seq !== state.head + 1) {
        throw new SessionPersistenceCorruptionError('Invalid State API append receipt');
      }
      state.head = receipt.head_seq;
      state.acked = end;
      state.pending = undefined;
    }
  }

  async getIntent(id, key) {
    return await this.client.request('GET', API + '/aux:mutate', {query: {
      session_id: this.sessionId(id), call_id: key,
    }});
  }

  async recoverStandaloneEffects(handle) {
    const state = handle.state;
    const recovery = new ToolCallRecovery();
    for (const event of state.log) recovery.observe(event);
    for (const fallback of recovery.results()) {
      const call = state.log[fallback.sourceEventSeqs?.[0]];
      if (call?.type !== 'tool/call' || call.seq < state.inheritedEventCount ||
          !this.effectTools.has(call.data.name)) continue;
      const key = this.toolKey(state, call);
      const digest = createHash('sha256').update(call.data.arguments).digest();
      await beginStandaloneTool(this.client, this.path(handle.id), handle.epoch, key, call.data.name, digest);
      throw Object.assign(new Error('DeepSeek tool requires reconciliation'), {code: TOOL_OUTCOME_UNKNOWN});
    }
  }

  async recoverGatewayEffects(handle) {
    const state = handle.state;
    const recovery = new ToolCallRecovery();
    for (const event of state.log) recovery.observe(event);
    for (const fallback of recovery.results()) {
      const call = state.log[fallback.sourceEventSeqs?.[0]];
      if (call?.type !== 'tool/call' || call.seq < state.inheritedEventCount ||
          !this.effectTools.has(call.data.name)) continue;
      const key = this.toolKey(state, call);
      const intent = await this.getIntent(state.id, key);
      if (intent.status !== 'PREPARED' && intent.status !== 'DISPATCHED') {
        throw new SessionPersistenceCorruptionError('DeepSeek tool intent and journal disagree');
      }
      if (intent.status === 'DISPATCHED' && intent.gateway_operation_id !== key) {
        throw new SessionPersistenceCorruptionError('DeepSeek tool Gateway key mismatch');
      }
      const result = await settleGatewayCall(this.gateway, {
        key, tool: call.data.name, prepared: intent.status === 'PREPARED',
        getArguments: () => JSON.parse(call.data.arguments),
        markDispatched: () => this.markDispatched(handle, key),
      });
      const event = {...fallback, seq: state.log.length};
      if (result.kind === 'known') {
        const {turn, step} = fallback.data;
        event.data = {turn, step, message: createToolResultMessage({
          callId: call.data.callId, content: result.payload.content, isError: Boolean(result.payload.isError),
        })};
      }
      const batch = materializeAppendBatch([event]);
      validateStoredEvents(state.header, [...state.log, ...batch]);
      state.log.push(...batch);
      await this.publish(handle);
    }
  }

  async markDispatched(handle, key) {
    return await this.client.request('POST', API + '/aux:mutate', {body: {
      session_id: this.sessionId(handle.id), lease_epoch: handle.epoch, transition: {
        call_id: key, gateway_operation_id: key, status: 'DISPATCHED',
      },
    }});
  }

  // The tool bridge calls this only after native checkpoint-policy flushed the
  // tool request. It passes the returned key to the actual MCP Gateway.
  async dispatchTool(id, callID) {
    const handle = this.writers.get(id);
    if (!handle) throw new SessionOwnershipLostError(id);
    handle.ensure('dispatchTool', true);
    return await handle.serialize(async () => {
      const call = handle.state.log.findLast(event => event.type === 'tool/call' &&
        event.data.callId === callID && this.effectTools.has(event.data.name));
      if (!call || call.seq >= handle.state.acked) throw new Error('Tool call has no durable checkpoint');
      const key = this.toolKey(handle.state, call);
      if (!this.gateway) {
        const digest = createHash('sha256').update(call.data.arguments).digest();
        const decision = await beginStandaloneTool(this.client, this.path(id), handle.epoch, key, call.data.name, digest);
        if (!decision.can_execute) {
          throw Object.assign(new Error('DeepSeek tool requires reconciliation'), {code: TOOL_OUTCOME_UNKNOWN});
        }
        return key;
      }
      const intent = await this.getIntent(id, key);
      if (intent.status !== 'PREPARED') throw new Error('Tool intent requires settlement before dispatch');
      await this.markDispatched(handle, key);
      return key;
    });
  }

  async release(handle) {
    clearInterval(handle.timer);
    try {
      if (!handle.lost) await this.client.request('POST', this.path(handle.id) + ':heartbeat', {body: {
        lease_epoch: handle.epoch, release: true,
      }});
    } finally {
      this.writers.delete(handle.id);
    }
  }

  snapshot(state) {
    return {header: structuredClone(state.header), formatStatus: 'current', eventCount: state.log.length,
      revision: SessionPersistenceRevision(`${state.id}:${state.head}:${state.log.length}`)};
  }

  async stat(id, options) {
    options?.signal?.throwIfAborted();
    try {
      return this.snapshot(this.writers.get(id)?.state ?? await this.load(id));
    } catch (error) {
      if (error instanceof SessionPersistenceNotFoundError) return undefined;
      throw error;
    }
  }

  async list(options) {
    const found = new Map();
    let cursor = '';
    do {
      options?.signal?.throwIfAborted();
      const page = await this.client.request('GET', API + '/sessions', {
        query: {cursor, limit: 100, harness: 'deepseek'},
      });
      for (const session of page.session_ids ?? []) {
        if (!session.startsWith(this.prefix)) continue;
        const id = session.slice(this.prefix.length);
        const info = await this.stat(id);
        if (info) found.set(id, info);
      }
      cursor = page.next_cursor;
    } while (cursor);
    for (const [id, writer] of this.writers) found.set(id, this.snapshot(writer.state));
    return [...found.values()];
  }

  async drain(close) {
    const outcomes = await Promise.allSettled([...this.writers.values()].map(handle => (
      close ? handle.close() : handle.closing ?? handle.flush()
    )));
    const failures = outcomes.filter(outcome => outcome.status === 'rejected').map(outcome => outcome.reason);
    if (failures.length) throw new AggregateError(failures, 'DeepSeek journal flush failed');
  }

  async flush() { await this.drain(false); }
  async dispose() { await this.drain(true); }
}
