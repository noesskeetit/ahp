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
import {beginStandaloneTool, loadJournalJSON, storeJournalJSON} from '../payload.mjs';
import {settleGatewayCall} from '../tool-gateway.mjs';
import {assertActiveTurn, isTurnControl} from '../turn-control.mjs';

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
  constructor(backend, state, access, shared = false) {
    this.backend = backend;
    this.state = shared ? undefined : state;
    this.access = access;
    this.id = state.id;
    this.header = state.header;
    this.inheritedEventCount = state.inheritedEventCount;
    this.tail = Promise.resolve();
    this.routed = [];
    this.closed = false;
    this.lost = false;
    this.steering = new Map();
    this.cancelRequested = false;
  }

  receiveTurnControl(control) {
    if (control.turn_seq !== undefined && control.turn_seq !== this.turnSeq) return;
    for (const event of control.pending_steer ?? []) {
      if (event.turn_seq === this.turnSeq && event.seq > this.steerAcked &&
          event.type === 'turn.steer' && !this.steering.has(event.seq)) {
        this.steering.set(event.seq, {event: structuredClone(event)});
      }
    }
    if (control.cancel_requested && !this.cancelRequested) {
      this.cancelRequested = true;
      this.agent?.cancel({kind: 'user'});
    }
  }

  async pendingSteerMessages() {
    const messages = [];
    const ids = new Set();
    for (const input of [...this.steering.values()].sort((a, b) => a.event.seq - b.event.seq)) {
      if (input.nativeSeq !== undefined) continue;
      if (!this.backend.decodeSteer) throw new Error('DeepSeek VM steering requires a native message decoder');
      input.decoding ??= Promise.resolve(this.backend.decodeSteer(input.event)).then(message => structuredClone(message));
      input.message = await input.decoding;
      if (input.message?.role !== 'user' || !input.message.id || input.message.source?.kind !== 'user') {
        throw new TypeError('Expected a DeepSeek user message for steering');
      }
      if (ids.has(input.message.id)) throw new TypeError('Expected distinct DeepSeek steering message IDs');
      ids.add(input.message.id);
      messages.push(input.message);
    }
    return messages;
  }

  steerAck(end) {
    if (this.steering.size === 0) return undefined;
    // The native events and ACK go into the same append transaction.
    const targets = [...this.steering.values()].sort((a, b) => a.event.seq - b.event.seq);
    for (const event of this.state.log.slice(this.state.acked, end)) {
      if (event.type !== 'user/message' || event.surfaceOp !== 'append') continue;
      const target = targets.find(input => isDeepStrictEqual(input.message, event.data));
      if (target) target.nativeSeq ??= event.seq;
    }
    const firstPending = targets.findIndex(input => !(input.nativeSeq < end));
    const applied = firstPending === -1 ? targets : targets.slice(0, firstPending);
    if (targets.slice(applied.length).some(input => input.nativeSeq < end)) {
      throw new SessionPersistenceCorruptionError('DeepSeek steering prefix is not durable in order');
    }
    if (applied.length > 0) {
      return {turn_seq: this.turnSeq, up_to_seq: applied.at(-1).event.seq, count: applied.length};
    }
  }

  confirmSteer(ack) {
    if (!ack) return;
    this.steerAcked = ack.up_to_seq;
    for (const seq of this.steering.keys()) {
      if (seq <= this.steerAcked) this.steering.delete(seq);
    }
  }

  route(event) {
    if (!this.closed && !this.lost) this.routed.push(event);
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
    if (this.lost) throw new SessionOwnershipLostError(this.id);
  }

  serialize(fn) {
    const work = this.tail.then(fn).catch(error => {
      if (this.access === 'write' && error.code === 'STALE_EPOCH') {
        this.loseOwnership();
        throw new SessionOwnershipLostError(this.id);
      }
      throw error;
    });
    this.tail = work.catch(() => {});
    return work;
  }

  async read(offset = 0, length, options) {
    this.ensure('read');
    options?.signal?.throwIfAborted();
    if (!nonnegative(offset) || (length !== undefined && !nonnegative(length))) {
      throw new RangeError('Expected a nonnegative offset and length');
    }
    return this.serialize(async () => {
      this.ensure('read');
      let state = this.state;
      if (this.access === 'read') {
        const owner = this.backend.writers.get(this.id)?.state;
        if (owner?.log && owner.head >= (state?.head ?? 0)) {
          state = owner;
        } else {
          state = this.state = await this.backend.load(this.id, state, options?.signal);
        }
      }
      return {eventState: 'detached', events: structuredClone(state.log.slice(
        offset, length === undefined ? undefined : offset + length))};
    });
  }

  loseOwnership() {
    this.lost = true;
    this.state = undefined;
    this.routed.length = 0;
    this.steering.clear();
    this.agent = undefined;
    clearInterval(this.timer);
    if (this.backend.writers.get(this.id) === this) this.backend.writers.delete(this.id);
  }

  append(events, options) {
    options?.signal?.throwIfAborted();
    this.ensure('append', true);
    const batch = materializeAppendBatch(events);
    return this.serialize(async () => {
      if (this.lost) throw new SessionOwnershipLostError(this.id);
      assertContiguous(this.id, batch, this.state.log.length);
      this.state.log.push(...batch);
    });
  }

  flush(options) {
    options?.signal?.throwIfAborted();
    this.ensure('flush', true);
    return this.serialize(() => this.drainRouted());
  }

  close() {
    if (this.closing) return this.closing;
    this.closed = true;
    clearInterval(this.timer);
    this.closing = this.serialize(async () => {
      if (this.access === 'read') {
        this.state = undefined;
        return;
      }
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
  constructor(ctx, {
    client, prefix = 'dsh-', ttlMs = 30000, heartbeat = true, effectTools = [], gateway,
    preclaimedTurn, decodeSteer,
  }) {
    super(ctx);
    this.client = client;
    this.prefix = prefix;
    this.ttlMs = ttlMs;
    this.heartbeat = heartbeat;
    this.effectTools = new Set(effectTools);
    this.gateway = gateway;
    this.preclaimedTurn = preclaimedTurn;
    this.decodeSteer = decodeSteer;
    this.writers = new Map();
    ctx.on('session/event', (session, event) => this.writers.get(session.id)?.route(event));
    ctx.on('agent/pre-step', async ({agent, signal}, next) => {
      const handle = this.writers.get(agent.session.id);
      if (handle?.turnSeq === undefined) return next();
      handle.agent = agent;
      if (handle.cancelRequested) agent.cancel({kind: 'user'});
      signal.throwIfAborted();
      const decision = await next();
      if (decision.kind !== 'enter') return decision;
      const steered = await handle.pendingSteerMessages();
      signal.throwIfAborted();
      const admitted = new Set(decision.messages.map(message => message.id));
      return {...decision, messages: [...decision.messages, ...steered.filter(message => !admitted.has(message.id))]};
    });
    ctx.on('agent/turn-stopping', async ({agent, signal}) => {
      const handle = this.writers.get(agent.session.id);
      if (handle?.turnSeq === undefined) return;
      await handle.flush();
      const [message] = await handle.pendingSteerMessages();
      signal.throwIfAborted();
      if (message) agent.steer(message);
    });
    ctx.on('session/flush', session => this.writers.get(session.id)?.flush());
    ctx.on('session/disposed', session => this.writers.get(session.id)?.close().catch(error => {
      ctx.logger.warn(`DeepSeek journal close failed: ${error.name}`);
    }));
    ctx.effect(() => () => this.dispose());
  }

  acceptTurnControl(id, turnSeq, control) {
    const handle = this.writers.get(id);
    if (handle?.turnSeq !== undefined && handle.turnSeq === turnSeq && !handle.closed && !handle.lost) {
      handle.receiveTurnControl(control);
    }
  }

  async resumePendingSteer(id, turnSeq) {
    const handle = this.writers.get(id);
    const agent = handle?.agent;
    const mayResume = () => handle?.turnSeq !== undefined && handle.turnSeq === turnSeq &&
      !handle.closed && !handle.lost &&
      !handle.cancelRequested && agent?.status === 'idle';
    if (!mayResume()) return false;
    const [message] = await handle.pendingSteerMessages();
    if (!message || !mayResume()) return false;
    agent.steer(message);
    return true;
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

  async getPage(id, after, limit, signal) {
    try {
      return await this.client.request('GET', this.path(id) + '/events', {
        query: {limit, after_seq: after}, signal,
      });
    } catch (error) {
      if (error.code === 'NOT_FOUND') throw new SessionPersistenceNotFoundError(id);
      throw error;
    }
  }

  verifyHeader(id, native) {
    const header = materializeCreateHeader(native);
    assertStoredId(id, header);
    assertVersion(header);
    return header;
  }

  async readHeader(id, row) {
    if (row?.id !== 'dsh:header' || row.type !== HEADER) {
      throw new SessionPersistenceCorruptionError('Missing DeepSeek header');
    }
    const {header: native, inheritedEventCount, effectKeyVersion = 1} =
      await loadJournalJSON(this.client, this.path(id), row.payload);
    if (effectKeyVersion !== 1 && effectKeyVersion !== 2) {
      throw new SessionPersistenceCorruptionError('Unknown DeepSeek tool-key format');
    }
    if (!nonnegative(inheritedEventCount)) {
      throw new SessionPersistenceCorruptionError('Invalid DeepSeek inherited prefix');
    }
    return {id, header: this.verifyHeader(id, native), inheritedEventCount, effectKeyVersion,
      log: [], head: row.seq, acked: 0, headerCommitted: true};
  }

  async load(id, state, signal, allowUnmaterialized = false) {
    let cursor = state?.head ?? 0;
    let page = await this.getPage(id, cursor, 100, signal);
    const head = page.head_seq;
    if (head < cursor) {
      throw new SessionPersistenceCorruptionError('DeepSeek journal head moved backwards');
    }
    while (cursor < head) {
      const after = cursor;
      const suffix = [];
      for (const item of page.events ?? []) {
        if (item.seq > head) break;
        if (item.seq !== cursor + 1) {
          throw new SessionPersistenceCorruptionError('DeepSeek journal has a sequence gap');
        }
        cursor = item.seq;
        if (isTurnControl(item)) continue;
        if (!state) {
          state = await this.readHeader(id, item);
          continue;
        }
        if (item.type !== EVENT) {
          throw new SessionPersistenceCorruptionError('Unexpected DeepSeek journal record');
        }
        suffix.push(await loadJournalJSON(this.client, this.path(id), item.payload));
      }
      if (cursor === after) throw new SessionPersistenceCorruptionError('Missing DeepSeek journal tail');
      if (state) {
        validateStoredEvents(state.header, suffix);
        assertContiguous(id, suffix, state.log.length);
        state.log.push(...suffix);
        state.head = cursor;
        state.acked = state.log.length;
      }
      if (cursor < head) page = await this.getPage(id, cursor, 100, signal);
    }
    if (!state) {
      if (allowUnmaterialized) return {head};
      throw new SessionPersistenceNotFoundError(id);
    }
    if (state.inheritedEventCount > state.log.length) {
      throw new SessionPersistenceCorruptionError('Invalid DeepSeek inherited prefix');
    }
    return state;
  }

  async readFirstHeader(id, signal) {
    let page = await this.getPage(id, 0, 1, signal);
    const head = page.head_seq;
    let cursor = 0;
    while (cursor < head) {
      const after = cursor;
      for (const item of page.events ?? []) {
        if (item.seq > head) break;
        if (item.seq !== cursor + 1) {
          throw new SessionPersistenceCorruptionError('DeepSeek journal has a sequence gap');
        }
        cursor = item.seq;
        if (!isTurnControl(item)) return {head, header: (await this.readHeader(id, item)).header};
      }
      if (cursor === after) throw new SessionPersistenceCorruptionError('Missing DeepSeek journal header page');
      if (cursor < head) page = await this.getPage(id, cursor, 100, signal);
    }
    throw new SessionPersistenceNotFoundError(id);
  }

  async inspect(id, signal) {
    try {
      const state = await this.client.request('GET', this.path(id), {query: {view: 'resume'}, signal});
      if (!state.head_seq) throw new SessionPersistenceNotFoundError(id);
      if (state.harness_type !== 'deepseek') throw new SessionPersistenceCorruptionError('Unexpected DeepSeek session harness');
      return state;
    } catch (error) {
      if (error.code === 'NOT_FOUND') throw new SessionPersistenceNotFoundError(id);
      throw error;
    }
  }

  track(state, lease, preclaimed = false) {
    const handle = new CompactSessionHandle(this, state, 'write');
    handle.epoch = lease.epoch;
    handle.preclaimed = preclaimed;
    handle.turnSeq = preclaimed ? lease.turn_seq : undefined;
    handle.steerAcked = handle.turnSeq;
    this.writers.set(state.id, handle);
    if (this.heartbeat && !preclaimed) {
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
    if (this.writers.has(id)) throw new SessionAlreadyExistsError(id);
    const preclaimed = options?.preclaimedTurn ?? this.preclaimedTurn?.(id);
    let head = 0;
    let lease;
    if (preclaimed) {
      await assertActiveTurn(this.client, this.path(id), this.sessionId(id), preclaimed, options?.signal);
      const stored = await this.load(id, undefined, options?.signal, true);
      if (stored.header || this.writers.has(id)) throw new SessionAlreadyExistsError(id);
      head = stored.head;
      lease = preclaimed;
    } else {
      if (await this.stat(id)) throw new SessionAlreadyExistsError(id);
      try {
        lease = await this.acquire(id);
      } catch (error) {
        if (error instanceof SessionAlreadyOwnedError) throw new SessionAlreadyExistsError(id);
        throw error;
      }
      if (lease.head_seq > 0) {
        await this.client.request('POST', this.path(id) + ':heartbeat', {body: {
          lease_epoch: lease.epoch, release: true,
        }});
        throw new SessionAlreadyExistsError(id);
      }
    }
    return this.track({id, header, inheritedEventCount, effectKeyVersion: 2, log: [], head,
      acked: 0, headerCommitted: false}, lease, Boolean(preclaimed));
  }

  async open(id, access, options) {
    options?.signal?.throwIfAborted();
    const writer = this.writers.get(id);
    const preclaimed = options?.preclaimedTurn ?? (access === 'write' ? this.preclaimedTurn?.(id) : undefined);
    if (access === 'read') {
      if (preclaimed) throw new TypeError('Preclaimed turns require write access');
      const shared = writer?.state?.log ? writer.state : undefined;
      return new CompactSessionHandle(this, shared ?? await this.load(id, undefined, options?.signal), access, Boolean(shared));
    }
    if (access !== 'write') throw new TypeError('Expected read or write access');
    if (writer) throw new SessionAlreadyOwnedError(id);
    let lease;
    if (preclaimed) {
      await assertActiveTurn(this.client, this.path(id), this.sessionId(id), preclaimed, options?.signal);
      if (this.writers.has(id)) throw new SessionAlreadyOwnedError(id);
      lease = preclaimed;
    } else {
      await this.inspect(id, options?.signal);
      lease = await this.acquire(id);
    }
    const handle = this.track({id}, lease, Boolean(preclaimed));
    try {
      handle.state = await this.load(id, undefined, options?.signal);
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
    await this.client.request('POST', this.path(handle.id) + ':heartbeat', {body: {
      lease_epoch: handle.epoch, ttl_ms: this.ttlMs,
    }});
  }

  async makeBatch(handle) {
    const {state, epoch, turnSeq} = handle;
    const storePayload = value => storeJournalJSON(this.client, this.path(state.id), epoch, value);
    const rows = !state.headerCommitted ? [row('dsh:header', HEADER, 'system', await storePayload({
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
    if (!state.headerCommitted && end < state.inheritedEventCount) {
      throw new RangeError('Inherited DeepSeek prefix must fit the first journal transaction');
    }
    const summary = {header: state.header, eventCount: end};
    // Large native headers remain in the journal; the catalog has a 4 KiB cap.
    if (Buffer.byteLength(JSON.stringify(summary, null, 1)) > 4000) delete summary.header;
    const catalog = {native_key: state.id, summary_metadata: summary};
    const request = {lease_epoch: epoch, key: `dsh:append:${state.head}`,
      expected_seq: state.head, events: rows, catalog};
    if (turnSeq !== undefined) {
      request.turn_seq = turnSeq;
      const ack = handle.steerAck(end);
      if (ack) request.steer_ack = ack;
    }
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
        ...(turnSeq !== undefined && {turn_seq: turnSeq}),
        status: unknown ? 'OUTCOME_UNKNOWN' : 'COMPLETED',
        ...(!unknown && {result: await storePayload(event.data.message)})});
    }
    if (intents.length > 0) request.intents = intents;
    if (outcomes.length > 0) request.tool_outcomes = outcomes;
    return {end, request};
  }

  async refreshControlHead(handle) {
    // Only ingress rows may race our native append. A terminal or native row
    // here means another writer progressed and this batch must remain fenced.
    const state = handle.state;
    let cursor = state.head;
    let page = await this.getPage(handle.id, cursor, 100);
    const head = page.head_seq;
    while (cursor < head) {
      const previous = cursor;
      for (const event of page.events ?? []) {
        if (event.seq > head) break;
        const ingress = (event.type === 'turn.steer' && event.turn_seq === handle.turnSeq) ||
          (event.type === 'turn.followup' && !event.turn_seq);
        if (event.seq !== cursor + 1 || !ingress || !isTurnControl(event)) return false;
        cursor = event.seq;
      }
      if (cursor === previous) return false;
      if (cursor < head) page = await this.getPage(handle.id, cursor, 100);
    }
    if (head <= state.head) return false;
    state.head = head;
    return true;
  }

  async publish(handle) {
    const state = handle.state;
    if (!handle.preclaimed) await this.renew(handle);
    let rebased = false;
    while (state.pending || !state.headerCommitted || state.log.length > state.acked) {
      state.pending ??= await this.makeBatch(handle);
      const {request, end} = state.pending;
      let receipt;
      try {
        receipt = await this.client.request('POST', this.path(state.id) + '/events:append', {body: request});
      } catch (error) {
        if (!rebased && handle.turnSeq !== undefined && error.code === 'CONFLICT' &&
            await this.refreshControlHead(handle)) {
          rebased = true;
          state.pending = undefined;
          continue;
        }
        throw error;
      }
      if (receipt.head_seq !== state.head + request.events.length || receipt.first_seq !== state.head + 1) {
        throw new SessionPersistenceCorruptionError('Invalid State API append receipt');
      }
      state.head = receipt.head_seq;
      state.headerCommitted = true;
      state.acked = end;
      state.pending = undefined;
      handle.confirmSteer(request.steer_ack);
      if (receipt.control && handle.turnSeq !== undefined) handle.receiveTurnControl(receipt.control);
      rebased = false;
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
      await beginStandaloneTool(this.client, this.path(handle.id), handle.epoch, key, call.data.name, digest, handle.turnSeq);
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
        ...(handle.turnSeq !== undefined && {turn_seq: handle.turnSeq}),
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
        const decision = await beginStandaloneTool(this.client, this.path(id), handle.epoch, key, call.data.name, digest, handle.turnSeq);
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
      if (!handle.lost && !handle.preclaimed) await this.client.request('POST', this.path(handle.id) + ':heartbeat', {body: {
        lease_epoch: handle.epoch, release: true,
      }});
    } finally {
      if (this.writers.get(handle.id) === handle) this.writers.delete(handle.id);
      handle.state = undefined;
      handle.routed.length = 0;
      handle.steering.clear();
      handle.agent = undefined;
    }
  }

  snapshot({id, head, header, eventCount, log, acked}) {
    const count = eventCount ?? log?.length;
    const pending = log ? log.length - acked : 0;
    return {header: structuredClone(header), formatStatus: 'current',
      ...(count !== undefined && {eventCount: count}), revision: SessionPersistenceRevision(`${id}:${head}:${pending}`)};
  }

  async stat(id, options) {
    const signal = options?.signal;
    signal?.throwIfAborted();
    const local = this.writers.get(id)?.state;
    if (local?.log) return this.snapshot(local);
    try {
      return this.snapshot({id, ...await this.readFirstHeader(id, signal)});
    } catch (error) {
      if (error instanceof SessionPersistenceNotFoundError) return undefined;
      throw error;
    }
  }

  async list(options) {
    const found = new Map();
    const signal = options?.signal;
    let cursor;
    do {
      signal?.throwIfAborted();
      const page = await this.client.request('GET', API + '/sessions', {
        query: {cursor, limit: 100, view: 'catalog', harness: 'deepseek'}, signal,
      });
      for (const session of page.sessions) {
        if (!session.session_id.startsWith(this.prefix) || !session.head_seq) continue;
        const id = session.session_id.slice(this.prefix.length);
        const summary = session.summary_metadata;
        const hasCount = nonnegative(summary?.eventCount) && summary.eventCount < session.head_seq;
        if (summary?.header && hasCount) {
          const header = this.verifyHeader(id, summary.header);
          found.set(id, this.snapshot({id, header, head: session.head_seq, eventCount: summary.eventCount}));
        } else {
          // Existing sessions and oversized metadata use a header-only fallback.
          const metadata = await this.readFirstHeader(id, signal).catch(error => {
            if (!(error instanceof SessionPersistenceNotFoundError)) throw error;
          });
          if (metadata) {
            found.set(id, this.snapshot({id, ...metadata,
              eventCount: hasCount && metadata.head === session.head_seq ? summary.eventCount : undefined}));
          }
        }
      }
      cursor = page.next_cursor;
    } while (cursor);
    for (const [id, writer] of this.writers) {
      if (writer.state?.log) found.set(id, this.snapshot(writer.state));
    }
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
