import {createHash} from 'node:crypto';
import {isDeepStrictEqual} from 'node:util';
import {beginStandaloneTool, loadJournalJSON, storeJournalJSON} from '../payload.mjs';
import {settleGatewayCall} from '../tool-gateway.mjs';
import {assertActiveTurn, isTurnControl} from '../turn-control.mjs';

const API = '/v1/state';
const HEADER = 'oc:header';
const ENTRY = 'openclaw.entry';

function digest(value) {
  return createHash('sha256').update(value).digest('hex');
}

function canonical(value) {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key])]));
  }
  return value;
}

function eventId(id) {
  return 'oc:entry:' + id;
}

function toolKey(entry, callID) {
  return 'oc:tool:' + digest(JSON.stringify([entry.id, callID]));
}

function callsIn(entry) {
  return entry.message?.role === 'assistant'
    ? (entry.message.content ?? []).filter(block => block.type === 'toolCall')
    : [];
}

function event(id, type, role, payload, parent) {
  return {id, type, role, codec: 'json-openclaw-tree-v1', payload,
    ...(parent && {parent_id: parent})};
}

export class OpenClawOutcomeUnknown extends Error {
  constructor(callID) {
    super(`OpenClaw tool ${callID} requires external reconciliation`);
    this.name = 'OpenClawOutcomeUnknown';
    this.callID = callID;
  }
}

/** Native SessionStorage with PostgreSQL entry and tree-pointer commits. */
export class CompactOpenClawStorage {
  constructor(core, client, sessionId, {ttlMs = 30000, heartbeat = true, effectTools = [], catalog} = {}) {
    this.core = core;
    this.client = client;
    this.sessionId = sessionId;
    this.path = `${API}/sessions/${encodeURIComponent(sessionId)}`;
    this.ttlMs = ttlMs;
    this.heartbeat = heartbeat;
    this.effectTools = new Set(effectTools);
    this.catalog = catalog;
    this.tail = Promise.resolve();
    this.gatewayMode = false;
  }

  async open({readOnly = false, preclaimedTurn} = {}) {
    if (preclaimedTurn) {
      if (readOnly) throw new TypeError('Preclaimed turns require write access');
      await assertActiveTurn(this.client, this.path, this.sessionId, preclaimedTurn);
      this.epoch = preclaimedTurn.epoch;
      this.preclaimed = true;
    } else if (!readOnly) {
      const lease = await this.client.request('POST', API + '/sessions:acquire', {body: {
        session_id: this.sessionId, harness_type: 'openclaw', format_version: 1, ttl_ms: this.ttlMs,
      }});
      this.epoch = lease.epoch;
    }
    this.readOnly = readOnly;
    this.head = 0;
    try {
      await this.refresh();
      if (!this.view) {
        if (readOnly) throw new Error('OpenClaw journal is not materialized');
        const metadata = {id: this.sessionId, createdAt: new Date().toISOString()};
        this.view = new this.core.InMemorySessionStorage({metadata});
        await this.commit(event(HEADER, 'openclaw.header', 'system', await this.storePayload(metadata)),
          this.catalog === undefined ? {} : {catalog: this.catalog});
      }
      if (!readOnly && this.heartbeat && !preclaimedTurn) {
        this.timer = setInterval(() => this.renew().catch(error => {
          if (error.code === 'STALE_EPOCH') this.fence(error);
        }), Math.max(100, this.ttlMs / 3));
        this.timer.unref();
      }
      return this;
    } catch (error) {
      await this.close().catch(() => {});
      throw error;
    }
  }

  async refresh() {
    const getPage = after => this.client.request('GET', this.path + '/events', {
      query: {limit: 100, after_seq: after},
    });
    const first = await getPage(this.head);
    if (first.head_seq < this.head) throw new Error('OpenClaw journal head moved backwards');
    let cursor = this.head;
    let metadata;
    const entries = [];
    let page = first;
    while (cursor < first.head_seq) {
      const previous = cursor;
      for (const item of page.events ?? []) {
        if (item.seq > first.head_seq) break;
        if (item.seq !== cursor + 1) throw new Error('OpenClaw journal history is incomplete');
        cursor = item.seq;
        if (isTurnControl(item)) continue;
        if (!this.view && !metadata) {
          if (item.id !== HEADER || item.type !== 'openclaw.header') throw new Error('OpenClaw journal header missing');
          metadata = await loadJournalJSON(this.client, this.path, item.payload);
          if (metadata.id !== this.sessionId) throw new Error('OpenClaw journal identity mismatch');
          continue;
        }
        if (item.type !== ENTRY) throw new Error('Unexpected OpenClaw journal entry type');
        const entry = await loadJournalJSON(this.client, this.path, item.payload);
        if (item.id !== eventId(entry.id) ||
            (item.parent_id ?? '') !== (entry.parentId ? eventId(entry.parentId) : '')) {
          throw new Error('OpenClaw journal tree identity mismatch');
        }
        entries.push(entry);
      }
      if (previous === cursor) throw new Error('OpenClaw journal history is incomplete');
      if (cursor < first.head_seq) page = await getPage(cursor);
    }
    if (!cursor) return;
    try {
      if (metadata) this.view = new this.core.InMemorySessionStorage({metadata, entries});
      else for (const entry of entries) await this.view.appendEntry(entry);
      const leaf = await this.view?.getLeafId();
      const parent = await this.view?.getAppendParentId();
      if ((leaf ? eventId(leaf) : '') !== (first.leaf_entry_id ?? '') ||
          (parent ? eventId(parent) : '') !== (first.append_parent_id ?? '')) {
        throw new Error('OpenClaw journal pointers do not match the native tree');
      }
      this.head = cursor;
    } catch (error) {
      this.poison = error;
      this.view = undefined;
      throw error;
    }
  }

  fence(error) {
    clearInterval(this.timer);
    this.poison = error;
    this.view = undefined;
  }

  ensureWrite() {
    if (this.poison) throw this.poison;
    if (!this.epoch || this.closed) throw new Error('OpenClaw journal requires a write lease');
  }

  serialize(fn) {
    const pending = this.tail.then(async () => {
      this.ensureWrite();
      try {
        return await fn();
      } catch (error) {
        if (error.code === 'STALE_EPOCH') this.fence(error);
        throw error;
      }
    });
    this.tail = pending.catch(() => {});
    return pending;
  }

  async renew() {
    await this.client.request('POST', this.path + ':heartbeat', {body: {
      lease_epoch: this.epoch, ttl_ms: this.ttlMs,
    }});
  }

  storePayload(value) {
    return storeJournalJSON(this.client, this.path, this.epoch, value);
  }

  async commit(row, mutations = {}) {
    const body = {lease_epoch: this.epoch, key: row.id, expected_seq: this.head, events: [row], ...mutations};
    const write = () => this.client.request('POST', this.path + '/events:append', {body});
    let receipt;
    try {
      receipt = await write();
    } catch (error) {
      if (!error.outcomeUnknown) throw error;
      receipt = await write();
    }
    if (receipt.first_seq !== this.head + 1 || receipt.head_seq !== this.head + 1) {
      throw new Error('Unexpected OpenClaw journal receipt');
    }
    this.head = receipt.head_seq;
  }

  async read(method, ...args) {
    await this.tail;
    if (this.poison) throw this.poison;
    if (this.closed) throw new Error('OpenClaw journal is closed');
    if (this.readOnly) {
      this.refreshing ??= this.refresh().finally(() => { this.refreshing = undefined; });
      await this.refreshing;
    }
    return await this.view[method](...args);
  }

  getMetadata() { return this.read('getMetadata'); }
  getLeafId() { return this.read('getLeafId'); }
  getAppendParentId() { return this.read('getAppendParentId'); }
  createEntryId() { return this.read('createEntryId'); }
  getEntry(id) { return this.read('getEntry', id); }
  findEntries(type) { return this.read('findEntries', type); }
  getLabel(id) { return this.read('getLabel', id); }
  getPathToRoot(id) { return this.read('getPathToRoot', id); }
  getEntries() { return this.read('getEntries'); }

  async intent(key) {
    return await this.client.request('GET', API + '/aux:mutate', {query: {
      session_id: this.sessionId, call_id: key,
    }});
  }

  async transition(key, status) {
    return await this.client.request('POST', API + '/aux:mutate', {body: {
      session_id: this.sessionId, lease_epoch: this.epoch,
      transition: {call_id: key, gateway_operation_id: key, status},
    }});
  }

  async trackedCall(callID) {
    const entries = await this.view.getEntries();
    const found = entries.flatMap(entry => callsIn(entry)
      .filter(call => call.id === callID && this.effectTools.has(call.name))
      .map(call => ({entry, call})));
    if (found.length > 1) throw new Error('Ambiguous OpenClaw tool call identity');
    return found[0];
  }

  dispatchTool(callID, name, args) {
    return this.serialize(async () => {
      const call = await this.trackedCall(callID);
      if (!call || call.call.name !== name) throw new Error('OpenClaw tool call was not checkpointed');
      const key = toolKey(call.entry, callID);
      const status = await this.intent(key);
      const argHash = digest(JSON.stringify(canonical(args)));
      if (status.tool_name !== name || Buffer.from(status.args_digest, 'base64').toString('hex') !== argHash) {
        throw new Error('OpenClaw tool arguments changed after the checkpoint');
      }
      if (!this.gatewayMode) {
        const admission = await beginStandaloneTool(this.client, this.path, this.epoch, key, name, Buffer.from(argHash, 'hex'));
        if (!admission.can_execute) throw new OpenClawOutcomeUnknown(callID);
        return key;
      }
      if (status.status !== 'PREPARED') throw new OpenClawOutcomeUnknown(callID);
      await this.transition(key, 'DISPATCHED');
      return key;
    });
  }

  assertSafeResume() {
    return this.serialize(async () => {
      for (const entry of await this.view.getEntries()) {
        for (const call of callsIn(entry)) {
          if (!this.effectTools.has(call.name)) continue;
          const key = toolKey(entry, call.id);
          const record = await this.intent(key);
          if (record.status === 'COMPLETED') continue;
          if (!this.gatewayMode) {
            const hash = digest(JSON.stringify(canonical(call.arguments)));
            await beginStandaloneTool(this.client, this.path, this.epoch, key, call.name, Buffer.from(hash, 'hex'));
          } else if (record.status === 'DISPATCHED') {
            await this.transition(key, 'OUTCOME_UNKNOWN');
          }
          throw new OpenClawOutcomeUnknown(call.id);
        }
      }
    });
  }

  async reconcileGatewayTools(session, gateway) {
    this.ensureWrite();
    const leaf = await this.getLeafId();
    const appendParent = await this.getAppendParentId();
    const activePath = new Set((await this.getPathToRoot(leaf)).map(entry => entry.id));
    const pending = [];
    for (const entry of await this.getEntries()) {
      for (const call of callsIn(entry)) {
        if (!this.effectTools.has(call.name)) continue;
        const key = toolKey(entry, call.id);
        const record = await this.intent(key);
        if (record.status === 'COMPLETED') continue;
        if ((record.status !== 'PREPARED' && record.status !== 'DISPATCHED') ||
            !activePath.has(entry.id) || leaf !== appendParent) {
          throw new OpenClawOutcomeUnknown(call.id);
        }
        await this.trackedCall(call.id); // Reject duplicate native identities before making a Gateway request.
        const argHash = digest(JSON.stringify(canonical(call.arguments)));
        if (record.tool_name !== call.name || Buffer.from(record.args_digest, 'base64').toString('hex') !== argHash ||
            (record.status === 'DISPATCHED' && record.gateway_operation_id !== key)) {
          throw new Error('OpenClaw tool intent and journal disagree');
        }
        pending.push({call, key, record});
      }
    }
    for (const {call, key, record} of pending) {
      const result = await settleGatewayCall(gateway, {
        key, tool: call.name, prepared: record.status === 'PREPARED', getArguments: () => call.arguments,
        markDispatched: () => this.serialize(() => this.transition(key, 'DISPATCHED')),
      });
      const unknown = result.kind === 'unknown';
      const message = {role: 'toolResult', toolCallId: call.id, toolName: call.name, timestamp: Date.now(),
        content: unknown ? [{type: 'text', text: 'TOOL_OUTCOME_UNKNOWN'}] : result.payload.content,
        isError: unknown || Boolean(result.payload.isError)};
      await session.appendMessage(message);
      if (message.isError) throw new OpenClawOutcomeUnknown(call.id);
    }
  }

  async mutations(entry) {
    const intents = callsIn(entry).filter(call => this.effectTools.has(call.name)).map(call => {
      const key = toolKey(entry, call.id);
      return {call_id: key, tool_name: call.name, idempotency_key: key,
        args_digest: [...Buffer.from(digest(JSON.stringify(canonical(call.arguments))), 'hex')]};
    });
    const changes = intents.length ? {intents} : {};
    const message = entry.message;
    if (message?.role !== 'toolResult') return changes;
    const call = await this.trackedCall(message.toolCallId);
    if (!call) return changes;
    const key = toolKey(call.entry, message.toolCallId);
    const record = await this.intent(key);
    if (record.status === 'DISPATCHED') {
      changes.tool_outcomes = [{
        call_id: key, gateway_operation_id: record.gateway_operation_id, status: message.isError ? 'OUTCOME_UNKNOWN' : 'COMPLETED',
        ...(!message.isError && {result: await this.storePayload(message)}),
      }];
    }
    return changes;
  }

  save(method, value) {
    return this.serialize(async () => {
      try {
        const previous = await this.view.getEntries();
        if (method === 'appendEntry') {
          const existing = await this.view.getEntry(value.id);
          if (existing) {
            if (!isDeepStrictEqual(existing, value)) throw new Error('OpenClaw entry ID changed bytes');
            return;
          }
        }
        await this.view[method](value);
        const entries = await this.view.getEntries();
        if (entries.length !== previous.length + 1) throw new Error('OpenClaw mutation must append one native entry');
        const entry = entries.at(-1);
        const leaf = await this.view.getLeafId();
        const parent = await this.view.getAppendParentId();
        const mutations = await this.mutations(entry);
        await this.commit(event(eventId(entry.id), ENTRY, entry.message?.role ?? 'system', await this.storePayload(entry),
          entry.parentId ? eventId(entry.parentId) : undefined), {
          pointers: {leaf_entry_id: leaf ? eventId(leaf) : '', append_parent_id: parent ? eventId(parent) : ''},
          ...mutations,
        });
      } catch (error) {
        this.poison ??= error;
        throw this.poison;
      }
    });
  }

  appendEntry(entry) { return this.save('appendEntry', entry); }
  setLeafId(id) { return this.save('setLeafId', id); }

  close() {
    if (this.closing) return this.closing;
    clearInterval(this.timer);
    this.closed = true;
    this.closing = (async () => {
      try {
        await this.tail;
        await this.refreshing?.catch(() => {});
        if (this.epoch && !this.poison && !this.preclaimed) {
          await this.client.request('POST', this.path + ':heartbeat', {body: {
            lease_epoch: this.epoch, release: true,
          }});
        }
      } finally {
        this.epoch = undefined;
        this.view = undefined;
      }
    })();
    return this.closing;
  }
}

/** Keep finalized native messages durable before the next tool or model call. */
export async function attachCompactOpenClawAgent(core, storage, {gateway, ...options}) {
  const session = new core.Session(storage);
  storage.gatewayMode = Boolean(gateway);
  if (gateway) await storage.reconcileGatewayTools(session, gateway);
  const transcript = await session.buildContext();
  const tools = (options.initialState?.tools ?? []).map(tool => {
    if (!storage.effectTools.has(tool.name)) return tool;
    return {...tool, async execute(id, args, signal, onUpdate) {
      const key = await storage.dispatchTool(id, tool.name, args);
      return await tool.execute(id, args, signal, onUpdate, {idempotencyKey: key});
    }};
  });
  const agent = new core.Agent({
    ...options,
    initialState: {...options.initialState, messages: transcript.messages, tools},
  });
  let blocked;
  const unsubscribe = agent.subscribe(async event => {
    if (event.type !== 'message_end') return;
    const message = event.message;
    await session.appendMessage(message);
    if (message.role === 'toolResult' && message.isError && storage.effectTools.has(message.toolName)) {
      blocked = new OpenClawOutcomeUnknown(message.toolCallId);
      throw blocked;
    }
  });
  async function run(operation) {
    await storage.assertSafeResume();
    await operation();
    if (blocked) throw blocked;
  }
  return {
    agent, session, unsubscribe,
    prompt: text => run(() => agent.prompt(text)),
    continue: () => run(() => agent.continue()),
  };
}
