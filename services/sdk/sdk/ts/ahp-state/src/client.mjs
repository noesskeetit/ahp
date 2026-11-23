import https from 'node:https';
import {createHash} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
import {readFileSync} from 'node:fs';

export class JournalHTTPError extends Error {
  constructor(status, code, outcomeUnknown = false) {
    super(`State journal: ${code}`);
    this.status = status;
    this.code = code;
    this.outcomeUnknown = outcomeUnknown;
  }
}

const API = '/v1/state';
const sessions = API + '/sessions';
const sessionPath = id => sessions + '/' + encodeURIComponent(id);

export class CompactJournalClient {
  constructor(config) {
    if (typeof config === 'string') {
      const handshakePath = config;
      const connection = JSON.parse(readFileSync(handshakePath, 'utf8'));
      config = {
        baseURL: connection.base_url, caFile: connection.ca_file,
        certFile: connection.cert_file, keyFile: connection.key_file,
        tokenSource: () => JSON.parse(readFileSync(handshakePath, 'utf8')).task_token,
      };
    }
    this.origin = new URL(config.baseURL);
    if (typeof config.tokenSource !== 'function') {
      throw new Error('State API requires a task token source');
    }
    this.tokenSource = config.tokenSource;
    if (this.origin.protocol !== 'https:' || this.origin.origin + '/' !== this.origin.href) {
      throw new Error('State API requires a bare HTTPS origin');
    }
    this.agent = new https.Agent({
      ca: readFileSync(config.caFile),
      cert: readFileSync(config.certFile),
      key: readFileSync(config.keyFile),
      keepAlive: true,
      maxSockets: 8,
    });
  }

  async request(method, path, {body, query, signal} = {}) {
    const url = new URL(path, this.origin);
    if (url.origin !== this.origin.origin || !url.pathname.startsWith('/v1/state/')) {
      throw new Error('Expected a relative State API route');
    }
    if (query instanceof URLSearchParams) url.search = query.toString();
    else if (query) {
      const parameters = new URLSearchParams();
      for (const [key, value] of Object.entries(query)) {
        if (value === undefined) continue;
        for (const item of Array.isArray(value) ? value : [value]) parameters.append(key, item);
      }
      url.search = parameters.toString();
    }
    return await this.#send(method, url, body, signal);
  }

  async requestTool(operation, body) {
    if (!['Call', 'Get', 'GetResult', 'FenceAndLookup'].includes(operation)) {
      throw new Error('Unsupported Tool Gateway operation');
    }
    return await this.#send('POST', new URL('/v1/tools/' + operation, this.origin), body);
  }

  async #send(method, url, body, signal) {
    signal?.throwIfAborted();
    const token = await this.tokenSource();
    if (typeof token !== 'string' || !token.trim()) throw new JournalHTTPError(0, 'FORBIDDEN');
    const mutation = method !== 'GET';
    const encoded = body === undefined ? undefined : JSON.stringify(body);
    const headers = {authorization: 'Bearer ' + token, accept: 'application/json'};
    if (encoded !== undefined) {
      headers['content-type'] = 'application/json';
      headers['content-length'] = Buffer.byteLength(encoded);
    }
    return new Promise((resolve, reject) => {
      const unknown = () => reject(new JournalHTTPError(0, 'TRANSPORT', mutation));
      const onError = () => signal?.aborted ? reject(signal.reason) : unknown();
      const request = https.request(url, {agent: this.agent, method, headers, signal}, response => {
        const chunks = [];
        let size = 0;
        response.on('data', chunk => {
          size += chunk.length;
          if (size > 8 << 20) return request.destroy();
          chunks.push(chunk);
        });
        response.on('end', () => {
          const status = response.statusCode;
          const success = status >= 200 && status < 300;
          const text = Buffer.concat(chunks).toString('utf8').trim();
          let parsed;
          if (text) {
            try {
              parsed = JSON.parse(text);
            } catch {
              reject(new JournalHTTPError(status, 'INVALID_RESPONSE', mutation));
              return;
            }
          }
          if (success) resolve(parsed);
          else reject(new JournalHTTPError(status, parsed?.reason || parsed?.code || 'HTTP_ERROR',
            mutation && status >= 500));
        });
        response.on('aborted', onError);
        response.on('error', onError);
      });
      request.setTimeout(15000, () => request.destroy());
      request.on('error', onError);
      request.end(encoded);
    });
  }

  acquireSession(body) {
    return this.request('POST', sessions + ':acquire', {body});
  }

  heartbeatLease(id, epoch, {ttlMs, signal} = {}) {
    return this.request('POST', sessionPath(id) + ':heartbeat', {
      body: {lease_epoch: epoch, ttl_ms: ttlMs}, signal,
    });
  }

  async releaseLease(id, epoch, status) {
    await this.request('POST', sessionPath(id) + ':heartbeat', {
      body: {lease_epoch: epoch, release: true, status},
    });
  }

  startHeartbeat(id, epoch, {ttlMs = 30000, intervalMs = ttlMs / 3, controller = new AbortController(), onControl, vmLease} = {}) {
    if (!Number.isFinite(intervalMs) || intervalMs < 1 || intervalMs >= ttlMs) {
      throw new RangeError('Heartbeat interval must be positive and shorter than the lease TTL');
    }
    const stop = new AbortController();
    const signal = stop.signal;
    const abort = () => stop.abort(controller.signal.reason);
    if (controller.signal.aborted) abort();
    else controller.signal.addEventListener('abort', abort, {once: true});
    let fenced = false;
    let error;
    const done = (async () => {
      while (!signal.aborted) {
        try {
          await delay(intervalMs, undefined, {signal});
          const receipt = await this.heartbeatLease(id, epoch, {ttlMs, signal});
          if (!signal.aborted) {
            const control = vmLease ? vmLease.applyHeartbeat(receipt) : receipt?.control;
            if (control) await onControl?.(control);
          }
          error = undefined;
        } catch (failure) {
          if (signal.aborted) return;
          error = failure;
          if (failure?.code !== 'TRANSPORT' && !(failure?.status >= 500)) {
            fenced = failure?.code === 'STALE_EPOCH';
            controller.abort(failure);
            return;
          }
        }
      }
    })().finally(() => controller.signal.removeEventListener('abort', abort));
    return {
      get fenced() { return fenced; },
      get error() { return error; },
      signal: controller.signal,
      done,
      stop: () => { stop.abort(); return done; },
    };
  }

  enqueueTurn(body) {
    return this.request('POST', sessions + ':enqueue', {body});
  }

  claimTurn(body = {}) {
    return this.request('POST', sessions + ':claim-turn', {body});
  }

  claimVMTurn(id, body) {
    return this.request('POST', sessionPath(id) + '/vm:claim', {body});
  }

  async requeueFencedVMTurn(id, body) {
    await this.request('POST', sessionPath(id) + '/vm:requeue-fenced', {body});
  }

  cancelSession(id, turnSeq) {
    return this.request('POST', sessionPath(id) + '/vm:cancel', {body: {turn_seq: turnSeq}});
  }

  getTurnStatus(id) {
    return this.request('GET', sessionPath(id));
  }

  inspectSession(id) {
    return this.request('GET', sessionPath(id), {query: {view: 'resume'}});
  }

  listSessions(query) {
    return this.request('GET', sessions, {query});
  }

  listCatalog(query) {
    return this.request('GET', sessions, {query: {...query, view: 'catalog'}});
  }

  appendEvents(id, body) {
    return this.request('POST', sessionPath(id) + '/events:append', {body});
  }

  getEvents(id, query) {
    return this.request('GET', sessionPath(id) + '/events', {query});
  }

  putCheckpoint(id, body) {
    return this.request('POST', sessionPath(id) + '/checkpoints', {body});
  }

  getCheckpoint(id, query) {
    return this.request('GET', sessionPath(id) + '/checkpoints', {query});
  }

  listCheckpoints(id, {metadata, ...query} = {}) {
    return this.request('GET', sessionPath(id) + '/checkpoints', {
      query: {...query, list: true, metadata: metadata === undefined ? undefined : JSON.stringify(metadata)},
    });
  }

  async putWrites(id, checkpointId, body) {
    await this.request('POST', sessionPath(id) + '/checkpoints/' + encodeURIComponent(checkpointId) + '/writes', {body});
  }

  getKV(query) {
    return this.request('GET', API + '/kv', {query});
  }

  mutateKV(body) {
    return this.request('POST', API + '/kv:mutate', {body});
  }

  getChatHistory(id, query) {
    return this.request('GET', sessionPath(id) + '/chat-history', {query});
  }

  stageBlob(id, epoch, data) {
    return this.request('POST', sessionPath(id) + '/blobs:stage', {
      body: {lease_epoch: epoch, data: Buffer.from(data).toString('base64')},
    });
  }

  async readBlob(id, ref) {
    const reply = await this.request('POST', sessionPath(id) + '/blobs:read', {body: ref});
    const data = Buffer.from(reply.data, 'base64');
    if (data.length !== ref.size_bytes || createHash('sha256').update(data).digest('hex') !== ref.sha256) {
      throw new Error('Journal blob does not match its descriptor');
    }
    return data;
  }

  beginStandaloneTool(id, body) {
    return this.request('POST', sessionPath(id) + '/tools:begin-standalone', {body});
  }

  getToolIntent(sessionId, callId) {
    return this.request('GET', API + '/aux:mutate', {query: {session_id: sessionId, call_id: callId}});
  }

  getUserState(namespace) {
    return this.request('GET', API + '/aux:mutate', {query: {state_namespace: namespace}});
  }

  async mutateToolIntent(body) {
    await this.request('POST', API + '/intents:mutate', {body});
  }

  async deleteSession(id) {
    await this.request('DELETE', sessionPath(id));
  }

  async setLegalHold(id, held) {
    await this.request('PATCH', sessionPath(id) + '/legal-hold', {body: {held}});
  }

  close() {
    this.agent.destroy();
  }
}

export function inlineJSON(value) {
  const data = Buffer.from(JSON.stringify(value), 'utf8');
  if (data.length > 256 << 10) throw new RangeError('Journal value requires artifact spill');
  return {inline: data.toString('base64')};
}

export function readInlineJSON(payload) {
  if (typeof payload.inline !== 'string') throw new Error('Journal value requires artifact hydration');
  return JSON.parse(Buffer.from(payload.inline, 'base64').toString('utf8'));
}
