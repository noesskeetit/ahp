import https from 'node:https';
import {readFileSync} from 'node:fs';

export class JournalHTTPError extends Error {
  constructor(status, code, outcomeUnknown = false) {
    super(`State journal: ${code}`);
    this.status = status;
    this.code = code;
    this.outcomeUnknown = outcomeUnknown;
  }
}

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

  async request(method, path, {body, query} = {}) {
    const url = new URL(path, this.origin);
    if (url.origin !== this.origin.origin || !url.pathname.startsWith('/v1/state/')) {
      throw new Error('Expected a relative State API route');
    }
    if (query) url.search = new URLSearchParams(query).toString();
    return await this.#send(method, url, body);
  }

  async requestTool(operation, body) {
    if (!['Call', 'Get', 'GetResult', 'FenceAndLookup'].includes(operation)) {
      throw new Error('Unsupported Tool Gateway operation');
    }
    return await this.#send('POST', new URL('/v1/tools/' + operation, this.origin), body);
  }

  async #send(method, url, body) {
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
      const request = https.request(url, {agent: this.agent, method, headers}, response => {
        const chunks = [];
        let size = 0;
        response.on('data', chunk => {
          size += chunk.length;
          if (size > 8 << 20) return request.destroy();
          chunks.push(chunk);
        });
        response.on('end', () => {
          let parsed;
          try {
            parsed = JSON.parse(Buffer.concat(chunks).toString('utf8'));
          } catch {
            reject(new JournalHTTPError(response.statusCode, 'INVALID_RESPONSE', mutation));
            return;
          }
          if (response.statusCode === 200) {
            resolve(parsed);
          } else {
            reject(new JournalHTTPError(response.statusCode, parsed.reason || parsed.code || 'HTTP_ERROR',
              mutation && response.statusCode >= 500));
          }
        });
        response.on('aborted', unknown);
        response.on('error', unknown);
      });
      request.setTimeout(15000, () => request.destroy());
      request.on('error', unknown);
      request.end(encoded);
    });
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
