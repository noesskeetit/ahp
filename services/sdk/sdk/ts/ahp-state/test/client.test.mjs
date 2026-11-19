import assert from 'node:assert/strict';
import {EventEmitter} from 'node:events';
import https from 'node:https';
import test from 'node:test';
import {CompactJournalClient, JournalHTTPError} from '../src/index.mjs';

test('the HTTPS transport accepts 204 and empty 200 responses', async t => {
  const responses = [204, 200];
  t.mock.method(https, 'request', (_url, _options, send) => {
    const request = new EventEmitter();
    request.setTimeout = () => {};
    request.end = () => {
      const response = new EventEmitter();
      response.statusCode = responses.shift();
      send(response);
      response.emit('end');
    };
    return request;
  });
  const client = new CompactJournalClient({
    baseURL: 'https://state.example', caFile: '/dev/null', certFile: '/dev/null', keyFile: '/dev/null',
    tokenSource: () => 'token',
  });
  t.after(() => client.close());
  assert.equal(await client.request('DELETE', '/v1/state/sessions/example'), undefined);
  assert.equal(await client.request('POST', '/v1/state/sessions/example:heartbeat'), undefined);
});

test('heartbeat fences and aborts when its lease epoch becomes stale', async () => {
  const client = Object.create(CompactJournalClient.prototype);
  const controller = new AbortController();
  client.heartbeatLease = async () => { throw new JournalHTTPError(409, 'STALE_EPOCH'); };
  const heartbeat = client.startHeartbeat('session', 1, {intervalMs: 1, controller});
  await heartbeat.done;
  assert.equal(heartbeat.fenced, true);
  assert.equal(controller.signal.aborted, true);
  assert.equal(controller.signal.reason.code, 'STALE_EPOCH');
});
