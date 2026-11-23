import assert from 'node:assert/strict';
import {EventEmitter} from 'node:events';
import https from 'node:https';
import test from 'node:test';
import {CompactJournalClient, JournalHTTPError} from '../src/index.mjs';
import {isTurnControl} from '../src/turn-control.mjs';

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

test('VM turn methods use the session-specific State API routes', async () => {
  const client = Object.create(CompactJournalClient.prototype);
  const calls = [];
  client.request = async (...args) => {
    calls.push(args);
    return {input_seq: 10};
  };
  const claim = {turn_seq: 10, generation: 2, execution_id: 'exec-a', ttl_ms: 30000};
  const requeue = {turn_seq: 10, lease_epoch: 2, execution_id: 'exec-a'};
  assert.deepEqual(await client.claimVMTurn('vm/session', claim), {input_seq: 10});
  await client.requeueFencedVMTurn('vm/session', requeue);
  assert.deepEqual(await client.cancelSession('vm/session', 10), {input_seq: 10});
  assert.deepEqual(calls, [
    ['POST', '/v1/state/sessions/vm%2Fsession/vm:claim', {body: claim}],
    ['POST', '/v1/state/sessions/vm%2Fsession/vm:requeue-fenced', {body: requeue}],
    ['POST', '/v1/state/sessions/vm%2Fsession/vm:cancel', {body: {turn_seq: 10}}],
  ]);
});

test('heartbeat delivers turn control through the existing renewal loop', async () => {
  const client = Object.create(CompactJournalClient.prototype);
  const controller = new AbortController();
  const control = {cancel_requested: true};
  let polls = 0;
  let observed;
  client.heartbeatLease = async () => {
    polls++;
    if (polls > 1) throw new Error('turn control was not delivered');
    return {control};
  };
  const heartbeat = client.startHeartbeat('session', 1, {
    intervalMs: 1, controller, onControl(delta) {
      observed = delta;
      controller.abort();
    },
  });
  await heartbeat.done;
  assert.equal(observed, control);
  assert.equal(polls, 1);
});

test('native journal readers skip canonical turn control and SDK completion rows', () => {
  const ingress = {codec: 'json', role: 'user', id: 'turn.input:' + 'a'.repeat(64)};
  const output = {codec: 'json', type: 'turn.output', id: 'turn.output:4', role: 'assistant', turn_seq: 4};
  const completed = {codec: 'json', type: 'turn.completed', id: 'turn.completed:4'};
  for (const row of [
    {...ingress, type: 'turn.input', turn_seq: 4},
    {...ingress, type: 'turn.steer', turn_seq: 4},
    {...ingress, type: 'turn.followup'},
    {...completed, role: 'assistant'},
    {...completed, role: 'system', turn_seq: 4},
    {codec: 'json', type: 'ahp.state.mutation', id: 'state-mutation-' + 'a'.repeat(64), role: 'system', turn_seq: 4},
    output,
  ]) assert.equal(isTurnControl(row), true, row.type);
  for (const row of [
    {...output, turn_seq: 5},
    {...output, turn_seq: undefined},
    {...output, role: 'system'},
    {...output, parent_id: 'native-parent'},
    {...output, type: 'native'},
    {...completed, role: 'system'},
  ]) assert.equal(isTurnControl(row), false, row.type);
});
