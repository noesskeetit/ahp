// Run against a fresh standalone State API on localhost using its signed handshake.
import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {readFileSync} from 'node:fs';
import {performance} from 'node:perf_hooks';
import {setTimeout as sleep} from 'node:timers/promises';
import {promisify} from 'node:util';
import {CompactJournalClient, inlineJSON} from '../../adapters/journal/compact-client.mjs';

const exec = promisify(execFile);
const leaseMs = 60000;
const spillEvery = 8;
const spillBytes = 320 << 10;
const inlineBytes = 2048;
const checkpointsPerTurn = 3;

function positive(value, min, max) {
  const number = Number(value);
  assert(Number.isSafeInteger(number) && number >= min && number <= max, `Expected an integer from ${min} to ${max}`);
  return number;
}

const handshake = process.env.AHP_STATE_HANDSHAKE;
assert(handshake, 'Set AHP_STATE_HANDSHAKE to a dedicated local stand');
const origin = new URL(JSON.parse(readFileSync(handshake, 'utf8')).base_url);
assert(origin.protocol === 'https:' && ['127.0.0.1', '::1', '[::1]', 'localhost'].includes(origin.hostname),
  'Pressure probe requires a loopback State API');
const run = process.env.AHP_PRESSURE_RUN;
assert(run && /^[a-z0-9][a-z0-9-]{0,31}$/.test(run), 'Set a fresh AHP_PRESSURE_RUN prefix');
const levels = (process.env.AHP_PRESSURE_LEVELS ?? '1,4,8,16,32,64,128')
  .split(',').map(value => positive(value, 1, 256));
assert.equal(new Set(levels).size, levels.length, 'Supply each concurrency level once');
const baseTurns = positive(process.env.AHP_PRESSURE_TURNS ?? '384', 1, 10000);
const waitMs = positive(process.env.AHP_PRESSURE_WAIT_MS ?? '40', 1, 1000);
const samplesPerWorker = positive(process.env.AHP_PRESSURE_SAMPLES_PER_WORKER ?? '8', 1, 10000);
const pids = Object.fromEntries((process.env.AHP_PRESSURE_PIDS ?? '').split(',').filter(Boolean).map(entry => {
  const [name, pid] = entry.split('=');
  assert(/^[a-z][a-z0-9-]*$/.test(name), 'Invalid process label');
  return [name, positive(pid, 1, 0x7fffffff)];
}));
pids.generator = process.pid;

function quantiles(values) {
  if (!values.length) return {n: 0};
  const sorted = values.toSorted((a, b) => a - b);
  return Object.fromEntries([['n', sorted.length], ...[50, 95, 99].map(percentile => {
    const value = sorted[Math.ceil((percentile / 100) * sorted.length) - 1];
    return [`p${percentile}`, Number(value.toFixed(2))];
  })]);
}

function cpuSeconds(value) {
  const [clock, centis] = value.split('.');
  const seconds = clock.split(':').reduce((total, field) => total * 60 + Number(field), 0);
  return seconds + (centis ? Number('0.' + centis) : 0);
}

// A process-tree sum may count shared RSS more than once. CPU seconds are
// process time, and can omit a process that exits between samples.
function sampler(roots) {
  const groups = Object.fromEntries(Object.keys(roots).map(name => [name, {
    rssKiB: 0, processes: 0, before: new Map(), after: new Map(),
  }]));
  let timer;
  let pending;
  let errors = 0;
  let samples = 0;
  async function sample() {
    try {
      const {stdout} = await exec('ps', ['-axo', 'pid=,ppid=,rss=,time='], {maxBuffer: 4 << 20});
      const children = new Map();
      const processes = new Map();
      for (const row of stdout.trim().split('\n')) {
        const [pid, ppid, rss, cpu] = row.trim().split(/\s+/);
        const id = Number(pid);
        const parent = Number(ppid);
        if (!children.has(parent)) children.set(parent, []);
        children.get(parent).push(id);
        processes.set(id, {rss: Number(rss), cpu: cpuSeconds(cpu)});
      }
      for (const [name, root] of Object.entries(roots)) {
        const group = groups[name];
        let rss = 0;
        const queue = processes.has(root) ? [root] : [];
        for (const pid of queue) {
          if (pid !== root && Object.values(roots).includes(pid)) continue;
          const item = processes.get(pid);
          queue.push(...(children.get(pid) ?? []));
          rss += item.rss;
          if (samples === 0) group.before.set(pid, item.cpu);
          group.after.set(pid, item.cpu);
        }
        group.processes = Math.max(queue.length, group.processes);
        group.rssKiB = Math.max(group.rssKiB, rss);
      }
      samples++;
    } catch {
      errors++;
    }
  }
  return {
    async start() {
      await sample();
      timer = setInterval(() => { pending ??= sample().finally(() => { pending = undefined; }); }, 250);
      timer.unref();
    },
    async finish(elapsedSeconds) {
      clearInterval(timer);
      await pending;
      await sample();
      return {samples, errors, interval_ms: 250, processes: Object.fromEntries(Object.entries(groups)
        .map(([name, group]) => {
          let cpu = 0;
          for (const [pid, value] of group.after) cpu += Math.max(0, value - (group.before.get(pid) ?? 0));
          return [name, {peak_process_sum_rss_mib: Number((group.rssKiB / 1024).toFixed(2)),
            peak_processes: group.processes, cpu_seconds: Number(cpu.toFixed(2)),
            average_cpu_cores: Number((cpu / elapsedSeconds).toFixed(2))}];
        }))};
    },
  };
}

async function measure(concurrency, turns, label) {
  const prefix = `${run}-${label}-`;
  const submitted = new Map();
  const counters = Object.create(null);
  const recorded = Object.create(null);
  const errors = [];
  let cursor = 0;
  let peakHTTP = 0;
  let inFlightHTTP = 0;
  let peakActive = 0;
  let active = 0;
  let peakModel = 0;
  let model = 0;
  let peakQueued = 0;
  let queued = 0;
  let halt = false;
  const clients = Array.from({length: concurrency}, () => new CompactJournalClient(handshake));

  function latency(name, duration) {
    (recorded[name] ??= []).push(duration);
  }

  async function call(name, request, retry = false) {
    const operation = counters[name] ??= {attempts: 0, ok: 0, errors: 0, retries: 0, errorCodes: {}};
    for (let attempt = 0; ; attempt++) {
      operation.attempts++;
      const start = performance.now();
      peakHTTP = Math.max(peakHTTP, ++inFlightHTTP);
      try {
        const reply = await request();
        operation.ok++;
        latency(name, performance.now() - start);
        return reply;
      } catch (error) {
        const code = error.code ?? error.name;
        operation.errors++;
        operation.errorCodes[code] = (operation.errorCodes[code] ?? 0) + 1;
        if (!retry || attempt > 0 || !error.outcomeUnknown) throw error;
        operation.retries++;
      } finally {
        inFlightHTTP--;
      }
    }
  }

  async function worker(client) {
    while (!halt && cursor < turns) {
      const sequence = cursor++;
      const session = prefix + sequence;
      const sent = {start: performance.now(), sequence, claimed: false, admitted: false};
      submitted.set(session, sent);
      try {
        const enqueue = await call('enqueue', () => client.enqueueTurn({
          session_id: session, harness_type: 'langgraph', format_version: 1,
          key: 'enqueue', input: inlineJSON({session, prompt: 'Summarize the local benchmark fixture.'}),
        }), true);
        assert.equal(enqueue.session_id, session);
        assert.equal(enqueue.head_seq, 1);
        sent.admitted = true;
        if (!sent.claimed) peakQueued = Math.max(peakQueued, ++queued);
        // Workers claim from the shared ready queue. A claim can take a turn
        // enqueued by another worker, whose acknowledgement may still be in flight.
        const turn = await call('claim', () => client.claimTurn({ttl_ms: leaseMs}));
        const received = performance.now();
        const record = submitted.get(turn.session_id);
        assert(record && !record.claimed, 'Claim returned an unknown or duplicate turn');
        record.claimed = true;
        if (record.admitted) queued--;
        assert.equal(turn.input_seq, 1);
        assert.equal(turn.head_seq, 1);
        assert.equal(turn.epoch, 1);
        assert.equal(JSON.parse(Buffer.from(turn.input.inline, 'base64')).session, turn.session_id);
        latency('enqueue_to_claim', received - record.start);
        peakActive = Math.max(peakActive, ++active);
        try {
          for (let step = 1; step <= checkpointsPerTurn; step++) {
            if (step === checkpointsPerTurn) {
              peakModel = Math.max(peakModel, ++model);
              const waitStart = performance.now();
              try {
                await sleep(waitMs);
              } finally {
                model--;
              }
              latency('model_wait', performance.now() - waitStart);
            }
            let payload = inlineJSON({step, context: 'a'.repeat(inlineBytes)});
            if (step === checkpointsPerTurn && record.sequence % spillEvery === 0) {
              const data = Buffer.alloc(spillBytes, step);
              data.write(turn.session_id);
              payload = {blob: await call('stage_blob', () => client.stageBlob(turn.session_id, turn.epoch, data))};
            }
            const commit = {
              lease_epoch: turn.epoch, key: `checkpoint-${step}`,
              checkpoint: {namespace: '', id: `cp-${step}`, snapshot_seq: turn.head_seq,
                ...(step > 1 && {parent_id: `cp-${step - 1}`}), metadata: {step}, channel_versions: {messages: step},
                codec: 'benchmark', payload},
            };
            const receipt = await call('checkpoint', () => client.putCheckpoint(turn.session_id, commit), true);
            assert.equal(receipt.checkpoint_id, commit.checkpoint.id);
          }
          const finish = await call('finish', () => client.appendEvents(turn.session_id, {
            lease_epoch: turn.epoch, key: 'finish', expected_seq: turn.head_seq, finish_turn: 'IDLE',
            events: [{id: 'done', type: 'turn.completed', role: 'assistant', codec: 'benchmark',
              payload: inlineJSON({session: turn.session_id, result: 'bounded-benchmark', steps: checkpointsPerTurn})}],
          }), true);
          assert.equal(finish.first_seq, turn.head_seq + 1);
          assert.equal(finish.head_seq, turn.head_seq + 1);
          record.terminal = true;
          const finished = performance.now();
          latency('claimed_to_finished', finished - received);
          latency('end_to_end', finished - record.start);
        } finally {
          active--;
        }
      } catch (error) {
        halt = true;
        errors.push({sequence, code: error.code ?? error.name});
      }
    }
  }

  const resources = sampler(pids);
  await resources.start();
  const started = performance.now();
  try {
    await Promise.all(clients.map(worker));
  } finally {
    for (const client of clients) client.close();
  }
  const elapsed = (performance.now() - started) / 1000;
  const countersWithRate = Object.fromEntries(Object.entries(counters).map(([name, count]) => [
    name, {...count, per_second: Number((count.ok / elapsed).toFixed(2))},
  ]));
  const finished = [...submitted.values()].filter(row => row.terminal).length;
  const report = {
    run, prefix, label, scheduled_concurrency: concurrency, offered_turns: turns, attempted_turns: cursor,
    finished_turns: finished, seconds: Number(elapsed.toFixed(3)),
    turns_per_second: Number((finished / elapsed).toFixed(2)),
    peak: {leased_turns: peakActive, model_waits: peakModel, pending_http: peakHTTP, locally_observed_queued: peakQueued},
    operations: countersWithRate, latency_ms: Object.fromEntries(Object.entries(recorded)
      .map(([stage, values]) => [stage, quantiles(values)])),
    resource_sampling: await resources.finish(elapsed), errors,
  };
  console.log(JSON.stringify(report));
  assert.equal(errors.length, 0, 'The level had failed turns');
  assert.equal(finished, turns, 'The level did not finish every offered turn');
}

console.log(JSON.stringify({configuration: {run, node: process.version, levels, base_turns: baseTurns,
  min_turns_per_worker: samplesPerWorker, model_wait_ms: waitMs, lease_ms: leaseMs,
  checkpoint_commits: checkpointsPerTurn, inline_padding_bytes: inlineBytes,
  spill_bytes: spillBytes, spill_every: spillEvery, loop: 'closed', native_harness: false}}));
await measure(Math.min(levels[0], 8), Math.max(8, levels[0]), 'warmup');
for (const concurrency of levels) {
  await measure(concurrency, Math.max(baseTurns, concurrency * samplesPerWorker), `c${concurrency}`);
}
