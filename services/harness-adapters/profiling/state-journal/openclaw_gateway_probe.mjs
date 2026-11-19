// Run with OpenClaw 2026.6.11. All generated state stays in one temp directory.
import assert from 'node:assert/strict';
import {execFile, spawn} from 'node:child_process';
import {createHash} from 'node:crypto';
import {once} from 'node:events';
import fs from 'node:fs';
import {createServer} from 'node:net';
import path from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';
import {promisify} from 'node:util';

const run = fs.mkdtempSync(path.join(process.argv[2] ?? '/tmp', 'openclaw-gateway-'));
const cli = process.env.AHP_OPENCLAW_CLI ?? 'openclaw';
const file = name => path.join(run, name);
const writeJSON = (name, data) => fs.writeFileSync(file(name), JSON.stringify(data) + '\n');
const readJSON = name => JSON.parse(fs.readFileSync(file(name), 'utf8'));
console.log(run);

const listener = createServer();
listener.listen(0, '127.0.0.1');
await once(listener, 'listening');
const port = listener.address().port;
await promisify(listener.close.bind(listener))();

const state = file('state');
const sessions = 'state/agents/main/sessions';
const store = sessions + '/sessions.json';
const key = 'agent:main:fixture';
const sessionId = '60b8d2f4-a5f0-4ccd-8f2d-bc946075711a';
const marker = 'ahp native transcript marker';
fs.mkdirSync(file(sessions), {recursive:true});
fs.mkdirSync(file('workspace'));
writeJSON(store, {[key]:{sessionId, updatedAt:Date.now(), label:'ahp-native-index'}});
const transcript = [
  {type:'session', version:3, id:sessionId, timestamp:new Date().toISOString(), cwd:file('workspace')},
  {type:'message', id:'724a4a83', parentId:null, timestamp:new Date().toISOString(),
    message:{role:'user', content:[{type:'text', text:marker}], timestamp:Date.now()}},
];
fs.writeFileSync(file(sessions + '/' + sessionId + '.jsonl'), transcript.map(row=>JSON.stringify(row)).join('\n')+'\n');
writeJSON('openclaw.json', {
  gateway:{mode:'local', bind:'loopback', auth:{mode:'none'}, controlUi:{enabled:false}, port},
  discovery:{mdns:{mode:'off'}}, cron:{enabled:false}, plugins:{enabled:false},
  logging:{file:file('openclaw-runtime.log')}, update:{checkOnStart:false},
  agents:{defaults:{workspace:file('workspace'), heartbeat:{every:'0m'}}}, session:{store:file(store)},
});
fs.writeFileSync(file('trace.cjs'), `
const fs = require('node:fs');
const read = fs.readFileSync;
fs.readFileSync = function(file, ...args) {
  const value = read.call(this, file, ...args);
  if (file === process.env.AHP_GATEWAY_PROBE_STORE) {
    fs.appendFileSync(process.env.AHP_GATEWAY_PROBE_TRACE, JSON.stringify({
      bytes:Buffer.byteLength(value), stack:new Error().stack.split('\\n').slice(2,10)
    })+'\\n');
  }
  return value;
};
`);
const env = {
  PATH:process.env.PATH, OPENCLAW_STATE_DIR:state, OPENCLAW_CONFIG_PATH:file('openclaw.json'),
  OPENCLAW_SKIP_CHANNELS:'1', OPENCLAW_SKIP_STARTUP_MODEL_PREWARM:'1',
  OPENCLAW_SESSION_CACHE_TTL_MS:'0',
};
const exec = promisify(execFile);
const version = (await exec(cli, ['--version'], {env, timeout:10000})).stdout.trim();
assert.match(version, /2026\.6\.11/);
const logfile = fs.openSync(file('gateway.log'), 'w', 0o600);
const gateway = spawn(cli, ['gateway', 'run', '--port', String(port), '--bind', 'loopback'], {
  env:{...env, NODE_OPTIONS:'--require='+file('trace.cjs'), AHP_GATEWAY_PROBE_STORE:file(store),
    AHP_GATEWAY_PROBE_TRACE:file('store-reads.jsonl')},
  stdio:['ignore', logfile, logfile],
});
fs.closeSync(logfile);
const exited = once(gateway, 'exit');
const trace = () => fs.existsSync(file('store-reads.jsonl'))
  ? fs.readFileSync(file('store-reads.jsonl'),'utf8').trim().split('\n').map(JSON.parse)
  : [];
const observations = {};
async function call(sample, method, params, admin = false) {
  const before = trace().length;
  // Admin and read calls pair with separate local devices in this fixture.
  const clientEnv = {...env, OPENCLAW_STATE_DIR:file(admin ? 'admin-client' : 'read-client')};
  const {stdout} = await exec(cli, ['gateway', 'call', method, '--params', JSON.stringify(params),
    '--timeout', '8000', '--json'], {env:clientEnv, timeout:20000});
  const result = JSON.parse(stdout);
  writeJSON(sample+'.json', result);
  observations[sample] = trace().slice(before);
  return result;
}

try {
  for (let i=0; i<100 && !fs.readFileSync(file('gateway.log'),'utf8').includes('[gateway] ready'); i++) {
    assert.equal(gateway.exitCode, null, 'Gateway exited during startup');
    await delay(150);
  }
  assert.match(fs.readFileSync(file('gateway.log'),'utf8'), /\[gateway\] ready/);
  assert.equal((await call('small-list', 'sessions.list', {limit:1})).sessions[0].label,'ahp-native-index');
  assert.match(JSON.stringify(await call('small-history','chat.history',{sessionKey:key, limit:1})), new RegExp(marker));
  await call('small-patch', 'sessions.patch', {key, label:'ahp-patched-index'}, true);
  const entries = readJSON(store);
  assert.equal(entries[key].label, 'ahp-patched-index');

  const prompt = 'p'.repeat(22500);
  for (let i=1; i<1407; i++) {
    entries['agent:main:fixture-'+i] = {sessionId:'fixture-id-'+i, updatedAt:Date.now()-100000-i,
      label:'fixture-'+i, skillsSnapshot:{prompt, skills:[], version:1}};
  }
  writeJSON(store, entries);
  const seededBytes = fs.statSync(file(store)).size;
  for (const label of ['first', 'repeat']) {
    const list = await call('large-list-'+label, 'sessions.list', {limit:1});
    assert.equal(list.sessions.length, 1);
    assert.equal(list.sessions[0].label, 'ahp-patched-index');
    assert.equal(list.totalCount, 1407);
    assert.ok(observations['large-list-'+label].some(row=>row.bytes===seededBytes &&
      row.stack.some(line=>line.includes('loadCombinedSessionStoreForGateway'))));
  }
  assert.match(JSON.stringify(await call('large-history','chat.history',{sessionKey:key, limit:1})), new RegExp(marker));
  await call('large-patch','sessions.patch',{key, label:'ahp-dense-patched'},true);
  const persisted = readJSON(store);
  assert.equal(persisted[key].label,'ahp-dense-patched');
  // This stock release enforces a default 500-entry maintenance cap on write.
  assert.equal(Object.keys(persisted).length, 500);
  const refs = Object.values(persisted).map(row=>row.skillsSnapshot?.promptRef?.hash).filter(Boolean);
  assert.equal(refs.length, 499);
  assert.deepEqual([...new Set(refs)], [createHash('sha256').update(prompt).digest('hex')]);
  const result = {version, cacheTtlMs:0, seededSessions:Object.keys(entries).length, seededBytes,
    persistedSessions:Object.keys(persisted).length, persistedBytes:fs.statSync(file(store)).size, promptReferences:refs.length,
    uniquePromptHashes:new Set(refs).size, observations};
  writeJSON('result.json', result);
  console.log(JSON.stringify({...result, observations:Object.fromEntries(
    Object.entries(observations).map(([sample, rows])=>[sample,rows.map(row=>row.bytes)]))}));
} finally {
  gateway.kill('SIGTERM');
  const deadline = AbortSignal.timeout(5000);
  await Promise.race([exited, once(deadline, 'abort')]);
  if (gateway.exitCode === null && gateway.signalCode === null) {
    gateway.kill('SIGKILL');
    await exited;
  }
}
