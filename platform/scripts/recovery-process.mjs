#!/usr/bin/env node
// Dedicated, fail-closed process recovery lane. No .env discovery and no stand
// restart: test-owned processes bind ephemeral localhost ports and use this DSN.
import { spawn, spawnSync } from 'node:child_process';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
for (const key of ['AHP_TEST_DATABASE_URL', 'AHP_RECOVERY_PYTHON']) {
  if (!process.env[key]) {
    console.error(`FAIL: ${key} must be explicitly configured; see tests/space/RECOVERY.md`);
    process.exit(1);
  }
}
const preflight = spawnSync(process.env.AHP_RECOVERY_PYTHON,
  ['-c', 'import ahp_state, langgraph, httpx'], { cwd: resolve(root, 'sdk/python'), stdio: 'inherit' });
if (preflight.error || preflight.status !== 0) {
  console.error('FAIL: AHP_RECOVERY_PYTHON must have sdk/python[dev,langgraph] installed');
  process.exit(1);
}
const child = spawn('go', ['test', '-race', '-p', '1', '-tags', 'state_integration',
  './tests/space', '-run', '^(TestSDKProcessRecovery_|TestTerminalFrameSurvivesTransientFailure_RealState$|TestCanceledThreadSurvivesTransientFailure_RealState$)',
  '-count=1', '-v', '-timeout=5m'], {
  cwd: root, env: process.env, detached: process.platform !== 'win32',
  stdio: ['ignore', 'pipe', 'inherit'],
});
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, () => {
    try {
      if (process.platform === 'win32') child.kill(signal);
      else process.kill(-child.pid, signal);
    } catch {}
  });
}
let transcript = '';
child.stdout.on('data', chunk => { transcript += chunk; process.stdout.write(chunk); });
child.on('error', () => { console.error('FAIL: could not start Go recovery tests'); process.exitCode = 1; });
child.on('close', code => {
  // A Go test timeout bypasses testing.Cleanup. Reap only this runner's process
  // group so a timed-out SDK subprocess cannot keep polling the test DB.
  if (process.platform !== 'win32') {
    try { process.kill(-child.pid, 'SIGKILL'); } catch {}
  }
  if (code !== 0) { process.exitCode = code || 1; return; }
  if (/--- SKIP:/.test(transcript) || !/--- PASS: TestSDKProcessRecovery_LostEffectACKAndPlaneRestart/.test(transcript)
      || !/--- PASS: TestSDKProcessRecovery_HITLTokenExpiryAndRevocation/.test(transcript)) {
    console.error('FAIL: required process recovery assertions were skipped or not executed');
    process.exitCode = 1;
    return;
  }
  console.log('PASS: process recovery contract (State/Identity/Coordinator + LangGraph SDK + real PG/mTLS). Not LLM E2E.');
});
