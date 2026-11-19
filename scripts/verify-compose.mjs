#!/usr/bin/env node
// scripts/verify-compose.mjs — Mac-safe STATIC gate for acceptance #1 "fresh compose".
// Validates compose.yaml WITHOUT a daemon/containers (read-only `docker compose config`):
//   * schema-valid render (config rc=0) under the `app` profile,
//   * the four separated service roles (all/state/identity/model) each render with AHP_ROLE
//     set and a healthcheck,
//   * no mutable `:latest` app image (locally-pinned tag only) and a pinned MinIO image.
// Does NOT build, start, or touch any container (never `up`); live ahp-handbook-go-* untouched.
// Skips (rc=3) if docker compose is unavailable (iv/VM may run the real `up` instead).
import { spawnSync } from 'node:child_process';
const COMPOSE = process.env.COMPOSE_FILE || 'compose.yaml';
const ROLES = ['all','state','identity','model'];
let pass=0, fail=0; const ok=(n,x='')=>{pass++;console.log('  OK '+n+(x?'  '+x:''))}; const bad=(n,x='')=>{fail++;console.log('  FAIL '+n+(x?'  '+x:''))};
const have = spawnSync('docker', ['compose','version'], { encoding:'utf8' });
if (have.status !== 0) { console.log('SKIP: docker compose unavailable (iv-VM: run the real up there)'); process.exit(3); }
const r = spawnSync('docker', ['compose','-f',COMPOSE,'config'], { encoding:'utf8', env:{...process.env, COMPOSE_PROFILES:'app', HTTP_PROXY:'',HTTPS_PROXY:'',http_proxy:'',https_proxy:'', NO_PROXY:'localhost,127.0.0.1,::1'} });
if (r.status !== 0) { console.log('compose config FAILED rc='+r.status+'\n'+String(r.stderr).slice(0,600)); process.exit(1); }
ok('compose.yaml schema-valid (docker compose config rc=0, app profile)');
const out = r.stdout;
// block per service: capture each role service's env+healthcheck section
for (const role of ROLES) {
  const svc = 'go-ahp-'+role;
  const blockRe = new RegExp('\\n  '+svc+':\\n([\\s\\S]*?)(?=\\n  [a-z]|$)');
  const m = out.match(blockRe);
  if (!m) { bad('service '+svc+' present',''); continue; }
  const blk = m[1];
  const hasRole = new RegExp('AHP_ROLE: '+role+'\\b').test(blk);
  const hasHc = /healthcheck:/.test(blk);
  (hasRole&&hasHc?ok:bad)(svc+': AHP_ROLE='+role+' + healthcheck', 'role='+hasRole+' hc='+hasHc);
}
const noLatest = !/:latest\b/.test(out);
(noLatest?ok:bad)('no mutable :latest image tags in rendered compose');
const minioPinned = /image:\s*\S*minio\S*:(?!latest)\S+/.test(out);
(minioPinned?ok:bad)('MinIO image pinned (non-latest tag)');
console.log(fail===0?`\nCOMPOSE STATIC GATE: PASS (${pass}) — fresh separated-roles compose schema-valid + roles+healthchecks + pinned images (VM up remains the runtime proof)`:`\nCOMPOSE STATIC GATE: FAIL (${fail}/${pass+fail})`);
process.exitCode = fail===0?0:1;
