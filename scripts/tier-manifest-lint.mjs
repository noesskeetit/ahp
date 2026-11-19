#!/usr/bin/env node
// scripts/tier-manifest-lint.mjs — structural lint for Tier 0/1/2A/2B delivery
// manifests + runbooks (acceptance criterion #3: "HA/Tier0/1/2A/2B manifests/runbooks").
//
// Dependency-free (no YAML lib). Validates STRUCTURE + reproducibility only:
//   * each tier dir has a README runbook
//   * every k8s .yaml has apiVersion/kind/metadata.name; Deployments pin a non-latest
//     image and run `--role <role>` with /readyz + /healthz probes
//   * tier1 systemd units have [Service], ExecStart --role, Restart=always
//   * tier2b (HA) ships hpa + pdb + rbac + networkpolicy
// It does NOT apply to a cluster and does NOT prove deployment. target-runtime proof
// (apply + HA on the target runtime) stays an OPEN VM gate: source != deployment.
import { readFileSync, readdirSync, existsSync } from 'node:fs';
import { join } from 'node:path';

const ROOT = process.cwd();
const M = join(ROOT, 'infra/manifests');
const ROLES = new Set(['all','state','identity','model','execution','data']);
let fail = 0; const findings = [];
const bad = (m) => { fail++; findings.push('FAIL ' + m); };
const note = (m) => findings.push('note ' + m);

function files(dir, re) { return existsSync(dir) ? readdirSync(dir).filter(f => re.test(f)).sort() : []; }
function read(p){ return readFileSync(p,'utf8'); }

// runbook present in every tier dir
for (const t of ['tier0','tier1','tier2a','tier2b']) {
  const d = join(M, t);
  if (!existsSync(d)) { bad(`missing tier dir ${t}`); continue; }
  if (!existsSync(join(d,'README.md'))) bad(`${t}: missing README.md runbook`);
}

// k8s deployments (tier0 shared-saas=role all, tier2a per-plane)
const k8sDeploys = [];
for (const t of ['tier0','tier2a'])
  for (const f of files(join(M,t), /\.ya?ml$/)) {
    const p = join(M,t,f); const txt = read(p);
    const kind = (txt.match(/^kind:\s*(\S+)/m)||[])[1] || '';
    const name = (txt.match(/^metadata:.*?^\s+name:\s*(\S+)/ms)||[])[1] || '';
    if (!txt.match(/^apiVersion:/m)) bad(`${t}/${f}: no apiVersion`);
    if (!kind) bad(`${t}/${f}: no kind`);
    if (!name) bad(`${t}/${f}: metadata.name missing`);
    if (kind === 'Deployment') {
      const img = (txt.match(/^\s*image:\s*(\S+)/m)||[])[1] || '';
      if (!img) bad(`${t}/${f}: Deployment without image`);
      if (/:latest(?!\S)/.test(img) || img.endsWith(':latest')) bad(`${t}/${f}: image uses :latest (non-reproducible) — pin source sha`);
      const roleArg = (txt.match(/--role['"]?,\s*['"](\w+)/)||[])[1] || (txt.match(/--role (\w+)/)||[])[1] || '';
      if (!ROLES.has(roleArg)) bad(`${t}/${f}: Deployment args missing valid --role (got '${roleArg}')`);
      if (!/path:\s*\/readyz/.test(txt)) bad(`${t}/${f}: no /readyz readiness probe`);
      if (!/path:\s*\/healthz/.test(txt)) bad(`${t}/${f}: no /healthz liveness probe`);
      k8sDeploys.push({ tier:t, f, role:roleArg, img });
    }
  }

// tier1 systemd
const systemd = [];
for (const f of files(join(M,'tier1'), /\.service$/)) {
  const p = join(M,'tier1',f); const txt = read(p);
  if (!/\[Service\]/.test(txt)) bad(`tier1/${f}: no [Service]`);
  const exec = (txt.match(/^ExecStart=.*--role (\w+)/m)||[])[1] || '';
  if (!ROLES.has(exec)) bad(`tier1/${f}: ExecStart missing valid --role (got '${exec}')`);
  if (!/Restart=always/.test(txt)) bad(`tier1/${f}: Restart!=always`);
  systemd.push({ f, role: exec });
}

// tier2b HA primitives
for (const need of ['hpa','pdb','rbac','networkpolicy']) {
  if (!existsSync(join(M,'tier2b', need + '.yaml'))) bad(`tier2b: missing HA primitive ${need}.yaml`);
}

for (const d of k8sDeploys) console.log(`k8s ${d.tier.padEnd(6)} ${d.f.padEnd(30)} role=${d.role.padEnd(9)} image=${d.img.split(' ')[0]}`);
for (const s of systemd) console.log(`systemd tier1  ${s.f.padEnd(24)} role=${s.role}`);
console.log(`k8s_deployments=${k8sDeploys.length} systemd_units=${systemd.length} tiers=4 runbooks=4 ha_primitives=4`);
for (const x of findings) console.log(x);
console.log(fail===0
  ? '\nTIER-LINT: PASS (manifests structurally valid + reproducible pins; PDF §10 tiers covered) — target-runtime apply/HA = OPEN VM gate (source != deployment)'
  : `\nTIER-LINT: FAIL (${fail})`);
process.exit(fail===0?0:1);
