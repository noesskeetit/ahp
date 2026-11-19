#!/usr/bin/env node
// DEF-GATE-SKIP-POLICY: skip-aware acceptance verdict for a `go test -json` run.
//
// Ownership of FORM vs MECHANISM. The acceptance vocabulary (the kinds of skip,
// the red/yellow split, the acceptance code names, the requirement to declare
// environment completeness BEFORE a run, and the tag dictionary) belongs to Volta
// by the director's assignment. His note is expected at
// reports/mac-director/skip-acceptance-policy-volta-20261110.md, which is NOT in
// the tree at this revision: the path is a pointer to a pending artifact, not
// evidence. Until it lands, the names below are working names that Volta can
// rename; this file is an implementation and carries no policy of its own, and a
// divergence here is reported to Volta rather than decided here.
//
// What is measured (not theoretical): a runner that trusts the exit code cannot
// tell a green package with 10 skips from a measured one, and `go test` prints
// `--- PASS` for a parent whose every subtest skipped. internal/executor without
// the real environment returns rc=0 and prints 12 PASS while only 11 legs
// measured anything. Both count an unmeasured leg as coverage.
//
// Volta's rule was written for `go test -v` logs, where the boundary lines are
// the only attribution available. Reading `go test -json` is stronger: every leg
// is a structured event, so parent/child relations and skip reasons need no text
// parsing, and a stream without attributable leg events is reported red
// (LOG_UNVERIFIABLE) instead of green.
//
// Kinds (Volta's vocabulary, tolerant of the spellings actually in the tree):
//   SKIP_ENV[tag=..] reason=.. missing=..  prerequisite not supplied -> red at full env
//   SKIP_EXPLICIT[:][tag=..] ..            somebody opted out       -> yellow, never green
//   (nothing)                              untagged                  -> red always
// The tree carries `SKIP_EXPLICIT: real PostgreSQL required`
// (tests/execution/a2a_harness_test.go:225, internal/execbridge/bridge_db_test.go:42)
// with no tag, so a classifier that demands [tag=..] files these under the worst
// class; that is why the tag is optional for the explicit kind here.
//
// Secret hygiene: leg output is never echoed. Only structured fields leave this
// tool, so a verdict report cannot carry a credential the way a raw log tail can.
//
// Usage:
//   node scripts/skip-verdict.mjs [--env-complete] [--designation NAME]...
//                                 [--tags <file.json>] [--allow-unexplained <file>]
//                                 [--allow-partial] [--report <path>]
//                                 [--stdin | -- <command>...]
// Environment declaration (Volta): AHP_GATE_ENV=full declares a complete run;
// a partial run is reconnaissance and is reported as not acceptance-eligible.
//
// Exit (COND-1). An entry point whose rc somebody reads must not be able to
// report green for a run that measured nothing at acceptance level, so the exit
// code carries eligibility as well as the verdict: 0 = PASS and acceptance
// eligible, 1 = FAIL, 2 = usage/stream error, 3 = NOT_ACCEPTANCE (the legs are
// green but the run did not declare a complete environment). --allow-partial is
// the operator's way of buying the 0 while the report keeps saying reconnaissance.
// The verdict field itself keeps Volta's PASS/FAIL vocabulary; eligibility is a
// separate field, so no new verdict word enters the policy.

import { spawn } from 'node:child_process';
import fs from 'node:fs';
import process from 'node:process';

// No dictionary ships with the mechanism (COND-5). The tag set and the classes
// behind it are Volta's; a copy here would drift into a second policy that keeps
// answering green after the real one has changed. Consumers pass one with --tags,
// and a tag that is not in it is refused (UNKNOWN_SKIP_TAG), never guessed.
// readTagFile is the only way a class enters this file.
export const TAG_CLASS = {};

const ENV_TOKEN = /SKIP_ENV\[tag=([A-Za-z0-9_.-]+)\]/;
// optional tag, optional colon: SKIP_EXPLICIT: text / SKIP_EXPLICIT text / SKIP_EXPLICIT[tag=x]
const EXPLICIT_TOKEN = /SKIP_EXPLICIT\b(?:\[tag=([A-Za-z0-9_.-]+)\])?/;
const REASON = /(?:^|[\s:])reason=(\S+)/;
const MISSING = /(?:^|[\s:])missing=([^\s]+)/;
const TERMINAL = new Set(['pass', 'fail', 'skip']);
// COND-2: reason= and missing= are the only two places where text leaves a leg and
// lands in the acceptance report, so they are the leak channel. A reason is a label
// and missing= is a list of variable NAMES; anything else is either a malformed
// token or a value that has no business in a report, and is withheld rather than
// copied. The grammar is checked against the tokens the tree actually prints
// (internal/executor/realdeps_test.go:58, internal/execbridge/tool_wire_test.go:102,
// internal/model/artifact_plane_gate_test.go:37-60), so an honest lane is never reddened.
const REASON_LABEL = /^[a-z0-9][a-z0-9_.-]{0,63}$/;
const VARIABLE_NAME = /^[A-Z][A-Z0-9_]{0,99}$/;
const TOKEN_DEFECT_DETAIL = {
  reason_not_a_label: 'reason= must be a lowercase label (a-z0-9_.-), the token printed a value instead',
  missing_not_a_variable_name: 'missing= must be a comma-separated list of environment variable names',
};

function tokenDefect(reason, missingRaw) {
  if (reason !== '' && !REASON_LABEL.test(reason)) return 'reason_not_a_label';
  if (missingRaw !== '') {
    for (const item of missingRaw.split(',')) {
      if (!VARIABLE_NAME.test(item)) return 'missing_not_a_variable_name';
    }
  }
  return null;
}

export function parseGoTestJSON(text) {
  const tests = new Map();
  const order = [];
  const pkgs = new Map();
  const pkgOrder = [];
  let sawJsonEvent = false;

  for (const line of String(text).split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    let ev;
    try { ev = JSON.parse(trimmed); } catch { continue; }
    if (!ev || typeof ev.Action !== 'string') continue;
    sawJsonEvent = true;
    const action = ev.Action;
    const pkg = ev.Package ?? '(unknown)';
    if (action === 'build-output' || action === 'build-action') continue;
    if (!pkgs.has(pkg)) {
      pkgs.set(pkg, { package: pkg, action: null, output: '', noTestFiles: false });
      pkgOrder.push(pkg);
    }
    const pkgRec = pkgs.get(pkg);
    if (typeof ev.Test !== 'string' || ev.Test === '') {
      if (action === 'output') {
        pkgRec.output += ev.Output ?? '';
        if ((ev.Output ?? '').includes('[no test files]')) pkgRec.noTestFiles = true;
      } else if (TERMINAL.has(action) || action === 'build-fail') {
        pkgRec.action = action === 'build-fail' ? 'fail' : action;
      }
      continue;
    }
    const key = `${pkg}\u0000${ev.Test}`;
    if (!tests.has(key)) {
      tests.set(key, { package: pkg, name: ev.Test, action: null, output: '', leaf: true });
      order.push(key);
    }
    const rec = tests.get(key);
    if (action === 'output') rec.output += ev.Output ?? '';
    else if (TERMINAL.has(action)) rec.action = action;
  }

  const outcomes = order.map((k) => tests.get(k)).filter((r) => r.action !== null);
  for (const rec of outcomes) {
    rec.leaf = !outcomes.some((o) => o !== rec && o.package === rec.package
      && o.name.startsWith(`${rec.name}/`));
  }
  const packages = pkgOrder.map((p) => pkgs.get(p))
    .filter((p) => p.action !== null || p.output !== '')
    .map((p) => ({ ...p, action: p.action ?? 'fail' }));
  return { outcomes, packages, sawJsonEvent };
}

export function classifySkips(outcomes, cfg = {}) {
  const registry = cfg.tagClass ?? TAG_CLASS;
  const skips = [];
  const byClass = {};
  const byKind = { skip_env: 0, skip_explicit: 0, untagged: 0, malformed: 0 };
  for (const rec of outcomes) {
    if (rec.action !== 'skip') continue;
    const envHit = ENV_TOKEN.exec(rec.output);
    const explicitHit = envHit ? null : EXPLICIT_TOKEN.exec(rec.output);
    const reason = REASON.exec(rec.output)?.[1] ?? '';
    const missingRaw = MISSING.exec(rec.output)?.[1] ?? '';
    const missing = missingRaw ? missingRaw.split(',').filter(Boolean) : [];
    let entry;
    if (envHit) {
      const tag = envHit[1];
      const defect = tokenDefect(reason, missingRaw);
      if (defect) {
        // The value is dropped here, not redacted later: nothing downstream can
        // echo what was never copied into the record.
        entry = {
          package: rec.package, name: rec.name, kind: 'malformed', tag,
          reason: '', missing: [], class: 'malformed-token', defect,
          withheld: defect === 'reason_not_a_label' ? ['reason'] : ['missing'],
        };
      } else {
        entry = {
          package: rec.package, name: rec.name, kind: 'skip_env', tag, reason, missing,
          class: registry[tag] ?? `unknown:${tag}`,
        };
      }
    } else if (explicitHit) {
      entry = {
        package: rec.package, name: rec.name, kind: 'skip_explicit',
        tag: explicitHit[1] ?? null, reason, missing, class: 'explicit-opt-out',
      };
    } else {
      entry = {
        package: rec.package, name: rec.name, kind: 'untagged', tag: null,
        reason: '', missing: [], class: 'untagged',
      };
    }
    skips.push(entry);
    byKind[entry.kind] += 1;
    byClass[entry.class] = (byClass[entry.class] ?? 0) + 1;
  }
  return {
    skips, byClass, byKind,
    untagged: skips.filter((s) => s.kind === 'untagged'),
    env: skips.filter((s) => s.kind === 'skip_env'),
    explicit: skips.filter((s) => s.kind === 'skip_explicit'),
    malformed: skips.filter((s) => s.kind === 'malformed'),
  };
}

// Volta 3: a parent may not report PASS while every subtest under it skipped.
// The primary fix lives with the package owner (skip the parent itself when the
// prerequisite covers the whole test); this is the acceptance-side net that makes
// the mistake visible without trusting anybody's discipline.
export function findVacuousParents(outcomes) {
  const vacuous = [];
  for (const rec of outcomes) {
    if (rec.action !== 'pass') continue;
    const subtree = outcomes.filter((o) => o.package === rec.package
      && o.name.startsWith(`${rec.name}/`));
    if (subtree.length === 0) continue;
    const measured = subtree.filter((o) => !o.leaf || o.action !== 'skip').length;
    if (measured > 0) continue;
    vacuous.push({
      package: rec.package,
      parent: rec.name,
      children: subtree.filter((o) => !o.name.slice(rec.name.length + 1).includes('/')).length,
      skippedChildren: subtree.filter((o) => o.action === 'skip').length,
    });
  }
  return vacuous;
}

export function countLevels(outcomes) {
  const c = {
    passLeaf: 0, failLeaf: 0, skipLeaf: 0,
    passAll: 0, failAll: 0, skipAll: 0,
    passContainer: 0, skipContainer: 0,
  };
  for (const rec of outcomes) {
    if (rec.action === 'pass') c.passAll++;
    else if (rec.action === 'fail') c.failAll++;
    else if (rec.action === 'skip') c.skipAll++;
    if (!rec.leaf) {
      if (rec.action === 'pass') c.passContainer++;
      if (rec.action === 'skip') c.skipContainer++;
      continue;
    }
    if (rec.action === 'pass') c.passLeaf++;
    else if (rec.action === 'fail') c.failLeaf++;
    else if (rec.action === 'skip') c.skipLeaf++;
  }
  return c;
}

function exceptionFor(allow, rec) {
  if (!Array.isArray(allow)) return null;
  return allow.find((e) => e && e.package === rec.package && e.name === rec.name
    && typeof e.justification === 'string' && e.justification.trim() !== '') ?? null;
}

export function evaluatePolicy(state, cfg = {}) {
  const outcomes = state.outcomes ?? [];
  const packages = state.packages ?? [];
  const cls = state.skips ? state : classifySkips(outcomes, cfg);
  const counts = state.counts ?? countLevels(outcomes);
  const vacuous = state.vacuous ?? findVacuousParents(outcomes);
  const envComplete = cfg.envComplete === true || cfg.gateEnv === 'full';
  const designations = cfg.designations ?? [];
  const allow = cfg.allowUnexplained ?? [];
  // COND-4: the wrapped command's own exit code is evidence. A stream that says
  // PASS while the command ends nonzero is contradicted by its own process.
  const commandExit = cfg.commandExit ?? null;

  const violations = [];
  const rows = [];
  const packageFailures = packages.filter((p) => p.action === 'fail').length;
  const noTestFilesPackages = packages.filter((p) => p.noTestFiles).length;

  if (outcomes.length === 0 && packages.length === 0) {
    violations.push({
      rule: 'LOG_UNVERIFIABLE',
      detail: 'no leg could be attributed: this is not a `go test -json` stream, or it produced no test events; an unverifiable run is red, not green',
    });
  }

  if (counts.failLeaf > 0 || packageFailures > 0) {
    violations.push({
      rule: 'TEST_FAILURES',
      count: counts.failLeaf,
      packages_failed: packageFailures,
      names: outcomes.filter((o) => o.action === 'fail' && o.leaf).map((o) => o.name),
    });
  }

  for (const v of vacuous) {
    violations.push({
      rule: 'PARENT_PASS_WITH_ALL_CHILDREN_SKIPPED',
      package: v.package, name: v.parent,
      children: v.children, skipped_children: v.skippedChildren,
      detail: 'parent reported PASS while every subtest under it skipped: nothing was measured (Volta 3)',
    });
  }

  const malformed = cls.malformed ?? cls.skips.filter((s) => s.kind === 'malformed');
  const byDefect = new Map();
  for (const s of malformed) {
    const key = `${s.tag}\u0000${s.defect}`;
    if (!byDefect.has(key)) byDefect.set(key, []);
    byDefect.get(key).push(s.name);
  }
  for (const [key, names] of byDefect) {
    const [tag, defect] = key.split('\u0000');
    violations.push({
      rule: 'MALFORMED_SKIP_TOKEN', tag, defect, names,
      detail: TOKEN_DEFECT_DETAIL[defect] ?? 'the skip token does not match its grammar',
      remedy: 'the value is withheld from this report; fix the token in the leg so reason= is a label and missing= names variables',
    });
  }
  for (const s of malformed) rows.push({ ...s, count: 1 });

  const byUnknownTag = new Map();
  for (const s of cls.skips) {
    if (s.kind !== 'skip_env' || !s.class.startsWith('unknown:')) continue;
    if (!byUnknownTag.has(s.tag)) byUnknownTag.set(s.tag, []);
    byUnknownTag.get(s.tag).push(s.name);
  }
  for (const [tag, names] of byUnknownTag) {
    violations.push({
      rule: 'UNKNOWN_SKIP_TAG', tag, class: `unknown:${tag}`, names,
      detail: 'skip tag is not in the dictionary; Volta owns the dictionary, register it there',
    });
  }

  const declaredUncovered = [];
  const untagged = [];
  for (const s of cls.untagged) {
    const exc = exceptionFor(allow, s);
    if (exc) {
      declaredUncovered.push(s);
      rows.push({ ...s, kind: 'declared-uncovered', justification: exc.justification, count: 1 });
    } else {
      untagged.push(s);
      rows.push({ ...s, count: 1 });
    }
  }
  if (untagged.length > 0) {
    violations.push({
      rule: 'UNTAGGED_SKIP',
      skips: untagged.map((s) => ({ package: s.package, name: s.name })),
      detail: 'skipped without a stated kind: nobody can tell an absent prerequisite from an opt-out (Volta: worst of the three kinds)',
      remedy: untagged.map((s) => `t.Skip("SKIP_ENV[tag=<dictionary tag>] reason=<cause> missing=<VARS> -- a skip here means not-covered, not passed") in ${s.package} ${s.name}`).join('; '),
    });
  }

  for (const s of cls.env) rows.push({ ...s, count: 1 });
  if (envComplete && cls.env.length > 0) {
    violations.push({
      rule: 'SKIP_ENV_AT_FULL_ENV',
      count: cls.env.length,
      tags: [...new Set(cls.env.map((s) => s.tag))],
      classes: [...new Set(cls.env.map((s) => s.class))],
      missing: [...new Set(cls.env.flatMap((s) => s.missing))],
      detail: 'the run declared a complete environment, so these legs had no prerequisite to skip on (Volta 1)',
    });
  }

  if (envComplete && designations.length === 0) {
    violations.push({
      rule: 'MISSING_DESIGNATION_AT_FULL_ENV',
      detail: 'a run claiming a complete environment must enumerate which storage it was designated for; writing into undesignated storage is forbidden, so an unenumerated claim of fullness is unverifiable (Volta 1, DEF-LANE-DB-DESIGNATION)',
    });
  }

  // Yellow, never green: an explicit opt-out is not covered by somebody's choice.
  // It must reach the report and the not-closed number, without reddening a
  // package whose owner is not this lane.
  for (const s of cls.explicit) rows.push({ ...s, count: 1 });

  // A leg that printed an environment-skip token and then reported PASS is the
  // dangerous shape: the skip was removed and nothing was measured. A leg that
  // printed it and FAILED is loud already and is often honest by design: the
  // red-by-default lanes print the marker while explaining how to opt out
  // (tests/execution/a2a_harness_test.go:228, internal/execbridge/bridge_db_test.go:47),
  // so flagging FAIL legs here produced four false positives on the first real run
  // and is deliberately not done. Blind spot, stated: a neutered skip whose body
  // then fails is visible only as a failure, and a body that measures nothing while
  // printing nothing cannot be seen from a stream at all.
  // COND-3: an exception buys "declared not covered" for an untagged skip and
  // nothing else. It used to disarm this detector too, which turned the whitelist
  // into an amnesty for the one shape that is an outright lie: a leg that printed
  // an environment-skip token and then reported PASS. A whitelisted stub stays
  // red and its row stays in the report, so the record survives the exception.
  const stubbed = outcomes.filter((o) => o.action === 'pass' && o.output.includes('SKIP_ENV[tag='));
  const stubbedReal = stubbed;
  if (stubbedReal.length > 0) {
    violations.push({
      rule: 'STUBBED_SKIP_PASSED_AS_MEASURED',
      names: stubbedReal.map((o) => o.name),
      detail: 'a leg printed an environment-skip token and then reported PASS: the skip was neutered and nothing was measured',
    });
    for (const o of stubbedReal) {
      rows.push({ package: o.package, name: o.name, kind: 'stubbed-skip', class: 'stubbed-skip', tag: null, reason: '', missing: [], count: 1 });
    }
  }

  // COND-6: a package where every leg skipped is not a pass, however correctly each
  // individual skip is named. Correctness of the labels is not coverage.
  const perPackage = new Map();
  for (const o of outcomes) {
    if (!o.leaf) continue;
    const rec = perPackage.get(o.package)
      ?? { package: o.package, leaves: 0, skipped: 0, measured: 0 };
    rec.leaves += 1;
    if (o.action === 'skip') rec.skipped += 1; else rec.measured += 1;
    perPackage.set(o.package, rec);
  }
  const unmeasured = [...perPackage.values()].filter((p) => p.measured === 0);
  if (unmeasured.length > 0) {
    violations.push({
      rule: 'NOTHING_MEASURED',
      packages: unmeasured.map((p) => ({
        package: p.package, leaves: p.leaves, skipped: p.skipped, measured: p.measured,
      })),
      detail: 'every leg in this package skipped: the run measured nothing, so it cannot be acceptance even where the skips are all explained',
    });
  }

  if (typeof commandExit === 'number' && commandExit !== 0) {
    violations.push({
      rule: 'COMMAND_EXIT_NOT_ZERO',
      command_exit: commandExit,
      detail: 'the wrapped command exited nonzero: a green stream from a failing command is a contradiction and is reported red',
    });
  }

  for (const p of packages.filter((x) => x.noTestFiles)) {
    rows.push({ package: p.package, name: null, kind: 'no-test-files', class: 'no_test_files', tag: null, reason: 'no test files', missing: [], count: 0 });
  }

  const notClosed = cls.env.length + cls.explicit.length + untagged.length
    + declaredUncovered.length + malformed.length;
  return {
    command_exit: commandExit,
    verdict: violations.length > 0 ? 'FAIL' : 'PASS',
    env: envComplete ? 'full' : 'partial',
    acceptance_eligible: envComplete,
    designations,
    counters: {
      ...counts,
      noTestFilesPackages,
      packageFailures,
      testsTotal: outcomes.length,
      byKind: cls.byKind,
      byClass: cls.byClass,
      malformed: malformed.length,
      notClosed,
      declaredUncovered: declaredUncovered.length,
    },
    violations,
    rows,
  };
}

// COND-1: the exit code is derived from the verdict AND the eligibility, in that
// precedence: FAIL outranks everything, --allow-partial never launders a failure,
// and a green run that never declared a complete environment leaves the process
// with 3 rather than the 0 an rc reader would call a passed gate.
export function exitCodeFor(result, cfg = {}) {
  if (result.verdict === 'FAIL') return 1;
  if (result.acceptance_eligible === true) return 0;
  return cfg.allowPartial === true ? 0 : 3;
}

export function acceptanceMode(result, cfg = {}) {
  if (result.verdict === 'FAIL') return 'fail';
  if (result.acceptance_eligible === true) return 'acceptance';
  return cfg.allowPartial === true ? 'reconnaissance' : 'not_acceptance';
}

export function renderRows(result) {
  return (result.rows ?? []).map((r) => `SKIP_POLICY ${JSON.stringify(r)}`);
}

export function renderText(result) {
  const c = result.counters ?? {};
  const lines = [];
  lines.push(`verdict: ${result.verdict}`);
  lines.push(`env: ${result.env}`);
  lines.push(`acceptance_eligible: ${result.acceptance_eligible === true}`
    + (result.acceptance_eligible ? '' : '  (partial environment: reconnaissance, not acceptance)'));
  // COND-1: mode and exit_code are printed as fields, so a reader never has to
  // infer eligibility from prose, and never reads a bare rc=0 as "the gate passed".
  if (result.mode) lines.push(`mode: ${result.mode}`);
  if (result.exit_code !== undefined) lines.push(`exit_code: ${result.exit_code}`);
  if (result.designations?.length) lines.push(`designations: ${result.designations.join(',')}`);
  // COND-5: which dictionary was in force is part of the measurement.
  if (result.dictionary) lines.push(`dictionary: ${result.dictionary}`);
  // COND-4: the command's own exit code, whatever the stream says.
  if (result.command_exit !== null && result.command_exit !== undefined) {
    lines.push(`command_exit: ${result.command_exit}`);
  }
  lines.push(`counters: pass_leaf=${c.passLeaf ?? 0} fail_leaf=${c.failLeaf ?? 0} skip_leaf=${c.skipLeaf ?? 0}`
    + ` pass_container=${c.passContainer ?? 0} pass_all=${c.passAll ?? 0} fail_all=${c.failAll ?? 0}`
    + ` skip_all=${c.skipAll ?? 0} tests_total=${c.testsTotal ?? 0}`
    + ` no_test_files_packages=${c.noTestFilesPackages ?? 0} package_failures=${c.packageFailures ?? 0}`);
  lines.push(`skips: ${JSON.stringify(c.byKind ?? {})}`
    + ` by_class=${JSON.stringify(c.byClass ?? {})}`
    + ` not_closed=${c.notClosed ?? 0} declared_uncovered=${c.declaredUncovered ?? 0}`);
  lines.push(`violations: ${(result.violations ?? []).length}`);
  for (const v of result.violations ?? []) lines.push(`VIOLATION ${JSON.stringify(v)}`);
  lines.push(...renderRows(result));
  return lines.join('\n');
}

function parseArgv(argv) {
  const cfg = { envComplete: false, designations: [], tagsPath: null, allowPath: null, report: null, stdin: false, cmd: null, allowPartial: false };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--env-complete') cfg.envComplete = true;
    else if (a === '--allow-partial') cfg.allowPartial = true;
    else if (a === '--stdin') cfg.stdin = true;
    else if (a === '--designation') cfg.designations.push(argv[++i] ?? '');
    else if (a === '--tags') cfg.tagsPath = argv[++i];
    else if (a === '--allow-unexplained') cfg.allowPath = argv[++i];
    else if (a === '--report') cfg.report = argv[++i];
    else if (a === '--') { cfg.cmd = argv.slice(i + 1); break; }
    else return { error: `unknown argument: ${a}` };
  }
  if (cfg.designations.some((d) => d === '')) return { error: '--designation needs a name' };
  if (!cfg.stdin && !cfg.cmd) return { error: 'nothing to read: pass --stdin or -- <command>' };
  if (cfg.cmd && cfg.cmd.length === 0) return { error: 'empty command after --' };
  return { cfg };
}

export function readJsonArrayFile(path, what) {
  if (!path) return [];
  const parsed = path === '-' ? [] : JSON.parse(fs.readFileSync(path, 'utf8'));
  if (!Array.isArray(parsed)) throw new Error(`${what} must contain a JSON array`);
  return parsed;
}

export function readAllowFile(path) {
  const entries = readJsonArrayFile(path, '--allow-unexplained file');
  for (const e of entries) {
    if (!e || typeof e.package !== 'string' || typeof e.name !== 'string'
      || typeof e.justification !== 'string' || e.justification.trim() === '') {
      throw new Error('every exception needs package, name and justification');
    }
  }
  return entries;
}

export function readTagFile(path) {
  const entries = readJsonArrayFile(path, '--tags file');
  const map = {};
  for (const e of entries) {
    if (!e || typeof e.tag !== 'string' || typeof e.class !== 'string') {
      throw new Error('every tag entry needs tag and class');
    }
    map[e.tag] = e.class;
  }
  return map;
}

function runCommand(cmd) {
  return new Promise((resolve, reject) => {
    const [bin, ...args] = cmd;
    const child = spawn(bin, args, { stdio: ['ignore', 'pipe', 'pipe'] });
    let out = '';
    let errBytes = 0;
    child.stdout.on('data', (d) => { out += d; });
    child.stderr.on('data', (d) => { errBytes += d.length; });
    child.on('error', reject);
    child.on('close', (code) => resolve({ out, errBytes, code: code ?? 1 }));
  });
}

async function readStdin() {
  const chunks = [];
  for await (const chunk of process.stdin) chunks.push(chunk);
  return Buffer.concat(chunks).toString('utf8');
}

const isMain = process.argv[1] && process.argv[1].endsWith('skip-verdict.mjs');
if (isMain) {
  const parsed = parseArgv(process.argv.slice(2));
  if (parsed.error) {
    process.stderr.write(`skip-verdict: ${parsed.error}\n`);
    process.stderr.write('usage: node scripts/skip-verdict.mjs [--env-complete] [--designation NAME]... [--tags <file>] [--allow-unexplained <file>] [--allow-partial] [--report <path>] [--stdin | -- <command>...]\n');
    process.exit(2);
  }
  const cfg = parsed.cfg;
  let stream = '';
  let cmdCode = null;
  try {
    if (cfg.stdin) stream = await readStdin();
    else {
      const run = await runCommand(cfg.cmd);
      stream = run.out;
      cmdCode = run.code;
      if (run.errBytes) process.stderr.write(`skip-verdict: the command wrote ${run.errBytes} bytes on stderr (not echoed)\n`);
    }
  } catch (e) {
    process.stderr.write(`skip-verdict: ${e.message}\n`);
    process.exit(2);
  }

  let allow = [];
  let tags = TAG_CLASS;
  let dictionary = 'absent';
  try {
    allow = readAllowFile(cfg.allowPath);
    if (cfg.tagsPath) {
      tags = readTagFile(cfg.tagsPath);
      dictionary = `${cfg.tagsPath} entries=${Object.keys(tags).length}`;
    }
  } catch (e) {
    process.stderr.write(`skip-verdict: ${e.message}\n`);
    process.exit(2);
  }
  const designations = [...cfg.designations,
    ...(process.env.AHP_GATE_DESIGNATION ?? '').split(',').map((s) => s.trim()).filter(Boolean)];

  const { outcomes, packages } = parseGoTestJSON(stream);
  const cls = classifySkips(outcomes, { tagClass: tags });
  const state = {
    outcomes, packages, skips: cls.skips, byClass: cls.byClass, byKind: cls.byKind,
    untagged: cls.untagged, env: cls.env, explicit: cls.explicit,
    vacuous: findVacuousParents(outcomes), counts: countLevels(outcomes),
  };
  const result = evaluatePolicy(state, {
    envComplete: cfg.envComplete,
    gateEnv: process.env.AHP_GATE_ENV,
    designations,
    allowUnexplained: allow,
    tagClass: tags,
    commandExit: cmdCode,
  });
  const allowPartial = cfg.allowPartial === true;
  result.exit_code = exitCodeFor(result, { allowPartial });
  result.mode = acceptanceMode(result, { allowPartial });
  result.dictionary = dictionary;
  const text = renderText(result);
  process.stdout.write(`${text}\n`);
  if (cfg.report) fs.writeFileSync(cfg.report, `${text}\n`);
  process.exit(result.exit_code);
}
