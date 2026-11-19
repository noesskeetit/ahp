// Russell / DEF-GATE-SKIP-POLICY: unit test for the skip-aware acceptance verdict
// mechanism. SYNTHETIC `go test -json` streams only (no Go, no database, no
// docker), asserting the mechanism cannot count an unmeasured leg as a pass. The
// vocabulary (kinds, acceptance code names, red/yellow split) is Volta's (his
// note is not in the tree yet, see the header of skip-verdict.mjs); the token and
// marker texts below are the ones actually printed by the tree
// (internal/executor/realdeps_test.go:35-37, tests/execution/a2a_harness_test.go:224-225,
// internal/execbridge/bridge_db_test.go:41-42).
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import {
  parseGoTestJSON,
  classifySkips,
  findVacuousParents,
  countLevels,
  evaluatePolicy,
  renderText,
  renderRows,
  readAllowFile,
  readTagFile,
  exitCodeFor,
} from './skip-verdict.mjs';

const PKG = 'github.com/ahp-platform/ahp/internal/executor';
// COND-5: the tool holds no dictionary of its own (Volta owns the form and the tag set), so
// every consumer passes one. This is that fixture; the tool must refuse tags it is not given.
const DICT = { real_deps: 'real_store_env', artifact_plane: 'real_store_env', artifact_parser: 'trusted_parser' };
const TOK_REAL = 'SKIP_ENV[tag=real_deps] reason=real_store_env_absent missing=AHP_TEST_DATABASE_URL,AHP_S3_ENDPOINT -- these legs measure the real artifact plane: a skip here means not-covered, not passed';
const TOK_PARSER = 'SKIP_ENV[tag=artifact_parser] reason=trusted_parser_absent missing=AHP_ARTIFACT_PARSER -- cmd/artifact-parser refuses to run outside Linux by design; a skip here means not-covered, not passed';
// both spellings really used in the tree, neither carries [tag=...]
const EXPL_SKIP_CALL = 'SKIP_EXPLICIT: real PostgreSQL required for the A2A journey';
const EXPL_LOG = 'SKIP_EXPLICIT AHP_LIVE_QWEN!=1 -- opt-out skip, NOT counted as acceptance';
const CANARY = 'CANARY-DO-NOT-LEAK-4f19';

function jl(events) {
  return events.map((e) => JSON.stringify({ Time: '2026-11-10T05:00:00.000Z', ...e })).join('\n') + '\n';
}
function leg(name, action, outputs = []) {
  const events = [{ Action: 'run', Package: PKG, Test: name }];
  for (const Output of outputs) events.push({ Action: 'output', Package: PKG, Test: name, Output });
  events.push({ Action: action, Package: PKG, Test: name, Elapsed: 0.01 });
  return events;
}
function skippedWithToken(name, token) {
  return leg(name, 'skip', [`    x_test.go:59: ${token}\n`, `=== RUN   ${name}\n`, `    x_test.go:61: ${token}\n`]);
}
function silentSkip(name) {
  return leg(name, 'skip', [`    something_test.go:12: ${name}: not applicable here\n`]);
}

function evaluateState(text, cfg = {}) {
  const { outcomes, packages } = parseGoTestJSON(text);
  const cls = classifySkips(outcomes, { tagClass: DICT, ...cfg });
  return {
    outcomes, packages, skips: cls.skips, byClass: cls.byClass, byKind: cls.byKind,
    untagged: cls.untagged, env: cls.env, explicit: cls.explicit,
    vacuous: findVacuousParents(outcomes), counts: countLevels(outcomes),
  };
}
const FULL = { envComplete: true, designations: ['pg:review_r_gate', 'store:review-r-gate'] };

const CARGO = jl([
  { Action: 'start', Package: PKG },
  ...leg('TestMeasuredPasses', 'pass', ['--- PASS: TestMeasuredPasses (0.01s)\n']),
  ...leg('TestMeasuredFails', 'fail', ['    x_test.go:9: boom\n', '--- FAIL: TestMeasuredFails (0.01s)\n']),
  ...skippedWithToken('TestNeedsRealStore', TOK_REAL),
  ...skippedWithToken('TestNeedsTrustedParser', TOK_PARSER),
  ...silentSkip('TestSilentlySkipped'),
  { Action: 'output', Package: PKG, Output: 'FAIL\n' },
  { Action: 'fail', Package: PKG },
]);

test('parse: keeps every test-level outcome and marks containers', () => {
  const { outcomes } = parseGoTestJSON(CARGO);
  assert.deepEqual(outcomes.map((o) => o.name).sort(), [
    'TestMeasuredFails', 'TestMeasuredPasses', 'TestNeedsRealStore',
    'TestNeedsTrustedParser', 'TestSilentlySkipped',
  ]);
  assert.ok(outcomes.every((o) => o.leaf === true), 'no child names here, all legs are leaves');
});

test('parse: last terminal action wins, output accumulates', () => {
  const { outcomes } = parseGoTestJSON(jl([
    ...leg('TestDup', 'pass', ['a\n']),
    { Action: 'output', Package: PKG, Test: 'TestDup', Output: 'b\n' },
    { Action: 'fail', Package: PKG, Test: 'TestDup' },
  ]));
  assert.equal(outcomes.length, 1, 'one record per (package,test), not one per event');
  assert.equal(outcomes[0].action, 'fail');
  assert.equal(outcomes[0].output, 'a\nb\n');
});

test('parse: subtests make the parent a container, package events stay separate', () => {
  const { outcomes, packages } = parseGoTestJSON(jl([
    { Action: 'start', Package: PKG },
    ...leg('TestParent', 'pass', []),
    ...leg('TestParent/first', 'skip', [`    x.go:1: ${TOK_REAL}\n`]),
    ...leg('TestParent/second', 'skip', [`    x.go:1: ${TOK_REAL}\n`]),
    { Action: 'output', Package: PKG, Output: 'ok  \tpkg\t0.1s\n' },
    { Action: 'pass', Package: PKG },
  ]));
  assert.equal(outcomes.find((o) => o.name === 'TestParent').leaf, false, 'a parent is not a measured leaf');
  assert.equal(outcomes.filter((o) => o.leaf).length, 2);
  assert.equal(packages.length, 1);
  assert.equal(packages[0].action, 'pass');
});

test('parse: "[no test files]" is its own class, never a measured pass', () => {
  const { outcomes, packages } = parseGoTestJSON(jl([
    { Action: 'output', Package: 'pkg/empty', Output: '?   \tpkg/empty\t[no test files]\n' },
    { Action: 'skip', Package: 'pkg/empty' },
  ]));
  assert.equal(outcomes.length, 0);
  assert.equal(packages[0].noTestFiles, true);
});

test('parse: a package that did not build is never an acceptance pass', () => {
  const res = evaluatePolicy(evaluateState(jl([
    { Action: 'start', Package: PKG },
    { Action: 'output', Package: PKG, Output: 'build error: undefined: nope\n' },
    { Action: 'fail', Package: PKG },
  ])), { tagClass: DICT });
  assert.equal(res.verdict, 'FAIL');
  assert.ok(res.violations.some((v) => v.rule === 'TEST_FAILURES'));
});

test('classify: env token names tag, class and missing variables once per test', () => {
  const { outcomes } = parseGoTestJSON(CARGO);
  const { skips, byClass, byKind } = classifySkips(outcomes, { tagClass: DICT });
  const real = skips.find((s) => s.name === 'TestNeedsRealStore');
  assert.equal(real.kind, 'skip_env');
  assert.equal(real.class, 'real_store_env');
  assert.equal(real.tag, 'real_deps');
  assert.equal(real.reason, 'real_store_env_absent');
  assert.deepEqual(real.missing, ['AHP_TEST_DATABASE_URL', 'AHP_S3_ENDPOINT']);
  assert.equal(skips.filter((s) => s.name === 'TestNeedsRealStore').length, 1, 'token printed twice counts once');
  assert.equal(byClass.real_store_env, 1);
  assert.deepEqual(byKind, { skip_env: 2, skip_explicit: 0, untagged: 1, malformed: 0 });
});

test('classify: the two spellings of one environment class normalize together', () => {
  const { outcomes } = parseGoTestJSON(jl([
    ...skippedWithToken('TestRealDepsSpelling', TOK_REAL),
    ...skippedWithToken('TestArtifactPlaneSpelling', 'SKIP_ENV[tag=artifact_plane] reason=real_store_env_absent missing=AHP_S3_ENDPOINT -- a skip here means not-covered, not passed'),
  ]));
  assert.equal(classifySkips(outcomes, { tagClass: DICT }).byClass.real_store_env, 2);
});

test('classify: SKIP_EXPLICIT without a tag is the explicit kind, not the worst class', () => {
  const { outcomes } = parseGoTestJSON(jl([
    ...leg('TestA2AJourney', 'skip', [`    a2a_harness_test.go:225: ${EXPL_SKIP_CALL}\n`]),
    ...leg('TestLiveQwen', 'skip', [`    a2a_harness_test.go:273: ${EXPL_LOG}\n`]),
  ]));
  const { skips, byKind } = classifySkips(outcomes, { tagClass: DICT });
  assert.deepEqual(byKind, { skip_env: 0, skip_explicit: 2, untagged: 0, malformed: 0 },
    'a classifier that demands [tag=..] files these as untagged and reddens half the tree on spelling');
  assert.equal(skips.find((s) => s.name === 'TestA2AJourney').class, 'explicit-opt-out');
  assert.equal(skips.find((s) => s.name === 'TestA2AJourney').tag, null);
});

test('classify: a tagged explicit skip keeps its tag', () => {
  const { outcomes } = parseGoTestJSON(jl([
    ...skippedWithToken('TestTaggedExplicit', 'SKIP_EXPLICIT[tag=live_provider] reason=live_opt_out missing=AHP_LIVE_QWEN -- opt-out'),
  ]));
  const { skips } = classifySkips(outcomes, { tagClass: DICT });
  assert.equal(skips[0].kind, 'skip_explicit');
  assert.equal(skips[0].tag, 'live_provider');
});

test('classify: an unregistered env tag is reported as unknown, not silently binned', () => {
  const { outcomes } = parseGoTestJSON(jl([
    ...skippedWithToken('TestMystery', 'SKIP_ENV[tag=somewhat_new] reason=whatever missing=X -- a skip here means not-covered, not passed'),
  ]));
  const { skips } = classifySkips(outcomes, { tagClass: DICT });
  assert.match(skips[0].class, /^unknown/);
  assert.equal(skips[0].tag, 'somewhat_new');
});

test('classify: a bare t.Skip is untagged, the kind acceptance must never trust', () => {
  const { outcomes } = parseGoTestJSON(jl([...silentSkip('TestSilentlySkipped')]));
  assert.deepEqual(classifySkips(outcomes, { tagClass: DICT }).byKind, { skip_env: 0, skip_explicit: 0, untagged: 1, malformed: 0 });
});

test('countLevels: honest counters do not inflate with containers', () => {
  const { outcomes } = parseGoTestJSON(jl([
    ...leg('TestP', 'pass', []),
    ...leg('TestP/a', 'skip', [`    x.go:1: ${TOK_REAL}\n`]),
    ...leg('TestP/b', 'skip', [`    x.go:1: ${TOK_REAL}\n`]),
    ...leg('TestQ', 'pass', []),
  ]));
  const c = countLevels(outcomes);
  assert.equal(c.passLeaf, 1, 'only TestQ measured something');
  assert.equal(c.skipLeaf, 2);
  assert.equal(c.passContainer, 1, 'TestP printed PASS without measuring anything');
  assert.equal(c.passAll, 2, 'the naive count is exactly what the wide gate reports today');
});

test('findVacuousParents: PASS parent whose every child skipped', () => {
  const { outcomes } = parseGoTestJSON(jl([
    ...leg('TestV', 'pass', ['--- PASS: TestV (0.00s)\n']),
    ...leg('TestV/a', 'skip', [`    x.go:1: ${TOK_REAL}\n`]),
    ...leg('TestV/b', 'skip', [`    x.go:1: ${TOK_REAL}\n`]),
    ...leg('TestH', 'pass', []),
    ...leg('TestH/a', 'pass', []),
  ]));
  const vac = findVacuousParents(outcomes);
  assert.deepEqual(vac.map((v) => v.parent), ['TestV']);
  assert.equal(vac[0].children, 2);
  assert.equal(vac[0].skippedChildren, 2);
});

const CARGO_ENV_SKIPS = jl([
  { Action: 'start', Package: PKG },
  ...leg('TestMeasuredPasses', 'pass', []),
  ...skippedWithToken('TestNeedsRealStore', TOK_REAL),
  ...skippedWithToken('TestNeedsTrustedParser', TOK_PARSER),
  { Action: 'output', Package: PKG, Output: 'ok  \tpkg\t0.1s\n' },
  { Action: 'pass', Package: PKG },
]);

test('invariant 1: declared full environment plus any SKIP_ENV fails acceptance and names the tags', () => {
  const res = evaluatePolicy(evaluateState(CARGO_ENV_SKIPS), { ...FULL, tagClass: DICT });
  assert.equal(res.verdict, 'FAIL');
  const v = res.violations.find((x) => x.rule === 'SKIP_ENV_AT_FULL_ENV');
  assert.ok(v, 'the rule must be named, not merely reflected in an exit code');
  assert.deepEqual([...v.tags].sort(), ['artifact_parser', 'real_deps']);
  assert.ok(v.classes.includes('real_store_env') && v.classes.includes('trusted_parser'));
});

test('a partial run is reconnaissance: env skips are not red, but they are not passes either', () => {
  const res = evaluatePolicy(evaluateState(CARGO_ENV_SKIPS), { tagClass: DICT });
  assert.equal(res.verdict, 'PASS');
  assert.equal(res.acceptance_eligible, false);
  assert.equal(res.env, 'partial');
  assert.equal(res.counters.passLeaf, 1, 'the two skips never join the pass counter');
  assert.equal(res.counters.notClosed, 2, 'they are reported as not covered');
  assert.equal(exitCodeFor(res), 3, 'COND-1: reconnaissance must not exit 0 for an rc reader');
  assert.equal(exitCodeFor(res, { allowPartial: true }), 0, 'only an explicit --allow-partial buys the 0');
});

test('AHP_GATE_ENV=full declares completeness the same way the flag does', () => {
  const res = evaluatePolicy(evaluateState(CARGO_ENV_SKIPS), { gateEnv: 'full', designations: ['pg:x'], tagClass: DICT });
  assert.equal(res.env, 'full');
  assert.equal(res.verdict, 'FAIL');
  assert.ok(res.violations.some((v) => v.rule === 'SKIP_ENV_AT_FULL_ENV'));
});

const CARGO_VACUOUS = jl([
  { Action: 'start', Package: PKG },
  ...leg('TestSealReplayDoesNotCreateASecondObject', 'pass', ['--- PASS: TestSealReplayDoesNotCreateASecondObject (0.00s)\n']),
  ...leg('TestSealReplayDoesNotCreateASecondObject/rewrites-reference', 'skip', [`    x.go:1: ${TOK_REAL}\n`]),
  ...leg('TestSealReplayDoesNotCreateASecondObject/second-put', 'skip', [`    x.go:1: ${TOK_REAL}\n`]),
  { Action: 'output', Package: PKG, Output: 'ok  \tpkg\t0.1s\n' },
  { Action: 'pass', Package: PKG },
]);

test('invariant 2: a parent may not PASS while every subtest skipped, in either environment', () => {
  for (const cfg of [{ tagClass: DICT }, { ...FULL, tagClass: DICT }]) {
    const res = evaluatePolicy(evaluateState(CARGO_VACUOUS), cfg);
    assert.equal(res.verdict, 'FAIL');
    assert.ok(res.violations.some((v) => v.rule === 'PARENT_PASS_WITH_ALL_CHILDREN_SKIPPED'));
  }
});

test('an untagged skip is red in either environment and prints the missing declaration', () => {
  const res = evaluatePolicy(evaluateState(CARGO_SILENT), { tagClass: DICT });
  assert.equal(res.verdict, 'FAIL');
  const v = res.violations.find((x) => x.rule === 'UNTAGGED_SKIP');
  assert.deepEqual(v.skips.map((s) => s.name), ['TestSilentlySkipped']);
  assert.match(v.remedy, /SKIP_ENV\[tag=/);
});

const CARGO_SILENT = jl([...leg('TestWhatever', 'pass', []), ...silentSkip('TestSilentlySkipped')]);

test('a declared exception tolerates an untagged skip and still reports it as not covered', () => {
  const allow = [{ package: PKG, name: 'TestSilentlySkipped', justification: 'RFC-0000: pinned separately' }];
  const res = evaluatePolicy(evaluateState(CARGO_SILENT), { allowUnexplained: allow });
  assert.equal(res.verdict, 'PASS');
  const row = res.rows.find((r) => r.name === 'TestSilentlySkipped');
  assert.equal(row.kind, 'declared-uncovered');
  assert.equal(res.counters.notClosed, 1, 'tolerated is not the same as measured');
});

test('an exception without justification does not clear anything, in policy and in the reader', () => {
  const res = evaluatePolicy(evaluateState(CARGO_SILENT), {
    allowUnexplained: [{ package: PKG, name: 'TestSilentlySkipped' }], tagClass: DICT,
  });
  assert.equal(res.verdict, 'FAIL');
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'skipverdict-allow-'));
  try {
    const bad = path.join(dir, 'bad.json');
    fs.writeFileSync(bad, JSON.stringify([{ package: PKG, name: 'TestSilentlySkipped' }]));
    assert.throws(() => readAllowFile(bad), /justification/);
    const good = path.join(dir, 'good.json');
    fs.writeFileSync(good, JSON.stringify([{ package: PKG, name: 'TestSilentlySkipped', justification: 'RFC-0000' }]));
    assert.equal(readAllowFile(good).length, 1);
    assert.deepEqual(readAllowFile(null), []);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('a stubbed skip that printed the token and then reached PASS is red', () => {
  const res = evaluatePolicy(evaluateState(jl([
    ...leg('TestSaysSkipAndPasses', 'pass', [`    executor_test.go:59: ${TOK_REAL}\n`, '--- PASS: TestSaysSkipAndPasses (0.00s)\n']),
  ])), { tagClass: DICT });
  assert.equal(res.verdict, 'FAIL');
  assert.deepEqual(res.violations.find((x) => x.rule === 'STUBBED_SKIP_PASSED_AS_MEASURED').names,
    ['TestSaysSkipAndPasses']);
});

test('a red-by-default leg that names SKIP_EXPLICIT in its own failure text is not a stubbed skip', () => {
  // the shape actually in the tree: the opt-out marker is printed while the leg
  // stays red (tests/execution/a2a_harness_test.go:224-228, internal/execbridge/bridge_db_test.go:41-47)
  const stream = jl([
    { Action: 'start', Package: PKG },
    ...leg('TestRedByDefault', 'fail', [
      `    bridge_db_test.go:41: ${EXPL_LOG}\n`,
      '    bridge_db_test.go:47: real PostgreSQL required; set AHP_ALLOW_SKIP_REAL_DB=1 only to downgrade this to a SKIP_EXPLICIT skip\n',
      '--- FAIL: TestRedByDefault (0.01s)\n',
    ]),
    ...leg('TestGuidanceOnPass', 'pass', [`    x_test.go:3: SKIP_EXPLICIT is the marker acceptance ignores\n`]),
    { Action: 'output', Package: PKG, Output: 'FAIL\n' },
    { Action: 'fail', Package: PKG },
  ]);
  const res = evaluatePolicy(evaluateState(stream), { tagClass: DICT });
  assert.ok(res.violations.some((v) => v.rule === 'TEST_FAILURES'));
  assert.ok(!res.violations.some((v) => v.rule === 'STUBBED_SKIP_PASSED_AS_MEASURED'),
    'a failing leg that only mentions the marker is red-by-default, not a lie');
});

test('an unregistered tag is red so the dictionary has to be updated by its owner', () => {
  const res = evaluatePolicy(evaluateState(jl([
    ...leg('TestWhatever', 'pass', []),
    ...skippedWithToken('TestMystery', 'SKIP_ENV[tag=somewhat_new] reason=whatever missing=X -- not covered'),
  ])), { tagClass: DICT });
  assert.equal(res.verdict, 'FAIL');
  assert.equal(res.violations.find((x) => x.rule === 'UNKNOWN_SKIP_TAG').tag, 'somewhat_new');
});

test('the dictionary is injectable, so Volta owns one copy of it, not four', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'skipverdict-tags-'));
  try {
    const f = path.join(dir, 'tags.json');
    fs.writeFileSync(f, JSON.stringify([{ tag: 'somewhat_new', class: 'real_store_env' }]));
    const tags = readTagFile(f);
    assert.deepEqual(tags, { somewhat_new: 'real_store_env' });
    const state = evaluateState(jl([
      ...skippedWithToken('TestMystery', 'SKIP_ENV[tag=somewhat_new] reason=whatever missing=X -- not covered'),
    ]), { tagClass: tags });
    const res = evaluatePolicy(state, { tagClass: tags });
    assert.ok(!res.violations.some((v) => v.rule === 'UNKNOWN_SKIP_TAG'), 'registered by the dictionary owner, so not red');
    assert.equal(res.counters.byClass.real_store_env, 1);
    assert.throws(() => readTagFile(jsonWrite(dir, 'bad.json', [{ tag: 'x' }])), /tag and class/);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
function jsonWrite(dir, name, value) {
  const f = path.join(dir, name);
  fs.writeFileSync(f, JSON.stringify(value));
  return f;
}

test('invariant: explicit opt-out is yellow, never green, and never reddens another lane', () => {
  const state = evaluateState(jl([
    ...leg('TestWhatever', 'pass', []),
    ...leg('TestA2AJourney', 'skip', [`    a2a_harness_test.go:225: ${EXPL_SKIP_CALL}\n`]),
  ]));
  for (const cfg of [{ tagClass: DICT }, { ...FULL, tagClass: DICT }]) {
    const res = evaluatePolicy(state, cfg);
    assert.equal(res.verdict, 'PASS', `explicit must not redden (cfg env=${res.env})`);
    assert.equal(res.counters.notClosed, 1, 'but it lands in the not-closed number');
    assert.equal(res.rows.find((r) => r.name === 'TestA2AJourney').kind, 'skip_explicit');
  }
});

test('a full-env run must enumerate its designation', () => {
  const clean = jl([...leg('TestA', 'pass', []), ...leg('TestB', 'pass', [])]);
  const bare = evaluatePolicy(evaluateState(clean), { envComplete: true, tagClass: DICT });
  assert.equal(bare.verdict, 'FAIL');
  assert.ok(bare.violations.some((v) => v.rule === 'MISSING_DESIGNATION_AT_FULL_ENV'));
  const declared = evaluatePolicy(evaluateState(clean), { envComplete: true, designations: ['pg:review_r_gate'], tagClass: DICT });
  assert.equal(declared.verdict, 'PASS');
  assert.deepEqual(declared.designations, ['pg:review_r_gate']);
});

test('a run whose legs cannot be attributed is unverifiable, not green', () => {
  const res = evaluatePolicy(evaluateState(''), { tagClass: DICT });
  assert.equal(res.verdict, 'FAIL');
  assert.ok(res.violations.some((v) => v.rule === 'LOG_UNVERIFIABLE'));
  const text = evaluatePolicy(evaluateState('=== RUN   TestA\n--- PASS: TestA\n'), { tagClass: DICT });
  assert.ok(text.violations.some((v) => v.rule === 'LOG_UNVERIFIABLE'), 'a -v text log is not an attributable stream here');
});

test('a fully measured green run passes and says so by field', () => {
  const res = evaluatePolicy(evaluateState(jl([
    ...leg('TestA', 'pass', []), ...leg('TestB', 'pass', []), ...leg('TestC', 'pass', []),
  ])), { ...FULL, tagClass: DICT });
  assert.equal(res.verdict, 'PASS');
  assert.deepEqual(res.violations, []);
  assert.equal(res.acceptance_eligible, true);
  assert.equal(res.env, 'full');
  assert.equal(res.counters.passLeaf, 3);
  assert.equal(res.counters.notClosed, 0);
});

test('a failed leg fails acceptance whatever the skips say', () => {
  const res = evaluatePolicy(evaluateState(CARGO), { tagClass: DICT });
  assert.equal(res.verdict, 'FAIL');
  assert.ok(res.violations.some((v) => v.rule === 'TEST_FAILURES'));
});

test('invariant: no credential value leaves the report, only structured fields do', () => {
  const stream = jl([
    ...leg('TestFailsWithCredentialInOutput', 'fail', [
      `    client.go:88: dial failed for AHP_MINIO_ROOT_USER=${CANARY} secret=${CANARY}\n`,
      '--- FAIL: TestFailsWithCredentialInOutput (0.01s)\n',
    ]),
    ...leg('TestSkipsWithCredentialInOutput', 'skip', [
      `    client.go:99: ${TOK_REAL} password=${CANARY}\n`,
    ]),
    { Action: 'output', Package: PKG, Output: `FAIL\tsession=${CANARY}\n` },
    { Action: 'fail', Package: PKG },
  ]);
  const res = evaluatePolicy(evaluateState(stream), { ...FULL, tagClass: DICT });
  assert.equal(res.verdict, 'FAIL');
  const rendered = `${renderText(res)}\n${renderRows(res).join('\n')}`;
  assert.ok(!rendered.includes(CANARY), 'the verdict report must never carry a credential value from leg output');
  assert.ok(rendered.includes('TestFailsWithCredentialInOutput'), 'names travel, values do not');
});

test('wiring: the mechanism is reachable from a tracked entry point, or acceptance measures nothing', () => {
  // scripts/run-tests.mjs:22-27 walks only tests/<category>/*.mjs, so a test under
  // scripts/ is invisible to every gate unless an npm script names it. Deleting the
  // wiring must redden a leg, otherwise the mechanism is delivered but never run.
  const pkg = JSON.parse(fs.readFileSync(new URL('../package.json', import.meta.url), 'utf8'));
  const verify = pkg.scripts?.['verify:skip-verdict'] ?? '';
  assert.match(verify, /node --test/, 'verify entry must run this file with the node test runner');
  assert.match(verify, /scripts\/skip-verdict\.test\.mjs/, 'this very file must be named');
  const gate = pkg.scripts?.['test:go:verdict'] ?? '';
  assert.match(gate, /scripts\/skip-verdict\.mjs/, 'the gate must route the stream through the verdict');
  assert.match(gate, /go test -json/, 'the gate must use -json, the only attributable form here');
  assert.ok(!/--allow-partial/.test(gate),
    'COND-1: the acceptance entry must not disarm its own eligibility check; reconnaissance is an operator decision made at call time');
});

test('rows are machine-readable with the fields the wide gate aggregates', () => {
  const res = evaluatePolicy(evaluateState(CARGO), { tagClass: DICT });
  const rows = renderRows(res);
  assert.ok(rows.length >= 3);
  for (const line of rows) {
    assert.match(line, /^SKIP_POLICY \{/);
    const r = JSON.parse(line.slice('SKIP_POLICY '.length));
    for (const key of ['package', 'kind', 'class', 'tag', 'reason', 'missing', 'count']) {
      assert.ok(Object.hasOwn(r, key), `row must carry ${key}`);
    }
  }
});

test('text report states verdict, env and eligibility as fields, not as an exit code', () => {
  const text = renderText(evaluatePolicy(evaluateState(CARGO), FULL));
  assert.match(text, /^verdict: FAIL$/m);
  assert.match(text, /^env: full$/m);
  assert.match(text, /^acceptance_eligible: true$/m);
  assert.match(text, /violations: [1-9]/);
  assert.match(text, /SKIP_POLICY /);
  const partial = renderText(evaluatePolicy(evaluateState(CARGO), {}));
  assert.match(partial, /^env: partial$/m);
  assert.match(partial, /reconnaissance, not acceptance/);
});

test('no test files is reported and never inflates the pass counter', () => {
  const res = evaluatePolicy(evaluateState(jl([
    { Action: 'output', Package: 'pkg/empty', Output: '?   \tpkg/empty\t[no test files]\n' },
    { Action: 'skip', Package: 'pkg/empty' },
  ])), { tagClass: DICT });
  assert.equal(res.counters.passLeaf, 0);
  assert.equal(res.counters.noTestFilesPackages, 1);
});

// ---------------------------------------------------------------------------
// Conditions from the director after the first merge (COND-1..COND-6).
// Each of these is a shape that made the merged mechanism decorative: an rc
// reader seeing green where nothing was measured, a leak channel in the token
// extractor, a whitelist that disarmed the lie detector, an ignored command
// exit, a package of pure skips called PASS, and a dictionary the tool kept a
// private copy of.
// ---------------------------------------------------------------------------

// fileURLToPath, not URL.pathname: the repository lives under a path with spaces and
// pathname percent-encodes them, which the child process cannot open.
const CLI = fileURLToPath(new URL('./skip-verdict.mjs', import.meta.url));
// The CLI legs must not inherit the ambient environment: AHP_GATE_ENV /
// AHP_GATE_DESIGNATION in the caller's shell would silently change what the leg proves.
const CLEAN_ENV = { PATH: process.env.PATH };
function cli(args, input = '') {
  return spawnSync(process.execPath, [CLI, ...args], { input, env: CLEAN_ENV, encoding: 'utf8' });
}
function withTemp(fn) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'skipverdict-cli-'));
  try { return fn(dir); } finally { fs.rmSync(dir, { recursive: true, force: true }); }
}

test('COND-1: the exit code accounts for acceptance eligibility, so an rc reader cannot see green where nothing was measured', () => {
  const measuredGreen = evaluatePolicy(evaluateState(jl([
    ...leg('TestA', 'pass', []), ...leg('TestB', 'pass', []),
  ])), { ...FULL, tagClass: DICT });
  assert.equal(exitCodeFor(measuredGreen), 0, 'only a fully measured, eligibility-declaring green run gets 0');
  const failed = evaluatePolicy(evaluateState(CARGO), { ...FULL, tagClass: DICT });
  assert.equal(exitCodeFor(failed), 1, 'FAIL outranks everything');
  assert.equal(exitCodeFor(failed, { allowPartial: true }), 1, '--allow-partial never launders a FAIL');
  const ineligibleGreen = evaluatePolicy(evaluateState(CARGO_ENV_SKIPS), { tagClass: DICT });
  assert.equal(ineligibleGreen.verdict, 'PASS');
  assert.equal(exitCodeFor(ineligibleGreen), 3, '3 = NOT_ACCEPTANCE: nothing was measured at acceptance level');
});

test('COND-6: a package whose every leaf skipped is NOTHING_MEASURED, even when every skip is named correctly', () => {
  const stream = jl([
    { Action: 'start', Package: PKG },
    ...skippedWithToken('TestNeedsRealStoreA', TOK_REAL),
    ...skippedWithToken('TestNeedsRealStoreB', TOK_REAL),
    { Action: 'output', Package: PKG, Output: `ok  \t${PKG}\t0.01s\n` },
    { Action: 'pass', Package: PKG },
  ]);
  for (const cfg of [{ tagClass: DICT }, { ...FULL, tagClass: DICT }]) {
    const res = evaluatePolicy(evaluateState(stream), cfg);
    assert.equal(res.verdict, 'FAIL', `a package of only skips is not a pass at env=${res.env}`);
    const v = res.violations.find((x) => x.rule === 'NOTHING_MEASURED');
    assert.ok(v, 'correctly named skips are still not coverage');
    assert.deepEqual(v.packages, [{ package: PKG, leaves: 2, skipped: 2, measured: 0 }]);
    assert.equal(res.counters.passLeaf, 0);
  }
});

test('COND-2: a SKIP_ENV reason that is not a label is malformed, and its value never reaches the report', () => {
  const leak = 'postgres://user:hunter2@db.internal:5432/prod';
  const res = evaluatePolicy(evaluateState(jl([
    ...leg('TestWhatever', 'pass', []),
    ...skippedWithToken('TestLeakyToken', `SKIP_ENV[tag=real_deps] reason=${leak} missing=AHP_TEST_DATABASE_URL -- a skip here means not-covered, not passed`),
  ])), { ...FULL, tagClass: DICT });
  const v = res.violations.find((x) => x.rule === 'MALFORMED_SKIP_TOKEN');
  assert.ok(v, 'a reason= carrying a DSN is a token defect; the acceptance report is exactly where it must not appear');
  assert.equal(v.defect, 'reason_not_a_label');
  assert.deepEqual(v.names, ['TestLeakyToken']);
  const rendered = `${renderText(res)}\n${renderRows(res).join('\n')}`;
  assert.ok(!rendered.includes('hunter2'), 'the value is withheld, not copied (Fermat measured the copy)');
  assert.ok(!rendered.includes(leak));
  assert.equal(res.verdict, 'FAIL');
});

test('COND-2: a missing= item that is not a variable name is malformed and withheld too', () => {
  const res = evaluatePolicy(evaluateState(jl([
    ...leg('TestWhatever', 'pass', []),
    ...skippedWithToken('TestLeakyMissing', 'SKIP_ENV[tag=real_deps] reason=real_store_env_absent missing=AHP_TEST_DATABASE_URL,hunter2-secret -- not covered'),
  ])), { tagClass: DICT });
  const v = res.violations.find((x) => x.rule === 'MALFORMED_SKIP_TOKEN');
  assert.ok(v, 'missing= lists variable NAMES; anything else is either a defect or a value');
  assert.equal(v.defect, 'missing_not_a_variable_name');
  assert.ok(!renderRows(res).join('\n').includes('hunter2-secret'));
  assert.equal(res.verdict, 'FAIL');
});

test('COND-2: the tokens the tree actually prints stay well-formed, so the grammar cannot redden an honest lane', () => {
  const real = [
    'SKIP_ENV[tag=real_deps] reason=real_store_env_absent missing=AHP_TEST_DATABASE_URL,AHP_S3_ENDPOINT,AHP_MINIO_ROOT_USER,AHP_MINIO_ROOT_PASSWORD -- a skip here means not-covered, not passed',
    'SKIP_ENV[tag=artifact_parser] reason=trusted_parser_absent missing=AHP_ARTIFACT_PARSER -- a skip here means not-covered, not passed',
    'SKIP_ENV[tag=artifact_plane] reason=object_store_env_absent missing=AHP_S3_ENDPOINT,AHP_MINIO_ROOT_USER,AHP_MINIO_ROOT_PASSWORD -- not covered',
    'SKIP_ENV[tag=artifact_plane] reason=object_store_endpoint_unparsable -- AHP_S3_ENDPOINT is not a host:port; refusing to guess which instance to write to',
    'SKIP_ENV[tag=artifact_plane] reason=object_store_not_lane_designated endpoint=172.18.0.6:9000 -- refusing to write outside the lane designation',
  ];
  const { outcomes } = parseGoTestJSON(jl([
    ...leg('TestWhatever', 'pass', []),
    ...real.flatMap((token, i) => skippedWithToken(`TestRealToken${i}`, token)),
  ]));
  const { skips, byKind, byClass } = classifySkips(outcomes, { tagClass: DICT });
  assert.equal(byKind.malformed, 0, `none of the tree's own tokens may be refused: ${JSON.stringify(skips.filter((s) => s.kind === 'malformed'))}`);
  assert.equal(byClass.real_store_env, 4);
  assert.equal(byClass.trusted_parser, 1);
});

test('COND-3: a declared exception never disarms the stubbed-skip detector, and the row stays in the report', () => {
  const allow = [{ package: PKG, name: 'TestSaysSkipAndPasses', justification: 'RFC-0000: pinned separately' }];
  const res = evaluatePolicy(evaluateState(jl([
    ...leg('TestSaysSkipAndPasses', 'pass', [`    executor_test.go:59: ${TOK_REAL}\n`, '--- PASS: TestSaysSkipAndPasses (0.00s)\n']),
  ])), { allowUnexplained: allow, tagClass: DICT });
  assert.equal(res.verdict, 'FAIL', 'an exception buys "declared not covered", never immunity from a lie');
  assert.ok(res.violations.some((v) => v.rule === 'STUBBED_SKIP_PASSED_AS_MEASURED'));
  assert.equal(res.rows.filter((r) => r.kind === 'stubbed-skip').length, 1,
    'a whitelisted record must stay in the report, not be scrubbed out of it');
});

test('COND-5: with no dictionary the real tag is refused, because the tool ships no copy of the dictionary', () => {
  const res = evaluatePolicy(evaluateState(jl([
    ...leg('TestWhatever', 'pass', []),
    ...skippedWithToken('TestNeedsRealStore', TOK_REAL),
  ]), { tagClass: {} }));
  assert.equal(res.verdict, 'FAIL');
  assert.equal(res.violations.find((x) => x.rule === 'UNKNOWN_SKIP_TAG').tag, 'real_deps',
    'refusing an unregistered tag is the mechanism half; registering it stays Volta\'s half');
  assert.equal(res.counters.byClass['unknown:real_deps'], 1);
});

test('COND-5: the mechanism source carries no dictionary literal of its own', () => {
  const src = fs.readFileSync(new URL('./skip-verdict.mjs', import.meta.url), 'utf8');
  assert.ok(!/real_deps\s*:/.test(src), 'a private tag-to-class copy drifts into a second policy; Volta owns one dictionary');
  assert.ok(!/artifact_plane\s*:/.test(src));
  assert.ok(!/trusted_parser'/.test(src), 'no class values baked in either');
  assert.match(src, /--tags/, 'the dictionary is supplied from outside');
});

test('COND-4: the wrapped command exit code travels as a field and a nonzero one is a violation', () => {
  const green = jl([...leg('TestA', 'pass', []), ...leg('TestB', 'pass', [])]);
  const res = evaluatePolicy(evaluateState(green), { ...FULL, tagClass: DICT, commandExit: 1 });
  assert.equal(res.verdict, 'FAIL', 'the director repro: a wrapped command ending in exit 1 used to print verdict PASS');
  assert.ok(res.violations.some((v) => v.rule === 'COMMAND_EXIT_NOT_ZERO'));
  assert.match(renderText(res), /^command_exit: 1$/m);
  const zero = evaluatePolicy(evaluateState(green), { ...FULL, tagClass: DICT, commandExit: 0 });
  assert.deepEqual(zero.violations, [], 'a zero exit adds nothing');
  assert.match(renderText(zero), /^command_exit: 0$/m);
});

test('COND-1 CLI: an ineligible green run exits 3; --allow-partial says reconnaissance; only a declared full env claims acceptance', () => {
  const measured = jl([...leg('TestA', 'pass', []), ...leg('TestB', 'pass', [])]);
  const bare = cli(['--stdin'], measured);
  assert.equal(bare.status, 3, `expected NOT_ACCEPTANCE, got:\n${bare.stdout}${bare.stderr}`);
  assert.match(bare.stdout, /^verdict: PASS$/m, 'the verdict field keeps Volta\'s vocabulary');
  assert.match(bare.stdout, /^exit_code: 3$/m);
  assert.match(bare.stdout, /^mode: not_acceptance$/m);

  const partial = cli(['--stdin', '--allow-partial'], measured);
  assert.equal(partial.status, 0);
  assert.match(partial.stdout, /^mode: reconnaissance$/m);

  const declared = cli(['--stdin', '--env-complete', '--designation', 'pg:review_r_gate'], measured);
  assert.equal(declared.status, 0);
  assert.match(declared.stdout, /^mode: acceptance$/m);
});

test('COND-4 CLI: a wrapped command that exits nonzero cannot produce a green verdict', () => {
  withTemp((dir) => {
    const f = path.join(dir, 'stream.json');
    fs.writeFileSync(f, jl([...leg('TestA', 'pass', []), ...leg('TestB', 'pass', [])]));
    const run = cli(['--env-complete', '--designation', 'pg:review_r_gate', '--',
      'sh', '-c', `cat ${JSON.stringify(f)}; exit 1`]);
    assert.equal(run.status, 1, `the exit code of the command must reach the verdict:\n${run.stdout}${run.stderr}`);
    assert.match(run.stdout, /^command_exit: 1$/m);
    assert.match(run.stdout, /COMMAND_EXIT_NOT_ZERO/);
    assert.match(run.stdout, /^verdict: FAIL$/m);

    const silent = cli(['--', 'sh', '-c', 'exit 2']);
    assert.equal(silent.status, 1);
    assert.ok(!/^verdict: PASS$/m.test(silent.stdout), 'no stream plus a failing command is not a pass');
    assert.match(silent.stdout, /LOG_UNVERIFIABLE/);
    assert.match(silent.stdout, /COMMAND_EXIT_NOT_ZERO/);
  });
});

test('COND-5 CLI: without --tags a real token is refused, with --tags its owner supplies the class', () => {
  const stream = jl([...leg('TestWhatever', 'pass', []), ...skippedWithToken('TestNeedsRealStore', TOK_REAL)]);
  const noTags = cli(['--stdin'], stream);
  assert.equal(noTags.status, 1, `an unregistered tag must not be green:\n${noTags.stdout}${noTags.stderr}`);
  assert.match(noTags.stdout, /^dictionary: absent$/m);
  assert.match(noTags.stdout, /UNKNOWN_SKIP_TAG/);
  withTemp((dir) => {
    const f = path.join(dir, 'tags.json');
    fs.writeFileSync(f, JSON.stringify([{ tag: 'real_deps', class: 'real_store_env' }]));
    const withTags = cli(['--stdin', '--tags', f], stream);
    assert.match(withTags.stdout, /^dictionary: .+ entries=1$/m);
    assert.ok(!withTags.stdout.includes('UNKNOWN_SKIP_TAG'), 'registered by the dictionary owner, so the mechanism stops complaining');
    assert.equal(withTags.status, 3, 'classified is still not an acceptance run');
  });
});

test('COND-6 CLI: a package of only env skips is red at any environment level', () => {
  const stream = jl([
    { Action: 'start', Package: PKG },
    ...skippedWithToken('TestNeedsRealStore', TOK_REAL),
    { Action: 'output', Package: PKG, Output: `ok  \t${PKG}\t0.01s\n` },
    { Action: 'pass', Package: PKG },
  ]);
  withTemp((dir) => {
    const f = path.join(dir, 'tags.json');
    fs.writeFileSync(f, JSON.stringify([{ tag: 'real_deps', class: 'real_store_env' }]));
    const run = cli(['--stdin', '--tags', f], stream);
    assert.equal(run.status, 1, 'reconnaissance still cannot call a package of pure skips a pass');
    assert.match(run.stdout, /NOTHING_MEASURED/);
  });
});
