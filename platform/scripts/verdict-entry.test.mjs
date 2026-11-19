// Fermat / gate acceptance PATH. The mechanism (scripts/skip-verdict.mjs, Russell)
// can refuse a verdict; nothing in the tree could yet ISSUE one, because the npm
// entry passed no dictionary and no environment statement. These legs pin the
// entry, not the counter:
//   * test:go:verdict must hand the mechanism Volta's dictionary (COND-5: the
//     canonical registry, never a copy inside the mechanism or inside this leg);
//   * acceptance requires a DECLARED complete environment plus a designation, and
//     the entry must be structurally unable to fabricate either;
//   * a partial run stays reconnaissance and is a separate, named entry.
// SYNTHETIC `go test -json` streams only (no Go build, no database, no docker).
// Registering tags is Volta's leg (tests/hygiene/skip_tags_test.go) and counting
// skips is Russell's mechanism: neither is reimplemented here, and the derived
// view is checked by calling Volta's own generator in --check mode.
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { readTagFile } from './skip-verdict.mjs';

const ROOT = fileURLToPath(new URL('..', import.meta.url));
const CLI = path.join(ROOT, 'scripts', 'skip-verdict.mjs');
const HEAD = 'node scripts/skip-verdict.mjs';
const ENTRY = 'test:go:verdict';
const RECON = 'test:go:reconnaissance';
// Volta's canonical registry and the machine view the mechanism can read
// (reports/mac-director/skip-acceptance-policy-volta-20261110.md, section 5.1).
const CANONICAL = 'tools/skip-tags.json';
const MECHANISM_VIEW = 'tools/skip-tags.mechanism.json';
const GENERATOR = 'tools/gen-skip-tags-mechanism.py';
// Ambient AHP_GATE_ENV / AHP_GATE_DESIGNATION would silently change what a probe
// proves, so probes start from an empty environment (Russell's discipline).
const CLEAN_ENV = { PATH: process.env.PATH };
const PKG = 'github.com/ahp-platform/ahp/internal/executor';

const pkg = JSON.parse(fs.readFileSync(path.join(ROOT, 'package.json'), 'utf8'));
const script = (name) => pkg.scripts?.[name] ?? null;

function entryParts(name) {
  const text = script(name);
  assert.ok(text, `package.json has no script "${name}"`);
  const at = text.indexOf(' -- ');
  assert.ok(at > 0, `${name} must wrap a command after " -- ": there is nothing to verdict without one`);
  const head = text.slice(0, at);
  assert.ok(head.startsWith(HEAD),
    `${name} must go through the mechanism, not around it: ${head}`);
  // The dictionary path is relative by contract, so whitespace splitting is safe
  // and an absolute path (which would make the entry machine-specific) is caught here.
  const flags = head.slice(HEAD.length).trim().split(/\s+/).filter(Boolean);
  assert.ok(!flags.some((f) => f.startsWith('/')),
    `${name} must not name absolute paths: ${flags.join(' ')}`);
  return { flags, command: text.slice(at + 4).trim() };
}

function tagsFlag(flags) {
  const i = flags.indexOf('--tags');
  if (i < 0) return null;
  return flags[i + 1] ?? '';
}

function probe(flags, input = '', env = CLEAN_ENV) {
  return spawnSync(process.execPath, [CLI, ...flags, '--stdin'],
    { input, env, encoding: 'utf8', cwd: ROOT });
}

function field(out, name) {
  const hit = out.match(new RegExp(`^${name}: (.*)$`, 'm'));
  // renderText puts explanatory prose after a value on the same line
  // ('acceptance_eligible: false  (partial environment: ...)'), so the value is
  // what stands before the double space; comparing prose to a literal reddens a
  // leg that measures the right thing.
  return hit ? hit[1].split(/\s{2,}/)[0] : null;
}

function jl(events) {
  return events.map((e) => JSON.stringify({ Time: '2026-11-10T06:00:00.000Z', ...e })).join('\n') + '\n';
}

function leg(name, action, outputs = []) {
  const events = [{ Action: 'run', Package: PKG, Test: name }];
  for (const Output of outputs) events.push({ Action: 'output', Package: PKG, Test: name, Output });
  events.push({ Action: action, Package: PKG, Test: name, Elapsed: 0.01 });
  return events;
}

// The token text is the one the tree really prints (internal/executor/realdeps_test.go:35-37).
function token(tag) {
  return `SKIP_ENV[tag=${tag}] reason=lane_stand_env_var_unset missing=AHP_TEST_DATABASE_URL,AHP_S3_ENDPOINT`
    + ' -- these legs measure the real plane: a skip here means not-covered, not passed';
}

function skippedWithToken(name, tag) {
  return leg(name, 'skip', [`    x_test.go:59: ${token(tag)}\n`]);
}

function measuredStream(tag) {
  return jl([
    ...leg('TestMeasuresSomething', 'pass', ['--- PASS: TestMeasuresSomething (0.01s)\n']),
    ...leg('TestMeasuresAnother', 'pass', ['--- PASS: TestMeasuresAnother (0.01s)\n']),
    ...(tag ? skippedWithToken('TestNeedsAStand', tag) : []),
    { Action: 'pass', Package: PKG, Elapsed: 0.02 },
  ]);
}

function failingStream() {
  return jl([
    ...leg('TestMeasuresSomething', 'pass', ['--- PASS: TestMeasuresSomething (0.01s)\n']),
    ...leg('TestBreaks', 'fail', ['--- FAIL: TestBreaks (0.01s)\n']),
    { Action: 'fail', Package: PKG, Elapsed: 0.02 },
  ]);
}

// A registered tag from the dictionary the entry is supposed to consume. Taken
// from the file rather than hardcoded: a leg that recites the registry is a
// second copy of it, which is exactly what COND-5 forbids.
function dictionaryTag() {
  const entries = JSON.parse(fs.readFileSync(path.join(ROOT, MECHANISM_VIEW), 'utf8'));
  assert.ok(Array.isArray(entries) && entries.length > 0, `${MECHANISM_VIEW} must be a non-empty JSON array`);
  return entries[0].tag;
}

function mechanismViewExists() {
  return fs.existsSync(path.join(ROOT, MECHANISM_VIEW));
}

// ---------------------------------------------------------------------------
// The entry.
// ---------------------------------------------------------------------------

test("P1 the acceptance entry hands the mechanism Volta's dictionary and still runs the real suite", () => {
  const { flags, command } = entryParts(ENTRY);
  const path_ = tagsFlag(flags);
  assert.equal(path_, MECHANISM_VIEW,
    `${ENTRY} must pass --tags ${MECHANISM_VIEW}: without a dictionary every honest skip is UNKNOWN_SKIP_TAG and the entry can only ever refuse`);
  assert.equal(flags.filter((f) => f === '--tags').length, 1, `${ENTRY} must name exactly one dictionary`);
  // The command must still be the real suite: an entry that quietly narrows to a
  // fixture would report acceptance over something nobody asked about.
  assert.match(command, /^go test\b/, `${ENTRY} must wrap go test, got: ${command}`);
  for (const need of ['-json', '-p=1', '-count=1', './...']) {
    assert.ok(command.split(/\s+/).includes(need), `${ENTRY} lost ${need}: ${command}`);
  }
  // The dictionary has to be readable by the mechanism's own reader, and the CLI
  // must say which dictionary was in force (COND-5 makes that part of the measurement).
  const map = readTagFile(path.join(ROOT, MECHANISM_VIEW));
  assert.ok(Object.keys(map).length > 0, `${MECHANISM_VIEW} resolved to an empty dictionary`);
  const res = probe(flags, measuredStream());
  assert.match(res.stdout, new RegExp(`^dictionary: ${MECHANISM_VIEW.replace('.', '\\.')} entries=\\d+$`, 'm'),
    `the run must record the dictionary it used: ${res.stdout}${res.stderr}`);
});

test('P2 acceptance and reconnaissance are two entries, and the acceptance one cannot call itself partial', () => {
  const acceptance = entryParts(ENTRY);
  assert.ok(!acceptance.flags.includes('--allow-partial'),
    `${ENTRY} must not pass --allow-partial: that turns Volta 1 (rc=3 for an undeclared environment) back into the rc=0 an rc-reader calls a passed gate`);
  const recon = entryParts(RECON);
  assert.ok(recon.flags.includes('--allow-partial'),
    `${RECON} exists to be the reconnaissance entry; without --allow-partial it is a duplicate of ${ENTRY}`);
  assert.equal(tagsFlag(recon.flags), tagsFlag(acceptance.flags),
    'both entries must read the same dictionary, or the two views of one run disagree');
  assert.equal(recon.command, acceptance.command,
    'both entries must run the same suite; reconnaissance is a different claim about the environment, not a different test set');
});

test('P3 the dictionary the entry consumes is Volta derived view, verified by his own checker', () => {
  assert.ok(fs.existsSync(path.join(ROOT, GENERATOR)),
    `${GENERATOR} is absent: the entry would consume a hand-kept machine view nobody can re-derive (Volta 5.1)`);
  const check = spawnSync('python3', [GENERATOR, '--check'], { cwd: ROOT, encoding: 'utf8' });
  assert.equal(check.status, 0,
    `${MECHANISM_VIEW} has drifted from ${CANONICAL}: ${check.stdout}${check.stderr}`);
});

test('P4 the mechanism still ships no dictionary of its own (COND-5)', () => {
  const source = fs.readFileSync(CLI, 'utf8');
  assert.match(source, /^export const TAG_CLASS = \{\};$/m,
    'the mechanism must not carry a private copy of the registry: it decides with the dictionary it is handed');
  if (mechanismViewExists()) {
    const entries = JSON.parse(fs.readFileSync(path.join(ROOT, MECHANISM_VIEW), 'utf8'));
    for (const e of entries) {
      assert.ok(!new RegExp(`TAG_CLASS\\s*=\\s*\\{[^}]*\\b${e.tag}\\b`).test(source),
        `the mechanism embedded ${e.tag} - that is a second dictionary`);
    }
  }
});

// ---------------------------------------------------------------------------
// What the dictionary actually changes, probed with the entry own flags.
// ---------------------------------------------------------------------------

test('P5 with the entry flags a registered tag is classified; without --tags the same stream is UNKNOWN_SKIP_TAG', () => {
  const { flags } = entryParts(ENTRY);
  const tag = dictionaryTag();
  const withDictionary = probe(flags, measuredStream(tag));
  assert.doesNotMatch(withDictionary.stdout, /UNKNOWN_SKIP_TAG/,
    `a registered tag must not be reported unknown: ${withDictionary.stdout}${withDictionary.stderr}`);
  const byClass = JSON.parse(field(withDictionary.stdout, 'skips').match(/by_class=(\{.*?\})/)[1]);
  const registry = readTagFile(path.join(ROOT, MECHANISM_VIEW));
  assert.equal(byClass[registry[tag]], 1,
    `the class must come from the registry (${tag} -> ${registry[tag]}), got ${JSON.stringify(byClass)}`);
  // The pre-change entry: the same flags minus the dictionary. This is the RED the
  // task asked for - the entry could not classify a real token at all.
  const without = flags.filter((f, i) => f !== '--tags' && flags[i - 1] !== '--tags');
  const withoutDictionary = probe(without, measuredStream(tag));
  assert.match(withoutDictionary.stdout, /UNKNOWN_SKIP_TAG/,
    'without a dictionary an honest skip must be reported as unknown - this is what the entry used to do');
});

test('P6 the entry cannot fabricate acceptance: full environment demands a designation and no env-skip', () => {
  const { flags } = entryParts(ENTRY);
  const tag = dictionaryTag();
  const noDesignation = probe(flags, measuredStream(), { ...CLEAN_ENV, AHP_GATE_ENV: 'full' });
  assert.equal(noDesignation.status, 1,
    `an undeclared designation is red, not green: ${noDesignation.stdout}${noDesignation.stderr}`);
  assert.match(noDesignation.stdout, /MISSING_DESIGNATION_AT_FULL_ENV/,
    'the refusal must name why: writing to undesignated storage is forbidden');
  const skippedAtFull = probe([...flags, '--designation', 'fermat-review-lane'], measuredStream(tag),
    { ...CLEAN_ENV, AHP_GATE_ENV: 'full' });
  assert.equal(skippedAtFull.status, 1,
    'a leg that skipped in a run claiming a complete environment cannot be accepted');
  assert.match(skippedAtFull.stdout, /SKIP_ENV_AT_FULL_ENV/,
    'and the refusal must name the rule, not a generic failure');
});

test('P7 reconnaissance is named as such and never launders a failure', () => {
  const { flags } = entryParts(RECON);
  const tag = dictionaryTag();
  const recon = probe(flags, measuredStream(tag));
  assert.equal(recon.status, 0, `a partial run may exit 0 as reconnaissance: ${recon.stdout}${recon.stderr}`);
  assert.equal(field(recon.stdout, 'mode'), 'reconnaissance', 'the mode must be a field, not prose');
  assert.equal(field(recon.stdout, 'acceptance_eligible'), 'false',
    'reconnaissance must state that it is not acceptance');
  const failed = probe(flags, failingStream());
  assert.equal(failed.status, 1, '--allow-partial must never launder a FAIL into a 0');
});

// ---------------------------------------------------------------------------
// Hygiene and discoverability.
// ---------------------------------------------------------------------------

test('P8 the dictionary the entry consumes carries labels only, never a credential', () => {
  const raw = fs.readFileSync(path.join(ROOT, MECHANISM_VIEW), 'utf8');
  const entries = JSON.parse(raw);
  for (const e of entries) {
    for (const [key, value] of Object.entries(e)) {
      assert.match(String(value), /^[a-z0-9][a-z0-9_]{0,63}$/,
        `${key} must hold a label, not free text: ${JSON.stringify(value).slice(0, 24)}`);
    }
  }
  // A registry is read into a report, so it is a leak channel like any other
  // (COND-2): nothing in it may look like a value that was ever used to authenticate.
  assert.doesNotMatch(raw, /(?:password|passwd|secret|api[_-]?key|access[_-]?key|token)\s*[:=]\s*\S/i,
    'the dictionary must not carry a credential-shaped assignment');
  assert.doesNotMatch(raw, /(^|[^A-Za-z0-9_])[A-Za-z0-9+/]{24,}={0,2}([^A-Za-z0-9_]|$)/,
    'the dictionary must not carry a base64-shaped blob');
});

test('P9 the leg is reachable from npm, so it cannot rot unnoticed', () => {
  const runner = script('verify:verdict-entry');
  assert.ok(runner, 'package.json must expose verify:verdict-entry: an unrunnable leg is a dead leg');
  assert.match(runner, /scripts\/verdict-entry\.test\.mjs/,
    'the entry must name this file, otherwise renaming it silently deletes the coverage');
});

test('P10 the acceptance path stays separate from the wide gate', () => {
  for (const name of [ENTRY, RECON]) {
    const text = script(name);
    assert.ok(text, `package.json has no script "${name}"`);
    assert.doesNotMatch(text, /run-tests\.py|full-gate-wide/,
      `${name} must not grow into the wide gate: the wide gate is another lane and another budget`);
  }
});
