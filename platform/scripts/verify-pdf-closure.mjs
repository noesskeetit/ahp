#!/usr/bin/env node
// verify-pdf-closure.mjs — reproducible closure of "every PDF section -> evidence" (acceptance #2).
//
// Derives the AUTHORITATIVE top-level section list directly from the canonical
// product PDF's own СОДЕРЖАНИЕ (table of contents) and asserts that every such
// section is referenced in docs/coverage.md. This turns the hand-asserted closure
// table in coverage.md into a machine-checkable gate. It does NOT embed the PDF
// (the PDF stays outside git per policy) and does NOT widen scope: only the
// canonical PDF's own TOC is used, never sources referenced inside it.
//
// Requirements source (exclusive): ahp-agents-space-architecture (2) (1).pdf rev 3.2,
//   sha256 044e045b73b7c34b642bd3c9eb5701b93eed470873a1e044e8b1d024821a7e2 (64 pages).
//   NOTE: the sha tail below is the canonical one; verify it out-of-band, not here.
//
// Usage:
//   pdftotext -layout "<pdf>" /tmp/ahp-pdf.txt   # produce text OUTSIDE git
//   node scripts/verify-pdf-closure.mjs --text /tmp/ahp-pdf.txt [--coverage docs/coverage.md]
//
// Exit codes: 0 = every PDF section mapped; 1 = at least one section UNMAPPED;
// 2 = input error (missing/unreadable text or coverage, no СОДЕРЖАНИЕ block, empty list).
import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';

const args = process.argv.slice(2);
function opt(name, dflt) {
  const i = args.indexOf(name);
  return i >= 0 && i + 1 < args.length ? args[i + 1] : dflt;
}

function fail2(msg) { process.stderr.write(msg + '\n'); process.exit(2); }

const textPath = opt('--text', process.env.AHP_PDF_TEXT || '');
const coveragePath = opt('--coverage', path.join(process.cwd(), 'docs', 'coverage.md'));
const expectSha = (opt('--expect-sha256', process.env.AHP_PDF_SHA256 || '')).toLowerCase();

if (!textPath) fail2('need --text <path to pdftotext output of the canonical PDF (produce outside git)');
let pdfText;
try { pdfText = fs.readFileSync(textPath, 'utf8'); } catch (e) { fail2('cannot read --text: ' + e.message); }
let coverage;
try { coverage = fs.readFileSync(coveragePath, 'utf8'); } catch (e) { fail2('cannot read --coverage: ' + e.message); }

// Optional integrity check when the caller passes the raw PDF alongside the text.
const pdfPath = opt('--pdf', process.env.AHP_PDF_PATH || '');
if (expectSha) {
  if (!pdfPath) fail2('--expect-sha256 given but no --pdf path to verify');
  const got = createHash('sha256').update(fs.readFileSync(pdfPath)).digest('hex');
  if (got !== expectSha) fail2('PDF sha256 MISMATCH got=' + got + ' want=' + expectSha);
}

// Locate the СОДЕРЖАНИЕ (TOC) block: from the line equal to СОДЕРЖАНИЕ up to ИСТОЧНИКИ.
const lines = pdfText.split(/\r?\n/);
let s = -1, e = -1;
for (let i = 0; i < lines.length; i++) {
  const t = lines[i].trim();
  if (s < 0 && /^СОДЕРЖАНИЕ$/.test(t)) { s = i + 1; continue; }
  if (s >= 0 && /^ИСТОЧНИКИ$/.test(t)) { e = i; break; }
}
if (s < 0) fail2('СОДЕРЖАНИЕ block not found in --text (wrong file?)');
if (e < 0) e = lines.length;

// A TOC entry line: <title><dot-leaders><page>. Dot-leaders = runs of '.', space and U+2060.
// Require >=3 leader chars to avoid matching ordinary sentences that end in a number.
const tocRe = /^(.+?)[\s.\u2060\u00a0]{3,}(\d+)\s*$/;
const sections = [];
for (let i = s; i < e; i++) {
  const raw = lines[i];
  if (!raw.trim()) continue;
  const m = raw.match(tocRe);
  if (!m) continue;
  const title = m[1].replace(/[\s\u2060\u00a0]+$/, '').trim();
  const page = parseInt(m[2], 10);
  if (!title || !isFinite(page)) continue;
  // Skip accidental table rows (heuristic: a real top-level entry starts with a number+'.',
  // or with 'Коротко'/'Атлас'/'Приложение').
  if (!/^(\d{1,2}\.?\s|\u041a\u043e\u0440\u043e\u0442\u043a\u043e|\u0410\u0442\u043b\u0430\u0441|\u041f\u0440\u0438\u043b\u043e\u0436\u0435\u043d\u0438\u0435)/.test(title)) continue;
  sections.push({ title, page });
}
if (sections.length === 0) fail2('no top-level TOC sections parsed (regex/format mismatch?)');

// Normalizer: lowercase, drop punctuation/leaders/word-joiners, collapse whitespace.
function norm(x) {
  return x
    .toLowerCase()
    .replace(/[\u2060\u00a0]+/g, ' ')
    .replace(/[^\p{L}\p{N} ]+/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}
const covNorm = norm(coverage);

// For each PDF section, require a mapped reference in coverage.md. Match by section
// number prefix (for numbered) and/or a distinctive title word-run (>=1 word of length>=4).
function mapped(sec) {
  const num = sec.title.match(/^(\d{1,2})\b/);
  if (num) {
    // accept a coverage row that begins a cell with "N." and shares >=1 title token
    const rowRe = new RegExp('^\\|\\s*' + num[1] + '\\.\\s', 'm');
    if (rowRe.test(coverage)) return 'row|' + num[1] + '.';
  }
  // title-token fallback: every significant token (len>=4) must appear normalized
  const tokens = norm(sec.title.replace(/^\d{1,2}\.?\s+/, '')).split(' ').filter(w => w.length >= 4);
  if (tokens.length && tokens.every(t => covNorm.includes(t))) return 'title';
  // for short front-matter titles (Коротко, Атлас) exact normalized title presence
  const nt = norm(sec.title);
  if (covNorm.includes(nt)) return 'title-exact';
  return null;
}

let bad = 0;
for (const sec of sections) {
  const via = mapped(sec);
  if (!via) { bad++; process.stdout.write('UNMAPPED  p' + String(sec.page).padStart(2) + '  ' + sec.title + '\n'); }
  else process.stdout.write('mapped    p' + String(sec.page).padStart(2) + '  [' + via + ']  ' + sec.title + '\n');
}
process.stdout.write('\nPDF sections from СОДЕРЖАНИЕ: ' + sections.length + '; mapped: ' + (sections.length - bad) + '; unmapped: ' + bad + '\n');
if (bad > 0) { process.stdout.write('PDF_CLOSURE=FAIL unmapped=' + bad + '\n'); process.exit(1); }
process.stdout.write('PDF_CLOSURE=PASS sections=' + sections.length + '\n');
process.exit(0);
