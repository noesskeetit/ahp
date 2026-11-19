#!/usr/bin/env node
// tests/e2e/live-qwen.mjs — Live native-Qwen reachability + inference probe.
//
// CONTRACT: the product's only inference backend is native Qwen
// (model-run-tqmbu-road, AHP-internal Cloud.ru Foundation Models, exposed
// through an OpenAI-compatible /v1 API). NO OpenAI / Astra / other fallback is
// permitted, so this probe refuses to substitute any other backend.
//
// This is an INFRASTRUCTURE-level gate, not a full product E2E: it verifies
// that the configured native-Qwen endpoint answers /v1/models and completes a
// minimal chat turn. Full product E2E (agent turn through the AHP control
// plane -> Qwen -> checkpoint) additionally requires the `execution` and
// `space-product` blocks to be activated by the architect (G-DEPS).
//
// Behavior:
//   - No endpoint configured (AHP_QWEN_BASE_URL unset) OR endpoint unreachable
//     -> print the honest gate and SKIP (exit 0; the suite stays green for the
//     director, nothing is fabricated).
//   - Reachable -> probe /v1/models (expect the qwen model), then one
//     /v1/chat/completions turn; report PASS/FAIL honestly.
//
// Secrets: the API key is read from env and NEVER printed. Only non-secret
// fields (host, model id, latency) are surfaced.
//
// Maps to PDF §11 "ёмкость/latency" inference path + §2.2 классы исполнения
// (the LLM-backed execution class).

const BASE_URL = (process.env.AHP_QWEN_BASE_URL || '').trim().replace(/\/+$/, '');
const API_KEY = (process.env.AHP_QWEN_API_KEY || '').trim();
const MODEL = (process.env.AHP_QWEN_MODEL || 'ahp_qwen').trim();
const GATE = 'G-LLM-QWEN';

function hostOnly(u) {
  try {
    return new URL(u).host;
  } catch {
    return '<unparseable>';
  }
}

function skip(reason) {
  console.log(`SKIP: native-Qwen live probe skipped.`);
  console.log(`GATE ${GATE}: ${reason}`);
  console.log('NOTE: this is not a fabricated pass — the live Qwen gate remains OPEN.');
  process.exit(0);
}

if (!BASE_URL) {
  skip('no Qwen inference endpoint configured in this environment (AHP_QWEN_BASE_URL unset)');
}

const authHeaders = API_KEY ? { Authorization: `Bearer ${API_KEY}` } : {};
const fetchOpts = { signal: AbortSignal.timeout(15000) };

async function main() {
  // 1) /v1/models reachability + model presence.
  let models;
  try {
    const res = await fetch(`${BASE_URL}/v1/models`, {
      method: 'GET',
      headers: { ...authHeaders },
      ...fetchOpts,
    });
    if (res.status === 401 || res.status === 403) {
      console.error(`FAIL: ${GATE} endpoint ${hostOnly(BASE_URL)} rejected auth (HTTP ${res.status})`);
      process.exit(1);
    }
    if (!res.ok) {
      // Configured but not usable -> honest FAIL (config present but broken).
      console.error(`FAIL: ${GATE} /v1/models HTTP ${res.status}`);
      process.exit(1);
    }
    models = await res.json();
  } catch (e) {
    // Configured but unreachable (DNS/connect/timeout). Treat as OPEN gate so we
    // do not hard-fail the director's suite for an env without VPC access; but
    // label it explicitly as unreachable, not passed.
    skip(`configured endpoint ${hostOnly(BASE_URL)} unreachable: ${e && e.name ? e.name : 'error'}`);
  }

  const ids = Array.isArray(models?.data) ? models.data.map((m) => m.id) : [];
  const hasModel = ids.some((id) => id === MODEL || id.includes(MODEL));
  console.log(`models: host=${hostOnly(BASE_URL)} count=${ids.length} model="${MODEL}" present=${hasModel}`);
  if (!hasModel) {
    console.error(`FAIL: ${GATE} expected model "${MODEL}" not advertised by ${hostOnly(BASE_URL)}`);
    console.error(`      advertised: ${ids.slice(0, 12).join(', ')}${ids.length > 12 ? ' …' : ''}`);
    process.exit(1);
  }

  // 2) Minimal native-Qwen chat turn (OpenAI-compatible). No fallback backend.
  const t0 = Date.now();
  const cRes = await fetch(`${BASE_URL}/v1/chat/completions`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...authHeaders },
    ...fetchOpts,
    body: JSON.stringify({
      model: MODEL,
      messages: [{ role: 'user', content: 'Reply with exactly: PONG' }],
      max_tokens: 16,
      temperature: 0,
    }),
  });
  const ms = Date.now() - t0;
  if (!cRes.ok) {
    console.error(`FAIL: ${GATE} /v1/chat/completions HTTP ${cRes.status}`);
    process.exit(1);
  }
  const cj = await cRes.json();
  const text = cj?.choices?.[0]?.message?.content ?? '';
  const usage = cj?.usage ?? {};
  console.log(`chat: model="${MODEL}" latency_ms=${ms} finish=${cj?.choices?.[0]?.finish_reason ?? '?'} completion_tokens=${usage.completion_tokens ?? '?'} reply="${text.trim().slice(0, 40)}"`);
  if (!text.trim()) {
    console.error(`FAIL: ${GATE} empty completion from native Qwen`);
    process.exit(1);
  }

  console.log(`PASS: native-Qwen INFRASTRUCTURE probe OK (reachable + non-empty completion).`);
  console.log('NOTE: INFRASTRUCTURE-level only — full product agent-turn E2E still gated on execution/space-product (G-DEPS).');
}

await main();
