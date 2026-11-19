#!/usr/bin/env node
// npm run smoke — HTTP-проверка живости API (GET /healthz).
//
// Объём: только HTTP. Это НЕ LLM-вызов и НЕ сквозной (E2E) сценарий: здесь нет
// агентов, моделей, вычислений, файлов и пользовательских путей.
//
// Два режима:
//   1. ВНЕШНИЙ — если AHP_API_URL явно экспортирован в окружении процесса
//      (так делает scripts/ci-serve-and-test.sh, поднимая свой сервис) — шлём
//      GET на него по http(s).
//   2. SELF-SPAWN (по умолчанию) — если AHP_API_URL в процессе не задан,
//      поднимаем локальный сервис на изолированной БД блока (mTLS) и проверяем
//      его /healthz. Значение AHP_API_URL из файла .env НЕ считается запросом
//      к внешнему сервису: там это дефолт, за которым никто не слушает, и
//      проверка на нём давала бы ложный провал.
//
// Ложный успех исключён. Ненулевой код возврата — если:
//   * запрос не выполнен или превысил таймаут;
//   * HTTP-статус вне 200..299;
//   * тело не является JSON, это не объект, поле status отсутствует или
//     status !== "ok".
//
// Только встроенные модули Node; shell не используется.

import { spawnService, pollReady, httpGet } from '../tests/helpers/ahp-client.mjs';
import https from 'node:https';
import { readFileSync } from 'node:fs';

// An explicitly selected machine API may require workload mTLS, just like its
// ordinary callers. Keep server verification enabled and never print key data.
async function externalHealth(url, signal) {
  const cert = process.env.AHP_TLS_CLIENT_CERT;
  const key = process.env.AHP_TLS_CLIENT_KEY;
  if (!cert && !key) return fetch(url, { headers: { accept: 'application/json' }, signal });
  if (!cert || !key || url.protocol !== 'https:') {
    throw new Error('mTLS smoke requires HTTPS and both AHP_TLS_CLIENT_CERT and AHP_TLS_CLIENT_KEY');
  }
  const tls = { cert: readFileSync(cert), key: readFileSync(key), rejectUnauthorized: true, minVersion: 'TLSv1.2' };
  if (process.env.AHP_TLS_CA) tls.ca = readFileSync(process.env.AHP_TLS_CA);
  return new Promise((resolve, reject) => {
    const req = https.request(url, { ...tls, method: 'GET', headers: { accept: 'application/json' }, signal }, res => {
      const chunks = []; let size = 0;
      res.on('data', chunk => {
        size += chunk.length;
        if (size > 65536) req.destroy(new Error('health response exceeds 64 KiB'));
        else chunks.push(chunk);
      });
      res.on('error', reject);
      res.on('end', () => resolve({ status: res.statusCode, text: async () => Buffer.concat(chunks).toString('utf8') }));
    });
    req.on('error', reject);
    req.end();
  });
}

function fail(message) {
  process.stderr.write(`\n[smoke] FAIL: ${message}\n`);
  process.exit(1);
}

// Проверка ответа /healthz — общая для обоих режимов.
function assertHealthz(status, bodyText, where) {
  if (status < 200 || status > 299) {
    fail(`unexpected HTTP status ${status} from ${where}: ${bodyText.slice(0, 500)}`);
  }
  let payload;
  try {
    payload = JSON.parse(bodyText);
  } catch {
    fail(`/healthz did not return JSON (status ${status}): ${bodyText.slice(0, 500)}`);
  }
  if (payload === null || typeof payload !== 'object' || Array.isArray(payload)) {
    fail(`/healthz JSON must be an object, got: ${bodyText.slice(0, 200)}`);
  }
  if (payload.status !== 'ok') {
    fail(`/healthz field "status" must be "ok", got ${JSON.stringify(payload.status ?? null)}`);
  }
}

// Только явный экспорт процесса включает внешний режим (ci-serve-and-test.sh).
const externalUrl = (process.env.AHP_API_URL || '').trim();

if (externalUrl !== '') {
  let healthzUrl;
  try {
    const base = new URL(externalUrl);
    if (base.protocol !== 'http:' && base.protocol !== 'https:') {
      throw new Error(`unsupported protocol ${base.protocol}`);
    }
    const basePath = base.pathname.replace(/\/+$/, '');
    base.pathname = basePath === '' ? '/healthz' : `${basePath}/healthz`;
    healthzUrl = base;
  } catch (err) {
    fail(`AHP_API_URL is not a valid http(s) URL (${err.message})`);
  }

  const timeoutMs = 10_000;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  let response;
  try {
    process.stdout.write(`[smoke] external GET ${healthzUrl.toString()}\n`);
    response = await externalHealth(healthzUrl, controller.signal);
  } catch (err) {
    const reason = err?.name === 'AbortError' ? `timed out after ${timeoutMs} ms` : err.message;
    fail(`request to ${healthzUrl.toString()} failed: ${reason}. Is the API running?`);
  } finally {
    clearTimeout(timer);
  }
  assertHealthz(response.status, await response.text(), healthzUrl.toString());
} else {
  // Self-spawn локального сервиса на изолированной БД блока.
  const port = Number(process.env.AHP_SMOKE_PORT || 19619);
  process.stdout.write(`[smoke] self-spawn local service on :${port} (mTLS)\n`);
  const svc = await spawnService({ port, role: 'all', tls: true });
  try {
    await pollReady(port, { tls: true, timeoutMs: 20000 });
    const res = await httpGet(port, '/healthz', { tls: true });
    process.stdout.write(`[smoke] GET https://127.0.0.1:${port}/healthz -> ${res.status}\n`);
    assertHealthz(res.status, res.text, `https://127.0.0.1:${port}/healthz`);
  } catch (err) {
    fail(`self-spawn smoke failed: ${err.message}\n--- service logs ---\n${svc.logs().slice(-1000)}`);
  } finally {
    svc.kill();
  }
}

process.stdout.write(
  '[smoke] OK: /healthz answered 2xx with {"status":"ok"}.\n' +
    '[smoke] scope: HTTP liveness only — not an LLM call and not an end-to-end scenario.\n',
);
