#!/usr/bin/env node
// AHP delivery lane: isolated human-origin proxy (:29710).
// Single persistent browser origin for the Agents Space demo:
//   * serves the built UI at "/"
//   * proxies /u-ahp/v1/* to the isolated composed backend (:29700)
//   * terminates upstream mTLS from privateenv paths (client cert held ONLY here,
//     never sent to the browser); the browser talks plain localhost + HttpOnly cookie
//   * forwards SSE streams unbuffered and relays Last-Event-ID for reconnect
//   * human-JWT/session cookie only: no signing key, no upstream URL disclosure
// No secrets are logged. Upstream is reached by loopback only.
import http from 'node:http';
import https from 'node:https';
import fs from 'node:fs';
import path from 'node:path';

const LISTEN = process.env.AHP_HUMAN_ORIGIN_LISTEN || '127.0.0.1:29710';
const UPSTREAM = process.env.AHP_HUMAN_ORIGIN_UPSTREAM || 'http://127.0.0.1:29700';
const UI_DIR = process.env.AHP_HUMAN_ORIGIN_UI || path.join(process.cwd(), 'web', 'static');
const up = new URL(UPSTREAM);
const USE_TLS = up.protocol === 'https:' || process.env.AHP_UPSTREAM_TLS === '1';
const COOKIE = process.env.AHP_BROWSER_COOKIE || 'ahp_browser_session';

// The prefixes ONE human origin hands to the composed front, listed one by one rather
// than as '/v1/': the machine namespaces (state, artifacts' BeginUpload/Seal, model
// TreeCost, identity control) must stay unreachable from a session origin, because the
// browser holds a human cookie and nothing else. /v1/space/, /v1/workspace/ and
// /v1/compute/ are owned by the front's own single-origin edge
// (cmd/ahp/single_origin.go), so refusing them HERE would only make a reachable surface
// look broken; a plane's 403/404 answer passes through verbatim, which is what lets a
// refusal leg measure the gate instead of the proxy.
const FORWARDED_PREFIXES = [
  '/u-ahp/v1/',      // auth + the human plane surfaces (registry/evals/triggers/memory/model Cost/artifacts)
  '/v1/space/',      // the Agent Space human surface: turns, stream, cancel
  '/v1/workspace/',  // forwarded so the front answers it; refused honestly while no human pipe exists
  '/v1/compute/',    // same rule: a mounted plane reaches the browser, an unmounted one says 404
];

// Server-side client cert for upstream mTLS. Paths come from privateenv only.
let agent = null;
if (USE_TLS) {
  agent = new https.Agent({
    cert: fs.readFileSync(process.env.AHP_TLS_CLIENT_CERT),
    key: fs.readFileSync(process.env.AHP_TLS_CLIENT_KEY),
    ca: fs.readFileSync(process.env.AHP_TLS_CA),
    // Server name of the backend workload identity.
    servername: process.env.AHP_UPSTREAM_SERVERNAME || 'ahp-composed-backend',
    keepAlive: true,
  });
}

const MIME = { '.html':'text/html; charset=utf-8', '.js':'text/javascript; charset=utf-8',
  '.css':'text/css; charset=utf-8', '.svg':'image/svg+xml', '.json':'application/json',
  '.ico':'image/x-icon', '.woff2':'font/woff2' };

// Static assets are ONLY public UI files. Serving-root is the UI dir; anything
// that could be a private key / cert / credential / secret is refused BEFORE the
// allowlist, so even a mis-placed secret.key or a cert path never crosses HTTP.
const SENSITIVE_EXT = ['.key','.pem','.crt','.cer','.der','.p12','.pfx','.jks','.env','.keytab','.sqlite','.db'];
const SENSITIVE_NAME = /(^|[._/])(secret|private|id_rsa|id_ed25519|credentials?|token|passwd|shadow|serverkey|server_key|ca[_-]?key)/i;
function serveUI(req, res, urlPath) {
  let rel = urlPath === '/' ? '/index.html' : urlPath;
  // Reject any raw traversal attempt outright (normalized path is checked too).
  if (rel.includes('..')) { res.writeHead(404, {'Content-Type':'text/plain'}).end('not found'); return; }
  const file = path.normalize(path.join(UI_DIR, rel));
  if (file !== UI_DIR && !file.startsWith(UI_DIR + path.sep)) { res.writeHead(403).end('forbidden'); return; }
  const base = path.basename(file);
  const ext = path.extname(file).toLowerCase();
  // Dotfiles (e.g. .env, .git) and any sensitive key/cert/credential are refused.
  // The name guard runs on the REQUESTED path, never on the absolute serving root: the
  // install location of the product is not caller input, and testing it here made every
  // UI that lives under a directory named like a secret (…/private/…) answer 404 while
  // the unit — whose UI dir sat outside that word — stayed green. Measured twice on the
  // AI Factory front: "/", "/app.js" and "/ahp-live.js" were all 404 for this reason.
  // Traversal is still refused twice over ('..' check + the UI_DIR prefix check above),
  // so guarding `rel` keeps the property: a caller cannot name a secret, wherever the UI
  // happens to be installed.
  if (base.startsWith('.') || SENSITIVE_EXT.includes(ext) || SENSITIVE_NAME.test(base) || SENSITIVE_NAME.test(rel)) {
    res.writeHead(404, {'Content-Type':'text/plain'}).end('not found'); return;
  }
  // Final gate: serve only allowlisted static UI asset extensions.
  if (!(ext in MIME)) { res.writeHead(404, {'Content-Type':'text/plain'}).end('not found'); return; }
  fs.readFile(file, (err, buf) => {
    if (err) { res.writeHead(404, {'Content-Type':'text/plain'}).end('not found'); return; }
    res.writeHead(200, {
      'Content-Type': MIME[path.extname(file)] || 'application/octet-stream',
      'Cache-Control': 'no-store',
      'X-Content-Type-Options': 'nosniff',
    }).end(buf);
  });
}

async function proxyToUpstream(req, res, targetPath) {
  const headers = {};
  for (const [k, v] of Object.entries(req.headers)) {
    if (['host','connection','content-length','transfer-encoding'].includes(k.toLowerCase())) continue;
    headers[k] = v;
  }
  headers['host'] = up.host;
  // The proxy is the trusted same-origin frontend; it asserts the CSRF marker the
  // browser surface requires on state-changing verbs (double-submit is redundant
  // because only this origin forwards to the backend).
  if (req.method !== 'GET' && req.method !== 'HEAD') headers['x-ahp-session'] = '1';
  // human session cookie is forwarded to the backend; nothing else is disclosed.
  const upstreamReq = (USE_TLS ? https : http).request(
    { protocol: up.protocol, host: up.hostname, port: up.port || (USE_TLS?443:80),
      path: targetPath, method: req.method, headers, agent },
    (upRes) => {
      const out = {};
      for (const [k, v] of Object.entries(upRes.headers)) {
        if (k.toLowerCase() === 'set-cookie') {
          // Force HttpOnly + SameSite=Strict on the browser session cookie.
          out['set-cookie'] = [].concat(v).map((c) => {
            let s = c.replace(/;\s*HttpOnly/ig,'').replace(/;\s*Secure/ig, USE_TLS?'; Secure':'');
            if (!/samesite/ig.test(s)) s += '; SameSite=Strict';
            return s + '; HttpOnly';
          });
          continue;
        }
        out[k] = v;
      }
      res.writeHead(upRes.statusCode || 502, out);
      upRes.pipe(res); // unbuffered: SSE/text/event-stream streams through live
    });
  upstreamReq.on('error', (e) => {
    if (!res.headersSent) res.writeHead(502, {'Content-Type':'application/json'});
    res.end(JSON.stringify({ error:'upstream_unreachable' }));
  });
  req.pipe(upstreamReq);
}

const server = http.createServer((req, res) => {
  const u = new URL(req.url, 'http://x');
  const p = u.pathname;
  if (p === '/healthz') { res.writeHead(200, {'Content-Type':'application/json'}).end(JSON.stringify({status:'ok', role:'human-origin'})); return; }
  if (FORWARDED_PREFIXES.some((pre) => p === pre || p.startsWith(pre))) {
    proxyToUpstream(req, res, p + (u.search || ''));
    return;
  }
  serveUI(req, res, p);
});
const [__host, __port] = LISTEN.split(':');
server.listen(Number(__port), __host, () => {
  const a = server.address();
  const bound = typeof a === 'object' && a ? `${a.address}:${a.port}` : LISTEN;
  console.error(JSON.stringify({msg:'human-origin listening', listen:bound, upstream:USE_TLS?'mTLS-backend':'loopback-backend', ui:UI_DIR}));
});
