#!/usr/bin/env node
// delivery lane: LIVE isolated proof of the human-origin (:ORIGIN -> :BACKEND).
//
// ENV-ONLY (root directive 2026-10-30): NO `docker exec`, NO hardcoded :25432 / :5432
// / password. The PG endpoint + creds come from the process env, exactly like the VM
// harness, so the SAME script starts against the handed-off isolated acceptance-pg
// (VM loopback :25495 or any endpoint) without a single edit. Resolution order:
//   1. AHP_DELIVERY_ENV_FILE (optional 0600, git-ignored env file) — sourced first.
//   2. AHP_TEST_DATABASE_URL (preferred) or AHP_DATABASE_URL (full DSN).
//   3. Discrete parts AHP_PG_HOST / AHP_PG_PORT / AHP_PG_DB / AHP_PG_USER /
//      AHP_PG_PASSWORD / AHP_PG_SSLMODE (default disable).
// The DB name MUST match ^ahp_hive_[a-z][a-z0-9_]{0,23}$ (launcher allowlist).
// The password is NEVER embedded in the exported DSN and NEVER printed.
//
// Steps: authenticated DB probe (real Go/pgx ping via the binary) -> start backend +
// origin -> provision two demo users (from AHP_TEST_USERS_JSON; ephemeral demo creds)
// -> REAL login + /me THROUGH the origin -> assert HttpOnly + SameSite=Strict and
// that /me.subject is the logged-in subject. Exits non-zero on any fail. No secret
// is ever printed.
import { spawn, execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';

const BACKEND = process.env.AHP_TEST_BACKEND_ADDR || '127.0.0.1:29700';
const ORIGIN  = process.env.AHP_TEST_ORIGIN_ADDR  || '127.0.0.1:29710';
const BIN = process.env.AHP_ISO_BIN || 'private/composed-backend/bin/ahp-full-iso29700-0633f33625ad';
const DB_ALLOW = /^ahp_hive_[a-z][a-z0-9_]{0,23}$/;
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
function envSeedIsSet(v){ return !!v && v.length>0; }

function loadEnvFile(p){ const o={}; for(const raw of readFileSync(p,'utf8').split(/\r?\n/)){ const l=raw.trim(); if(!l||l.startsWith('#'))continue; const i=l.indexOf('='); if(i<=0)continue; let v=l.slice(i+1).trim(); if((v[0]==='"'&&v.endsWith('"'))||(v[0]==="'"&&v.endsWith("'")))v=v.slice(1,-1); o[l.slice(0,i).trim()]=v; } return o; }

function resolvePg(){
  const env = { ...process.env };
  if (env.AHP_DELIVERY_ENV_FILE){ const p=resolve(env.AHP_DELIVERY_ENV_FILE); if(!existsSync(p)) throw new Error('AHP_DELIVERY_ENV_FILE set but unreadable'); Object.entries(loadEnvFile(p)).forEach(([k,v])=>{ if(env[k]===undefined||env[k]==='')env[k]=v; }); }
  const full = env.AHP_TEST_DATABASE_URL || (/^postgres(ql)?:\/\//.test(env.AHP_DATABASE_URL||'') ? env.AHP_DATABASE_URL : '');
  let host='', port='', db='', user='', pass='', sslmode='';
  if (full){
    const u=new URL(full); host=u.hostname; port=u.port; db=u.pathname.replace(/^\//,'');
    user=decodeURIComponent(u.username||''); pass=decodeURIComponent(u.password||'');
    sslmode=u.searchParams.get('sslmode')||'';
  }
  if(env.AHP_PG_HOST)host=env.AHP_PG_HOST; if(env.AHP_PG_PORT)port=env.AHP_PG_PORT;
  if(env.AHP_PG_DB)db=env.AHP_PG_DB; if(env.AHP_PG_USER)user=env.AHP_PG_USER;
  if(env.AHP_PG_PASSWORD)pass=env.AHP_PG_PASSWORD; if(env.AHP_PG_SSLMODE)sslmode=env.AHP_PG_SSLMODE;
  if(env.AHP_DB_NAME)db=env.AHP_DB_NAME;
  sslmode=sslmode||'disable';
  const die=(m)=>{ throw new Error('PG_CONFIG: '+m); };
  if(!host)die('host unset (AHP_PG_HOST or full AHP_TEST_DATABASE_URL); no default endpoint');
  if(!port)die('port unset (AHP_PG_PORT or full AHP_TEST_DATABASE_URL); no default :5432/:25432');
  if(!db)die('db unset (AHP_PG_DB or AHP_DB_NAME)');
  if(!DB_ALLOW.test(db))die(`db "${db}" not in allowlist ^ahp_hive_[a-z][a-z0-9_]{0,23}$`);
  if(!user)die('user unset (AHP_PG_USER)');
  if(!pass)die('password unset (AHP_PG_PASSWORD or AHP_DELIVERY_ENV_FILE)');
  const enc=u=>encodeURIComponent(u).replace(/%3A/gi,':');
  // Passwordless canonical URL (safe to log); pgx reads PGPASSWORD from env.
  const url=`postgres://${encodeURIComponent(user)}@${host}:${port}/${db}?sslmode=${sslmode}`;
  return { url, pass, host, port, db, user, sslmode, enc };
}

let pg; try { pg=resolvePg(); } catch(e){ console.error('FAIL:', e.message); process.exit(2); }
console.log(`PG_RESOLVED host=${pg.host} port=${pg.port} db=${pg.db} user=${pg.user} sslmode=${pg.sslmode} (password from env, not printed)`);

const base={ ...process.env, NO_PROXY:'localhost,127.0.0.1,::1' };
delete base.HTTP_PROXY; delete base.http_proxy; delete base.HTTPS_PROXY; delete base.https_proxy;
const dbEnv={ ...base, AHP_DATABASE_URL:pg.url, AHP_TEST_DATABASE_URL:pg.url, PGPASSWORD:pg.pass, AHP_DB_NAME:pg.db };

// Authenticated probe: the ahp binary pings the pool for every role before dispatch;
// a provision role with no users fails AFTER a successful authed ping, so absence of a
// ping/pool error proves credentials authenticate. Classifies 28P01 (auth) vs refused.
function classifyProbe(errText){
  if(/password authentication failed|28P01|no password supplied/i.test(errText)) return {kind:'AUTH_FAIL_28P01', hint:'wrong credential/role for THIS endpoint; a port swap does NOT fix it'};
  if(/could not connect|connection refused|no such file|connect econnrefused/i.test(errText)) return {kind:'CONN_REFUSED', hint:'endpoint not listening (wrong host/port or tunnel down)'};
  if(/timeout/i.test(errText)) return {kind:'CONN_TIMEOUT'};
  return {kind:'OTHER'};
}
try{
  execFileSync(BIN, ['--role','provision-browser-users'], { env:{...dbEnv, AHP_ROLE:'provision-browser-users'}, stdio:['ignore','ignore','pipe'] });
}catch(e){
  const t=String(e.stderr||e.message||'');
  if(/database (ping|connection pool) failed/i.test(t)){ const c=classifyProbe(t); console.error(`DB_PROBE=${c.kind} host=${pg.host} port=${pg.port} ${c.hint||''}`.trim()); process.exit(c.kind==='AUTH_FAIL_28P01'?28:11); }
  // No ping/pool failure => authed ping succeeded (provision then failed on empty users).
}
console.log(`DB_PROBE=auth_ok client=go host=${pg.host} port=${pg.port} db=${pg.db}`);

const users = JSON.parse(process.env.AHP_TEST_USERS_JSON || '[]');
if (users.length<2){ console.error('GATE_FAIL: set AHP_TEST_USERS_JSON with >=2 demo users [{username,password,subject,tenant,...}]'); process.exit(2); }

// AHP_DEV_JWT_SEED must be a STABLE 32-byte dev-only key: with it unset, devAuth()
// and devIdentityAuth() each derive a fresh RANDOM Ed25519 key, so browser tokens
// minted by the identity signer fail the state verifier ("not a valid browser
// token"). Ephemeral/dev-only (never a production secret); same posture as the
// durable composed-backend which sets a deterministic seed. Override via env.
if (!envSeedIsSet(process.env.AHP_DEV_JWT_SEED)) dbEnv.AHP_DEV_JWT_SEED = createHash('sha256').update('ahp-hive-delivery-iso-dev-jwt-seed-v1').digest('hex');
execFileSync(BIN, [], { env:{...dbEnv, AHP_ROLE:'provision-browser-users', AHP_PROVISION_USERS_JSON:JSON.stringify(users)}, stdio:'ignore' });
const be=spawn(BIN,[],{env:{...dbEnv, AHP_ENABLE_BROWSER:'1', AHP_ROLE:'all', AHP_LISTEN:BACKEND, AHP_ENABLE_DATA:process.env.AHP_ENABLE_DATA||'0', AHP_ENABLE_EXECUTION:process.env.AHP_ENABLE_EXECUTION||'0'}});
const or=spawn('node',['scripts/human-origin.mjs'],{env:{...dbEnv, AHP_HUMAN_ORIGIN_LISTEN:ORIGIN, AHP_HUMAN_ORIGIN_UPSTREAM:`http://${BACKEND}`, AHP_HUMAN_ORIGIN_UI:process.env.AHP_HUMAN_ORIGIN_UI||'web/static'}});
// Readiness wait: the backend runs state/identity/model migrations before it binds,
// so a fixed sleep races it (origin is up before the upstream is). Poll the backend
// directly until /healthz is 200 (fail the run if it never comes up).
let ready=false;
for(let i=0;i<60;i++){ try{ const h=await fetch(`http://${BACKEND}/healthz`); if(h.status===200){ ready=true; break; } }catch(_){} await sleep(500); }
if(!ready){ console.error('FAIL: backend never became ready on '+BACKEND); be.kill('SIGTERM'); or.kill('SIGTERM'); process.exit(2); }
await sleep(300);
let fails=0; const chk=(n,c)=>{ console.log(`${c?'PASS':'FAIL'}: ${n}`); if(!c) fails++; };
const H=`http://${ORIGIN}`;
try{
  const health=await fetch(`${H}/healthz`); chk('origin /healthz 200', health.status===200);
  const u0=users[0];
  const r=await fetch(`${H}/u-ahp/v1/auth/login`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:u0.username,password:u0.password})});
  chk('origin login 200', r.status===200);
  const sc=r.headers.getSetCookie?r.headers.getSetCookie():[r.headers.get('set-cookie')];
  const joined=sc.join(';');
  chk('session cookie HttpOnly', /httponly/i.test(joined));
  chk('session cookie SameSite=Strict', /samesite=strict/i.test(joined));
  const cookie=(sc.map(c=>c.split(';')[0]).join('; '));
  // whoami is a Bearer-resource: the session cookie mints an access token via
  // POST /auth/token, then GET /me/user carries it. /me/user with cookie-only MUST
  // be 401 (correct fail-closed), which we assert too so the gate is non-hollow.
  const neg=await fetch(`${H}/u-ahp/v1/me/user`,{headers:{Cookie:cookie}});
  chk('/me/user cookie-only is 401 (fail-closed: Bearer required)', neg.status===401);
  const tokR=await fetch(`${H}/u-ahp/v1/auth/token`,{method:'POST',headers:{'Content-Type':'application/json','x-ahp-session':'1',Cookie:cookie}});
  const tok=tokR.status===200?await tokR.json():{};
  chk('auth/token mints access_token', tokR.status===200 && !!tok.access_token);
  const me=await fetch(`${H}/u-ahp/v1/me/user`,{headers:{Authorization:`Bearer ${tok.access_token||''}`}});
  const bod=me.status===200?await me.json():{};
  if(me.status!==200) console.log(`DEBUG /me/user status=${me.status} body=${JSON.stringify(await me.clone().text()).slice(0,180)}`);
  chk('origin /me/user (Bearer) 200', me.status===200);
  chk('me.subject is the logged-in subject', bod.subject===u0.subject);
}catch(e){ console.error('probe error', e.message); fails++; }
finally{ be.kill('SIGTERM'); or.kill('SIGTERM'); }
console.log(fails===0?'LIVE_HUMAN_ORIGIN_PASS':'LIVE_HUMAN_ORIGIN_FAIL'); process.exit(fails===0?0:1);
