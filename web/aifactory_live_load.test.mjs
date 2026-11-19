// Регресс DEF-H28-1: живой слой обязан переживать собственную загрузку.
//
// Измеренный дефект: в web/aifactory/ahp-live.js объект экспорта ссылался на
// несуществующий идентификатор reviewMemory. ReferenceError возникал на этапе
// вычисления скрипта, поэтому window.AHP_LIVE не присваивался вообще, и весь
// живой слой прототипа был мёртв (ни одной живой панели) при полностью
// исправном бэкенде. Статический синтаксис (node --check) этого не ловит.
//
// Тест воспроизводит загрузку так же, как браузер: берёт порядок <script> из
// index.html и вычисляет файлы одним скриптом в песочнице со стабом DOM.
// Дополнительно проверяется, что ни один экспорт api не является «фантомом»
// (имя есть, функции нет) и что при загрузке не трогается хранилище страницы.
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import vm from "node:vm";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.join(path.dirname(fileURLToPath(import.meta.url)), "aifactory");

function scriptOrder() {
  const html = readFileSync(path.join(here, "index.html"), "utf8");
  return [...html.matchAll(/<script\s+src="([^"]+)"/g)].map((m) => m[1]);
}

function elementStub() {
  const node = {
    style: {}, dataset: {}, children: [], value: "", textContent: "", innerHTML: "",
    classList: { add() {}, remove() {}, toggle() {}, contains: () => false },
    setAttribute() {}, getAttribute: () => null, removeAttribute() {},
    addEventListener() {}, removeEventListener() {},
    querySelector: () => null, querySelectorAll: () => [],
    contains: () => false,
    appendChild() {}, append() {}, prepend() {}, insertBefore() {},
    insertAdjacentHTML() {}, before() {}, after() {},
    remove() {}, focus() {}, click() {}, closest: () => null, scrollIntoView() {},
    offsetHeight: 0, firstChild: null,
  };
  return node;
}

function load(fetchImpl = () => new Promise(() => {})) {
  const order = scriptOrder();
  assert.ok(order.length >= 2, "index.html не объявляет скрипты");
  assert.ok(order.includes("app.js"), "index.html не подключает app.js");
  assert.ok(order.includes("ahp-live.js"), "index.html не подключает ahp-live.js");
  assert.ok(order.indexOf("app.js") < order.indexOf("ahp-live.js"),
    "ahp-live.js обязан загружаться после app.js");

  const doc = {
    readyState: "complete", title: "",
    documentElement: elementStub(), body: elementStub(), head: elementStub(),
    getElementById: () => elementStub(), querySelector: () => elementStub(),
    querySelectorAll: () => [], createElement: () => elementStub(),
    createTextNode: () => elementStub(), addEventListener() {}, removeEventListener() {},
  };
  const storageTrip = (name) => ({
    getItem() { throw new Error(`${name} трогается при загрузке страницы`); },
    setItem() { throw new Error(`${name} трогается при загрузке страницы`); },
    removeItem() { throw new Error(`${name} трогается при загрузке страницы`); },
    clear() { throw new Error(`${name} трогается при загрузке страницы`); },
  });
  const ctx = {
    document: doc,
    location: { search: "", pathname: "/", hash: "" },
    history: { replaceState() {} },
    // Загрузка намеренно изолируется от сетевого пробa boot(): он проверяет
    // живое состояние панели, а не семантику загрузки скрипта. Нерезолвившийся
    // промис убирает пост-тестовую асинхронную активность (и ложный fail).
    fetch: fetchImpl,
    URLSearchParams, FormData, AbortController, URL,
    EventSource: class {},
    crypto: globalThis.crypto, TextEncoder, Math, Date, JSON, Promise, console,
    setTimeout, clearTimeout, setInterval: () => 0, clearInterval() {},
    requestAnimationFrame: (f) => setTimeout(f, 0),
    btoa: globalThis.btoa, atob: globalThis.atob,
    navigator: { userAgent: "node-test" },
    alert() {}, confirm: () => true, prompt: () => null, open: () => null, scrollTo() {},
    addEventListener() {}, removeEventListener() {},
    matchMedia: () => ({ matches: false, addEventListener() {} }),
    localStorage: storageTrip("localStorage"),
    sessionStorage: storageTrip("sessionStorage"),
  };
  ctx.window = ctx;
  ctx.self = ctx;
  vm.createContext(ctx);
  const source = order.map((name) => readFileSync(path.join(here, name), "utf8")).join("\n;\n");
  vm.runInContext(source, ctx, { filename: "aifactory/index.html" });
  return ctx;
}

test("page scripts evaluate and publish the live layer (DEF-H28-1)", () => {
  const ctx = load();
  assert.ok(ctx.window.AHP_LIVE, "window.AHP_LIVE не определён после загрузки index.html");
  for (const name of ["AHP_CANONICAL", "AHP_SHAPES"]) {
    assert.ok(ctx.window[name], `window.${name} не определён после загрузки index.html`);
  }
});

test("every exported api entry is a real function", () => {
  const ctx = load();
  const api = ctx.window.AHP_LIVE?.api;
  assert.ok(api && typeof api === "object", "AHP_LIVE.api не экспортирован");
  const names = Object.keys(api);
  assert.ok(names.length > 20, `подозрительно бедный экспорт: ${names.length}`);
  const phantoms = names.filter((n) => typeof api[n] !== "function");
  assert.deepEqual(phantoms, [], "экспортированные имена без реализации: " + phantoms.join(", "));
});

test("dispatcher hooks are covered by exported functions", () => {
  const ctx = load();
  const api = ctx.window.AHP_LIVE.api;
  // Точка перехвата диспетчера обязана существовать: через неё мок-действие
  // становится настоящим вызовом или честно отказывается.
  assert.equal(typeof ctx.window.AHP_LIVE.hook, "function", "AHP_LIVE.hook потерян");
  assert.equal(typeof api.raw, "function", "нет живого транспортного примитива raw()");
});

test("workspace editor opens files without treating owner-only membership as a failure", async () => {
  const ctx = load(async (path) => {
    if (!path.includes("/workspace/")) return new Promise(() => {});
    const op = path.split("/").at(-1);
    const status = op === "ListMembers" ? 403 : 200;
    const body = op === "GetWorkspace" ? {id:"project-id",name:"project"} :
      op === "ListMembers" ? {code:"FORBIDDEN"} : [];
    return {status, text:async()=>JSON.stringify(body),headers:{get:()=>""}};
  });
  ctx.document.querySelector = selector => selector === "#header" ? null : elementStub();
  Object.assign(ctx.AHP_LIVE.state.S,{online:true,exp:Date.now()+600000,subject:"editor"});
  await ctx.AHP_LIVE.api.openWorkspace(false,"project");
  assert.equal(ctx.AHP_LIVE.state.L.workspace.name,"project");
  assert.equal(ctx.AHP_LIVE.state.L.workspaceMembersAllowed,false);
  assert.equal(ctx.AHP_LIVE.state.L.plates.some(p=>p.code==="workspace_ListMembers"),false);
});

test("workspace membership service failure remains visible", async () => {
  const ctx = load(async (path) => {
    if (!path.includes("/workspace/")) return new Promise(() => {});
    const op = path.split("/").at(-1), status=op==="ListMembers"?503:200;
    return {status,text:async()=>JSON.stringify(op==="GetWorkspace"?{id:"p",name:"project"}:[]),headers:{get:()=>""}};
  });
  ctx.document.querySelector = selector => selector === "#header" ? null : elementStub();
  Object.assign(ctx.AHP_LIVE.state.S,{online:true,exp:Date.now()+600000,subject:"editor"});
  await ctx.AHP_LIVE.api.openWorkspace(false,"project");
  assert.equal(ctx.AHP_LIVE.state.L.plates.some(p=>p.code==="workspace_ListMembers"&&p.status===503),true);
});

test("new addressed conversation sends only its installation, existing conversation inherits its executor", async()=>{
 for(const existing of [false,true]) {
  let accepted;
  const ctx=load(async(path,options)=>{
   if(!path.endsWith('/AcceptTurn')) return new Promise(()=>{});
   accepted=JSON.parse(options.body);
   return {status:503,text:async()=>JSON.stringify({code:'UNAVAILABLE'}),headers:{get:()=>''}};
  });
  ctx.document.querySelector=selector=>selector==='#header'?null:elementStub();
  Object.assign(ctx.AHP_LIVE.state.S,{online:true,exp:Date.now()+600000,subject:'user'});
  if(existing) ctx.AHP_LIVE.state.L.chat={thread_id:'existing',context_id:'conversation',agent_installation_id:'original'};
  await ctx.AHP_LIVE.api.runTurn('Hello',{context_id:'conversation',agent_installation_id:'chosen',max_steps:4});
  assert.ok(accepted);
  if(existing) {assert.equal(accepted.thread_id,'existing');assert.equal(accepted.agent_installation_id,undefined);}
  else assert.equal(accepted.agent_installation_id,'chosen');
  for(const forbidden of ['version_id','endpoint','task_id','token','agent_config']) assert.equal(accepted[forbidden],undefined);
 }
});
