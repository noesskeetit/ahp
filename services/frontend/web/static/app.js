"use strict";

// Minimal, honest Space API client. Talks only to real /v1/space/* operations.
// No mock responses: every rendered line comes from a server round-trip.
//
// Wire contract (locked by tests/space/handler_http_test.go): every list/read
// op returns an OBJECT envelope, not a bare array. ReadStream => {events:[...]}
// with per-event fields {kind, sequence, payload} (NOT "seq").

const last = { A: null, B: null }; // per-subject { turnID, threadID, after }
const seenArtifacts = new Set();   // dedupe artifact rows by artifact_id

function subjectConfig(s) {
  const box = document.querySelector(`fieldset[data-subject="${s}"]`);
  const url = box.querySelector('[data-field="url"]').value.trim().replace(/\/$/, "");
  const token = box.querySelector('[data-field="token"]').value.trim();
  return { url, token };
}

async function call(s, op, body) {
  const { url, token } = subjectConfig(s);
  if (!url) throw new Error(`Subject ${s}: base URL is required`);
  const headers = { "Content-Type": "application/json" };
  if (token) headers["Authorization"] = "Bearer " + token;
  const resp = await fetch(`${url}/v1/space/${op}`, {
    method: "POST",
    headers,
    body: JSON.stringify(body || {}),
  });
  const text = await resp.text();
  let data;
  try { data = text ? JSON.parse(text) : null; } catch { data = text; }
  if (!resp.ok) {
    const code = data && data.code ? data.code : `HTTP ${resp.status}`;
    throw new Error(`${op}: ${code}`);
  }
  return data;
}

// chatContext returns a context id Artifacts accepts: no "/", bounded, stable per
// subject for the lifetime of the page, so deleting the chat later finds exactly this
// conversation's artifacts.
const chatContexts = {};
function chatContext(s) {
  if (!chatContexts[s]) {
    chatContexts[s] = "ui-" + s.toLowerCase() + "-" + Date.now().toString(36) +
      "-" + Math.random().toString(36).slice(2, 10);
  }
  return chatContexts[s];
}

function log(line) {
  const el = document.getElementById("log");
  el.textContent += line + "\n";
  el.scrollTop = el.scrollHeight;
}

function renderThreadEvent(kind, payload) {
  const el = document.getElementById("thread");
  const div = document.createElement("div");
  div.className = "ev ev-" + kind;
  let text = `[${kind}] `;
  try { text += typeof payload === "string" ? payload : JSON.stringify(payload); }
  catch { text += String(payload); }
  div.textContent = text.slice(0, 500);
  el.appendChild(div);
  el.scrollTop = el.scrollHeight;
}

// renderArtifacts surfaces artifact metadata the tool step genuinely produced
// (id + sha256 + mime) and its provenance (e.g. tool=emil.delegate). It shows
// real data the Space stream already carries; it does NOT fabricate a download
// endpoint (file bytes are served by the Data contract, wired separately).
// toolFrameError pulls a refusal out of a tool frame's content. Core turns a tool
// failure into {"error": "..."} fed back to the model, so this is the only place a
// person can see it before the turn reports `done`.
function toolFrameError(content) {
  const c = content && typeof content === "object" ? content : null;
  if (!c) return "";
  if (typeof c.error === "string" && c.error.trim()) return c.error.trim();
  if (Array.isArray(c.artifacts) === false && Array.isArray(c.tool_results)) {
    for (const r of c.tool_results) {
      if (r && typeof r.error === "string" && r.error.trim()) return r.error.trim();
    }
  }
  return "";
}

const toolErrors = {};
function noteToolError(s, message) {
  toolErrors[s] = (toolErrors[s] || 0) + 1;
  const panel = document.getElementById("artifacts");
  const div = document.createElement("div");
  div.className = "artifact artifact-error";
  div.textContent = `отказ инструмента: ${message}`;
  panel.appendChild(div);
  log(`${s}: отказ инструмента — ${message}`);
}

// renderArtifacts surfaces artifact metadata the tool step genuinely produced and
// offers the ONE download path the product allows: resolve the digest the frame names
// into (artifact_id, exact sealed version), then download that exact version. The
// human edge refuses "latest", so the button resolves first and only then links.
async function downloadByName(s, sha256) {
  const { url, token } = subjectConfig(s);
  const headers = token ? { Authorization: "Bearer " + token } : {};
  const resolved = await fetch(`${url}/u-ahp/v1/artifacts/Resolve?sha256=${encodeURIComponent(sha256)}`, { headers });
  if (!resolved.ok) {
    const body = await resolved.text();
    throw new Error(`Resolve HTTP ${resolved.status}: ${body.slice(0, 160)}`);
  }
  const data = await resolved.json();
  const v = (data.versions || [])[0];
  if (!v || !v.artifact_id || !v.version) throw new Error("Resolve answered without an exact version");
  const dl = `${url}/u-ahp/v1/artifacts/download?artifact_id=${encodeURIComponent(v.artifact_id)}&version=${encodeURIComponent(v.version)}`;
  // The download route authenticates by bearer, which a plain <a href> cannot carry,
  // so the bytes are fetched with the same credential and handed over as a blob URL.
  const got = await fetch(dl, { headers });
  if (!got.ok) {
    const body = await got.text();
    throw new Error(`download HTTP ${got.status}: ${body.slice(0, 160)}`);
  }
  const bytes = new Uint8Array(await got.arrayBuffer());
  const mime = v.mime || "application/octet-stream";
  const blobUrl = URL.createObjectURL(new Blob([bytes], { type: mime }));
  const a = document.createElement("a");
  a.href = blobUrl;
  a.download = v.name || v.artifact_id;
  a.className = "artifact-download";
  a.dataset.sha256 = v.sha256 || sha256;
  a.dataset.size = String(bytes.length);
  a.textContent = `Скачать ${v.name || v.artifact_id} (${bytes.length} Б)`;
  document.getElementById("artifacts").appendChild(a);
  return dl;
}

// artifactFrames normalizes what a tool step actually carried. Measured on the live
// stream, save_artifact's frame puts the artifact IN content ({id, sha256, size,
// version}), while a batch step nests a list under content.artifacts. Reading only the
// second shape is how a produced artifact stayed invisible in the UI.
function artifactFrames(content) {
  if (!content || typeof content !== "object") return [];
  if (Array.isArray(content.artifacts)) return content.artifacts;
  if (content.artifact && typeof content.artifact === "object") return [content.artifact];
  if (content.sha256 && (content.id || content.artifact_id)) return [content];
  return [];
}

function renderArtifacts(s, content, provenance) {
  const artifacts = artifactFrames(content);
  if (artifacts.length === 0) return;
  const panel = document.getElementById("artifacts");
  for (const a of artifacts) {
    const id = a.artifact_id || a.id || a.sha256;
    if (!id || seenArtifacts.has(id)) continue;
    seenArtifacts.add(id);
    const div = document.createElement("div");
    div.className = "artifact";
    const prov = provenance && provenance.tool ? ` · via ${provenance.tool}` : "";
    const name = a.name || a.filename || "";
    div.textContent = `artifact ${id}${name ? " · " + name : ""}${a.mime_type ? " · " + a.mime_type : ""}${a.sha256 ? " · sha256:" + String(a.sha256).slice(0, 12) + "…" : ""}${prov}`;
    panel.appendChild(div);
    if (a.sha256) {
      const row = document.createElement("div");
      row.className = "artifact";
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "artifact-download-btn";
      btn.textContent = "Скачать по digest";
      btn.onclick = () => {
        btn.disabled = true;
        downloadByName(s, a.sha256).catch((e) => {
          row.textContent = `скачивание недоступно: ${e.message}`;
          btn.disabled = false;
        });
      };
      row.appendChild(btn);
      panel.appendChild(row);
    }
  }
}

async function pollStream(s) {
  const st = last[s];
  if (!st) return;
  const data = await call(s, "ReadStream", { thread_id: st.threadID, after: st.after, limit: 100 });
  // Server returns the {events:[...]} envelope; iterate the inner array only.
  const events = (data && data.events) || [];
  for (const ev of events) {
    if (ev.sequence > st.after) st.after = ev.sequence;
    let payload = ev.payload;
    try { payload = JSON.parse(ev.payload); } catch {}
    if (ev.kind === "hitl") {
      document.getElementById("hitl-id").value = (payload && payload.hitl_id) || "";
    }
    renderThreadEvent(ev.kind, payload);
    if (ev.kind === "tool" && payload && payload.content) {
      // A failed tool call arrives as a tool frame whose content carries {"error":…}:
      // Core feeds that text back to the model instead of ending the run, so the turn
      // can still go `done`. Surfacing it here is the difference between a person
      // seeing "the artifact was refused" and seeing a silent success.
      const toolError = toolFrameError(payload.content);
      if (toolError) noteToolError(s, toolError);
      renderArtifacts(s, payload.content, payload.content.provenance);
    }
    if (ev.kind === "status") {
      const status = payload && payload.status;
      if (["done", "failed", "canceled"].includes(status)) {
        if (status === "done" && toolErrors[s] > 0) {
          log(`${s}: done, но ${toolErrors[s]} отказ(ов) инструмента в этом ходе — см. ниже артефактов может не быть`);
        }
        // The consumption of a finished turn belongs on the result card; the read is
        // fire-and-forget because an unavailable cost face must never change what the
        // turn itself says about its outcome.
        showCost(s, (last[s] && last[s].threadID) || (payload && payload.thread_id) || "");
        return true;
      }
    }
  }
  return false;
}

async function send() {
  const s = document.getElementById("subject").value;
  const content = document.getElementById("input").value;
  try {
    const acc = await call(s, "AcceptTurn", {
      key: "ui-" + Date.now(),
      // Artifacts names a context with [A-Za-z0-9][A-Za-z0-9_.:@-]{0,127}: a "/" is
      // refused, and a refused save_artifact used to end the turn `done`. One chat
      // context per browser session, stable across turns of the same thread.
      context_id: chatContext(s),
      input: { role: "user", content },
      max_steps: 8,
    });
    last[s] = { turnID: acc.turn_id, threadID: acc.thread_id, after: 0 };
    seenArtifacts.clear();
    log(`${s}: accepted turn=${acc.turn_id} thread=${acc.thread_id}`);
    await call(s, "DispatchTurn", {});
    for (let i = 0; i < 240; i++) {
      if (await pollStream(s)) break;
      await new Promise((r) => setTimeout(r, 500));
    }
    document.getElementById("input").value = "";
  } catch (e) { log(`${s}: ${e.message}`); }
}

async function lastOp(op) {
  const s = document.getElementById("subject").value;
  const st = last[s];
  if (!st) { log(`${s}: no active turn`); return; }
  try {
    if (op === "CancelTurn") await call(s, "CancelTurn", { turn_id: st.turnID });
    else await call(s, "Steer", { turn_id: st.turnID, instruction: document.getElementById("input").value || "reconsider" });
    log(`${s}: ${op} sent for ${st.turnID}`);
  } catch (e) { log(`${s}: ${e.message}`); }
}

async function respond(approved) {
  const s = document.getElementById("subject").value;
  const st = last[s] || {};
  const id = document.getElementById("hitl-id").value.trim();
  if (!id) { log(`${s}: hitl_id required`); return; }
  try {
    await call(s, "RespondHITL", { hitl_id: id, approved, thread_id: st.threadID, response: { approved } });
    log(`${s}: RespondHITL ${id} approved=${approved}`);
  } catch (e) { log(`${s}: ${e.message}`); }
}

document.getElementById("send").addEventListener("click", send);
document.getElementById("steer").addEventListener("click", () => lastOp("Steer"));
document.getElementById("cancel").addEventListener("click", () => lastOp("CancelTurn"));
document.getElementById("approve").addEventListener("click", () => respond(true));
document.getElementById("reject").addEventListener("click", () => respond(false));
document.getElementById("list").addEventListener("click", async () => {
  const s = document.getElementById("subject").value;
  try { log(`${s}: ListTurns ${JSON.stringify(await call(s, "ListTurns", {}))}`); }
  catch (e) { log(`${s}: ${e.message}`); }
});
document.getElementById("recover").addEventListener("click", async () => {
  const s = document.getElementById("subject").value;
  try { log(`${s}: Recover ${JSON.stringify(await call(s, "Recover", {}))}`); }
  catch (e) { log(`${s}: ${e.message}`); }
});
document.getElementById("clear").addEventListener("click", () => {
  document.getElementById("log").textContent = "";
});

// showCost surfaces what the turn consumed (PDF rev3.2 :347 "стоимость задачи"). It reads
// the human cost route, which exchanges the browser's own session for the scoped task token
// and asks the Model Gateway about the task THIS thread's run charged -- a browser neither
// names a task nor holds a workload certificate, so it cannot read that face directly.
// The numbers are the ledger's own fields, passed through: nothing here adds a unit, a total
// or a zero. No answer means no line: a cost the ledger never produced must not appear on
// screen, and a total across more than one denomination would be a second denomination
// invented here (the gateway flags that case in `units`).
async function showCost(s, threadID) {
  if (!threadID) return;
  const { url, token } = subjectConfig(s);
  const headers = token ? { Authorization: "Bearer " + token } : {};
  let res;
  try {
    res = await fetch(`${url}/u-ahp/v1/model/Cost?thread_id=${encodeURIComponent(threadID)}`, { headers });
  } catch (e) {
    return;
  }
  if (!res.ok) return;
  const data = await res.json().catch(() => null);
  const trees = (data && Array.isArray(data.trees)) ? data.trees : [];
  if (!trees.length) return;
  const panel = document.getElementById("artifacts");
  if (!panel) return;
  for (const t of trees) {
    const div = document.createElement("div");
    div.className = "cost";
    div.dataset.treeRoot = t.tree_root || "";
    div.dataset.unit = t.unit || "";
    div.dataset.spent = String(t.spent ?? "");
    div.dataset.limit = String(t.limit ?? "");
    div.dataset.tokensIn = String(t.tokens_in ?? "");
    div.dataset.tokensOut = String(t.tokens_out ?? "");
    div.dataset.tokensTotal = String(t.tokens_total ?? "");
    div.dataset.generations = String(t.generations ?? "");
    div.dataset.budgetKey = (t.root && t.root.budget_key) || "";
    const spent = Number(t.spent || 0);
    const limit = Number(t.limit || 0);
    const ofLimit = limit > 0 ? ` из ${limit}` : "";
    // The ledger denominates a run tree in steps and a task tree in tokens; the screen
    // names whichever one the row carries, and always shows the token observations so a
    // person can see that they are zero rather than guess. No currency symbol appears
    // anywhere: the cost API answers tokens, and an invented money number would be a
    // second ledger.
    const tokenPart = `токены: in ${t.tokens_in ?? 0} / out ${t.tokens_out ?? 0} / Σ ${t.tokens_total ?? 0} (генераций: ${t.generations ?? 0})`;
    div.textContent = `Стоимость хода · единица реестра: ${t.unit || "?"} · списано ${spent}${ofLimit}` +
      (t.exhausted ? " — лимит выбран" : ` (остаток ${t.available_to_root ?? t.remaining ?? "?"})`) +
      ` · ${tokenPart}`;
    panel.appendChild(div);
  }
  if ((data.units || []).length > 1) log(`${s}: в дереве несколько denomination (${data.units.join(", ")}) — суммировать их нельзя`);
}

// --- Developer / Admin surfacing (real /v1/space/* only; no mock) ---
// GetTurn returns the server's authoritative snapshot for the caller's own turn.
async function getTurn() {
  const s = document.getElementById("subject").value;
  const st = last[s];
  if (!st) { log(`${s}: no active turn`); return; }
  try {
    const snap = await call(s, "GetTurn", { turn_id: st.turnID });
    log(`${s}: GetTurn ${st.turnID} => ${JSON.stringify(snap).slice(0, 400)}`);
  } catch (e) { log(`${s}: GetTurn ${e.message}`); }
}

// Two-subject isolation probe: using the CURRENT subject's bearer, attempt to read
// the OTHER subject's last turn through the real API. The Space handler answers a
// cross-scope GetTurn with NOT_FOUND (never FORBIDDEN), so a foreign subject can
// not even learn the turn exists. A successful read here is a real privacy leak and
// is reported loudly; the expected outcome is the server refusing with NOT_FOUND.
async function isolationProbe() {
  const s = document.getElementById("subject").value;
  const other = s === "A" ? "B" : "A";
  const theirs = last[other];
  if (!theirs || !theirs.turnID) {
    log(`isolation: run a turn as ${other} first, then probe from ${s}`);
    return;
  }
  try {
    await call(s, "GetTurn", { turn_id: theirs.turnID });
    log(`isolation LEAK: ${s} read ${other}'s turn ${theirs.turnID} (expected NOT_FOUND)`);
  } catch (e) {
    if (e.message.includes("NOT_FOUND")) {
      log(`isolation OK: ${s} -> GetTurn(${other} turn) refused NOT_FOUND`);
    } else {
      log(`isolation: ${s} -> GetTurn(${other} turn) => ${e.message}`);
    }
  }
}

document.getElementById("getturn").addEventListener("click", getTurn);
document.getElementById("isoprobe").addEventListener("click", isolationProbe);
