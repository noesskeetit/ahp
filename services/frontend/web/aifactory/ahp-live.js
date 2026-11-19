// Живой слой прототипа AI Factory: превращает макет в клиент настоящих API AHP.
//
// Правила, которые здесь обязательны (прямое решение заказчика + решения директора):
//   1) Metadata идёт через человеческие грани одного origin; bytes upload — presigned PUT: /u-ahp/v1/* и /v1/space/.
//      Машинного креденциала у браузера нет: scoped task token выдаёт раннер из верифицированной
//      человеческой сессии (internal/space/exchange.go: ExchangeMiddleware), принципал/tenant/
//      deployment берутся из проверенного scope, никогда из тела или заголовков запроса.
//   2) Ни токенов, ни паролей в браузере не хранится: ни в хранилище страницы, ни в её
//      персистентном хранилище. Bearer живёт в памяти вкладки и перевыпускается по
//      HttpOnly-куке сессии (web/aifactory_hygiene_test.go стережёт это по байтам бинаря).
//   3) Никаких симуляций и ложных успехов: если плоскость не смонтирована или путь не сведён,
//      показывается честная плашка с кодом причины и измеренным статусом ответа.
//   4) Стоимость показывается в ТОКЕНАХ (стоимость дерева задачи), как её отдаёт Model Gateway.
//   5) Скачивание артефакта идёт по точному digest, и контрольная сумма считается здесь же,
//      вне полосы: crypto.subtle по полученным байтам, сравнение с digest из кадра запуска.
(() => {
  // Capture authorization response once, then remove it before API calls or rendering.
  const oauthQuery = new URLSearchParams(location.search);
  let pendingOAuthCallback = oauthQuery.has("state") && (oauthQuery.has("code") || oauthQuery.has("error"))
    ? { state: oauthQuery.get("state"), code: oauthQuery.get("code") || "", error: oauthQuery.get("error") || "" } : null;
  if (pendingOAuthCallback) history.replaceState(null, "", location.pathname + "#/connections");

  const S = { bearer: "", exp: 0, subject: "", tenant: "", display: "", online: false };

  // Ход по умолчанию просит файл: артефакт создаёт инструмент сохранения артефактов,
  // и только тогда нога «скачать и сверить digest» имеет что измерять. Без этой просьбы
  // путь доходит до done без артефакта — что и есть измеренный DEF-H24-9.
  const DEFAULT_RUN_TEXT = "Создай файл result.txt с коротким текстом и сохрани его артефактом."

  // L — только то, что вернула платформа. Пустой массив здесь значит «платформа ничего
  // не вернула», а не «мы подставили демо-данные».
  const L = {
    catalog: null,
    registered: null,
    publish: null,
    verdict: null,
    turn: null,
    frames: [],
    cursor: 0,
    turns: [],
    turnHistory: [],
    streamState: "idle",
    artifact: null,
    cost: null,
    memories: null,
    memoryNote: "",
    memoryScope: "personal",
    memoryProject: "",
    projectMemoryProfile: "",
    memorySettings: null,
    memoryStatus: null,
    memoryOperations: [],
    triggers: null,
    chats: null,
    chat: null,
	activeAgentInstallation: "",
    streamDrift: [],
    rights: null,
    workspace: null,
    compute: null,
    plates: [],
    audit: [],
  };

  document.addEventListener("change", event => {
	if(event.target?.name==="ahp-agent-target") L.activeAgentInstallation=event.target.value;
    if(event.target?.id==="ahp-project-memory") L.projectMemoryProfile=event.target.value;
    if(event.target?.id==="ahp-mem-scope") {L.memoryScope=event.target.value;L.memoryOutcome=null;L.memorySettings=null;L.memoryStatus=null;paint();}
  });
  document.addEventListener("input",event=>{if(event.target?.id==="ahp-mem-project"){L.memoryProject=event.target.value;L.memorySettings=null;L.memoryStatus=null;L.memoryOutcome=null;L.memories=null;}});
  const nowISO = () => new Date().toISOString();

  // Формы ответов платформы разбирает отдельный модуль (web/aifactory/ahp-shapes.js):
  // он знает, что память лежит в hit.memory, у триггера нет поля id, а кадр —
  // {kind, sequence, payload}, а не {type, seq}. Здесь остаётся только транспорт,
  // состояние и отрисовка.
  const SHAPES = window.AHP_SHAPES;
  const hex64 = (s) => (String(s).match(/[0-9a-f]{64}/g) || []);

  function audit(code, detail) {
    L.audit.unshift({ t: nowISO(), code, detail: detail === undefined ? "" : String(detail).slice(0, 160) });
    if (L.audit.length > 40) L.audit.pop();
  }

  // plate — единственная форма отказа: человек видит код причины, а не пустой успех.
  function plate(code, message, status) {
    const entry = { code, message: String(message || "").slice(0, 300), status: status || "" };
    L.plates = [entry].concat(L.plates.filter((p) => p.code !== code)).slice(0, 6);
    audit("plate:" + code, status || "");
    toast(message || code, "error");
    paint();
  }
  const unplate = (code) => {
    L.plates = L.plates.filter((p) => p.code !== code);
  };

  const emptyLive = JSON.stringify(L);
  let sessionEpoch = 0, minting = null, pollGeneration = 0, liveStreamAbort = null;
  const pendingActions = new Set();
  function clearSession() {
    sessionEpoch++;
    pollGeneration++;
    liveStreamAbort?.abort();
    Object.assign(S, { bearer: "", exp: 0, subject: "", tenant: "", display: "", online: false });
    for (const key of Object.keys(L)) delete L[key];
    Object.assign(L, JSON.parse(emptyLive));
    closeModal();
    paint();
  }
  function sessionExpired() {
    clearSession();
    plate("session_missing", "Сессия завершилась. Войдите снова, чтобы продолжить.", 401);
  }

  // Auth endpoints never refresh recursively; each protected request retries once.
  async function raw(path, { body, method, bearerRequired = true, raw: wantRaw = false, retry = true } = {}) {
    if (bearerRequired && (!S.online || S.exp <= Date.now())) {
      if (!await mint()) return { status: 401, json: { code: "UNAUTHENTICATED", message: "Сессия завершилась" } };
    }
    const epoch = sessionEpoch;
    const headers = {};
    const binary = typeof Blob !== "undefined" && body instanceof Blob;
    if (body !== undefined) headers["content-type"] = binary ? (body.type || "application/octet-stream") : "application/json";
    if (path.startsWith("/u-ahp/v1/auth/")) headers["x-ahp-session"] = "1";
    if (bearerRequired && S.bearer) headers.authorization = "Bearer " + S.bearer;
    let r;
    try { r = await fetch(path, {
      method: method || (body !== undefined ? "POST" : "GET"), headers, credentials: "include",
      body: binary ? body : body !== undefined ? JSON.stringify(body) : undefined,
    }); } catch { return { status: 0, json:{code:"CONNECTION_ERROR",message:"Нет соединения с платформой"} }; }
    if (bearerRequired && epoch !== sessionEpoch) return { status: 401, json: { code: "SESSION_CHANGED" } };
    if (bearerRequired && r.status === 401) {
      if (retry && await mint()) return raw(path, { body, method, bearerRequired, raw: wantRaw, retry: false });
      if (S.online) sessionExpired();
    }
    if (wantRaw) return { status: r.status, bytes: new Uint8Array(await r.arrayBuffer()), type: r.headers.get("content-type") || "" };
    const text = await r.text();
    let json = null;
    try { json = JSON.parse(text); } catch { /* Response remains an explicit error. */ }
    return { status: r.status, json, text, computeBackend:r.headers.get("AHP-Compute-Backend") || "", computeDisclosure:r.headers.get("AHP-Compute-Disclosure") || "" };
  }
  const errText = (r) => (r.json && (r.json.error?.message || r.json.message || r.json.error?.code || r.json.code)) || (r.status >= 400 ? "HTTP " + r.status : "ответ не похож на JSON");

  async function mint() {
    if (minting) return minting;
    minting = (async () => {
      const epoch = sessionEpoch;
      const r = await raw("/u-ahp/v1/auth/token", { body: {}, bearerRequired: false });
      if (epoch !== sessionEpoch) return false;
      if (r.status !== 200 || !r.json?.access_token) {
        if (S.online || S.bearer) sessionExpired(); else clearSession();
        return false;
      }
      S.bearer = r.json.access_token;
      S.exp = Date.now() + (Number(r.json.expires_in) || 600) * 1000 - 20000;
      S.subject = r.json.subject || "";
      S.tenant = r.json.tenant || "";
      S.online = true;
      unplate("session_missing");
      audit("session_minted", S.subject + "/" + S.tenant);
      return true;
    })();
    try { return await minting; } finally { minting = null; }
  }

  async function logout() {
    const r = await raw("/u-ahp/v1/auth/logout", { body: {}, bearerRequired: false });
    if (r.status !== 200) return plate("logout_refused", "Выход не подтверждён: " + errText(r), r.status);
    clearSession();
    loginModal();
    return true;
  }

  async function login(user, pass) {
    const r = await raw("/u-ahp/v1/auth/login", { body: { username: user, password: pass }, bearerRequired: false });
    if (r.status !== 200) {
      plate("login_refused", "Платформа отказала во входе: " + errText(r), r.status);
      return false;
    }
    S.display = r.json.displayName || r.json.subject || "";
    const ok = await mint();
    if (!ok) plate("token_refused", "Сессия есть, но платформа не выдала human-токен", "");
    else { audit("login_ok", r.json.subject); const toasts=$("#toasts"); if(toasts) toasts.innerHTML=""; }
    if (ok && pendingOAuthCallback) await completePersonalOAuth();
    paint();
    return ok;
  }

  function loginModal() {
    openModal(
      "Вход в AHP",
      notice("Креденциалы выдаёт оператор стенда. Пароль передаётся серверу для входа; поле очищается после ответа. Учётные данные не сохраняются в хранилище браузера.") +
        `<div class="grid">${field("Пользователь", "ahp-user", "", "accept")}${field("Пароль", "ahp-pass", "", "", "password")}</div>`,
      btn("Войти", "ahp-login", "primary") + btn("Отмена", "close"),
      { center: true },
    );
  }

  // ---- плоскости --------------------------------------------------------------
  const space = (op, body) => raw("/v1/space/" + op, { body });

  // Беседы и история — настоящие операции Space (internal/space/handler.go: listChats,
  // listTurns), а не память вкладки: после reload UI восстанавливает выбранный чат,
  // его ходы и кадры с сервера. Данные вкладки не сохраняются в хранилище браузера.
  async function loadChats() {
    const r = await space("ListChats", { limit: 20 });
    if (r.status !== 200) {
      L.chats = null;
      return plate("chats_refused", "ListChats: " + errText(r), r.status);
    }
    const got = SHAPES.chats(r.json);
    L.chatOutcome = SHAPES.outcome(r.status, got.rows, got.missing);
    if (L.chatOutcome.state === "drift") {
      return plate("chats_shape_drift", "ListChats: " + L.chatOutcome.missing.join(", ") + " — контракт грани изменён", r.status);
    }
    unplate("chats_shape_drift");
    L.chats = L.chatOutcome.rows;
    audit("chats_list", "threads=" + L.chats.length);
    paint();
    return L.chats;
  }

  // Открыть беседу = прочитать её ходы (ListTurns) и доесть поток (ReadStream).
  // Так «пусто после reload» перестаёт быть состоянием интерфейса.
  async function openChat(threadId) {
    const id = threadId || (L.chats && L.chats[0] && L.chats[0].thread_id);
    if (!id) return plate("chat_missing", "Нет выбранной беседы");
    const r = await space("ListTurns", { thread_id: id, limit: 20 });
    if (r.status !== 200) return plate("turns_refused", "ListTurns: " + errText(r), r.status);
    const got = SHAPES.turns(r.json);
    const outcome = SHAPES.outcome(r.status, got.rows, got.missing);
    if (outcome.state === "drift") {
      return plate("turns_shape_drift", "ListTurns: " + outcome.missing.join(", ") + " — контракт грани изменён", r.status);
    }
    unplate("turns_shape_drift");
    const turns = outcome.rows;
    pollGeneration++;
    liveStreamAbort?.abort();
    L.userMessage = r.json.turns?.[r.json.turns.length - 1]?.input?.content || "";
    const last = [...turns].sort((a,b)=>String(a.createdAt).localeCompare(String(b.createdAt))).at(-1) || null;
    const known = (L.chats || []).find((c) => c.thread_id === id);
    L.chat = { thread_id: id, context_id: (known && known.context_id) || "", project_id: last?.projectID || "", project_memory_profile:last?.projectMemoryProfile || "",agent_installation_id:last?.agentInstallationID || "" };
	L.activeAgentInstallation=L.chat.agent_installation_id;
    L.turns = turns;
    L.turnHistory = [...(r.json.turns || [])].sort((a,b)=>String(a.created_at).localeCompare(String(b.created_at)));
    L.turn = last
      ? { turn_id: last.id, thread_id: last.thread_id || id, project_id: L.chat.project_id, project_memory_profile:L.chat.project_memory_profile, agent_installation_id:L.chat.agent_installation_id,context_id: L.chat.context_id, status: last.status, steps: last.steps, max_steps: last.maxSteps }
      : null;
    L.frames = [];
    L.cursor = 0;
    L.artifact = null;
    L.cost = null;
    audit("chat_opened", id + " turns=" + turns.length);
    if (last) {
      for (let page=0;page<20;page++) { const read=await readMore(id); if(!read.ok || read.count<50) break; }
      L.streamState = "history:" + last.status;
      if (SHAPES.isTerminalStatus(last.status)) {
        await findArtifact();
        await cost();
      }
    }
    paint();
    return true;
  }

  async function loadCatalog() {
    const r = await raw("/u-ahp/v1/registry/CatalogList", { body: { roles: [] } });
    if (r.status !== 200) { L.catalog=null; L.catalogOutcome=SHAPES.outcome(r.status,null); return plate("registry_unavailable", "Каталог: " + errText(r), r.status); }
    unplate("registry_unavailable");
    // Карточка каталога: {installation_id, agent_id, system, kind, environment,
    // version_id, digest, audience, visibility, shares_between_users, roles,
    // published} (internal/registry/catalog.go:22-35). Полей owner/class у неё нет.
    const got = SHAPES.catalog(r.json);
    L.catalogOutcome = SHAPES.outcome(r.status, got.rows, got.missing);
    if (L.catalogOutcome.state === "drift") {
      return plate("catalog_shape_drift", "CatalogList: " + L.catalogOutcome.missing.join(", ") + " — контракт грани изменён", r.status);
    }
    unplate("catalog_shape_drift");
    L.catalog = L.catalogOutcome.rows;
    audit("catalog_list", "cards=" + L.catalog.length);
    paint();
  }

  async function registerAgent(agentId) {
    if (!agentId) return plate("agent_id_required", "Нужен идентификатор агента (нижний регистр)");
    const r = await raw("/u-ahp/v1/registry/RegisterAgent", { body: { agent_id: agentId } });
    if (r.status !== 200) return plate("register_refused", "RegisterAgent: " + errText(r), r.status);
    unplate("register_refused");
    L.registered = r.json.agent || null;
    audit("register_agent", agentId);
    await loadCatalog();
    return true;
  }

  // Публикуемый манифест = registry.Manifest ядра. Дважды измерено и исправлено:
  //   * транспорт отклоняет unknown fields (мнимое поле image давало 400);
  //   * ValidateManifest отвергает неприготовленные значения — 'private'/'internal'/
  //     'service' из макета давали 400 INVALID_ARGUMENT, а без state/capabilities/
  //     limits манифест вообще не проходит валидацию.
  // Форму манифеста даёт общий модуль (ahp-canonical.js): та же форма, что проверяют
  // тест против ValidateManifest и вектора платформенных digest'ов.
  const publishManifest = (agentId) => window.AHP_CANONICAL.publishManifest(agentId);

  // F3: публикация версии. digest пересчитывается из канонического манифеста на
  // клиенте (ahp-canonical.js — копия contracts.Digest): Store.PutVersion сверяет
  // его со своим и отвергает любой другой, так что без канонизации публикация
  // физически невозможна. Ожидание честное: без passing-verdict платформа обязана
  // отказать (409), и именно этот отказ есть доказательство гейта «оценки перед
  // публикацией».
  async function publishVersion(agentId, calculateOnly=false) {
    if (!agentId) return plate("publish_inputs", "Нужен agent_id");
    if (!window.AHP_CANONICAL) {
      return plate("canonical_missing", "Канонизатор digest (ahp-canonical.js) не загружен — публиковать нечем");
    }
    let manifest;
    const custom=val("ahp-pub-manifest");
    try { manifest=custom ? JSON.parse(custom) : publishManifest(agentId); }
    catch { return plate("manifest_json","Манифест должен быть JSON-объектом."); }
    const image=custom ? manifest?.components?.find(c=>c.a2a)?.image : val("ahp-pub-image");
    if(!/^.+@sha256:[0-9a-f]{64}$/.test(image || "")) return plate("image_required","Укажите образ агента с неизменяемым @sha256 digest.");
    if(!custom) manifest.components[0].image=image;
    if ((manifest.state?.partition || "").length > 64) {
      return plate("partition_too_long", "Имя пространства состояния длиннее 64 байт: shorten agent_id");
    }
    let digest;
    try {
      digest = await window.AHP_CANONICAL.manifestDigest(manifest);
    } catch (e) {
      return plate("manifest_not_canonical", "Манифест не приводится к канону: " + e.message);
    }
    if(calculateOnly) { L.versionDraft={digest,manifest,agent_id:agentId}; unplate("image_required"); paint(); return L.versionDraft; }
    const r = await raw("/u-ahp/v1/registry/PutVersion", {
      body: { agent_id: agentId, digest, manifest },
    });
    L.publish = { status: r.status, message: r.status === 200 ? "Версия принята" : errText(r), at: nowISO(), digest, version:r.json?.version };
    unplate("publish_refused");
    if (r.status !== 200 && r.status !== 409) plate("publish_refused", "PutVersion: " + errText(r), r.status);
    audit("put_version", r.status + " " + (r.json && r.json.code ? r.json.code : ""));
    await verdict(digest);
    paint();
    return r;
  }

  async function verdict(digest) {
    const r = await raw("/u-ahp/v1/evals/Verdict", { body: { agent_digest: digest } });
    L.verdict = { digest, status: r.status, message: errText(r), verdict: r.json?.verdict || null };
    audit("evals_verdict", r.status);
    paint();
  }

  // ---- запуск (F4), отмена (F7), стоимость (F6) --------------------------------
  // context id беседы: взятый из выбранного чата либо новый уникальный токен.
  // Правила — серверные (internal/artifacts/access.go:286): «/» запрещён, иначе
  // артефакты в этом контексте сохраняться не перестанут, и ход умрёт на
  // сохранении файла. Подсказка показывается до отправки (DEF-H24-11).
  const autoContext = () => "h24-" + nowISO().replace(/[^0-9a-z]/gi, "").slice(0, 14);

  async function runTurn(text, overrides) {
    if (!text) return plate("turn_input_required", "Нужен текст хода");
    if (!SHAPES) return plate("shapes_missing", "Модуль форм ответа (ahp-shapes.js) не загружен — ход не отправляется");
    const opts = overrides || {};
    const context = opts.context_id || val("ahp-context") || (L.chat && L.chat.context_id) || autoContext();
    const badContext = SHAPES.contextIDError(context);
    if (badContext) return plate("context_invalid", "context id: " + badContext, "");
    // Бюджет шагов берётся из поля формы: раньше поле отображалось, а уходил
    // константой 4 (UI-03). Пустое поле = бюджет не назван, и он честно не
    // отправляется — платформа различает «не назван» и «назван» (coordinator.go:290-302).
    const budget = SHAPES.stepBudget(opts.max_steps !== undefined ? opts.max_steps : val("ahp-run-steps"));
    if (budget.error) plate("steps_adjusted", budget.error, "");
    if (!budget.value && budget.stated && budget.error && budget.error.startsWith("max_steps —")) return undefined;
    const body = { key: "h24-ui-" + Date.now(), context_id: context, input: { role: "user", content: text } };
    const memoryBinding=SHAPES.projectMemoryBinding({current:L.chat,workspace:L.workspace,selectedProfile:opts.project_memory_profile ?? val("ahp-project-memory") ?? L.projectMemoryProfile,projectID:opts.project_id});
    if(memoryBinding.error) return plate("project_memory_required",memoryBinding.error);
    if(memoryBinding.projectID) body.project_id=memoryBinding.projectID;
    if(!memoryBinding.locked && memoryBinding.profile) body.project_memory_profile=memoryBinding.profile;
	const existingThread = opts.thread_id || (L.chat && L.chat.thread_id);
	if (existingThread) body.thread_id = existingThread;
	const selectedAgent=opts.agent_installation_id ?? (val("ahp-agent-target") || L.activeAgentInstallation);
	if(!existingThread && selectedAgent) body.agent_installation_id=selectedAgent;
    if (budget.stated && budget.value > 0) body.max_steps = budget.value;
    const acc = await space("AcceptTurn", body);
    if (acc.status !== 200) return plate("accept_refused", "AcceptTurn: " + errText(acc), acc.status);
    L.turn = {
      turn_id: acc.json.turn_id, thread_id: acc.json.thread_id, project_id: acc.json.project_id || "", project_memory_profile:acc.json.project_memory_profile || "",agent_installation_id:acc.json.agent_installation_id || "", key: body.key,
      context_id: context, status: acc.json.status || "accepted", max_steps: budget.value || null,
    };
    L.chat = {thread_id:acc.json.thread_id,context_id:context,project_id:acc.json.project_id || "",project_memory_profile:acc.json.project_memory_profile || "",agent_installation_id:acc.json.agent_installation_id || ""};
	L.activeAgentInstallation=L.chat.agent_installation_id;
    L.turnHistory = [...(L.turnHistory || []).filter(t=>t.id!==acc.json.turn_id),{id:acc.json.turn_id,thread_id:acc.json.thread_id,input:body.input,status:"accepted",created_at:nowISO()}];
    L.userMessage = text;
    const input=document.querySelector('[name="ahp-run-text"]'); if(input) input.value="";
    L.frames = [];
    L.cursor = 0;
    L.streamDrift = [];
    L.streamState = "dispatching";
    L.artifact = null;
    L.cost = null;
    audit("turn_accepted", L.turn.turn_id);
    const disp = await space("DispatchTurn", { turn_id: L.turn.turn_id });
    L.streamState = disp.status === 200 ? "streaming" : "dispatch_refused:" + disp.status;
    if (disp.status !== 200) plate("dispatch_refused", "DispatchTurn: " + errText(disp), disp.status);
    paint();
    history.replaceState(null,"", "#/chats?thread=" + encodeURIComponent(L.turn.thread_id));
    loadChats();
    poll().catch(e => plate("stream_error", e.message));
    return true;
  }

  // Статус хода берёт GetTurn (internal/space/types.go Turn) — вид кадра о
  // терминальности не сообщает: kinds бывают model/tool/done/status, и кадр
  // status есть у незавершённого хода.
  const turnStatus = (j) => (j && (j.status || (j.turn && j.turn.status))) || "";

  // Кадры читаются по продуктовому контракту стрима:
  // {id, thread_id, key, kind, sequence, payload} (internal/space/stateclient.go:643-650).
  // Курсор — по sequence; `e.seq`/`e.type` платформа не отдаёт (UI-01/UI-03).
  async function readMore(threadId) {
    const r = await space("ReadStream", { thread_id: threadId, after: L.cursor || 0, limit: 50 });
    if (r.status !== 200) return { ok: false, status: r.status, error: errText(r) };
    const got = SHAPES.frames(r.json);
    L.streamDrift = got.missing || [];
    if (L.streamDrift.length) {
      plate("stream_shape_drift", "Кадры: " + L.streamDrift.join(", ") + " — контракт потока изменён", r.status);
      return { ok: false, status: r.status, error: "shape drift: " + L.streamDrift.join(", ") };
    }
    unplate("stream_shape_drift");
    const evs = (r.json && r.json.events) || [];
    for (const e of evs) {
      if (Number(e.sequence) <= L.cursor) continue;
      L.frames.push(e);
      L.cursor = Number(e.sequence);
    }
    if (evs.length) paint();
    return { ok: true, count: evs.length };
  }

  async function finishTurn(status) {
    L.turn.status = status;
    const history=await space("ListTurns", {thread_id:L.turn.thread_id,limit:100});
    if(history.status===200 && Array.isArray(history.json?.turns)) { L.turnHistory=[...history.json.turns].sort((a,b)=>String(a.created_at).localeCompare(String(b.created_at))); L.turns=history.json.turns; }
    L.streamState = "terminal:" + status;
    audit("turn_terminal", status);
    await findArtifact();
    await cost();
    paint();
  }

  async function streamLive(threadId, generation, signal) {
    while (!signal.aborted && generation === pollGeneration && S.online) {
      try {
        if (S.exp <= Date.now() && !await mint()) return;
        const response=await fetch("/v1/space/stream/"+encodeURIComponent(threadId)+"?after="+(L.cursor||0), {
          headers:{authorization:"Bearer "+S.bearer},credentials:"include",signal,
        });
        if (response.status===401) { if (!await mint()) return; continue; }
        if (!response.ok || !response.body) throw new Error("HTTP "+response.status);
        const reader=response.body.getReader(), text=new TextDecoder();
        const decode=SHAPES.sseDecoder(event=>{
          if (generation!==pollGeneration || signal.aborted || event.thread_id!==threadId) return;
          if (event.type==="error") throw new Error("Поток прерван");
          if (event.type==="end") return; // only GetTurn decides completion
          if (!Number.isSafeInteger(event.cursor) || event.cursor<=L.cursor) return;
          L.frames.push({id:event.event_id,thread_id:threadId,key:event.key,kind:event.type,sequence:event.cursor,payload:event.payload});
          L.cursor=event.cursor;L.streamState="streaming";paint();
        });
        try { for (;;) { const part=await reader.read();if(part.done) break;decode(text.decode(part.value,{stream:true})); } }
        finally { await reader.cancel().catch(()=>{});reader.releaseLock(); }
      } catch (error) { if (signal.aborted) return; L.streamState="reconnecting"; }
      await new Promise(resolve=>setTimeout(resolve,400));
    }
  }

  async function poll() {
    if (!L.turn) return;
    const generation=++pollGeneration, threadId=L.turn.thread_id,turnId=L.turn.turn_id;
    liveStreamAbort?.abort();
    const controller=new AbortController();liveStreamAbort=controller;
    const streaming=streamLive(threadId,generation,controller.signal);
    try {
      for (let i=0;i<400;i++) {
        if (generation!==pollGeneration || !S.online || !L.turn) return;
        const t=await space("GetTurn",{turn_id:turnId});
        if (generation!==pollGeneration || !L.turn) return;
        const st=turnStatus(t.json);
        if(st&&L.turn.status!==st){L.turn.status=st;paint();}
        if (SHAPES.isTerminalStatus(st)) {
          controller.abort();await streaming;
          // Drain every durable page before marking the turn complete.
          for(let page=0;page<1400;page++) {const got=await readMore(threadId);if(!got.ok) throw new Error("Не удалось дочитать ответ: "+got.error);if(got.count<50) break;if(page===1399)throw new Error("Ответ превысил окно чтения");}
          if(st==="failed"||st==="timeout")plate("turn_failed","Ход завершён: "+(t.json?.failure_reason||st),"");
          await finishTurn(st);return;
        }
        await new Promise(resolve=>setTimeout(resolve,1000));
      }
      L.streamState="timeout";plate("stream_timeout","Ход ещё не завершён. Обновите беседу для продолжения наблюдения","");
    } finally {controller.abort();await streaming;}
  }

  async function cancelTurn() {
    if (!L.turn) return plate("nothing_to_cancel", "Активного хода нет");
    const current = L.turn;
    const r = await space("CancelTurn", { turn_id: current.turn_id });
    if (r.status !== 200) return plate("cancel_refused", "CancelTurn: " + errText(r), r.status);
    if (L.turn !== current || !S.online) return;
    current.canceled = true;
    current.durable = r.json?.terminal_frame_durable === true;
    audit("cancel_turn", current.turn_id);
    toast("Отмена принята: terminal_frame_durable=" + (current.durable ? "true" : "false"));
    setTimeout(async () => {
      if (L.turn !== current || !S.online) return;
      const t = await space("GetTurn", { turn_id: current.turn_id });
      if (L.turn !== current || !S.online) return;
      current.status = t.json?.status || t.json?.turn?.status || current.status;
      paint();
    }, 1500);
    paint();
  }

  // F6: стоимость дерева задачи, в токенах.
  async function cost() {
    if (!L.turn?.thread_id) return plate("cost_no_thread", "Нет потока задачи — стоимость читать негде");
    const r = await raw("/u-ahp/v1/model/Cost?thread_id=" + encodeURIComponent(L.turn.thread_id));
    if (r.status !== 200) {
      L.cost = { status: r.status, message: errText(r) };
      return plate("cost_refused", "Cost: " + errText(r), r.status);
    }
    unplate("cost_refused");
    L.cost = { status: 200, json: r.json, at: nowISO() };
    audit("cost_read", Object.keys(r.json || {}).join(","));
    paint();
  }

  // ---- артефакт: найти, разрешить по digest, скачать и сверить побайтово (F5) ---
  // Артефакт берётся из кадра инструмента, а не из догадки: tool-кадр содержит карточку
  // {id, version, sha256, size} — тот же identity, что отдаёт Resolve по digest.
  async function findArtifact() {
    // Право называть артефакт есть только у ArtifactRef {id, version, sha256}
    // из tool-кадра (internal/artifacts/store.go:41-49). Произвольный 64-hex из
    // текста промпта или аргумента инструмента артефактом не считается: он
    // показывается как неподтверждённый и разрешается только вручную (UI-03).
    const ref = SHAPES.artifactRef(L.frames);
    if (!ref) {
      const loose = SHAPES.looseDigest(L.frames);
      L.artifact = loose
        ? { state: "unconfirmed", digest: loose, note: "в кадрах нет ArtifactRef {id, version, sha256}: 64-hex найден вне карточки инструмента. Разрешить можно вручную, но это не доказанный артефакт хода." }
        : { state: "no_artifact_ref", note: "ход не сохранил артефакт: в терминальных кадрах нет карточки save_artifact (DEF-H24-5/DEF-H24-9: тихий done без артефакта)" };
      audit("artifact_absent", L.artifact.state);
      paint();
      return;
    }
    L.artifact = {
      state: "ref", digest: ref.sha256, artifact_id: ref.id, version: ref.version,
      size: ref.size, from_frame: true, sequence: ref.sequence,
    };
    audit("artifact_ref", ref.sha256.slice(0, 12) + "@" + ref.version.slice(0, 8));
    await resolve(ref.sha256, ref);
  }

  // Resolve по digest отдаёт {sha256, versions:[{artifact_id, version, sha256, size,
  // name, mime}]}. Версия берётся ТОЧНАЯ — та, что названа в ArtifactRef хода;
  // «первая из ответа» недопустима: у одного digest может быть несколько версий,
  // и скачанное перестало бы быть тем, что создал ход.
  async function resolve(sha, expected) {
    const clean = (sha || "").replace(/^sha256:/, "").trim();
    if (!/^[0-9a-f]{64}$/.test(clean)) return plate("digest_invalid", "digest должен быть 64 hex");
    const r = await raw("/u-ahp/v1/artifacts/Resolve?sha256=" + clean);
    if (r.status !== 200) {
      L.artifact = { state: "refused", digest: clean, status: r.status, message: errText(r) };
      return plate("resolve_refused", "Resolve: " + errText(r), r.status);
    }
    unplate("resolve_refused");
    const got = SHAPES.versions(r.json);
    if (got.missing.length) {
      return plate("resolve_shape_drift", "Resolve: " + got.missing.join(", ") + " — контракт грани изменён", r.status);
    }
    unplate("resolve_shape_drift");
    const versions = got.rows || [];
    const wanted = (expected && expected.version) || (L.artifact && L.artifact.version) || "";
    const wantedId = expected?.id || (wanted ? L.artifact?.artifact_id : "");
    const chosen = versions.find((v) => v.sha256 === clean && v.version === wanted && (!wantedId || v.artifactId === wantedId)) || (!wanted && versions.length === 1 && versions[0].sha256 === clean ? versions[0] : null);
    if (!chosen) {
      L.artifact={state:"refused",digest:clean,status:200,message:"Точная версия артефакта отсутствует в ответе Resolve"};
      return plate("version_missing", "Resolve не вернул версию " + wanted + " для этого digest — скачивание не выполняется", r.status);
    }
    unplate("version_missing");
    L.artifact = Object.assign({}, L.artifact || {}, {
      state: "resolved",
      digest: clean,
      artifact_id: chosen.artifactId,
      version: chosen.version,
      versions: versions.map((v) => v.version),
      size: chosen.size,
      name: chosen.name || "",
      mime: chosen.mime || "",
      raw: r.json,
    });
    audit("artifact_resolved", L.artifact.artifact_id + "@" + L.artifact.version);
    paint();
    return true;
  }

  // Скачивание по точной паре (artifact_id, version): грань отвергает «latest»,
  // и UI не делает вид, что умеет иначе. Контрольная сумма считается здесь же,
  // вне полосы; при несовпадении файл ЧЕЛОВЕКУ НЕ ОТДАЁТСЯ (UI-03: раньше
  // скачивание происходило и после sha_mismatch).
  async function download(override) {
    const art = override || L.artifact;
    if (!art?.artifact_id || !art?.version) return plate("download_no_artifact", "Артефакт не разрешён по точной версии — качать нечего", "");
    const q = "/u-ahp/v1/artifacts/download?artifact_id=" + encodeURIComponent(art.artifact_id) +
      "&version=" + encodeURIComponent(art.version);
    const r = await raw(q, { raw: true });
    if (r.status !== 200) return plate("download_refused", "download: " + (r.text || "HTTP " + r.status), r.status);
    const buf = await crypto.subtle.digest("SHA-256", r.bytes.buffer);
    const computed = [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, "0")).join("");
    const verdict = SHAPES.downloadVerdict({ wanted: art.digest, computed, bytes: r.bytes.length });
    if (L.artifact === art || !L.artifact) {
      L.artifact = Object.assign({}, art, { download: { bytes: verdict.bytes, computed, match: verdict.match, delivered: verdict.deliver, at: nowISO() } });
    }
    audit("download_verified", "bytes=" + verdict.bytes + " sha_match=" + verdict.match + " delivered=" + verdict.deliver);
    if (!verdict.deliver) {
      // Никакого blob/anchor: несверенные байты человеку не выдаются.
      return plate("sha_mismatch", verdict.reason, "");
    }
    unplate("sha_mismatch");
    unplate("download_refused");
    const url = URL.createObjectURL(new Blob([r.bytes], { type: r.type || art.mime || "application/octet-stream" }));
    const a = document.createElement("a");
    a.href = url;
    a.download = (art.name || art.artifact_id) + "@" + art.version;
    a.click();
    setTimeout(() => URL.revokeObjectURL(url), 30000);
    paint();
    return verdict;
  }

  // ---- личная / проектная память (Hindsight) -------------------------------
  const memoryRPC = (operation, body) => raw("/u-ahp/v1/memory/" + operation, { body });
  function memorySelection() {
    const scope = val("ahp-mem-scope") || L.memoryScope || "personal";
    L.memoryScope = scope;
    const project = val("ahp-mem-project") || L.memoryProject || L.workspace?.name || "";
    L.memoryProject = project;
    return {scope,project_id:scope === "project" ? project : ""};
  }
  async function memoryDetails(selection) {
    const [settings,status] = await Promise.all([memoryRPC("Settings",selection),memoryRPC("Status",selection)]);
    L.memorySettings = settings.status === 200 ? settings.json : null;
    L.memoryStatus = status.status === 200 ? status.json : null;
  }
  async function proposeMemory(text) {
    if (!text) return plate("memory_text_required", "Нужен текст записи");
    const selection = memorySelection();
    if(selection.scope === "project" && !selection.project_id) return plate("memory_project_required","Выберите личный Workspace.");
    const r = await memoryRPC("Remember",{project_id:selection.project_id,target:selection.scope,text,op_key:"remember-"+crypto.randomUUID()});
    if(r.status!==202 || !r.json?.id) return plate("memory_refused","Сохранение: "+errText(r),r.status);
    unplate("memory_refused");
    L.memoryOperations.unshift({...r.json,...selection});
    L.memoryOperations=L.memoryOperations.slice(0,10);
    L.memoryNote="Запись поставлена в обработку. Она станет доступна после завершения операции.";
    audit("memory_queued",r.json.id);paint();
    await refreshMemoryOperation(r.json.id,selection);
    return true;
  }
  async function refreshMemoryOperation(id,selection) {
    const selected=selection || L.memoryOperations.find(o=>o.id===id) || memorySelection();
    const r=await memoryRPC("Operation",{id,scope:selected.scope,project_id:selected.project_id});
    if(r.status!==200) return plate("memory_operation_refused",errText(r),r.status);
    L.memoryOperations=L.memoryOperations.map(o=>o.id===id?{...o,...r.json}:o);
    if(r.json.state==="succeeded") L.memoryNote="Обработка завершена. Обновите список, чтобы увидеть извлечённые факты.";
    else if(r.json.state==="failed") L.memoryNote="Обработка не завершена: "+(r.json.error || "ошибка");
    await memoryDetails(memorySelection());paint();
  }
  async function searchMemory(q) {
    const selection=memorySelection();
    if(selection.scope==="project"&&!selection.project_id) return plate("memory_project_required","Укажите имя личного Workspace.");
    await memoryDetails(selection);
    const selectedEnabled=L.memorySettings && (selection.scope==="personal"?L.memorySettings.personal_enabled:L.memorySettings.project_enabled);
    if(L.memorySettings && !selectedEnabled){L.memories=[];L.memoryOutcome=null;L.memoryNote="Память выключена. Данные сохранены, чтение и новые записи остановлены.";paint();return;}
    const r=await memoryRPC("Search",{...selection,query:q || ""});
    if(r.status!==200){L.memories=null;L.memoryOutcome=SHAPES.outcome(r.status,null);return plate("memory_search_refused",errText(r),r.status);}
    unplate("memory_search_refused");
    const got=SHAPES.memories(r.json);L.memoryOutcome=SHAPES.outcome(r.status,got.rows,got.missing);
    if(L.memoryOutcome.state==="drift") return plate("memory_shape_drift",got.missing.join(", "),r.status);
    unplate("memory_shape_drift");L.memories=L.memoryOutcome.rows;
    if(r.json.degraded?.length)L.memoryNote="Часть памяти временно недоступна: "+r.json.degraded.join(", ")+". Поиск выполнен по доступным областям.";
    await memoryDetails(selection);audit("memory_search","facts="+L.memories.length);paint();
  }
  async function updateMemorySettings(action) {
    const selection=memorySelection();if(!L.memorySettings) return plate("memory_settings_missing","Сначала обновите настройки памяти.");
    let body={...selection,op_key:"settings-"+crypto.randomUUID()};
    if(action==="idle") body.idle_days=Number(val("ahp-mem-idle"));
    else if(action==="personal") body.use_personal=!L.memorySettings.use_personal;
    else if(selection.scope==="project") body.project_enabled=!L.memorySettings.project_enabled;
    else body.personal_enabled=!L.memorySettings.personal_enabled;
    delete body.scope;if(!body.project_id) delete body.project_id;
    const r=await memoryRPC("UpdateSettings",body);if(r.status!==200) return plate("memory_settings_refused",errText(r),r.status);
    await searchMemory("");
  }
  async function clearMemory() {
    const selection=memorySelection();
    if(!window.confirm("Очистить выбранную память? История чатов сохранится, а факты этого банка будут удалены.")) return;
    const r=await memoryRPC("Clear",{...selection,op_key:"clear-"+crypto.randomUUID()});
    if(r.status!==202) return plate("memory_clear_refused",errText(r),r.status);
    L.memoryNote="Старое поколение закрыто. Физическая очистка выполняется фоново.";L.memoryOperations=[];await searchMemory("");
  }
  async function editMemory(id,invalidate=false) {
    const record=L.memories?.find(m=>m.id===id);if(!record)return;
    const selection=memorySelection();let text="";
    if(!invalidate){text=window.prompt("Исправленный факт",record.text);if(text===null)return;}
    const r=await memoryRPC(invalidate?"Invalidate":"Correct",{...selection,memory_ref:id,text,reason:"Изменение пользователем в консоли",op_key:"curate-"+crypto.randomUUID()});
    if(r.status!==202)return plate("memory_edit_refused",errText(r),r.status);
    L.memoryOperations.unshift({...r.json,...selection});L.memoryNote="Исправление поставлено в обработку.";paint();
  }

  // ---- триггеры (F9) ---------------------------------------------------------
  async function createTrigger(target, intervalSec, maxFires) {
    if (!target?.trim()) return plate("trigger_target_required", "Укажите ID установки агента из каталога.");
    const body = {
      target: target.trim(),
      interval: Number(intervalSec || 60) * 1000000000,
      max_fires: Number(maxFires || 1),
      delegated: false,
    };
    const r = await raw("/u-ahp/v1/triggers/CreateSchedule", { body });
    if (r.status !== 200 && r.status !== 201) return plate("trigger_refused", "CreateSchedule: " + errText(r), r.status);
    unplate("trigger_refused");
    unplate("trigger_target_required");
    audit("trigger_created", r.json?.trigger_id || "");
    await listTriggers();
  }

  async function listTriggers() {
    const r = await raw("/u-ahp/v1/triggers/List", { body: {} });
    if (r.status !== 200) { L.triggers=null; L.triggerOutcome=SHAPES.outcome(r.status,null); return plate("trigger_list_refused", "List: " + errText(r), r.status); }
    unplate("trigger_list_refused");
    // У триггера НЕТ поля id: грань отдаёт trigger_id, fired_count, max_fires,
    // interval_ms (internal/triggers/httpapi.go). Чтение t.id давало пустые
    // идентификаторы и кнопку «FireNow» без цели.
    const got = SHAPES.triggers(r.json);
    L.triggerOutcome = SHAPES.outcome(r.status, got.rows, got.missing);
    if (L.triggerOutcome.state === "drift") {
      return plate("trigger_shape_drift", "List: " + L.triggerOutcome.missing.join(", ") + " — контракт грани изменён", r.status);
    }
    unplate("trigger_shape_drift");
    L.triggers = L.triggerOutcome.rows;
    audit("trigger_list", "rows=" + L.triggers.length);
    paint();
  }

  // FireNow — операция над наступившими сроками, её тело {limit} (httpapi.go:186-196),
  // а не {id}: триггер по имени здесь не вызывается. Отключение триггера — Revoke
  // с trigger_id (httpapi.go:161-171).
  async function fireTrigger(limit) {
    const r = await raw("/u-ahp/v1/triggers/FireNow", { body: { limit: Number(limit) || 1 } });
    if (r.status !== 200) return plate("fire_refused", "FireNow: " + errText(r), r.status);
    unplate("fire_refused");
    audit("trigger_fire", "limit=" + (Number(limit) || 1));
    await listTriggers();
  }

  async function revokeTrigger(id) {
    if (!id) return plate("trigger_id_required", "Нужен trigger_id из списка");
    const r = await raw("/u-ahp/v1/triggers/Revoke", { body: { trigger_id: id } });
    if (r.status !== 200) return plate("revoke_refused", "Revoke: " + errText(r), r.status);
    unplate("revoke_refused");
    audit("trigger_revoke", id);
    await listTriggers();
  }

  // ---- workspace (F10) и права (F11): измеряем как есть ------------------------
  async function probeWorkspace() { go("workspace"); }

  // Проверка прав обязана быть двусторонней (UI-06): владелец свой объект ВИДИТ,
  // чужой — НЕТ, и оба объекта настоящие. Выдуманный «foreign-probe» ничего не
  // проверяет: на несуществующем id любую стену отдает 404. Панель держит обе
  // ветки, чужой id вводится операторски из сессии второго принципала.
  async function probeOwn() {
    const id = (L.artifact && L.artifact.artifact_id) || "";
    const version = (L.artifact && L.artifact.version) || "";
    if (!id || !version) return plate("own_probe_no_object", "Нет собственного объекта точной версии: сначала сохраните артефакт ходом", "");
    const r = await raw("/u-ahp/v1/artifacts/download?artifact_id=" + encodeURIComponent(id) + "&version=" + encodeURIComponent(version), { raw: true });
    L.rights = {
      case: "owner_positive", id: id + "@" + version, status: r.status,
      message: r.status === 200 ? "владелец получил свой артефакт (" + r.bytes.length + " байт)" : "HTTP " + r.status,
    };
    audit("rights_owner_positive", r.status);
    paint();
    return L.rights;
  }

  async function probeForeign(id) {
    const clean = String(id || "").trim();
    if (!clean) {
      return plate("foreign_id_required", "Нужен id СУЩЕСТВУЮЩЕГО объекта второго tenant: возьмите его из сессии второго принципала, заглушка не принимается", "");
    }
    const r = await raw("/u-ahp/v1/artifacts/download?artifact_id=" + encodeURIComponent(clean) + "&version=00000000-0000-4000-8000-000000000000", { raw: true });
    L.rights = {
      case: "foreign_negative", id: clean, status: r.status,
      message: r.json ? errText(r) : "HTTP " + r.status,
    };
    audit("rights_foreign_negative", r.status);
    paint();
    return L.rights;
  }

  // Compute: плоскость монтируется операторски, и с MOCK-бэкендом она обязана
  // сказать об этом языком ответа (mock_marker). Браузер не получает машинного
  // креденциала, поэтому ножа измеряет ответ грани и рисует ровно его: отказ с
  // кодом причины или lifecycle с явным маркером MOCK. Панель никогда не
  // заявляет ни изоляцию, ни задержки — это не её доказательство.
  async function probeCompute() { go("sandbox"); }

  // ---- UI --------------------------------------------------------------------
  const UNAVAILABLE = [
    ["repositories_not_wired", "Repositories / Git-источники", "в ядре нет грани repositories: подключение Git недоступно."],
    ["environments_not_wired", "Environments и секреты", "грани environments/secrets нет; секреты живут на сервере (Vault), браузер их не хранит и не передаёт."],
    ["ci_registration_link_missing", "Ссылка регистрации артефакта из CI", "платформа не отдаёт человеку ссылку регистрации; путь CI/CD — машинная грань."],
  ];

  // app.js строит таблицу из объектов {cells,search,status} (app.js:365-370 зовёт
  // r.cells.map). earlier здесь возвращался готовый <tr>: непустой список ронял весь
  // раздел с «Cannot read properties of undefined (reading 'map')» (UI-01, измерено
  // в живом Chromium). Форма строки таблицы теперь живёт в AHP_SHAPES.tableRow.
  const row = (cells, meta) => (SHAPES ? SHAPES.tableRow(cells, meta) : { cells: cells || [] });

  // Одно состояние списка на весь раздел: «ещё не спрашивали», «пусто» (200 и
  // ноль строк), «недоступно» (отказ с причиной), «дрейф контракта» (поля не те).
  // Заглушить ошибку пустым результатом — именно тот режим, от которого
  // отказывается заказчик; поэтому drift/unavailable рисуются красным.
  function listState(outcome, emptyText, neverText) {
    if (!outcome) return notice(neverText, "info");
    if (outcome.state === "empty") return notice(emptyText, "info");
    if (outcome.state === "unavailable") return notice("Недоступно (HTTP " + outcome.status + "): " + esc(outcome.message || "отказ плоскости"), "error");
    if (outcome.state === "drift") return notice("Дрейф контракта: плоскость не вернула " + esc((outcome.missing || []).join(", ")) + ". Список не показан, чтобы не выдавать пустые поля за данные.", "error");
    return "";
  }

  const statusPill = (s) => {
    const ok = ["completed", "done", "passed", "active", "enabled"].includes(String(s));
    const bad = ["failed", "canceled", "refused", "timeout", "EXHAUSTED"].includes(String(s));
    return `<span class="tag ${ok ? "success" : bad ? "danger" : ""}">${esc(s || "—")}</span>`;
  };

  function sessionCard() {
    if (!S.online) {
      return panel("Сессия", notice("Входа нет. Прототип не показывает демо-данные: без сессии платформы здесь пусто.") +
        `<div class="actions">${btn("Войти", "ahp-login-modal", "primary")}</div>`, "F1");
    }
    return panel(
      "Сессия",
      `<div class="kv"><div><span>Субъект</span><strong>${esc(S.subject)}</strong></div><div><span>Тенант</span><strong>${esc(S.tenant)}</strong></div><div><span>Имя</span><strong>${esc(S.display || "—")}</strong></div><div><span>Human-токен</span><strong>в памяти страницы, до ${new Date(S.exp).toLocaleTimeString("ru-RU")}</strong></div></div>` +
        notice("Scoped task token выдаёт раннер из этой сессии; браузер не выбирает ни принципала, ни deployment.", "info"),
      "F1",
    );
  }

  function catalogCard() {
    const cards = (L.catalogOutcome && L.catalogOutcome.rows) || [];
    const body =
      listState(L.catalogOutcome, "Каталог пуст: платформа вернула 200 и ноль карточек (публикации закрыты гейтом оценок).", "Каталог ещё не запрошен.") +
      (cards.length
        ? table(
            ["Агент", "Установка", "Версия", "Digest", "Видимость", "Аудитория", "Опубликован"],
            cards.map((c) => row(
              [esc(c.agent_id), esc(c.installation_id), esc(c.version_id || "—"),
               esc((c.digest || "").replace(/^sha256:/, "").slice(0, 12) || "—"),
               esc(c.visibility || "—"), esc(c.audience || "—"), statusPill(c.published ? "published" : "unpublished")],
              { status: c.published ? "published" : "unpublished" },
            )),
            { placeholder: "Поиск по каталогу" },
          )
        : "") +
      `<div class="actions">${btn("Обновить каталог", "ahp-catalog", "secondary")}</div>` +
      `<div class="divider"></div><h3>Завести агента</h3>` +
      `<div class="grid">${field("agent_id", "ahp-agent", "", "h24-demo-agent")}</div><div class="actions">${btn("RegisterAgent", "ahp-register", "secondary")}</div>` +
      (L.registered ? notice("Зарегистрирован: " + esc(L.registered.agent_id || L.registered.id || ""), "info") : "");
    return panel("Каталог агентов", body, "F2");
  }

  function publishCard() {
    const digest = L.versionDraft?.digest || L.publish?.digest || L.verdict?.digest || "";
    const gate = L.publish
      ? (L.publish.status === 409
          ? notice("Публикация отклонена гейтом оценок (409): " + esc(L.publish.message), "error")
          : L.publish.status === 200
            ? notice("Версия опубликована (200) — для этого digest есть passing-verdict.", "info")
            : notice("PutVersion → " + L.publish.status + ": " + esc(L.publish.message), "error"))
      : notice("Ещё не публиковали. Гейт устроен так: без пройденного вердикта на точный digest публикация невозможна.", "info");
    const v = L.verdict
      ? notice("Verdict(" + esc(L.verdict.digest.slice(0, 14)) + "…) → " + L.verdict.status + ": " + esc(L.verdict.message), L.verdict.status === 200 ? "info" : "error")
      : "";
    return panel(
      "Публикация версии (гейт оценок)",
      `<div class="grid">${field("agent_id", "ahp-pub-agent", L.registered?.agent_id || L.catalog?.[0]?.agent_id || "", "my-agent")}${field("Образ с digest", "ahp-pub-image", L.versionDraft?.manifest?.components?.[0]?.image || "", "registry/agent@sha256:…")}</div>` +
      `<details><summary>Полный манифест</summary><label class="field"><span>JSON манифеста версии</span><textarea name="ahp-pub-manifest" rows="8" placeholder="Необязательно: манифест из вашего CI/CD"></textarea></label></details>` +
      `<div class="kv"><div><span>digest (канонический, считается на клиенте)</span><strong>${digest ? esc(digest) : "— рассчитается при PutVersion"}</strong></div></div>` +
        `<div class="actions">${btn("Рассчитать digest", "ahp-version-digest", "secondary")}${btn("Опубликовать версию", "ahp-publish", "primary")}${btn("Verdict", "ahp-verdict", "secondary")}</div>` +
        gate + v,
      "F3",
    );
  }

  function runCard() {
    const f = L.frames.slice(-8);
    // Кадр = {kind, sequence, payload}: (e.seq/e.type) платформа не отдаёт.
    // На экран и в журнал уходит identity-часть (kind, sequence, имя инструмента,
    // digest+version артефакта), а не аргументы инструмента и не содержимое.
    const brief = (e) => {
      const p = (e && e.payload) || {};
      const call = p.tool_call || null;
      const content = p.content || null;
      const bits = [String(e.kind || "?"), "#" + (e.sequence ?? "?")];
      if (call && call.name) bits.push("tool=" + call.name);
      if (content && content.sha256) bits.push("artifact=" + String(content.sha256).slice(0, 12) + "@" + String(content.version || "").slice(0, 8));
      return bits.join(" ");
    };
    const frames = f.length
      ? `<div class="codebox"><pre>${esc(f.map(brief).join("\n"))}</pre></div>`
      : notice("Кадров пока нет.", "info");
    const turn = L.turn
      ? `<div class="kv"><div><span>turn_id</span><strong>${esc(L.turn.turn_id)}</strong></div><div><span>thread_id</span><strong>${esc(L.turn.thread_id)}</strong></div><div><span>context</span><strong>${esc(L.turn.context_id)}</strong></div><div><span>статус</span><strong>${statusPill(L.turn.status)}</strong></div><div><span>стрим</span><strong>${esc(L.streamState)}</strong></div></div>`
      : notice("Ход ещё не создавался.", "info");
    return panel(
      "Запуск и стрим",
      turn +
        `<div class="grid">${field("Текст хода", "ahp-run-text", "", "Проверь живой контур")}${field("max_steps", "ahp-run-steps", "6", "пусто = бюджет не назван")}${field("context_id", "ahp-context", (L.chat && L.chat.context_id) || "", "без «/»")}</div>` +
        `<div class="actions">${btn("AcceptTurn + Dispatch", "ahp-run", "primary")}${btn("CancelTurn", "ahp-cancel", "secondary")}${btn("GetTurn", "ahp-getturn", "secondary")}</div>` +
        `<div class="divider"></div><h3>Кадры (последние 8)</h3>` + frames,
      "F4 / F7",
    );
  }

  function artifactCard() {
    const a = L.artifact;
    let body = notice("Артефакта пока нет: он появляется из терминального кадра хода.", "info");
    if (a?.state === "no_artifact_ref") body = notice(a.note, "error");
    if (a?.state === "unconfirmed") body = notice(a.note, "error");
    if (a?.state === "refused") body = notice("Resolve(" + esc(a.digest.slice(0, 12)) + "…) → " + a.status + ": " + esc(a.message), "error");
    if (a?.state === "resolved") {
      body =
        `<div class="kv"><div><span>artifact_id</span><strong>${esc(a.artifact_id || "—")}</strong></div>` +
        `<div><span>version (точная)</span><strong>${esc(a.version || "—")}</strong></div>` +
        `<div><span>версий у digest</span><strong>${esc(String((a.versions || []).length))}</strong></div>` +
        `<div><span>имя</span><strong>${esc(a.name || "—")}</strong></div>` +
        `<div><span>digest</span><strong>${esc(a.digest.slice(0, 20))}…</strong></div></div>` +
        (a.download
          ? notice(
              "Получено " + a.download.bytes + " байт; SHA-256 посчитана в браузере: " + a.download.computed.slice(0, 16) +
                "… совпадает=" + (a.download.match ? "ДА" : "НЕТ") + "; файл выдан=" + (a.download.delivered ? "ДА" : "НЕТ (отказ)"),
              a.download.match ? "info" : "error",
            )
          : "");
    }
    return panel(
      "Артефакт: Resolve по digest и download со сверкой",
      body +
        `<div class="grid">${field("sha256 (64 hex)", "ahp-sha", a?.state === "resolved" ? a.digest : "", "0".repeat(16) + "…")}</div>` +
        `<div class="actions">${btn("Resolve", "ahp-resolve", "secondary")}${btn("Скачать и сверить", "ahp-download", "primary")}</div>`,
      "F5",
    );
  }

  function costCard() {
    const c = L.cost;
    if (!c) return panel("Стоимость задачи", notice("Прочитайте стоимость после запуска — запрос идёт по человеческой грани /u-ahp/v1/model/Cost?thread_id=…", "info"), "F6");
    if (c.status !== 200) return panel("Стоимость задачи", notice("Cost → " + c.status + ": " + esc(c.message), "error"), "F6");
    const trees = c.json?.trees || [];
    return panel("Расход задачи", trees.length ? table(["Единица","Вход","Выход","Всего","Списано","Остаток"],trees.map(t=>row([esc(t.unit),esc(t.tokens_in),esc(t.tokens_out),esc(t.tokens_total),esc(t.spent),esc(t.remaining)])),{footer:false}) : notice("Model Gateway не вернул бюджетных деревьев для этой задачи.") + `<div class="actions">${btn("Обновить", "ahp-cost", "secondary")}</div>`);
  }

  function memoryCard() {
    const rows=L.memoryOutcome?.rows || [];
    const settings=L.memorySettings,status=L.memoryStatus;
    const scope=L.memoryScope || "personal";
    const enabled=settings && (scope==="personal"?settings.personal_enabled:settings.project_enabled);
    const controls=`<div class="grid"><label>Область<select id="ahp-mem-scope"><option value="personal" ${scope==="personal"?"selected":""}>Личная память</option><option value="project" ${scope==="project"?"selected":""}>Память проекта</option></select></label>${scope==="project"?field("Личный Workspace", "ahp-mem-project",L.memoryProject || L.workspace?.name || ""):""}</div>`;
    const data=rows.length?table(["Факт","Область","Тип","Действия"],rows.map(m=>row([
      `<div class="memory-text">${esc(m.text)}</div><div class="sub mono">${esc(m.id)}</div>`,esc(m.kind),esc(m.factType || "—"),
      m.factType==="observation"?"Производный вывод: исправьте исходный факт":btn("Исправить","ahp-memory-edit","compact",`data-id="${esc(m.id)}"`)+btn("Убрать из использования","ahp-memory-invalidate","compact",`data-id="${esc(m.id)}"`)
    ])),{placeholder:"Фильтр по фактам"}):"";
    const operations=L.memoryOperations.length?table(["Операция","Состояние",""],L.memoryOperations.map(o=>row([esc(o.id),esc(o.state),btn("Обновить статус","ahp-memory-operation","compact",`data-id="${esc(o.id)}"`)])),{footer:false}):"";
    const settingsUI=settings?`<p>Память: ${enabled?"включена":"выключена"}. Состояние: ${esc(status?.state || "неизвестно")}. В обработке: ${esc(status?.pending_operations ?? "—")}.</p><div class="actions">${btn(enabled?"Выключить":"Включить","ahp-memory-toggle","secondary")}${btn("Очистить память","ahp-memory-clear","secondary")}</div>`+(scope==="project"?`<p>Личная память в проекте: ${settings.use_personal?"включена":"выключена"}</p>${btn("Изменить использование личной памяти","ahp-memory-personal-toggle","secondary")}<div class="grid">${field("Удалить память после дней бездействия","ahp-mem-idle",String(settings.idle_days))}</div>${btn("Сохранить срок","ahp-memory-idle","secondary")}`:""):notice("Настройки ещё не получены. Нажмите «Обновить».","info");
    return panel("Личная и проектная память",controls+`<div class="actions">${btn("Обновить","ahp-memory-search","secondary")}</div>`+
      listState(L.memoryOutcome,"Подходящих фактов пока нет.","Обновите список памяти.")+data+
      (L.memoryNote?notice(esc(L.memoryNote),"info"):"")+operations+
      `<div class="grid">${field("Текст записи или поисковый запрос","ahp-mem-text","","Что стоит помнить дальше?")}</div><div class="actions">${btn("Запомнить","ahp-memory-propose","primary")}${btn("Найти","ahp-memory-search","secondary")}</div>`+settingsUI+
      notice("Проектная память доступна только Workspace с единственным владельцем. Удаление чата сохраняет знания. Новая сессия проекта использует тот же банк.","info"),"F8");
  }

  function triggerCard() {
    const rows = (L.triggerOutcome && L.triggerOutcome.rows) || [];
    return panel(
      "Триггеры",
      listState(L.triggerOutcome, "Triggers List ответил 200 и ноль триггеров.", "Триггеров пока нет.") +
        (rows.length
          ? table(
              ["trigger_id", "Статус", "Сработало", "Лимит", "Интервал, мс", "Цель", "Действие"],
              rows.map((t) => row([
                esc(t.id), statusPill(t.status), esc(t.fired === null ? "—" : t.fired), esc(t.max === null ? "—" : t.max),
                esc(t.intervalMs === null ? "—" : t.intervalMs), esc(t.target || "—"),
                btn("Revoke", "ahp-trigger-revoke", "compact", `data-id="${esc(t.id)}"`),
              ], { status: t.status })),
              { placeholder: "Поиск" },
            )
          : "") +
        `<div class="grid">${field("Агент из каталога (ID установки)", "ahp-trg-target", L.invocation?.installation_id || L.catalog?.[0]?.installation_id || "", "ID установки")}${field("интервал, сек", "ahp-trg-interval", "60")}${field("max_fires", "ahp-trg-max", "1")}</div>` +
        `<div class="actions">${btn("CreateSchedule", "ahp-trigger-create", "secondary")}${btn("Обновить список", "ahp-trigger-list", "secondary")}${btn("FireNow (limit=1)", "ahp-trigger-fire", "secondary")}</div>`,
      "F9",
    );
  }

  function rightsCard() {
    const r = L.rights;
    const verdictText = !r
      ? notice("Ни одна ветка не проверена. Нужно ДВЕ: владелец получает свой объект, чужой — отказ; оба id настоящие.", "info")
      : r.case === "owner_positive"
        ? notice("Владелец → " + esc(r.id) + ": HTTP " + r.status + " · " + esc(r.message), r.status === 200 ? "info" : "error")
        : notice("Чужой объект → " + esc(r.id) + ": HTTP " + r.status + " · " + esc(r.message), r.status === 403 || r.status === 404 ? "info" : "error");
    return panel(
      "Права: владелец видит, чужой — нет",
      verdictText +
        `<div class="grid">${field("id объекта второго tenant", "ahp-foreign-id", "")}</div>` +
        `<div class="actions">${btn("Владелец: свой артефакт", "ahp-rights-own", "secondary")}${btn("Чужой: отказ", "ahp-rights-foreign", "secondary")}${btn("Workspace-проба", "ahp-workspace", "secondary")}</div>`,
      "F10 / F11",
    );
  }

  const unavailableCard = () =>
    panel(
      "Честно недоступно в текущем прототипе",
      UNAVAILABLE.map(([code, title, why]) => notice(code + " · " + title + " — " + why, "info")).join("") +
        notice("Sandbox показывает backend и маркер MOCK из ответа запуска среды.", "info"),
      "F12",
    );

  const auditCard = () =>
    panel(
      "Журнал ног (ids/коды/числа, без содержимого)",
      `<div class="codebox"><pre>${esc(L.audit.slice(0, 20).map((a) => a.t + " " + a.code + " " + a.detail).join("\n") || "пусто")}</pre></div>`,
    );

  const platesBar = () =>
    L.plates.length
      ? `<div class="page-heading"><div><h2>Не удалось выполнить запрос</h2><p class="muted">Проверьте причину и повторите действие.</p></div></div>` +
        L.plates.map((p) => notice("[" + esc(p.code) + "]" + (p.status ? " HTTP " + p.status : "") + " · " + esc(p.message), "error")).join("")
      : "";

  function computeCard() { return panel("Sandbox", notice("Управление средой и подтверждённый backend доступны в разделе Sandbox.") + btn("Открыть Sandbox","ahp-compute-probe","secondary")); }

  function chatsCard() {
    const rows = (L.chatOutcome && L.chatOutcome.rows) || [];
    return panel(
      "Беседы (State)",
      listState(L.chatOutcome, "ListChats ответил 200 и ноль бесед: ходов ещё не создавалось.", "Беседы ещё не запрошены.") +
        (rows.length
          ? table(
              ["thread_id", "context_id", "Создана", ""],
              rows.map((c) => row([
                esc(c.thread_id), esc(c.context_id), esc(c.created_at || "—"),
                btn("Открыть", "ahp-chat-open", "compact", `data-id="${esc(c.thread_id)}"`),
              ])),
              { placeholder: "Поиск по беседам" },
            )
          : "") +
        `<div class="actions">${btn("Обновить беседы", "ahp-chats", "secondary")}${btn("Открыть последнюю", "ahp-chat-last", "secondary")}</div>` +
        (L.turns && L.turns.length ? notice("Ходов в выбранной беседе: " + L.turns.length + " (ListTurns)", "info") : ""),
      "F4 / reload",
    );
  }

  function liveView() {
    if (!SHAPES) {
      return heading("Живой контур", "Модуль форм ответа не загружен") +
        notice("aifactory/ahp-shapes.js не загрузился, поэтому ответы платформenе разбираются. Раздел намеренно пуст: рисовать неразобранный JSON как данные нельзя.", "error");
    }
    return (
      heading("Живой контур", "Настоящие API платформы на одном origin: " + location.host) +
      platesBar() +
      `<div class="grid-2">${sessionCard()}${chatsCard()}</div>` +
      `<div class="grid-2">${catalogCard()}${publishCard()}</div>` +
      `<div class="grid-2">${runCard()}${artifactCard()}</div>` +
      `<div class="grid-2">${costCard()}${memoryCard()}</div>` +
      `<div class="grid-2">${triggerCard()}${rightsCard()}</div>` +
      `<div class="grid-2">${computeCard()}</div>` +
      `<div class="grid-2">${unavailableCard()}</div>` +
      auditCard()
    );
  }

  const LOCAL_ACTIONS = new Set([
    "close", "outside", "mobile-nav", "navigate", "docs", "copy-code", "wizard-next", "wizard-prev",
    "stack-menu", "component", "integration", "tags", "tags-save", "project-save", "notifications",
  ]);

  const REAL_BY_MOCK = {
    task: "ahp-run", "task-run": "ahp-run", stop: "ahp-cancel", "stop-confirm": "ahp-cancel",
    "artifact-download": "ahp-download", "eval-run": "ahp-verdict", "eval-report": "ahp-verdict",
    deploy: "ahp-deploy-guide", "guide-deploy": "ahp-deploy-guide", rollback: "ahp-deploy-guide",
    "repo-new": "repositories_not_wired", "repo-save": "repositories_not_wired", "repo-detail": "repositories_not_wired",
    "repo-edit": "repositories_not_wired", "repo-sync": "repositories_not_wired", "repo-delete": "repositories_not_wired",
    "env-new": "environments_not_wired", "env-save": "environments_not_wired", "env-detail": "environments_not_wired",
    "env-edit": "environments_not_wired", "env-delete": "environments_not_wired",
    "sandbox-detail": "ahp-compute-probe", "snapshot-detail": "ahp-compute-probe", "snapshot-download": "ahp-compute-probe",
    "ci-simulate": "ci_registration_link_missing", "config-download": "ci_registration_link_missing",
    "eval-download": "ci_registration_link_missing", "eval-fail": "evals_write_not_human",
  };

  async function hook(name, el) {
    if (await profileAdmin?.hook(name,el)) return true;
    if (String(name).startsWith("ahp-")) return run(name, el);
    if (name === "refresh") {
      const page=route()[0].split("?")[0];
      if(page==="workspace") await openWorkspace(false,L.workspace?.name);
      else if(page==="sandbox") await computeAction("GetComputer");
      else if(page==="stats" || page==="chats") await cost();
      else if(page==="memory") await searchMemory(val("ahp-mem-text"));
      else if(page==="crons") await listTriggers();
      else if(page==="stacks") await loadCatalog();
      else await loadInitialData();
      return true;
    }
    if (name === "docs") { openModal("О рабочем пространстве", notice("Интерфейс подключён к AHP: реестр агентов, память, расписания и чаты используют серверные API. Недоступное действие показывает причину отказа. MOCK в Sandbox означает симуляцию MicroVM, без гарантии изоляции."), btn("Закрыть","close")); return true; }
    if (LOCAL_ACTIONS.has(name)) return false;
    const mapped = REAL_BY_MOCK[name];
    if (mapped && String(mapped).startsWith("ahp-")) return run(mapped, el);
    if (mapped) { plate(mapped, "Действие «" + name + "» не подключено: " + WHY(mapped), ""); return true; }
    plate("action_not_wired", "Действие «" + name + "» не подключено к платформе: результат не выдаётся, чтобы не выдавать ложный успех.", "");
    return true;
  }

  const WHY = (code) => (UNAVAILABLE.find((u) => u[0] === code) || [, "нет product-грани для этого действия"])[1];

  function val(name) {
    const f = document.querySelector(`[name="${name}"]`);
    return f ? f.value.trim() : "";
  }

  async function run(name, el) {
    if (name !== "ahp-login-modal" && !S.online && name !== "ahp-login") {
      plate("session_missing", "Сначала войдите — платформы без сессии нет.", "");
      loginModal();
      return true;
    }
    switch (name) {
      case "ahp-login-modal": loginModal(); return true;
      case "ahp-login": {
        const u = val("ahp-user"), p = document.querySelector('[name="ahp-pass"]')?.value || "";
        if (!u || !p) return plate("login_inputs", "Нужны пользователь и пароль"), true;
        let ok;
        try { ok = await login(u, p); }
        finally { const input = document.querySelector('[name="ahp-pass"]'); if (input) input.value = ""; }
        if (ok) { closeModal(); await loadInitialData(); }
        return true;
      }
      case "ahp-approvals": await approvals(); return true;
      case "ahp-connections": await connections(); return true;
      case "ahp-connect": await connectPersonal(el.dataset.connection); return true;
      case "ahp-disconnect": await disconnectPersonal(el.dataset.connection); return true;
      case "ahp-approve": await approve(el?.dataset?.id); return true;
      case "ahp-workspace-open": await openWorkspace(); return true;
      case "ahp-workspace-create": await openWorkspace(true); return true;
      case "ahp-workspace-file": await workspaceFile(el?.dataset?.id); return true;
      case "ahp-workspace-upload": await uploadWorkspaceFile(); return true;
      case "ahp-workspace-candidate": await reviewWorkspaceCandidate(el?.dataset?.id); return true;
      case "ahp-workspace-check": await checkWorkspaceCandidate(); return true;
      case "ahp-workspace-publish": await publishWorkspaceCandidate(); return true;
      case "ahp-workspace-reject": await rejectWorkspaceCandidate(); return true;
      case "ahp-workspace-revision": await workspaceRevision(el?.dataset?.id); return true;
      case "ahp-workspace-add-member": await workspaceMember("AddMember",val("ahp-workspace-member")); return true;
      case "ahp-workspace-remove-member": await workspaceMember("RemoveMember",el?.dataset?.id); return true;
      case "ahp-compute-create": await computeAction("CreateComputer"); return true;
      case "ahp-compute-get": await computeAction("GetComputer"); return true;
      case "ahp-compute-wake": await computeAction("Wake"); return true;
      case "ahp-compute-sleep": await computeAction("Sleep"); return true;
      case "ahp-compute-stop": await computeAction("StopIncarnation"); return true;
      case "ahp-invoke-open": invokeForm(el?.dataset?.id); return true;
      case "ahp-invoke": await invokeInstallation(el?.dataset?.id,val("ahp-invoke-text")); return true;
      case "ahp-invoke-refresh": await observeInvocation(); return true;
      case "ahp-eval-run": await runEvaluation(val("ahp-eval-suite"),val("ahp-eval-digest")); return true;
      case "ahp-deployments": await deployments(el?.dataset?.id); return true;
      case "ahp-install": {
        let granted;try {granted=JSON.parse(val("ahp-install-granted") || '{"self_tasks":0}');} catch {plate("capabilities_json","Разрешённые возможности должны быть JSON-объектом.");return true;}
        await registryChange("Install", {agent_id:val("ahp-install-agent"),version_id:val("ahp-install-version"),environment:val("ahp-install-env"),key:crypto.randomUUID(),granted}); return true;
      }
      case "ahp-promote": await registryChange("Promote", {installation_id:el?.dataset?.id,version_id:val("ahp-promote-version")}); return true;
      case "ahp-rollback": await registryChange("Rollback", {installation_id:el?.dataset?.id}); return true;
      case "ahp-chat-new": pollGeneration++; liveStreamAbort?.abort(); L.turn = null; L.chat = null; L.activeAgentInstallation=""; L.projectMemoryProfile=""; L.turnHistory=[]; L.turns=[]; L.frames = []; L.artifact = null; L.cost = null; L.userMessage = ""; history.replaceState(null,"", "#/chats"); render(); return true;
      case "ahp-logout": await logout(); return true;
      case "ahp-catalog": await loadCatalog(); return true;
      case "ahp-register": await registerAgent(val("ahp-agent")); return true;
      case "ahp-version-digest": await publishVersion(val("ahp-pub-agent"),true); return true;
      case "ahp-publish": await publishVersion(val("ahp-pub-agent")); return true;
      case "ahp-verdict": await verdict(L.publish?.digest || L.verdict?.digest || "sha256:" + "0".repeat(64)); return true;
      case "ahp-deploy-guide": go("live"); plate("publish_gate", "Публикация — в разделе «Публикация версии»: без passing-verdict платформа отвечает 409.", ""); return true;
      case "ahp-run": await runTurn(val("ahp-run-text") || DEFAULT_RUN_TEXT); return true;
      case "ahp-cancel": await cancelTurn(); return true;
      case "ahp-getturn": {
        if (!L.turn) return plate("no_turn", "Хода нет");
        const t = await space("GetTurn", { turn_id: L.turn.turn_id });
        L.turn.status = t.json?.status || t.json?.turn?.status || "unknown";
        audit("get_turn", t.status + " " + L.turn.status);
        paint();
        return true;
      }
      case "ahp-resolve": await resolve(val("ahp-sha")); return true;
      case "ahp-download": await download(); return true;
      case "ahp-cost": await cost(); return true;
      case "ahp-compute-probe": await probeCompute(); return true;
      case "ahp-memory-propose": await proposeMemory(val("ahp-mem-text")); return true;
      case "ahp-memory-search": await searchMemory(val("ahp-mem-text")); return true;
      case "ahp-trigger-create": await createTrigger(val("ahp-trg-target"), val("ahp-trg-interval"), val("ahp-trg-max")); return true;
      case "ahp-trigger-list": await listTriggers(); return true;
      case "ahp-trigger-fire": await fireTrigger(1); return true;
      case "ahp-trigger-revoke": await revokeTrigger(el?.dataset?.id || val("ahp-mem-id")); return true;
      case "ahp-memory-toggle": await updateMemorySettings("enabled"); return true;
      case "ahp-memory-personal-toggle": await updateMemorySettings("personal"); return true;
      case "ahp-memory-idle": await updateMemorySettings("idle"); return true;
      case "ahp-memory-clear": await clearMemory(); return true;
      case "ahp-memory-edit": await editMemory(el?.dataset?.id); return true;
      case "ahp-memory-invalidate": await editMemory(el?.dataset?.id,true); return true;
      case "ahp-memory-operation": await refreshMemoryOperation(el?.dataset?.id); return true;
      case "ahp-chats": await loadChats(); return true;
      case "ahp-chat-open": await openChat(el?.dataset?.id); history.replaceState(null,"", "#/chats?thread=" + encodeURIComponent(el?.dataset?.id)); paint(); return true;
      case "ahp-chat-last": await openChat(); return true;
      case "ahp-workspace": await probeWorkspace(); return true;
      case "ahp-rights-own": await probeOwn(); return true;
      case "ahp-rights-foreign": await probeForeign(val("ahp-foreign-id")); return true;
      default: return false;
    }
  }

  // Original example sections now render the same server state as diagnostics.
  const dateText = value => value ? new Date(value).toLocaleString("ru-RU") : "—";
  function memorySection() {
    return heading("Память", "Личные предпочтения и знания выбранного проекта", "") + memoryCard();
  }
  function triggersSection() {
    const rows = L.triggerOutcome?.rows || [];
    return heading("Crons", "Ограниченные расписания запуска задач", btn("Обновить", "ahp-trigger-list", "secondary")) +
      panel("Расписания", listState(L.triggerOutcome, "Расписаний пока нет.", "Загружаем расписания…") +
        (rows.length ? table(["Расписание", "Состояние", "Период", "Выполнено", "Следующий запуск", ""], rows.map(t => row([
          `${esc(t.target)}<div class="sub mono">${esc(t.id)}</div>`, statusPill(t.status), t.intervalMs == null ? "—" : esc(t.intervalMs / 1000) + " сек",
          `${esc(t.fired)} / ${esc(t.max ?? "—")}`, dateText(t.nextFireAt), t.status === "ACTIVE" ? btn("Отключить", "ahp-trigger-revoke", "compact", `data-id="${esc(t.id)}"`) : "",
        ], {status:t.status})), {placeholder:"Фильтр по расписаниям"}) : "")) +
      panel("Создать расписание", `<div class="grid cols3">${field("Агент из каталога (ID установки)", "ahp-trg-target", L.invocation?.installation_id || L.catalog?.[0]?.installation_id || "", "ID установки")}${field("Интервал, секунд", "ahp-trg-interval", "60", "", "number")}${field("Максимум запусков", "ahp-trg-max", "1", "", "number")}</div><div class="actions">${btn("Создать расписание", "ahp-trigger-create", "primary")}</div>` + notice("Периодическое задание имеет конечный лимит запусков. Его состояние и количество срабатываний возвращает платформа."));
  }
  function stacksSection() {
    const rows = L.catalog || [];
    return heading("Agent Stacks", "Установленные агентские системы из реестра платформы", btn("Обновить", "ahp-catalog", "secondary")) +
      panel("Агентские системы", listState(L.catalogOutcome, "Установленных систем пока нет. Зарегистрируйте агента и опубликуйте проверенную версию.", "Загружаем каталог…") +
        (rows.length ? table(["Название", "Окружение", "Публикация", "Версия", ""], rows.map(c => row([
          `${esc(c.agent_id)}<div class="sub mono">${esc(c.installation_id)}</div>`, esc(c.environment), statusPill(c.published ? "published" : "unpublished"),
          `<span class="mono">${esc(c.digest?.slice(0, 22) || c.version_id)}</span>`, btn("Запустить задачу", "ahp-invoke-open", "compact", `data-id="${esc(c.installation_id)}"`) + btn("История", "ahp-deployments", "compact", `data-id="${esc(c.installation_id)}"`),
        ])), {placeholder:"Поиск по системам"}) : "")) +
      invocationCard() + evalSection() + `<div class="grid cols2">${panel("Регистрация агента", `<div class="grid">${field("Идентификатор агента", "ahp-agent", "", "my-agent")}</div><div class="actions">${btn("Зарегистрировать", "ahp-register", "primary")}</div>` + (L.registered ? notice("Зарегистрирован: " + esc(L.registered.agent_id || L.registered.id)) : ""))}${publishCard()}</div>` +
      panel("Установка версии", `<div class="grid cols3">${field("Агент", "ahp-install-agent", L.registered?.agent_id || L.registered?.id || "")}${field("ID опубликованной версии", "ahp-install-version", L.publish?.version?.id || "")}${field("Окружение", "ahp-install-env", "test")}</div><label class="field"><span>Разрешённые возможности, JSON</span><textarea name="ahp-install-granted" rows="3">${esc(JSON.stringify(L.versionDraft?.manifest?.capabilities || {self_tasks:0}))}</textarea></label><div class="actions">${btn("Установить", "ahp-install", "primary")}</div>` + notice("Установка доступна для версии, прошедшей оценки. Факт записи в реестр не заменяет проверку исполнения агента."));
  }
  function invokeForm(id) {
    const card=L.catalog?.find(c=>c.installation_id===id);
    if(!card) return plate("installation_missing","Установка не найдена в загруженном каталоге.");
    openModal("Задача для "+card.agent_id, notice("Будет вызвана выбранная установка: "+esc(id)) + `<label class="field"><span>Задача агенту</span><textarea name="ahp-invoke-text" rows="4"></textarea></label>`, btn("Запустить", "ahp-invoke", "primary", `data-id="${esc(id)}"`)+btn("Отмена","close"));
  }
  async function invokeInstallation(id,text) {
    if(!id || !text) return plate("invoke_input","Выберите установку и введите задачу.");
    const card=L.catalog?.find(c=>c.installation_id===id);
    const r=await raw("/u-ahp/v1/registry/Invoke",{body:{installation_id:id,input:{text}}});
    if(r.status!==200 || !r.json?.task_id) return plate("invoke_refused","Запуск агента: "+errText(r),r.status);
    L.invocation={installation_id:id,agent_id:card?.agent_id || "",task_id:r.json.task_id,status:"accepted",observed:false};
    unplate("invoke_refused");closeModal();paint();
    history.replaceState(null,"", "#/stacks?installation="+encodeURIComponent(id)+"&task="+encodeURIComponent(r.json.task_id));
    observeInvocation(true).catch(e=>plate("invoke_observe_error",e.message));
    return L.invocation;
  }
  async function observeInvocation(wait=false) {
    const current=L.invocation;
    if(!current?.task_id) return;
    for(let i=0;i<(wait?120:1);i++) {
      if(!S.online || L.invocation!==current) return;
      const r=await raw("/u-ahp/v1/registry/GetInvokedTask",{body:{installation_id:current.installation_id,task_id:current.task_id}});
      if(r.status!==200) return plate("invoke_observe_refused","Статус выбранного агента: "+errText(r),r.status);
      const task=r.json?.task;
      if(!task || task.id!==current.task_id || !task.status?.state) return plate("invoke_task_shape","Runtime не вернул состояние ожидаемой задачи.");
      current.status=task.status.state;current.task=task;current.observed=true;
      unplate("invoke_observe_refused");paint();
      if(["completed","failed","canceled","rejected"].includes(current.status)) return current;
      if(!wait) return current;
      await new Promise(r=>setTimeout(r,1000));
    }
    current.observationPaused=true;paint();
  }
  function invocationCard() {
    const inv=L.invocation;if(!inv) return "";
    const messages=[...(inv.task?.history || []),...(inv.task?.artifacts || []),...(inv.task?.status?.message ? [inv.task.status.message] : [])];
    const text=messages.flatMap(m=>(m.parts || []).filter(p=>p.kind==="text").map(p=>p.text)).join("\n");
    return panel("Задача выбранного агента",`<div class="kv"><div><span>Установка</span><strong>${esc(inv.agent_id || inv.installation_id)}</strong></div><div><span>Задача</span><strong>${esc(inv.task_id)}</strong></div><div><span>Состояние runtime</span><strong>${statusPill(inv.status)}</strong></div></div>` +
      (inv.observed ? "" : notice("Задача принята. Завершение ещё не подтверждено runtime.")) + (text ? `<div class="ahp-message">${esc(text)}</div>` : "") +
      (inv.observationPaused ? notice("Окно ожидания завершилось. Задача не объявляется завершённой; обновите её состояние.") : "") + btn("Обновить состояние","ahp-invoke-refresh","secondary"));
  }
  async function runEvaluation(suite,digest) {
    if(!suite || !digest) return plate("eval_inputs","Укажите операторский набор оценок и digest версии.");
    const r=await raw("/u-ahp/v1/evals/RunEval",{body:{suite_id:suite,agent_digest:digest}});
    if(r.status!==200 || !r.json?.verdict) return plate("eval_run_refused","Оценки: "+errText(r),r.status);
    if(!["pass","fail"].includes(r.json.verdict.outcome) || r.json.verdict.agent_digest!==digest || r.json.verdict.task_set_id!==suite) return plate("eval_run_shape","Evals не вернул вердикт для выбранного набора и digest.");
    L.evalRun=r.json.verdict;L.verdict={digest,status:r.status,verdict:r.json.verdict,message:r.json.verdict.outcome};
    unplate("eval_run_refused");paint();return L.evalRun;
  }
  function evalSection() {
    const v=L.evalRun;
    return panel("Оценки перед публикацией",`<div class="grid">${field("Операторский набор оценок", "ahp-eval-suite", "", "suite-id")}${field("Digest версии", "ahp-eval-digest", L.versionDraft?.digest || L.publish?.digest || L.verdict?.digest || "")}</div>` +
      `<div class="actions">${btn("Запустить оценки", "ahp-eval-run", "primary")}</div>` + (v ? notice("Результат: "+esc(v.outcome),v.outcome==="pass"?"info":"error") : notice("Набор оценок и его эталонные задания задаёт оператор платформы. Статус публикуется только после ответа Evals.")));
  }
  async function deployments(id) {
    const r = await raw("/u-ahp/v1/registry/ListDeployments", {body:{installation_id:id,limit:50}});
    if (r.status !== 200) return plate("deployments_refused", "История: " + errText(r), r.status);
    const got = SHAPES.deployments(r.json);
    if (got.missing.length) return plate("deployments_shape", got.missing.join(", "));
    openModal("История обновлений", (got.rows.length ? table(["Действие", "Версия", "Когда"],got.rows.map(d=>row([esc(d.action),esc(d.digest),dateText(d.createdAt)]))) : notice("Обновлений пока нет.")) +
      `<div class="divider"></div>${field("ID новой версии", "ahp-promote-version", "")}`, btn("Применить версию", "ahp-promote", "primary", `data-id="${esc(id)}"`) + btn("Откатить к предыдущей", "ahp-rollback", "secondary", `data-id="${esc(id)}"`) + btn("Закрыть", "close"), {wide:true});
  }
  async function registryChange(operation, body) {
    const r = await raw("/u-ahp/v1/registry/" + operation, {body});
    if (r.status !== 200) return plate("registry_change", operation + ": " + errText(r), r.status);
    unplate("registry_change"); closeModal(); await loadCatalog();
    toast("Реестр подтвердил изменение версии");
  }
  function chatArtifact() {
    const a=L.artifact;
    if(a?.state!=="resolved") return a ? notice(esc(a.note || a.message || "Файл пока не доступен.")) : "";
    return panel("Файл результата", `<div class="actions"><strong>${esc(a.name || a.artifact_id)}</strong><span class="sub">${a.size == null ? "" : esc(a.size)+" байт"}</span>${btn("Скачать файл", "ahp-download", "primary")}</div>` +
      (a.download ? notice(a.download.match ? "Контрольная сумма совпала. Файл передан для скачивания." : "Контрольная сумма не совпала. Файл не выдан.", a.download.match ? "info" : "error") : "") +
      `<details><summary>Версия и контрольная сумма</summary><div class="kv"><div><span>Артефакт</span><strong>${esc(a.artifact_id)}</strong></div><div><span>Версия</span><strong>${esc(a.version)}</strong></div><div><span>SHA-256</span><strong>${esc(a.digest)}</strong></div></div></details>`);
  }
  function projectMemoryControl() {
    const binding=SHAPES.projectMemoryBinding({current:L.chat,workspace:L.workspace,selectedProfile:L.projectMemoryProfile});
    if(!binding.projectID && !L.workspace) return "";
    return `<label class="field"><span>Память проекта</span><select id="ahp-project-memory" name="ahp-project-memory" ${binding.locked?"disabled":""}><option value="" ${binding.profile!=="shared"?"selected":""}>Личная память проекта</option><option value="shared" ${binding.profile==="shared"?"selected":""}>Общая память проекта</option></select><span class="sub">${binding.locked?"Режим закреплён за этим диалогом.":"Общие факты доступны участникам с учётом прав и согласования."}</span></label>`;
  }
  function conversationAgentControl() {
	const current=L.chat || L.turn;
	if(current?.thread_id) {
	  const installation=current.agent_installation_id || "";
	  const card=L.catalog?.find(c=>c.installation_id===installation);
	  return `<label class="field"><span>Собеседник</span><input value="${esc(installation?(card?.agent_id || installation):"Личный Dot")}" disabled><span class="sub">Закреплён за этой беседой.</span></label>`;
	}
	const cards=(L.catalog || []).filter(c=>c.published && c.kind==="agent" && c.agent_id!=="agents-space");
	return `<label class="field"><span>Собеседник</span><select name="ahp-agent-target"><option value="" ${L.activeAgentInstallation?"":"selected"}>Личный Dot</option>${cards.map(c=>`<option value="${esc(c.installation_id)}" ${L.activeAgentInstallation===c.installation_id?"selected":""}>${esc(c.agent_id)}</option>`).join("")}</select><span class="sub">Выберите агента для новой беседы.</span></label>`;
  }
  function chatSection() {
    const textFrames = SHAPES.modelMessages(L.frames);
    const turn = L.turn;
    const threads = L.chats || [];
    const renderFrame=f=>`<article class="ahp-message"><span class="sub">Агент · ${f.partial ? "Частичный ответ" : esc(f.kind)}</span><div>${esc(f.payload.content)}</div></article>`;
    const assigned=new Set();
    const history=(L.turnHistory || []).map(t=>{
      const output=textFrames.filter(f=>t.run_id && String(f.key).startsWith(t.run_id+":"));output.forEach(f=>assigned.add(f));
      return `<article class="ahp-message user"><span class="sub">Вы · ${dateText(t.created_at)}</span><div>${esc(t.input?.content || "")}</div></article>`+output.map(renderFrame).join("");
    }).join("");
    const conversation=history+textFrames.filter(f=>!assigned.has(f)).map(renderFrame).join("");

    return heading("Чаты", "История задач хранится на платформе", btn("Новый чат", "ahp-chat-new", "primary") + btn("Обновить", "ahp-chats", "secondary")) +
      `<div class="ahp-chat-layout"><section class="panel ahp-chat-list"><h2>Беседы</h2>${threads.length ? threads.map(c=>`<button class="ahp-chat-link ${turn?.thread_id===c.thread_id?'selected':''}" data-action="ahp-chat-open" data-id="${esc(c.thread_id)}"><strong>${esc(c.context_id)}</strong><span class="sub">${dateText(c.created_at)}</span></button>`).join("") : notice("Бесед пока нет.")}</section>` +
      panel(turn ? esc(turn.context_id) : "Новая беседа", (turn ? `<div class="actions">${statusPill(turn.status)}<span class="sub">${esc(turn.thread_id)}</span></div>` : "") +
        `<div class="ahp-conversation">${conversation || notice("Отправьте сообщение. Ответ и результаты работы появятся здесь.")}</div>` +
        `<div class="grid">${field("Название беседы", "ahp-context", turn?.context_id || L.chat?.context_id || "", "my-task")}${conversationAgentControl()}${projectMemoryControl()}<label class="field"><span>Сообщение агенту</span><textarea name="ahp-run-text" rows="3" placeholder="Опишите задачу"></textarea></label>${field("Бюджет шагов", "ahp-run-steps", "8", "", "number")}</div><div class="actions">${btn("Отправить", "ahp-run", "primary", turn && !SHAPES.isTerminalStatus(turn.status) ? "disabled" : "")}${turn && !SHAPES.isTerminalStatus(turn.status) ? btn("Остановить", "ahp-cancel", "secondary") : ""}</div>` +
        (L.artifact ? chatArtifact() : "") + (L.cost ? costCard() : "")) + `</div>`;
  }
  function overviewSection() {
    return heading("Обзор", "Agent Harness Platform · рабочее пространство") +
      `<section class="panel hero"><div class="hero-copy"><span class="eyebrow">AI FACTORY / AGENT HARNESS PLATFORM</span><h2>Агенты, знания и задачи в одном месте</h2><p>Запускайте задачи в чатах, сохраняйте знания и управляйте агентскими системами. Данные загружаются из вашей сессии платформы.</p><div class="actions">${btn("Открыть чаты", "navigate", "primary", 'data-path="chats"')}${btn("Agent Stacks", "navigate", "secondary", 'data-path="stacks"')}</div></div><img class="hero-image" src="assets/ai-agents.png" alt="AI Agents"></section>` +
      `<div class="grid cols4">${metric("Agent Stacks", L.catalog?.length ?? "—", "в каталоге", "stack", "stacks")}${metric("Память", L.memoryOutcome?.rows?.length ?? "—", "найдено записей", "memory", "memory")}${metric("Crons", L.triggers?.length ?? "—", "доступно расписаний", "cron", "crons")}${metric("Чаты", L.chats?.length ?? "—", "в последней странице", "agent", "chats")}</div>`;
  }
  async function connections() {
    const r = await raw("/u-ahp/v1/tools/OAuthConnections", {body:{}});
    L.connections = r.status === 200 && Array.isArray(r.json?.connections) ? r.json.connections : null;
    if (r.status !== 200) return plate("connections_refused", "Подключения: " + errText(r), r.status);
    unplate("connections_refused"); paint();
  }
  async function connectPersonal(connection) {
    const r = await raw("/u-ahp/v1/tools/OAuthBegin", {body:{connection},retry:false});
    if (r.status !== 200 || !r.json?.authorize_url) return plate("connection_refused", "Не удалось открыть согласие: " + errText(r), r.status);
    // Only the gateway supplies this URL; no agent content becomes an authorization link.
    location.assign(r.json.authorize_url);
  }
  async function disconnectPersonal(connection) {
    const r = await raw("/u-ahp/v1/tools/OAuthRevoke", {body:{connection},retry:false});
    if (r.status !== 200) return plate("connection_refused", "Отключение требует повторной проверки: " + errText(r), r.status);
    toast("Личный аккаунт отключён"); await connections();
  }
  async function completePersonalOAuth() {
    if (!pendingOAuthCallback) return;
    const response = pendingOAuthCallback; pendingOAuthCallback = null;
    const r = await raw("/u-ahp/v1/tools/OAuthCallback", {body:response,retry:false});
    if (r.status !== 200) plate("connection_refused", "Подключение не подтверждено. Начните подключение заново: " + errText(r), r.status);
    else toast(response.error ? "Согласие отменено" : "Личный аккаунт подключён");
    await connections();
  }
  function connectionsSection() {
    const names = {disconnected:"Не подключён",auth_required:"Требуется подключение",active:"Подключён",revoked:"Отключён",refresh_unknown:"Требуется повторное подключение"};
    return heading("Подключения", "Ваши личные аккаунты. Согласие подтверждается у провайдера.", btn("Обновить", "ahp-connections", "secondary")) +
      panel("Личные подключения", !L.connections ? notice("Нажмите «Обновить», чтобы увидеть доступные подключения.") : !L.connections.length ? notice("Провайдеры не зарегистрированы.") :
        table(["Подключение","Состояние",""],L.connections.map(c=>row([esc(c.connection),esc(names[c.status]||c.status),
          btn(c.status==="active"?"Подключить заново":"Подключить", "ahp-connect","compact",`data-connection="${esc(c.connection)}"`) +
          (c.status!=="disconnected"?btn("Отключить", "ahp-disconnect","secondary compact",`data-connection="${esc(c.connection)}"`):"")]))));
  }

  async function approvals() {
    const r = await raw("/u-ahp/v1/tools/Approvals", {body:{limit:50}});
    L.approvals = r.status === 200 && Array.isArray(r.json?.approvals) ? r.json.approvals : null;
    if (r.status !== 200) return plate("approvals_refused", "Согласования: " + errText(r), r.status);
    unplate("approvals_refused"); paint();
  }
  async function approve(id) {
    const item = L.approvals?.find(a=>a.request_id===id);
    if (!item) return plate("approval_missing", "Запрос не найден в текущем списке. Обновите согласования.");
    const r = await raw("/u-ahp/v1/tools/Approve", {body:{request_id:item.request_id,digest:item.digest,tool:item.tool}});
    if (r.status !== 200 || r.json?.approved !== true) return plate("approval_refused", "Подтверждение: " + errText(r), r.status);
    unplate("approval_refused"); toast("Подтверждена одна операция инструмента"); await approvals();
  }
  function approvalsSection() {
    return heading("Согласования", "Операции инструментов, ожидающие вашего решения", btn("Обновить", "ahp-approvals", "secondary")) + panel("Ожидают решения", L.approvals === undefined || L.approvals === null ? notice("Нажмите «Обновить», чтобы запросить доступные согласования.") : !L.approvals.length ? notice("Ожидающих запросов нет.") :
      table(["Инструмент", "Операция", "Срок", ""], L.approvals.map(a=>row([esc(a.tool),`${esc(a.operation_key)}<div class="sub mono">${esc(a.digest)}</div>`,dateText(a.expires_at),btn("Подтвердить операцию", "ahp-approve", "compact", `data-id="${esc(a.request_id)}" ${Date.parse(a.expires_at) <= Date.now() ? "disabled" : ""}`)]))));
  }
  async function computeAction(operation) {
    const id = L.computer?.id || val("ahp-computer-id");
    if (operation !== "CreateComputer" && !id) return plate("computer_missing", "Создайте среду или укажите её идентификатор.");
    const body = operation === "CreateComputer" ? {class:"X",profile:"ephemeral",key:crypto.randomUUID()} : {computer_id:id};
    if (operation === "StopIncarnation") {
      if (!L.incarnation?.id) return plate("incarnation_missing", "Сначала запустите среду или разбудите её.");
      body.incarnation_id=L.incarnation.id;
    }
    const r = await raw("/u-ahp/v1/compute/" + operation, {body});
    if (r.status !== 200) return plate("compute_refused", "Sandbox: " + errText(r), r.status);
    if (operation === "CreateComputer" || operation === "GetComputer") L.computer=r.json;
    if (operation === "Wake") L.incarnation=r.json;
    if (r.computeDisclosure) L.computeDisclosure=r.computeDisclosure;
    if (operation === "StopIncarnation") L.incarnation=null;
    L.computeLast={operation,at:nowISO(),response:r.json};
    unplate("compute_refused");
    if (!["GetComputer","CreateComputer"].includes(operation)) await computeAction("GetComputer");
    paint();
  }
  function sandboxSection() {
    const c=L.computer, inc=L.incarnation;
    const mock=inc?.backend === "mock";
    return heading("Sandbox", "Рабочие среды агентов", btn("Создать среду", "ahp-compute-create", "primary")) +
      panel("Среда исполнения", (mock ? `<div class="actions">${badge("MOCK", "error")}</div>` + notice(esc(L.computeDisclosure || "Симуляция MicroVM: гипервизор и изоляция отсутствуют.")) : notice("Тип среды подтверждается ответом запуска. Создание записи само по себе не запускает MicroVM.")) +
        `<div class="grid">${field("Идентификатор среды", "ahp-computer-id", c?.id || "")}</div><div class="actions">${btn("Открыть", "ahp-compute-get", "secondary")}${btn("Запустить / разбудить", "ahp-compute-wake", "primary")}${btn("Усыпить", "ahp-compute-sleep", "secondary")}${btn("Остановить", "ahp-compute-stop", "secondary")}</div>` +
        (c ? table(["Среда", "Класс", "Профиль", "Состояние", "Backend"],[row([esc(c.id),esc(c.class),esc(c.profile),statusPill(c.status),esc(inc?.backend || "не запущен")])],{footer:false}) : notice("Среда ещё не выбрана.")) +
        (L.computeLast ? `<p class="sub">Последний подтверждённый запрос: ${esc(L.computeLast.operation)} · ${dateText(L.computeLast.at)}</p>` : ""));
  }
  document.addEventListener("change", event => {
    if(event.target.id === "ahp-workspace-upload-file") L.workspaceUploadFile=event.target.files?.[0] || null;
  });
  const workspaceRPC = (operation,body,options={}) => raw("/u-ahp/v1/workspace/"+operation,{body,...options});
  let workspaceLoadGeneration=0,workspaceReviewGeneration=0;
  function workspaceTarget() {
    if(!L.workspace)return null;
    const workspace={...L.workspace},generation=workspaceLoadGeneration,epoch=sessionEpoch;
    return {workspace,current:()=>generation===workspaceLoadGeneration && epoch===sessionEpoch && L.workspace?.id===workspace.id};
  }
  function workspaceCandidateTarget() {
    const target=workspaceTarget(),review=L.workspaceReview,generation=workspaceReviewGeneration;
    if(!target || review?.candidate?.workspace_id!==target.workspace.id)return null;
    return {...target,review,current:()=>target.current() && generation===workspaceReviewGeneration && L.workspaceReview===review};
  }
  async function openWorkspace(create=false,selectedName) {
    const name=selectedName || val("ahp-workspace-name") || L.workspace?.name;
    if(!name)return plate("workspace_name","Укажите название проекта.");
    const generation=++workspaceLoadGeneration,epoch=sessionEpoch;
    const r=await workspaceRPC(create?"Create":"GetWorkspace",{workspace:name});
    if(generation!==workspaceLoadGeneration || epoch!==sessionEpoch)return;
    if(r.status!==200)return plate("workspace_refused","Workspace: "+errText(r),r.status);
    // Commit one coherent snapshot: a late head-file response must never replace
    // the file hashes of a revision the user selected while loading the project.
    const next={workspace:r.json,workspaceRevision:r.json.head_rev,workspaceFiles:[],workspaceMembers:[],workspaceMembersAllowed:true,workspaceRevisions:[],workspaceCandidates:[]};
    for(const [operation,key] of [["ListMembers","workspaceMembers"],["ListRevisions","workspaceRevisions"],["ListFiles","workspaceFiles"],["ListCandidates","workspaceCandidates"]]) {
      if(operation==="ListFiles" && !r.json.head_rev)continue;
      const res=await workspaceRPC(operation,{workspace:name,...(operation==="ListFiles"?{revision:r.json.head_rev}:{})});
      if(generation!==workspaceLoadGeneration || epoch!==sessionEpoch)return;
      if(operation==="ListMembers" && res.status===403){next.workspaceMembersAllowed=false;unplate("workspace_ListMembers");continue;}
      if(res.status!==200){plate("workspace_"+operation,operation+": "+errText(res),res.status);continue;}
      unplate("workspace_"+operation);
      next[key]=Array.isArray(res.json)?res.json:[];
    }
    if(L.workspace?.id!==r.json.id){L.workspaceReview=null;L.workspaceUploadFile=null;}
    Object.assign(L,next);unplate("workspace_refused");paint();return true;
  }
  async function workspaceFile(path) {
    const f=L.workspaceFiles?.find(f=>f.path===path);
    if(!f || !L.workspaceRevision) return plate("workspace_file_missing","Выберите файл опубликованной ревизии.");
    const r=await workspaceRPC("ReadFile",{workspace:L.workspace.name,revision:L.workspaceRevision,path:f.path},{raw:true});
    if(r.status!==200) return plate("workspace_file_refused","Чтение файла: HTTP "+r.status,r.status);
    const hash=[...new Uint8Array(await crypto.subtle.digest("SHA-256",r.bytes))].map(x=>x.toString(16).padStart(2,"0")).join("");
    if(hash!==f.digest.replace(/^sha256:/,"")) return plate("sha_mismatch","Контрольная сумма файла Workspace не совпала; файл не выдан.");
    const url=URL.createObjectURL(new Blob([r.bytes],{type:"application/octet-stream"}));
    const a=document.createElement("a");a.href=url;a.download=f.path.split("/").at(-1);a.click();setTimeout(()=>URL.revokeObjectURL(url),30000);
  }
  async function reviewWorkspaceCandidate(id) {
    const target=workspaceTarget(),generation=++workspaceReviewGeneration;
    if(!target || !id)return;
    const r=await workspaceRPC("CandidateDetails",{candidate_id:id});
    if(!target.current() || generation!==workspaceReviewGeneration)return;
    if(r.status!==200) {L.workspaceReview=null; return plate("workspace_review",errText(r),r.status);}
    if(r.json?.candidate?.workspace_id!==target.workspace.id)return;
    L.workspaceReview=r.json; unplate("workspace_review"); paint();
  }
  async function uploadWorkspaceFile() {
    const target=workspaceTarget(),review=L.workspaceReview,reviewGeneration=workspaceReviewGeneration;
    const file=document.getElementById("ahp-workspace-upload-file")?.files?.[0] || L.workspaceUploadFile;
    if(!file || !target) return plate("workspace_upload","Выберите файл и проект.");
    const current=()=>target.current() && reviewGeneration===workspaceReviewGeneration;
    if(!file.size || file.size>64*1024*1024) return plate("workspace_upload","Выберите непустой файл размером до 64 МиБ.");
    let uploaded;
    try {
      uploaded=await window.AHP_DIRECT_UPLOAD.upload(file,(operation,body)=>raw("/u-ahp/v1/artifacts/upload?operation="+operation,{body}));
    } catch(error) {
      if(current())plate("workspace_upload",error.message || "Не удалось загрузить файл.");
      return;
    }
    if(!current())return;
    if(uploaded.status!==200) return plate("workspace_upload",errText(uploaded),uploaded.status);
    let d=review;
    if(!d || d.candidate.workspace_id!==target.workspace.id || d.candidate.state!=="pending") {
      const c=await workspaceRPC("CreateCandidate",{workspace:target.workspace.name,base_rev:target.workspace.head_rev || ""});
      if(!current())return;
      if(c.status!==200) return plate("workspace_upload",errText(c),c.status);
      const r=await workspaceRPC("CandidateDetails",{candidate_id:c.json.id});
      if(!current())return;
      if(r.status!==200) return plate("workspace_upload",errText(r),r.status);
      d=r.json;
    }
    if(d.candidate.workspace_id!==target.workspace.id)return;
    const files=(d.files || []).filter(f=>f.path!==file.name);
    files.push({path:file.name,artifact_id:uploaded.json.id,mode:420,digest:uploaded.json.sha256});
    const updated=await workspaceRPC("UpdateCandidateFiles",{candidate_id:d.candidate.id,files});
    if(!current())return;
    if(updated.status!==200) return plate("workspace_upload",errText(updated),updated.status);
    L.workspaceUploadFile=null; unplate("workspace_upload");
    if(await openWorkspace(false,target.workspace.name))await reviewWorkspaceCandidate(d.candidate.id);
  }
  async function checkWorkspaceCandidate() {
    const target=workspaceCandidateTarget();if(!target)return;
    const id=target.review.candidate.id;
    const r=await workspaceRPC("VerifyCandidate",{candidate_id:id});
    if(!target.current())return;
    if(r.status!==200) return plate("workspace_check","Проверка целостности: "+errText(r),r.status);
    if(r.json?.candidate?.workspace_id!==target.workspace.id)return;
    L.workspaceReview=r.json;unplate("workspace_check");paint();
  }
  async function publishWorkspaceCandidate() {
    const target=workspaceCandidateTarget();if(!target)return;
    const d=target.review;
    if(!confirm("Опубликовать эту проверенную ревизию? Файлы станут доступны участникам проекта."))return;
    // Sharing is a deliberate owner action, separate from executing checks.
    for(const id of new Set(d.owned_artifacts || [])) {
      if(!target.current())return;
      const shared=await raw("/u-ahp/v1/artifacts/Publish",{body:{id,workspace:target.workspace.name}});
      if(!target.current())return;
      if(shared.status!==200)return plate("workspace_publish",errText(shared),shared.status);
    }
    const r=await workspaceRPC("Publish",{candidate_id:d.candidate.id,expected_head:target.workspace.head_rev || ""});
    if(!target.current())return;
    if(r.status!==200) {await reviewWorkspaceCandidate(d.candidate.id);return plate("workspace_publish","Публикация отклонена; обновите проект и проверьте конфликт: "+errText(r),r.status);}
    L.workspaceReview=null;unplate("workspace_publish");await openWorkspace(false,target.workspace.name);
  }
  async function rejectWorkspaceCandidate() {
    const target=workspaceCandidateTarget();if(!target || !confirm("Отклонить выбранный черновик? Опубликованные ревизии сохранятся."))return;
    const id=target.review.candidate.id;
    const r=await workspaceRPC("RejectCandidate",{candidate_id:id});
    if(!target.current())return;
    if(r.status!==200)return plate("workspace_review",errText(r),r.status);
    L.workspaceReview=null;await openWorkspace(false,target.workspace.name);
  }
  async function workspaceRevision(id) {
    const generation=++workspaceLoadGeneration,epoch=sessionEpoch;
    const r=await workspaceRPC("ListFiles",{workspace:L.workspace.name,revision:id});
    if(generation!==workspaceLoadGeneration || epoch!==sessionEpoch)return;
    if(r.status!==200)return plate("workspace_revision",errText(r),r.status);
    L.workspaceRevision=id;L.workspaceFiles=r.json || [];paint();
  }
  async function workspaceMember(operation,subject) {
    if(!subject?.trim())return plate("workspace_member","Укажите идентификатор участника.");
    const target=workspaceTarget();if(!target)return;
    const r=await workspaceRPC(operation,{workspace:target.workspace.name,subject:subject.trim()});
    if(!target.current())return;
    if(r.status!==200)return plate("workspace_member",errText(r),r.status);
    unplate("workspace_member");await openWorkspace(false,target.workspace.name);
  }
  function workspaceSection() {
    const w=L.workspace,d=L.workspaceReview,c=d?.candidate;
    const owner=L.workspaceMembers?.some(m=>m.subject===S.subject&&m.is_owner);
    const canPublish=owner && c?.state==="pending" && d.checks?.length>0 && d.checks.every(x=>x.status==="passed"&&x.target_digest===c.digest);
    const review=d ? `<p class="sub">Черновик ${esc(c.id)} · ${esc(c.state)} · digest ${esc(c.digest)}</p>`+
      table(["Файл","SHA-256"],(d.files || []).map(f=>row([esc(f.path),esc(f.digest)])))+
      (d.checks?.length ? table(["Проверка","Результат","Текущий digest"],d.checks.map(x=>row([esc(x.name),statusPill(x.status),x.target_digest===c.digest?"да":"нет"]))) : notice("Проверок ещё нет. Публикация заблокирована."))+
      notice("Проверка целостности сверяет SHA-256 и размер документов/данных. Исходный код и исполняемые файлы требуют проверок доверенного CI; результат тестов здесь не подставляется.")+
      (c.state==="pending" ? `<div class="actions">${btn("Проверить целостность","ahp-workspace-check","secondary")}${btn("Опубликовать ревизию","ahp-workspace-publish","primary",canPublish?"":"disabled")}${btn("Отклонить черновик","ahp-workspace-reject","secondary")}</div>` : notice(c.conflict_branch?"Сохранена конфликтная ветка: "+esc(c.conflict_branch):"Черновик завершён.")) : notice("Загрузите файл или выберите существующий черновик.");
    return heading("Workspace", "Файлы, проверенные ревизии и участники проекта")+
      panel("Выбор проекта",`<div class="grid">${field("Название проекта","ahp-workspace-name",w?.name || "","my-project")}</div><div class="actions">${btn("Открыть проект","ahp-workspace-open","primary")}${btn("Создать проект","ahp-workspace-create","secondary")}</div>`)+
      (w?.id ? panel("Рабочая копия",`<p class="sub">1. Загрузите файл → 2. Выполните проверки → 3. Опубликуйте ревизию</p><label class="field">Файл до 64 МиБ<input type="file" id="ahp-workspace-upload-file"></label>${L.workspaceUploadFile ? `<p class="sub">Выбран: ${esc(L.workspaceUploadFile.name)}</p>` : ""}<div class="actions">${btn("Добавить файл в черновик","ahp-workspace-upload","secondary")}</div>`+review+
        (L.workspaceCandidates?.length ? table(["Черновик","Состояние",""],L.workspaceCandidates.map(x=>row([`<span title="${esc(x.id)}">${esc(x.id.slice(-12))}</span>`,statusPill(x.state),btn("Открыть","ahp-workspace-candidate","compact",`data-id="${esc(x.id)}"`)]))) : ""))+
        panel("Файлы опубликованной ревизии",`<p class="sub">${esc(w.name)} · ${esc(L.workspaceRevision || "ещё не опубликована")}</p>`+(L.workspaceFiles?.length ? table(["Файл","SHA-256",""],L.workspaceFiles.map(f=>row([esc(f.path),esc(f.digest.slice(0,20))+"…",btn("Скачать","ahp-workspace-file","compact",`data-id="${esc(f.path)}"`)]))) : notice("В этой ревизии файлов пока нет.")))+
        `<div class="grid cols2">${panel("Участники",(L.workspaceMembersAllowed===false?notice("Список участников и публикация ревизий доступны владельцу проекта."):table(["Участник","Роль",""],(L.workspaceMembers || []).map(m=>row([esc(m.subject),esc(m.role),owner&&!m.is_owner?btn("Убрать","ahp-workspace-remove-member","compact",`data-id="${esc(m.subject)}"`):""]))))+(owner?`<div class="grid">${field("ID участника","ahp-workspace-member","","subject-id")}</div><div class="actions">${btn("Добавить редактора","ahp-workspace-add-member","secondary")}</div>`:""))}${panel("Ревизии",L.workspaceRevisions?.length ? table(["Ревизия","Автор",""],L.workspaceRevisions.map(r=>row([`<span title="${esc(r.id)}">${esc(r.id.slice(-12))}</span>`,esc(r.author_subject),btn("Просмотреть","ahp-workspace-revision","compact",`data-id="${esc(r.id)}"`)]))) : notice("Ревизий пока нет."))}</div>` : "");
  }
  const profileAdmin = window.AHP_PROFILE_ADMIN?.create({raw,L,paint,plate,unplate});
  function sectionView(page) {
    if (!S.online) return heading("Agent Harness Platform", "Рабочее пространство") + sessionCard();
    const settings = profileAdmin?.view(page); if(settings !== undefined) return settings;
    if(page === "repositories" || page === "environments") return heading(page === "repositories" ? "Repositories" : "Environments", "Подключения проекта") + panel("Подключение недоступно", notice(page === "repositories" ? "API подключения Git-репозиториев в текущем backend отсутствует. Регистрация и установка готовых агентских версий доступны в Agent Stacks." : "API управления окружениями пока не подключён. Секреты хранятся на сервере и не вводятся в этом интерфейсе."));
    return ({memory:memorySection, crons:triggersSection, stacks:stacksSection, chats:chatSection, dashboard:overviewSection, stats:()=>heading("Stats", "Стоимость выбранной задачи")+costCard(), sandbox:sandboxSection,workspace:workspaceSection,approvals:approvalsSection,connections:connectionsSection})[page]?.();
  }

  // ---- врезка в оболочку прототипа -------------------------------------------
  function paint() {
    const page = route()[0].split("?")[0];
    const main = $("#main");
    const section = sectionView(page);
    if (main && section !== undefined) {
      const oldConversation = main.querySelector(".ahp-conversation");
      const thread = L.chat?.thread_id || L.turn?.thread_id || "";
      const readingTop = oldConversation?.scrollTop || 0;
      const followConversation = !oldConversation || paint.lastThread !== thread || oldConversation.scrollHeight - oldConversation.clientHeight - readingTop < 40;
      const drafts = new Map([...main.querySelectorAll("input[name],textarea[name],select[name]")].filter(e=>e.value!==e.defaultValue).map(e=>[e.name,e.value]));
      const focus = main.contains(document.activeElement) ? document.activeElement.name : "";
      main.innerHTML = platesBar() + section;
      if (paint.lastPage === page) for (const e of main.querySelectorAll("input[name],textarea[name],select[name]")) if (drafts.has(e.name)) e.value = drafts.get(e.name);
      if (focus) main.querySelector(`[name="${focus}"]`)?.focus();
      const conversation = main.querySelector(".ahp-conversation");
      if (conversation) conversation.scrollTop = followConversation ? conversation.scrollHeight : readingTop;
      paint.lastThread = thread;
      paint.lastPage = page;
      document.title = `${{chats:"Чаты",memory:"Memory",crons:"Crons",stacks:"Agent Stacks",dashboard:"Обзор",sandbox:"Sandbox",stats:"Stats"}[page] || "AHP"} · AHP · Cloud.ru`;
    }
    if (page === "live") {
      const main = $("#main");
      if (main) main.innerHTML = liveView();
      const active = $("#sidebar a[href='#/live']");
      if (active && proto_class()) active.classList.add(proto_class());
    }
    for (const button of document.querySelectorAll("button[data-action]")) if (pendingActions.has(button.dataset.action)) button.disabled=true;
    stampSession();
  }
  const proto_class = () => {
    const a = [...document.querySelectorAll("#sidebar a")].find((x) => x.getAttribute("href") !== "#/live" && x.className);
    return a ? (a.className.split(" ").filter((c) => c !== "sidebar-back")[0] || "") : "";
  };

  function stampSession() {
    const header = $("#header");
    if (!header) return;
    let chip = $("#ahp-session-chip");
    if (!chip) {
      chip = document.createElement("div");
      chip.id = "ahp-session-chip";
      chip.className = "topcell";
      chip.innerHTML = `<span class="tag"></span><button class="textbtn" data-action="ahp-logout">Выйти</button>`;
      header.append(chip);
    }
    chip.querySelector(".tag").textContent = S.online ? S.subject + " · " + S.tenant : "нет сессии";
    chip.querySelector(".tag").className = "tag" + (S.online ? " success" : " danger");
    chip.querySelector("button").hidden = !S.online;
  }

  async function boot() {
    const ok = await mint();
    paint();
    if (!ok) {
      plate("session_missing", "Войдите, чтобы увидеть настоящие данные платформы.", "");
      loginModal();
      return;
    }
    // Восстановление после reload: беседы и последняя история читаются с сервера.
    // HttpOnly-кука сессии переживает перезагрузку, поэтому состояние возвращается
    // без какого-либо хранилища в браузере (UI-03).
    await completePersonalOAuth();
    await loadInitialData();
  }

  async function loadInitialData() {
    await Promise.allSettled([loadChats(), loadCatalog(), listTriggers(), searchMemory("")]);
    const query = new URLSearchParams(location.hash.split("?")[1] || "");
    const selected = query.get("thread");
    if(query.get("installation") && query.get("task")) {L.invocation={installation_id:query.get("installation"),task_id:query.get("task"),status:"unknown",observed:false}; await observeInvocation();}
    if (selected && L.chats?.some(c => c.thread_id === selected)) await openChat(selected);
    paint();
  }

  const api = {
    hook,
    plate,
    paint,
    boot,
    state: { S, L },
    api: {
      login, mint, logout, loadCatalog, registerAgent, publishVersion, verdict, runTurn, cancelTurn, cost, resolve, download,
      proposeMemory, searchMemory, editMemory, createTrigger, listTriggers, fireTrigger, revokeTrigger,
      loadChats, openChat, invokeInstallation, observeInvocation, runEvaluation, connections, connectPersonal, disconnectPersonal, completePersonalOAuth, approvals, approve, computeAction, openWorkspace, workspaceFile, probeWorkspace, probeOwn, probeForeign, probeCompute, readMore, findArtifact, space, raw,
    },
  };
  window.AHP_LIVE = api;

  // Перехват диспетчера и рендера: единая точка, где мок-действие либо становится
  // настоящим вызовом, либо честно отказывается.
  const originalAction = action;
  action = async function (name, el) {
    if (pendingActions.has(name)) return;
    const guarded=String(name).startsWith("ahp-");
    if(guarded) {pendingActions.add(name); if(el) el.disabled=true;}
    try { if (await api.hook(name, el)) return; return await originalAction.call(this,name,el); }
    finally { if(guarded) {pendingActions.delete(name); if(el) el.disabled=false; for(const b of document.querySelectorAll("button[data-action]")) if(b.dataset.action===name) b.disabled=false;} }
  };
  const originalRender = render;
  render = function () {
    originalRender.call(this);
    api.paint();
  };
  boot().catch(e => plate("connection_error", "Не удалось связаться с платформой: " + e.message));
})();
