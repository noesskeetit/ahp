// Формы ответов платформенных плоскостей — единственное место, где интерфейс
// разбирает JSON платформы.
//
// Почему это отдельный файл. Независимый аудит (reports/independent-audit-20261102/
// frontend.md, UI-01/UI-02) измерил два рассинхрона сразу: строка строки-строки
// уходила в таблицу, ждущую объектов {cells}, а память читалась по плоскому
// контракту, которого платформа никогда не отдавала. Ошибка рендера маскировала
// ошибку разбора: таблица падала раньше, чем успевала показать пустые поля. Поэтому
// разбор вынесен сюда и закрыт тестами на НАСТОЯЩИХ снимках ответов
// (web/testdata/aifactory-shapes-live.json), а не на демо-данных example.
//
// Каждое имя поля ниже обязано источником: Go-структурой или живым замером.
//   * память:   internal/memory/search.go:29-36 {hits:[{memory:{...},score}]},
//               internal/memory/propose.go:51-53 {memory:{...},similar}
//   * триггеры: internal/triggers/httpapi.go (List -> {triggers:[...]}, поля
//               trigger_id/status/fired_count/max_fires/interval_ms; поле id отсутствует)
//   * реестр:   internal/registry/catalog.go:22-35 (карточка), agents.go:22-25,
//               deploy.go:13-24 (deployment), httpapi.go (конверты cards/agent/
//               version/installation/deployments)
//   * оценки:   internal/evals/httpapi.go:100-111 {verdict:{outcome,passing,
//               total_cost,task_summary:{total,passed}}}
//   * кадры:   internal/space/stateclient.go:643-650 {id,thread_id,key,kind,
//               sequence,payload} — поля seq/type платформой не отдаются
//   * артефакт: internal/artifacts/store.go:41-49 (ArtifactRef) и
//               /u-ahp/v1/artifacts/Resolve -> {sha256,versions:[{artifact_id,
//               version,sha256,size,name,mime}]}
//   * правила:  internal/artifacts/access.go:286 (context id), internal/space/
//               coordinator.go:284-302 + internal/core/runtime.go (бюджет шагов)
//
// Файл не знает про DOM: его можно проверить уз-тестом (web/aifactory_shapes.test.mjs)
// и загрузить в браузер тем же байтом.
(() => {
  // ---- примитивы -------------------------------------------------------------
  const isArr = (v) => Array.isArray(v);
  const isObj = (v) => v !== null && typeof v === "object" && !Array.isArray(v);
  const text = (v) => (v === null || v === undefined ? "" : typeof v === "string" ? v : String(v));
  const number = (v) => (typeof v === "number" && Number.isFinite(v) ? v : null);

  // Поле обязательное и отсутствующее — это дрейф контракта, а не пустая ячейка.
  // Молча нарисовать «—» означало бы принять 200 OK с незнакомой формой за успех,
  // поэтому читатель возвращает список отсутствующих обязательных полей, а UI
  // показывает его отдельной плашкой.
  function readOne(item, spec) {
    const raw = item[spec.name];
    if (raw === undefined || raw === null || raw === "") {
      return { value: spec.type === "num" ? null : "", missing: !!spec.required };
    }
    if (spec.type === "num") {
      const n = number(raw);
      return { value: n, missing: n === null };
    }
    if (spec.type === "bool") return { value: !!raw, missing: false };
    return { value: text(raw), missing: false };
  }

  function readList(json, envelope, spec, unwrap) {
    const items = isObj(json) && isArr(json[envelope]) ? json[envelope] : null;
    if (items === null) {
      return { rows: null, missing: [envelope + " (плоскость не вернула массив)"] };
    }
    const rows = [];
    const missing = new Set();
    for (const entry of items) {
      const source = unwrap ? (isObj(entry) ? unwrap(entry) : null) : entry;
      if (!isObj(source)) {
        missing.add(unwrap ? unwrap.label + " (вложенный объект не найден)" : "элемент не является объектом");
        rows.push(null);
        continue;
      }
      const row = {};
      for (const field of spec) {
        const got = readOne(source, field);
        row[field.as || field.name] = got.value;
        if (got.missing) missing.add(field.name);
      }
      rows.push(row);
    }
    return { rows: rows.filter((r) => r !== null), missing: [...missing] };
  }

  // ---- контракты -------------------------------------------------------------
  const SPEC = {
    chat: [
      { name: "thread_id", required: true },
      { name: "context_id", required: true },
      { name: "created_at" },
    ],
    turn: [
      { name: "id", required: true },
      { name: "thread_id", required: true },
      { name: "project_id", as: "projectID" },
      { name: "project_memory_profile", as: "projectMemoryProfile" },
	  { name: "agent_installation_id", as: "agentInstallationID" },
      { name: "status", required: true },
      { name: "steps", type: "num" },
      { name: "max_steps", type: "num", as: "maxSteps" },
      { name: "failure_reason", as: "failureReason" },
      { name: "created_at", as: "createdAt" },
    ],
    frame: [
      { name: "kind", required: true },
      { name: "sequence", type: "num", required: true },
    ],
    // Память лежит ВНУТРИ hit.memory (search.go:29-31); плоского hit.id платформа
    // не отдаёт никогда — именно это и ломало таблицу памяти.
    memory: [
      { name: "id", required: true },
      { name: "text", required: true },
      { name: "state", required: true },
      { name: "kind" },
      { name: "subject" },
      { name: "op_key", as: "opKey" },
    ],
    trigger: [
      { name: "trigger_id", required: true, as: "id" },
      { name: "status", required: true },
      { name: "fired_count", type: "num", as: "fired" },
      { name: "max_fires", type: "num", as: "max" },
      { name: "interval_ms", type: "num", as: "intervalMs" },
      { name: "kind" },
      { name: "target" },
      { name: "next_fire_at", as: "nextFireAt" },
    ],
    catalogCard: [
      { name: "agent_id", required: true },
      { name: "installation_id", required: true },
      { name: "version_id" },
      { name: "digest" },
      { name: "visibility" },
      { name: "audience" },
      { name: "environment" },
      { name: "kind" },
      { name: "published", type: "bool" },
    ],
    deployment: [
      { name: "id", required: true },
      { name: "agent_id", required: true },
      { name: "version_id" },
      { name: "digest" },
      { name: "action" },
      { name: "environment" },
      { name: "created_at", as: "createdAt" },
    ],
    artifactVersion: [
      { name: "artifact_id", required: true, as: "artifactId" },
      { name: "version", required: true },
      { name: "sha256", required: true },
      { name: "size", type: "num" },
      { name: "name" },
      { name: "mime" },
    ],
    verdict: [
      { name: "agent_digest", required: true, as: "agentDigest" },
      { name: "outcome", required: true },
      { name: "passing", type: "bool" },
      { name: "total_cost", type: "num", as: "totalCost" },
      { name: "task_set_id", as: "taskSetId" },
    ],
  };

  const unwrapMemory = (hit) => hit.memory;
  unwrapMemory.label = "hit.memory";

  // ---- контракт таблицы app.js ----------------------------------------------
  // app.js строит <table> из объектов {cells:[...], search?, status?} и вызывает
  // r.cells.map. Строка <tr> здесь даёт «Cannot read properties of undefined
  // (reading 'map')» — измеренный UI-01. Единственная правильная форма строки
  // таблицы поэтому живёт здесь и проверяется тестом.
  function tableRow(cells, meta) {
    const list = (cells || []).map((c) => (c === null || c === undefined ? "" : String(c)));
    return Object.assign({ cells: list, search: list.join(" ").replace(/<[^>]+>/g, "") }, meta || {});
  }

  // ---- вывод, который обязан быть честным ------------------------------------
  // Пусто и недоступно — РАЗНЫЕ состояния: 200 с нулём строк есть «платформа
  // ничего не вернула», не-200 есть «плоскость отказала». Оба отличаются от
  // «мы ещё не спрашивали».
  function outcome(status, rows, missing) {
    if (status !== 200) return { state: "unavailable", rows: [], missing: missing || [], status };
    if (!isArr(rows)) return { state: "drift", rows: [], missing: missing || [], status };
    if (missing && missing.length) return { state: "drift", rows, missing, status };
    return { state: rows.length ? "ok" : "empty", rows, missing: [], status };
  }

  // ---- точный ArtifactRef ------------------------------------------------------
  // Кадр инструмента несёт {id, version, sha256, size} (internal/artifacts/
  // store.go:41-49). Брать «первый попавшийся 64-hex из всего JSON» нельзя: этим
  // правом обладает только сам ArtifactRef — иначе под артефакт подписывается
  // любое постороннее шестнадцатеричное значение из промпта или аргумента
  // инструмента (измеренный UI-03).
  const HEX64 = /^[0-9a-f]{64}$/;
  function exactArtifact(value, requireSize) {
    if (!isObj(value)) return null;
    const {id, version, sha256, size} = value;
    const validID = v => typeof v === "string" && v.length > 0 && v.length <= 512 && !/[\s\x00-\x1f\x7f]/.test(v);
    if (!validID(id) || !validID(version) || typeof sha256 !== "string" || !HEX64.test(sha256)) return null;
    if ((requireSize || size !== undefined) && (!Number.isSafeInteger(size) || size <= 0 || size > 64 * 1024 * 1024)) return null;
    return {id, version, sha256, size: size === undefined ? null : size};
  }

  // Core externalizes large tool/model outputs with a bounded preview. An
  // ordinary model JSON object is not sufficient to name a downloadable file.
  function outputArtifact(content) {
    if (!isObj(content) || content.reader !== "read_output_chunk" || content.truncated !== true ||
        typeof content.preview !== "string" || content.preview.length > 4096) return null;
    return exactArtifact(content.artifact, true);
  }

  function artifactRef(frames) {
    let found = null;
    for (const frame of frames || []) {
      const payload = isObj(frame) && isObj(frame.payload) ? frame.payload : null;
      if (!payload || !["tool", "model"].includes(frame.kind)) continue;
      if (payload.step_type && payload.step_type !== frame.kind) continue;
      const content = payload.content;
      const ref = outputArtifact(content) || (frame.kind === "tool" ? exactArtifact(content, false) : null);
      if (ref) found = {...ref, sequence: number(frame.sequence)};
    }
    return found;
  }

  // Простой 64-hex в ответах встречается и вне ArtifactRef. Он годится только
  // как приглашение «разрешить вручную», и UI обязан назвать его именно так.
  function looseDigest(frames) {
    const raw = JSON.stringify(frames || []);
    const all = raw.match(/[0-9a-f]{64}/g) || [];
    return all.length ? all[all.length - 1] : "";
  }

  // ---- терминальный статус -----------------------------------------------------
  // Авторитет — GetTurn.status (internal/space/types.go Turn), а не вид кадра:
  // kinds бывают model/tool/done/status, и кадр status не обещает терминальности.
  const TERMINAL = ["completed", "done", "failed", "canceled", "timeout"];
  const isTerminalStatus = (status) => TERMINAL.includes(text(status).toLowerCase());

  // ---- правила ввода, зеркальные серверным -----------------------------------
  // context id: internal/artifacts/access.go:286. '/' сервер отвергает с
  // «cannot carry artifacts» — подсказка нужна ДО отправки (DEF-H24-11).
  const CONTEXT_ID = /^[A-Za-z0-9][A-Za-z0-9_.:@-]{0,127}$/;
  function contextIDError(value) {
    const v = text(value);
    if (!v) return "context id пуст: платформа свяжет ход с беседой только по имени контекста";
    if (v.includes("/")) return "context id не может содержать «/»: артефакты в таком контексте сохраняться не будут";
    if (!CONTEXT_ID.test(v)) return "context id — один токен из букв, цифр и . _ : @ - до 128 символов";
    return "";
  }

  // Бюджет шагов: space принимает число как есть, ядро ограничивает 64
  // (internal/space/coordinator.go:290-302). Пустое поле = бюджет не назван.
  function stepBudget(value) {
    const v = text(value).trim();
    if (!v) return { value: 0, error: "", stated: false };
    const n = Number(v);
    if (!Number.isInteger(n) || n <= 0) return { value: 0, error: "max_steps — целое положительное число", stated: true };
    if (n > 64) return { value: 64, error: "max_steps ограничен 64 (ядро), отправлено 64", stated: true };
    return { value: n, error: "", stated: true };
  }

  // Скачивание: при несовпадении контрольной суммы файл НЕ отдаётся (UI-03).
  // Развязка «проверил — не отдал» live-измеряется ногой с подменённым digest.
  function downloadVerdict({ wanted, computed, bytes }) {
    const match = text(wanted).toLowerCase() === text(computed).toLowerCase();
    return {
      match,
      deliver: match,
      bytes: number(bytes) || 0,
      reason: match ? "" : "sha256 скачанного не совпал с digest артефакта — файл не отдан",
    };
  }

  // SSE can split in the middle of UTF-8/text lines; TextDecoder runs before
  // this bounded framing helper. Projection-only end events do not advance cursor.
  function sseDecoder(onEvent) {
    let pending = "";
    return chunk => {
      pending += chunk;
      if (pending.length > 4 * 1024 * 1024) throw new Error("Поток превысил размер кадра");
      let match;
      while ((match = /\r?\n\r?\n/.exec(pending))) {
        const block = pending.slice(0, match.index);
        pending = pending.slice(match.index + match[0].length);
        const data = block.split(/\r?\n/).filter(l => l.startsWith("data:")).map(l => l.slice(5).trimStart()).join("\n");
        if (data) onEvent(JSON.parse(data));
      }
    };
  }

  // One message per generation. Final output replaces partial text; it is never
  // appended a second time. A missing terminal remains explicitly partial.
  function modelMessages(frames) {
    const out = [], byGeneration = new Map(), cursors = new Map();
    for (const frame of frames || []) {
      let p = frame.payload || {};
      if (frame.kind === "model" && outputArtifact(p.content)) p = {...p, content:p.content.preview};
	  if (frame.kind === "model" && isObj(p.content) && typeof p.content.text === "string") p = {...p, content:p.content.text};
      if (typeof p.content !== "string" || !p.content) continue;
      if (frame.kind === "model_delta") {
        if (!p.generation_id || !Number.isInteger(p.delta_sequence) || p.delta_sequence <= (cursors.get(p.generation_id) || 0)) continue;
        cursors.set(p.generation_id,p.delta_sequence);
        let message=byGeneration.get(p.generation_id);
        if (!message) { message={...frame,payload:{...p,content:""},partial:true};byGeneration.set(p.generation_id,message);out.push(message); }
        if (message.partial) message.payload.content += p.content;
      } else if (frame.kind === "model" && p.generation_id && byGeneration.has(p.generation_id)) {
        const message=byGeneration.get(p.generation_id);message.payload={...p};message.kind="model";message.partial=false;
      } else { const message={...frame,payload:{...p},partial:false};out.push(message);if(p.generation_id) byGeneration.set(p.generation_id,message); }
    }
    return out;
  }

  function projectMemoryBinding({current, workspace, selectedProfile = "", projectID} = {}) {
    const locked = !!current?.thread_id;
    const rawProfile = locked ? (current.project_memory_profile || "") : selectedProfile;
    if (!["", "native", "shared"].includes(rawProfile)) {
      return {projectID: "", profile: "", locked, error: "Неизвестный режим памяти проекта"};
    }
    const profile = rawProfile === "shared" ? "shared" : "";
    const id = locked ? (current.project_id || "") :
      (projectID ?? (profile === "shared" ? workspace?.id : workspace?.name) ?? "");
    if (profile === "shared" && !id) {
      return {projectID: "", profile, locked, error: "Сначала выберите проект в разделе Workspace"};
    }
    return {projectID: id, profile, locked, error: ""};
  }

  window.AHP_SHAPES = {
    projectMemoryBinding,
    sseDecoder,
    modelMessages,
    SPEC,
    tableRow,
    outcome,
    isTerminalStatus,
    artifactRef,
    looseDigest,
    contextIDError,
    stepBudget,
    downloadVerdict,
    chats: (json) => readList(json, "threads", SPEC.chat),
    turns: (json) => readList(json, "turns", SPEC.turn),
    frames: (json) => readList(json, "events", SPEC.frame),
    memories: (json) => {
      if(isObj(json) && Object.hasOwn(json,"facts")) {
        const parsed=readList(json,"facts",[{name:"memory_ref",required:true,as:"id"},{name:"text",required:true},{name:"scope",required:true,as:"kind"},{name:"type",as:"factType"}]);
        if(parsed.rows) parsed.rows=parsed.rows.map(r=>({...r,state:"indexed"}));
        return parsed;
      }
      return readList(json,"hits",SPEC.memory,unwrapMemory);
    },
    proposed: (json) => readList({ records: isObj(json) && isObj(json.memory) ? [json.memory] : null }, "records", SPEC.memory),
    triggers: (json) => readList(json, "triggers", SPEC.trigger),
    catalog: (json) => readList(json, "cards", SPEC.catalogCard),
    deployments: (json) => readList(json, "deployments", SPEC.deployment),
    versions: (json) => readList(json, "versions", SPEC.artifactVersion),
    verdict: (json) => readList({ records: isObj(json) && isObj(json.verdict) ? [json.verdict] : null }, "records", SPEC.verdict),
  };
})();
