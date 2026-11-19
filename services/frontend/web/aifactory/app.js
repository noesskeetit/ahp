"use strict";
const STORE = "ahp-cloud-prototype-v1";
const $ = (s, r = document) => r.querySelector(s);
const esc = (v) =>
  String(v ?? "").replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );
const uid = () => crypto.randomUUID?.() || Math.random().toString(36).slice(2);
const now = () =>
  new Date().toLocaleString("ru-RU", {
    day: "2-digit",
    month: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  });
const clone = (v) => JSON.parse(JSON.stringify(v));
const icons = {
  home: "icon-home-s",
  stack: "icon-ip-block",
  repo: "tabler-git-branch",
  env: "tabler-key",
  memory: "tabler-database",
  sandbox: "tabler-terminal-2",
  cron: "tabler-clock",
  stats: "tabler-chart-bar",
  plus: "icon-plus-s",
  close: "icon-cross-s",
  refresh: "icon-update-s",
  search: "search__icon-search",
  filter: "icon-filter-s",
  arrow: "icon-chevron-right-s",
  back: "icon-chevron-left-s",
  chevron: "icon-chevron-down-s",
  check: "icon-check-xs",
  help: "icon-question",
  link: "icon-link",
  agent: "icon-evolution-ai-agents",
  mcp: "icon-mcp-server",
  apps: "icon-main-menu",
  logo: "icon-cloud-logo-light",
  evolution: "icon-evolution",
  bell: "icon-bell",
  more: "icon-more-s",
  collapse: "icon-vertical-menu-close",
  snapshot: "tabler-camera",
  play: "tabler-player-play",
  pause: "tabler-player-pause",
  download: "tabler-download",
  copy: "tabler-copy",
  history: "tabler-history",
  rocket: "tabler-rocket",
  settings: "tabler-settings",
  file: "tabler-file-code",
  plug: "tabler-plug-connected",
  upload: "tabler-cloud-upload",
  tag: "tabler-tag",
  trash: "tabler-trash",
  server: "tabler-server",
  success: "tabler-circle-check",
  folder: "tabler-folder",
  shield: "tabler-shield-lock",
};
const icon = (name, small = false) =>
  `<img class="icon${small ? " small" : ""}" src="assets/icons/${icons[name] || icons.stack}.svg" alt="" aria-hidden="true">`;
const actionIcons = {
  deploy: "play",
  stop: "pause",
  update: "refresh",
  integration: "plug",
  "ci-simulate": "upload",
  "update-apply": "upload",
  "apply-pending": "upload",
  rollback: "history",
  "rollback-confirm": "history",
  "eval-run": "play",
  "eval-report": "file",
  "eval-download": "download",
  "config-download": "download",
  "artifact-download": "download",
  "snapshot-download": "download",
  "copy-code": "copy",
  "repo-sync": "refresh",
  "repo-new": "plus",
  "memory-new": "plus",
  "env-new": "plus",
  "stack-new": "plus",
  "cron-new": "plus",
  "agent-cron": "cron",
  "cron-trigger": "play",
  "stack-create": "rocket",
  "stack-draft": "file",
  "load-demo": "rocket",
  guide: "arrow",
  docs: "help",
  "wizard-next": "arrow",
  "wizard-prev": "back",
  "snapshot-detail": "snapshot",
  "sandbox-detail": "sandbox",
  component: "file",
  tags: "tag",
  export: "download",
};
const btn = (text, action, kind = "", extra = "") => {
  const key =
    actionIcons[action] ||
    (action.endsWith("-save")
      ? "check"
      : action.endsWith("-delete")
        ? "trash"
        : action.endsWith("-edit")
          ? "settings"
          : "");
  return `<button class="btn ${kind}" data-action="${action}" ${extra}>${key && !text.includes("<img") ? icon(key, true) : ""}${text}</button>`;
};
const ibtn = (name, action, label, extra = "") =>
  `<button class="iconbtn" data-action="${action}" aria-label="${esc(label)}" title="${esc(label)}" ${extra}>${icon(name)}</button>`;
const badge = (text, kind = "") =>
  `<span class="status ${kind}">${esc(text)}</span>`;
const tags = (list) =>
  (list || []).map((t) => `<span class="tag">${esc(t)}</span>`).join("");
const link = (label, route) => `<a href="#/${route}">${esc(label)}</a>`;
const notice = (text, kind = "") =>
  `<div class="notice ${kind}">${icon(kind === "error" ? "close" : "help")}<div>${text}</div></div>`;
const empty = (title, copy, action, buttonText, type = "stack") =>
  `<div class="empty"><div class="empty-icon">${icon(type)}</div><h2>${title}</h2><p>${copy}</p>${action ? btn(`${buttonText} ${icon("plus")}`, action, "primary") : ""}</div>`;
const field = (
  label,
  name,
  value = "",
  placeholder = "",
  type = "text",
  hint = "",
) =>
  `<label class="field"><span>${label}</span><input name="${name}" type="${type}" value="${esc(value)}" placeholder="${esc(placeholder)}" ${type === "password" ? 'autocomplete="new-password"' : ""}>${hint ? `<small>${hint}</small>` : ""}</label>`;
const select = (label, name, options, value = "") =>
  `<label class="field"><span>${label}</span><select name="${name}">${options
    .map((o) => {
      let v = typeof o === "string" ? o : o[0],
        l = typeof o === "string" ? o : o[1];
      return `<option value="${esc(v)}" ${value === v ? "selected" : ""}>${esc(l)}</option>`;
    })
    .join("")}</select></label>`;
const kv = (items) =>
  `<dl class="keyvalue">${items.map(([k, v]) => `<dt>${k}</dt><dd>${v}</dd>`).join("")}</dl>`;
const code = (text, terminal = false) =>
  `<div class="codebox ${/^https?:\/\/\S+$/.test(text) ? "endpoint" : ""}"><pre class="${terminal ? "terminal" : ""}">${esc(text)}</pre>${btn("Копировать", "copy-code", "compact copy")}</div>`;
const yamlSyntax = (text) =>
  text
    .split("\n")
    .map((line) => {
      const commentAt = line.indexOf("#");
      const source = commentAt < 0 ? line : line.slice(0, commentAt);
      const comment = commentAt < 0 ? "" : line.slice(commentAt);
      const match = source.match(/^(\s*)([^:\s][^:]*?)(:)(.*)$/);
      const content = match
        ? `<span class="yaml-indent">${esc(match[1])}</span><span class="yaml-key">${esc(match[2])}</span><span class="yaml-punctuation">${match[3]}</span><span class="yaml-value">${esc(match[4])}</span>`
        : esc(source);
      return `${content}${comment ? `<span class="yaml-comment">${esc(comment)}</span>` : ""}`;
    })
    .join("\n");
const yamlBox = (text) =>
  `<div class="yaml-editor"><pre class="yaml-highlight" aria-hidden="true"><code>${yamlSyntax(text)}</code></pre><textarea class="yaml-textbox" aria-label="Итоговая YAML-конфигурация" readonly spellcheck="false">${esc(text)}</textarea>${btn("Копировать", "copy-code", "compact copy")}</div>`;
const panel = (title, body, action = "") =>
  `<section class="panel">${title ? `<div class="panel-head"><h2>${icon({ "Sandbox API": "plug", "Начало работы": "rocket", "Последние изменения": "history", "Ресурсы проекта": "folder", "Общая информация": "file", "Состав системы": "stack", "Компоненты системы": "stack", "Источник конфигурации": "repo", "История изменений": "history", "Проверка агентской системы": "success", "История eval-запусков": "stats", Память: "memory", Sandbox: "sandbox", Расписания: "cron", "Задачи агентов": "agent", "Состояние компонентов": "server" }[title] || "folder")}<span>${title}</span></h2>${action}</div>` : ""}${body}</section>`;
function fresh() {
  return {
    repos: [],
    evolutionRepos: [],
    envs: [],
    memories: [],
    stacks: [],
    sandboxes: [],
    crons: [],
    evals: [],
    events: [],
    guide: false,
    project: "Agent Platform",
    statsPeriod: "week",
  };
}
let state;
try {
  state = fresh(); // Состояние браузера не используется: данные даёт только платформа.
  if (!Array.isArray(state.stacks)) state = fresh();
  if (!Array.isArray(state.evolutionRepos)) state.evolutionRepos = [];
  if (state.envs.some((e) => Array.isArray(e.vars))) {
    state.envs = state.envs.flatMap((environment) =>
      environment.vars.map((variable) => ({
        id: uid(),
        ...variable,
        updated: environment.updated || now(),
      })),
    );
  }
  state.stacks.forEach((s) => {
    if (!s.sourceType) {
      s.sourceType = "repository";
      s.sourceId = s.repoId || "";
    }
  });
} catch {
  state = fresh();
}
let epoch = 0,
  modal = null,
  draft = null,
  returnFocus = null;
let route = () =>
  decodeURIComponent((location.hash.slice(2) || "dashboard").split("?")[0]).split("/");
let currentStack = () => state.stacks.find((s) => s.id === route()[1]);
function sourceFor(s) {
  const type = s?.sourceType || "repository";
  const id = s?.sourceId || s?.repoId;
  return type === "evolution"
    ? state.evolutionRepos.find((r) => r.id === id)
    : state.repos.find((r) => r.id === id);
}
function sourceLabel(s) {
  const source = sourceFor(s);
  return source ? source.name : "—";
}
function sourceKind(s) {
  return (s?.sourceType || "repository") === "evolution"
    ? "Evolution Repo"
    : "Repository";
}
function save() {
  // Намеренный no-op: браузер не хранит ни состояние, ни секреты.
  // Единственный источник истины — AHP (см. ahp-live.js).
}
function toast(text, type = "") {
  const el = document.createElement("div");
  el.className = "toast " + type;
  el.textContent = text;
  $("#toasts").append(el);
  setTimeout(() => el.remove(), 5000);
}
function after(ms, fn) {
  const token = epoch;
  setTimeout(() => {
    if (token === epoch) fn();
  }, ms);
}
function event(
  title,
  detail,
  source = "Пользователь",
  stackId = "",
  status = "success",
  version = null,
) {
  const ev = {
    id: uid(),
    title,
    detail,
    source,
    stackId,
    status,
    time: now(),
    at: Date.now(),
    version: version ? clone(version) : null,
  };
  state.events.unshift(ev);
  save();
  return ev;
}
function go(path) {
  closeModal();
  history.pushState(null, "", "#/" + path);
  render();
  window.scrollTo(0, 0);
}
function stackStatus(s) {
  return badge(
    s.status === "running"
      ? "Запущен"
      : s.status === "deploying"
        ? "Обновляется"
        : s.status === "error"
          ? "Ошибка"
          : "Остановлен",
    s.status === "running"
      ? ""
      : s.status === "error"
        ? "error"
        : s.status === "deploying"
          ? "info"
          : "warn",
  );
}
function steps() {
  return [
    {
      name: "Выберите источник конфигурации",
      desc: "Используйте Evolution Repo проекта или подключите внешний Git-репозиторий.",
      done: !!(state.repos.length || state.evolutionRepos.length),
      action: "repo-new",
      page: "repositories",
    },
    {
      name: "Добавьте переменные окружения",
      desc: "Создайте глобальные переменные и секреты проекта.",
      done: !!state.envs.length,
      action: "env-new",
      page: "environments",
    },
    {
      name: "Создайте Agent Stack",
      desc: "Выберите конфигурацию и проверьте состав системы.",
      done: !!state.stacks.length,
      action: "stack-new",
      page: "stacks",
    },
    {
      name: "Разверните систему",
      desc: "Запустите агентов и MCP из готовых образов.",
      done: state.stacks.some((s) => s.versions.length),
      action: "guide-deploy",
      page: "stacks",
    },
    {
      name: "Проверьте систему и подключите CI/CD",
      desc: "Eval запускается автоматически после deploy и обновлений; регистрируйте версии образов из CI/CD.",
      done:
        state.evals.some((e) => e.status === "passed") &&
        state.events.some((e) => e.source === "CLI / API"),
      action: "guide-eval",
      page: "stacks",
    },
  ];
}
const stepsHtml = () =>
  `<div class="steps-list">${steps()
    .map(
      (s, i) =>
        `<div class="journey-step"><span class="step-circle ${s.done ? "done" : ""}">${s.done ? icon("check", true) : i + 1}</span><div class="step-copy"><strong>${s.name}</strong><p>${s.desc}</p></div>${btn(s.done ? "Открыть" : "Настроить", s.done ? "navigate" : s.action, s.done ? "ghost" : "secondary", s.done ? `data-path="${s.page}"` : "")}</div>`,
    )
    .join("")}</div>`;
function shell() {
  const [page] = route();
  const active = page === "stack" ? "stacks" : page;
  const menu = [
    ["dashboard", "Обзор", "home"],
    ["chats", "Чаты", "agent"],
    ["stacks", "Agent Stacks", "stack", state.stacks.length],
    ["repositories", "Repositories", "repo", state.repos.length],
    ["environments", "Environments", "env", state.envs.length],
    ["memory", "Memory", "memory", state.memories.length],
    ["sandbox", "Sandbox", "sandbox", state.sandboxes.length],
    ["crons", "Crons", "cron", state.crons.length],
    ["workspace", "Workspace", "folder"],
    ["approvals", "Согласования", "shield"],
    ["connections", "Подключения", "link"],
    ["profile", "Мой Dot", "agent"],
    ["admin", "Администрирование", "shield"],
    ["stats", "Stats", "stats"],
    ["live", "Диагностика", "settings"],
  ];
  $("#header").innerHTML =
    `<div class="topcell logo"><a href="#/factory" aria-label="Главная Cloud.ru">${icon("logo")}</a></div><div class="topcell platform">${icon("evolution")}${icon("chevron", true)}</div><div class="topcell apps">${ibtn("apps", "navigate", "Каталог сервисов", 'data-path="factory"')}</div><div class="topcell project"><button class="project-button" data-action="project"><span>${esc(state.project)}<small>Проект</small></span>${icon("chevron", true)}</button></div>${ibtn("apps", "mobile-nav", "Меню навигации", 'class="mobile-menu"').replace('class="iconbtn"', 'class="iconbtn mobile-menu"').replace('class="mobile-menu"', "")}<nav class="breadcrumbs" aria-label="Хлебные крошки"><a class="crumb-evolution" href="#/factory">Evolution</a><span class="crumb-evolution">/</span><a href="#/factory">AI Factory</a><span>/</span><a href="#/dashboard">AHP</a><span class="crumb-extra">/</span><span class="crumb-extra">${esc(page === "stack" ? currentStack()?.name || "Стек" : menu.find((m) => m[0] === active)?.[1] || "Сервисы")}</span></nav><div class="topright"><span class="proto-label">Интерактивный прототип</span>${ibtn("help", "docs", "Помощь")}${ibtn("bell", "notifications", "Уведомления")}<span class="avatar" title="Демо-пользователь">ДП</span></div>`;
  const complete = steps().filter((s) => s.done).length;
  $("#sidebar").innerHTML =
    `<a class="sidebar-back" href="#/factory">${icon("back", true)} Все сервисы AI Factory</a><div class="sidebar-product"><span class="product-icon">${icon("agent")}</span><span>Agent Harness Platform</span></div><nav>${menu.map(([id, title, ic, n]) => `${id === "stats" ? '<div class="nav-divider"></div>' : ""}<a class="navitem ${active === id ? "active" : ""}" href="#/${id}" ${active === id ? 'aria-current="page"' : ""}>${icon(ic)}<span>${title}</span>${n ? `<span class="count">${n}</span>` : ""}</a>`).join("")}</nav><div class="sidebar-bottom">${btn(`${icon("help")} Помощь и документация`, "docs", "ghost")}<div class="sidebar-footer"><span>AHP · серверные данные</span></div></div>`;
}
function heading(title, subtitle = "", actions = "") {
  return `<div class="page-heading"><div><h1><span class="heading-icon">${icon({ dashboard: "agent", factory: "apps", stacks: "stack", stack: "stack", repositories: "repo", environments: "env", memory: "memory", sandbox: "sandbox", crons: "cron", stats: "stats" }[route()[0]] || "agent")}</span><span>${title}</span></h1>${subtitle ? `<p class="muted">${subtitle}</p>` : ""}</div><div class="actions">${actions}</div></div>`;
}
function table(
  headers,
  rows,
  { filter = true, placeholder = "Поиск", statuses = [], footer = true } = {},
) {
  return `${filter ? `<div class="toolbar">${ibtn("refresh", "refresh", "Обновить список")}${icon("search")}<input class="search" data-filter="search" aria-label="${placeholder}" placeholder="${placeholder}">${statuses.length ? `<select data-filter="status" aria-label="Фильтр по статусу"><option value="">Все статусы</option>${statuses.map(([v, l]) => `<option value="${v}">${l}</option>`).join("")}</select>` : ""}</div>` : ""}<div class="tablewrap"><table class="data-table"><thead><tr>${headers.map((h) => `<th scope="col">${h}</th>`).join("")}</tr></thead><tbody>${rows.map((r) => `<tr data-search="${esc(r.search || r.cells.join(" ").replace(/<[^>]+>/g, " "))}" data-status="${r.status || ""}">${r.cells.map((c, i) => `<td>${i === 0 && !String(c).includes('class="icon') ? `<div class="row-title">${icon({ repositories: "repo", environments: "env", memory: "memory", crons: "cron", stacks: "stack", sandbox: "snapshot", stack: "file" }[route()[0]] || "file", true)}<div>${c}</div></div>` : c}</td>`).join("")}</tr>`).join("")}<tr class="no-results" hidden><td colspan="${headers.length}" class="muted">По вашему запросу ничего не найдено</td></tr></tbody></table></div>${footer ? `<div class="table-footer"><span data-count>Записей: ${rows.length}</span><span>Показаны все записи</span></div>` : ""}`;
}
function dashboard() {
  let deployed = state.stacks.filter((s) => s.status === "running").length;
  const completed = steps().filter((s) => s.done).length;
  return (
    heading(
      "Agent Harness Platform",
      "Развёртывание и эксплуатация агентских систем",
      btn(`${icon("plus")} Создать Agent Stack`, "stack-new", "primary"),
    ) +
    `<section class="panel hero"><div class="hero-copy"><span class="eyebrow">AI FACTORY / AGENT HARNESS PLATFORM</span><h2>${deployed ? "Ваша агентская система под контролем" : "От конфигурации до работающей системы"}</h2><p>${deployed ? "Управляйте агентами, MCP-серверами и обновлениями в одном месте. Исходный код и сборка остаются в вашем CI/CD." : "Подключите agent.yaml, выберите готовые образы и запустите агентов. AHP объединит компоненты в систему и поможет управлять её работой."}</p><div class="actions">${btn(deployed ? "Открыть Agent Stacks" : "Начать настройку", deployed ? "navigate" : "guide", "primary", deployed ? 'data-path="stacks"' : "")}${btn("Открыть готовый пример", "load-demo", "secondary")}</div></div><img class="hero-image" src="assets/ai-agents.png" alt="Иллюстрация сервиса AI Agents из консоли Cloud.ru"></section>` +
    `<div class="grid cols4" style="margin-bottom:16px">${metric("Agent Stacks", state.stacks.length, `${deployed} запущено`, "stack", "stacks")}${metric(
      "Компоненты",
      state.stacks.reduce((n, s) => n + s.components.length, 0),
      "Агенты и MCP-серверы",
      "agent",
      "stacks",
    )}${metric("Sandbox", state.sandboxes.filter((b) => b.active).length, "Активные рабочие среды", "sandbox", "sandbox")}${metric("Память", state.memories.reduce((n, m) => n + m.used, 0) + " <small>МБ</small>", "Объём сохранённых данных", "memory", "memory")}</div>` +
    `<div class="split-content">${panel(completed < 5 ? "Начало работы" : "Последние изменения", completed < 5 ? stepsHtml() : activities(state.events.slice(0, 4)), completed < 5 ? `<span class="muted small">${completed} из 5 шагов</span>` : link("Все стеки", "stacks"))}${panel(
      "Ресурсы проекта",
      kv([
        [
          "Repository",
          link(`${state.repos.length} подключено`, "repositories"),
        ],
        ["Environments", link(`${state.envs.length} переменных`, "environments")],
        ["Банки памяти", link(`${state.memories.length} создано`, "memory")],
        [
          "Расписания",
          link(
            `${state.crons.filter((c) => c.enabled).length} активно`,
            "crons",
          ),
        ],
      ]) +
        '<div class="divider"></div><h3>Как работает AHP</h3><p class="muted small" style="margin-top:10px">Ваш CI/CD собирает и публикует образы. AHP получает конфигурацию и версии артефактов, развёртывает систему и отслеживает её состояние.</p>' +
        btn(
          "Посмотреть путь пользователя",
          "docs",
          "ghost",
          'style="margin-top:12px"',
        ),
    )}</div>`
  );
}
function metric(label, value, foot, ic, path) {
  return `<a class="metric-link" href="#/${path}"><div class="metric"><div class="metric-label"><span class="metric-icon">${icon(ic)}</span>${label}</div><div class="metric-value">${value}</div><div class="metric-foot">${foot}</div></div></a>`;
}
function activities(events) {
  return events.length
    ? events
        .map(
          (e) =>
            `<div class="activity"><span class="event-dot" style="${e.status === "error" ? "border-color:var(--red)" : ""}"></span><div class="event-copy"><strong>${esc(e.title)}</strong><p>${esc(e.detail)}</p><time>${e.time} · ${esc(e.source)}</time></div>${badge(e.status === "error" ? "Ошибка" : "Выполнено", e.status === "error" ? "error" : "")}</div>`,
        )
        .join("")
    : '<p class="muted small">Здесь появится история ваших действий и обновлений.</p>';
}
function factory() {
  return (
    heading(
      "AI Factory",
      "Инструменты для создания, развёртывания и эксплуатации AI-приложений",
    ) +
    `<div class="grid cols3">${[
      [
        "Agent Harness Platform",
        "Развёртывание агентских систем из agent.yaml. Агенты, MCP, память и sandbox в одном месте.",
        "stack",
        "dashboard",
      ],
      [
        "AI Agents",
        "Разработка автономных помощников для выполнения конкретных задач.",
        "agent",
        "",
      ],
      [
        "Foundation Models",
        "Доступ к фундаментальным моделям через API.",
        "stats",
        "",
      ],
      [
        "ML Inference",
        "Развёртывание моделей машинного обучения.",
        "sandbox",
        "",
      ],
      ["Notebooks", "Среда для экспериментов и разработки.", "repo", ""],
      [
        "Managed RAG",
        "Работа с базами знаний и источниками данных.",
        "memory",
        "",
      ],
    ]
      .map(
        ([name, desc, ic, path]) =>
          `<section class="panel"><div class="resource-card" style="border:0;padding:0"><span class="product-icon">${icon(ic)}</span><div><h3>${name}</h3>${name === "Agent Harness Platform" ? '<span class="tag">Preview</span>' : ""}</div></div><p class="muted" style="margin:18px 0 22px">${desc}</p>${btn("Открыть", path ? "navigate" : "outside", "secondary", path ? `data-path="${path}"` : `data-name="${name}"`)}</section>`,
      )
      .join("")}</div>` +
    notice(
      "Это локальный прототип. Раздел Agent Harness Platform интерактивен; остальные карточки показывают расположение продукта в AI Factory.",
    )
  );
}
function repositories() {
  return (
    heading(
      "Repositories",
      "Внешние источники конфигурации: GitHub, GitLab и другие Git-сервисы",
      btn(`${icon("plus")} Подключить репозиторий`, "repo-new", "primary"),
    ) +
    panel(
      "Evolution Repos проекта",
      state.evolutionRepos.length
        ? table(
            ["Название", "Ветка", "Последняя ревизия", "Назначение"],
            state.evolutionRepos.map((r) => ({
              cells: [
                `<strong>${esc(r.name)}</strong>`,
                esc(r.branch || "main"),
                `<code>${esc(r.commit || "—")}</code>`,
                "Репозиторий проекта",
              ],
            })),
            { filter: false },
          )
        : `<p class="muted">В проекте пока нет Evolution Repo. Создайте его в Evolution — он появится здесь для выбора при создании Agent Stack.</p>`,
    ) +
    panel(
      "Внешние Repositories",
      state.repos.length
        ? table(
            [
              "Название",
              "Провайдер",
              "Синхронизация",
              "Последняя ревизия",
              "Обновлено",
              "",
            ],
            state.repos.map((r) => ({
              search: r.name + " " + r.url,
              cells: [
                `<button class="textbtn" data-action="repo-detail" data-id="${r.id}">${esc(r.name)}</button><div class="sub">${esc(r.url)}</div>`,
                esc(r.provider),
                badge("Подключён"),
                `<code>${esc(r.commit)}</code>`,
                esc(r.synced),
                btn(
                  "Синхронизировать",
                  "repo-sync",
                  "compact",
                  `data-id="${r.id}"`,
                ),
              ],
            })),
          )
        : empty(
            "Подключите внешний репозиторий",
            "Repository нужен только для внешнего Git-источника. Для репозитория проекта выберите Evolution Repo в мастере Agent Stack.",
            "repo-new",
            "Подключить репозиторий",
            "repo",
          ),
    )
  );
}
function environments() {
  return (
    heading(
      "Environments",
      "Глобальные переменные окружения и секреты проекта",
      btn(`${icon("plus")} Создать переменную`, "env-new", "primary"),
    ) +
    panel(
      "",
      state.envs.length
        ? table(
            ["Переменная", "Значение", "Тип", "Источник", "Изменена", ""],
            state.envs.map((v) => ({
              cells: [
                `<button class="textbtn" data-action="env-detail" data-id="${v.id}"><code>${esc(v.key)}</code></button>`,
                v.secret ? '<span class="secrets">••••••••</span>' : esc(v.value),
                v.secret ? "Секрет" : "Переменная",
                esc(v.source || "Вручную"),
                esc(v.updated),
                ibtn(
                  "more",
                  "env-detail",
                  "Открыть переменную",
                  `data-id="${v.id}"`,
                ),
              ],
            })),
          )
        : empty(
            "Добавьте первую переменную окружения",
            "Переменные глобальны для проекта и автоматически доступны всем Agent Stack.",
            "env-new",
            "Создать переменную",
            "env",
          ),
    )
  );
}
function stacks() {
  return (
    heading(
      "Agent Stacks",
      "Агентские системы в проекте",
      btn(`${icon("plus")} Создать Agent Stack`, "stack-new", "primary"),
    ) +
    panel(
      "",
      state.stacks.length
        ? table(
            [
              "Название",
              "Статус",
              "Компоненты",
              "Конфигурация",
              "Обновлён",
              "",
            ],
            state.stacks.map((s) => ({
              search: s.name + " " + s.tags.join(" ") + " " + s.path,
              status: s.status,
              cells: [
                `${link(s.name, "stack/" + s.id)}<div>${tags(s.tags)}</div>`,
                stackStatus(s),
                `${s.components.length} компонента`,
                `<span class="mono">${esc(s.path)}</span><div class="sub">${esc(s.branch)}</div>`,
                esc(s.updated),
                ibtn(
                  "arrow",
                  "navigate",
                  "Открыть стек",
                  `data-path="stack/${s.id}"`,
                ),
              ],
            })),
            {
              statuses: [
                ["running", "Запущен"],
                ["stopped", "Остановлен"],
                ["error", "Ошибка"],
                ["deploying", "Обновляется"],
              ],
              placeholder: "Поиск по названию или меткам",
            },
          )
        : empty(
            "Создайте первый Agent Stack",
            "Выберите agent.yaml в репозитории. Платформа покажет агентов, MCP-серверы и связанные ресурсы перед запуском.",
            "stack-new",
            "Создать Agent Stack",
          ),
    )
  );
}
function yaml(s) {
  return `# Иллюстративная конфигурация прототипа; схема AHP не утверждена\nname: ${s.name}\nsource:\n  type: ${sourceKind(s)}\n  name: ${sourceLabel(s)}\n  branch: ${s.branch}\n  path: ${s.path}\nenvironmentVariables: project\ncomponents:\n${s.components.map((c) => `  ${c.name}:\n    type: ${c.type}\n    image: ${c.image}\n    digest: ${c.digest}`).join("\n")}\n${s.memoryId ? `memory: ${state.memories.find((m) => m.id === s.memoryId)?.name || "—"}\n` : ""}evals: ${s.path.replace("agent.yaml", "eval/")}`;
}
const baseComponents = () => [
  {
    id: "orchestrator",
    name: "orchestrator",
    type: "agent",
    image: "макет · образ выдаёт платформа",
    digest: "sha256:41ab8f62d9e03c5a",
    version: "v1.0.0",
  },
  {
    id: "researcher",
    name: "researcher",
    type: "agent",
    image: "макет · образ выдаёт платформа",
    digest: "sha256:7dc4e981be206a3f",
    version: "v1.0.0",
  },
  {
    id: "knowledge-mcp",
    name: "knowledge-mcp",
    type: "mcp",
    image: "макет · образ выдаёт платформа",
    digest: "sha256:9ff3614d80c7ae2b",
    version: "v1.0.0",
  },
];
function components(s, preview = false) {
  return table(
    ["Компонент", "Тип", "Состояние", "Образ", ""],
    s.components.map((c) => ({
      cells: [
        `<div class="row-title">${icon(c.type === "mcp" ? "mcp" : "agent")}<strong>${esc(c.name)}</strong></div>`,
        c.type === "mcp" ? "MCP-сервер" : "Агент",
        preview ? badge("Готов к запуску", "neutral") : stackStatus(s),
        `<span class="mono">${esc(c.image)}</span><div class="sub mono">${esc(c.digest)}</div>`,
        preview
          ? ""
          : btn("Журналы", "component", "compact", `data-id="${c.id}"`),
      ],
    })),
    { filter: false, footer: false },
  );
}
function stackPage() {
  const s = currentStack();
  if (!s)
    return (
      heading("Стек не найден") +
      panel(
        "",
        empty(
          "Этого стека больше нет",
          "Вернитесь к списку или создайте новую агентскую систему.",
          "navigate",
          "Открыть стеки",
        ),
      )
    );
  const tab = route()[2] || "overview";
  const busy = s.status === "deploying";
  const actions =
    btn("CI/CD", "integration", "", `data-id="${s.id}"`) +
    btn("Обновить", "update", "secondary", busy ? "disabled" : "") +
    btn(
      s.status === "running"
        ? "Остановить"
        : s.versions.length
          ? "Запустить"
          : "Развернуть",
      s.status === "running" ? "stop" : "deploy",
      "primary",
      busy ? "disabled" : "",
    ) +
    ibtn("more", "stack-menu", "Действия со стеком");
  const tabs = [
    ["overview", "Обзор"],
    ["components", "Компоненты"],
    ["configuration", "Конфигурация"],
    ["updates", "Updates"],
    ["eval", "Eval"],
    ["resources", "Ресурсы"],
  ];
  let body = "";
  if (tab === "overview")
    body = `${!s.versions.length ? `<div class="next-action"><div>${icon("stack")} <strong>Конфигурация готова к развёртыванию</strong><p>Образы доступны. Проверьте состав и запустите систему.</p></div>${btn("Развернуть", "deploy", "primary")}</div>` : ""}${s.status === "error" ? notice("Последнее обновление завершилось с ошибкой. Ниже показано фактическое состояние. Диагностика доступна в Updates.", "error") : ""}<div class="grid cols3" style="margin-bottom:16px">${metric("Компоненты", s.components.length, "Агенты и MCP", "stack", "stack/" + s.id + "/components")}${metric("Версия", s.versions.length ? "r" + s.versions.length : "—", s.versions.length ? "Конфигурация зафиксирована" : "Ещё не развёрнут", "repo", "stack/" + s.id + "/updates")}${metric("Eval", state.evals.find((e) => e.stackId === s.id && e.status === "passed") ? "100 <small>%</small>" : "—", "Последняя успешная проверка", "stats", "stack/" + s.id + "/eval")}</div><div class="split-content">${panel(
      "Общая информация",
      kv([
        ["Статус", stackStatus(s)],
        [
          sourceKind(s),
          esc(sourceLabel(s)),
        ],
        ["Git-ревизия", `<code>${esc(s.commit)}</code>`],
        ["Конфигурация", `<code>${esc(s.path)}</code>`],
        ["Ветка", esc(s.branch)],
        ["Environments", link(`${state.envs.length} глобальных переменных`, "environments")],
        ["Метки", tags(s.tags) + btn("Изменить", "tags", "compact ghost")],
        ["Обновление", s.auto ? "Автоматически" : "Вручную"],
      ]),
    )}${panel("Последние изменения", activities(state.events.filter((e) => e.stackId === s.id).slice(0, 3)))}</div>${panel("Состав системы", components(s))}`;
  if (tab === "components") body = panel("Компоненты системы", components(s));
  if (tab === "configuration")
    body = panel(
      "Источник конфигурации",
      kv([
        [
          "Источник",
          esc(sourceFor(s)?.url || sourceLabel(s)),
        ],
        ["Ветка / ревизия", `${esc(s.branch)} / <code>${esc(s.commit)}</code>`],
        ["Путь", `<code>${esc(s.path)}</code>`],
      ]) +
        '<div class="divider"></div><h3>Итоговая конфигурация</h3>' +
        notice(
          "Показан пример схемы. Формат docker-agent / agent.yaml и контракт подстановки версий требуют согласования.",
        ) +
        yamlBox(yaml(s)) +
        `<div class="settings-group"><label class="switch"><input type="checkbox" data-change="auto" ${s.auto ? "checked" : ""}>Автоматически применять изменения конфигурации и образов</label><p class="small muted" style="margin-top:8px">В демосреде изменения поступают через кнопку «Обновить».</p></div>`,
    );
  if (tab === "updates")
    body = panel(
      "История изменений",
      state.events
        .filter((e) => e.stackId === s.id)
        .map(
          (ev) =>
            `<div class="activity"><span class="event-dot" style="${ev.status === "error" ? "border-color:var(--red)" : ""}"></span><div class="event-copy"><strong>${esc(ev.title)}</strong><p>${esc(ev.detail)}</p><time>${ev.time} · ${esc(ev.source)}</time>${ev.version ? `<p class="mono">${esc(ev.version.commit)} · ${esc(ev.version.components[0].digest)}</p>` : ""}</div><div>${badge(ev.status === "error" ? "Ошибка" : "Успешно", ev.status === "error" ? "error" : "")}${ev.version && s.status !== "deploying" ? btn("Откатить", "rollback", "compact ghost", `data-event="${ev.id}"`) : ""}</div></div>`,
        )
        .join("") || '<p class="muted">Обновлений пока нет.</p>',
    );
  if (tab === "eval") body = evalContent(s);
  if (tab === "resources") body = resourcesContent(s);
  return (
    `<a class="backlink" href="#/stacks">${icon("back", true)} Agent Stacks</a>` +
    heading(
      `<span class="detail-heading">${esc(s.name)} ${stackStatus(s)}</span>`,
      "",
      actions,
    ) +
    `<nav class="tabs" aria-label="Разделы стека">${tabs.map(([id, t]) => `<a class="tab ${tab === id ? "active" : ""}" href="#/stack/${s.id}/${id}" ${tab === id ? 'aria-current="page"' : ""}>${t}</a>`).join("")}</nav>` +
    body
  );
}
function evalContent(s) {
  const runs = state.evals.filter((e) => e.stackId === s.id);
  const latest = runs[0];
  return (
    panel(
      "Проверка агентской системы",
      `<p class="muted" style="margin-bottom:18px">Eval проверяет совместную работу агентов и MCP-серверов на выбранной версии стека.</p>${notice("Eval запускается автоматически после каждого успешного deploy или обновления Agent Stack. Ручной запуск недоступен.")}<div class="resource-card"><span class="product-icon">${icon("stats")}</span><div><h3>support-quality</h3><p>${esc(s.path.replace("agent.yaml", "eval/"))} · 12 сценариев</p><p>Маршрутизация, поиск знаний, инструменты и ограничения</p></div>${latest?.status === "running" ? badge("Выполняется", "info") : badge("Автозапуск после deploy", "neutral")}</div>${latest ? `<div class="divider"></div><div class="panel-head"><div><h3>Последний запуск</h3><p class="muted small">${latest.time} · версия r${latest.revision}</p></div>${badge(latest.status === "running" ? "Выполняется" : latest.status === "passed" ? "Все проверки пройдены" : "Есть ошибки", latest.status === "running" ? "info" : latest.status === "passed" ? "" : "error")}</div>${latest.status === "running" ? '<div class="progress"><span style="width:60%"></span></div>' : `<div class="result-score" style="${latest.status === "failed" ? "color:var(--red)" : ""}">${latest.passed}<small> / 12 сценариев</small></div>`}` : ""}`,
    ) +
    (runs.length
      ? panel(
          "История eval-запусков",
          table(
            ["Время", "Версия", "Результат", "Сценарии", ""],
            runs.map((e) => ({
              cells: [
                e.time,
                "r" + e.revision,
                badge(
                  e.status === "running"
                    ? "Выполняется"
                    : e.status === "passed"
                      ? "Пройден"
                      : "Ошибка",
                  e.status === "passed"
                    ? ""
                    : e.status === "running"
                      ? "info"
                      : "error",
                ),
                e.status === "running" ? "—" : e.passed + "/12",
                btn(
                  "Отчёт",
                  "eval-report",
                  "compact",
                  `data-id="${e.id}" ${e.status === "running" ? "disabled" : ""}`,
                ),
              ],
            })),
            { filter: false },
          ),
        )
      : "")
  );
}
function resourcesContent(s) {
  const m = state.memories.find((m) => m.id === s.memoryId),
    boxes = state.sandboxes.filter((b) => b.stackId === s.id),
    crons = state.crons.filter((c) => c.stackId === s.id);
  return (
    panel(
      "Память",
      m
        ? `<div class="resource-card">${icon("memory")}<div><h3>${esc(m.name)}</h3><p>${m.used} МБ использовано</p></div>${btn("Открыть", "memory-detail", "compact", `data-id="${m.id}"`)}</div>`
        : notice(
            "Банк памяти не подключён. Его можно выбрать при создании стека.",
          ),
    ) +
    panel(
      "Sandbox",
      boxes.length
        ? boxes
            .map(
              (b) =>
                `<div class="resource-card">${icon("sandbox")}<div><h3>${esc(b.name)}</h3><p>Процессы, действия агентов, snapshots и результаты</p></div>${btn(b.active ? "Открыть" : "Snapshot", b.active ? "sandbox-detail" : "snapshot-detail", "compact", `data-id="${b.id}"`)}</div>`,
            )
            .join("")
        : empty(
            "Агент подготовит рабочую среду",
            "Sandbox создаётся самим агентом и появляется здесь автоматически. Внешние агенты могут использовать Sandbox API.",
            "",
            "",
            "sandbox",
          ),
      link("Открыть Sandbox API", "sandbox"),
    ) +
    panel(
      "Расписания",
      crons.length
        ? crons
            .map(
              (c) =>
                `<div class="resource-card">${icon("cron")}<div><h3>${esc(c.name)}</h3><p>${esc(c.schedule)} · ${esc(c.timezone)}</p></div>${badge(c.enabled ? "Включено" : "Отключено", c.enabled ? "" : "neutral")}</div>`,
            )
            .join("")
        : '<p class="muted">У стека пока нет расписаний.</p>',
      btn("Создать", "cron-new", "secondary"),
    )
  );
}
function memoryPage() {
  return (
    heading(
      "Memory",
      "Общая память для агентов и агентских систем",
      btn(`${icon("plus")} Создать банк памяти`, "memory-new", "primary"),
    ) +
    panel(
      "",
      state.memories.length
        ? table(
            ["Название", "Использовано", "Связанные стеки", "Создан", ""],
            state.memories.map((m) => ({
              cells: [
                `<button class="textbtn" data-action="memory-detail" data-id="${m.id}">${esc(m.name)}</button>`,
                m.used + " МБ",
                state.stacks
                  .filter((s) => s.memoryId === m.id)
                  .map((s) => link(s.name, "stack/" + s.id))
                  .join(", ") || "Не подключён",
                m.created,
                ibtn(
                  "more",
                  "memory-detail",
                  "Открыть банк",
                  `data-id="${m.id}"`,
                ),
              ],
            })),
          )
        : empty(
            "Создайте банк памяти",
            "Сохраняйте знания и контекст, которые агенты смогут использовать между задачами.",
            "memory-new",
            "Создать банк памяти",
            "memory",
          ),
    )
  );
}
function sandboxApi() {
  return panel(
    "Sandbox API",
    `<p class="muted small">Внешний агент может использовать API для запуска sandbox и работы с процессами, файлами и snapshots. Агенты создают рабочие среды самостоятельно.</p>${notice("Недоступно в текущем прототипе · compute_not_mounted: плоскость вычислений не смонтирована, реального endpoint нет.")}`,
  );
}
function memoryApi(m) {
  return `<h3 class="icon-title">${icon("plug")}Memory API · подключение внешнего агента</h3><p class="muted small" style="margin-top:8px">Передайте агенту ссылку на этот банк и учётные данные с доступом к памяти.</p><p class="muted small">Креденциал выдаёт платформа на время сессии; в браузере он не хранится.</p>${notice("Недоступно в текущем прототипе · memory_bank_link: адрес банка выдаёт платформа, ручная ссылка не используется.")}<div class="divider"></div>`;
}
function sandboxPage() {
  const tab = route()[1] === "snapshots" ? "snapshots" : "all";
  const archives = state.sandboxes.flatMap((b) =>
    b.snapshots.map((snapshot) => ({ b, snapshot })),
  );
  const tabs = `<nav class="tabs" aria-label="Разделы Sandbox"><a class="tab ${tab === "all" ? "active" : ""}" href="#/sandbox" ${tab === "all" ? 'aria-current="page"' : ""}>${icon("sandbox", true)} Все sandbox <span class="tag">${state.sandboxes.length}</span></a><a class="tab ${tab === "snapshots" ? "active" : ""}" href="#/sandbox/snapshots" ${tab === "snapshots" ? 'aria-current="page"' : ""}>${icon("snapshot", true)} Snapshots <span class="tag">${archives.length}</span></a></nav>`;
  let body = "";
  if (tab === "all")
    body = state.sandboxes.length
      ? table(
          ["Название", "Статус", "Стек", "Процессы", "Создан", ""],
          state.sandboxes.map((b) => ({
            status: b.active ? "running" : "completed",
            search: b.name + " " + (b.active ? "Запущен" : "Завершён"),
            cells: [
              `<div class="row-title">${icon("sandbox")}<button class="textbtn" data-action="${b.active ? "sandbox-detail" : "snapshot-detail"}" data-id="${b.id}">${esc(b.name)}</button></div>`,
              badge(
                b.active ? "Запущен" : "Завершён",
                b.active ? "" : "neutral",
              ),
              link(
                state.stacks.find((s) => s.id === b.stackId)?.name || "—",
                "stack/" + b.stackId,
              ),
              b.active ? "1 процесс" : "—",
              b.created,
              btn(
                b.active ? "Открыть" : "Snapshot",
                b.active ? "sandbox-detail" : "snapshot-detail",
                "compact",
                `data-id="${b.id}"`,
              ),
            ],
          })),
          {
            statuses: [
              ["running", "Запущен"],
              ["completed", "Завершён"],
            ],
          },
        )
      : empty(
          "Sandbox появятся по мере работы агентов",
          "Агент сам запускает sandbox, когда ему нужна рабочая среда. Здесь отображаются запущенные и завершённые среды. У завершённой среды можно открыть snapshot и скачать результаты.",
          "",
          "",
          "sandbox",
        );
  if (tab === "snapshots")
    body = archives.length
      ? notice(
          "Снимки сохраняют файловые изменения среды. Откройте snapshot, чтобы посмотреть действия агентов и скачать результаты.",
        ) +
        table(
          ["Snapshot", "Sandbox", "Стек", "Изменения", "Создан", ""],
          archives.map(({ b, snapshot: x }) => ({
            search: x.name + " " + b.name,
            cells: [
              `<div class="row-title">${icon("snapshot")}<button class="textbtn" data-action="snapshot-detail" data-id="${b.id}">${esc(x.name)}</button></div>`,
              esc(b.name),
              link(
                state.stacks.find((s) => s.id === b.stackId)?.name || "—",
                "stack/" + b.stackId,
              ),
              `${x.files} файла · ${esc(x.size)}`,
              x.created,
              btn("Открыть", "snapshot-detail", "compact", `data-id="${b.id}"`),
            ],
          })),
        )
      : empty(
          "Snapshots пока нет",
          "После завершения работы агента здесь появятся снимки среды и сохранённые результаты.",
          "",
          "",
          "snapshot",
        );
  return (
    heading("Sandbox", "Запущенные и завершённые рабочие среды агентов") +
    sandboxApi() +
    tabs +
    panel("", body)
  );
}

function cronsPage() {
  return (
    heading(
      "Crons",
      "Действия по расписанию, созданные вами и агентами",
      btn(`${icon("plus")} Создать расписание`, "cron-new", "primary"),
    ) +
    panel(
      "",
      state.crons.length
        ? table(
            ["Название", "Стек", "Расписание", "Создатель", "Состояние", ""],
            state.crons.map((c) => ({
              status: c.enabled ? "enabled" : "disabled",
              search: c.name + " " + c.creator + " " + c.schedule,
              cells: [
                `<button class="textbtn" data-action="cron-detail" data-id="${c.id}">${esc(c.name)}</button>`,
                link(
                  state.stacks.find((s) => s.id === c.stackId)?.name || "—",
                  "stack/" + c.stackId,
                ),
                `<code>${esc(c.schedule)}</code><div class="sub">${esc(c.timezone)}</div>`,
                esc(c.creator),
                `<label class="switch"><input type="checkbox" aria-label="Включить ${esc(c.name)}" data-change="cron-toggle" data-id="${c.id}" ${c.enabled ? "checked" : ""}>${c.enabled ? "Включено" : "Выключено"}</label>`,
                btn("Открыть", "cron-detail", "compact", `data-id="${c.id}"`),
              ],
            })),
            {
              statuses: [
                ["enabled", "Включено"],
                ["disabled", "Выключено"],
              ],
            },
          )
        : empty(
            "Автоматизируйте повторяющиеся задачи",
            "Настройте время запуска и действие для агентской системы. Здесь также появятся расписания, созданные агентами.",
            "cron-new",
            "Создать расписание",
            "cron",
          ),
    )
  );
}
function statsPage() {
  const days =
    state.statsPeriod === "day"
      ? ["00:00", "04:00", "08:00", "12:00", "16:00", "20:00"]
      : ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"];
  const total = state.sandboxes.length;
  const values = days.map((_, i) =>
    total ? Math.max(1, Math.round(((i + 1) * total) / 2)) : 0,
  );
  return (
    heading(
      "Stats",
      "Статистика работы агентских систем",
      `<select style="width:180px" aria-label="Период статистики" data-change="stats-period"><option value="week" ${state.statsPeriod === "week" ? "selected" : ""}>Последние 7 дней</option><option value="day" ${state.statsPeriod === "day" ? "selected" : ""}>Последние 24 часа</option></select>`,
    ) +
    notice(
      "Статистика иллюстрирует интерфейс. Счётчики операций основаны на локальных действиях, распределение по времени демонстрационное.",
    ) +
    `<div class="grid cols4" style="margin-bottom:16px">${metric("Развёртывания", state.events.filter((e) => e.version).length, "Применённые версии", "stack", "stacks")}${metric("Eval-запуски", state.evals.length, "Проверки всей системы", "stats", "stacks")}${metric("Задачи агента", total, "Созданные рабочие среды", "sandbox", "sandbox")}${metric("Память", state.memories.reduce((n, m) => n + m.used, 0) + " <small>МБ</small>", "Общий использованный объём", "memory", "memory")}</div>` +
    `<div class="grid cols2">${panel("Задачи агентов", `<div class="chart">${days.map((d, i) => `<div class="bar-group"><div class="bar" style="height:${values[i] ? Math.max(10, (values[i] / Math.max(...values)) * 90) : 0}%" data-value="${values[i]}"></div><small>${d}</small></div>`).join("")}</div>`)}${panel(
      "Состояние компонентов",
      kv([
        [
          "Запущено",
          String(
            state.stacks
              .filter((s) => s.status === "running")
              .reduce((n, s) => n + s.components.length, 0),
          ),
        ],
        [
          "Остановлено",
          String(
            state.stacks
              .filter((s) => s.status === "stopped")
              .reduce((n, s) => n + s.components.length, 0),
          ),
        ],
        [
          "Ошибки обновлений",
          String(state.events.filter((e) => e.status === "error").length),
        ],
        [
          "Успешные eval",
          String(state.evals.filter((e) => e.status === "passed").length),
        ],
      ]),
    )}</div>`
  );
}
function render() {
  shell();
  const [page] = route();
  const views = {
    factory,
    dashboard,
    stacks,
    repositories,
    environments,
    memory: memoryPage,
    sandbox: sandboxPage,
    crons: cronsPage,
    stats: statsPage,
    stack: stackPage,
  };
  $("#main").innerHTML = (views[page] || dashboard)();
  document.title = `${page === "stack" ? currentStack()?.name || "Стек" : { dashboard: "Обзор", factory: "AI Factory", stacks: "Agent Stacks", repositories: "Repositories", environments: "Environments", memory: "Memory", sandbox: "Sandbox", crons: "Crons", stats: "Stats" }[page] || "AHP"} · AHP · Cloud.ru`;
}
function openModal(
  title,
  body,
  foot = "",
  { wide = false, center = false, key = "" } = {},
) {
  returnFocus = document.activeElement;
  modal = key || title;
  $("#overlay-root").innerHTML =
    `<div class="modal-backdrop ${center ? "center" : ""}"><section class="drawer ${wide ? "wide" : ""}" role="dialog" aria-modal="true" aria-labelledby="dialog-title"><div class="drawer-head"><h2 id="dialog-title">${esc(title)}</h2>${ibtn("close", "close", "Закрыть")}</div><div class="drawer-body">${body}</div>${foot ? `<div class="drawer-foot">${foot}</div>` : ""}</section></div>`;
  document.body.style.overflow = "hidden";
  requestAnimationFrame(() =>
    $(
      ".drawer input:not([type=checkbox]):not([type=radio]), .drawer select, .drawer .iconbtn",
    )?.focus(),
  );
}
function closeModal() {
  const had = !!modal;
  modal = null;
  $("#overlay-root").innerHTML = "";
  document.body.style.overflow = "";
  if (had && returnFocus?.isConnected) returnFocus.focus();
}
const cancel = () => btn("Отмена", "close");
function values() {
  return Object.fromEntries(
    [
      ...document.querySelectorAll(
        ".drawer input[name],.drawer select[name],.drawer textarea[name]",
      ),
    ].map((e) => [e.name, e.type === "checkbox" ? e.checked : e.value.trim()]),
  );
}
function error(text) {
  const el = $(".form-error");
  if (el) {
    el.textContent = text;
    el.setAttribute("role", "alert");
  } else toast(text, "error");
}
const formError = () => '<p class="form-error" aria-live="polite"></p>';
function showGuide() {
  openModal(
    "Начало работы",
    `<p class="muted" style="margin-bottom:15px">Пройдите путь от подключения конфигурации до проверки работающей системы.</p>${stepsHtml()}<div class="divider"></div><h3>Хотите сразу посмотреть результат?</h3><p class="muted small" style="margin:10px 0 16px">Готовый пример содержит стек support-dev, память, sandbox, расписание и историю развёртываний.</p>${btn("Открыть готовый пример", "load-demo", "secondary")}`,
    btn("Продолжить самостоятельно", "close"),
    { key: "guide" },
  );
}
function repoForm(existing) {
  openModal(
    existing ? "Настройки Repository" : "Подключить репозиторий",
    `${notice("Repository подключается только для внешнего Git-источника. Evolution Repo проекта выбирать здесь не нужно.")}${field("Название", "name", existing?.name || "agent-system-config", "agent-system-config")}${select("Провайдер", "provider", ["GitLab", "GitHub", "Другой Git"], existing?.provider || "GitLab")}${field("URL репозитория", "url", existing?.url || "https://git.example.com/team/agent-system-config.git", "https://git.example.com/team/config.git", "url")}${select("Доступ", "access", ["Приватный репозиторий", "Публичный репозиторий"], existing?.access || "Приватный репозиторий")}${field("Ветка по умолчанию", "branch", existing?.branch || "develop")}<label class="checkbox-field"><input name="webhook" type="checkbox" ${existing?.webhook === false ? "" : "checked"}><span>Обновлять конфигурацию по webhook</span></label><div class="file-tree">dev/agent.yaml<br>dev/eval/<br>stage/agent.yaml<br>prod/agent.yaml</div>${formError()}`,
    cancel() +
      btn(
        existing ? "Сохранить" : "Проверить и подключить",
        "repo-save",
        "primary",
        existing ? `data-id="${existing.id}"` : "",
      ),
    { key: "repo-form" },
  );
}
function repoDetail(r) {
  if (!r) return toast("Репозиторий не найден", "error");
  openModal(
    r.name,
    `${kv([
      ["Провайдер", esc(r.provider)],
      ["Адрес", esc(r.url)],
      ["Ветка", esc(r.branch)],
      ["Ревизия", `<code>${esc(r.commit)}</code>`],
      ["Синхронизация", badge("Подключён")],
      ["Обновлено", r.synced],
    ])}<div class="divider"></div><h3>Webhook</h3><p class="muted small" style="margin:10px 0">${r.webhook ? "Включён. Изменение Git-конфигурации обновляет связанные стеки." : "Автоматическая синхронизация выключена."}</p>${notice("Недоступно в текущем прототипе · repository_webhook_not_wired: webhook repositories не настроен.")}<div class="divider"></div><h3>Конфигурации</h3><div class="file-tree">dev/agent.yaml<br>dev/eval/<br>stage/agent.yaml<br>prod/agent.yaml</div>${formError()}`,
    btn("Удалить", "repo-delete", "danger", `data-id="${r.id}"`) +
      btn("Настроить", "repo-edit", "", `data-id="${r.id}"`) +
      btn("Синхронизировать", "repo-sync", "primary", `data-id="${r.id}"`),
  );
}
function envForm(existing) {
  openModal(
    existing ? "Изменить переменную окружения" : "Создать переменную окружения",
    `${notice("Переменная будет глобальной для всего проекта и автоматически доступной каждому Agent Stack. Значения секретов не сохраняются.")}${field("Имя", "key", existing?.key || "APP_ENV", "APP_ENV")}<label class="checkbox-field"><input name="secret" type="checkbox" ${existing?.secret ? "checked" : ""}><span>Это секрет</span></label>${field("Значение", "value", existing?.secret ? "" : existing?.value || "development", "Значение", existing?.secret ? "password" : "text", existing?.secret ? "Значение будет скрыто и не сохранится в демо." : "")}<label class="checkbox-field"><input name="secretManager" type="checkbox" ${existing?.source === "Secret Manager" ? "checked" : ""}><span>Получать из Secret Manager</span></label>${formError()}`,
    cancel() +
      btn(
        "Сохранить переменную",
        "env-save",
        "primary",
        existing ? `data-id="${existing.id}"` : "",
      ),
    { key: "env-form" },
  );
}
function envDetail(v) {
  if (!v) return toast("Переменная не найдена", "error");
  openModal(
    v.key,
    notice(
      "Глобальная переменная проекта. Секреты скрыты; в прототипе сохраняется только факт их настройки.",
    ) +
      kv([
        ["Значение", v.secret ? '<span class="secrets">••••••••</span>' : esc(v.value)],
        ["Тип", v.secret ? "Секрет" : "Переменная окружения"],
        ["Источник", esc(v.source || "Вручную")],
        ["Доступ", "Все Agent Stack проекта"],
      ]) +
      formError(),
    btn("Удалить", "env-delete", "danger", `data-id="${v.id}"`) +
      btn("Изменить", "env-edit", "primary", `data-id="${v.id}"`),
    { wide: true },
  );
}
function wizard(step = 0) {
  if (!draft)
    draft = {
      step: 0,
      name: "support-dev",
      tags: "team-support, dev",
      sourceType: state.evolutionRepos.length ? "evolution" : "repository",
      sourceId: state.evolutionRepos[0]?.id || state.repos[0]?.id || "",
      branch: "develop",
      path: "dev/agent.yaml",
      memoryId: state.memories[0]?.id || "",
      auto: true,
    };
  draft.step = step;
  const progress = `<div class="wizard-steps">${["Источник", "Окружение", "Проверка", "Запуск"].map((t, i) => `<div class="wizard-step ${i === step ? "active" : ""}"><span class="step-circle ${i <= step ? "current" : ""}">${i + 1}</span><span>${t}</span></div>${i < 3 ? '<span class="wizard-connector"></span>' : ""}`).join("")}</div>`;
  let body = "";
  if (step === 0) {
    const choices = [
      ["evolution", "Evolution Repo (репозиторий проекта)"],
      ["repository", "Repository (внешний Git-источник)"],
    ];
    const sources = draft.sourceType === "evolution" ? state.evolutionRepos : state.repos;
    body = sources.length
      ? `${field("Название стека", "name", draft.name)}${field("Метки через запятую", "tags", draft.tags)}${select("Источник конфигурации", "sourceType", choices, draft.sourceType).replace('<select name="sourceType">', '<select name="sourceType" data-change="wizard-source">')}${select(draft.sourceType === "evolution" ? "Evolution Repo" : "Repository", "sourceId", sources.map((r) => [r.id, r.name]), draft.sourceId)}<p class="muted small" style="margin-top:-8px">${draft.sourceType === "evolution" ? "Репозиторий уже создан в текущем проекте Evolution." : "Repository обязателен только для внешних источников: GitHub, GitLab и других Git-сервисов."}</p><div class="form-row">${field("Ветка", "branch", draft.branch)}${select("Путь к конфигурации", "path", ["dev/agent.yaml", "stage/agent.yaml", "prod/agent.yaml"], draft.path)}</div><div class="file-tree">agent-system-config/<br>&nbsp; dev/agent.yaml<br>&nbsp; dev/eval/<br>&nbsp; stage/agent.yaml<br>&nbsp; prod/agent.yaml</div>`
      : empty(
          draft.sourceType === "evolution" ? "В проекте нет Evolution Repo" : "Сначала подключите внешний Repository",
          draft.sourceType === "evolution" ? "Создайте репозиторий проекта в Evolution или переключитесь на внешний Repository." : "Подключите GitHub, GitLab или другой внешний Git-источник.",
          draft.sourceType === "evolution" ? "" : "repo-new",
          draft.sourceType === "evolution" ? "" : "Подключить репозиторий",
          "repo",
        );
  }
  if (step === 1)
    body = `${select("Банк памяти · необязательно", "memoryId", [["", "Не подключать"], ...state.memories.map((m) => [m.id, m.name])], draft.memoryId)}${notice(`Все ${state.envs.length} глобальных переменных окружения проекта будут доступны стеку автоматически.`)}<label class="checkbox-field"><input name="auto" type="checkbox" ${draft.auto ? "checked" : ""}><span>Автоматически применять изменения конфигурации и версий образов</span></label><h3 class="form-section">Eval-сценарии</h3><div class="selected-resource"><code>${esc(draft.path.replace("agent.yaml", "eval/"))}</code><p class="muted small">12 демонстрационных сценариев · support-quality</p></div>`;
  if (step === 2) {
    body = `<h3>Проверка конфигурации</h3><div class="readiness">${icon("check")}Формат agent.yaml<span class="muted">Корректен · демосхема</span></div><div class="readiness">${icon("check")}Глобальные переменные окружения<span class="muted">${state.envs.length} доступно проекту</span></div><div class="readiness">${icon("check")}Готовые образы<span class="muted">Доступны · демосхема</span></div><div class="divider"></div><h3>Состав агентской системы</h3>${components({ components: baseComponents() }, true)}<p class="muted small" style="margin-top:14px">Состав и валидация демонстрационные. Git и registry не опрашиваются.</p>`;
  }
  if (step === 3)
    body = `${notice("Сборка выполняется в вашем CI/CD. AHP использует готовые образы и разворачивает систему целиком.")}<h3 style="margin-bottom:20px">Всё готово к запуску</h3>${kv(
      [
        ["Название", esc(draft.name)],
        [
          draft.sourceType === "evolution" ? "Evolution Repo" : "Repository",
          esc((draft.sourceType === "evolution" ? state.evolutionRepos : state.repos).find((r) => r.id === draft.sourceId)?.name || "—"),
        ],
        [
          "Конфигурация",
          `<code>${esc(draft.branch)} / ${esc(draft.path)}</code>`,
        ],
        ["Environments", `${state.envs.length} глобальных переменных проекта`],
        ["Компоненты", "2 агента, 1 MCP-сервер"],
        [
          "Метки",
          tags(
            draft.tags
              .split(",")
              .map((t) => t.trim())
              .filter(Boolean),
          ),
        ],
      ],
    )}<div class="divider"></div><label class="checkbox-field"><input name="simulateFailure" type="checkbox"><span>Показать ошибку загрузки образа (демосценарий)</span></label>`;
  openModal(
    "Создание Agent Stack",
    progress + body + formError(),
    `<span class="foot-info">Шаг ${step + 1} из 4</span>${step ? btn("Назад", "wizard-prev") : cancel()}${step === 3 ? btn("Создать черновик", "stack-draft") + btn("Создать и развернуть", "stack-create", "primary") : btn("Продолжить", "wizard-next", "primary", step === 0 && !(draft.sourceType === "evolution" ? state.evolutionRepos.length : state.repos.length) ? "disabled" : "")}`,
    { wide: true, key: "wizard" },
  );
}
function createStack(deploy = false) {
  const v = values();
  const s = {
    id: uid(),
    name: draft.name,
    sourceType: draft.sourceType,
    sourceId: draft.sourceId,
    repoId: draft.sourceType === "repository" ? draft.sourceId : "",
    memoryId: draft.memoryId,
    path: draft.path,
    branch: draft.branch,
    tags: draft.tags
      .split(",")
      .map((t) => t.trim())
      .filter(Boolean),
    auto: draft.auto,
    commit: (draft.sourceType === "evolution" ? state.evolutionRepos : state.repos).find((r) => r.id === draft.sourceId)?.commit || "a14f8d2",
    components: baseComponents(),
    status: "stopped",
    versions: [],
    updated: now(),
  };
  state.stacks.push(s);
  event("Стек создан", s.path + " · " + s.branch, "Пользователь", s.id);
  draft = null;
  go("stack/" + s.id);
  if (deploy) deployStack(s, { fail: v.simulateFailure });
  else toast("Agent Stack создан. Можно развернуть систему.");
}
function deployStack(
  s,
  {
    fail = false,
    source = "Пользователь",
    title = "Развёртывание стека",
    candidate = null,
  } = {},
) {
  if (!s || s.status === "deploying") return;
  s.status = "deploying";
  save();
  render();
  showDeployment(s, title, fail, source, candidate);
}
function showDeployment(s, title, fail, source, candidate) {
  const operation = uid();
  s.operation = operation;
  const labels = [
    "Проверка конфигурации",
    "Фиксация версий образов",
    "Загрузка образов из registry",
    "Запуск агентов и MCP-серверов",
  ];
  const operationBody = (n) =>
    `<p class="muted" style="margin-bottom:18px">${esc(s.name)} · ${esc(source)}</p><div class="timeline-steps">${labels.map((t, i) => `<div class="journey-step"><span class="step-circle ${i < n ? "done" : i === n ? "current" : ""}">${i < n ? icon("check", true) : i + 1}</span><div class="step-copy"><strong>${t}</strong><p>${i < n ? "Завершено" : i === n ? "Выполняется…" : "Ожидает"}</p></div></div>`).join("")}</div><div class="progress"><span style="width:${(n + 1) * 25}%"></span></div><p class="muted small">Локальная симуляция операции. Можно закрыть окно и следить за состоянием стека.</p>`;
  openModal(title, operationBody(0), btn("Скрыть", "close"), {
    key: "deployment",
  });
  [1, 2, 3].forEach((n) =>
    after(n * 550, () => {
      if (s.operation !== operation) return;
      if (modal === "deployment")
        $(".drawer-body").innerHTML = operationBody(n);
    }),
  );
  after(2400, () => {
    if (s.operation !== operation) return;
    delete s.operation;
    if (fail) {
      s.status = "error";
      event(
        "Ошибка развёртывания",
        "knowledge-mcp: registry access denied. Проверьте учётные данные registry.",
        source,
        s.id,
        "error",
      );
      if (modal === "deployment")
        openModal(
          "Не удалось развернуть систему",
          notice(
            "Registry вернул ошибку доступа для knowledge-mcp. Фактическое состояние стека: ошибка.",
            "error",
          ) +
            code(
              "Здесь появится вывод реального pull и регистрации образа, когда будет подключена грань реестра. Симуляцию вывода показываем честно: её нет.",
              true,
            ),
          btn("Закрыть", "close") + btn("Повторить", "deploy", "primary"),
          { key: "deploy-error" },
        );
      toast("Развёртывание завершилось с ошибкой", "error");
    } else {
      if (candidate) {
        s.components = clone(candidate.components);
        s.commit = candidate.commit;
        s.path = candidate.path || s.path;
      }
      s.status = "running";
      s.updated = now();
      const version = {
        id: uid(),
        revision: s.versions.length + 1,
        commit: s.commit,
        path: s.path,
        components: clone(s.components),
        time: now(),
      };
      s.versions.push(version);
      if (!state.sandboxes.some((b) => b.stackId === s.id))
        createTask(s, "Подготовить отчёт по базе знаний системы поддержки", {
          silent: true,
        });
      event(
        title + " завершено",
        `Версия r${version.revision} · ${s.components.length} компонента запущено`,
        source,
        s.id,
        "success",
        version,
      );
      runEval(s, false, true);
      if (modal === "deployment")
        openModal(
          "Система успешно запущена",
          notice(
            "Все компоненты готовы к работе. Версии образов и конфигурация зафиксированы; Eval запущен автоматически.",
          ) +
            components(s) +
            `<div class="divider"></div><h3>Проверка</h3><p class="muted" style="margin:10px 0">Автоматический Eval проверяет совместную работу агентов и MCP.</p>`,
          btn("Открыть стек", "navigate", "", `data-path="stack/${s.id}"`) +
            btn(
              "Открыть Eval",
              "navigate",
              "primary",
              `data-path="stack/${s.id}/eval"`,
            ),
          { wide: true, key: "deployed" },
        );
      toast("Стек " + s.name + " запущен");
    }
    save();
    render();
  });
}
function integration(s) {
  if (!s) return toast("Сначала создайте стек", "error");
  openModal(
    "Интеграция с CI/CD",
    notice(
      "CLI, endpoint и имена аргументов ниже иллюстративные. Команды не выполняются и не являются утверждённым API AHP.",
    ) +
      `<p class="muted" style="margin-bottom:20px">Добавьте deploy-stage после сборки, тестов и публикации образа. У каждого артефакта своя ссылка регистрации.</p>${select(
        "Компонент",
        "component",
        s.components.map((c) => [c.id, c.name]),
      )}${notice("Недоступно в текущем прототипе · artifact_registration_link: ссылка регистрации артефакта выдаётся платформой, вручную не составляется.")}<p class="muted small">Регистрация версии и успешное завершение развёртывания — разные результаты. В прототипе можно воспроизвести оба этапа.</p>`,
    btn("Закрыть", "close") +
      btn("Симулировать deploy-stage", "ci-simulate", "primary"),
    { wide: true, key: "integration" },
  );
}
function updateForm(s, source = "CLI / API", componentId = "") {
  if (!s) return toast("Сначала создайте стек", "error");
  if (s.status === "deploying")
    return toast("Дождитесь текущего развёртывания");
  const c = s.components.find((c) => c.id === componentId) || s.components[0];
  openModal(
    "Обновление Agent Stack",
    `${notice("Выберите событие. Прототип подготовит новую версию и применит её по политике обновлений стека.")}<label class="field"><span>Источник изменения</span><select name="source" data-change="update-source">${["CLI / API", "Git webhook", "Изменяемый тег"].map((x) => `<option ${source === x ? "selected" : ""}>${x}</option>`).join("")}</select></label>${select(
      "Компонент",
      "component",
      s.components.map((c) => [c.id, c.name]),
      c.id,
    )}${field("Новая версия образа", "image", c.image.replace(/:[^:]+$/, ":v1.1.0"))}${field("Новая Git-ревизия", "commit", source === "Git webhook" ? "f82b4a1" : s.commit)}<div class="selected-resource"><strong>Текущая версия</strong><p class="mono" style="margin-top:8px">${esc(c.image)}<br>${esc(c.digest)}</p></div><label class="checkbox-field"><input name="fail" type="checkbox"><span>Симулировать ошибку загрузки образа</span></label><p class="muted small">Для изменяемого тега сохраняется название образа, меняется digest. Для Git webhook меняется ревизия конфигурации.</p>${formError()}`,
    cancel() +
      btn(
        s.auto ? "Применить обновление" : "Зарегистрировать версию",
        "update-apply",
        "primary",
      ),
    { key: "update" },
  );
}
function runEval(s, fail = false, automatic = false) {
  if (!s || s.status !== "running")
    return toast("Для eval нужен запущенный стек", "error");
  if (state.evals.some((e) => e.stackId === s.id && e.status === "running"))
    return toast("Проверка уже выполняется");
  const run = {
    id: uid(),
    stackId: s.id,
    status: "running",
    revision: s.versions.length,
    version: clone(s.versions.at(-1)),
    time: now(),
    passed: 0,
  };
  state.evals.unshift(run);
  save();
  if (!automatic) go("stack/" + s.id + "/eval");
  event("Eval запущен", `Версия r${run.revision} · 12 сценариев`, automatic ? "AHP" : "Пользователь", s.id);
  toast(automatic ? "Автоматический Eval запущен" : "Eval запущен: 12 сценариев");
  after(2000, () => {
    if (!state.evals.includes(run)) return;
    run.status = fail ? "failed" : "passed";
    run.passed = fail ? 10 : 12;
    event(
      "Eval " + (fail ? "завершён с ошибками" : "пройден"),
      `${run.passed}/12 сценариев · версия r${run.revision}`,
      "Eval",
      s.id,
      fail ? "error" : "success",
    );
    save();
    render();
    toast(
      fail ? "Eval: 2 сценария не пройдены" : "Eval: все 12 сценариев пройдены",
      fail ? "error" : "",
    );
  });
}
function evalReport(run) {
  const names = [
    "Маршрутизация запроса",
    "Планирование задачи",
    "Поиск по базе знаний",
    "Вызов MCP-инструмента",
    "Передача контекста",
    "Использование памяти",
    "Обработка пустого ответа",
    "Ограничение инструментов",
    "Ответ со ссылками",
    "Обработка таймаута",
    "Проверка конфиденциальности",
    "Итоговый ответ",
  ];
  openModal(
    "Отчёт eval · r" + run.revision,
    `${notice("Демонстрационный отчёт. Все результаты сгенерированы внутри прототипа.")}<div class="panel-head"><div class="result-score">${run.passed}<small> / 12</small></div>${badge(run.status === "passed" ? "Пройден" : "Есть ошибки", run.status === "passed" ? "" : "error")}</div>${kv(
      [
        ["Запуск", run.time],
        ["Версия", "r" + run.revision],
        ["Ревизия", `<code>${esc(run.version?.commit || "—")}</code>`],
      ],
    )}${table(
      ["Сценарий", "Результат", "Время"],
      names.map((name, i) => ({
        cells: [
          name,
          badge(
            i < run.passed ? "Пройден" : "Ошибка",
            i < run.passed ? "" : "error",
          ),
          (0.4 + i * 0.15).toFixed(2) + " с",
        ],
      })),
      { filter: false, footer: false },
    )}`,
    btn("Закрыть", "close") +
      btn("Скачать отчёт", "eval-download", "primary", `data-id="${run.id}"`),
    { wide: true },
  );
}
function memoryForm() {
  openModal(
    "Создать банк памяти",
    field("Название", "name", "support-memory") +
      field("Описание", "description", "Знания и контекст системы поддержки") +
      notice(
        "Банк можно подключить к нескольким агентам через конфигурацию стека. В прототипе доступна привязка при создании Agent Stack.",
      ) +
      formError(),
    cancel() + btn("Создать банк", "memory-save", "primary"),
    { key: "memory-form" },
  );
}
function memoryDetail(m) {
  if (!m) return toast("Банк не найден", "error");
  const linked = state.stacks.filter((s) => s.memoryId === m.id);
  openModal(
    m.name,
    memoryApi(m) +
      kv([
        ["Описание", esc(m.description)],
        ["Использовано", m.used + " МБ"],
        ["Создан", m.created],
        [
          "Связанные стеки",
          linked.map((s) => esc(s.name)).join(", ") || "Не подключён",
        ],
      ]) +
      `<div class="divider"></div><h3>Использование памяти</h3><p class="muted small" style="margin-top:10px">Агенты сохраняют контекст при выполнении задач. Размер увеличивается после демонстрационного запуска.</p>` +
      formError(),
    btn("Удалить банк", "memory-delete", "danger", `data-id="${m.id}"`) +
      btn("Закрыть", "close"),
  );
}
function taskForm() {
  const running = state.stacks.filter((s) => s.status === "running");
  if (!running.length) {
    openModal(
      "Выполнить задачу",
      empty(
        "Сначала запустите Agent Stack",
        "Задачу можно отправить в запущенную систему. После запуска появится sandbox с процессами и артефактами.",
        "guide-deploy",
        "Перейти к запуску",
        "sandbox",
      ),
      btn("Закрыть", "close"),
    );
    return;
  }
  openModal(
    "Задача для агентской системы",
    select(
      "Agent Stack",
      "stackId",
      running.map((s) => [s.id, s.name]),
      currentStack()?.id || running[0].id,
    ) +
      `<label class="field"><span>Задача</span><textarea name="task">Найди ответы на частые вопросы клиентов и подготовь краткий отчёт со ссылками на источники.</textarea><small>Исполнение имитируется локально. Внешние модели не вызываются.</small></label>` +
      formError(),
    cancel() + btn("Выполнить задачу", "task-run", "primary"),
    { key: "task-form" },
  );
}
function createTask(s, task, { silent = false } = {}) {
  const b = {
    id: uid(),
    stackId: s.id,
    name: "sandbox-" + String(state.sandboxes.length + 1).padStart(3, "0"),
    created: now(),
    active: true,
    task,
    actions: [
      { time: now(), agent: "orchestrator", text: "Получена задача: " + task },
      {
        time: now(),
        agent: "orchestrator",
        text: "Агент запросил Sandbox API и запустил рабочую среду",
      },
    ],
    snapshots: [],
    files: [],
  };
  state.sandboxes.unshift(b);
  event("Задача агента запущена", b.name, "orchestrator", s.id);
  save();
  if (!silent) {
    closeModal();
    go("sandbox");
    sandboxDetail(b);
  }
  after(2100, () => {
    if (!state.sandboxes.includes(b)) return;
    b.active = false;
    b.actions.push(
      {
        time: now(),
        agent: "researcher",
        text: "Выполнен поиск через knowledge-mcp",
      },
      {
        time: now(),
        agent: "researcher",
        text: "Сохранены отчёт и список источников",
      },
      {
        time: now(),
        agent: "orchestrator",
        text: "Задача завершена. Создан snapshot изменений файлов.",
      },
    );
    b.files = [
      {
        name: "report.md",
        type: "text/markdown",
        content:
          "# Отчёт агента\n\nДемонстрационный результат работы системы " +
          s.name +
          ".\n\nЗадача: " +
          task +
          "\n\n## Результат\nПодготовлены ответы на частые вопросы.\n\n## Источники\n- https://knowledge.example.test/faq\n\nСгенерировано локальным прототипом AHP.\n",
      },
      {
        name: "sources.json",
        type: "application/json",
        content: JSON.stringify(
          {
            demo: true,
            sources: ["https://knowledge.example.test/faq"],
            stack: s.name,
          },
          null,
          2,
        ),
      },
    ];
    b.snapshots = [
      {
        id: uid(),
        name: "after-report",
        base: "sandbox-base:2026.10",
        files: 2,
        size: "24 КБ",
        created: now(),
      },
    ];
    const m = state.memories.find((m) => m.id === s.memoryId);
    if (m) m.used += 8;
    event(
      "Задача агента завершена",
      `${b.name} · 2 артефакта · 1 snapshot`,
      "orchestrator",
      s.id,
    );
    save();
    render();
    if (modal === "sandbox:" + b.id) sandboxDetail(b, "snapshots");
    toast("Агент завершил задачу. Артефакты доступны в Sandbox.");
  });
}
function sandboxDetail(b, tab = "processes") {
  if (b && !b.active && tab === "processes") tab = "snapshots";
  if (!b) return toast("Sandbox не найден", "error");
  const tabs = [
    ...(b.active ? [["processes", "Процессы"]] : []),
    ["actions", "Действия"],
    ["snapshots", "Snapshots"],
    ["files", "Артефакты"],
  ];
  let content = "";
  if (tab === "processes")
    content =
      table(
        ["Процесс", "Агент", "Состояние"],
        [
          {
            cells: [
              "python report.py",
              "researcher",
              badge(
                b.active ? "Выполняется" : "Завершён",
                b.active ? "info" : "neutral",
              ),
            ],
          },
        ],
        { filter: false, footer: false },
      ) +
      '<div class="divider"></div>' +
      kv([
        ["Задача", esc(b.task)],
        [
          "Стек",
          esc(state.stacks.find((s) => s.id === b.stackId)?.name || "—"),
        ],
        ["Создан", b.created],
      ]);
  if (tab === "actions")
    content = b.actions
      .map(
        (a) =>
          `<div class="activity"><span class="event-dot"></span><div><strong>${esc(a.agent)}</strong><p>${esc(a.text)}</p><time>${a.time}</time></div></div>`,
      )
      .join("");
  if (tab === "snapshots")
    content = b.snapshots.length
      ? notice(
          "Snapshot фиксирует изменения файлов относительно базового образа. Скачиваемые результаты доступны на вкладке «Артефакты».",
        ) +
        table(
          ["Snapshot", "Базовый образ", "Изменения", "Создан"],
          b.snapshots.map((x) => ({
            cells: [
              esc(x.name),
              `<code>${esc(x.base)}</code>`,
              `${x.files} файла · ${x.size}`,
              x.created,
            ],
          })),
          { filter: false, footer: false },
        )
      : '<p class="muted">Snapshot появится после завершения задачи.</p>';
  if (tab === "files")
    content = b.files.length
      ? b.files
          .map(
            (f, i) =>
              `<div class="file-entry">${icon("repo")}<div><strong>${esc(f.name)}</strong><br><small>${new Blob([f.content]).size} байт</small></div>${btn("Скачать", "artifact-download", "compact", `data-id="${b.id}" data-index="${i}"`)}</div>`,
          )
          .join("")
      : '<p class="muted">Артефакты появятся после завершения задачи.</p>';
  openModal(
    b.active ? b.name : "Snapshot · " + b.name,
    `<p class="muted" style="margin-bottom:15px">${b.active ? "Рабочая среда агента" : "Сохранённые результаты завершённой среды"} · ${esc(state.stacks.find((s) => s.id === b.stackId)?.name || "—")}</p><div class="tabs">${tabs.map(([id, label]) => `<button class="tab ${tab === id ? "active" : ""}" data-action="sandbox-tab" data-id="${b.id}" data-tab="${id}">${label}</button>`).join("")}</div>${content}`,
    btn(
      "Создать расписание от агента",
      "agent-cron",
      "secondary",
      `data-id="${b.id}" ${b.active ? "disabled" : ""}`,
    ) + (!b.active && b.snapshots.length ? btn("Скачать snapshot", "snapshot-download", "primary", `data-id="${b.id}"`) : "") + btn("Закрыть", "close"),
    { wide: true, key: "sandbox:" + b.id },
  );
}
function cronForm(existing, agent = false, stackId = "") {
  if (!state.stacks.length)
    return openModal(
      "Создать расписание",
      empty(
        "Сначала создайте Agent Stack",
        "Расписание выполняет действие для конкретной агентской системы.",
        "stack-new",
        "Создать стек",
        "cron",
      ),
      btn("Закрыть", "close"),
    );
  openModal(
    existing
      ? "Изменить расписание"
      : agent
        ? "Расписание от агента"
        : "Создать расписание",
    `${agent ? notice("Агент orchestrator предлагает периодически обновлять отчёт. В демосреде вы можете проверить и сохранить расписание.") : ""}${field("Название", "name", existing?.name || "daily-knowledge-report")}${select(
      "Agent Stack",
      "stackId",
      state.stacks.map((s) => [s.id, s.name]),
      existing?.stackId || stackId || currentStack()?.id || state.stacks[0].id,
    )}${field("Cron-выражение", "schedule", existing?.schedule || "0 9 * * *", "0 9 * * *", "text", "В прототипе поддержаны ежедневный запуск: M H * * * и интервалы: */N * * * *.")}${select("Часовой пояс", "timezone", ["Europe/Moscow", "UTC"], existing?.timezone || "Europe/Moscow")}<label class="field"><span>Задача</span><textarea name="task">${esc(existing?.task || "Обнови отчёт по базе знаний и сохрани результаты.")}</textarea></label><p class="muted small">Eval не запускается по расписанию: он выполняется автоматически только после deploy или обновления стека.</p><div class="selected-resource"><strong>Предпросмотр</strong><p class="small muted" data-cron-preview style="margin-top:6px">${cronPreview(existing?.schedule || "0 9 * * *", existing?.timezone || "Europe/Moscow")}</p></div><label class="checkbox-field"><input type="checkbox" name="enabled" ${existing?.enabled === false ? "" : "checked"}><span>Включить расписание</span></label>${formError()}`,
    cancel() +
      btn(
        "Сохранить расписание",
        "cron-save",
        "primary",
        `${existing ? `data-id="${existing.id}"` : ""} data-agent="${agent ? "true" : "false"}"`,
      ),
    { key: "cron-form" },
  );
}
function validCron(v) {
  const p = v.split(/\s+/);
  return (
    p.length === 5 &&
    ((/^\d+$/.test(p[0]) &&
      +p[0] < 60 &&
      /^\d+$/.test(p[1]) &&
      +p[1] < 24 &&
      p.slice(2).every((x) => x === "*")) ||
      (/^\*\/[1-9]\d?$/.test(p[0]) &&
        +p[0].slice(2) <= 59 &&
        p.slice(1).every((x) => x === "*")))
  );
}
function cronPreview(schedule, tz) {
  if (!validCron(schedule)) return "Укажите поддерживаемое cron-выражение.";
  const [m, h] = schedule.split(" ");
  if (m.startsWith("*/")) return `Каждые ${m.slice(2)} мин. · ${tz}`;
  return `Ежедневно в ${h.padStart(2, "0")}:${m.padStart(2, "0")} · ${tz}`;
}
function cronDetail(c) {
  if (!c) return toast("Расписание не найден", "error");
  const s = state.stacks.find((s) => s.id === c.stackId);
  openModal(
    c.name,
    kv([
      ["Стек", esc(s?.name || "—")],
      [
        "Действие",
        "Выполнить задачу агента",
      ],
      ["Расписание", `<code>${esc(c.schedule)}</code>`],
      ["Часовой пояс", esc(c.timezone)],
      ["План запуска", esc(cronPreview(c.schedule, c.timezone))],
      ["Создатель", esc(c.creator)],
      [
        "Состояние",
        badge(c.enabled ? "Включено" : "Отключено", c.enabled ? "" : "neutral"),
      ],
    ]) +
      '<div class="divider"></div><h3>История выполнений</h3>' +
      (c.runs.length
        ? activities(c.runs)
        : '<p class="muted small" style="margin-top:12px">Запусков пока нет. Время в демосреде не запускает задачи автоматически; используйте «Запустить сейчас».</p>') +
      formError(),
    btn("Удалить", "cron-delete", "danger", `data-id="${c.id}"`) +
      btn("Изменить", "cron-edit", "", `data-id="${c.id}"`) +
      btn(
        "Запустить сейчас",
        "cron-trigger",
        "primary",
        `data-id="${c.id}" ${!c.enabled || s?.status !== "running" ? "disabled" : ""}`,
      ),
    { key: "cron-detail" },
  );
}
function download(name, content, type = "application/json") {
  const blob = new Blob([content], { type });
  const url = URL.createObjectURL(blob),
    a = document.createElement("a");
  a.href = url;
  a.download = name;
  a.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
  toast("Подготовлен файл " + name);
}
function confirmAction(title, body, action, extra = "") {
  openModal(
    title,
    `<p>${body}</p>${formError()}`,
    cancel() + btn("Подтвердить", action, "danger", extra),
    { center: true, key: "confirm" },
  );
}
function loadDemo() {
  const msg =
    "Готовый пример недоступен: прототип работает только с реальными API платформы.";
  if (window.AHP_LIVE?.plate) AHP_LIVE.plate("demo_data_disabled", msg);
  else toast(msg, "error");
}
function docs() {
  openModal(
    "Как устроена Agent Harness Platform",
    `<h3>Ваш Git и CI/CD → готовые образы → AHP</h3><p class="muted" style="margin:12px 0 20px">AHP развёртывает систему по agent.yaml. Сборка приложений и unit-тесты выполняются в вашем CI/CD.</p>${stepsHtml()}<div class="divider"></div><h3>Три источника обновлений</h3><div class="activity"><div><strong>Git webhook</strong><p>Изменение agent.yaml обновляет конфигурацию связанных стеков.</p></div></div><div class="activity"><div><strong>CLI / Public API</strong><p>Deploy-stage регистрирует ссылку на новую версию образа конкретного компонента.</p></div></div><div class="activity"><div><strong>Изменяемый тег</strong><p>Платформа обнаруживает новый digest у latest и применяет обновление.</p></div></div><div class="divider"></div><h3>О прототипе</h3><p class="muted" style="margin-top:10px">Данные хранятся только в вашем браузере. Все подключения, развёртывания, eval и задачи симулируются. Откат, ручное подтверждение и некоторые детали экранов — предложения из flow.md. Схема YAML и CLI иллюстративные.</p>`,
    btn("Закрыть", "close"),
    { wide: true, key: "docs" },
  );
}
function stackMenu(s) {
  openModal(
    "Действия со стеком",
    kv([
      ["Стек", esc(s.name)],
      ["Статус", stackStatus(s)],
    ]) +
      '<div class="divider"></div>' +
      `<div class="grid">${btn("Изменить метки", "tags")}${btn("Скачать итоговую конфигурацию", "config-download")}${btn("Удалить Agent Stack", "stack-delete-confirm", "danger", s.status === "deploying" ? "disabled" : "")}</div>` +
      notice(
        "Удаление стека сохраняет общие банки памяти, Repository и глобальные переменные окружения. Связанные расписания и sandbox удаляются в локальной демосреде.",
        "warn",
      ),
    btn("Закрыть", "close"),
    { center: true },
  );
}
function filterTable() {
  const search = ($("[data-filter=search]")?.value || "").toLowerCase();
  const status = $("[data-filter=status]")?.value || "";
  let count = 0;
  document
    .querySelectorAll(".data-table tbody tr[data-search]")
    .forEach((row) => {
      row.hidden = !(
        row.dataset.search.toLowerCase().includes(search) &&
        (!status || row.dataset.status === status)
      );
      if (!row.hidden) count++;
    });
  const none = $(".no-results");
  if (none) none.hidden = !!count;
  const total = $("[data-count]");
  if (total) total.textContent = "Записей: " + count;
}
async function action(name, el) {
  const id = el?.dataset.id;
  const s = currentStack();
  const v = () => values();
  switch (name) {
    case "close":
      closeModal();
      break;
    case "navigate":
      go(el.dataset.path || "stacks");
      break;
    case "mobile-nav":
      $("#sidebar").classList.toggle("open");
      break;
    case "guide":
      showGuide();
      break;
    case "docs":
      docs();
      break;
    case "refresh":
      filterTable();
      toast("Список обновлён");
      break;
    case "notifications":
      openModal(
        "Уведомления",
        activities(state.events.slice(0, 8)),
        btn("Закрыть", "close"),
      );
      break;
    case "outside":
      openModal(
        esc(el.dataset.name),
        notice(
          "Этот сервис показан для контекста AI Factory. Пользовательский путь AHP доступен в Agent Harness Platform.",
        ) +
          btn("Перейти в AHP", "navigate", "primary", 'data-path="dashboard"'),
        btn("Закрыть", "close"),
        { center: true },
      );
      break;
    case "project":
      openModal(
        "Выберите проект",
        `<p class="muted small" style="margin-bottom:18px">Работа ведётся в локальном демонстрационном проекте.</p>${field("Название проекта", "project", state.project)}${formError()}`,
        cancel() + btn("Сохранить", "project-save", "primary"),
        { center: true },
      );
      break;
    case "project-save": {
      const x = v();
      if (!x.project) return error("Укажите название проекта");
      state.project = x.project;
      save();
      closeModal();
      render();
      break;
    }
    case "demo-settings":
      openModal(
        "Настройки прототипа",
        notice(
          "Браузер не хранит ни состояние, ни секреты. Разделы без реального API помечены кодом причины.",
        ) +
          `<div class="grid">${btn("Загрузить готовый пример", "load-demo", "secondary")}${btn("Начать с пустого проекта", "reset-confirm")}${btn(state.guide ? "Скрыть подсказки" : "Показать подсказки", "toggle-guide")}</div>`,
        btn("Закрыть", "close"),
        { center: true },
      );
      break;
    case "toggle-guide":
      state.guide = !state.guide;
      save();
      closeModal();
      render();
      break;
    case "reset-confirm":
      confirmAction(
        "Начать заново?",
        "Будут удалены локальные демонстрационные данные и история действий.",
        "reset",
      );
      break;
    case "reset":
      epoch++;
      state = fresh();
      draft = null;
      save();
      go("dashboard");
      toast("Открыт пустой проект");
      break;
    case "load-demo":
      if (
        state.stacks.length ||
        state.repos.length ||
        state.envs.length ||
        state.memories.length
      )
        confirmAction(
          "Загрузить готовый пример?",
          "Текущие локальные данные будут заменены демонстрационной системой support-dev.",
          "load-demo-confirm",
        );
      else loadDemo();
      break;
    case "load-demo-confirm":
      loadDemo();
      break;
    case "repo-new":
      draft = null;
      repoForm();
      break;
    case "repo-edit":
      repoForm(state.repos.find((r) => r.id === id));
      break;
    case "repo-detail":
      repoDetail(state.repos.find((r) => r.id === id));
      break;
    case "repo-save": {
      const x = v();
      if (!x.name || !x.url || !x.branch)
        return error("Укажите название, адрес репозитория и ветку.");
      try {
        const url = new URL(x.url);
        if (!["https:", "http:", "ssh:"].includes(url.protocol)) throw Error();
      } catch {
        return error("Укажите корректный URL Git-репозитория.");
      }
      if (state.repos.some((r) => r.name === x.name && r.id !== id))
        return error("Repository с таким названием уже существует.");
      if (x.url.includes("denied") || x.token === "invalid")
        return error(
          "Демонстрационная ошибка: доступ к репозиторию отклонён. Исправьте адрес или токен.",
        );
      const r = {
        id: id || uid(),
        name: x.name,
        url: x.url,
        provider: x.provider,
        access: x.access,
        branch: x.branch,
        webhook: x.webhook,
        commit: "a14f8d2",
        synced: now(),
      };
      if (id)
        Object.assign(
          state.repos.find((r) => r.id === id),
          r,
        );
      else state.repos.push(r);
      event("Repository " + (id ? "изменён" : "подключён"), r.name);
      closeModal();
      go("repositories");
      toast("Подключение проверено и сохранено");
      break;
    }
    case "repo-sync": {
      const r = state.repos.find((r) => r.id === id);
      if (!r) break;
      r.synced = now();
      event(
        "Repository синхронизирован",
        r.name + " · " + r.commit,
        "Git webhook",
      );
      save();
      closeModal();
      render();
      toast("Конфигурация актуальна. Новых изменений нет.");
      break;
    }
    case "repo-delete":
      if (state.stacks.some((s) => s.repoId === id))
        return error(
          "Repository используется стеком. Сначала удалите связанный стек.",
        );
      confirmAction(
        "Удалить Repository?",
        "Будет удалено только локальное подключение. Репозиторий в Git сохранится.",
        "repo-delete-confirm",
        `data-id="${id}"`,
      );
      break;
    case "repo-delete-confirm":
      state.repos = state.repos.filter((r) => r.id !== id);
      save();
      closeModal();
      render();
      toast("Подключение удалено");
      break;
    case "env-new":
      draft = null;
      envForm();
      break;
    case "env-edit":
      envForm(state.envs.find((r) => r.id === id));
      break;
    case "env-detail":
      envDetail(state.envs.find((r) => r.id === id));
      break;
    case "env-save": {
      const x = v();
      if (!x.key) return error("Укажите имя переменной.");
      if (!/^[A-Z_][A-Z0-9_]*$/i.test(x.key))
        return error("Имя переменной может содержать буквы, цифры и знак подчёркивания.");
      if (state.envs.some((e) => e.key === x.key && e.id !== id))
        return error("Переменная с таким именем уже существует.");
      const env = {
        id: id || uid(),
        key: x.key,
        value: x.secret ? undefined : x.value,
        secret: x.secret,
        source: x.secretManager ? "Secret Manager" : "Вручную",
        updated: now(),
      };
      if (id)
        Object.assign(
          state.envs.find((e) => e.id === id),
          env,
        );
      else state.envs.push(env);
      event("Переменная окружения " + (id ? "изменена" : "создана"), env.key);
      save();
      go("environments");
      toast("Переменная окружения сохранена");
      break;
    }
    case "env-delete":
      confirmAction(
        "Удалить переменную окружения?",
        "Она перестанет быть доступна всем Agent Stack проекта.",
        "env-delete-confirm",
        `data-id="${id}"`,
      );
      break;
    case "env-delete-confirm":
      state.envs = state.envs.filter((e) => e.id !== id);
      save();
      closeModal();
      render();
      break;
    case "memory-new":
      memoryForm();
      break;
    case "memory-save": {
      const x = v();
      if (!x.name) return error("Укажите название банка.");
      if (state.memories.some((m) => m.name === x.name))
        return error("Банк с таким названием уже существует.");
      state.memories.push({
        id: uid(),
        name: x.name,
        description: x.description,
        used: 0,
        created: now(),
      });
      event("Банк памяти создан", x.name);
      go("memory");
      toast("Банк памяти создан");
      break;
    }
    case "memory-detail":
      memoryDetail(state.memories.find((m) => m.id === id));
      break;
    case "memory-delete":
      if (state.stacks.some((s) => s.memoryId === id))
        return error(
          "Банк памяти используется стеком. Сначала удалите связанный стек.",
        );
      confirmAction(
        "Удалить банк памяти?",
        "Демонстрационные данные банка будут удалены.",
        "memory-delete-confirm",
        `data-id="${id}"`,
      );
      break;
    case "memory-delete-confirm":
      state.memories = state.memories.filter((m) => m.id !== id);
      save();
      closeModal();
      render();
      break;
    case "stack-new":
      draft = null;
      wizard(0);
      break;
    case "wizard-next": {
      const x = v();
      if (draft.step === 0) {
        if (!x.name || !x.branch || !x.sourceId)
          return error("Укажите название стека, источник и ветку.");
        if (state.stacks.some((s) => s.name === x.name))
          return error("Стек с таким названием уже существует.");
        Object.assign(draft, x);
      }
      if (draft.step === 1) {
        Object.assign(draft, x);
      }
      wizard(draft.step + 1);
      break;
    }
    case "wizard-prev":
      Object.assign(draft, v());
      wizard(draft.step - 1);
      break;
    case "stack-create":
      createStack(true);
      break;
    case "stack-draft":
      createStack(false);
      break;
    case "guide-deploy": {
      const target =
        state.stacks.find((s) => s.status !== "running") || state.stacks[0];
      if (!target) return wizard(0);
      go("stack/" + target.id);
      break;
    }
    case "guide-eval":
      if (!state.stacks.length) return wizard(0);
      go("stack/" + state.stacks[0].id + "/eval");
      break;
    case "deploy":
      if (!s) return toast("Откройте Agent Stack", "error");
      closeModal();
      deployStack(s);
      break;
    case "stop":
      if (s)
        confirmAction(
          "Остановить " + esc(s.name) + "?",
          "Компоненты перестанут работать. Банки памяти и результаты задач сохранятся. Автообновления не запустят стек без вашего действия.",
          "stop-confirm",
        );
      break;
    case "stop-confirm":
      if (s) {
        s.status = "stopped";
        s.updated = now();
        event(
          "Стек остановлен",
          "Автоматический запуск отключён до возобновления",
          "Пользователь",
          s.id,
        );
        save();
        closeModal();
        render();
        toast("Стек остановлен");
      }
      break;
    case "stack-menu":
      if (s) stackMenu(s);
      break;
    case "stack-delete-confirm":
      if (s)
        confirmAction(
          "Удалить " + esc(s.name) + "?",
          "Связанные sandbox и расписания будут удалены из демосреды. Общие Repository, переменные окружения и банки памяти сохранятся.",
          "stack-delete",
        );
      break;
    case "stack-delete":
      if (s) {
        state.stacks = state.stacks.filter((x) => x.id !== s.id);
        state.sandboxes = state.sandboxes.filter((b) => b.stackId !== s.id);
        state.crons = state.crons.filter((c) => c.stackId !== s.id);
        state.evals = state.evals.filter((e) => e.stackId !== s.id);
        event("Стек удалён", s.name);
        save();
        go("stacks");
        toast("Agent Stack удалён");
      }
      break;
    case "tags":
      if (s)
        openModal(
          "Метки стека",
          field("Метки через запятую", "tags", s.tags.join(", ")) + formError(),
          cancel() + btn("Сохранить", "tags-save", "primary"),
          { center: true },
        );
      break;
    case "tags-save":
      if (s) {
        s.tags = v()
          .tags.split(",")
          .map((t) => t.trim())
          .filter(Boolean);
        event("Метки изменены", s.tags.join(", "), "Пользователь", s.id);
        save();
        closeModal();
        render();
      }
      break;
    case "config-download":
      if (s) download(s.name + "-agent.yaml", yaml(s), "text/yaml");
      break;
    case "component": {
      if (!s) break;
      const c = s.components.find((c) => c.id === id);
      openModal(
        "Компонент · " + esc(c.name),
        kv([
          ["Тип", c.type === "mcp" ? "MCP-сервер" : "Агент"],
          ["Состояние", stackStatus(s)],
          ["Образ", `<code>${esc(c.image)}</code>`],
          ["Digest", `<code>${esc(c.digest)}</code>`],
        ]) +
          '<div class="divider"></div><h3>Журнал компонента</h3>' +
          code(
            s.status === "running"
              ? `${now()} INFO image resolved: ${c.digest}\n${now()} INFO configuration loaded\n${now()} INFO component ready\n${now()} INFO waiting for tasks`
              : `${now()} INFO component is not running\n${now()} INFO inspect stack Updates for operation details`,
            true,
          ),
        btn("Закрыть", "close") +
          btn(
            "Обновить образ",
            "update-component",
            "primary",
            `data-id="${c.id}"`,
          ),
        { wide: true },
      );
      break;
    }
    case "update-component":
      updateForm(s, "CLI / API", id);
      break;
    case "integration":
      integration(state.stacks.find((s) => s.id === id) || s);
      break;
    case "ci-simulate":
      updateForm(s || state.stacks[0], "CLI / API", v().component);
      break;
    case "update":
      updateForm(s);
      break;
    case "update-apply": {
      if (!s) break;
      const x = v();
      if (!x.image || !x.commit) return error("Укажите образ и Git-ревизию.");
      if (
        !/^[a-z0-9][a-z0-9._/-]*(?::[a-zA-Z0-9._-]+|@sha256:[a-fA-F0-9]+)$/.test(
          x.image,
        )
      )
        return error(
          "Укажите образ с тегом или digest в том виде, в котором его отдал реестр платформы. Готовый путь реестра здесь не выдумывается.",
        );
      const candidate = {
        components: clone(s.components),
        commit: x.source === "Git webhook" ? x.commit : s.commit,
        path: s.path,
      };
      const c = candidate.components.find((c) => c.id === x.component);
      if (x.source === "Изменяемый тег") {
        c.image = c.image.replace(/:[^:]+$/, ":latest");
        c.version = "latest";
      } else {
        c.image = x.image;
        c.version = x.image.split(":").at(-1);
      }
      c.digest = "sha256:" + uid().replaceAll("-", "").slice(0, 16);
      if (s.status === "stopped" || !s.auto) {
        s.pending = { candidate, source: x.source, fail: x.fail };
        event(
          "Доступна новая версия",
          x.source + " · " + c.image,
          x.source,
          s.id,
        );
        save();
        closeModal();
        render();
        toast(
          s.status === "stopped"
            ? "Новая версия зарегистрирована. Остановленный стек не запущен."
            : "Новая версия зарегистрирована. Ожидает применения.",
        );
      } else {
        closeModal();
        deployStack(s, {
          candidate,
          source: x.source,
          title:
            x.source === "Git webhook"
              ? "Обновление конфигурации"
              : "Обновление артефакта",
          fail: x.fail,
        });
      }
      break;
    }
    case "apply-pending":
      if (s?.pending) {
        const p = s.pending;
        delete s.pending;
        deployStack(s, {
          candidate: p.candidate,
          source: p.source,
          fail: p.fail,
          title: "Применение новой версии",
        });
      }
      break;
    case "rollback": {
      const ev = state.events.find((e) => e.id === el.dataset.event);
      if (!ev?.version || !s) break;
      openModal(
        "Откат версии",
        notice(
          "Откат применяет сохранённые конфигурацию и образы. Память, файлы sandbox и внешние данные не откатываются.",
          "warn",
        ) +
          kv([
            ["Стек", esc(s.name)],
            ["Целевая версия", "r" + ev.version.revision],
            ["Git-ревизия", `<code>${esc(ev.version.commit)}</code>`],
          ]) +
          components({ components: ev.version.components }, true),
        cancel() +
          btn(
            "Применить выбранную версию",
            "rollback-confirm",
            "primary",
            `data-event="${ev.id}"`,
          ),
        { wide: true },
      );
      break;
    }
    case "rollback-confirm": {
      const ev = state.events.find((e) => e.id === el.dataset.event);
      if (s && ev?.version) {
        closeModal();
        deployStack(s, {
          candidate: ev.version,
          source: "Пользователь",
          title: "Откат к r" + ev.version.revision,
        });
      }
      break;
    }
    case "eval-run":
      toast("Eval запускается автоматически после deploy или обновления Agent Stack");
      break;
    case "eval-fail":
      toast("Eval запускается автоматически после deploy или обновления Agent Stack");
      break;
    case "eval-report": {
      const run = state.evals.find((e) => e.id === id);
      if (run) evalReport(run);
      break;
    }
    case "eval-download": {
      const run = state.evals.find((e) => e.id === id);
      if (run)
        download(
          "eval-r" + run.revision + ".json",
          JSON.stringify({ demo: true, ...run }, null, 2),
        );
      break;
    }
    case "task":
      taskForm();
      break;
    case "task-run": {
      const x = v();
      const target = state.stacks.find((s) => s.id === x.stackId);
      if (!target || target.status !== "running")
        return error("Выберите запущенный стек");
      if (!x.task) return error("Опишите задачу");
      createTask(target, x.task);
      break;
    }
    case "snapshot-download": {
      const box = state.sandboxes.find(b=>b.id===id);
      if(box?.snapshots.length) download(box.name+"-snapshot.json",JSON.stringify({format:"ahp-demo-snapshot-v1",demo:true,sandbox:box.name,snapshots:box.snapshots,files:box.files},null,2));
      break;
    }
    case "snapshot-detail":
      sandboxDetail(
        state.sandboxes.find((b) => b.id === id),
        "snapshots",
      );
      break;
    case "sandbox-detail":
      sandboxDetail(state.sandboxes.find((b) => b.id === id));
      break;
    case "sandbox-tab":
      sandboxDetail(
        state.sandboxes.find((b) => b.id === id),
        el.dataset.tab,
      );
      break;
    case "artifact-download": {
      const b = state.sandboxes.find((b) => b.id === id);
      const f = b?.files[+el.dataset.index];
      if (f) download(f.name, f.content, f.type);
      break;
    }
    case "agent-cron": {
      const b = state.sandboxes.find((b) => b.id === id);
      cronForm(null, true, b?.stackId);
      break;
    }
    case "cron-new":
      cronForm();
      break;
    case "cron-edit":
      cronForm(state.crons.find((c) => c.id === id));
      break;
    case "cron-detail":
      cronDetail(state.crons.find((c) => c.id === id));
      break;
    case "cron-save": {
      const x = v();
      if (!x.name || !x.task) return error("Укажите название и текст задачи");
      if (!validCron(x.schedule))
        return error(
          "Поддерживаются M H * * * (минуты 0–59, часы 0–23) или */N * * * * (N от 1 до 59).",
        );
      if (state.crons.some((c) => c.name === x.name && c.id !== id))
        return error("Расписание с таким названием уже существует");
      const old = state.crons.find((c) => c.id === id);
      const c = {
        id: id || uid(),
        ...x,
        action: "task",
        creator:
          old?.creator ||
          (el.dataset.agent === "true"
            ? "Агент: orchestrator"
            : "Пользователь"),
        runs: old?.runs || [],
      };
      if (old) Object.assign(old, c);
      else state.crons.push(c);
      event(
        "Расписание " + (old ? "изменено" : "создано"),
        c.name,
        c.creator,
        c.stackId,
      );
      go("crons");
      toast("Расписание сохранено");
      break;
    }
    case "cron-trigger": {
      const c = state.crons.find((c) => c.id === id);
      const target = state.stacks.find((s) => s.id === c?.stackId);
      if (!c?.enabled || target?.status !== "running")
        return error("Расписание выключено или стек не запущен");
      c.runs.unshift({
        id: uid(),
        title: "Запуск по расписанию",
        detail: "Отправлена задача агента",
        source: c.creator,
        time: now(),
        status: "success",
      });
      event("Расписание запущено", c.name, c.creator, c.stackId);
      save();
      createTask(target, c.task);
      break;
    }
    case "cron-delete":
      confirmAction(
        "Удалить расписание?",
        "Новые запуски этого расписания будут прекращены.",
        "cron-delete-confirm",
        `data-id="${id}"`,
      );
      break;
    case "cron-delete-confirm":
      state.crons = state.crons.filter((c) => c.id !== id);
      save();
      closeModal();
      render();
      toast("Расписание удалено");
      break;
    case "copy-code": {
      const text = el.parentElement.querySelector("pre")?.textContent || "";
      try {
        await navigator.clipboard.writeText(text);
        toast("Скопировано");
      } catch {
        const area = document.createElement("textarea");
        area.value = text;
        document.body.append(area);
        area.select();
        const ok = document.execCommand("copy");
        area.remove();
        toast(
          ok ? "Скопировано" : "Выделите текст и скопируйте вручную",
          ok ? "" : "error",
        );
      }
      break;
    }
  }
}
document.addEventListener("click", (e) => {
  const el = e.target.closest("[data-action]");
  if (el) {
    e.preventDefault();
    action(el.dataset.action, el).catch((err) => {
      console.error(err);
      toast("Не удалось выполнить действие: " + err.message, "error");
    });
  }
  if (e.target.classList.contains("modal-backdrop") && modal !== "deployment")
    closeModal();
});
document.addEventListener("input", (e) => {
  if (e.target.dataset.filter) filterTable();
  if (
    modal === "cron-form" &&
    ["schedule", "timezone"].includes(e.target.name)
  ) {
    const x = values();
    $("[data-cron-preview]").textContent = cronPreview(x.schedule, x.timezone);
  }
});
document.addEventListener(
  "scroll",
  (e) => {
    const textarea = e.target.closest?.(".yaml-textbox");
    if (!textarea) return;
    const highlight = textarea.parentElement.querySelector(".yaml-highlight");
    highlight.scrollTop = textarea.scrollTop;
    highlight.scrollLeft = textarea.scrollLeft;
  },
  true,
);
document.addEventListener("change", (e) => {
  const t = e.target;
  if (t.dataset.filter) filterTable();
  if (t.dataset.change === "auto") {
    const s = currentStack();
    if (s) {
      s.auto = t.checked;
      event(
        "Политика обновления изменена",
        s.auto ? "Автоматическое применение" : "Ручное применение",
        "Пользователь",
        s.id,
      );
      save();
      toast("Политика обновления сохранена");
    }
  }
  if (t.dataset.change === "cron-toggle") {
    const c = state.crons.find((c) => c.id === t.dataset.id);
    if (c) {
      c.enabled = t.checked;
      event(
        "Расписание " + (c.enabled ? "включено" : "отключено"),
        c.name,
        "Пользователь",
        c.stackId,
      );
      save();
      render();
    }
  }
  if (t.dataset.change === "stats-period") {
    state.statsPeriod = t.value;
    save();
    render();
  }
  if (t.dataset.change === "update-source") {
    const image = $("input[name=image]");
    if (t.value === "Изменяемый тег")
      image.value = image.value.replace(/:[^:]+$/, ":latest");
    if (t.value === "Git webhook") $("input[name=commit]").value = "f82b4a1";
  }
  if (t.dataset.change === "wizard-source") {
    if (!draft) return;
    Object.assign(draft, values());
    draft.sourceType = t.value;
    const sources = t.value === "evolution" ? state.evolutionRepos : state.repos;
    draft.sourceId = sources[0]?.id || "";
    wizard(0);
  }
  if (modal === "integration" && t.name === "component") {
    const s = currentStack();
    if (s) integrationComponent(s, t.value);
  }
});
function integrationComponent(s, id) {
  const c = s.components.find((c) => c.id === id);
  if (!c) return;
  const box = document.querySelector(".drawer .notice");
  if (box)
    box.textContent =
      "Недоступно в текущем прототипе · artifact_registration_link: ссылку регистрации выдаёт платформа.";
}
document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") {
    if (modal) closeModal();
    $("#sidebar").classList.remove("open");
  }
  if (e.key === "Tab" && modal) {
    const focusable = [
      ...document.querySelectorAll(
        ".drawer button:not(:disabled),.drawer a,.drawer input:not(:disabled),.drawer select,.drawer textarea",
      ),
    ].filter((x) => x.offsetParent !== null);
    const first = focusable[0],
      last = focusable.at(-1);
    if (e.shiftKey && document.activeElement === first) {
      e.preventDefault();
      last?.focus();
    } else if (!e.shiftKey && document.activeElement === last) {
      e.preventDefault();
      first?.focus();
    }
  }
});
window.addEventListener("hashchange", () => {
  closeModal();
  $("#sidebar").classList.remove("open");
  render();
  window.scrollTo(0, 0);
});
// Не оставляем бесконечные операции после перезагрузки страницы.
for (const s of state.stacks) {
  if (s.status === "deploying") {
    s.status = "error";
    delete s.operation;
    state.events.unshift({
      id: uid(),
      title: "Операция прервана",
      detail: "Демостраница была перезагружена. Повторите развёртывание.",
      source: "Демосреда",
      stackId: s.id,
      status: "error",
      time: now(),
      at: Date.now(),
    });
  }
}
for (const r of state.evals) {
  if (r.status === "running") {
    r.status = "failed";
    r.passed = 0;
  }
}
for (const b of state.sandboxes) {
  if (b.active) {
    b.active = false;
    b.actions.push({
      time: now(),
      agent: "Демосреда",
      text: "Задача прервана перезагрузкой страницы. Запустите новую задачу.",
    });
  }
}
// Предложение применить зарегистрированную версию видно на любой вкладке стека.
const renderBase = render;
render = function () {
  renderBase();
  const s = currentStack();
  if (s?.pending) {
    const el = document.createElement("div");
    el.className = "next-action";
    el.innerHTML = `<div><strong>Доступна новая версия</strong><p>${esc(s.pending.source)} · ${esc(s.pending.candidate.components[0].image)}</p></div>${btn("Применить", "apply-pending", "primary", s.status === "deploying" ? "disabled" : "")}`;
    const tabs = $(".tabs", $("#main"));
    tabs?.after(el);
  }
};
save();
if (!location.hash) location.hash = "/dashboard";
render();
