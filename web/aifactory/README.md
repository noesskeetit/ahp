# AHP в AI Factory

Актуальная сводка подтверждений и ограничений: [приёмка 2 ноября](evidence/ACCEPTANCE-2026-11-02.md). Записи отдельных ранних прогонов ниже являются историей проверки.

Интерфейс выбранного пользователем прототипа `/Users/aogabbasov/Downloads/example`, подключённый к AHP. Для работы нужен единый human origin: статические файлы, `/u-ahp/v1/*` и `/v1/space/*`. Открытие одного `index.html` не создаёт backend.

## Поддержанные пути

- Memory: предложение, одобрение и поиск пользовательской памяти.
- Crons: создание ограниченного расписания, просмотр и отключение.
- Чаты: запуск, серверная история, продолжение в том же thread, отмена, стоимость и скачивание точной версии артефакта после проверки SHA-256.
- Agent Stacks: регистрация, расчёт canonical digest манифеста с неизменяемым image digest, операторские Evals, публикация, установка, обновление и откат. Запуск выбранной установки использует Registry Invoke; состояние читается через GetInvokedTask до подтверждения runtime.
- Workspace: создание, участники, опубликованные ревизии, список файлов и скачивание с проверкой digest.
- Согласования: только выданные текущему пользователю pending операции; подтверждение включает точный request_id.
- Sandbox: настоящий Compute API жизненного цикла. Если ответ incarnation содержит backend=mock, интерфейс явно показывает симуляцию MicroVM.

Нет локальных демонстрационных данных или сохранения bearer/пароля в browser storage. Сессия восстанавливается через серверную HttpOnly cookie. Отсутствующие Git/environment API показаны как недоступные. Успешная запись в Registry или принятие задачи не выдаётся за завершённое исполнение.

## Файлы и проверки

`app.js` сохраняет оболочку и навигацию исходного дизайна. `ahp-live.js` подключает продуктовые разделы; `ahp-shapes.js` проверяет формы ответов; `ahp-canonical.js` повторяет canonical manifest контракт Go. Локальные ресурсы оформления находятся в `assets/`; дополнительные Tabler Icons доступны по MIT (`assets/TABLER-LICENSE.txt`).

```sh
node scripts/aifactory-browser.test.mjs
node --test web/aifactory_canonical.test.mjs
go test ./web
```

Контрактные Chromium тесты используют HTTP fixtures и не доказывают live исполнение. Они проверяют непустые/пустые/error состояния, exact ArtifactRef и отказ при digest mismatch, выбранную installation, same-thread history, Workspace/approvals/Compute контракты и окончание сессии.

Для отдельного writable стенда используйте `scripts/aifactory-mutation-browser.mjs`. Нужны `AHP_UI_LIVE_ORIGIN` и `AHP_UI_LIVE_LOGIN_JSON` — путь к игнорируемому JSON с email/username и password. Дополнительный `AHP_UI_LIVE_SEED_JSON` задаёт installation_id, suite_id, agent_digest, опубликованный workspace и approval_request_id. Сценарий реально изменяет выбранный стенд; разрешайте только выделенный acceptance origin. `AHP_UI_LIVE_STEPS` выбирает шаги через запятую; `AHP_UI_LIVE_REPORT` сохраняет статусы и IDs без credentials или ответов модели. Отсутствующие seeds явно помечаются not_run.

## Root frontend repair, 2026-11-02

Shape, canonical digest and exact ArtifactRef readers were ported from the frozen
frontend/aifactory-example commit `83e6fd3eca6851af6519ef06854a78e925cfe0ab`
(including its earlier canonicalizer). The source was reviewed and corrected:
`readList` had a shadowed-loop-variable TDZ, and Resolve now matches both artifact
ID and version. No private live-response fixture was imported.

`node scripts/aifactory-browser.test.mjs` runs isolated Chromium contract fixtures,
not live-provider E2E. It requires `AHP_PLAYWRIGHT_MODULE` pointing to a Playwright
installation (the local default is /tmp/h10chain/pw/node_modules/playwright/index.mjs).
The initial four regressions failed on be0cafb4 and passed after repair.
`node --test web/aifactory_canonical.test.mjs` and `go test ./web` verify the browser
manifest digest against the Go platform vectors.


### Verification scope (2026-11-02)

The original Memory, Crons and Agent Stacks sections render platform responses.
Chats restore server history, select an exact ArtifactRef/version and verify bytes
before delivery. Workspace, approvals and the marked Compute lifecycle use human
surfaces. The approvals surface preserves the verified human token; Compute and
Workspace use the existing scoped task exchange.

Read-only Chromium against local preview :29812 (candidate files) proxying the
existing human origin :29712 observed Memory=1, Crons=8, Chats=8, Catalog=0.
It opened a completed history with 7 frames, downloaded 164 bytes with matching
SHA-256, restored the same artifact ID/version after reload, checked mobile
scrollWidth=390 at viewport=390, zero browser storage entries, no page errors or
refusal plates, and logout cleared credentials/data. No new LLM task or domain
record was created by this leg. Screenshots were kept outside the repository.

Re-run with `AHP_UI_LIVE_ORIGIN`, `AHP_UI_LIVE_USERS_JSON` (an ignored credentials
file), `AHP_UI_LIVE_SUBJECT`, and `node scripts/aifactory-readonly-browser.mjs`.
The test requires nonempty existing Memory/Crons and an artifact in the newest
chat. Do not treat its result as evidence for new-task execution or deployment.

Browser contract tests: 16 passed. Canonical digest vectors: 9 passed. Compute,
Workspace and approval contracts are covered by isolated fixtures; real mutation
results are recorded below. Git and
environment APIs remain explicitly unavailable.

### Isolated write journey (2026-11-02, human :29822)

Real browser actions confirmed Memory Propose → Review active → reload, bounded
schedule Create → Revoke, Workspace Create → reload/open, and CancelTurn with
terminal_frame_durable=true → canceled → reload. Browser storage remained empty;
no page errors occurred. Two messages had distinct turn IDs in one durable thread,
reached done and downloaded artifacts with verified SHA-256. The second artifact
failed the previous-message fact check, so semantic continuation was NOT accepted;
the runtime owner reproduced missing thread history. Compute Wake returned an
explicit 403 because the execution runner was not mounted. Cost returned 404.
These failures remain failures until the repaired stand is checked again.

A real approval request was created by task-facing Tools.Call (409), approved in
the browser using its exact request_id, and retried with the identical operation
key. Tools.Call and Get both reported SUCCEEDED. This confirms the human approval
and durable operation path; destination side-effect evidence is a separate check.
Private reports remain outside the repository, at /tmp/ahp-root-ui-mutation.json,
/tmp/ahp-root-ui-approval.json and /tmp/ahp-root-ui-compute.json.

### Repaired stand checks

Compute create → Wake → Sleep → Stop passed through the browser after the runner
mount repair, with incarnation.backend=mock and the visible MOCK disclosure.
This is lifecycle proof for the explicitly permitted mock; no VM execution or
isolation claim is made.

`scripts/aifactory-workspace-acceptance.py` executed real task API upload → seal →
candidate update. Publish before checks returned 409. The checker downloaded the
sealed bytes, verified SHA-256 and the candidate canonical digest, then recorded
SetCheck. Explicit owner Artifacts.Publish granted the workspace access; only then
Workspace.Publish succeeded. ReadFile returned the same 88 bytes. Use
AHP_UI_STAND_DIR and AHP_UI_WORKSPACE_REPORT; the task token must come from a real
human control exchange, never a signing bypass. This is an API acceptance path;
the browser currently provides project creation and published-file reading.

`scripts/aifactory-isolation-browser.mjs` used two real sessions in one tenant.
The owner saw the existing Memory record and downloaded the published Workspace
file with a matching digest. The outsider did not see that Memory record, and
GetWorkspace plus ReadFile of the known existing revision both returned 403.
Inputs: AHP_UI_LIVE_ORIGIN, AHP_UI_LIVE_LOGIN_JSON, AHP_UI_OUTSIDER_LOGIN_JSON,
AHP_UI_WORKSPACE_REPORT, AHP_UI_MEMORY_ID, optional AHP_UI_ISOLATION_REPORT.
Reports: /tmp/ahp-root-ui-compute-repaired.json,
/tmp/ahp-root-ui-workspace-api-fresh.json, /tmp/ahp-root-ui-isolation.json.
