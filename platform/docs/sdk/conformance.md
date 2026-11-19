# SDK: проверенный контракт и границы

Этот документ описывает блок runtime/startup и его проверку, а не готовность всей
платформы. Источник требований — архитектура AHP rev 3.2, §4.2/§6.8. Agent Space
не нужен для запуска приведённых примеров. Новые адаптеры ADK/CrewAI здесь не реализованы.

## Установка и запуск

Базовый Python SDK не требует LangGraph:

```sh
python -m pip install ./sdk/python
# Только для графов и стандартного контейнерного runtime:
python -m pip install './sdk/python[langgraph]'
```

Импорт — `ahp_state`; имя дистрибутива пока историческое `ahp-langgraph`.
Версии адаптера: LangGraph 0.6.11, langgraph-checkpoint 2.1.2. Core AHP написан на Go.
`mtls_client` доступен без LangGraph и использует CA/сертификат/ключ из workload
hand-out. Ни один пример не получает административный токен автоматически.

Конкретные standalone примеры:

- `sdk/examples/standalone-go/main.go`: workload mTLS → State PutKV/GetKV;
- `sdk/examples/standalone-python/state_client.py`: framework-neutral State API;
- `sdk/examples/standalone-python/graph.py`: builder обычного LangGraph с `ctx.saver`
  и `ctx.store`, для `python -m ahp_state.container --module graph`.

Для первых двух платформа выдаёт приватный JSON-файл уже claimed run:
`base_url`, `ca_file`, `cert_file`, `key_file`, `task_token`, `fence`
(`run_id`, `worker`, `epoch`). Файл не кладут в Git. Выполнение:

```sh
go run ./sdk/examples/standalone-go /private/handout.json write
python sdk/examples/standalone-python/state_client.py /private/handout.json
go run ./sdk/examples/standalone-go /private/handout.json verify
```

Повтор write использует исходные operation_key, expected_version и payload.
Lease должна оставаться активной. Это короткий workload smoke, не бесконечный
агент: за жизненный цикл запуска отвечает вызывающий run coordinator/runtime.

## Что исправлено и чем проверено

| Контракт | Реальная проверка |
|---|---|
| Native ingress и общий SDK | Два одновременных подписанных пользователя на одном экземпляре SDK/State client, настоящие A2A/mTLS/State/PostgreSQL: отдельные run/checkpoint/history и отказ чужому чтению. `NativeTaskTokenSource` берёт credential текущего запроса; без проверенного ingress нет fallback. Identity authorization в этом стендовом тесте — явный fixture. |
| Ошибочный фиксированный State token | Подписанный запрос другого subject при одинаковых task/context раньше запускал graph и создавал run в чужой области. Теперь scope ответа CreateThread сверяется с ingress principal до CreateRun/claim/graph; пустой thread при такой ошибочной конфигурации всё ещё может создаться. |
| Повторные синхронные saver вызовы | Настоящий LangGraph `invoke`, get/list/put_writes через PG + HTTP + workload mTLS. До исправления повторное использование async pool давало `Event loop is closed`. |
| Совместные sync/async Store вызовы | 4 sync worker + 4 async task на одном Store, реальные Put/Get/Search, исходный клиент не заменяется. До исправления обнаружен ReadError из-за временной замены общего `_client`. |
| Serializer и metadata | Байты `00 ff` в channel value и pending write, вложенная metadata, parent checkpoint; sync и async поверхности. |
| История checkpoint | `before` возвращает старые, а не новые checkpoint; metadata filter, нулевой limit. До исправления live sync/async тесты воспроизводили обратную фильтрацию. |
| HITL и восстановление | Два отдельных Python-процесса, interrupt/resume, предыдущий завершённый узел не исполняется повторно. |
| Pending writes | Один параллельный узел падает; после перезапуска его успешный сосед не исполняется повторно. |
| Durability до следующего узла | Ошибка записи checkpoint после первого узла останавливает выполнение до второго. |
| Python startup | Проверка исходника до import, включая `__init__.py` родительского пакета; запрещённые DB/network/durable-file/foreign saver конструкции дают отказ до исполнения. |
| Runtime привязка | Граф должен использовать именно `ctx.saver` текущей lease; если store задан, именно `ctx.store`. Другой экземпляр AHP saver также отклоняется. |
| Go checker | Ошибки обхода/разбора и пустой набор Go-файлов не дают PASS; известные direct DB/exec/network/disk вызовы дают ERROR, в том числе import aliases. |
| Общий API без фреймворка | Go write → Python read/write → новые процессы retry → Go verify, реальные owner API. |
| Установка | Отдельная чистая venv и wheel: `langgraph` отсутствует, State/Recovery/mTLS/checker импортируются; test harness не попадает в wheel. |

Команда реального сценария, после настройки изолированных DSN:

```sh
AHP_SDK_PYTHON=/absolute/venv/bin/python go test -tags sdk_conformance \
  ./internal/core -run '^TestSDKRuntimeRealOwners$' -count=1 -v
```

Тест обязательно требует `AHP_TEST_DATABASE_URL` и `AHP_SDK_PYTHON`: отсутствие
инфраструктуры — ошибка, а не SKIP. Он не вызывает LLM, не проверяет Qwen и не
подменяет реальные microVM. Test-only сервер использует настоящие Identity и
State owner API, реальную БД и проверяемый клиентский TLS.

## Transport и startup: точные ограничения

Sync adapter создаёт отдельный transport через явную `http_client_factory` на
каждую операцию и закрывает его при успехе, исключении и CancelledError. Фабрика
сохраняет mTLS/CA, timeout, limits и proxy policy; private поля httpx не читаются.
`StateAPIClient.with_token` сохраняет эту фабрику и привязывает текущий токен;
ContextVars остаются у вызывающего узла. Цена — новый pool/TLS handshake для
sync операции; async путь использует постоянный pool.

Для custom injected `httpx.AsyncClient` без factory async API работает, sync
API возвращает понятную ошибку. Sync API непосредственно внутри работающего
asyncio loop запрещён: используйте async API или sync graph в worker thread.
Отмена ожидающего `asyncio.to_thread` не прерывает физически Python thread;
сквозная принудительная отмена произвольного sync кода не заявляется.

Startup поддерживает обычные `.py` модули и packages. Источники обнаруживаются
без исполнения родителей через PathFinder; проверяется весь верхний package,
либо один файл для одиночного модуля. Extensions/zip/custom loaders без
инспектируемого исходника отклоняются. Локальные helper-файлы вне выбранного
package должны отдельно входить в CI source root. Образ должен быть immutable
между check и import. Наличие более одной реплики не ослабляет правила:
платформенный saver обязателен уже для одной реплики. Граф без Store допустим,
если ему не нужна KV-память; заданный foreign store запрещён.

AST checker — ограниченный lint, не песочница и не доказательство отсутствия
всех эффектов. Динамический импорт, reflection, native extensions, зависимости
и произвольный alias flow не охватываются полностью. Изоляция сети/FS остаётся
ответственностью platform policies. Go source checker не заменяет `go build`.

`alist` пока читает до 1000 последних checkpoint одного namespace: у State
ListCheckpoints нет cursor. Выбор точного ID использует GetCheckpoint напрямую.
Полная пагинация длинной истории требует отдельного расширения owner API.
Effects loop/task_id проверяется отдельно `TestSDKEffectsRealOwners`: одинаковый
`call_0` в новых итерациях допустим, восстановление той же task сохраняет прежний
intent. Два Python-процесса и отдельный synthetic tool counter проверяют повтор
после interrupt, positional fallback и UNKNOWN без второй физической отправки.
Это не LLM/Tool Gateway E2E. Логический node name служит только описанием;
внутри LangGraph owner получает `node=__pregel_task_id` из закреплённого runtime.
Явный чужой task_id и не привязанный к node-вызову positional counter отклоняются.

При обновлении существующего выполняющегося графа нельзя автоматически считать
старый logical-node intent новым task-id intent: миграцию незавершённых запусков
надо согласовать отдельно. Не переключайте SDK image посреди незавершённого run.
Дополнительная защита читает legacy intent через State owner API до новой
отправки: completed/unknown/dispatched row под прежним node/call вызывает
`INTENT_IDENTITY_MIGRATION_REQUIRED`. Старый intent не копируется, не меняется,
новый не создаётся. Legacy negative scenarios проверены реальным счётчиком.
Вне LangGraph можно передать `Effects(..., task_id=stable_id)` явно; старый
низкоуровневый вызов без task_id сохраняет node scope и не обещает graph semantics.

## Inventory публичных контрактов двух SDK

Срез этой ветки: runtime candidate поверх сборки `6661bfd3`. Это не обещание
паритета всех сервисов. Параллельные клиентские изменения принимаются отдельно.

| Контракт | Go | Python | Evidence / незакрытая часть |
|---|---|---|---|
| State: thread/run/lease/KV/checkpoint/intent/inbox/stream | `sdk/go/state`, orchestration `pkg/sdk` | `ahp_state.StateAPIClient`, IntentRecorder | Реальные owner tests и standalone пример. Python общий API async; sync — LangGraph adapter. |
| Recovery/export/import | `sdk/go/state` + `sdk/go/artifacts` | StateAdminClient + TransferClient | Отдельный transfer блок: строгий TransferGrant, source/target authority, labels, bytes/hash, UNKNOWN recovery. См. checkpoint-recovery.md. |
| LangGraph saver/store/container | Go имеет framework-neutral saver/store и executor, не Python LangGraph | AHPCheckpointSaver, AHPStore, AgentService | Реальные runtime и Effects матрицы; история пока ограничена 1000. |
| Artifacts | Public `sdk/go/artifacts` upload/seal/read/transfer | Payload spill/hydration внутри State SDK + public TransferClient | Общий Python Artifacts CRUD client не выделен; это gap публичного паритета. |
| Memory | Новый `sdk/go/memory` у Memory worker | Новый `ahp_memory` у Memory worker | Независимый блок, пока не включён в данный base; статус не выводится из этих тестов. |
| Compute/workspace | `sdk/go/compute` существует | Выделенного общего клиента в base нет | Нужна отдельная контрактная матрица, в этом блоке runtime sandbox не проверен. |
| Model Gateway | Выделенного public client в base нет | Нет | Generic Effects callback не заменяет клиента сервиса. Следующий SDK block. |
| Tool/MCP Gateway | Выделенного public client в base нет | Нет | Следующий SDK block; никаких direct provider credentials в SDK. |
| AHP A2A caller | `pkg/sdk` содержит executor/runtime; `sdk/go/agent` ходит в Agent Space | AgentService — runtime, не общий A2A caller | Нужны public AHP A2A clients отдельно от AS. |
| Registry/Identity human administration, IAM/KMS | Не покрыты этим SDK блоком | Не покрыты этим SDK блоком | Не подменять workload clients административным доступом; отдельный control-plane scope. |

Acceptance этого блока не означает готовность всех SDK, произвольных harnesses,
всей платформы, реального IAM/KMS провайдера или Agent Space.
