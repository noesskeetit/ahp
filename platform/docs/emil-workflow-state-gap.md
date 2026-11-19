# Gap-analysis: Emil `workflow.py` Core Agent → State API / RunCoordinator / AHP gateways

Status: **draft — `ready_for_director_review`.** Задача 15 (`docs/implementation.md`: совместимость
workflow.py со State API) остаётся **ОТКРЫТОЙ** до принятия этого анализа директором.

Владелец анализа: **space-product**. Дата: 2026-10-30.

## 0. Провенанс и границы

- Исходник Emil (только для чтения, материал для анализа/доработки): `go-platform/.local/emil-core-src`
  @ commit `5ed238b4ac6d71b583304f0104edb37345ec432e`. Имя автора, тег/порт контейнера (`:18000`) и
  неизменность upstream **не** являются продуктовыми требованиями (прямое уточнение пользователя 30.10.2026,
  `decisions/2026-10-30-emil-image-scope-correction.md`).
- Единственный источник требований: PDF `ahp-agents-space-architecture (2) (1).pdf`, rev 3.2, 64 стр.,
  sha256 `044e045b73b7c34b642bd3c9eb5701b93eed470873a1e044e8b1d024821a7e2` (проверено `shasum` на этом
  сеансе). Ссылки внутри PDF требования не расширяют.
- Слово «Emil» в PDF отсутствует. PDF задаёт роль «Core Agent» с движком `workflow.py`, обязанным хранить
  состояние по единому контракту **State API** (PDF л.167–168: «Core Agent — своим workflow.py… хранит в
  Agent State Store по контракту State API»; л.249–250: «шаги — workflow.py… stateless-пул… в процессе
  агента»; л.488: «пишет… workflow.py»). Риск «объём перевода на State API» (PDF §риски, с.61 по указанию
  директора) — цель этого документа.
- Метод: структурный разбор (`grep` классов/методов) + прицельное чтение тел (create/transition/lease/
  outbox/_history_before; kernel.py/tool.py/artifact_service.py). Линейные оценки S/M/L — оценка, а не
  измерение; помечены как оценка.

## 1. Ключевой вывод

У Emil **свой** PostgreSQL-стор ран-состояния — `PostgresWorkflowStore` + `InMemoryWorkflowStore` +
`OutboxDispatcher` (`workflow.py`, схемы `core_runs`, `core_outbox`, события, аренды, FIFO-промоушен,
HITL-апрувы, бюджет, recovery). Это функционально **дубль** AHP **State API** (35 операций) +
**RunCoordinator** (11 методов). PDF требует, чтобы Core Agent ходил в **единый State API**, а не в свой
стор. Значит «перевод workflow.py на State API» = **вырезать собственный PG-стор Emil и заменить его
клиентом State API/RunCoordinator**, плюс перевести модель/тулы/артефакты/креды на AHP-шлюзы. Ядро-движок
(`runtime.CoreAgent`, цикл model↔tool и сборка шагов) остаётся **в процессе** (PDF: шаги в процессе агента),
но становится **stateless** (PDF: «stateless-пул Core Agent», л.250/430/1971) — вся персистенция уходит по
Разъёму 3 (State API) и Разъёму 2 (API-клиенты с токеном задачи), вход по A2A (Разъём 1, PDF л.408).

## 2. Таблица A — методы `workflow.py` → точка контракта AHP → требуемый патч

Левая колонка — `class PostgresWorkflowStore` (InMemory-дубль зеркален, правится так же).

| # | метод `workflow.py` (строка) | что делает Emil сейчас | точка контракта AHP | требуемый патч | объём |
|---|---|---|---|---|---|
| A1 | `create` / `_admit_root` (1075 / 996) | INSERT `core_runs`, root-admission, первый event, context_sequence | `Coordinator.AcceptTurn` → `State CreateThread`+`CreateRun` | удалить PG-INSERT; открыть thread/run через State API; context_sequence из thread | **L** |
| A2 | `transition` / `_transition_locked` (2097 / 1723) | state-machine ход + `core_events` + outbox row | `Coordinator` (DispatchTurn/RespondHITL) + `State PutBatch`/`AppendStream`/`Release`/`Suspend` | состояние ведёт State/Coordinator; Emil лишь посылает переходы | **L** |
| A3 | `acquire_lease`/`renew_lease`/`release_lease` (2147/2177/2202) | `FOR UPDATE` lease_owner/token/expires; `LEASE_LOST` | `State ClaimRun`/`Heartbeat`/`Release` с fence `{run_id,worker,epoch}` | заменить lease на lease/epoch State; `LEASE_LOST`→`STALE_EPOCH` | **M** |
| A4 | `append_inbound`/`pending_inbound`/`consume_inbound` (1381/1482/1494) | steering/inbox очередь в PG | `State Enqueue`/`ReadInbox`/`AckInbox`; `Coordinator.Steer` | переписать на inbox State (объект-конверт `{events:[…]}`) | **S** |
| A5 | `_promote_queued`/`_promote_next` (1577/1657) | FIFO-порядок хода | `Coordinator.DispatchTurn` (FIFO) + State inbox FIFO | удалить локальный промоушен; FIFO делает Coordinator | **M** |
| A6 | `_outbox` + `OutboxDispatcher._claim/drain_once` (964 / 2251/2269) | таблица `core_outbox`, поллинг-диспетчер | `Coordinator.EmitOutbox` + `State Enqueue` | убрать свой outbox/диcпetчер; outbox через Coordinator | **M** |
| A7 | `enter_approval`/`reserve_approval` (1966/2038) | HITL-апрув в PG | `Coordinator.SubmitHITL`/`RespondHITL` | HITL через Coordinator | **M** |
| A8 | `consume_budget`/`release_budget` (1247/1293) | лимит model_turns/tool_calls | `internal/core` bounded-steps (уже наша сторона) | перенять наш step-limit; удалить бюджет Emil | **S** |
| A9 | `recoverable`/`recover_context_sessions` (2214/1676) | скан зависших, reseed контекста | `Coordinator.Recover`/`RehydrateTurn` | recovery через Coordinator; контекст-резид из State | **M** |
| A10 | `_history_before` (984) | история терм. ходов по context_sequence (reseed) | `State ListRuns`/`ReadThread` + `Coordinator` reseed | читать историю из State | **M** |
| A11 | `get`/`by_task`/`lookup_task`/`is_cancelled`/`request_cancel` (1319/1369/1565/1338/1350) | загрузка/поиск/отмена рана | `State GetRun`/`GetThreadByContext`; `Coordinator.CancelTurn` | заменить на State/Coordinator вызовы | **S** |
| A12 | `_event`/`_audit` (919/941) | журнал событий/меток | `State AppendStream` + журнал меток AHP (PDF л.476) | события через AppendStream, метки из scope | **M** |

Итог таблицы A: `workflow.py` — это **не «адаптер», а параллельная реализация State API/Coordinator**.
Основной объём (3×L + ~6×M) — удаление собственного PG-стора и перенос семантики (FIFO, lease/epoch,
outbox, HITL, recovery) на AHP.

## 3. Таблица B — смежные файлы Emil → шлюз/сервис AHP

| файл Emil | роль сейчас | AHP-назначение по PDF | требуемый патч | объём |
|---|---|---|---|---|
| `runtime.py` `CoreAgent` (5872 стр., RunResult/`_DurableCancelEvent`/`_TaskControlEvent`/`attach_stream`) | движок model↔tool, шаги, стрим, cancel | Core Agent ядро в `internal/core` + `as-runner` (stateless-пул, л.250/430/1971) | вынести персистенцию в State; цикл остаётся в процессе; cancel/steering — fence State | **L** |
| `model.py` `CompatibleHttpModel`/`ModelRouter`/`ModelCapabilities`/`FallbackDecision` | свой HTTP-модел-клиент + роутинг/fallback | **Model Gateway** (PDF л.305, л.1971) | ход через Model GW (AHP_LLM_* роль → Qwen); fallback-политику AHP не дублировать | **M** |
| `mcp.py` `McpManager`/`McpConnector` + `credentials.py` | свои MCP-пулы и внешние креды | **Tool & Credential Gateway** (PDF л.194: «токены внешних систем живут только в Tool & Credential GW») | убрать локальные креды; MCP по владельцу полномочий через Tool& Cred GW | **M** |
| `artifact_service.py` `ArtifactService`/`S3ArtifactBackend`(свой SigV4)/`InMemoryArtifactBackend` | свой S3/MinIO, save/load/versions | **Artifacts**: presigned PUT→seal(size/digest/проверки) (PDF л.320–322) | писать через AHP `internal/artifacts` BeginUpload→PUT→**Seal** с метками задачи (= нашенский `save_artifact`); seal — **в AHP Artifacts**, не в приватном хранилище Emil | **M** |
| `tools.py` `ToolRegistry`/`ToolRuntime` (schema-validate, private-reasoning redaction, timeout, `request_input`) | свой тул-рантайм | `internal/core.ToolRegistry` + Tool GW | схемы/таймауты совместимы; HITL `request_input`→Coordinator.SubmitHITL; redaction сохранить | **S** |
| `kernel.py` `KernelCompiler` (instruction segments + protected digest) | сборка системного промпта | `internal/core` system/skills/context assembly | перенять как источник формата контекста; protected-digest идею сохранить | **S** |
| `a2a.py`/`a2a_sdk.py` (Task/Message/Artifact/Part/AgentCard/TaskState) | вход по A2A | `adapters/emil` (A2A-клиент) + AHP A2A-вход (Разъём 1) | уже есть `adapters/emil`; согласовать 0.3 wire и provenance | **S** |

## 4. Сведение: требуемые патчи (по контурам)

1. **State persistence (L):** удалить `PostgresWorkflowStore`/`InMemoryWorkflowStore`/`OutboxDispatcher`;
   внедрить State-API клиент поверх `internal/stateapi` контракта (объект-конверты `{events:[]}`,
   `{checkpoints:[]}`; fence `{run_id,worker,epoch}` в теле; scope из task-token; strict decode).
2. **RunCoordinator semantics (L→M):** FIFO, lease/epoch, outbox, HITL, recovery, budget — на
   `internal/space.Coordinator`.
3. **Stateless engine (L):** `runtime.CoreAgent` → цикл в процессе, но вся запись через State; вход A2A.
4. **Model GW (M):** модель через Model Gateway (Qwen роль), без своего fallback.
5. **Tool & Credential GW (M):** MCP/креды только в Tool& Cred GW (PDF л.194).
6. **Artifacts (M):** артефакты через AHP `internal/artifacts` с Seal по меткам задачи (seal — в AHP,
   не в `artifact_service.S3ArtifactBackend`).
7. **A2A adapter (S):** согласовать `adapters/emil` (wire 0.3, provenance).

Суммарная оценка объёма (оценка): **L** целиком (≈3 L-контура + 4–5 M + 3 S). Доминанта — п.1–3: замена
собственного PG-стора на State API и обезличивание движка до stateless-пула.

## 5. Два режима CoreAgent / RunCoordinator (PDF л.104/107/465/468)

- **Режим «Run Coordinator ведёт ход»:** исполнитель a2a без раннера и без шагов (л.104) — тул-hop идёт
  через Tool GW, персистенция в State/Coordinator. Для Emil-агента как внешнего A2A-агента — это то, что
  уже делает `adapters/emil`.
- **Режим «Core Agent ведёт шаги»:** stateless-пул `as-runner` на ядре, шаги в `workflow.py`, чекпоинты в
  State API (л.249/480/488). Для переносимого нашего Core Agent — это `internal/core` + `cmd/as-runner`
  (уже stateless), чекпоинты через `SaveCheckpoint`/State. Emil-движок здесь — референс формата шагов, а
  не обязательная зависимость.

## 6. Что СОХРАНЯЕТСЯ и НЕ меняется

- Реальные model/tool E2E, подлинность артефакта (MinIO PUT==GET sha), durable PG, recovery, изоляция,
  fail-closed — без смягчения (директива 04:2xZ).
- `adapters/emil` остаётся нашим адаптером; исходник/образ Emil не удаляются.
- Наше ядро `internal/core`, `cmd/as-runner`, State API/Coordinator — не переписываются под Emil;
  патчи применяются, когда Emil-движок решим встраивать как референс-реализацию Core Agent.

## 7. Влияние на приёмку

Этот анализ закрывает подготовительную часть задачи 15 (gap + план патчей + объём) и переводит Emil из
«внешнего гейта» в «материал доработки». Приёмка блока остаётся по работающему адаптированному runtime
(PDF) и реальным model/tool/artifact/durable/recovery, а не по имени/порту Emil. Готово к ревью директором.
