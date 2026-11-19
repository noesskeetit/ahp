# Паритет публичных клиентов Go и Python

Таблица построена измерением поверхности кода на базе `7a3a7a22` (методы клиента
перечислены из `go doc -all ./sdk/go/<pkg>` и из `def`/`async def` соответствующих
модулей Python), а не по памяти. Имя столбца «Общих» — методы, чьи имена совпадают
после нормализации регистра (Go `CamelCase` → `snake_case`), то есть реально
эквивалентные операции, а не просто равные счётчики.

| Семейство | Go | Python | Общих | Только Go | Только Python |
|---|---|---|---|---|---|
| State | 42 | 43 | 39 | `ClaimRunFor`, `ImportThreadTo`, `RehomeExport`, `ReleaseWithReason`, `WithTokenSource` | `base_url`, `call`, `close`, `fork_for_sync`, `http_client`, `with_token` (служебные доступы) |
| Artifacts | 17 | 14 | 14 | `Error`/`Unwrap` (обвязка ошибок), `WithTokenSource` | — |
| Memory | 14 | 14 | 12 | `Management` (доступ к management-клиенту) | `close`/`aclose` (жизненный цикл) |
| Model | 4 | 5 | 4 | — | `accept` (приём агентом; у Go это отдельный `tools.ApprovalClient`) |
| Tools | 7 | 7 | 7 | — | — |
| Compute | 46 | 44 | 44 | `BeginInstallDetails`, `CompleteWithOutput` | — |
| A2A | 7 | 6 | 6 | `Error` (обвязка ошибок) | — |
| Agent/Space | 13 | 11 | 0 | фасад Space (`AcceptTurn`, `Dispatch`, `OutboxEmit`, …) | контейнерный runtime (`serve`, `run_leased`, `steering`, …) |

## Что эти числа означают и чего они не означают

- `Agent/Space: 0 общих` — это не «пробел паритета», а разные роли: Go-пакет — клиент
  плоскости Space, Python-модуль — runtime контейнера агента. Сводить их в одну
  строку нельзя; обе стороны проверяются своими тестами.
- Счётчики «только Python» в State — это доступы (`http_client`, `base_url`,
  `call`), а не операции контракта. Они не делают Python шире Go.

## Пробелы, закрытые в этом блоке

1. **Artifacts Go: классификация неопределённого результата мутации** (унаследованная
   граница, зафиксированная в `docs/implementation-pitfalls.md` коммитом `7a3a7a22`).
   Реализовано: `sdk/go/artifacts/outcome.go` — `UnknownOutcomeError`,
   `IsOutcomeUnknown`; прерванная мутация (`BeginUpload`, `Seal`, `Process`,
   `TransferGrant`, `CopyFromTransferGrant`) больше не выглядит как
   `INVALID_ARGUMENT`: код `UNKNOWN`, причина `outcome_unknown`, исходный код/причина
   владельца сохраняются через `Unwrap` (в частности `transfer_outcome_unknown`).
   Чтение остаётся обычным ответом — у него нет побочного эффекта, который нужно
   примирять. Измерено: `sdk/go/artifacts/outcome_test.go` (смерть транспорта на
   каждой мутации и на чтениях; нечитаемое тело; 4xx/5xx; предотменённый контекст),
   плюс сохранение существующего теста `transfer_error_test.go`.
2. **State Python: связанный claim и терминальная причина.** `claim_run(..., run_id=)`
   и `claim_run_for(worker, run_id)`, `release(fence, status, close_reason=)` —
   паритет с `ClaimRunFor`/`ReleaseWithReason` Go. Измерено:
   `sdk/python/tests/test_state_claim_and_cause_contract.py` (5 тестов; до правки
   4 из 5 падают).
3. **Tools: читаемый результат операции (`ToolResult` / `tool_result`) в обоих SDK.**
   Контракт владельца примонтирован полосой `tools-sdk` (роль: `POST
   /v1/tools/GetResult`, mTLS + task-bearer, capability `tools`); здесь закрыта
   публичная поверхность: Go `(*tools.Client).ToolResult` и
   `tools.FetchResultBytes`, Python `ToolsClient.tool_result` /
   `AsyncToolsClient.tool_result` и `fetch_result_bytes` /
   `fetch_result_bytes_async`. Обе стороны: только `SUCCEEDED` строка с
   provenance отдаёт результат; `NOT_FOUND` (ключа нет ИЛИ он чужой),
   `FORBIDDEN` (нет verified context, чужой/отсутствующий provenance, другой
   context того же субъекта, отозванный грант, не-task bearer, примирённая
   строка), `CONFLICT` (pending/uncertain/failed/stale) проходят сквозь клиент
   без смягчения и без повтора; чтение никогда не помечается как
   неизвестный исход эффекта. Артефактный результат берётся из Artifacts-владельца
   под ТЕКУЩИМ полномочием и проверяется по заявленному digest до выдачи байтов.
   Измерено: `sdk/go/tools/result_test.go` (дабл-владелец: категории, отсутствие
   повтора, самопротиворечивый 200, digest/размер),
   `sdk/python/tests/test_tools_result_contract.py` (12 тестов, sync+async),
   живая нога `TestPublicSDKOwnerAPIsRealPostgres/ToolsResultReadUnderOwnerAuthority`
   (реальные PG + Identity + mTLS + MCP-фикстура: provenance, отсутствие второго
   dispatch, CONFLICT на uncertain, NOT_FOUND чужому) и её Python-дубль в
   `sdk/python/ahp_clients/tests/live_contract.py`. Исполнительные примеры:
   `sdk/examples/tool-result` (Go и Python).
   Границы (по отчёту владельца `boyle-tools-result-c34a78a6.json`): реальный
   внешний MCP-провайдер — NOT_RUN (протокольная фикстура); артефактный sink в
   композиции владельца — фикстура, не настоящий Artifacts-адаптер; примирённые
   строки остаются нечитаемыми (403), меток у них нет и выдумывать их нельзя;
   человеческое чтение результата — отдельный контракт, не реализован.

## Пробелы, которые остаются (и почему они не закрыты здесь)

- **Chunk-чтение большого результата отдельным роутом.** Владелец контракта
  отдаёт переполненный результат как неизменяемый `artifact_ref`, байты читаются
  штатным Artifacts-владельцем (`FetchResultBytes` / `fetch_result_bytes` с
  проверкой digest); отдельного `ReadOutputChunk`-роута в примонтированной
  композиции нет, поэтому и публичного метода под выдуманный роут нет.
- **Человеческое чтение результата.** `GetResult` сознательно task-facing;
  UI-чтение — отдельный контракт владельца, а не задача SDK.
- **Go `Memory.Management` / Python-обвязка жизненного цикла** — различия формы, а
  не операций; менять их без требования нельзя (см. источник требований — PDF rev 3.2).
- **Go Compute `BeginInstallDetails`, `CompleteWithOutput`** — отсутствие типизированных
  методов в Python при том, что транспорт умеет вызвать операцию. Закрывается по
  требованию, не «в процессе».

## Как это проверяется целиком

- Реальные owner-API прогоны (PostgreSQL + Identity-гранты + клиентские сертификаты):
  `TestPublicSDKOwnerAPIsRealPostgres` (`cmd/ahp/sdk_public_clients_test.go`), включая
  подтест `PythonActualOwnerAPI`, который гоняет Python по тем же реальным сокетам.
- Go-поверхность полосы: `go test ./sdk/... ./pkg/sdk/... ./tests/sdk/ ./adapters/emil/`.
- Python-поверхность: `python3 -m pytest tests/...` из `sdk/python` (для модулей,
  тянущих LangGraph, нужен extras `[langgraph]` — на хосте без него эти файлы не
  собираются, и новые тесты сознательно от этого независимы).
