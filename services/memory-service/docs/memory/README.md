# AHP Memory: Hindsight и SDK

Memory сохраняет модель коллеги: личный банк `(tenant, subject)` и проектный банк `(tenant, subject, private Workspace)`. State остаётся владельцем истории и completion outbox. Новый backend выбирается `AHP_ENABLE_MEMORY=1`, `AHP_MEMORY_BACKEND=hindsight`; явный `legacy` сохраняет proposal/review API. Автоматической миграции legacy corpus нет.

Карта точного обратного импорта семи feature commits находится в [import-provenance.json](import-provenance.json). Документы в [colleague](colleague/MEMORY.md) сохранены как источник контракта: команды и ссылки там относятся к исходным sibling repositories, а прежние результаты автора не являются результатами этой интеграции.

## Развёртывание

`infra/memory/compose.hindsight.yaml` создаёт только отдельные Hindsight API/PG, сеть и volumes с префиксом `memory-hindsight` в Compose project `ahp-handbook`. API опубликован на loopback `38888`. Native UI наружу не опубликован. Для воспроизводимой проверки использован upstream Hindsight 1.1.0, digest `sha256:50311d02b4d5d2b7c5242bd336edf8735df8374eaad496a92f49c83b37d2aa06`.

Пустые имена настроек — [memory.env.example](../../infra/memory/memory.env.example). Ключ approved LLM и пароль новой PG передаются через ignored `.env`/Vault. Подключение к личному Hindsight `127.0.0.1:8889`, его базе или его банкам для продукта не допускается.

```sh
docker compose -p ahp-handbook -f infra/memory/compose.hindsight.yaml \
  --env-file /path/to/ignored/memory.env up -d
```

Go composition требует data deletion coordinator, настоящую Identity authority, workload mTLS и `AHP_MEMORY_HINDSIGHT_URL`. Agent SDK обращается только к `/v1/memory` на AHP origin; native API и credentials остаются за Memory service. Native extraction/rerank пока не списываются из AHP Model Gateway task/tree budgets — это сохранённое ограничение исходной интеграции, а не доказанная бюджетная совместимость.

## Go/Python parity

| Контракт | Go `sdk/go/memory` | Python `ahp_memory` |
|---|---|---|
| Личная/проектная запись | `Remember` | `remember` |
| Поиск, проектный контекст, страницы | `Search`, `Context`, `List` | `search`, `context`, `list` |
| Исправление/инвалидация | `Correct`, `Invalidate` | `correct`, `invalidate` |
| Durable receipt | `Operation` | `operation` |
| Настройки/статус чтения | `Settings`, `Status` | `settings`, `status` |
| Изменение настроек/очистка/повтор | `Management().UpdateSettings/Clear/Retry` | `MemoryManagementClient.update_settings/clear/retry` |
| Отмена | `context.Context` | HTTP timeout; `AsyncMemoryClient` передаёт asyncio cancellation |
| Асинхронный интерфейс | обычные Go context-aware методы | `AsyncMemoryClient`, `AsyncMemoryManagementClient` |

Управляющий класс не выдаёт полномочий. Сервер требует ingress/control purpose и `memory:review` или `memory:delete`; обычный agent token с этими capabilities всё равно получает отказ. Raw human token на task face также запрещён. Tenant, subject и физический банк не выбираются из тела запроса. Проект проверяется против серверной привязки context и текущего единственного владельца Workspace.

Все mutations требуют стабильный ключ. Клиенты не повторяют запрос автоматически. При потере ответа следует повторить тот же payload с тем же ключом; изменение payload под прежним ключом возвращает конфликт. `queued`/HTTP 202 означает запись намерения; завершение проверяется через `Operation`. `Clear` возвращает `state=deleting`, а завершение native cleanup проверяется через `Status`, не через несуществующий operation ID. Platform transport errors и Memory errors сохраняют HTTP status/code/message. Redirects запрещены, размер ответа ограничен 4 MiB.

Python пакет не импортирует LangGraph. Он включён в существующий distribution без переименования `ahp_state`; текущие зависимости distribution могут включать LangGraph независимо от импорта `ahp_memory`.

```go
client, err := memory.New(ahpOrigin, currentTaskToken, workloadMTLSClient)
if err != nil { return err }
op, err := client.Remember(ctx, stableKey, memory.RememberRequest{
    Target: memory.Personal, Text: fact,
    SourceRefs: []string{"state://runs/" + runID + "/input"},
})
// Poll client.Operation(ctx, op.ID, memory.Personal, ""); queued is not success.
```

```python
from ahp_memory import MemoryClient

client = MemoryClient(ahp_origin, current_task_token, http_client=workload_mtls_http)
receipt = client.remember(stable_key, {
    "target": "personal", "text": fact,
    "source_refs": [f"state://runs/{run_id}/input"],
})
receipt = client.operation(receipt["id"], "personal")
```

## Проверки и границы

`TestHindsightNativeAgentAuthority` использует production Memory mount, реальную PostgreSQL, Identity и TLS handshake. Он вызывает публичные Go/Python SDK, native retain/recall и проверки изоляции/отзыва. Opt-in: `AHP_TEST_HINDSIGHT_URL` и `AHP_TEST_HINDSIGHT_OWNED=1`; без конфигурации это SKIP, не доказательство native работы. Тест использует синтетические факты, но настоящий LLM extraction.

Дополнительно настоящий State SDK создаёт и завершает `payload.type=turn` run частного проекта; production completion consumer автоматически доставляет его в native Hindsight. Короткий grant выбирает документированный immediate-flush путь, не меняя стандартные окна буферизации. Добавление участника Workspace немедленно закрывает доступ к частному проектному банку. Произвольный generic SDK run не становится автоматическим источником: consumer обрабатывает только `type=turn` и ограниченные input/stream text. Это не полная приёмка frontend, всех методов curation, quotas или disaster recovery.

Перед native Retain worker повторяет проверку срока identity и живого grant после сетевых Status/Configure. Отзыв/expiry закрывают отправку; временный отказ authority оставляет durable intent в очереди без dispatch. Между успешной проверкой IAM и native HTTP dispatch остаётся обычная межсервисная граница; атомарность IAM/Hindsight не заявляется.

```sh
go test -race -count=1 ./internal/memory/... ./internal/core ./internal/space ./internal/state ./sdk/go/memory
go test -race -count=1 -tags ahp_data,ahp_exec \
  -run '^TestHindsightNativeAgentAuthority$' ./cmd/ahp
PYTHONPATH=sdk/python python3 -m pytest -q sdk/python/tests/test_memory_client.py
```

PG tests принимают `AHP_TEST_DATABASE_URL`, а исходные tests коллеги также поддерживают `TEST_DATABASE_URL`. Native test ожидает обычные изолированные AHP S3/PG env. Запускать на отдельной test database: legacy `TestMemoryFaceLifecycleAndACL` при включённом `AHP_MOUNT_MEMORY=1` очищает свои migrated schemas после теста.

Legacy harness отражает отдельный reviewed контракт: PDF rev3.2 §7 различает `proposed/active`, требует `review_required`, source evidence, operation key и повторную проверку допустимого owner/project множества при retrieval. Владелец читает своё предложение через `Read`; agent Search возвращает активные записи после разрешённого review. Capability reviewer сама по себе не даёт доступа к чужому source. Проверки owner inspection, rejected foreign review, active retrieval и idempotent mutation keys сохранены отдельно от Hindsight.
