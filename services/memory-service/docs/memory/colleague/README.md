# AHP Memory Service на Hindsight

Go-сервис личной и проектной памяти в существующей AHP composition. Один личный банк на `(tenant, subject)` и банк на каждый личный Workspace с единственным владельцем. Чаты хранятся в State; удаление чата сохраняет извлечённые знания.

Контракт: [integration/MEMORY.md](../integration/MEMORY.md). Проверки: [VALIDATION.md](VALIDATION.md). После актуализации все 20 модулей собираются и их тесты компилируются. Live E2E с Hindsight ещё требуется; совместимость описана в [MEMORY-UPDATE](../integration/MEMORY-UPDATE.md).

## Код

| Пакет | Ответственность |
|---|---|
| `longterm` | Настройки, проектные привязки, durable очередь, поколения банков, курсоры, idle expiry, erasure |
| `longterm/pgstore` | JSONB records в `ahp_longterm`, общий SQL pool AHP |
| `hindsight` | Штатные native bank/retain/recall/list/curation/operation API |
| `ahpbridge` | AHP principal, live Identity grants и labels, Workspace/State authority, browser RPC |
| `longterm/httpapi` | REST API без собственного IAM |
| `longterm/sdk` | Go-клиент и три memory tools без выбора физического банка моделью |
| `memory` | Прежний proposal/review backend для явного `AHP_MEMORY_BACKEND=legacy` |

`longterm/IMPORT.json` фиксирует импорт из первоначального `services/ahp-memory`. Эта копия каноническая для платформы. Upstream Hindsight не изменялся. Legacy-факты автоматически не мигрируют.

## Включение

Используются существующие `platform-runtime/cmd/ahp` (`--role all`, build tag `ahp_data`) и `cmd/as-runner`. Требуются native Identity/browser/data composition, workload mTLS и приватный Hindsight. `AHP_ENABLE_MEMORY=1` выбирает Hindsight по умолчанию. Недостающая authority/configuration останавливает mount.

Примеры: [compose.memory.yaml](../integration/compose.memory.yaml), [memory.env.example](../integration/memory.env.example). Они дополняют обычную конфигурацию AHP. Sibling workspace собирается `python3 integration/bootstrap-workspace.py` из корня `ahp_platform`.

## Проверки

Из `memory-service`:

```sh
go test -race ./longterm ./hindsight ./ahpbridge ./longterm/httpapi ./longterm/sdk
# PostgreSQL: отдельная disposable TEST_DATABASE_URL
go test -race ./longterm/pgstore
```

Полная проверка модулей выполняется штатным `integration/verify-workspace.py`. Пропущенные PG tests и историческая приёмка не считаются проверкой новой интеграции.

Актуальный upstream export: `7c810ec2aa5d0d650205807f6d0e12489fd0fe62`; baseline ownership сохранён в `SOURCE.json` и `integration/source-map.json`. Локальная интеграция не переписывает исходные export hashes.

Автор обновил legacy backend: policy-bound embeddings, read-label journal, reindex и tenant deletion. Эти изменения сохранены. Его контракт и ограничения описаны в [handoff/memory-integration.md](../integration/handoff/memory-integration.md); новые Hindsight операции и автоматическая запись описаны отдельно в [MEMORY.md](../integration/MEMORY.md).
