# Подключение и замена сервиса памяти

Локальная доработка поверх source `7c810ec2aa5d` включает Hindsight по умолчанию. Актуальный контракт — [MEMORY.md](../MEMORY.md), адаптация новых owner/replay API — [MEMORY-UPDATE.md](../MEMORY-UPDATE.md). Описание ниже относится к upstream backend `AHP_MEMORY_BACKEND=legacy`; его проверки и corpus сохранены. Требования к authority, labels и erasure применяются и к новому backend.

Рабочий репозиторий: `memory-service`. Это долговременные знания агента: предложение записи, согласование, поиск, чтение, метки доступа и удаление. История чата и checkpoints исполнения относятся к `state-service`, проектные файлы и версии — к `workspace-service`/`artifacts-service`.

## Где находится код

| Задача | Файл или каталог после посервисного экспорта |
|---|---|
| HTTP контракт | `memory-service/memory/memoryapi/api.go` |
| Модель данных и PostgreSQL миграция | `memory-service/memory/store.go` |
| Поиск и ранжирование | `memory-service/memory/search.go` |
| Жизненный цикл записи | `memory-service/memory/propose.go`, `lifecycle.go`, `reindex.go` |
| Embeddings и политика выхода к поставщику | `memory-service/memory/embed/` |
| Словарь capabilities | `memory-service/memory/capabilities.go` |
| Подключение к запускаемому backend | `platform-runtime/cmd/ahp/mount_memory.go` |
| Общее удаление данных | `platform-runtime/cmd/ahp/` и `memory-service/memory/tenant_deletion.go` |
| UI | `frontend/web/aifactory/ahp-live.js` |

HTTP вход legacy-профиля — `POST /v1/memory/{Operation}`; дополнительный PDF-профиль при native Hindsight — `/v1/memory/shared/{Operation}`. Операции: `Propose`, `Review`, `Search`, `Read`, `Get`, `Reindex`, `AddLabels`, `Delete`, `DeleteSubject`, `Fence`. `Read {id}` читает память, `Get {op_key}` возвращает квитанцию операции. Старый псевдоним Get → Read исправлен.

Вход требует mTLS и task JWT с audience `ahp`, а также capability `memory`. Дополнительные права: `memory:review` для Review/Reindex, `memory:label`, `memory:delete`, `memory:delete-subject` для соответствующих операций. `Fence {op_key}` закрывает ключ, не удаляет память и не является псевдонимом DeleteSubject. Подробности — [контракт ключей](../../../memory-operation-keys.md). Удаление tenant координируется общей управляющей операцией платформы.

## Что важно сохранить при замене

1. Область `tenant/deployment/subject` берётся из проверенного principal. Чужой scope из JSON не даёт права читать или писать данные.
2. Согласование, метки данных и действующие полномочия проверяются при чтении и записи. Нельзя делать новую память общедоступной только ради успешного Search.
3. Read journal участвует в проверке меток записи. Ошибка проверки не должна превращаться в разрешение.
4. Удаление пользователя/tenant сначала запрещает новые записи, затем очищает данные. Новый backend должен участвовать в существующей сохраняемой операции удаления, включая повторы и поздние writes.
5. История чата и checkpoints остаются в State. Замена Memory сама по себе не требует переписывать их хранилище.
6. Подключение к внешней памяти делается через явно настроенный endpoint/credentials и текущую политику доступа. Значения credentials не добавляются в этот репозиторий.

## Текущий backend

PostgreSQL с pgvector; текстовый поиск, embeddings и связи между записями. Для настоящего семантического режима использовался BAAI/bge-m3 с размерностью 1024. Режим `lexical` разрешён только как явная ограниченная конфигурация; отсутствие embedding endpoint не должно незаметно выдаваться за семантический поиск.

Имена настроек есть в `memory-service/.env.example`. Помимо endpoint/model/dimension нужны версия embedding-пространства, выбранный provider и policy file. Предпочтителен `AHP_MEMORY_EMBEDDING_KEY_FILE` с правами `0600`. Настоящего файла ключа в Git нет. Смена модели/размерности требует согласованной миграции и `Reindex`, а не удаления существующих знаний.

Текущая hybrid composition запускается через `cmd/ahp --role all` с `AHP_ENABLE_MEMORY=1` и `AHP_ENABLE_BROWSER=1`: в данном wiring именно второй флаг подключает live Identity authority. Без неё hybrid-режим отказывается стартовать. Нужны настроенные TLS/Identity, проверка текущих меток и зависимости Artifacts/Workspace. Это ограничение текущей composition, а не требование включать браузер внутри будущего отдельного Memory-сервиса. Для режима `lexical` не задавайте embedding BASE_URL: одновременно выбранный lexical и настроенный embedding backend отклоняются как противоречивая конфигурация.

В текущей composition `newMemoryStore` получает зависимости Artifacts, Workspace, Identity/меток. Если вместо встроенной библиотеки подключается отдельный сервис, эти зависимости надо выразить его адаптерами и сохранить проверки выше; просто заменить DSN недостаточно.

## Сборка и проверка

Сначала клонируйте `integration` и выполните `bootstrap-workspace.py` согласно его README: он получает согласованные sibling repos и создаёт `go.work`. Затем:

```sh
cd memory-service
go build -tags ahp_exec,ahp_data ./...
go test -race -p 1 ./memory/... -count=1
```

Для интеграционных тестов укажите `AHP_TEST_DATABASE_URL` на отдельную PostgreSQL с pgvector. Некоторые составные проверки также требуют Artifacts/MinIO и embedding backend; env указаны в соответствующих tests. `SKIP` из-за отсутствия инфраструктуры не подтверждает исправность интеграции. Для тестов, создающих отдельные базы, используйте тестовый кластер с соответствующим разрешением и pgvector template; production DSN недопустим.

Минимальная приёмка нового backend: запись → согласование → поиск по перефразированному запросу → чтение; запрет другого пользователя; отзыв доступа; удаление пользователя и tenant без появления поздних записей; повтор с тем же ключом; недоступность backend без обхода проверок. После этого — настоящий пользовательский сценарий в Agents Space.

Последние результаты текущего Memory получены до этой публикации на отдельных проверенных сборках: реальный embedding endpoint, сохранение исходной записи при переиндексации, поиск и запрет чужого доступа. Они не являются результатами будущего сервиса коллеги.
