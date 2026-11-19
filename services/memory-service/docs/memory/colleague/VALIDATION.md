# Проверка интеграции Memory

Дата: 2 ноября 2026. Go: `/usr/local/go/bin/go`, версия 1.27.3. Проверялось актуализированное дерево 20 Git repositories, source `7c810ec2aa5d0d650205807f6d0e12489fd0fe62`, с локальной интеграцией памяти. Это не historical deployed build.

## Выполнено

| Проверка | Результат |
|---|---|
| `go test -race ./ahpbridge ./longterm ./hindsight ./longterm/httpapi ./longterm/sdk` из `memory-service` | 56 test cases, pass; transport и upstream представлены локальными recorder/RoundTripper без сетевого listener |
| `go test -race ./auth ./contracts ./labels` из `shared-common` | Pass |
| `node --test frontend/web/aifactory_memory.test.mjs frontend/web/aifactory_canonical.test.mjs frontend/web/streaming.test.mjs` | 15 tests, pass |
| `node --check` для изменённых `ahp-live.js` и `ahp-shapes.js` | Pass |
| `integration/verify-workspace.py build` | Все 20 модулей, tags `ahp_exec,ahp_data`: pass |
| `integration/verify-workspace.py compile-tests` | Все 20 модулей, tags `ahp_exec,ahp_data,state_integration`: pass; tests не исполняются |
| Целевые Core memory/replay и logical-step tests | 4 tests с `-race`: pass |
| Hindsight owner / tenant control adapter | 2 tests с `-race`: pass, включая отказ неавторизованного fence |
| Space project immutability и cold rehydration | 2 tests с `-race`: pass |
| `frontend`: `go test ./web -count=1` | Полный package suite: pass |
| `gofmt` изменённых/новых Go-файлов | Выполнен |
| `docker compose ... config --quiet` для base + memory overlay | Pass с синтетическими placeholders, без daemon/контейнеров |

Покрыты: cadence первого ожидающего материала, независимость веток, источники пользователя/ассистента, scopes, idempotency/parallel retry, lost ACK/native operation identity, zero-fact completion, labels before read, scope isolation, clear/generation/late source, failed payload expiry, settings, idle cleanup, subject fence, неизменяемая owner-scoped project binding, personal-only chat, native listing, live grant revocation, single-owner Workspace, trusted State consumer marker и browser RPC. Новый SDK-тест проверяет, что модель не может подменить runtime project/source references.

HTTP adapter test использует настоящий JWT signer/verifier и production middleware с синтетической TLS metadata через `httptest.NewRecorder`. Он не проверяет handshake, CA trust и сетевую конфигурацию. Native client fixtures сверены с локальным upstream v1.1.0; они не доказывают работу запущенного native сервера или качество LLM extraction.

Для offline проверок использовались `GOCACHE=/tmp/ahp-memory-go-cache`, `GOPATH=/tmp/ahp-memory-go`, `GOMODCACHE=/tmp/ahp-memory-go-mod`, `GOPROXY=off`. `GOPROXY=off` — локальное ограничение проверки, не рекомендуемая конфигурация CI.

## Не подтверждено

- PostgreSQL/Hindsight runtime: sandbox запрещает открытие сокетов, включая Unix socket в `/tmp`; соединение с Docker ранее также было запрещено. Никаких контейнеров, живой БД или model calls эта проверка не подтвердила. PG-проверка передана команде.
- Реальный mTLS suite: sandbox не разрешает сетевой listener. Recorder проверяет application guard, а не TLS infrastructure.
- Запуск Compose overlay, реальная миграция PostgreSQL, native retain/recall/consolidation/curation/erase, browser UI и нагрузочные измерения не выполнены. Установка Go dependencies завершена пользователем; ограничение компиляции снято.

PG-приёмка оставлена команде по прямому решению пользователя. Скомпилированы, но не исполнялись на живой БД: `state/completion_test.go` и `longterm/pgstore/tenant_test.go`. Core memory/replay и Space binding tests уже исполнены. Подготовлен [изолированный PG runner](../integration/scripts/validate-memory-pg.sh); он не использует существующие базы. Новые проверки tenant fence также входят в автономный suite `longterm`.

## Обязательная приёмка на изолированном стенде

После доступности модулей и настоящих PG/Redis/S3/Hindsight:

1. Выполнить `python3 integration/verify-workspace.py build` и `compile-tests` с Go в PATH. Затем с отдельными `AHP_TEST_DATABASE_URL`/`AHP_TEST_REDIS_URL` — `test`. Проверить новые State/Core/Space tests отдельно, не засчитывая skip как pass. Для `longterm/pgstore` задана собственная disposable `TEST_DATABASE_URL`.
2. Поднять обычный AHP Identity/browser/data стек и pinned Hindsight по [инструкции](../integration/MEMORY.md). Провести настоящий TLS positive/negative тест и revoke grant во время queued/submitted обработки.
3. Создать владельцу personal Workspace Alpha и Beta. В Alpha сообщить личное предпочтение и принять проектное решение с полезным tool result. Дождаться успешной native обработки. В новом чате Alpha проверить обе области; в Beta — личную память и отсутствие проектного Alpha. Второму пользователю доступ к обоим банкам закрыт.
4. Проверить новый чат без Workspace, resume/restart, повтор terminal event и native lost ACK, successful extraction нуля фактов. Нет дубликатов, ложного статуса indexed и повторной инъекции на каждом tool step.
5. Отключить `use_personal` в Alpha. Проверить отсутствие личных фактов у runtime и невозможность обойти настройку прямым scoped запросом без `project_id`. Проверить глобальное выключение, повторное включение без backfill старых сообщений и очистку.
6. Добавить второго участника в Alpha: проектная память блокируется для обоих как неподдерживаемый shared Workspace; ни один банк не публикуется. Владелец сохраняет возможность очистить собственный закрытый банк.
7. Удалить чат, в котором уже получены факты. Создать новый чат того же проекта: знания остаются, история удалённого чата недоступна. Pending материал без доступной State evidence не обещается восстановить.
8. Выполнить clear, project-bank delete, subject deletion и idle expiry с сокращённым тестовым сроком/часами. Проверить late events, lease takeover и позднее native завершение; закрытый bank ID не возвращается в обслуживание, subject deletion не завершён до cleanup.
9. Проверить браузерную страницу: правильный scope, settings/403/disabled/degraded состояния, queued → completed/failed, исправление исходного факта и derived observation, сохранение project binding после reload. Подтвердить отсутствие ложной записи в списке до окончания extraction.
10. Измерить recall P95, queue/retain lag, backlog и retries при сбоях, число банков/фактов/операций, SQL contention и LLM/embedding/rerank стоимость. Для production нужны утверждённые квоты, SLO и backup/erasure policy.

Сквозное удаление самого Workspace проверяется после появления соответствующей операции в Workspace/control plane. Memory endpoint уже есть; платформенный lifecycle не объявляется завершённым.
