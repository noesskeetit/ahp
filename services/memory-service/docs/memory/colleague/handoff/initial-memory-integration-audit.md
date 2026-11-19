# AHP: оценка прототипа и интеграция Hindsight Memory

Дата: 2 ноября 2026. Объект: локальный `ahp_platform`, исходный export commit `5ada0af33527a34469a32c8f8a931cad90849f2e` и внесённая интеграция памяти. Это аудит исходников и доступных проверок, не подтверждение работающего production deployment.

После этого аудита все 20 Git-репозиториев обновлены до source `7c810ec2aa5d`; часть отмеченных здесь ограничений закрыта upstream, а интеграция адаптирована к новым контрактам. Актуальное состояние — [MEMORY-UPDATE.md](../MEMORY-UPDATE.md). Ниже сохранён аудит исходного checkpoint `5ada0af`.

## Вывод

В AHP уже есть содержательная основа агентской платформы: live grants и делегирование, durable State с fencing, модельный и инструментальный шлюзы, Workspace, Agents Space и общая composition. Прототип существенно дальше набора интерфейсных заглушек. Однако готовность к production не подтверждена. До пилота нужно собрать именно эту копию, поднять настоящий стек, проверить права и удаление сквозными сценариями. До эксплуатации несколькими клиентами добавляются квоты, нагрузочная приёмка и эксплуатационные контракты.

Память перенесена в существующий Go-модуль `memory-service` и подключена к настоящим точкам интеграции AHP. Создавать отдельную платформу вокруг прежнего standalone-прототипа не потребовалось. Важное ограничение результата: автономное ядро и AHP authority/RPC adapter проверены; новая State/PostgreSQL/composition часть ещё не прошла полную сборку и live E2E из-за ограничений среды. Наличие кода и успешный unit test не заменяют этот этап.

## Что находится в export

Это 20 sibling Go modules с логическими import paths `github.com/ahp-platform/handoff/...`, а не 20 уже развернутых независимых сервисов. Существующие executables в `platform-runtime/cmd/ahp` и `cmd/as-runner` собирают сервисные библиотеки. Штатный bootstrap создаёт общий `go.work`. Файлы `SOURCE.json` и `integration/source-map.json` фиксируют исходный export; их хэши после доработки закономерно отличаются от текущих файлов и не переписываются задним числом.

Исторический `integration/handoff/readiness-matrix.md` описывает другой live build (`6453201d`) и test-only изменения. Его screenshots, старые отчёты и ссылки на внешние `/tmp` evidence нельзя считать доказательством этой копии. `integration/handoff/pending` содержит отдельно отложенные Registry/tenant deletion/Triggers изменения; они не применялись автоматически.

| Область | Что есть в коде | Что это доказывает и чего ещё не доказывает |
|---|---|---|
| Identity / transport | Проверенные JWT, workload mTLS, scope, live grants, actor chain, delegation, журнал меток контекста | Хороший общий фундамент авторизации. Нужна реальная сквозная проверка provisioned пользователей, отзыва grant и TLS в целевом окружении |
| State | SQL threads/runs, lease/epoch, checkpoints, stream, idempotency, deletion fences | Есть механизм устойчивого выполнения. Нужны повторная PG/crash/race приёмка export и новой outbox |
| Model Gateway | Routing, generation journal, бюджеты и квоты; tree-budget wiring за флагом | Нельзя повторять старое «общий бюджет вообще не начат». При этом инвариант бюджета при всей глубине делегирования надо принять испытаниями; Hindsight пока вне этого контура |
| Tools / Artifacts | Штатные policy, approvals, durable mutation journal, S3 и метки данных | Интеграция памяти использует существующие полномочия и journal; реальные provider/S3 сценарии здесь не повторялись |
| Workspace / Registry | Ревизии, candidates/publication, ownership/membership, версии и pinned configuration | Можно однозначно сопоставить личный проект с private Workspace. Нет штатного DeleteWorkspace lifecycle для полного исполнения требования удаления проекта |
| Agents Space / Core / Frontend | Human edge, task exchange, durable turns, streams, model/tool loop и рабочие поверхности консоли | Есть реальные точки для recall, tools и project binding. Новый UX и перезапуск/делегирование с памятью требуют live проверки |
| Compute | Explicit mock host/microVM путь в прототипе | Не является доказательством готовности облачного execution/isolation слоя |
| Deployment / integration | Render/apply scaffolding, recipes, много тестовых сценариев и историческая приёмка | Полезная основа передачи команде, но не воспроизводимая приёмка текущего дерева сама по себе |
| Memory до изменений | Proposal/review модель, отдельные scope/ACL и lexical retrieval; optional embedding seam | Контракт расходился с согласованной автоматической личной/проектной Hindsight памятью |

## Принятые продуктовые решения

Один личный банк на пользователя внутри tenant и банк на каждый его личный Workspace. Общего tenant-банка и session-bank нет. История сессии живёт в State. Workspace поддерживается только с одним участником-владельцем. Расширение membership не делает банк общим.

Удаление чата сохраняет уже извлечённые знания. Clear памяти, удаление пользователя и удаление проектного банка — отдельные действия. При удалении проекта его банк должен удаляться; Memory endpoint реализован, а отсутствующий Workspace deletion lifecycle остаётся явной задачей платформы. По умолчанию проектная память очищается после 30 дней бездействия; срок настраивается.

Hindsight получает полезные пользовательские сообщения, выводы, решения, находки и текстовые результаты работы агента. Два retain mission отбирают личные и проектные факты; ошибки семантического разделения допускаются для MVP. Жёсткую изоляцию владельцев, выбор банка и права сервис не делегирует модели.

## Что изменено в платформе

| Участок | Реализация |
|---|---|
| `memory-service/longterm` | Импортированное Go-ядро: очереди, поколения, selective retain, cursors, leases, settings, clear/delete/idle expiry, subject erasure fence, listing, неизменяемая привязка context к проекту |
| `memory-service/hindsight` | Тонкий клиент штатного Hindsight v1.1.0: bank configuration, async retain/status, recall/list, native curation, bank deletion. Код upstream не изменялся |
| `memory-service/ahpbridge` | Проверенные transport claims, live Identity authorize/Evaluate/AddLabels, single-owner Workspace, State evidence, browser RPC. Без второго IAM и без сохранения bearer token |
| `platform-runtime/cmd/ahp` | Default backend `hindsight` при включённом Memory feature; штатный SQL pool/data deletion coordinator, worker и completion consumer. `legacy` остаётся явно выбираемым режимом |
| `state-service/state` | Опциональная completion outbox в транзакции завершения run; ограниченная scope-checked evidence projection; lease/ack/retry/order API для доверенного consumer |
| `agents-space/space` | ProjectID в durable turn/recovery/response, неизменяемость проекта продолженного чата, ingress binding до принятия проектного хода; новые browser operations в allowlist |
| `execution-service/core`, `cmd/as-runner` | Recall перед model loop, замена старой инъекции при resume, три per-request memory tools через существующий mutation journal, task token и проект из runtime |
| `frontend/web/aifactory` | Личная/проектная область, Workspace, просмотр/поиск, async operations, settings/clear/correct/invalidate, project_id при создании/восстановлении чата |
| `integration` | Native Hindsight Compose overlay, пример настроек и [контракт интеграции](../MEMORY.md) |

При чтении память добавляет intrinsic private subject label в штатный журнал Identity до выдачи текста. Агент не может переключить личный банк на другого пользователя или выбрать проект из tool arguments. Прямой запрос без `project_id` из проектного контекста также не позволяет обойти `use_personal`.

## Оценка проработанности

Продуктовый MVP достаточно определён для реализации: владельцы банков, роли State/Hindsight/AHP, автоматические и явные записи, настройки, права, асинхронность, исправления и пользовательское удаление зафиксированы. Базовые API и state machine уже реализованы. Не нужно ещё раз выбирать между session/user/project/tenant для начала разработки.

При этом всю систему пока нельзя назвать полностью проработанной для production:

| Приоритет | Незакрытый вопрос | Условие завершения |
|---|---|---|
| До пилота | Полная сборка именно этого export и интеграции | CI собирает все 20 modules, tagged composition и новые тесты без неизвестных зависимостей |
| До пилота | Реальная цепочка State → queue → native Hindsight → новая сессия | Проверить successful/zero-fact extraction, retry/lost ACK, restart и отсутствие дубликатов/доступа чужого owner |
| До пилота | Private Workspace и контекст | Чужой/shared Workspace отвергается, membership change блокирует проектную память, прямые запросы не обходят серверную привязку |
| До пилота | Удаление и срок бездействия | Chat-delete сохраняет факты; clear/subject-delete исключают позднее восстановление; idle boundary и сбои native удаления проверены на живой БД |
| До пилота | Native inference endpoint и стоимость | Утвердить внутренние LLM/embedding/rerank providers и локальные лимиты. Hindsight сейчас не использует task/tree budget Model Gateway автоматически |
| До продуктового релиза | Удаление Workspace | Реализовать платформенную операцию с durable dispatch в Memory; определить stable project ID при rename/recreate |
| До масштабирования | Квоты и производительность | Измерить число банков/операций/фактов, P95 recall/retain lag, стоимость моделей, PG contention, рост receipts/tombstones и backpressure. Текущие owner-level JSONB scans не имеют подтверждённого лимита нагрузки |
| До production | Эксплуатационный контракт | Зафиксировать SLO, мониторинг отставания/ошибок/cleanup, миграции, backup/restore и сроки физического стирания. Минимальные tombstones — всё ещё pseudonymous metadata |
| По итогам пилота | Качество и UX | Проверить разделение личных/проектных фактов на русскоязычных задачах, hallucination/contradictions, восстановление проектов в UI, большие списки, состояния отказа и неготового результата |

Заглушки моделей/вычисления и статистические эвристики не выдаются за native memory quality. Базовое masking очевидных credentials в evidence не является полноценным DLP. Автоматическая память реализована для штатного Space/Core пути; произвольным custom agent containers доступен SDK, но внедрение в их внутренний цикл не предполагается автоматически.

## Передача разработке

Основной вход: [integration/MEMORY.md](../MEMORY.md), код [memory-service](../../memory-service/README.md), [VALIDATION](../../memory-service/VALIDATION.md), [архитектура](hindsight-memory-architecture.md). Сначала завершить сборку и приёмочные сценарии пилота, затем нагрузочный и эксплуатационный контур. Старый standalone `services/ahp-memory` сохранён как источник первоначального прототипа; каноническая AHP интеграция находится в `ahp_platform/memory-service`.
