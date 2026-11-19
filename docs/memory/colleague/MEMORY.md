# Личная и проектная память AHP

Статус: интеграция адаптирована к upstream checkpoint `7c810ec2aa5d`; все 20 модулей собираются, тесты всех модулей компилируются. Сквозная приёмка с Hindsight и браузером ещё требуется. Изменения — [MEMORY-UPDATE.md](MEMORY-UPDATE.md), результаты и ограничения — [VALIDATION](../memory-service/VALIDATION.md).

Полная [архитектура личной и проектной памяти](handoff/hindsight-memory-architecture.md) включена в репозиторий. Публикация и согласованный checkout описаны в [PUBLISH.md](PUBLISH.md).

## Продуктовый контракт

- Личный банк принадлежит `(tenant, subject)`. Новый агент, deployment или чат не создаёт новый личный банк.
- Проектный банк принадлежит `(tenant, subject, Workspace)`. MVP поддерживает Workspace, у которого ровно один участник и этот участник — владелец. Проектный банк используется всеми чатами этого проекта.
- Банка сессии и общей памяти tenant нет. История, checkpoints и leases остаются в State.
- Удаление чата сохраняет уже извлечённые знания. Clear памяти — отдельная команда; она не удаляет историю чатов. Удаление пользователя закрывает все его банки и запрещает позднюю запись.
- При добавлении участников в Workspace доступ к проектной памяти блокируется. Личная память не публикуется. Владелец может очистить свой закрытый проектный банк.
- Память проекта удаляется после 30 дней бездействия по умолчанию. Настройка `idle_days` относится к банку проекта; она не удаляет сам Workspace и его файлы. Поддерживаемый диапазон API — 1–36500 дней.

`project_id` сейчас — имя Workspace в существующем API AHP. Подмена владельца и физического `bank_id` через клиентский запрос не допускается. При будущем добавлении rename/recreate Workspace нужно перейти на его устойчивый ID и определить миграцию привязок.

## Путь одного хода

1. Browser → существующий Space ingress → штатный обмен human session на task authority. Новый чат получает выбранный Workspace. Продолжение читает проект из durable run payload; смена проекта требует нового чата.
2. Перед принятием проектного хода ingress вызывает `POST /v1/memory/bind`. Сервис проверяет Identity и Workspace и сохраняет неизменяемую привязку `(tenant, subject, context) → project`. Агент не может создать или изменить её. Каждый новый ход обновляет активность проекта с собственным ключом операции.
3. Core вызывает MemoryDriver перед модельным циклом. Для проекта запрашивает релевантные знания проекта и, если разрешено настройкой, личные предпочтения. Для чата без проекта читает личную память. На следующих tool-шагах того же выполнения контекст повторно не читается. При resume старый помеченный блок заменяется свежим, служебная метка не отправляется провайдеру.
4. Агент получает `memory_search`, `memory_remember`, `memory_correct`. Модель выбирает цель `personal`/`project`; runtime добавляет текущий проект, task token, State reference и устойчивый ключ операции. Модель не получает API для смены владельца или создания произвольного банка.
5. State атомарно фиксирует terminal `done`/`failed` и событие completion в одной SQL-транзакции. Отдельный потребитель внутри composition читает ограниченную проекцию input + последних model/tool frames. Hidden reasoning, tool arguments и блоки инъекции памяти не включаются.
6. Потребитель проверяет сохранённые сервером claims по живому Identity grant, Workspace и State references и передаёт материал в durable очередь Memory. Bearer token в outbox/очереди не сохраняется.
7. Две независимые ветки вызывают штатный Hindsight retain с разными `retain_mission`: личная отбирает устойчивые сведения о пользователе, проектная — решения, выводы, находки и результаты работы. Observations и консолидация остаются штатными.

Возможны два recall и два retain, но это не две синхронные записи на каждый запрос модели. Проектная ветка отправляется после 5 новых завершённых ходов или 2 минут с первого ожидающего материала; личная — после 20 ходов или 10 минут. Таймер не продлевается каждым новым сообщением. Явное remember отправляется сразу в выбранную область. Короткий оставшийся срок grant может ускорить отправку.

Изоляция владельцев и разрешённые области источника — серверные проверки. Семантическое разделение фактов между двумя частными банками выполняет Hindsight; оно может ошибаться, как согласовано для MVP. Нет отдельного LLM-router, форка экстрактора или стадии ручного approve каждого факта.

## Настройки и интерфейс

Существующая страница памяти в `frontend/web/aifactory/ahp-live.js` использует штатный Space proxy. Она предоставляет выбор области и личного Workspace, список/поиск, явную запись, исправление исходного факта, invalidation, включение/выключение и очистку. Для проекта доступны `use_personal` и срок бездействия. Операции показываются отдельно от найденных фактов: `202 queued` ещё не означает, что запись проиндексирована.

| Настройка | Действие |
|---|---|
| `personal_enabled` | Глобально прекращает чтение и новые записи личного банка; сохранённые факты остаются |
| `project_enabled` | Прекращает чтение и новые записи выбранного проектного банка |
| `use_personal` | Разрешает чтение и автоматическое пополнение личной памяти из этого проекта |
| `idle_days` | Задаёт удаление памяти после бездействия проекта |
| Clear | Закрывает поколение банка; старые задания не могут заполнить новое поколение |

Чтение настроек и просмотр статуса не продлевают активность. Новый принятый проектный ход продлевает её. Прямой runtime API поддерживает run heartbeat для инфраструктуры с длительными задачами; обычный Core использует привязку при приёме хода и completion. Задачи, переживающие срок своих grant, требуют штатного решения о продлении полномочий; сервис памяти его не обходит.

Списки ограничены API-пагинацией (`GET items`, максимум 100 фактов). Текущий UI показывает первую страницу; расширенный просмотр истории изменений, автоматический polling и выбор Workspace из каталога остаются развитием интерфейса.

## Авторизация и API

Machine face смонтирован в существующем `ahp --role all` с тегом `ahp_data`. Он использует тот же mTLS transport, JWT verifier и живой Identity authorize, что остальные planes. Успешный recall/list сначала добавляет метки источников и intrinsic private subject в существующий Identity context journal и только затем отдаёт текст. Нового IAM и второго журнала меток нет.

| Действие | Штатная capability | Дополнительное ограничение |
|---|---|---|
| Read/search/remember/correct | `memory` | Текущий владелец, проект по серверной привязке, native label policy |
| Изменить настройки / повторить операцию | `memory:review` | Ingress authority или control |
| Clear | `memory:delete` | Ingress authority или control |
| Удалить проектный банк | `memory:delete` | Control deployment |
| Удалить память пользователя | `memory:delete-subject` | Существующий административный deletion coordinator |
| Прямые runtime events | `memory` | Только deployment из `AHP_MEMORY_RUNTIME_DEPLOYMENTS` |

State consumer — отдельный внутренний вызов с непередаваемым через HTTP маркером. Он по-прежнему проходит живую авторизацию; allowlist публичных event producers не требуется для чтения доверенной State outbox.

REST: `remember`, `search`, `context`, `items`, `settings`, `status`, `operations/{id}`, `items/{ref}`, `bind`, `clear` под `/v1/memory/`. Runtime face: `/v1/memory/runtime/{turns,flush,runs}`. `DELETE /v1/memory/projects/{id}` закрывает банк проекта. Mutations требуют `Idempotency-Key`.

Browser POST RPC через `/u-ahp/v1/memory/`: `Remember`, `Search`, `Settings`, `UpdateSettings`, `Status`, `Operation`, `Clear`, `Correct`, `Invalidate`. Proxy не открывает runtime/admin endpoints. Адаптер переводит эти команды в тот же REST handler, сохраняя проверенный principal. Пустой Search означает listing.

## Хранение, доставка и удаление

Новый платформенный tenant deletion подключён через тот же durable coordinator: Memory сначала ставит tenant fence, затем выдаёт полный inventory владельцев, после чего выполняет subject erasure. Owner directory регистрируется атомарно с записью под shared tenant lock; fence эксклюзивен. После fence worker сохраняет corpus и прекращает автоматическую очистку до явного erasure intent, включая legal hold. При наличии legacy corpus он тоже участвует в inventory/erasure. Это административный lifecycle, а не общая память tenant. Переход SQL schema v1→v2 требует остановки старых writers перед миграцией; подробности в [MEMORY-UPDATE.md](MEMORY-UPDATE.md).

Go orchestration использует общий SQL pool AHP и отдельный namespace `ahp_longterm`. В нём находятся user/project settings, привязки, receipts, cursors, jobs и поколения банков. Native Hindsight имеет свою PostgreSQL базу. Банк — логический объект Hindsight, не отдельная схема PostgreSQL на каждого пользователя. API/worker не требуют локального диска для состояния.

State outbox находится в `ahp_state.run_completions`; её consumer API не опубликован в HTTP. Повторная доставка безопасна за счёт стабильного event digest и ключа операции. События одного thread не обгоняют ожидающего предшественника; разные threads могут обрабатываться независимо. Lease ограждает подтверждение чужим/устаревшим worker. Это доставка at-least-once, не обещание однократного LLM-вычисления при любом сбое upstream.

Worker сохраняет native `operation_id`; курсор движется только после `completed`, включая ноль извлечённых фактов. Удаление закрывает поколение и ставит барьер времени источников. Старые банк-ID повторно проверяются и очищаются, чтобы поздняя старая операция не вернула их в обслуживание. Subject erase ставит постоянный write fence и считается незавершённым до очистки native банков.

Удаление чата не вызывает удаление банков. Если история удалена до обработки completion, State evidence уже может быть недоступна: незаписанный материал не обещается восстановить. Уже сохранённые факты остаются. Invalidation исключает факт из использования, но не заменяет полное стирание банка. Физическое удаление резервных копий требует отдельной эксплуатационной политики.

## Включение и проверка

1. Собрать sibling workspace: `python3 integration/bootstrap-workspace.py` из корня `ahp_platform`.
2. Подготовить обычную Identity/browser/data composition, Workload TLS и отдельные тестовые PG/Redis/S3. Добавить настройки из [memory.env.example](memory.env.example). `AHP_ENABLE_BROWSER=1` выбирает штатную Identity authority; permissive dev mode для нового Memory backend не допускается. Новый mount требует действующий data deletion coordinator.
3. Для локального Hindsight использовать `docker compose -f integration/compose.yaml -f integration/compose.memory.yaml --env-file /path/to/local.env up -d`. Overlay добавляет зависимости, а не новые выдуманные Go executables. LLM endpoint должен быть утверждённым внутренним провайдером. Native local embedding/reranker models нужно загрузить или включить в image заранее; автоматический учёт через AHP Model Gateway здесь не реализован.
4. Из `platform-runtime` собрать `/tmp/ahp` командой `go build -tags ahp_data,ahp_exec -o /tmp/ahp ./cmd/ahp`, затем `/tmp/as-runner` командой `go build -tags ahp_data,ahp_exec -o /tmp/as-runner ./cmd/as-runner`. Запустить штатный `ahp --role all` и as-runner с `AS_MEMORY_URL`, workload mTLS и capability `memory` в run grant. Для console settings/clear нужны соответствующие permissions в provisioned ingress grant. В этом варианте composition State producer и Memory consumer работают с одним State store.
5. Пройти [приёмочные сценарии](../memory-service/VALIDATION.md). Исторические screenshots и отчёты об old Memory не подтверждают эту интеграцию.

Backend по умолчанию при `AHP_ENABLE_MEMORY=1` — `hindsight`. Старый proposal/review API доступен только при явном `AHP_MEMORY_BACKEND=legacy`; миграции его фактов в Hindsight нет. Новый UI рассчитан на Hindsight. Отключение feature или возврат legacy не удаляет native данные автоматически.

## Что остаётся до эксплуатации

Реальные PG/outbox tests, native contract/E2E и browser проверки пока обязательны. У Workspace ещё нет операции удаления самого проекта, поэтому Memory предоставляет delete endpoint, а сквозной Workspace-delete lifecycle требует реализации в Workspace/control plane. Нужны квоты, нагрузочные измерения, observability очереди/ретраев, управляемая политика backups и проверка erasure на живом Hindsight. Native model/embedding/rerank calls пока не списываются из task/tree budgets AHP. Отдельные custom agent containers получают SDK; автоматическая инъекция внутри произвольного стороннего runtime не реализована.

Pinned upstream для контракта: [Hindsight v1.1.0](https://github.com/vectorize-io/hindsight/tree/v1.1.0), [native deployment example](https://github.com/vectorize-io/hindsight/blob/v1.1.0/docker/docker-compose/external-pg/docker-compose.yaml). Обновление native версии требует повторной contract-приёмки.
