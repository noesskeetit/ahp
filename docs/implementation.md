# AHP и Agents Space: план полной реализации на Go

> Для исполнителей: использовать цикл subagent-driven-development — реализация, проверка соответствия, исправление. Пользователь поручил Codex оркестрацию, Qwen3.8-Flash-Next — написание кода, и явно разрешил параллельных исполнителей. Каждый исполнитель владеет только указанными файлами.

**Цель:** реализовать все части `ahp-agents-space-architecture (2) (1).pdf`, включая AHP, Agents Space, SDK, Compute, UI, поставку и проверки. Этапы не являются точками остановки.

**Спецификация:** `/Users/aogabbasov/Downloads/ahp-agents-space-architecture (2) (1).pdf`, 64 страницы, текст редакции 3.2 от 21.10.2026. Приватный исходник и извлечённый текст остаются вне репозитория.

**Архитектура:** общие сервисы AHP владеют своими схемами и API записи. Agents Space — отдельный системный деплой, пользующийся A2A gateway, платформенными API с токеном задачи и State API. Координатор ходов принадлежит Agents Space; кастомные агенты ведут свой цикл. Никакого прямого доступа Space к БД платформы.

**Стек:** Go 1.26, net/http, PostgreSQL/pgx, Redis, S3, Vault, OpenTelemetry; web UI. Python допускается только для требуемого PDF адаптера LangGraph. Новая реализация создаётся с нуля в `go-platform/`; прежний backend не используется.

## Общие ограничения

- Область tenant/deployment/subject приходит из проверенной идентичности, не из тела запроса.
- Публичный JWT человека и внутренний task token не взаимозаменяемы; внутренние соединения привязываются к идентичности workload.
- Отдельные схемы и владельцы записи; связи между сервисами через API.
- Одна аренда thread, время PostgreSQL, растущая epoch; старые записи отклоняются атомарно.
- Любой побочный эффект получает устойчивый ключ и digest. После отправки неизвестный исход не повторяется автоматически.
- Метки task и context только накапливаются; проверяются при модели, публикации, записи памяти, делегировании и ответе в чат.
- Секреты только во внешнем хранилище или игнорируемом окружении. Qwen получает требования и исходный код, но не значения секретов.
- PostgreSQL/Redis/S3 проверки выполняются на реальных изолированных сервисах. Fixture, локальный тест и production-проверка отмечаются раздельно.
- Ресурсы Docker только проекта `ahp-handbook`, с новыми именами сервисов и томов `go-*`. Старые ресурсы не используются.
- Тесты KVM/SDS, mTLS, IAM, HA, Tier 2B и нагрузка должны подтверждаться соответствующим runtime; отсутствие окружения не превращается в успешную проверку.

## Проверки, определяющие реализацию

1. Повтор сообщения/операции с другими аргументами, одновременно и после рестарта.
2. Zombie worker, перехват аренды, отмена до/после отправки, UNKNOWN через следующий ход.
3. Чужой tenant, deployment, subject, thread, context, operation и namespace; отзыв доступа во время работы.
4. Приватный факт из предыдущего хода: запрет публикации через память, файл, проект, модель и общий чат.
5. Потерянные события, закрытый SSE, ответы человека, каскадная отмена и удаление, обновление версии во время ожидания.

## Задачи и границы файлов

Для каждой задачи: сначала значимые тесты инвариантов, затем код, scoped checks, независимое ревью, исправление и общий прогон. Общие API фиксируются до параллельной реализации. В `progress.md` записывается реальный результат каждого gate.

- [ ] 01. `pkg/contracts`, `pkg/auth`, `pkg/labels`: Scope, ошибки, канонический digest; Ed25519 JWT с audience/token_use/expiry/workload binding; монотонные метки. Проверка: `go test -race ./pkg/...`.
- [ ] 02. `internal/state`, `sdk/go/state`: PostgreSQL State API — thread/run FIFO, lease/epoch, checkpoint/parts/writes, intents, inbox/suspend, stream, KV, lifecycle и atomic batches. Проверка: 100 конкурентных claims, stale epoch, IDOR, restart, дедупликация, удаление.
- [ ] 03. `internal/identity`: grants, Evaluate, task tokens, actor chain/delegation, label journal task/context, subtree revoke. Проверка: deny narrowing violations, наследование меток, отзыв без участия агента.
- [ ] 04. `internal/operations`: библиотека service-owned PostgreSQL ledger: ACCEPTED/DISPATCHED/SUCCEEDED/FAILED/FENCED/UNKNOWN, digest, durable guards, reconciliation. Проверка: конкурентный dispatch, fence before/after send, cross-deployment access.
- [ ] 05. `internal/registry`: manifest validation, immutable versions/configs, installs per tenant, catalog visibility, pin/drain/promote/rollback; skills. Проверка: version pinning, install isolation, state compatibility.
- [ ] 06. `internal/gateway`: внешний/внутренний A2A, message idempotency scoped by caller+target, owned contexts, task tree limits, durable delivery/status_version, SSE replay, cancel. Проверка: три агента, цикл, fan-out, потерянное событие и replay.
- [ ] 07. `internal/model`: маршруты, durable generation/stream, atomic parent/child reservations, provider calls, label policy, cancellation, usage. Проверка: конкурирующий бюджет и реальный Qwen вызов через gateway.
- [ ] 08. `internal/tools`: MCP connector transport, Vault/OAuth/RFC8693, approval digest только с human JWT, side-effect ledger и guard, independent Guardian. Проверка: нельзя подтвердить агентским токеном, секреты не попадают в ответы.
- [ ] 09. `internal/artifacts`: S3 presigned upload, seal hash/version/labels, derivation readiness, proxy download with current rights, precise ArtifactRef. Проверка: реальный S3, revocation, upload mismatch, download after Compute sleep.
- [ ] 10. `internal/workspace`: base_rev/CoW candidates, checks, expected_head CAS, nonoverlap merge/recheck, conflict branches. Проверка: конкурентная публикация и приватные файлы.
- [ ] 11. `internal/memory`: scoped proposals, provenance/evidence, review, dedup/search, labels and lifecycle. Проверка: user/workspace/agent isolation, private fact denied from shared sink.
- [ ] 12. `internal/compute`, `cmd/compute-host`: ephemeral/project/personal, incarnation/volume fencing, commands/PTY/attach, activity leases, snapshots, package rollback, export; Firecracker/Kata backend и отдельный Linux supervisor. Проверка: реальные Linux/KVM сценарии; host simulation отдельно.
- [ ] 13. `internal/triggers`: schedules/webhooks, delegation refs, durable fire idempotency, revoke and proactive limits.
- [ ] 14. `internal/space`: workspace/profile/admin и Run Coordinator поверх API; AcceptTurn/lease/intent/checkpoint/inbox/HITL/messages; runner и a2a executors, history owned/pushed, context reseed и deletion.
- [ ] 15. `internal/core`, `cmd/as-runner`: реальный model/tool loop, context/skills/compaction, bounded steps, durable handoff/resume, steering, full cancellation. Тот же Go core используется шаблоном custom agent. Совместимость с упомянутым в PDF Core Agent/workflow.py требует доступного исходника и отдельной проверки; самописный цикл нельзя объявлять проверенной адаптацией того upstream.
- [ ] 16. `sdk/go`, `sdk/python`, `examples`: A2A server/client, service clients, stable intents; AHPCheckpointSaver/AHPStore, LangGraph interrupt/resume и pending writes; два взаимодействующих custom deployment.
- [ ] 17. `web`: пользовательский чат/stream/files/Computer/approvals/connectors, developer deploy UI, tenant admin. Проверка браузером двух субъектов и полного пользовательского пути.
- [ ] 18. `cmd/ahp`, `infra`, `scripts`, `package.json`: runnable service wiring, fresh Compose resources, migrations, TLS/workload identity, deployment controller/manifests, Tier 0/1/2A/2B, OTel and evals. `npm run check`, `npm run smoke`, отдельный live E2E.
- [ ] 19. `tests/e2e`, `tests/chaos`, `tests/load`: все corner cases раздела 9; upload→model→command→artifact→download; два сотрудника, три деплоя, restart, unknown, cancellation, publish conflicts, subject deletion. Реальные измерения latency/RSS и границы проверенной нагрузки.

## Решения оркестратора

- Первый State API driver — единый HTTP-сервис с PostgreSQL: PDF прямо допускает один driver для PoC. Контракт сохраняет возможность второго driver; полная матрица отмечает его отдельно.
- Сервисные модули сначала собираются общим Go module, но запускаются отдельными ролями; это не даёт Agents Space права обходить API.
- Новая реализация не наследует результаты прежних тестов, данные, контейнеры или секреты. Явное исключение пользователя — чтение двух настроек Qwen для исполнителей.
- Дополнение пользователя 29.10.2026: получить образ Core Agent Эмиля из GitLab, ветка `mcp_auth`. Выполнен новый checkout и сборка исходного образа, см. [provenance](emil-core-image.md). Образ и исходник Emil ветки `mcp_auth` — МАТЕРИАЛ ДЛЯ АНАЛИЗА И ДОРАБОТКИ Core Agent под архитектуру PDF rev3.2, а не обязательная внешняя сторона продукта: имя автора, исходный тег/порт контейнера (:18000) и неизменность upstream НЕ являются продуктовыми требованиями или критерием готовности (прямое уточнение пользователя 30.10.2026). Go остаётся языком новых сервисов AHP. Задача 15 (совместимость его workflow.py со State API / RunCoordinator / AHP gateways) остаётся ОТКРЫТОЙ до gap-analysis; существование образа её не закрывает.
- План выполняется без промежуточных запросов разрешения, согласно прямой инструкции пользователя. Непроверенные внешние runtime gates остаются явно открытыми.
