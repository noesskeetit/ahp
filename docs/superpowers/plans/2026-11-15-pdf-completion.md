# Завершение проверок AHP и личного Dot по PDF

> **For agentic workers:** Execute inline with superpowers:executing-plans. Пользователь прямо поручил автономно допроверить и доработать платформу; повторное утверждение подхода не требуется.

**Goal:** Закрыть доступные на Mac функциональные пробелы и недостающие доказательства требований PDF, сохранив различие между реализацией, лабораторным тестом и собранным стендом.

**Architecture:** Продолжать Go-платформу, её owner API и Go/Python SDK. Личный Hindsight backend коллеги сохраняет personal/private-project semantics. Общая память AHP рассматривается отдельно от этих личных областей. Реальные PostgreSQL, Redis, MinIO, Vault и upstream Hindsight; физический Compute и облачная authority остаются в разрешённых профилях mock/synthetic.

**Tech Stack:** Go 1.26.2, Python SDK/LangGraph 0.6.11, PostgreSQL/Redis/MinIO/Vault/Hindsight, Chromium, локальная Lima/Docker на Mac.

**Spec:** Пользовательский `ahp-agents-space-architecture (2) (1).pdf`, rev3.2, 64 страницы, SHA256 `044e045b73b7c34b642bd3c9eb5701b93eed470873a1e0a44e8b1d024821a7e2`. Сверка: PDF §§2.1–2.5, 3–4, 6–9, 11, 13. Базовый рабочий срез `7fc1451b8cfebb1038612b73ad43886ca775e309`; прежние доказательства не перезаписываются.

## Global Constraints

- Только Mac; одна тяжёлая suite, непрерывные guards RAM/swap/диска.
- Только собственные ahp-handbook ресурсы и проверенные owner labels. Тестовая, deployed и destructive-fixture БД различаются.
- Secret values, raw transcripts и private logs не попадают в git/публичные отчёты. Существующий разрешённый AI Factory ключ — только reference/Vault.
- State хранит непрозрачные native данные; возобновление графа — обязанность runtime/адаптера. Изменение одного scope не расширяет соседний.
- Физическая microVM не обязательна для прототипа по решению пользователя. Не выдавать mock за реальную изоляцию.
- Все результаты привязаны к source/runtime SHA; PASS процесса не заменяет native verdict и проверку содержимого.
- Не менять правила личной памяти коллеги для получения общих workspace/agent scopes.
- Не обращаться к чужим Kubernetes contexts, контейнерам, банкам памяти или данным. Не выполнять GitLab push в рамках локальной приёмки.

## Review Focus

- Сбой после внешнего эффекта и до ACK: повторное исполнение недопустимо без сверки.
- Shared workspace: private факт не появляется у второго сотрудника через память, commit или следующий ход; отзыв действует на текущий запрос.
- Плановый GC: живые ссылки/hold/retention защищают байты; недоступный owner запрещает сборку.
- Реальный контейнерный SDK runtime: restart, interrupt, inbox и новая epoch проверяются вне процесса, а не только сериализацией.
- Неподписанный/неоценённый артефакт не допускается в контролируемый релиз; rollback не меняет начатую задачу.

## Task 1: Проверки State/SDK с настоящим LangGraph-процессом

**Files:** `tests/space/recovery_process_realstate_test.go`, `sdk/python/ahp_state/agent.py`, `checkpoint_saver.py`, `effects.py`, `tests/space/*realstate*`.

**Interfaces:** actual shipped Python runtime ↔ mTLS State/Identity/Coordinator; внешний HTTP effect fixture deliberately non-idempotent. Модель здесь не подменяет выполнение графа; отсутствие LLM E2E отмечается отдельно.

- [ ] Прочитать существующий process-recovery сценарий, установить явные gates и сверить pinned interpreter.
- [ ] Запустить `go test -json -race -p=1 -count=1 -tags ahp_exec,ahp_data,state_integration ./tests/space -run '^TestSDKProcessRecovery_'` с реальными PG и `AHP_RECOVERY_PYTHON`.
- [ ] При дефекте сохранить RED, исправить причину и повторить профильные тесты.
- [ ] Подтвердить lost ACK + SIGKILL + plane restart → UNKNOWN, explicit reconciliation, effect count=1; HITL → expired/foreign/revoked refusals.
- [ ] Запустить остальные релевантные `state_integration` проверки; затем контейнерный/реальный model путь адаптера.

Expected: native процессы продолжили нужные границы; старый writer отказан; неизвестный исход не превратился в разрешение на повтор.

## Task 2: Workspace/Artifacts и два сотрудника

**Files:** `internal/workspace`, `internal/artifacts`, `internal/space/ui_*`, `web/aifactory/ahp-live.js`, `tests/workspace-lifecycle`, новый deployed browser сценарий в private evidence control.

**Interfaces:** human UI ↔ Workspace/Artifacts APIs ↔ реальный PG/MinIO; membership и CAS — у owner.

- [ ] Прогнать Create → upload → candidate → verify → publish → read/download двумя существующими demo-сотрудниками.
- [ ] Параллельные кандидаты одного head: один publish, другой conflict; существующий head и файлы не портятся.
- [ ] Удаление участника немедленно запрещает последующие reads/downloads.
- [ ] Исправлять обнаруженные source дефекты через воспроизводимый RED→GREEN.

Expected: точные bytes/SHA, прозрачный conflict и строгий отзыв доступа, без глобальных ошибок интерфейса.

## Task 3: Постоянная lifecycle/retention очистка

**Files:** `cmd/ahp/mount_gc.go`, `internal/artifacts/gc*`, `internal/state/common_retention*`, `tests/dote2e/live_operational*` и owner configuration.

- [ ] Проверить существующие mount/scheduler tests и реальные API lifecycle legs, включая upload-key release.
- [ ] На отдельном собственном tenant создать expired unreferenced artifact, живую ссылку, legal hold и retention floor.
- [ ] Включить scheduler с явным allowlist tenant; дождаться фактического удаления строки и версии объекта только у garbage-кандидата.
- [ ] Подтвердить отсутствие overlap и bounded drain; остановка/недоступность owner не разрешает удаление.
- [ ] Оставить на стенде рабочую ограниченную конфигурацию scheduler и её доказательства.

Expected: автоматическая очистка, не ручной RunOnce; защищённые данные остались доступны.

## Task 4: Приёмка релизов и deploy controller

**Files:** `internal/registry`, `internal/deploycontroller`, `cmd/ahp-deploy`, operator PKI/config fixtures, собственный локальный Kubernetes при наличии ресурсов.

- [ ] Разобрать действующий signed admission/SBOM/evals контракт и причины выключенного режима.
- [ ] Прогнать admit/refuse на настоящем собранном артефакте с контролируемой PKI, без ослабления verifier.
- [ ] Проверить promote/rollback и pin начатого запуска на его image/config.
- [ ] Проверить контроллер на собственном локальном Kubernetes; target/API fixtures не маркировать настоящим rollout.

Expected: traceable source→image→admission→deployment; неподписанный/неоценённый кандидат отказан.

## Task 5: Остальные функциональные расхождения PDF

**Files:** `internal/memory`, `cmd/ahp/mount_hindsight.go`, `internal/space`, `tests/dote2e/interlocutor_and_recovery_test.go`, SDK adapters.

- [ ] Разделить private native Memory и требуемые общие user/workspace/agent API; выбрать явный совместимый дополнительный профиль, не расширяя private банк.
- [ ] Проверить review/labels/membership и сценарий утечки через память/commit/следующий ход.
- [ ] Проверить адресуемый каталоговый агент и versioned runtime contract; закрыть недостающие необходимые API/UX пути.
- [ ] Проверить второй разрешённый Dot deployment и ключ personal Computer с realm на mock substrate.
- [ ] Проверить external UNKNOWN guard через owner Gateway после рестарта и следующего хода, с реальным контролируемым внешним эффектом.

Expected: каждый открытый пункт имеет явную реализацию и proof либо конкретное объективное ограничение; последний не считается готовым.

## Task 6: Повторная приёмка и профиль производительности

**Files:** `docs/coverage.md`, новые PDF requirements rows, C4 reports/verification/manifests; `profiling/` при необходимости.

- [ ] Составить requirement-level матрицу PDF с source/runtime/version/proof и отдельными статусами IMPLEMENTED / VERIFIED / OPEN / PERMITTED_MOCK.
- [ ] Собрать чистые frozen binaries; прогнать нужные deployed/native/browser cases и `npm run check` / `npm run smoke` без исключений и снятия обязательных gates.
- [ ] Выполнить bounded profiling именно этой сборки: фиксированные ресурсы, одинаковый workload, число активных и завершённых turns, ошибки, latency, CPU/RSS/PG/queue.
- [ ] Проверить отрицательные сценарии, bytes/SHA, auth boundaries; не применять прежние capacity цифры к новой сборке.
- [ ] Проверить отсутствие секретов и целостность доказательств; обновить читаемые MD/HTML в Downloads.

Expected: критерии прототипа PDF подтверждены воспроизводимо. Цель завершается только после фактического закрытия обязательной работы; неизвестные production/HA свойства не становятся PASS из-за истечения времени.
