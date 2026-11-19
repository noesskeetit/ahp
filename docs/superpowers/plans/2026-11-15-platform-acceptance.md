# AHP/Dots: завершение остаточной приёмки

> **For agentic workers:** Execute inline with superpowers:executing-plans. Пользователь уже выбрал автономное исполнение; повторное согласование плана не требуется.

**Goal:** Закрыть функциональные пробелы AHP/Dots и проверить рабочий прототип по PDF, включая настоящий Memory и сквозные пользовательские сценарии.

**Architecture:** Сохранить действующий Go-продукт и его границы owner API. Новый кандидат использует собственный Hindsight API/PG и отдельные tenant/порты. Процессы предыдущего кандидата остановлены: разные конфигурации не должны одновременно разбирать одну очередь. Его данные, бинарники и результаты сохранены. Unit/integration-набор использует отдельную PostgreSQL-БД; deployed-сценарии — БД рабочего кандидата.

**Tech Stack:** Go 1.26.2, Go/Python SDK, PostgreSQL/Redis/MinIO/Vault, upstream Hindsight, Chromium, локальная Linux VM на Mac.

**Spec:** Пользовательский PDF rev3.2, SHA256 044e045b73b7c34b642bd3c9eb5701b93eed470873a1e0a44e8b1d024821a7e2. Базовый source 834531c83be84e19dd6f5072dfba564ba2e47851; прошлые результаты не переобъявляются новой приёмкой.

## Global Constraints

- Только Mac; одна тяжёлая suite и контроль RAM/swap/диска.
- Ресурсы только ahp-handbook с проверенными ownership labels. Личный Hindsight и чужие контейнеры/данные не используются для тестов.
- AI Factory credential разрешён пользователем; значения не печатать, не коммитить, исходный env не менять.
- Physical microVM mock и синтетические Cloud IAM/KMS разрешены; соответствующие гарантии помечаются явно.
- Новые требования из ADR/документации upstream не добавляются к PDF; они служат справкой реализации.
- SKIP и BLOCKED не равны приёмке. Go exit code дополняется native evidence verdict.

## Review Focus

- Чужой tenant/subject/context не читает память, файлы и задачи; отзыв действует во время исполнения.
- Retain queued подтверждает только постановку; ждём извлечённых фактов и проверяем смысл recall.
- После restart сохраняются подтверждённые данные, внешнее действие не повторяется вслепую.
- Browser действительно получает поток/файл; проверяются bytes/SHA и запреты доступа.
- State и owner API остаются владельцами данных, runtime не обращается к их БД напрямую.

## Task 1: Полнота требований и окружения

**Files:** C3/private/architecture.txt; C3/requirements-matrix.json; C3/progress.md; данный план.

- [x] Повторно проверить PDF hash и извлечь текст вне git.
- [ ] Для каждого сервиса/раздела связать требования с текущим кодом и конкретным proof; отделить несовпадение от отсутствия проверки.
- [ ] Классифицировать 49 старых SKIP: обязательное, отдельно проверенное, разрешённый mock, optional benchmark.

## Task 2: Настоящий Memory

**Files:** infra/memory/compose.hindsight.yaml; internal/memory/hindsight/*; internal/memory/ahpbridge/*; cmd/ahp/memory*; sdk/go/memory; sdk/python/ahp_memory; C3/native_memory.py.

- [x] Зафиксировать RED при явно включённых native зависимостях, не SKIP.
- [x] Поднять проверенный upstream image по digest, отдельную PG и approved модель; ограничить CPU/RAM, native API не публиковать наружу.
- [x] Выполнить native retain/status/list/recall/delete и owner API сценарии, исправить реальные дефекты через RED→GREEN.
- [x] Выполнить TestHindsightNativeAgentAuthority: настоящие Go/Python SDK, mTLS, ACL, отзыв, State completion outbox.
- [x] Сохранить точную native/model/image provenance и не использовать личные банки.

## Task 3: Сквозной работающий кандидат

**Files:** tests/dote2e/*; tests/registry-triggers/*; tests/workspace-lifecycle/*; internal/artifactsapi/browser_upload_test.go; cmd/ahp/registry_profile_browser_test.go; C3/deployed.py.

- [x] Собрать чистые 4 бинарника и поднять новый tenant на отдельных портах с Hindsight.
- [ ] Выполнить Go/Python custom agent, Dot grant/config/history, HITL approve/cancel, delegation, memory, файлы и restart.
- [x] Проверить браузерный direct upload и profile ACL на реальном Chromium/PG/MinIO.
- [ ] Проверить live Tool Gateway, mounted A2A/catalog и Workspace lifecycle.
- [ ] Исправить обнаруженные продуктовые пробелы, сохранив отрицательные сценарии; native verdict должен соответствовать фактам.

## Task 4: Итоговый контроль

**Files:** docs/coverage.md; C3/RESULTS.json; C3/VERIFICATION.json; C3/REPORT.md/html.

- [ ] Проверить оставшиеся обязательные PDF пути; профиль mock/production пререквизитов указывать на уровне требований.
- [ ] npm run check на чистом source, без исключений пакетов и скрытого снятия обязательных gates; npm run smoke запущенного кандидата.
- [ ] Сверить матрицу PDF с code/runtime evidence. Проверить отсутствие секретов и hashes.
- [ ] Выдать краткий результат и читаемый отчёт, не заменяя результат подсчётом unit-тестов.
