# Доведение AHP до требований PDF rev3.2

**Уточнение пользователя от 03.11.2026:** текущая приёмка — AHP с полноценными Python и Go SDK; Dot личный, продуктовая доработка Agent Space отложена. [Решение и границы](../../decisions/2026-11-03-ahp-first-personal-dot.md) имеют приоритет над первоначальным совместным планом ниже. Шаг 4 и AS UX в других шагах не являются блокерами AHP. Отложенное не считается выполненным.

Основание: выбранный заказчиком PDF rev3.2 (64 страницы), опубликованный реестр 139 требований, прямое поручение «ну делай». База исходников 7c810ec2aa5d0d650205807f6d0e12489fd0fe62. Этот план не заменяет PDF и не добавляет требований из сторонних документов.

## Общие ограничения

- Backend Go; полноценные Python и Go SDK, LangGraph — отдельный адаптер. Реальные microVM остаются явно обозначенными MOCK по решению заказчика. Дополнение заказчика 02.11: внешние Cloud IAM/KMS/BYOK допускаются на mocks/синтетическом контуре; см. docs/poc-exceptions.md.
- Не использовать чужие проекты/данные/контейнеры. Только ресурсы AHP ahp-handbook. Никаких reset/clean/prune/down/remove-orphans.
- Секреты, PDF и сырые логи не входят в Git. Отчёты содержат команды, hash, результат и предел проверки без credentials.
- Каждому исполнителю отдельная worktree и owned paths. Новые native исполнители разрешены более поздним прямым запросом заказчика; старое ограничение только Qwen не применяется к этому поручению.
- Тестировать инварианты на реальных PG/Redis/MinIO. Отсутствие DSN/SKIP не является PASS. Fixture, compile и scoped тест не являются настоящим LLM E2E.
- Строгие owner API, tenant/subject/deployment scope, labels, lease/fencing, digest/idempotency, UNKNOWN semantics сохраняются.
- Текущее состояние всей платформы не принято. Baseline full Linux check имеет timing failure TestB148Sit2_CostComparison; исправить причину проверки, не скрывать её SKIP или ослаблением инварианта.
- Уже опубликованные репозитории не перезаписывать: перед каждой публикацией учитывать remote commits коллеги, особенно Memory Service. Только normal push, не force.

## Порядок и границы работ

### 1. API-only AS и Artifacts client (ARC-02)
Owned: cmd/as-runner, sdk/go/artifacts (новый), internal/modelapi, минимальные изменения Model-owned budget API/treedelegate. Не менять internal/artifacts/api, Core и SDK State без согласования с владельцем.
Убрать PG/MinIO/Model ledger из runner. Артефакты создаются BeginUpload/PUT/Seal через API, чтение через Download API; права берутся из проверенного task token. Tree budget остаётся атомарным у Model owner и не обходится новыми локальными счётчиками.
Проверка: настоящий PG/MinIO+HTTP/mTLS, создание и скачивание byte-equal файла, чужой subject/context denied, revoked token denied, child reserve/concurrency/replay не увеличивают общий бюджет; runner работает без storage credentials.

### 2. Большие данные в SDK и Core (ST-13, TL-06, AS-10)
Owned: sdk/go/state, sdk/python/ahp_state, pkg/sdk, internal/core, необходимые только payload helper файлы pkg/contracts; отдельные tests SDK/Core.
Автоматический spill/hydration checkpoint значений, большие tool outputs и final outputs через Artifacts. Малые данные и существующие сериализованные checkpoints остаются совместимыми. Превью ограничено, read_output_chunk проверяет текущие права. Не повышать лимит State, не терять metadata/pending writes и не повторять неизвестный внешний effect.
Общий Artifacts HTTP client принадлежит задаче 1; согласовать интерфейс сразу и использовать его, не заводить второй прямой store.
Проверка: boundary/large round-trip, рестарт, digest/size mismatch, чужой доступ/отзыв, сохранение serializer metadata, no double tool dispatch, реальная PG/HTTP интеграция и LangGraph saver.

### 3. Обработка документов и direct upload (AR-02/03/04)
Owned: internal/artifacts, internal/artifactsapi, cmd/artifact-parser (новый при необходимости), изолированные ingestion tests, browser upload файлы internal/space/frontend. Shared cmd/ahp wiring согласовать, отдельный новый mount/helper предпочтителен.
Существующий Derivations/Readiness развить в реальные content/extraction/search/quarantine состояния. Обработчик извлечения запускается изолированно с ограничениями; cache tenant+SHA, chunk/context budget и fail-closed quarantine. Presigned PUT из браузера, seal на сервере, без передачи credentials и без публичного download URL.
Проверка: реальные файлы/PG/MinIO, невалидный и слишком большой документ, карантин, tenant cache isolation, parser timeout/error, byte SHA, browser direct-upload path и отрицательная авторизация.

### 4. Продуктовые пути AS (AS-02/03/04/09/13) — отложено 03.11

Следующий абзац сохраняет прежний объём для истории. Общий Dot исключён из текущей модели; новые общие чаты/приглашения и прочая продуктовая работа Agent Space не выполняются в текущем AHP потоке. Уже собранные profile/admin изменения сохраняются.
После интеграции файловых изменений: profile overlay+skills/runtime pin; admin management API/UI; приглашение/принятие/отзыв; project-owned chat с автором каждого хода и audience gates. Сначала согласовать контракт State/Identity/Workspace, не изображать общий чат снятием subject filter.
Проверка двух пользователей: членство, потеря членства, личные credentials не наследуются, старый thread/history, неучастник denied, изменения профиля влияют только на новые закреплённые runs.

### 5. Допуск и развёртывание агентов (SDK/REG)
Обязательны оба SDK: Python и Go, с общей таблицей покрытия публичных контрактов и одинаковыми сценариями авторизации, ошибок, streaming, отмены, больших данных, повторов и восстановления. LangGraph adapter не заменяет framework-neutral Python SDK. Проверки и исполняемые примеры работают через AHP API без Agent Space. Исследование дополнительных framework adapters не является утверждением их готовности.

Conformance/startup gates, SBOM, image signature verification, stage eval/chaos/security promotion, tenant approval и полный custom deployment lifecycle. Использовать проверенные upstream зависимости; не придумывать свою криптографию.
Проверка реального OCI image: signed accepted, wrong digest/signature denied, fail eval blocks promotion, A/B/A history preserved.

### 6. Инфраструктура и безопасность (ID/SE/TR/ST-14)
Подключить существующие OIDC/JWKS и RFC8693 adapters, ресурсный IAM adapter с fail-closed авторизацией; mesh STRICT policies; KMS/BYOK provisioning. Для согласованной PoC-приёмки использовать синтетический IAM и локальные тестовые ключи, проверяя решения, отзыв, изоляцию и криптографические операции; реальный Cloud.ru контур не является блокером. DB proxy и opt-in VPC relay в применимых профилях. Не объявлять fixture реальной корпоративной integration.
Проверка реального доступного профиля: workload identity, denied bypass, revoke/cache invalidation, recovery на независимом failure domain при наличии среды. Недоступный внешний ресурс фиксировать точно, продолжая другие работы.

### 7. Computer/Browser при MOCK VM (CP/SE-08)
Платформенные API/SDK потоки browser realm/downloads, personal runtime, scale-to-zero, команды, окружение/revision/rollback. Доработка экранов Agent Space отложена. Не расширять исключение mock на пропуск платформенных контрактов.

### 8. Memory/Workspace остаток (ME/WS)
Полнота SecretFirewall/provenance/review, применение памяти в агентском сценарии; Workspace nonoverlap merge→CI и conflict branch. Сначала сверить опубликованный Memory remote с работой коллеги, не переписывать его backend.

Изменения коллеги feature/hindsight-memory получены в семи отдельных репозиториях. Его модель личной и личной проектной памяти — продуктовая основа. Проверять Memory через самостоятельный агент и Python/Go SDK; Agent Space preload/UI не блокирует AHP. Наличие участников Workspace по-прежнему не даёт права публиковать личную память.

### 9. Общая сборка и развёртывание (AC-07/OB)
Интегрировать принятые commits в одну ветку. Исправить timing-тест корректным детерминированным инвариантом и сохранить смысл проверки. Полный npm run check на Linux+реальных зависимостях, затем smoke и один immutable deployment: самостоятельный custom agent через AHP, оба SDK, files, OAuth, Memory, удаления, crash/recovery, trace collector. Agent Space не является обязательным клиентом приёмки; browser используется там, где платформенный контракт требует человеческого OAuth/consent. Не перезапускать основной стенд до готового проверяемого candidate.

### 10. Продуктовая нагрузка и матрица (AC)
Измерить ACK/queue→send/cancel/revoke/hop p95, конкуренцию и RSS, деградацию под ограничениями. Availability требует явного периода. Расчётные 10k users не считать достигнутой ёмкостью.
Обновить реестр только по exact accepted scope, разделить code complete / configured / verified / external blocked. Передать source в посервисные GitLab repos с учётом чужих commits и secret scan.

Отдельные разделы реестра: AHP, Python SDK, Go SDK, отложенный Agent Space, исследования. Общие чаты и приглашения не учитываются как текущие блокеры AHP; изменение знаменателя не выдаётся за новый процент выполненных требований PDF.

## Приёмка и продолжение

Каждый блок: воспроизводимый поведенческий тест → изменение → покрывающие проверки → независимый review по diff → устранение существенных замечаний → интеграция. Успех первой волны не является основанием остановить остальные задачи текущего AHP объёма. Принимать AHP и оба SDK по обновлённой границе; общую полноту исходного PDF и Agent Space не объявлять. Исследования harnesses начинать по готовности нужного контракта без ожидания завершения Agent Space.
