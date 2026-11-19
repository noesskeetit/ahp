# Матрица покрытия: архитектура → задача → проверка

> **Историческая матрица baseline октября.** Статусы и блокировки ниже относятся к указанным в строках версиям, а не к текущей сборке. Наличие строки `mapped` подтверждает сопоставление раздела PDF, но не выполнение требования. Текущие SDK-проверки и их ограничения: [conformance](sdk/conformance.md), [native ownership](superpowers/plans/2026-11-16-native-sdk-ownership.md), [shared ingress](superpowers/plans/2026-11-16-native-sdk-ingress.md). Полная приёмка PDF остаётся открытой; актуальная сервисная матрица и immutable receipts ведутся в отдельном пакете локальной приёмки.

Статусы: NOT_VERIFIED (не реализовано / не проверено). Основание: docs/implementation.md (задачи 01–19), спецификация §1–14.

### Легенда статусов (delivery integration, 2026-10-29; baseline go-platform 8b14a59)
- **NOT_VERIFIED** — не реализовано / не проверено.
- **L1_realized** — артефакт есть в исходниках (манифесты/код), но НЕ развёрнут на целевом рантайме.
- **L2_local_test** — проверено локальным прогоном в изолированной БД `ahp_hive_delivery` (compose/chaos/e2e/smoke/load); **source ≠ deployment**.
- **L4_target_runtime** — доказано на целевом окружении (K8s/HA/реальные провайдеры/live-Qwen). **Все L4 — OPEN.**
- **owner-committed** — owner-блок (identity `a65ebc1`, state `461aa2c`, compute `27cd438`) закоммичен и проинтегрирован в delivery worktree; byte-identity доказана (91 файлов: 87 byte-identical, 4 = общие `.gitignore`/`go.mod` RFC-мержи, 0 missing). Бывший `BLOCKED-ON-STATE-WIP` снят.
- Live-Qwen-E2E = NOT_RUN (G-LLM-QWEN). Проверки State ниже опираются на imported WIP соседа.

## Замыкание «каждый раздел PDF → evidence» (acceptance #2; PDF rev 3.2, sha `044e045b…a7e2`)

Полный СОДЕРЖАНИЕ PDF (64 стр.) перечислен ниже; каждый раздел указывает, где он
разобран в этой матрице. Это закрывает буквальный гейт «каждый раздел PDF mapped to
evidence» — включая вводные разделы, которые раньше не имели явной строки.

| PDF-раздел (СОДЕРЖАНИЕ) | с. | Разбор в этой матрице | Статус замыкания |
|---|---|---|---|
| Коротко (exec-резюме, тезисы 1–12) | 4 | тезис→свой раздел (§1–§11): тезисы 1–2→§1–2, 3→§3, 4→§4, 5→§2/§8, 6→§5, 7→§6, 8–9→§4/§compute, 10→§8, 11→§10–11, 12→§13–14 | mapped (резюме, не самостоятельные требования) |
| Атлас схем (рис. 1–21) | 5–26 | рисунки иллюстрируют §1–§8.3; несущие продуктово: рис.1→§2(gateway/A2A), рис.4→§1(разъём деплоя), рис.14→§5(мультиагент), рис.16→§6(контракты), рис.17→§8.3(изоляция), рис.18→§4.4(стык агента), рис.20→§2.5(State API), рис.21→§7(данные) | mapped (домашние строки разделов; source≠deployment) |
| 1. Два слоя и граница между ними | 27 | §1–2 | ✓ строка §1–2 |
| 2. Слой 1: AHP (12 сервисов) | 28 | §1–2 | ✓ строка §1–2 |
| 2.5 State API (единый контракт) | 25,28–33 | §2.5 | ✓ строка §2.5 |
| 3. Agents Space — распределённый harness | 34 | §3 | ✓ строка §3 |
| 4. Кастомные агенты на AHP (4.4 стык) | 38 | §4 | ✓ строка §4 |
| 5. Мультиагентные системы (A/B/C/D) | 42 | §5 | ✓ строка §5 |
| 6. Общие контракты (A2A/грант/дерево задач) | 45 | §6 | ✓ строка §6 |
| 7. Данные: файлы, память, версии | 47 | §7 | ✓ строка §7 |
| 8. Безопасность (8.3 изоляция) | 48 | §8 | ✓ строка §8 |
| 9. Корнер-кейсы (~70 ситуаций) | 51 | §9 (все строки) | ✓ строка §9 |
| 10. Уровни поставки Tier 0/1/2A/2B | 56 | §10–11 | ✓ строка §10–11 |
| 11. Ёмкость и стоимость | 57 | §10–11 | ✓ строка §10–11 |
| 12. Как сведены источники | 57 | §12, Прил.А/Б | ✓ мета |
| 13. План | 59 | §13–14 | ✓ мета/дорожная карта |
| 14. Риски и открытые вопросы | 60 | §13–14 | ✓ мета/открытые гейты |
| Приложение А. Соответствие имён | 61 | §12 | ✓ мета/справочник |
| Приложение Б. Словарь | 62 | §12 | ✓ мета/справочник |

Замечание: «Коротко» и «Атлас схем» — производные (резюме и иллюстрации), самостоятельных
продуктовых требований не вводят; каждый их тезис/рисунок замкнут на домашнюю строку
раздела со своим статусом L1/L2/L4/owner-committed. L4 для всех целевых рантаймов OPEN.

### Проверяемость замыкания (репроducible gate, acceptance #2)

Замыкание «каждый раздел PDF → evidence» выше — не ручное утверждение, а машинально
проверяемый гейт: `scripts/verify-pdf-closure.mjs` извлекает верхнеуровневые разделы из
собственного СОДЕРЖАНИЕ канонического PDF и падает, если любой из них не упомянут в этом
файле. PDF в Git НЕ кладётся (политика); текст получают вне репозитория.

```
# текст PDF вне git (sha НЕ выводится; сверка по --expect-sha256 опциональна)
pdftotext -layout "$AHP_PDF_PATH" /tmp/ahp-pdf.txt
node scripts/verify-pdf-closure.mjs --text /tmp/ahp-pdf.txt \
     --coverage docs/coverage.md --pdf "$AHP_PDF_PATH" \
     --expect-sha256 044e045b73b7c34b642bd3c9eb5701b93eed470873a1e0a44e8b1d024821a7e2
# или: npm run verify:pdf-closure  (env AHP_PDF_TEXT/AHP_PDF_PATH/AHP_PDF_SHA256)
```

Факт прогона 2026-10-30 (isolated scratch): `shasum` PDF == канонический
`044e045b…a7e2`; СОДЕРЖАНИЕ дало **18** верхнеуровневых разделов (1–14 + «Коротко» +
«Атлас схем» + Прил.А + Прил.Б) → **PDF_CLOSURE=PASS sections=18** (rc=0). Негатив-тест:
удаление строки раздела из копии coverage.md → `PDF_CLOSURE=FAIL unmapped=1` (rc=1), значит
гейт не no-op. Неверный sha → rc=2 (integrity). Статусы L1/L2/L4 замыкания не меняются —
гейт доказывает ПОЛНОТУ перечня разделов, а не их продуктовую верификацию.

## §1–2 Слои, разъём деплоя, классы, контракт

| § | Требование | Задача | Проверка | Статус |
|---|---|---|---|---|
| 1 | Три контакта разъёма (A2A/API/State), нет прямого ingress | 06,16 | e2e: обход gateway невозможен | NOT_VERIFIED |
| 1 | AHP не видит шагов; граница доверия = деплой | 06,05 | изоляция областей State | NOT_VERIFIED |
| 1 | Agents Space = обычный деплой, без доступа к БД AHP | 14,18 | NetworkPolicy/смотка | NOT_VERIFIED |
| 2.1 | 12 сервисов AHP, своя схема, один API записи | 18 | wiring+миграции | NOT_VERIFIED |
| 2.2 | Класс S: свой пул, токен на ход, профиль personal | 12,18 | runtime-пул (нет KVM) | NOT_VERIFIED |
| 2.2 | Класс C: namespace, NetworkPolicy, readOnlyRootFS | 05,18 | манифест-deploy | NOT_VERIFIED |
| 2.2 | Класс X: microVM, supervisor вне гостя, egress-прокси | 12 | Linux/KVM — нет стенда | NOT_VERIFIED |
| 2.3 | Пункты 1–4,8 контракта (принудительно) | 06,03,08 | негативные e2e | NOT_VERIFIED |
| 2.3 | Пункты 5,6,9,10 (State API/SDK) | 02,16 | конформанс State | NOT_VERIFIED |
| 2.4 | Грант/токен: aud, SPIFFE-привязка, перевыпуск, отзыв | 01,03 | mTLS+azp mismatch | L2_local_test (mTLS self-spawn + SPIFFE-привязка в task_token; negative e2e); L4 OPEN |
| 2.4 | Ключ операции: digest, KEY_REUSE_MISMATCH, Get/Fence | 04 | конкурентный dispatch | L2_local_test (chaos op-key-reuse 10/10 на боевом PG: KEY_REUSE_MISMATCH отклонён — тот же op-key+другой payload→400, corruption impossible (GetKV вернул исходный {v:1}, {v:999} не персистился), fence-before-key (stale-epoch→409 STALE_EPOCH), distinct op-key→version=2, идемпотентный replay→same version; FINDING: pkg/transport/json.go маппит contracts.ErrKeyReuse→HTTP 400 code=INVALID_ARGUMENT, точный KEY_REUSE_MISMATCH не в wire-code); L4 OPEN |
| 2.4 | SIDE_EFFECT_UNKNOWN + guard через следующие задачи | 04,08 | UNKNOWN через ход | L2_local_test (chaos unknown-outcome 13/13: crash без Release→run stays non-terminal/UNKNOWN (не failed/canceled), re-claim того же run, идемпотентный replay без дубля); L4 OPEN |
| 2.4 | Журнал меток task+context, наследование | 03 | наследование/усиление | NOT_VERIFIED |
| 2.4 | Бюджет дерева: резерв/возврат в Model Gateway | 07 | конкурирующий бюджет | NOT_VERIFIED |

## §2.5 State API

> **G-INTEGRATION (open):** независимый integration-validation воспроизвёл wire-несовместимость Space↔State (SDK `AppendStream`/`PutCheckpoint` → HTTP 400 «request body is not valid JSON») на принятом state-блоке `461aa2c`. Это upstream scope владельца state (не дрейф delivery: byte-identity подтверждена). Сквозной live whole-system E2E остаётся OPEN.

| § | Требование | Задача | Проверка | Статус |
|---|---|---|---|---|
| 2.5 | Thread/Run FIFO/Lease/Checkpoint/Writes/Intent/Inbox/Suspend/Stream/KV/Lifecycle | 02 | конформанс-набор | L2_local_test/owner-committed (state-api e2e через imported WIP; зачтено: owner-коммит существует, byte-identical) |
| 2.5 | Атомарные пакеты (outbox не расходится) | 02 | пакет снимок+stream+run | L2_local_test/owner-committed (через owner-committed state snapshot) |
| 2.5 | Гарантии 1–7 (область, эпоха, идемпотентность, порядок, 1МиБ, синхр.реплика, каскад) | 02 | 100 claims, stale epoch, restart | L2_local_test/owner-committed (epoch-fence 8/8; через owner-committed state snapshot) |
| 2.5 | Драйвер А (единый сервис) — выбран PoC | 02 | реальный PostgreSQL | L2_local_test/owner-committed (реальный PostgreSQL ahp_hive_delivery) |
| 2.5 | Драйвер Б (БД деплоя, RLS от SDK) | — | не в объёме задач | NOT_VERIFIED |
| 2.5 | Конформанс для обоих драйверов + миграция областей | — | нет задачи | NOT_VERIFIED |
| 2.5 | Безопасность State API (8 рисков таблицы) | 02,03 | IDOR/логи/админ-маршрут | L2_local_test/owner-committed (403 no-token/404 unknown; через owner-committed state snapshot) |

## §3 Agents Space

| § | Требование | Задача | Проверка | Статус |
|---|---|---|---|---|
| 3.2 | as-web/workspace/profile/admin | 14,17 | браузер, 2 субъекта | NOT_VERIFIED |
| 3.3 | Протокол хода (AcceptTurn…CommitMessage) | 14 | e2e ход | NOT_VERIFIED |
| 3.3 | Квитанции намерений, зомби-раннер, Fence новым раннером | 14,04 | chaos зомби до/после | L2_local_test (chaos unknown-outcome: zombie old-fence PutKV→409 STALE_EPOCH «после перехвата» + re-claim новым worker epoch A=1→B=2, current-fence PutKV→200 = Fence новым раннером); intent-receipts=space lane; L4 OPEN |
| 3.3 | Suspend/пробуждение, сверка детей tasks/get | 14,06 | потерянное событие | NOT_VERIFIED |
| 3.3 | Steering nextTurn/nextStep, отмена с Fence | 14,15 | inbox-гонка | NOT_VERIFIED |
| 3.4 | Понятия→примитивы (Dot, проект, чат, Computer, навык, коннектор) | 14,11,12 | срез | NOT_VERIFIED |
| 3.6 | Computer: writer lease, typed patch hash, rollback пакетов, сон по activity leases, realms | 12 | KVM-стенд — нет | NOT_VERIFIED |
| 3.7 | Публикация PreparePublish→checks→CAS, WORKSPACE_CONFLICT | 10 | конкурентная публикация | NOT_VERIFIED |
| 3.7 | Личный коннектор ≠ общее исполнение; потолок при коммите | 08,10 | приватный файл в коммите | NOT_VERIFIED |

## §4 Кастомные агенты, SDK

| § | Требование | Задача | Проверка | Статус |
|---|---|---|---|---|
| 4.2 | A2A-адаптер, contextId=thread, messageId-идемпотентность | 16 | контрактные тесты | NOT_VERIFIED |
| 4.2 | AHPCheckpointSaver+ durability=sync, pending writes | 16 | рестарт пода посреди хода | NOT_VERIFIED |
| 4.2 | interrupt→input-required, resume, steering, отложенные вызовы | 16 | chaos-тест | NOT_VERIFIED |
| 4.2 | Ключи по task_id+tool_call_id; KEY_REUSE_MISMATCH | 16,04 | повторяющийся tool_call_id | NOT_VERIFIED |
| 4.2 | ahp-sdk check (чужие чекпоинтеры, egress, диск) | 16 | CI-проверка | NOT_VERIFIED |
| 4.3 | Шаблон Core Agent на State API | 15 | требует исходник workflow.py — ЗАВИСИМОСТЬ | NOT_VERIFIED |
| 4.4 | Ход a2a, транскрипт vs чекпоинт, resubscribe с курсора | 14,06 | рестарт координатора в потоке | NOT_VERIFIED |
| 4.4 | history owned/pushed, пересев, каскад удаления, contexts/delete | 14,02,05 | удаление чата в ходу | L2_local_test partial (chaos cancel-delete: DeleteThread→200→GetThread 404 каскад + legalhold→403 + delete-after-remove→200); history/пересев=space lane; L4 OPEN |
| 4.4 | Агент как инструмент Dot: дочерний грант, taint external | 14,03 | делегирование с метками | NOT_VERIFIED |
| 4.5 | Конвейер, pin версии, drain/state.compat, откат, установка в tenant, каталог | 05 | version pinning, install isolation | NOT_VERIFIED |

## §5 Мультиагентность

| § | Требование | Задача | Проверка | Статус |
|---|---|---|---|---|
| 5.2 | Схемы A/B/C/D, запрет общей БД двух деплоев | 05,06,16 | система из 2 деплоев | NOT_VERIFIED |
| 5.3 | Дерево задач: субъект един, act, глубина 4, цикл, fan-out, атомарные счётчики | 06,03 | цикл A>B>A, fan-out сверх лимита | NOT_VERIFIED |
| 5.3 | Отмена=отзыв поддерева; дети через Triggers для фоновой работы | 06,03,13 | отмена корня из 3 агентов | NOT_VERIFIED |
| 5.4 | 3 вида HITL; согласование только с JWT человека, digest | 08,06 | агентский токен отказ | NOT_VERIFIED |
| 5.5 | Общий деплой: область субъекта, state.shared ревью, аудитория personal | 02,05,16 | чужой thread в общем деплое | NOT_VERIFIED |
| 5.6 | Видимость system/tenant/catalog; нет задач в Space от агентов | 05,06 | confused-deputy отказ | NOT_VERIFIED |
| 5.7 | Агенты-конфигурации; фреймворк в Computer (класс X, egress-подстановка) | 14,12 | политика tenant на Model Gateway | NOT_VERIFIED |

## §6 Контракты

| § | Требование | Задача | Проверка | Статус |
|---|---|---|---|---|
| 6.1 | A2A-профиль, статусы, IDEMPOTENCY_MISMATCH, ArtifactRef, Agent Card (history/steering) | 06,16 | контрактные тесты | NOT_VERIFIED |
| 6.2 | Claims JWT, публичный/внутренний входы, IssueTaskToken | 01,03,06 | токены не взаимозаменяемы | NOT_VERIFIED |
| 6.3 | Жизненный цикл ключа, передача ключа MCP, срок хранения | 04,08 | Get чужого деплоя — «не найдено» | NOT_VERIFIED |
| 6.5 | HITL-запросы в Tool GW со сроком | 08 | срок запроса | NOT_VERIFIED |
| 6.6 | Манифест деплоя + deploy controller (ns, SA, NetPol, области, лимиты) | 05,18 | валидация манифеста | L2_local_test (cmd/ahp-deploy --validate-only + fixtures, 6 tests); L4 apply OPEN |
| 6.7 | События at-least-once, status_version, дедуп | 06 | потеря/дубль события | NOT_VERIFIED |
| 6.8 | DeleteSubject, хук own_tables, метки Seal/памяти/коммита, W3C traceparent, allowlist | 02,03,09,18 | удаление сотрудника | L2_local_test (pkg/otel traceparent; partial); L4 OPEN |

## §7 Данные

| § | Требование | Задача | Проверка | Статус |
|---|---|---|---|---|
| 7 | Presigned PUT, Seal, readiness-флаги, изолированные парсеры, Export, прокси Range, read_output_chunk | 09 | реальный S3, отзыв прав | NOT_VERIFIED |
| 7 | Память: scopes, состояния, гейты second-brain, Secret Firewall, гибрид-поиск с двойной проверкой прав | 11 | приватное в scope agent — отказ | NOT_VERIFIED |
| 7 | Workspace: неизменяемые ревизии, retention рабочей копии | 10 | ревизия в использовании | NOT_VERIFIED |

## §8 Безопасность

| § | Требование | Задача | Проверка | Статус |
|---|---|---|---|---|
| 8.1 | Два входа+admin-хост, ExtAuthz/IAM Evaluate, кэш по эпохе гранта | 03,06,18 | live IAM — ЗАВИСИМОСТЬ | NOT_VERIFIED |
| 8.1 | Внешние токены только в Tool GW: RFC8693, OAuth PKCE, Vault ротация, запрет passthrough | 08 | реальный OAuth — нет контура | NOT_VERIFIED |
| 8.1 | Triggers: делегирование, увольнение закрывает запуски | 13 | revoke-fire гонка | NOT_VERIFIED |
| 8.2 | Метки: только понижение потолка, taint, таблица точек записи (6 строк) | 03,07,08,09,10,11 | сценарий утечки руководителя | NOT_VERIFIED |
| 8.3 | Границы таблицы (tenant/deploy/env/издатель/субъект/песочница/человек) | 18 | негативный набор | NOT_VERIFIED |
| 8.4 | Зоны C/S/X, egress allowlist по гранту, SSRF-прокси, DB proxy, L3/L4 по включению | 12,08,18 | KVM/сети — нет стенда | NOT_VERIFIED |
| 8.5 | Требования Cloud.ru (mTLS STRICT, логи без секретов, VMGenID и др.) | 18,12 | mTLS/ИБ-ревью — открыто | L2_local_test (mTLS client self-spawn); L4 K8s STRICT OPEN |

## §9 Корнер-кейсы (все строки)

| § | Кейс | Задача | Статус |
|---|---|---|---|
| 9.1 | Потерян ответ message/send — повтор | 06 | NOT_VERIFIED |
| 9.1 | Тот же messageId от другого вызывающего | 06 | NOT_VERIFIED |
| 9.1 | Тот же messageId, другое содержимое | 06 | NOT_VERIFIED |
| 9.1 | Чужой contextId | 06 | NOT_VERIFIED |
| 9.1 | Два сообщения в один чат одновременно | 02,14,16 | NOT_VERIFIED |
| 9.1 | Токен задачи на публичном / JWT на внутреннем | 01,06 | NOT_VERIFIED |
| 9.2 | Под упал до вызова инструмента | 02,04,16 | L2_local_test/owner-committed (chaos SIGKILL+restart 4/4) |
| 9.2 | Под упал после отправки, до ответа | 04 | L2_local_test/owner-committed (chaos SIGKILL+restart 4/4) |
| 9.2 | Зависший под после перехвата | 02,04 | NOT_VERIFIED |
| 9.2 | Узел вызывает модель и инструмент | 16 (sdk check) | NOT_VERIFIED |
| 9.2 | Повторяющийся tool_call_id | 16,04 | NOT_VERIFIED |
| 9.2 | Узел дольше аренды (heartbeat) | 16 | NOT_VERIFIED |
| 9.2 | State Store недоступен (sync) | 16,02 | L2_local_test/owner-committed (chaos graceful-shutdown 3/3 SIGTERM) |
| 9.2 | Ожидание дольше срока токена | 03 | NOT_VERIFIED |
| 9.2 | Координатор пропустил событие ребёнка | 14,06 | NOT_VERIFIED |
| 9.3 | Выкатка при ожидающих задачах (drain) | 05,06 | NOT_VERIFIED |
| 9.3 | Окно drain истекло / state.compat | 05 | NOT_VERIFIED |
| 9.3 | Откат после записи чекпоинтов | 05 | L2_local_test (chaos checkpoint-rollback 18/18 на боевом PG: cp-a immutable после cp-b/cp-c [PDF:1772 неизменившиеся части не переписываются], ListCheckpoints append-only, всё переживает SIGKILL+респавн=durable rollback-target); L4 OPEN (VM) |
| 9.3 | Обновление между делегированиями | 05,14 | NOT_VERIFIED |
| 9.4 | Цикл A>B>A | 06,03 | NOT_VERIFIED |
| 9.4 | Тысячи подзадач (fan-out лимит) | 06 | NOT_VERIFIED |
| 9.4 | Дети тратят бюджет | 07 | NOT_VERIFIED |
| 9.4 | Родитель завершился, дети работают | 06,13 | NOT_VERIFIED |
| 9.4 | Родитель упал, дети закончили | 06,16 | NOT_VERIFIED |
| 9.4 | input-required по дереву | 06,14 | NOT_VERIFIED |
| 9.4 | Согласование ребёнка (родитель не может) | 08 | NOT_VERIFIED |
| 9.4 | OAuth ребёнка напрямую субъекту | 08 | NOT_VERIFIED |
| 9.4 | Ответ ребёнка с инъекцией (taint) | 03,08 | NOT_VERIFIED |
| 9.4 | Агент ставит задачу Dot — отказ | 06 | NOT_VERIFIED |
| 9.4 | Подагент в процессе прочитал инъекцию | 15,08 | NOT_VERIFIED |
| 9.4 | Ссылка на файл в делегировании | 09,06 | NOT_VERIFIED |
| 9.4 | Чтение сохранённой ссылки после отзыва | 09 | NOT_VERIFIED |
| 9.4 | Частый обмен через gateway | 19 (замер) | NOT_VERIFIED |
| 9.5 | Кэш общего агента A→B | 16,05 | NOT_VERIFIED |
| 9.5 | Приватное в память scope agent | 11 | NOT_VERIFIED |
| 9.5 | Приватное прошлого хода в общий чат | 03,14 | NOT_VERIFIED |
| 9.5 | Агент издателя в двух tenants | 05 | NOT_VERIFIED |
| 9.5 | Stage к prod-коннектору | 03,08 | NOT_VERIFIED |
| 9.5 | Get(key) чужого деплоя | 04 | L2_local_test (chaos data-isolation 5/5 cross-deploy 404 invisible) |
| 9.5 | Токен в песочнице | 01,16 | NOT_VERIFIED |
| 9.5 | Содержимое в атрибутах телеметрии | 18 | NOT_VERIFIED |
| 9.5 | Приватное на внешнюю модель | 07 | NOT_VERIFIED |
| 9.5 | Удаление сотрудника (каскад) | 02,03,11,13 | NOT_VERIFIED |
| 9.6 | Отмена во время вызова инструмента | 04,03 | L2_local_test (chaos cancel-delete 1/1) |
| 9.6 | Агент игнорирует tasks/cancel | 03 | NOT_VERIFIED |
| 9.6 | Отмена хода Dot с цепочкой из 3 | 06,14 | NOT_VERIFIED |
| 9.6 | Увольнение посреди фоновой задачи | 03,13 | NOT_VERIFIED |
| 9.6 | Отзыв доступа во время Range-скачивания | 09 | NOT_VERIFIED |
| 9.7 | Steering queue/interrupt/none | 14,16 | NOT_VERIFIED |
| 9.7 | Координатор упал в потоке (resubscribe) | 14,06 | NOT_VERIFIED |
| 9.7 | Под агента упал в ходе (таймаут хода) | 16,14 | NOT_VERIFIED |
| 9.7 | Транскрипт ≠ чекпоинт (норма) | 14 | NOT_VERIFIED |
| 9.7 | Правка реплики/форк — пересев | 14 | NOT_VERIFIED |
| 9.7 | Версия без state.compat | 05,14 | NOT_VERIFIED |
| 9.7 | Удаление чата (DeleteThread/contexts-delete) | 02,14 | L2_local_test (chaos cancel-delete: DeleteThread→404 каскад, legalhold→403 FORBIDDEN, remove-hold→delete→404-chain); space transcript-cleanup=lane |
| 9.7 | Личный коннектор → чат проекта | 03,14 | NOT_VERIFIED |
| 9.7 | Агент хочет весь чат Dot | 14 | NOT_VERIFIED |
| 9.7 | Два поручения одному агенту в чате | 14 | NOT_VERIFIED |
| 9.7 | Ожидание ответа неделю (срок гранта) | 03,14 | NOT_VERIFIED |
| 9.7 | Агент без message/stream | 14,06 | NOT_VERIFIED |
| 9.7 | Огромный ответ — файл | 09,16 | NOT_VERIFIED |
| 9.8 | Гонка двух реплик за run | 02 | L2_local_test (epoch-fence+unknown-outcome: единственный валидный epoch-держатель, stale→409 STALE_EPOCH, monotonic epoch A=1/B=2 → single-winner гарантирован State API); true-concurrent race L4 OPEN |
| 9.8 | Снимок больше лимита | 02,16 | NOT_VERIFIED |
| 9.8 | Отказ узла хранилища (синхр. реплика) | 02 | NOT_VERIFIED |
| 9.8 | Сдвиг часов пода | 02 | NOT_VERIFIED |
| 9.8 | Чужой subject в общем деплое | 02 | NOT_VERIFIED |
| 9.8 | Смена формата снимка LangGraph | 16 | NOT_VERIFIED |
| 9.8 | Удаление чата в ходу | 02,03 | NOT_VERIFIED |
| 9.8 | Смена драйвера State Store | — (драйвер Б вне объёма) | NOT_VERIFIED |

## §10–11 Поставка, ёмкость

| § | Требование | Задача | Проверка | Статус |
|---|---|---|---|---|
| 10 | Tier 0/1 (namespace, BYOK) | 18 | контур — нет | L1_realized manifest-only (Tier0 shared SaaS + Tier1 namespace/BYOK; BOUNDARY semantics PDF §10); L4 OPEN |
| 10 | Tier 2A: 3 условия (телеметрия, cache_salt, граница ИБ), WAN-задержка SLA | 18,19 | PoC-замер — нет | L1_realized manifest-only (Tier2A boundary manifests; residency-at-rest; cache_salt/граница ИБ runbook); L4 OPEN |
| 10 | Tier 2B: суверенная ячейка, локальная LLM, подписанные релизы | 18 | нет окружения | L1_realized manifest-only (Tier2B sovereign: HPA/NetPol/PDB/RBAC; локальная LLM не деплоилась); L4 OPEN |
| 10 | Нет межъядеечной федерации; management plane вне горячего пути | 18 | — | NOT_VERIFIED |
| 11 | SLO: ACK p95≤300мс, шаг≤1с, отмена≤1с, 99,9%; память Core Agent на ход | 19 | нагрузочный — нет | L2_local_test measured: checkpoint p95=134.67ms(≤300), gateway p95=49.90ms(≤50), RSS~18.5MB; local micro-bench ≠ target-runtime; L4 OPEN |

## §12, Прил.А, Прил.Б — мета/справочные разделы PDF (mapped, не продуктовые требования)

| § | Раздел PDF | Характер | Статус в матрице |
|---|---|---|---|
| 12 | «Как сведены источники» | Мета-примечание самого документа (сверка источников PDF). НЕ продуктовое требование и НЕ тестовый корпус. Ссылки внутри PDF не расширяют scope (requirements_source_policy=exclusive-user-pdf). | MAPPED_META (требование не вводит; учтено для полноты «каждый раздел PDF mapped») |
| Пр.А | «Соответствие имён» | Справочник терминов PDF↔реализация (AHP/Agents Space имена). Использован при именовании плоскостей compose/ролей. | MAPPED_REF (справочно; импорта имён в код — нет) |
| Пр.Б | «Словарь» | Глоссарий (Core Agent, Thread/Run, artifact, realm и т.п.). Опора для терминологии в этом документе. | MAPPED_REF (справочно; требования не вводит) |

## §13–14 Сквозные проверки и риски

| § | Проверка/риск | Задача | Статус |
|---|---|---|---|
| 13 | Полный список сквозных срезов (19 пунктов) | 19 | NOT_VERIFIED |
| 14 | Ёмкость Core Agent (gigaconf/bench) | 19 | NOT_VERIFIED |
| 14 | Два режима workflow.py — контрактные тесты | 15 | NOT_VERIFIED (нет исходника) |
| 14 | Fencing тома SDS | 12 | NOT_VERIFIED (нет SDS) |
| 14 | Охват Token Exchange (инвентаризация) | 08 | NOT_VERIFIED |
| 14 | Задержка перехода gateway (≤50мс p95) | 19 | L2_local_test p95=49.90ms (≤50, local); L4 OPEN |

## Зависимости и границы

- Исходник Core Agent/workflow.py недоступен: §4.3, §3.1, риск «два режима» — оценка объёма невозможна (задача 15 явно не объявляет самописный цикл адаптацией).
- Linux/KVM/SDS-стенд отсутствует: класс X, Computer, fencing тома, VMGenID — только host-simulation, NOT_VERIFIED.
- Live IAM Cloud.ru / RFC8693 / внешний OAuth: §8.1, §8.5 — без контура не проверяется.
- Реальные провайдеры моделей/S3/Vault для live-E2E: задача 07/09/18 — отдельно от fixture.
- Драйвер Б State API и миграция областей — вне нумерации задач.

## 5 существенных пробелов

1. Драйвер Б State API + конформанс для обоих драйверов и экспорт/импорт областей (§2.5) не имеют задачи.
2. Компьютер/профиль personal целиком (lease, typed patch, rollback, сон, realms, снимки) зависит от KVM/SDS — вся §3.6 и §9.6 «скачивание после сна VM» непроверяемы.
3. Tier 2A/2B (cache_salt, BYOK, суверенная ячейка, локальная LLM) сведены к задаче 18 без отдельной проверки границ residency.
4. Принуждение контракта агента без SDK (§2.3 п.5,6,9,10; §14 «корректность кастомных агентов») — только ahp-sdk check + chaos, для self-hosted Agent Server проверки нет.
5. Live-зависимости (IAM, OAuth-SaaS, провайдеры, KVM) не должны отмечаться как пройденные; в матрице все такие строки NOT_VERIFIED.


## Delivery integration evidence (L2_local_test; local micro-benchmarks ≠ target-runtime proof)
Обновлено 2026-10-29 (whole-block интеграция; owner-блоки committed и byte-identical; fresh-прогоны на изолированной БД `ahp_hive_delivery`).

- **Сборка**: `go build ./...` exit 0 (offline, `-mod=mod` и `-mod=readonly`); `go vet ./...` чист. Whole-tree компилируется после интеграции owner-снапшотов.
- **`npm run check`** — PASS (RC=0): `go vet` + `go test -race` для 27 in-scope пакетов, 0 исключено как открытые RFC. Лог `runs/.../logs/check-final-1793285208.log`.
- **`npm run smoke`** — PASS: self-spawn :19619 mTLS `/healthz` = 200 `{"status":"ok"}`.
- **`npm run test:e2e`** — PASS 4/4: health-check (mTLS `/healthz` `/readyz`=200, `/debug/stats`); separated-roles (см. ниже); live-qwen — честный SKIP (G-LLM-QWEN, не фиктивный PASS); state-api 17/17 (403 no-token, 404 unknown, CreateThread+идемпотентный replay, CreateRun, ClaimRun epoch=1 + task_token SPIFFE `spiffe://ahp/tenant/dev/deployment/*`, Heartbeat, PutKV v1 + KEY_REUSE 400, PutCheckpoint/GetCheckpoint/GetPart round-trip, Release).
- **`npm run test:chaos`** — PASS 5/5: cancel-delete 13/13 (cancel+delete+legalhold-403), data-isolation 5/5 (cross-deploy Get 404 invisible), epoch-fence 8/8 (STALE_EPOCH 409), graceful-shutdown 3/3 (SIGTERM exit0, порт закрыт), restart 4/4 (SIGKILL+restart, consistency).
- **`npm run test:load`** (local single-node) — PASS 3/3: PutCheckpoint p50=49.06 p95=112.14 p99=126.75 ms (SLO ≤300) → WITHIN; control-API `/healthz` p50=6.69 p95=45.64 p99=48.12 ms (PoC ≤50, **впритык**, variance по прогонам 44.8–51.4 ms) → REPORTED; RSS peak 18.7 MB (≤256). **local ≠ target runtime.** Лог `runs/.../logs/final-consolidated-1793286950.log`.
- **Separated roles + per-role OTel** — committed e2e-gate `tests/e2e/separated-roles.mjs` (`npm run test:e2e`): один `bin/ahp` стартует тремя раздельными ролями state/identity/model (:29731/2/3, tls:false), каждая независимо достигает `/readyz`=200 и отдаёт `/healthz`=200 `{"status":"ok"}`; после SIGTERM-flush в stderr каждой роли assert-ится OTel server-span `"name":"GET /healthz"` с `"service":"<своя роль>"` и отсутствие spans чужих ролей (изоляция). 18/18 PASS. Ad-hoc-подкрепление: `logs/multirole.log`, `logs/otelproof.log`; unit `TestHTTPMiddlewareEmitsServerSpan`.
- **Composed all-role OTel on real served traffic** — `tests/e2e/otel-trace.mjs` (ck36): `bin/ahp-full` role=all c `OTEL_EXPORTER_OTLP_ENDPOINT=""` (offline in-house JSON-exporter → stderr); живые запросы — `/readyz`=200 и РЕАЛЬНЫЙ state `CreateThread` на реальном PG; SIGTERM→`tp.Shutdown` флашит батч. Парсинг stderr span-JSON ассертит: `GET /readyz` server-span `status=200 service=all`; `POST /v1/state/*` server-span `status=200` (имя cardinality-bounded, raw id вырезан); у каждого server-span присутствуют `http.method`+`http.status_code`+`service`; `trace_id`=32hex/`span_id`=16hex; ни одно имя не содержит UUID. **9/0 PASS.** Unit `pkg/otel/otel_test.go` (`TestHTTPMiddlewareEmitsServerSpan` и др.) = ok. Это L2_local на живой serve-трафике; OTLP-collector target = внешний контур (не Mac).
- **Deploy controller** (`cmd/ahp-deploy`) — validate-only RC=0 на манифесте §6.6; generate → 6 объектов (Namespace/ServiceAccount/NetworkPolicy/StateScope/Role/RoleBinding); негатив invalid-visibility покрыт go-тестом.
- **Compose** — `docker compose -p ahp-handbook -f compose.yaml config` OK; профили `app`: `go-ahp-all/state/identity/model` (AHP_ROLE + healthcheck `/healthz`). **Docker image build заблокирован** (buildx write perms + offline Go-прокси) → compose-роли = L2 (native runnable + config OK), НЕ L4.
- **Tier manifests** (0/1/2A/2B) — structural YAML-валидация PyYAML: 27 docs, required-поля на месте, 0 ошибок = **L1_realized**. `kubectl` настроен на чужой реальный кластер (`37.44.196.176`) — ничего не применялось; offline k8s-schema-validation невозможна. **L4 target-runtime = OPEN.**
- **Integrated snapshot manifest** — `infra/integration/manifest.json` (baseline + 3 owner-коммита с tree/parent/bundle-sha256 + byte-identity + PDF sha256). Провенанс ephemeral state-бандла сохранён в `acceptance/integration/delivery/`.

### Открытые гейты (delivery acceptance требует реальных API соседей / целевых окружений)
- **G-LLM-QWEN** — live-Qwen E2E: нет endpoint (`AHP_QWEN_BASE_URL` unset). Честный SKIP, не PASS.
- **G-INTEGRATION** — Execution/Data кандидатов нет; Space `changes_required 6edf20b`; space↔state wire-несовместимость (см. §2.5). Сквозной live whole-system E2E невозможен.
- **G-TIER2A-2B-L4** — нет k8s target (kubectl на чужом реальном кластере — не трогать). Tier = L1_realized.
- **G-COMPOSE-APP-BUILD** — docker build заблокирован (buildx perms + offline proxy).
- **G-CAPACITY** — control-API p95 ~45–51 ms local (PoC ≤50, впритык); target-runtime не доказан.
- **G-IAM** — Cloud.ru IAM / RFC8693 Token Exchange / внешний OAuth: без контура не проверяется.

## Delivery evidence refresh — 2026-10-29 (checkpoint-29, изолированная БД `ahp_hive_delivery`)
Свежие прогоны этого сеанса. `accepted` не объявляется; это кандидат на ревью директора.

### Прогоны (фактический вывод)
- **`npm run smoke`** — PASS: self-spawn `:19619` mTLS `/healthz` → 200 `{"status":"ok"}`.
- **`npm run test:chaos`** — PASS 6/6 (было 5/5; добавлен угол «unknown»):
  - cancel-delete 13/13 — Release(canceled)+Heartbeat→409 STALE_EPOCH, DeleteThread→404, legalhold→403 до снятия.
  - data-isolation 5/5 — cross-subject и cross-deployment GetKV→**404** (невидимость, не 403).
  - epoch-fence 8/8 — STALE_EPOCH 409 для PutKV/PutCheckpoint на старом fence, в т.ч. после Release.
  - graceful-shutdown 3/3 — SIGTERM exit0, порт закрыт.
  - restart 4/4 — SIGKILL(крэш)+restart, consistency + readiness-DB-ping.
  - **unknown-outcome 13/13 (НОВЫЙ)** — PDF §9.2+§9.8: воркер A claim(ttl=2s)+PutKV(v1)→падает без Release; GetRun остаётся non-terminal (running, не failed/canceled = исход unknown); Heartbeat старого fence→409; worker-B перехватывает тот же run, epoch монотонно 1→2; zombie-PutKV A→409; идемпотентный replay op-1 под B→та же версия 1 (нет дубля durable-записи); Release(done); GetRun=done.
  - Тем самым покрыты ВСЕ 4 PDF-угла chaos: **restart / cancel / unknown / delete**.
- **`npm run test:load`** — PASS 3/3 (local single-node, REPORTED ≠ target):
  - PutCheckpoint ×40 @8 conc: ok=40 errors=0, **p50=43.93 p95=80.58 p99=102.80 ms** (PDF §11 Durable-ACK ≤300 → WITHIN), throughput 145 writes/s.
  - control-API `/healthz` 50 RPS×3s: success 150/150, **p50=7.14 p95=51.18 p99=53.76 ms** (PoC ≤50, впритык; variance по прогонам 45–51), availability 100%.
  - RSS peak **18.7 MB** (≤256), goroutines=8, go1.26.2.

### Закрытие G-LLM-QWEN (native-Qwen live доказан)
- Go `AHP_LIVE_QWEN=1 go test ./internal/model/ -run LiveQwen` — PASS.
- `tests/e2e/model-to-artifact.mjs` 12/12 (коммит `a880598`): G0 `native-qwen-assert.sh`=ALLOW → реальный native-Qwen `/chat/completions` (secret из Vault; хост-suffix `modelrun.inference.cloud.ru`, model∈{`model-run-tqmbu-road`,`qwen-38-27b-fp8`}) → REAL MinIO presigned PUT → Seal version → Download sha256-match → State CreateThread/CreateRun/GetRun читает ссылку на артефакт из PG.
- `tests/e2e/data-plane.mjs` 12/12 (коммит `aedef49`), `tests/e2e/composed-serve.mjs` 11/11 — create-run→PG state + Identity human `/v1/identity/roots` 201.
- **Разграничение:** «native-Qwen endpoint достижим и model→versioned-artifact→PG-state склейка реальна» = CLOSED. «Сквозное выполнение задачи боевым Emil-tool» = отдельный гейт G-EMIL-TOOL (ниже).

### Обновлённые открытые гейты (checkpoint-29)
- **G-LLM-QWEN — CLOSED** (native-Qwen live + model→artifact→PG-state; см. выше). OpenAI/Astra не использовались.
- **G-EMIL-TOOL — CLOSED (Checkpoint-65, 2026-10-30)**: бэкенд боевого Emil-core `:18000` починен (model-base больше не `127.0.0.1:1`); РЕАЛЬНЫЙ A2A tool-call проходит — модель вызывает `core_skill_activate`, `function_response.status=succeeded` с настоящим content-digest активированного скилла, собственная отчётность Emil подтверждает `model_turns>=1, tool_calls>=1`. Закрыто каноническим `tests/e2e/live-journey.mjs` **17/17** (`LIVE_JOURNEY=GREEN`, `JOURNEY=WHOLE-GREEN`). Историческое состояние OPEN (задачи `0d802bf5…`, `a6f30550…`, `87ef1caa…` = `failed/MODEL_UNAVAILABLE`) сохранено выше в чекпоинтах 32–43. Стоп-ин `:19081` (`ahp-vm-tool-agent`) как доказательство НЕ использовался; `httptest`/`newMockState` в live-suitе нет.
- **G-WHOLETEST — УТОЧНЁН (checkpoint-41)**: `npm run check` = `go vet`+`go test -race` по всему дереву. **Disk-гейт снят: `avail=24 GiB`≥20** (kvm-lab paused). Read-only host `$HOME/go/pkg/mod` блокировал любую go-сборку → офлайн-обход с СОХРАНЁННЫМ `go.sum`: `GOPROXY=file://$HOME/go/pkg/mod/cache/download` + writable `GOMODCACHE=/tmp/ahp-modcache` + `GOSUMDB=off` (только внешний sum.golang.org; `go.sum` активен) + `GOTOOLCHAIN=local GOFLAGS=-mod=mod`. **RUNNABLE-ПОВЕРХНОСТЬ собрана на Mac (свежий прогон @ae383ec):** `go build -tags "ahp_exec ahp_data" ./cmd/ahp`=RC0/0stderr; `go vet -tags "ahp_data"` composition set=RC0/0stderr; `go test -p1 -count=1` (без -race) `./cmd/ahp`=ok, `./pkg/otel/...`=ok (изолированная БД). **ОСТАТОК гейта:** `go test -race` по ВСЕМУ дереву → VM-plane (директор запретил тяжёлый -race; RAM/swap через sysctl/ps в песочнице заблокированы → неизмерим). Тяжёлое → VM (gigasearch-proxy).
- **G-GOSDK — CLOSED (2026-11-01T21:46Z T-GW)**: эта запись была ЛОЖЬЮ для текущей ревизии. `github.com/modelcontextprotocol/go-sdk v1.8.0` ДОСТУПЕН офлайн (есть в host `~/go/pkg/mod` и в `/tmp/ahp-modcache`, `GOPROXY=off` работает); `internal/tools` собирается и даёт зелёные тесты на реальном PG, и он слинкован в `cmd/ahp` (импортёр `internal/executor/guardian.go`). Теперь у tool-plane есть и продуктовое лицо: `internal/tools/toolsapi`, монтируется в `cmd/ahp` под `/v1/tools/` при `AHP_ENABLE_TOOLS=1` (`Call/Get/Fence/Approve/ConnectionState`, реальный MCP round-trip без egress). Историческое состояние OPEN сохранено ниже по тексту чекпоинтов для аудита и не переписывается.
- **G-TIER2A-2B-L4 — OPEN**: k8s target нет (чужой кластер не трогать); Tier = L1_realized (structural YAML).
- **G-COMPOSE-APP-BUILD — OPEN**: docker image build заблокирован (buildx perms + offline proxy); compose-роли = L2.
- **G-CAPACITY — OPEN**: p95 control-API впритык/слегка>50 local; target-runtime не доказан (L4).
- **G-IAM — OPEN**: Cloud.ru IAM / RFC8693 Token Exchange / внешний OAuth — без контура.

## Checkpoint-34 (2026-10-29T19:53Z) — ре-прогон на боевых зависимостях + Emil-tool hop entrypoint
Ре-прогон на БОЕВЫХ зависимостях (PG `ahp_hive_delivery` `:25432` + боевой MinIO VM `:19000` через
in-session туннель + боевой Emil `:18000`==VM `:19080`), prebuilt-бинари, G0 native-Qwen gate, без fallback:
- `tests/e2e/composed-serve.mjs` — **11/0**: ВСЕ роли смонтированы (ноль not-mounted), data-plane `403`,
  execution `/a2a/healthz`=200 / public face `403`, `/debug/stats`=200 `go1.26.2`.
- `tests/e2e/model-to-artifact.mjs` — **11/0**: РЕАЛЬНЫЙ native-Qwen 136 B → presigned PUT→боевой VM MinIO `200`
  → versioned artifact sha256 `ef988266…` → CreateThread/CreateRun/GetRun на реальном PG, ссылка прочитана обратно.
  Т.е. **create-run→model→versioned-artifact→PG-state** GREEN на боевом MinIO (без Emil-core).
- `tests/e2e/data-plane.mjs` — **12/0**: versioning + presigned PUT→боевой VM MinIO `200` + Range `206`
  + foreign-tenant `404` + soft-delete→`404`. Это O1-примитивы MinIO на БОЕВОМ MinIO.

### Новый entrypoint Emil-tool hop: `tests/e2e/emil-tool-hop.mjs` (wiring, 4 assertions, НЕ мок)
- Emil agent-card `200` (`core-agent`, `core_task_start`) → `message/send` вернул структурированный A2A Task
  (hop **WIRED**; taskId `723f04c3-2714-4a22-b636-9642fb5c4920`, ts `2026-10-29T19:53:25Z`) → `tasks/get`=
  `failed` / `MODEL_UNAVAILABLE`. Печатается `EMIL_TOOL_HOP=external-gate G-EMIL-TOOL`.
- Честная семантика: утверждается только ЧТО hop смонтирован (Emil достижим по A2A, протокол adapters/emil
  работает, возвращается валидный Task). Исход Emil-задачи — внешний root-ассет; тест НИКОГДА не синтезирует
  completed-задачу и не мокает Emil. Только транспортный сбой/отсутствие taskId = genuine defect (exit 1).

### Сквозной tool-leg доказан отдельно (директорская VM-склейка)
`acceptance/integration/MERGED-CANONICAL-GREEN-20261029.md`: merged canonical `012a849` + принятый space-lane
→ `go build ./...` = rc 0 и `TestLiveRealFullJourney` real PASS ×3: create-run→native-Qwen→Emil-shaped A2A artifact
35 B (sha `01d14a6f…`)→versioned MinIO seal (VersionID `616d458b`)→PG durable→restart/restore epoch-fence e1→e2
→authorized download (sha-match, intruder denied). **Зелёная tool-нога шла на native-Qwen tool-agent `:19081`
(fail-closed), НЕ Emil core `:19080`.** Т.е. сквозной tool-leg proof существует через РЕАЛЬНЫЙ A2A tool;
specifically боевой Emil-core backend (`MODEL_UNAVAILABLE`) = отдельный внешний гейт G-EMIL-TOOL, а не дефект delivery.

## Checkpoint-38 (2026-10-29T20:47Z) — ЕДИНЫЙ боевой сквозной journey (директорский О2/О1 steer)
Одно SSH-туннельное окно к VM `192.144.13.231` (`:19000` MinIO / `:19080` Emil), prebuilt-бинари, БД `ahp_hive_delivery`,
proxy очищён, native-only, G0 канонический (`acceptance/integration-validation/scripts/native-qwen-assert.sh`).
Цель — реальный путь **create-run→model→Emil-tool→versioned-artifact→PG-state** на БОЕВЫХ зависимостях (root-верифицированные эндпоинты):
- **STAGE 1 `composed-serve.mjs` = 11/0** (all-role `ahp-full`, VM MinIO `:19000`): `/readyz`=200 real-PG, НОЛЬ «plane not
  mounted» (data+execution смонтированы), identity `403`+create-root `201`, state CreateThread/CreateRun/GetRun на реальном PG,
  data `403`, execution `/a2a/healthz`=200 + public `403`, `/debug/stats`=200 `service=all go1.26.2`.
- **STAGE 2 `model-to-artifact.mjs` = 12/0** (VM MinIO `:19000`): канонический **G0 ALLOW** (rc 0, host `*.modelrun.inference.cloud.ru`
  model `model-run-tqmbu-road`) → РЕАЛЬНЫЙ native-Qwen completion 136 B → BeginUpload → presigned PUT→VM MinIO `200` →
  Seal versioned `a029c840…` sha256 `ef988266…` → Download sha-match → CreateThread/CreateRun/GetRun на реальном PG (ссылка прочитана обратно).
- **STAGE 3 `emil-tool-hop.mjs` = wiring 4/0** (боевой Emil `:19080`, `core-agent`/`core_task_start`): `message/send` вернул
  структурированный A2A Task (hop **WIRED**, taskId `8b07fcb7-6a98-458a-a9e4-7ff10584c0c1`, ts `2026-10-29T20:47:25Z`) →
  `failed`/`MODEL_UNAVAILABLE` ⇒ `EMIL_TOOL_HOP=external-gate G-EMIL-TOOL`. Hop смонтирован и работает по протоколу; исход
  Emil-задачи — внешний backend Emil-core (root-ассет), НЕ мокается и НЕ синтезируется.

Вывод: **create-run→model→versioned-artifact→PG-state GREEN на боевом VM MinIO + канонический G0 ALLOW**;
единственная незакрытая нога — Emil-core tool-результат = внешний гейт **G-EMIL-TOOL** (Emil `MODEL_UNAVAILABLE`),
что директор отдельно подтвердил как внешний root-гейт, а не дефект delivery. Блок по-прежнему UNACCEPTED (приёмка — только директор).

## Ре-верификация §9 chaos / §11 load / smoke — @24fb8ce (2026-10-29T20:55Z, БД `ahp_hive_delivery`)
- **§9 chaos: 6/6 (46 assert)** — L2_local_test. Покрытые углы State/data-плоскости: restart(SIGKILL+restore)→§9.2; epoch-fence(stale-epoch reject, zombie-after-takeover)→§9.2; unknown-outcome(crash→не-terminal, re-claim same-run, idempotent-replay, SIDE_EFFECT_UNKNOWN-guard)→§9.2; graceful-shutdown(sync durability stop)→§9.2; cancel-delete(отмена=Release canceled + DeleteThread cascade + legalhold 403)→§9.6/§9.8; data-isolation(cross-subject/cross-deploy 404 invisible)→§9.5/§9.8. Остальные §9-углы честно NOT_VERIFIED (принадлежат space-product/identity/compute-plane владельцам).
- **§11 load: 3/3** — L2_local_test, REPORTED ≠ target-runtime: checkpoint p95=83.23ms(≤300 WITHIN), control-API p95=44.33ms(≤50 впритык), RSS 18.8MB(≤256).
- **smoke: exit 0** — liveness-only (`/healthz` 200), не LLM/E2E.
- Операционная первопричина ранних `0/6` (DB SASL auth) и её устранение — см. docs/progress.md ck39. source≠deployment.

## Ре-верификация O2 composed all-role + Emil-hop гейт — @a6b9448 (2026-10-29T21:00Z, БД `ahp_hive_delivery`, локальный MinIO `:29000`)
- **O2 «все роли, НОЛЬ not-mounted»: `composed-serve.mjs` 11/0** — L2_local_test на РЕАЛЬНОМ PG (`ahp_hive_delivery`) + РЕАЛЬНОМ MinIO (`:29000`). На бинаре `bin/ahp-full` (role=all, tags `ahp_exec ahp_data`): `/readyz`=200 real-PG-ping; stderr НЕ содержит «data/execution plane not mounted» (обе опциональные плоскости смонтированы); identity create-root `403`→`201`(real PG); state CreateThread/CreateRun/GetRun real-PG; data `/v1/artifacts/BeginUpload` `403` (mounted, не 404); execution `/a2a/healthz`=200 real-PG-migrated + `/a2a/public` `403`; `/debug/stats`=200 go1.26.2. source≠deployment.
- **Emil-tool hop (нога journey create-run→model→Emil-tool): wiring GREEN, гейт ОТКРЫТ** — `emil-tool-hop.mjs` 4/0 против БОЕВОГО Emil (`:18000`≡VM`:19080`): agent-card `200 core-agent`, `message/send`→A2A Task `0dda6229-524c-4e84-9d18-47cb72fdb1fb`, task `failed`/`MODEL_UNAVAILABLE` → **G-EMIL-TOOL подтверждён свежим прогоном** (hop смонтирован; backend Emil-core = root-asset; не мокается). availability=200 ≠ execution proof.

## Checkpoint-42 (2026-10-29T21:35Z) — WHOLE-TREE offline build + INTEGRATED test run (БД `ahp_hive_delivery`, MinIO `:29000`)
Класс утверждений: **L2_local_test** (Mac go1.26.2 darwin/arm64, офлайн file-proxy GOPROXY, `go.sum` не отключён).
`source ≠ deployment` — это локальный прогон, НЕ target-runtime и НЕ зелёный -race whole-tree (RAM/disk гейт директора, VM-plane).

### A) WHOLE-TREE build (tags `ahp_exec ahp_data`)
`go build ./... -tags "ahp_exec ahp_data"` → **RC=1**, единственный failing-лист — `internal/tools/client.go:6:2`
(`github.com/modelcontextprotocol/go-sdk@v1.8.0`: zip отсутствует в host-кэше, есть только v1.4.0). 1 строка ошибки на всё дерево.
Точный offline-deficit (директорский policy п.4 — «требует, но нет в кэше»), все — закрытие одного direct-пинa go-sdk (go.mod:9):
- `github.com/modelcontextprotocol/go-sdk v1.8.0`  (DIRECT, go.mod:9) ← лист `internal/tools`
- `github.com/google/jsonschema-go v0.4.3`         (indirect, go.mod:23)
- `github.com/segmentio/encoding v0.5.4`           (indirect, go.mod:37)
- `golang.org/x/oauth2 v0.36.0`                    (indirect, go.mod:49; `go mod why` = main module does not need it → zip не грузится пока не импортирован)
Хэши всех четырёх ЕСТЬ в `go.sum` → root может `go mod download` точных версий с sum-verification без обхода.
НЕ делал `replace`/downgrade/vendor/`GONOSUMDB`. Runnable-поверхность `cmd/ahp` не импортирует ни `internal/tools`, ни один дефицит
(`go list -deps ./cmd/ahp` ∩ дефицит = ∅) → runnable-доказательство не затронуто.

### B) INTEGRATED test run (всё дерево кроме go-sdk-листа; единая изолированная БД + боевой локальный MinIO)
Env интеграции (имена; значения не печатаются): `AHP_TEST_DATABASE_URL`→`ahp_hive_delivery`,
`AHP_S3_ENDPOINT=http://127.0.0.1:29000`, `AHP_MINIO_ROOT_USER`/`AHP_MINIO_ROOT_PASSWORD` — из env разрешённого
контейнера проекта `ahp-handbook-go-minio-1`. Чужие БД/контейнеры не тронуты.
Итог по 43 пакетам: **rc=0: 42, rc=1: 1** (только `internal/tools` = G-GOSDK leaf). В том числе зелёные КРОСС-ПЛОСКОСТНЫЕ integration-наборы
(не моя ownership — я провижу окружение, чужой код НЕ правлю): `internal/artifactsapi`(data), `internal/memory`(space),
`internal/workspace`, `internal/state`, `internal/state/conformance`, `tests/{compute,identity,space,state}`,
`internal/{identity,gateway,core,model,operations,registry,triggers}`, `pkg/*`, `cmd/{ahp,ahp-deploy,as-runner}`, `sdk/go/state`.

### C) ПЕРВЫЙ реальный интеграционный дефект/требование провижининга (не спекуляция)
Изначально broad-прогон дал rc=1 на `internal/{artifactsapi,memory,workspace,state/conformance}`. Корни разобраны:
1. `state/conformance,artifactsapi,memory,workspace` — integration-наборы gated env `AHP_TEST_DATABASE_URL` (≠ `AHP_DATABASE_URL`); после выставления — green кроме memory.
2. `artifactsapi/workspace` — gated ещё `AHP_S3_ENDPOINT` (+ root-креды MinIO); после выставления → **ok**.
3. `internal/memory` — `CONFLICT: memory: migrate memory schema: storage failure`. Первопричина (DDL `store.go:162` `CREATE EXTENSION IF NOT EXISTS vector`, pgvector 0.8.7 в контейнере): **`permission denied to create extension "vector"; Must be superuser`** — pgvector в этом Postgres НЕ trusted-расширение, поэтому `Store.Migrate` под непривилегированной ролью не проходит, а pgx-ошибка проглатывается `storageError` в `ErrConflict`.
**Требование к target-runtime (delivery/runbook):** перед миграцией memory-плоскости БД должна иметь pgvector, установленный superuser-ом (`CREATE EXTENSION vector`), ИЛИ миграция должна идти привилегированной ролью. После superuser `CREATE EXTENSION vector` в `ahp_hive_delivery` → `internal/memory` = **ok**. Это не продуктовый дефект кода, а требование provisioning-а изолированной БД (моя роль).
Блок по-прежнему UNACCEPTED (приёмку ставит только директор).

## Checkpoint-43 (2026-10-30T00:38Z) — единый journey entrypoint + воспроизводимость без VM
| PDF-сегмент (сквозной продуктовый flow) | Evidence | Статус |
| --- | --- | --- |
| `create-run → model → versioned artifact → PG state` | `npm run journey` → `tests/e2e/journey.mjs` LEG-1 `model-to-artifact` 12/0 на ЛОКАЛЬНОМ боевом MinIO `:29000` + PG `ahp_hive_delivery` + РЕАЛЬНЫЙ native-Qwen (G0 ALLOW), version `426aec71` sha `ef988266`, GetRun читает artifact-ref из PG | GREEN (боевые зависимости, без VM) |
| `model → Emil-tool` (hop wiring) | LEG-2 `emil-tool-hop` — A2A task получен от боевого Emil-core `:18000` | WIRED |
| `Emil-tool completion` (боевой backend Emil) | `EMIL_TOOL_HOP=external-gate G-EMIL-TOOL`; свежие taskId `0b78afb1`/`5af7357d`/`888909b5` = `MODEL_UNAVAILABLE` | **G-EMIL-TOOL (внешний, root-ассет)** |
| whole-journey (composed) | `JOURNEY=green-except-emil-tool` | все delivery-ноги GREEN; completion гейтится внешним Emil |
Вывод: единственный незакрытый сегмент сквозного journey — боевой Emil-core model backend (root-ассет). Всё delivery-owned GREEN и воспроизводится на локальном боевом MinIO (не только VM). Не мокается.

## Checkpoint-44 (2026-10-30T00:44Z) — runnable UI/API serve (direct/root: «runnable UI/API для пользователя»)
| Требование (директор/роот) | Evidence | Статус |
| --- | --- | --- |
| Runnable UI/API serve на БОЕВЫХ зависимостях | `npm run demo` → `tests/e2e/demo-ui.mjs`: all-role `bin/ahp-full` + фронт `http://127.0.0.1:29790/`; `/api/planes` все mounted | RUNNABLE (PG `ahp_hive_delivery` + MinIO `:29000` + Emil `:18000`) |
| Реальный сквозной `create-run→…→PG state` | `POST /api/run` → `{ok:true,persisted:true,readStatus:200,runID:…77lahpty…}`; `demo:selftest` planes MOUNTED + create-run PERSISTED | GREEN (боевые deps, mock=NONE) |
| Реальный `model→versioned artifact→PG state` | `demo:selftest -- model` → `model->artifact->PG: passed=12 failed=0` | GREEN |
| Emil-tool hop (боевой backend Emil) | `/api/emil-hop` → `external-gate`; taskId `447053a0`/`114dcc4a` = `MODEL_UNAVAILABLE` | **G-EMIL-TOOL (внешний, root-ассет)** — не мокается |
| availability ≠ task-execution | `/api/deps` помечен «availability only, NOT task-execution proof» | честно |
| status прототипа | `/api/plan` `status=unaccepted-prototype` + открытые гейты (G-EMIL-TOOL/G-GOSDK/VM-plane) | UNACCEPTED (приёмка — директор) |
Вывод ck44: у пользователя есть единая команда `npm run demo` и URL дашборда/API; все delivery-owned сегменты сквозного пути зелёные на боевых зависимостях (PG+MinIO+Qwen), completion гейтится только внешним Emil-core.

### VM green-run runbook (ck44 serve, target-runtime proof; Mac disk<20)
Serve ck44 VM-портативен: `AHP_S3_ENDPOINT`/`AHP_EMIL_URL` пробрасываются и в `bin/ahp-full`, и в probe (Mac-портов не хардкожено). Для VM-боевых адресов:
```
# read/control на VM (root-форма; глобальный ssh config НЕ менять, дубли туннелей НЕ плодить)
ssh -F /dev/null -i ~/.ssh/gigasearch-proxy -o BatchMode=yes -o StrictHostKeyChecking=accept-new alex@192.144.13.231
# внутри VM (или на хосте с живым root-туннелем VM:19000->Mac:29000, VM:19080->Mac Emil:18000):
export AHP_S3_ENDPOINT=http://127.0.0.1:19000 AHP_EMIL_URL=http://127.0.0.1:19080
export AHP_DB_NAME=ahp_hive_delivery AHP_NATIVE_QWEN_ASSERT="$HQ/acceptance/integration-validation/scripts/native-qwen-assert.sh"
npm run --silent demo:selftest -- model   # G0 gate -> planes -> create-run->PG -> model->artifact->PG -> Emil-hop
```
Ожидание target-runtime: planes MOUNTED + create-run/model legs GREEN; Emil-hop = G-EMIL-TOOL пока Emil-core не поднимет native-Qwen runtime (kvm-lab tool-agent по директиве). source≠deployment: этот runbook НЕ заменяет фактический VM-прогон iv; без него Tier-гейты остаются GATED.

### Checkpoint-46 — offline-build + acceptance-pack (PDF-раздел → evidence)
| PDF-раздел / требование | Evidence (свежий прогон ck46) | Статус |
| --- | --- | --- |
| Собираемость продукта без egress (реализация требований PDF) | whole-tree `go build ./...` + `-tags 'ahp_exec ahp data'` rc=0 offline @host modcache; go.mod/go.sum чистый diff | CLOSED (host) |
| Готовый acceptance-pack (гейт #5) | `scripts/acceptance-pack.mjs` → PACK_RC=0, `{run:4,cited:5,gated:3}` (json+md) | RUN (generator) |
| `npm run check`/`npm run smoke` (гейт #2) | check go test 44/44 PASS rc=0 (skip-race локально); smoke self-spawn `/healthz`=200 rc=0 | GREEN (Mac-safe) |
| `live Qwen E2E` + model→artifact→PG (гейт #2) | `demo:selftest -- model` planes 5/5 MOUNTED; model→artifact→PG `12/0`; create-run PERSISTED `…2xo6mcdx…` | GREEN (боевые deps, mock=NONE) |
| Emil-tool completion (внешняя зависимость) | hop живой taskId `77c28c31…`→`MODEL_UNAVAILABLE`; native-Qwen tool-agent VM`:19081` GREEN×3 (kvm-lab) | **G-EMIL-TOOL (внешний)** — не мокается |
| Whole-system `-race`, Tier L4/IAM/compose-app/OTLP | VM-preferred (disk/swap гейт) | GATED (VM/iv) |

### Checkpoint-47 — separated roles + OTel (PDF §observability + §separate-deployability + §11 health/readiness)
| Требование PDF | Evidence (свежий прогон ck47, Mac) | Статус |
| --- | --- | --- |
| Раздельная разворачиваемость сервисов (separated roles) | `test:e2e separated-roles` rc=0: state/identity/model отдельные процессы, `/readyz`=200 независимо, per-role OTel span без sibling-ролей | GREEN |
| Observability через OpenTelemetry (§11) | `test:e2e otel-trace` rc=0 (9/0): live serve→traced spans, attrs method+status+service, trace/span id корректны, cardinality-bounded, SPIFFE WI | GREEN |
| Health/readiness surface (§11) | `/healthz`+`/readyz`=200 на каждой роли (liveness+DB-ping readiness) | GREEN |

## Checkpoint-49 (2026-10-30) — PDF-замыкание «каждый раздел» + СВЕЖИЕ chaos/load (acceptance #2/#4)
- **acceptance #2 «каждый раздел PDF mapped to evidence»**: добавлена в начало матрицы таблица «Замыкание каждый раздел PDF → evidence» — перечислены ВСЕ разделы СОДЕРЖАНИЕ PDF rev 3.2 (включая ранее неучтённые «Коротко» (с.4, exec-резюме п.1–12) и «Атлас схем» (с.5–26, рис.1–21)). Каждый раздел → домашняя строка матрицы + статус; несущие рисунки замаплены явно (рис.4→§1 разъём, рис.14→§5 мультиагент, рис.17→§8.3 изоляция, рис.18→§4.4 стык, рис.20→§2.5 State API, рис.21→§7 данные). Гейт закрыт на уровне «каждый раздел соотнесён»; продуктовые статусы — в домашних строках (L4 OPEN).
- **acceptance #4 chaos — СВЕЖИЙ прогон @8d0ae01** (не cite): `npm run test:chaos` → **6/6 PASS, 0 fail**, изолир. PG `ahp_hive_delivery`. Покрыты углы PDF §9: `restart` (SIGKILL→порт закрыт→тот же DB→consistency+readiness), `unknown-outcome` (13 assert: лизинг-истечение, STALE_EPOCH 409, re-claim того же run, монотонная эпоха A=1/B=2, zombie-fence 409, идемпотентный replay без дубля, Release done), `cancel-delete`, `epoch-fence`, `graceful-shutdown` (выход по таймауту, in-flight доезжают), `data-isolation`.
- **acceptance #4 load — СВЕЖИЙ прогон @8d0ae01** (не cite): `npm run test:load` → **3/3 PASS**.
  - `checkpoint`: 10×50 PutCheckpoint, ok=50 errors=0, p50=42ms **p95=104ms p99=127ms** (PDF §11 SLO p95≤300ms — WITHIN), все durable(200); честно «local single-node, NOT target runtime».
  - `latency`: p50=4.1ms **p95=49ms** (PDF §11 control-API p95≤50ms — REPORTED), availability ≥99%.
  - `rss`: peak **19.1 MB** (service=all, goroutines=8, go1.26.2), порог 256MB (PDF §11 footprint — REPORTED, local≠target).
- **Консолидированный pack `--all`** (chaos+load инлайн): PACK_RC=0, summary **{run:8,cited:4,gated:2}** — chaos и load переведены cited→run; гейты только G-EMIL-TOOL (root/kvm-lab) и Tier-L4/IAM/compose-app/OTLP (VM target-runtime).
- Продуктовый код не менялся (`git diff 012a849..HEAD -- internal/ cmd/ go.mod go.sum` = пусто); правлены только delivery `docs/coverage.md`, `docs/progress.md`. source≠deployment сохранён; секреты не печатались.
Блок UNACCEPTED; приёмку ставит только директор.


## Checkpoint-61 (2026-10-30) — fresh chaos @HEAD 6a3fd4e + честный подъём §-матрицы (acceptance #4)
Свежий (не cite) прогон ВСЕХ 6 chaos-тестов на HEAD `6a3fd4e` в изолир. PG `ahp_hive_delivery` (Mac-safe, без -race), G0 native-Qwen assert не нужен (state-plane chaos). Verdict-строки:
- `chaos/restart` → 4/4 PASS (SIGKILL→порт закрыт→тот же DB→healthz+readyz(DB-ping) consistency).
- `chaos/cancel-delete` → 13/13 PASS (Release(canceled)→GetRun status=canceled; Heartbeat на released lease→409 STALE_EPOCH; DeleteThread→200→GetThread→404 каскад; legalhold→delete→403 FORBIDDEN; remove-hold→delete→200).
- `chaos/unknown-outcome` → 13/13 PASS (worker-A PutKV durable; crash без Release→GetRun stays running=UNKNOWN (не failed/canceled); Heartbeat expired lease→409 STALE_EPOCH; worker-B re-claim того же run, epoch A=1→B=2 monotonic; zombie old-fence PutKV→409 STALE_EPOCH; idempotent replay op→same version без дубля; Release done→GetRun done).
- `chaos/epoch-fence` → 8/8 PASS (current-fence→200; stale-epoch PutKV/PutCheckpoint→409 STALE_EPOCH; post-Release old fence→409).
- `chaos/data-isolation` → 5/5 PASS (cross-subject + cross-deploy GetKV→404 invisible).
- `chaos/graceful-shutdown` → 3/3 PASS (чистый выход code=0/signal=null, порт закрыт, grace-период).
ИТОГО 46 assert / 0 fail. Продуктовый код НЕ менялся (`git diff --quiet 012a849..HEAD -- internal/ cmd/ go.mod go.sum` = IDENTICAL). На основании свежего прогона подняты §-строки матрицы (было NOT_VERIFIED) — только реально покрытые state-plane chaos, с явной partial-пометкой чужих lanes: §2.4 SIDE_EFFECT_UNKNOWN, §3.3 зомби-раннер/Fence-новым-раннером, §4.4 каскад-удаления (partial), §9.7 DeleteThread/contexts-delete, §9.8 гонка-двух-реплик-за-run (single-winner). Чужие lanes (space transcript/CoreLoop steering/intent-receipts) оставлены NOT_VERIFIED/partial честно. source≠deployment сохранён (L4 OPEN); секреты не печатались. Блок UNACCEPTED.


### Candidate-repro @ck61b HEAD 8ffbdc95 (2026-10-30, Mac-safe, своя БД ahp_hive_delivery)
Свежие прогоны acceptance #2/#4 воспроизводимы ровно на candidate-HEAD (не cite из ck49/ck60):
- `npm run check` → OK: go vet + go test 44 in-scope пакетов rc=0 (race SKIPPED → VM-gate).
- `npm run smoke` (self-spawn mTLS role=all, AHP_API_URL unset) → `/healthz`=200 `{"status":"ok"}` rc=0 (liveness; не LLM/E2E).
- `npm run test:load` → 3/3 PASS @HEAD: PutCheckpoint ok=50 errors=0 p50=55.0 **p95=96.38** p99=111.56ms (PDF §11 Durable-ACK p95≤300 — WITHIN), все durable(200); control-API p50=5.94 **p95=50.06** p99=54.50ms (§11 ≤50 PoC — REPORTED, local≠target); RSS peak **19.0 MB** (service=all, goroutines=8, go1.26.2) — REPORTED.
- chaos 6/6 (46 assert/0 fail) — см. Checkpoint-61 выше.
Эти 4 набора — то, что независимый директор может воспроизвести на candidate без VM/egress. Target-runtime p95/L4/whole-tree -race остаются VM-gate (iv/director); source≠deployment сохранён. Продуктовый код == 012a849 (docs-only). Emil-hop @HEAD: wiring 200 A2A Task, backend MODEL_UNAVAILABLE (внешний G-EMIL-TOOL).

- `docker compose -f compose.yaml --profile app --profile dev config --quiet` → rc=0 (валидный манифест, без запуска контейнеров; resolved services: go-ahp-all, go-ahp-identity, go-ahp-model, go-ahp-state + go-minio/go-postgres/go-redis/go-vault). Профили app/dev объявляют **раздельные role-сервисы** (identity/model/state/all) — manifest-level «separated service roles» acceptance #1; runtime-независимость ролей подтверждена e2e `separated-roles` (отдельные процессы, независимый /readyz) run:pass. Target-runtime развёртывание compose-app образа = VM-plane L4 OPEN.



## Checkpoint-62 (2026-10-30) — chaos op-key-reuse закрывает §2.4 KEY_REUSE_MISMATCH (acceptance #4)
Свежий (не cite) прогон НОВОГО chaos-теста `tests/chaos/op-key-reuse.mjs` на БОЕВОМ изолир. PG `ahp_hive_delivery` (Mac-safe, без -race), HEAD `904395f1` (продуктовый код == composed `012a849`, `git diff --quiet 012a849..HEAD -- internal/ cmd/ go.mod go.sum` = IDENTICAL):
- **`node tests/chaos/op-key-reuse.mjs` → 10 PASS / 0 FAIL** (авто-discovery glob `tests/chaos/*.mjs`): CreateThread/CreateRun/ClaimRun→lease; PutKV op-1→v1; **идемпотентный replay op-1 (тот же digest)→200 v1 без дубля**; **PutKV op-1 с ДРУГИМ payload→KEY_REUSE_MISMATCH (400)**; **GetKV→исходный {v:1}, переиспользованный {v:999} НЕ персистится (corruption impossible)**; **distinct op-2→200 v2** (guard per-key, не global); **stale-epoch probe переиспользованного op-1→409 STALE_EPOCH (fence раньше idempotency-разрешения)**; Release(done) current-fence→200.
- **`npm run test:chaos` → 7/7 PASS, 0 fail** (было 6/6): restart 4/4 + cancel-delete 13/13 + unknown-outcome 13/13 + epoch-fence 8/8 + data-isolation 5/5 + graceful-shutdown 3/3 + **op-key-reuse 10/10**.
- **Cross-cutting FINDING (НЕ delivery-ownership fix; для iv/director/transport-owner):** `pkg/transport/json.go:103` маппит `contracts.ErrKeyReuse` на **HTTP 400 + code=`INVALID_ARGUMENT`** (message несёт `KEY_REUSE_MISMATCH`). Клиент на wire НЕ получает точный код ошибки `KEY_REUSE_MISMATCH` — он схлопнут в `INVALID_ARGUMENT`. Поведение (отклонение + отсутствие corruption) ВЕРНО и зафиксировано тестом;Transport/код-маппинг ≠ delivery-ownership, поэтому зафиксировано как finding, а не правка.
- §2.4 строка «Ключ операции: digest, KEY_REUSE_MISMATCH, Get/Fence» поднята NOT_VERIFIED→**L2_local_test**. Четыре PDF-угла chaos (restart/cancel/unknown/delete) остаются закрыты + доп. угол §2.4 op-key.
source≠deployment сохранён (L4 OPEN); секреты не печатались. Блок UNACCEPTED; приёмку ставит только директор.

## Checkpoint-65 (2026-10-30T03:2xZ) — CANONICAL WHOLE LIVE JOURNEY = GREEN (боевые Qwen+Emil-core+MinIO+PG)

Снят последний composed-FULL гейт.HQ-контракт `decisions/2026-10-30-live-journey-contract.md` выполнен delivery-ногой.

- **Новый канонический единый путь:** `tests/e2e/live-journey.mjs` (395 стр.) → **17 pass / 0 fail**, `LIVE_JOURNEY=GREEN`, rc=0. Одна цепочка, все hops боевые:
  1. **РЕАЛЬНАЯ native-Qwen** (G0 `native-qwen-assert`=ALLOW, host=`*.modelrun.inference.cloud.ru`, model=`model-run-tqmbu-road`), ответ модели реальный;
  2. **РЕАЛЬНЫЙ Emil-core tool-call** на `:18000` (agent-card `core-agent v1.0.0`): модельIssued `core_skill_activate` с args `{"names":["validate-data"]}`, `function_response=succeeded`, активация с настоящим digest `32db2d496aef…`, `already_active=false`; **проверка по собственной бухгалтерии Emil**: `model_turns=2`, `tool_calls=1`, `completion_reason=completed`, контентно-адресованный артефакт Emil;
  3. **РЕАЛЬНЫЕ байты tool-вывода Emil** → composed data-plane → **боевой MinIO `:29000`**: BeginUpload→presigned PUT(200)→Seal(versioned)→Download, **`sha256(PUT)==sha256(GET)`** (свежий прогон `53a8c59111c8d7f9…`, ранее `ef01e2c668216f78…`), плюс содержательная проверка: скачанные байты несут настоящий `emil.task_id` и настоящий tool-digest;
  4. **РЕАЛЬНЫЙ PG** (`ahp_hive_delivery`): CreateThread→CreateRun(артефакт+version+sha+Emil-task)→GetRun read-back подтверждает все три.
- **Anti-mock/anti-stand-in вшиты в тест (fail-closed):** порт `:19081` (`ahp-vm-tool-agent`, kvm стоп-ин) запрещён assertion'ом; `AHP_E2E_ALLOW_MOCK` запрещён; имя Emil-card обязано быть `core-agent`; отсутствие живой зависимости = FAIL с причиной, не skip.
- **Оркестратор** `tests/e2e/journey.mjs` переписан честно: LEG-1 (12/0, component), LEG-2 (component), **LEG-3 = канонический whole-journey**, и `JOURNEY_WHOLE` теперь выводится ТОЛЬКО из LEG-3 + совпадения `PUT/GET sha`. Свежий сводный прогон: **`JOURNEY=WHOLE-GREEN`, `JOURNEY_WHOLE=COMPLETE`, rc=0**.
- **G-EMIL-TOOL закрыт** (см. обновление гейта выше): причина была внешняя (`LLM_API_BASE=http://127.0.0.1:1`), фикс — вне delivery; delivery зафиксировала восстановление реальным прогоном, а не заявлением.
- **Границы честно:** это **локальный** прогон на Mac с боевыми зависимостями → НЕ target-runtime и НЕ deployment-доказательство (`source ≠ deployment`). Open остаётся: VM-plane (`-race` whole-tree, целевой p95, Tier-0/1/2A/2B развёртывания, OTLP-коллектор) и приёмка root.
- Продукт побайтово == composed `012a849`; изменения только `tests/e2e/*`, `package.json` (delivery-owned скрипт `journey:live`), `docs/*`.

## Checkpoint-66 (2026-10-30T03:1xZ) — fresh re-runs @HEAD + acceptance-pack G-EMIL-TOOL→CLOSED
Свежие прогоны этого сеанса на HEAD=`e7d6ff4` (product code byte-identical seed 012a849):
- **live journey (canonical):** `node tests/e2e/live-journey.mjs` → 17 pass/0 fail, `LIVE_JOURNEY=GREEN`, rc=0. Emil-core `:18000` task `0b5218f6-6da1-4675-b8a1-6bd47c485463`, `core_skill_activate(validate-data)` succeeded, Emil provenance `tool_calls=1 model_turns=2`; MinIO `sha256(PUT)==sha256(GET)=2a3371365b7f8906…`; PG run `20261030T030551…`.
- **reproducibility:** второй независимый прогон внутри acceptance-pack → task `892b5cba-4675-4770-9916-60bd2aabe6f4`, `sha256 PUT==GET=5637abbaf6094498…`, PG run `20261030T030934…`. Journey воспроизводим, не one-shot.
- **`npm run check`:** rc=0 — 44 in-scope пакета `go vet`+`go test` GREEN (race SKIPPED → VM-gate).
- **`npm run smoke`:** rc=0 — mTLS self-spawn `:19619`, `/healthz`=200 `{"status":"ok"}` (liveness only).
- **acceptance-pack:** summary `{run:9, cited:7, gated:1}`; введён run-item `live-journey-whole` (real Qwen→Emil:18000→MinIO PUT==GET→PG); `emil-tool-completion` переведён **gated→run:pass**. Единственный gated = `tier2a2b-l4-iam-compose-otlp` (VM target-runtime).
- **SUPERSEDE:** строки гейтов `G-EMIL-TOOL OPEN/MODEL_UNAVAILABLE` в чекпойнтах ≤64 (coverage 391/395/406/446/456/480/497/519 и пр.) — ИСТОРИЯ, не текущий статус. Актуально: **G-EMIL-TOOL CLOSED** (backend model-base исправлен вне delivery; delivery подтвердила реальным прогоном). Открытые внешние гейты: VM-plane (Tier L4/IAM/compose-app/OTLP; whole-tree `-race`; целевой p95) + `oauth2 v0.36` egress-блокер + приёмка root.

> Re-pin 2026-10-30T07:35Z: integrated checkout (Space 90a33a5 + executor) — pdf-closure 18/18 mapped; fresh Mac-safe gates on revision bin sha 88213c15.. recorded in acceptance-pack.json (macSafeGatesRefresh2026_10_30). OPEN gates unchanged: VM target-runtime, a2a model/tool via separated execution plane (workload-principal/L4), oauth2 v0.36 (no egress).

## Checkpoint-67 (2026-10-30T15:0xZ) — ISO human-origin component (:29700 + :29710) — component ACCEPTED, journey L4 OPEN

Director route `20261030T145544Z`: изолированный human-origin слой принят **как component** (login+ListThreads+human-origin proxy). Это **не** whole-journey приёмка; `:29612` стабильный mTLS-бэкенд не тронут.

- **Композиция по хэшу** (`scripts`+checkout candidate HEAD `98d7d09`): base `986ac0a` + Identity browser `9b143de` + canonical `pkg/auth` `2c42002` (jti omitempty; смержен с browser — `pkg/auth` unit rc0) + State durable `ListThreads` `7df96b34`. `go build -tags ahp_exec,ahp_data` **rc0 офлайн**; `tests/identity` browser rc0. `oauth2 v0.36` реально резолвится из host file-cache (rc0), без downgrade/replace.
- **:29710 human-origin** (`scripts/human-origin.mjs`, hardened sha `01889283…`): отдаёт UI (`Agents Space`, no-store/nosniff), проксирует `/u-ahp/v1/*`→`:29700`, server-side mTLS-termination (клиент-сертификат только на origin), HttpOnly+SameSite=Strict, **unbuffered SSE/Last-Event-ID**, static assets по allowlist. **Independent unit test БЕЗ Docker** `npm run verify:human-origin` → **зелёный** (healthz/UI/traversal→404/HttpOnly+SameSite/CSRF-inject/unbuffered-SSE). Найден и исправлен дефект своего tooling (`server.listen` arg-order; %20 path).
- **VM isolated harness** (`scripts/vm/iso-vm-harness.sh`+`vm-journey.mjs`, HEAD `98d7d09`): hash-import, изолированный PG (allowlist `ahp_hive_[a-z][a-z0-9_]{0,23}`), mTLS CA/server/client, MinIO bucket-ns, foreground `:29700`(mTLS)+`:29710`; journey-verify: HARD-ноги (login HttpOnly/me/**two-subject**/restart) + `run→model→artifact→download sha PUT==GET` **GATED** до re-pin Space facade + execution canonical `core.Runtime` (RFC-001) — не фабрикуется.
- **Status по layer-легенде:** proxy/human-origin = **L2_local_test** (offline unit, source≠deployment). Live browser-chat journey (login-through-origin→Space AcceptTurn→real model→tool→versioned artifact→durable PG→restart) = **L4_target_runtime OPEN**: ждёт (1) изолированный VM AHP test PG/mTLS namespace `ahp-*` (root personal VM; Mac Docker-сокет отсутствует — Desktop вслепую не поднимал), (2) identity-freeze bundle supersede `9b143de` + space contract-фиксы → re-pin facade (`20261030T145405Z`), (3) execution canonical runner (execution lane), (4) `launchctl bootstrap` персистентности = host-op (root). Целый прототип **UNACCEPTED**; приёмку ставит только директор/IV.
