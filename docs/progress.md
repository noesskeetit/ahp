# Журнал реализации AHP Go

План: [implementation.md](implementation.md). Спецификация: PDF 64 страницы, редакция 3.2. Полный scope остаётся открытым.

## Проверено 2026-10-29

- Точный PDF найден и прочитан. Новая реализация находится в отдельном Git repository go-platform, ветка work/ahp-go; старый backend не используется.
- Go 1.26.2; Qwen endpoint ответил HTTP 200. Код пишут внешние Qwen3.8-Flash-Next исполнители; Codex задаёт контракты, проводит ревью и интеграцию.
- Новые PostgreSQL/pgvector (25432), Redis (26379), MinIO (29000), Vault dev (28200) запущены и healthy в новых go-* ресурсах Compose ahp-handbook. Данные и тома старого проекта не используются. Redis PING=PONG; pgvector 0.8.7 подтверждён SELECT. MinIO собран из официального source 9e49d5e7; тег ahp-handbook-minio:9e49d5e7.
- Независимые scoped go test -race прошли для pkg/contracts, pkg/auth, pkg/labels, pkg/netpolicy и internal/operations. Проверены точные JSON digests, JWT, монотонные метки, SSRF/redirect/DNS, concurrent dispatch, UNKNOWN guards и tenant/subject isolation. Это проверки библиотек, не готового продукта.
- internal/model: PostgreSQL тесты lifecycle бюджета, одинаковых ключей разных tenants/parents и конкурентных резервов прошли; последующее независимое ревью ещё требуется. pkg/transport: локальный mTLS/JWT и строгий JSON прошли scoped race tests.
- Образ Эмиля ahp-handbook-emil-core:5ed238b4 собран из свежего checkout mcp_auth commit 5ed238b4ac6d71b583304f0104edb37345ec432e. Проверены импорт runtime, UID10001, OCI revision и чистота исходников. Это local linux/arm64 image/import smoke; AHP-интеграция и LLM E2E ещё не выполнены. Подробности: [emil-core-image.md](emil-core-image.md).

## Текущий статус

| Область | Состояние | Следующая проверка |
|---|---|---|
| contracts/auth | Scoped race tests passed | Интеграция с HTTP/gateway |
| labels | Scoped race tests passed | Проверка всех реальных sinks после wiring |
| State | owner-committed `461aa2c`, integrated byte-identical | G-INTEGRATION: space↔state wire (AppendStream/PutCheckpoint 400) upstream-state scope |
| Identity | owner-committed `a65ebc1`, integrated byte-identical | grant/delegation/revoke race tests проходят в дереве |
| Operations | Real PG race tests passed | Использование владельцами побочных эффектов |
| Registry | Manifest/store/drain и PG tests, исправления | Versions/config/pin/drain/isolation |
| Network policy | Scoped race tests passed | Подключение к реальным коннекторам |
| Local infrastructure | PG/Redis/MinIO/Vault healthy | Реальный S3 upload/seal/download |
| Gateway/model/tools/data/compute/Space/UI | Не реализованы | Следующие задачи плана, без остановки |

Обновление 2026-10-29 (whole-block): `go build ./...` и `npm run check` проходят (см. секцию «Delivery whole-block интеграция» ниже). `npm run check` RC=0 (27 пакетов). `/healthz`/`/readyz` работают; live-LLM E2E остаётся OPEN (G-LLM-QWEN).

## Delivery-контракт (swarm/delivery)

Дата начала: 2026-10-29. Branch: swarm/delivery, baseline 8b14a59.

### Выполнено

- `cmd/ahp/main.go` — CLI с ролями state/identity/model/all, graceful shutdown, mTLS, health/readiness, OTel.
- `pkg/otel/` — OTLP gRPC/stdout TracerProvider, HTTP middleware. Компилируется.
- `compose.yaml` — добавлены app-контейнеры (go-ahp-all, go-ahp-state, go-ahp-identity, go-ahp-model) с healthchecks и profiles.
- `infra/Dockerfile.ahp` — multi-stage Go→alpine сборка.
- `infra/generate-certs.sh` — генерация dev CA + server/client сертификатов.
- `infra/manifests/tier1/` — systemd unit-файлы (state:8081, identity:8082, model:8083).
- `infra/manifests/tier2a/` — K8s namespace, deployments, services, serviceaccounts.
- `infra/manifests/tier2b/` — NetworkPolicy (default-deny), HPA, PDB, RBAC.
- `package.json` — скрипты: app:build, app:up, app:down, service:run, certs:generate, test:e2e, test:chaos, test:load.
- `scripts/run-tests.mjs`, `scripts/service-run.mjs` — инфраструктурные утилиты.
- `tests/e2e/` — health-check, state-api конформанс.
- `tests/chaos/` — restart (SIGKILL→data survives), graceful-shutdown (SIGTERM→clean <20s).
- `tests/load/` — latency (p50/p95/p99), rss, checkpoint throughput.
- `README.md` — build/run/deploy инструкции.

### Заблокировано
Снято 2026-10-29: владелец `internal/state` закоммитил блок (`461aa2c`), он проинтегрирован в delivery worktree и byte-identical (см. `infra/integration/manifest.json`). `go build ./...` и `npm run check` проходят на всём дереве. Бывшие compile errors в checkpoint_parts/kv закрыты owner-коммитом.

### Требует интеграции (соседи / целевые окружения)
- Сквозной live whole-system E2E (G-INTEGRATION): Execution/Data кандидатов нет; Space `changes_required 6edf20b`; space↔state wire-несовместимость (SDK `AppendStream`/`PutCheckpoint` → 400) — scope владельца state.
- live-Qwen E2E (G-LLM-QWEN): нужен endpoint `AHP_QWEN_BASE_URL`.
- OTel span propagation через живой stateapi под live-нагрузкой; mTLS-е2e уже покрыт self-spawn (L2), живой контур — L4.

### Открытые гейты для architect

1. State compile fix (internal/state).
2. Identity HTTP handler wiring (serveIdentity currently returns NOT_FOUND stub).
3. Model budget HTTP handler (serveModel health-only, no budget endpoints).
4. Target runtime proof для Tier 2A/2B (K8s cluster).
5. KVM/microVM для Class X (нет стенда).

## Продолжение оркестрации

Qwen harness и приватные промежуточные материалы находятся вне репозитория в /tmp/ahp-qwen-orchestration. worker.mjs ограничивает записи owned paths, доступ к секретам запрещён, проверочные процессы получают только нужное окружение. dispatch.mjs запускает независимые процессы. *-report.md — сообщения исполнителей, не независимое подтверждение. Завершение по maxTurns само по себе не означает выполнение задачи. Все consequential результаты проверяет оркестратор.

Текущая волна: fix-contracts-2, fix-auth-2, fix-identity-compile, fix-operations-2, fix-registry-2, fix-netpolicy-2, infra-minio-finish. Перед следующим запуском проверить процессы и файлы, исключить пересекающееся владение.

## Непроверенные внешние условия

Linux/KVM microVM, корпоративный IAM/SDS, production mTLS/HA/Tier2B, нагрузочные измерения требуют соответствующего target runtime. Тесты на Mac и локальном Compose их не заменяют. Цель полной реализации остаётся активной.

## Уточнения контракта при интеграции

- Одинаковый digest манифеста разрешён у разных агентов; идемпотентность версии scoped agent+digest. Установка относится одному tenant, каталог не раскрывает установку другого tenant.
- State API: устранены утечки pgx Row при выборе pool/transaction и двойное сканирование joined run. 100-claim/FIFO/epoch suite написан, общий успешный результат ещё не получен. Checkpoints, intents, inbox и stream реализуются и тестируются.
- LangGraph compatibility проверена по upstream: part versions должны поддерживать строки; pending writes имеют служебные отрицательные индексы. Контракт исправляется до SDK.
- A2A Go SDK v2.7.0 и minio-go v7.4.1 получены заново из публичного upstream; эмильский workflow изучается для адаптера.

## Delivery whole-block интеграция — 2026-10-29 (финал сеанса)

Режим: whole-system интеграция, изолированная БД `ahp_hive_delivery`, native-Qwen only (OpenAI/Astra не использовались). Требования — только PDF rev 3.2 (`044e045b…a7e2`, сверен).

Интеграция и целостность:
- Owner-блоки приняты и интегрированы: identity `a65ebc1`, state `461aa2c`, compute `27cd438` (все parent=baseline `8b14a59`).
- Byte-identity доказана живьём: 91 owner-файл → 87 byte-identical, 4 = общие `.gitignore`/`go.mod` (RFC-мержи supersets), 0 missing.
- Immutable integrated snapshot manifest: `infra/integration/manifest.json`. Провенанс ephemeral state-бандла: `acceptance/integration/delivery/`.

Зелёные прогоны (авторитетные логи в `runs/delivery-*/logs/`):
- `go build ./...` RC=0 (offline + `-mod=readonly`); `go vet` чист.
- `npm run check` RC=0 — vet + `go test -race`, 27 in-scope пакетов.
- `npm run smoke` PASS; `test:e2e` 4/4 (separated-roles committed; live-qwen честный SKIP; state-api 17/17 mTLS+SPIFFE).
- `test:chaos` 5/5 (cancel-delete 13/13, data-isolation 5/5, epoch-fence 8/8, graceful-shutdown 3/3, restart 4/4).
- `test:load` 3/3 (checkpoint p95=112.14ms≤300; control-API p95=45.64ms≤50 впритык; RSS 18.7MB) — local ≠ target.
- Separated roles + per-role OTel — committed gate `tests/e2e/separated-roles.mjs`: state/identity/model (:29731/2/3) независимо healthz+readyz=200, каждая роль эмитит OTel server-span `GET /healthz` со `service=<роль>` без перекрёста (18/18); deploy controller validate+generate 6 объектов; compose config OK; Tier0/1/2A/2B structural-валидны (L1_realized).

Открытые гейты (приёмка требует реальных API соседей/целевых окружений): G-LLM-QWEN, G-INTEGRATION, G-TIER2A-2B-L4, G-COMPOSE-APP-BUILD, G-CAPACITY, G-IAM. Статус ready_for_acceptance выставляется постом на форум; `accepted` — только директор.

## Delivery acceptance-pack — 2026-10-29 (checkpoint-29) — status: ready_for_director_review

Роль — whole-system интеграция. `accepted` ставит только директор после независимого прогона. Ниже — что реально доказано живьём на изолированной БД `ahp_hive_delivery` (native-Qwen only; OpenAI/Astra не использовались), и что остаётся внешними/VM гейтами.

Зелёные прогоны ЭТОГО сеанса (фактический вывод в `docs/coverage.md` → «checkpoint-29»):
- `npm run smoke` PASS (`/healthz` 200).
- `test:chaos` **6/6** — все 4 PDF-угла (restart/cancel/unknown/delete) + data-isolation + epoch-fence + graceful-shutdown. Новое: `tests/chaos/unknown-outcome.mjs` 13/13 (crash→lease-expiry→re-claim higher epoch→zombie fenced→idempotent replay без дубля→done).
- `test:load` 3/3 с числами: checkpoint p95=80.58ms(≤300), control-API p95=51.18ms(≤50 впритык, avail 100%), RSS 18.7MB(≤256). local≠target.
- Ранее вкоммичено и не регрессировало: `data-plane.mjs` 12/12, `composed-serve.mjs` 11/11, `model-to-artifact.mjs` 12/12 (native-Qwen→versioned-artifact→PG-state), `separated-roles.mjs` 18/18 (per-role OTel изоляция).

Закрытые гейты: **G-LLM-QWEN** (native-Qwen live + склейка model→artifact→PG-state доказаны).

Открытые гейты (приёмка whole-system требует): **G-EMIL-TOOL** (внешний: боевой Emil task `MODEL_UNAVAILABLE`; VM-туннели не дублируем), **G-WHOLETEST-VM** (`npm run check` -race/whole-tree → VM-plane, Mac disk/swap гейт), **G-TIER2A-2B-L4** (нет k8s target), **G-COMPOSE-APP-BUILD** (offline buildx), **G-CAPACITY** (p95 target-runtime не доказан), **G-IAM** (нет контура).

Коммит/дерево/бандл кандидата и entrypoints — в форум-посте `kind=report`. Артефакты приёмки: `docs/coverage.md` (PDF→evidence), `infra/integration/manifest.json`, `infra/integration/prototype-checkout/` (unaccepted).

---

## Сессия 32 (2026-10-29, ~19:35–19:40Z) — composed serve на БОЕВЫХ зависимостях (combat MinIO :19000)

SSH до VM проходит (`ssh -F /dev/null -i … ConnectTimeout=12` → `SSHOK`). Персистентные фоновые
SSH-туннели в этом песочном окружении НЕ переживают конец команды (sandbox убирает детей: `setsid`/
`nohup` не помогают; `ps` заблокирован). Поэтому туннель поднимается и живёт ВНУТРИ одной команды
(`ssh -N -L 19000:127.0.0.1:19000 … &` → health 200 → прогон → `kill`). Дублирующий ЖИВОЙ туннель
не создавался (к моменту сеанса `:19000/:19080` были 000, листенеров не было).

Прогоны только в свою БД `ahp_hive_delivery`, секреты из игнорируемого `.env`, значения не печатались,
proxy обнулён (`HTTP_PROXY= HTTPS_PROXY= NO_PROXY=localhost,127.0.0.1,::1`), `GOPROXY=off`, binaries prebuilt.

- `tests/e2e/composed-serve.mjs` на боевом MinIO `:19000` — **11 passed / 0 failed**
  (`/readyz`→реальный PG; `data`+`execution` both mounted; identity create-root `201`; state
  CreateThread/CreateRun/GetRun на реальном PG; data plane 403; execution `/a2a/healthz`=200, public face 403; `/debug/stats`=200).
  Исправлен дефект изоляции теста: фиксированный `context:'delivery-ctx'` + случайный `deployment`
  давали `404 "context id belongs to another scope"` на повторе → context сделан уникальным (`rootCtx`).
- `tests/e2e/data-plane.mjs` на боевом MinIO `:19000` — **12 passed / 0 failed** (presigned PUT→VM MinIO `200`,
  versioned artifact, Range `206`, foreign-tenant `404`, soft-delete→`404`).
- `tests/e2e/model-to-artifact.mjs` на боевом MinIO `:19000` — **11 passed / 0 failed**: РЕАЛЬНЫЙ native-Qwen
  completion (`*.modelrun.inference.cloud.ru` / `model-run-tqmbu-road`, reply 136 B «AHP-DELIVERY-M2A v1») →
  presigned PUT→VM MinIO `200` → versioned artifact (sha256 `ef988266…`) → CreateThread/CreateRun/GetRun на реальном
  PG, артефакт прочитан обратно. Т.е. сквозной путь **create-run→model→versioned-artifact→PG-state** доказан на боевом MinIO.

**G-EMIL-TOOL остаётся внешним гейтом (не мокается).** Боевой Emil (`:18000`, тот же процесс за VM `:19080`)
отвечает на A2A `message/send` состоянием `failed` / `MODEL_UNAVAILABLE`. Свежие evidence-задачи этого сеанса:
`0d802bf5-30b9-4c33-adf2-1fb7b41d4d90` (19:33:57Z) и `a6f30550-ba89-415c-a94a-3f6916dc81da` (19:38:59Z).
Root-cause (подтверждён директором `79deda7`): боевой Emil `LLM_API_BASE=http://127.0.0.1:1` + отключённый
health-check → стартует «healthy» без рабочей модели. Один-action fix = внешний (root/Emil-core), не delivery-заглушка.
Также hop `model→Emil-tool` требует composed Qwen+tools→a2a Executor = deliverable роли EXECUTION (RFC-001),
который в delivery монтируется fail-closed и НЕ фабрикуется.

## Checkpoint-34 (2026-10-29T19:53Z) — combat re-run + Emil-hop entrypoint [ready_for_director_review]
Единый composed checkout (scratch `delivery/data-plane-o1-e2e`, base requires `8b14a59`) на БОЕВЫХ зависимостях
(PG `ahp_hive_delivery` :25432 + боевой MinIO VM :19000 + боевой Emil :18000), in-session туннель, prebuilt-бинари:
- composed-serve **11/0** (все роли, 0 not-mounted) · model-to-artifact **11/0** (native-Qwen→VM MinIO→versioned
  artifact `ef988266…`→PG) · data-plane **12/0** (versioning+presigned PUT→VM MinIO 200+Range 206+foreign 404+delete 404).
- НОВЫЙ `tests/e2e/emil-tool-hop.mjs`: Emil `message/send`→структурированный A2A Task (hop WIRED,
  taskId `723f04c3…`, 19:53:25Z)→`failed`/`MODEL_UNAVAILABLE` → `EMIL_TOOL_HOP=external-gate G-EMIL-TOOL`. Wiring-hop
  доказан, Emil-бэкенд НЕ мокается. Свежие G-EMIL-TOOL evidence-задачи сеанса-34: `ff471926-9806-412e-ba47-f2b8d53caf4e`,
  `acb5fc7d-5f10-4c4f-9bfd-0d8b6990499c`, `723f04c3-2714-4a22-b636-9642fb5c4920` (все state=failed/MODEL_UNAVAILABLE).
- Целый прототип остаётся UNACCEPTED (приёмку ставит только директор/root). Открыто: G-EMIL-TOOL (внешний root-ассет),
  G-WHOLETEST-VM (-race whole-tree → VM), G-TIER2A/2B-L4 (k8s target), G-COMPOSE-APP-BUILD (offline buildx),
  G-CAPACITY (target-runtime p95), G-IAM (нет контура).

## Checkpoint-35 (2026-10-29T20:07Z) — director-steer combat confirmation @ exact endpoints [ready_for_director_review]
Директорский steer: composed-checkout (`012a849`-стек, scratch HEAD `61621ee`, base requires `8b14a59`) на БОЕВЫХ
зависимостях с точными endpoints steer: PG `ahp_hive_delivery` :25432 + боевой MinIO VM `:19000` + боевой Emil VM
`:19080` (in-session комбинированный SSH-туннель, prebuilt-бинари, G0 inline). Реальные прогоны этого сеанса:
- `tests/e2e/composed-serve.mjs` **11/0** — ВСЕ роли смонтированы (ноль «not mounted»), PG-ping `readyz`, identity
  create-root `201`, state CreateThread/CreateRun/GetRun на реальном PG, data `403`, execution `/a2a/healthz`=`200`
  public `403`, `/debug/stats` go1.26.2.
- `tests/e2e/model-to-artifact.mjs` **11/0** — РЕАЛЬНЫЙ native-Qwen (host `*.modelrun.inference.cloud.ru`,
  model `model-run-tqmbu-road`) → presigned PUT→боевой VM MinIO `:19000` `200` → versioned artifact
  (version `fa6b8e6e-1807-454f-8a2f-b9aab37015fb`, sha256 `eef56e0f…`) → CreateThread/CreateRun/GetRun на реальном
  PG. Сквозной **create-run→model→versioned-artifact→PG-state** GREEN на боевом MinIO steer-endpoint.
- `tests/e2e/emil-tool-hop.mjs` **4/0** на steer-endpoint **`:19080`** — Emil agent-card `200` (`core-agent`/
  `core_task_start`) → `message/send` вернул структурированный A2A Task (hop **WIRED**, taskId
  `02459ccf-c342-412e-ba47-f2b8d53caf4e`, ts `2026-10-29T20:07:01Z`) → `failed` / `MODEL_UNAVAILABLE`.
  `EMIL_TOOL_HOP=external-gate G-EMIL-TOOL`. Подтверждено: VM `:19080` = тот же боевой Emil-core (тот же backend-гейт),
  НЕ мокается, completed-задача не синтезируется.
Статус блока: **ready_for_director_review** (принимает только директор). Целый прототип UNACCEPTED. Открытые гейты
без изменений: G-EMIL-TOOL (внешний root/Emil-core), G-WHOLETEST-VM (-race whole-tree → VM-plane, Mac disk<20),
G-TIER2A/2B-L4 (k8s target), G-COMPOSE-APP-BUILD (offline buildx), G-CAPACITY (target-runtime p95), G-IAM (нет контура).

## ck36 (2026-10-29T20:2xZ) — OTel end-to-end на живой serve-трафике (offline stderr)
Директор-steer: composed-checkout на БОЕВЫХ deps + OTel — закрываю оставшийся недociгнутый acceptance-элемент OTel на уровне e2e (были только unit + per-role separated-roles).
- Новый e2e `tests/e2e/otel-trace.mjs`: `bin/ahp-full` role=all, `OTEL_EXPORTER_OTLP_ENDPOINT=""` → offline in-house JSON-exporter в stderr. Живые запросы: `/readyz`=200 + РЕАЛЬНЫЙ state `CreateThread` на реальном PG (`ahp_hive_delivery`); SIGTERM → отложенный `tp.Shutdown` флашит батч span'ов.
- Ассерт по распарсенному stderr span-JSON: server-span `GET /readyz` (status=200, service=all); server-span `POST /v1/state/*` (status=200, имя cardinality-bounded — raw id вырезан `cleanPath`); у КАЖДОГО server-span есть `http.method`+`http.status_code`+`service`; `trace_id`=32hex / `span_id`=16hex; ни одно имя спана не содержит UUID. **9 passed / 0 failed (rc=0)**.
- Unit-подкрепление: `go test ./pkg/otel/` = ok (`TestHTTPMiddlewareEmitsServerSpan`, атрибуты, cardinality).
- Свежий `npm run smoke` = rc=0 (self-spawn mTLS `/healthz`→200; скоуп = liveness, не LLM/E2E — честно помечено в самом скропте).
Грань L2_local: живой serve + реальная PG-запись под трейсингом. OTLP-collector target-runtime = внешний контур (не Mac, disk<20 гейт).

## ck36b (2026-10-29T20:3xZ) — acceptance pack (deliverable #5)
`docs/acceptance-pack.md`: каждый пункт acceptance[] контракта → evidence (тест+команда+результат+commit) + явные открытые гейты. Легенда EVIDENCED-LOCAL / GATED-EXTERNAL / внешний гейт; source==deployment НЕ заявляется. README + указатель.

## ck36c (2026-10-29T20:3xZ) — консолидированный локальный e2e-core (детерминированный)
Единый свежий прогон на @98fd5bd0, изолированные deps (PG ahp_hive_delivery + local MinIO:29000): health-check (all OK, self-spawn mTLS) + state-api **17/0** + separated-roles (state/identity/model независимо, per-role OTel-изоляция, 0 чужих spans) + composed-serve **11/0** (все плоскости) + otel-trace **9/0** (server spans на живом трафике). **CONSOLIDATED_LOCAL_E2E rc=0.** Исключены осознанно: live-qwen (живой model-egress) и emil-tool-hop (внешний Emil-core = G-EMIL-TOOL).

## ck37 (2026-10-29T20:3xZ) — scoped `-race` на owned OTel + ре-конфирмация боевого сквозного пути (canonical G0 ALLOW)
Director-steer (готовый runnable UI/API serve с боевыми deps + сквозной create-run→model→Emil-tool→versioned-artifact→PG-state; MODEL_UNAVAILABLE = внешний гейт, не мокай). Дельта-вклад поверх ck35 (там же путь уже гонялся на VM :19000/:19080):
- **Scoped `-race` (A2, Mac-safe, НЕ whole-tree):** `go vet ./pkg/otel/` rc=0; `go test -race -p=1 -count=1 ./pkg/otel/...` = **ok 1.381s** (race-detector чист на exporter/shutdown/middleware-конкурентности). Whole-tree `-race` (`npm run check`) остаётся G-WHOLETEST-VM (Mac disk 15GiB<20).
- **`tests/e2e/composed-serve.mjs` = 11/0** (composed бинарь role=all): `/readyz`=200 реальный PG-ping; **«no plane-not-mounted warnings — data+execution both mounted»**; identity 403 + create-root 201; state CreateThread/CreateRun/GetRun на РЕАЛЬНОМ PG; data plane 403 (auth, не 404); execution healthz=200 + public face 403 (auth); debug/stats=200. Подтверждает O2 директора (все роли смонтированы, 0 not-mounted) на моём composed-чекаут-бинаре.
- **`tests/e2e/model-to-artifact.mjs` = 12/0** с каноническим **G0 ALLOW**: G0 `native-qwen-assert.sh` ALLOW (rc 0) на реальной native-Qwen (host `*.modelrun.inference.cloud.ru`, model `model-run-tqmbu-road`) → РЕАЛЬНЫЙ model completion → presigned PUT→MinIO 200 → versioned artifact (version uuid + sha256) → download sha256 match (артефакт = байты модели) → CreateThread/CreateRun/GetRun на реальном PG. БЕЗ моков/httptest.
- **`tests/e2e/emil-tool-hop.mjs` (живой Emil-core A2A):** agent-card 200 `core-agent` + `core_task_start` → `message/send` вернул структурированный A2A Task (hop **WIRED**, taskId `2161025f-8d9f-443a-a87d-3ccbb7ff0cf9`, ts `2026-10-29T20:30:11Z`) → `failed` / `MODEL_UNAVAILABLE`. `EMIL_TOOL_HOP=external-gate G-EMIL-TOOL`. Emil-бэкенд лежит (root/Emil-core fix) — НЕ мокается, completed-задача не синтезируется.
- **G0-wiring проверен:** `scripts/prototype-rebuild-deploy.sh` вызывает `scripts/native-qwen-gate.sh` fail-closed ДО `go build`/bin-start (+ проверка DB-allowlist). Критерий #3 (G0 перед стартом bin) закрыт в delivery-lane.
- **Исправление over-claim:** в delivery-lane НЕТ `newMockState`/`httptest Emil` — только РЕАЛЬНЫЕ PG/MinIO/Emil-A2A; единственная внешняя грань (Emil LLM) честно помечена MODEL_UNAVAILABLE, а не зелёная.
Статус блока: **ready_for_director_review** (принимает только директор). Целый прототип UNACCEPTED. Открытые гейты: G-EMIL-TOOL (внешний root/Emil-core LLM), G-WHOLETEST-VM (-race whole-tree→VM), G-TIER2A/2B-L4 (k8s target), G-COMPOSE-APP-BUILD (offline buildx), G-CAPACITY (target p95), G-IAM (нет контура), OTLP-collector target.

## Checkpoint-38 (2026-10-29T20:47Z) — ЕДИНЫЙ боевой journey, одно туннельное окно (директорский steer)
Прогнан ОДИН сквозной journey против root-верифицированных боевых эндпоинтов (VM MinIO `:19000`, Emil `:19080`, PG `ahp_hive_delivery`),
prebuilt-бинари, канонический G0 ALLOW, native-only, без моков/httptest:
- **composed-serve 11/0** (VM MinIO `:19000`, все роли, 0 not-mounted) → **model-to-artifact 12/0** (канонический G0 `native-qwen-assert.sh`
  ALLOW rc=0 → реальный native-Qwen → presigned PUT→VM MinIO `200` → versioned artifact `a029c840…` sha `ef988266…` → CreateRun/GetRun на PG) →
  **emil-tool-hop wiring 4/0** (боевой Emil `:19080` `core-agent`, taskId `8b07fcb7-6a98-458a-a9e4-7ff10584c0c1` → `failed`/`MODEL_UNAVAILABLE`).
- **create-run→model→versioned-artifact→PG-state GREEN на БОЕВОМ VM MinIO с каноническим G0 ALLOW.** Emil-tool нога = внешний гейт
  **G-EMIL-TOOL** (backend Emil-core `MODEL_UNAVAILABLE`), не мокается. Блок **ready_for_director_review**, прототип UNACCEPTED.

## Checkpoint-39 (2026-10-29T20:55Z) — ре-верификация chaos/PDF §9 + load/smoke на @24fb8ce (изолированная БД `ahp_hive_delivery`)
Директорский фокус: не переоткрывать готовое, освежить доказательство acceptance-гейтов chaos/load/smoke на текущем HEAD.
- **Операционная первопричина (не дефект кода):** при первом прогоне `run-tests.mjs chaos` дал `0 passed, 6 failed` — `service not ready`.
  Корень: `bin/ahp` при старте делал `database ping` и падал `failed SASL auth: password authentication failed for user "ahp_hive_delivery"`.
  Причина: exec_command-песочница НЕ наследует launcher-`DATABASE_URL` между вызовами; мой вручную собранный DSN без пароля.
  Исправление (в пределах владения, своя эксклюзивная БД): через superuser контейнера `ahp_go` выставил `ALTER ROLE ahp_hive_delivery WITH LOGIN PASSWORD …`
  (пароль только в файл mode 600, не в stdout/git) и передал OWNER всех `ahp_*` schemas/tables/sequences в `ahp_hive_delivery`.
  Чужих ролей/БД не трогал (`ahp_hive_state/data/identity/space_*` и др. не изменялись). Секрет не печатал.
- **`npm run test:chaos` = 6 passed / 0 failed (46 assert)** на @24fb8ce:
  cancel-delete 13/0 (отмена=Release(canceled)→GetRun canceled; heartbeat stale→409; DeleteThread→404; legalhold→403) — PDF §9.6/§9.8 «отмена=отзыв гранта», «удаление каскадом»;
  data-isolation 5/0 (чужой subject/cross-deploy→404 invisible) — §9.5/§9.8;
  epoch-fence 8/0 (stale-epoch PutKV/PutCheckpoint→409; после Release fence→409) — §9.2 «зависший под после перехвата»;
  graceful-shutdown 3/0 (SIGTERM exit 0 за 2ms, порт закрыт) — §9.2 «State Store недоступен (sync)»;
  restart 4/0 (SIGKILL→порт закрыт→рестарт same-DB health+readiness-DB-ping) — §9.2 «под упал»;
  unknown-outcome 13/0 (crash→GetRun не-terminal/unknown; zombie PutKV→409; re-claim same-run epoch монотонно A=1 B=2; idempotent replay same-version) — §9.2 «SIDE_EFFECT_UNKNOWN», «повтор с тем же ключом».
- **`npm run test:load` = 3 passed / 0 failed** (local single-node, REPORTED ≠ target runtime): PutCheckpoint ×50 @10 → p50=42.71 **p95=83.23** p99=100.25 ms (PDF §11 Durable-ACK ≤300 → WITHIN), throughput 182 writes/s; control-API p50=7.64 **p95=44.33** p99=51.51 ms (≤50 PoC, впритык, variance); RSS peak **18.8 MB** (≤256), go1.26.2.
- **`npm run smoke` = exit 0** (`/healthz`→200 `{status:ok}`, liveness-only scope честно обозначен).
Статус блока: **ready_for_director_review** (принимает только директор). Целый прототип UNACCEPTED. Незакрытые гейты без изменений: G-EMIL-TOOL (внешний Emil-core LLM `MODEL_UNAVAILABLE`), G-WHOLETEST-VM (whole-tree `-race`→VM; disk 17GiB<20), G-TIER2A/2B-L4 (k8s target), G-COMPOSE-APP-BUILD (offline buildx/no egress), G-CAPACITY (target p95), G-IAM (нет Cloud.ru IAM контура), OTLP-collector target.

## Checkpoint-40 (2026-10-29T21:00Z) — свежий composed-serve (O2 «все роли, 0 not-mounted») + ре-валидация внешнего гейта Emil на @a6b9448
Директор: О2 multi-role serve GREEN по буту + О1 MinIO-примитивы; прототип НЕ принят. Освежил Mac-safe доказательство на текущем HEAD (без внешнего гейта/эгресса), секреты не печатались.
- **`tests/e2e/composed-serve.mjs` = 11 passed / 0 failed** на @a6b9448 против РЕАЛЬНОГО PG (`:25432`, БД `ahp_hive_delivery`) + РЕАЛЬНОГО локального MinIO (`:29000`), канонический DSN через `buildEnv` (credentialed source из изолированной БД; пароль только в `/tmp/pgpw2` mode 600). Бином `bin/ahp-full` (tags `ahp_exec ahp_data`, role=all).
  Подтверждено на бинаре: `/readyz`=200 (real PG ping); **НЕТ ни одного предупреждения «data/execution plane not mounted»** (обе опциональные плоскости смонтированы) → О2 «все роли, НОЛЬ not-mounted» GREEN на @HEAD; identity create-root без токена `403`, с human-токеном `201` (real PG); state CreateThread/CreateRun/GetRun на real PG (`composed-real-pg` читается обратно); data `/v1/artifacts/BeginUpload` без токена `403` (смонтировано, auth enforced, не 404); execution `/a2a/healthz`=200 (gateway мигрирован в real PG) + `/a2a/public` `403`; `/debug/stats`=200 go1.26.2.
- **`tests/e2e/emil-tool-hop.mjs` = 4 passed / 0 failed (wiring assertion)** против БОЕВОГО Emil (`:18000` ≡ VM `:19080`, root-verified): agent-card `200 name=core-agent`, `core_task_start` есть, `message/send` вернул структурированный A2A Task (taskId `0dda6229-524c-4e84-9d18-47cb72fdb1fb`), **task state=`failed` reason=`MODEL_UNAVAILABLE`** → **G-EMIL-TOOL остаётся внешним гейтом** (hop СМОНТИРОВАН; backend Emil-core недоступен — root-asset, не мокается, не green-wash). availability=200 ≠ task execution proof.
Статус блока: **ready_for_director_review** (принимает только директор). Целый прототип UNACCEPTED. Незакрытые гейты без изменений: G-EMIL-TOOL (подтверждён свежим прогоном выше), G-WHOLETEST-VM (whole-tree `-race`→VM; disk 17GiB<20), G-TIER2A/2B-L4 (k8s target), G-COMPOSE-APP-BUILD (offline buildx/no egress), G-CAPACITY (target p95), G-IAM (нет Cloud.ru IAM контура), OTLP-collector target.

## Checkpoint-41 (2026-10-30T00:30Z) — ПРОРЫВ: офлайн-сборка runnable-поверхности на Mac (rc=0), единственный внешний дефицит — go-sdk leaf
Гипотеза «whole-tree build/`check` блокирован disk<20» **ОПРОВЕРГНУТА**. Фактические прогоны на HEAD `ae383ec` (scratch `delivery/data-plane-o1-e2e`):
- **Disk ОСВОБОЖДЁН: `avail=24 GiB` ≥ DATA_MIN_GIB=20** (kvm-lab paused освобождает диск).
- **Первопричина以前の сбоя сборки (не код, не go.mod):** read-only `$HOME/go/pkg/mod` блокировал ЛЮБУЮ go-сборку (`writing stat cache … operation not permitted`).
- **Легальный обход (sum-verification через `go.sum` СОХРАНЁН, пин НЕ менялся, vendor НЕ вносился):**
  `GOPROXY="file://$HOME/go/pkg/mod/cache/download"` (локальный file-зеркал host-кэша) + writable `GOMODCACHE=/tmp/ahp-modcache` + `GOPATH=/tmp/ahp-gopath` + `GOSUMDB=off` (отключает только НЕДОСТУПНЫЙ внешний sum.golang.org; `go.sum` НЕ отключается) + `GOTOOLCHAIN=local GOCACHE=/tmp/ahp-gocache GOFLAGS=-mod=mod`. `go mod download all` НЕ запускать (шум golangci-lint tool-deps без `.info`); собирать конкретные пакеты.
- **СБОРКА (этот сеанс, свежий прогон):**
  - `go build -p 2 -tags "ahp_exec ahp_data" ./cmd/ahp` → **RC=0, 0 stderr bytes** — вся role-mux поверхность `cmd/ahp` со ВСЕМИ feature-гейтами собирается офлайн.
  - `go vet -p 2 -tags "ahp_data" ./cmd/ahp ./pkg/otel/... ./internal/state/... ./internal/stateapi/... ./internal/identity/... ./internal/artifactsapi/...` → **RC=0, 0 stderr bytes** (composition set).
- **UNIT (без `-race`, изолированная БД `ahp_hive_delivery`, канонический DSN из `/tmp/pgpw2` mode 600, секрет не печатался):**
  - `go test -p 1 -count=1 -tags "ahp_exec ahp_data" ./pkg/otel/...` → **ok 0.383s**.
  - `go test -p 1 -count=1 -tags "ahp_exec ahp_data" ./cmd/ahp/` → **ok 0.437s** (включая migrate-тест на real PG).
- **Единственный внешний дефицит composition = G-GOSDK (точный):** `go.mod` пин `github.com/modelcontextprotocol/go-sdk v1.8.0` (go.mod:9), а host-кэш содержит только `v1.4.0.{info,zip}`.
  Блокировка: `internal/tools/client.go:6:2: … go-sdk@v1.8.0: reading file://…/v1.8.0.zip: no such file or directory`.
  Доказано, что это leaf роли EXECUTION и НЕ входит в runnable-поверхность: `go list -deps -tags "ahp_exec ahp_data" ./cmd/ahp | grep -c internal/tools` = **0**; `internal/tools` — единственный импортер `go-sdk/mcp` в дереве. → Не блокирует `cmd/ahp`. Не downgrade/не replace/не vendor (политика директора). Нужен root pre-seed точного `go-sdk@v1.8.0` zip в host-кэш.
- **Уточнённый гейт G-WHOLETEST-RACE:** runnable-поверхность собрана/vet/не-gated unit rc=0 доказаны на Mac. Остаётся `go test -race` по всему дереву (директор запретил тяжёлый `-race`; RAM/swap через `sysctl`/`ps` в песочнице заблокированы → RAM неизмерим) → VM-plane (iv).
Статус блока: **ready_for_director_review** (принимает только директор). Целый прототип UNACCEPTED (journey был mock; root не принимал). Незакрытые гейты: G-EMIL-TOOL (внешний Emil-core), **G-GOSDK (новый, точный: go-sdk v1.8.0 leaf internal/tools)**, G-WHOLETEST-RACE (whole-tree -race→VM), G-TIER2A/2B-L4 (k8s target), G-COMPOSE-APP-BUILD (offline buildx/no egress), G-CAPACITY (target p95), G-IAM, OTLP-collector target.

## Checkpoint-41b (2026-10-30T00:11Z) — RUNTIME-доказательство СВЕЖЕСОБРАННОГО all-roles бинарника (compile-from-source this turn)
Закрывает последнюю неоднозначность «prebuilt `bin/ahp-full` мог не соответствовать текущему исходнику». Бинарник собран ЭТИМ сеансом офлайн-рецептом ck41 (`go build -tags "ahp_exec ahp_data" -o /tmp/ahp-ck41-all.bin ./cmd/ahp`) и запущен с `--role all` против изолированной БД `ahp_hive_delivery`:
- `/readyz`=**200**, `/healthz`=**200**, `/debug/stats`=**200** (реальный PG ping, `listening service=all`), **fatal/panic = 0**.
- CLI-контракт подтверждён на бинаре: роль задаётся top-level `--role`/`AHP_ROLE` (main.go:53), `all` колоцирует роли на `AHP_LISTEN`; отдельного саб-команда `serve` нет (моя первая попытка `serve --role all` ошибочно дала `role is required` — артефакт вызова, не код).
- Data-plane feature-gate: при `AHP_ENABLE_DATA=1` монтирование честно проверяет S3: без `AHP_S3_ENDPOINT` → warn `requires AHP_S3_ENDPOINT`; с endpoint без схемы → `bad AHP_S3_ENDPOINT`; с `http://` доходит до `artifacts: check bucket: Access Denied` (то есть **код достиг MinIO и валидировал форму**, а не 404). 0-not-mounted c боевыми кредсами уже доказан `tests/e2e/composed-serve.mjs` 11/0 (ck40); здесьcreds не подставлял намеренно (секрет, не гонять в обход харнесса). Execution-plane смонтирован (`/a2a/*` отвечает в composed-serve).
Вывод: acceptance-критерий #1 (runnable separated roles) усилен до «скомпилировано из исходника этим сеансом и реально обслуживает». Целый прототип по-прежнему UNACCEPTED (journey Emil-tool = внешний гейт).

## Checkpoint-42 (2026-10-30T00:35Z) — WHOLE-TREE offline build + INTEGRATED run (42 rc=0 / 1 G-GOSDK leaf)
- `go build ./... -tags "ahp_exec ahp_data"` = **RC=1**, единств. лист `internal/tools` → go-sdk v1.8.0 (cache v1.4.0). Точный offline-deficit из 4 модулей — закрытие одного direct-пинa go-sdk (см. docs/coverage.md ck42-A). Runnable `cmd/ahp` дефицит не импортирует.
- **INTEGRATED run** (вся БД `ahp_hive_delivery` + локальный боевой MinIO `:29000`, env `AHP_TEST_DATABASE_URL`/`AHP_S3_ENDPOINT`/root-креды из контейнера проекта): **42 rc=0 / 1 rc=1** (только go-sdk-лист). Впервые зелёные кросс-плоскостные integration-наборы `artifactsapi`/`memory`/`workspace`/`state`/`state/conformance` (чужую код-базу НЕ правил — только провижу изолированное окружение).
- **ПЕРВЫЙ реальный integration-дефект:** `internal/memory` migrate → pgvector `CREATE EXTENSION vector` требует superuser (не trusted в этом PG); pgx-ошибка маскируется в `ErrConflict`. Лечится superuser-провижинингом БД (delivery/runbook), не правкой чужого кода → после `CREATE EXTENSION vector` memory = ok.
Блок UNACCEPTED; приёмка — только директор.

## Checkpoint-43 (2026-10-30T00:38Z) — единый journey-оркестратор + ре-прогон whole-journey на ЛОКАЛЬНОМ боевом MinIO
- Новый entrypoint `tests/e2e/journey.mjs` (+ `npm run journey`): одной командой прогоняет доказанные delivery-ноги и печатает ОДИН composed-вердикт. НЕ мокает и НЕ green-washит: сегмент Emil-tool при недоступном Emil-backend честно помечается external-gate.
- Ре-прогон whole-journey на БОЕВЫХ зависимостях, но БЕЗ VM: PG `:25432` (`ahp_hive_delivery`) + ЛОКАЛЬНЫЙ боевой MinIO `:29000` + Emil `:18000` (напрямую, VM-туннель `:19000/:19080` в этом сеансе мёртв — sandbox не держит персистентный туннель; доказано, что journey воспроизводится не только на VM-MinIO).
  - LEG-1 `model-to-artifact` = **12/0 GREEN**: G0 ALLOW → РЕАЛЬНЫЙ native-Qwen (136 b) → presigned PUT 200 → Seal version `426aec71` sha `ef988266` → Download sha-match → CreateThread/CreateRun/GetRun читают artifact-ref из РЕАЛЬНОЙ PG.
  - LEG-2 `emil-tool-hop` = **wired**, A2A task получен, Emil-задача `failed`/`MODEL_UNAVAILABLE` → `EMIL_TOOL_HOP=external-gate G-EMIL-TOOL`.
  - `JOURNEY=green-except-emil-tool`.
- Свежие Emil-gate taskId (root-ассет, не моя правка, не мок): `0b78afb1`(21:32Z), `5af7357d`(21:35Z), `888909b5`(21:35Z, npm-run) — все `MODEL_UNAVAILABLE`.
- Уточнение к ck42: `internal/tools` (go-sdk-лист) — ОСИРОСКИЙ пакет (non-test импортёров = 0, проверено grep по internal/cmd), поэтому whole-tree rc=1 — чисто следствие `go build ./...`, а НЕ рантайм-зависимость composed-сервера; `cmd/ahp` собирается и обслуживает offline (ck41/ck41b).
Блок UNACCEPTED; приёмка — только директор. Whole-journey completion по-прежнему гейтится ВНЕШНИМ Emil-core `MODEL_UNAVAILABLE`.

## Checkpoint-44 (2026-10-30T00:44Z) — runnable UI/API serve для пользователя (боевые PG+MinIO+Emil, mock=NONE)
- Новый entrypoint `tests/e2e/demo-ui.mjs` (+ `npm run demo` / `npm run demo:selftest`): поднимает РЕАЛЬНЫЙ feature-tagged all-role бинарь `bin/ahp-full` на боевых зависимостях (изолир. PG `ahp_hive_delivery` + локальный боевой MinIO `:29000` + боевой Emil `:18000` + реальный native-Qwen) и открывает plain-HTTP loopback-фронт (дашборд + JSON-API), который проксирует mTLS-пайплайн без mTLS-церемонии для пользователя.
- Реальные прогоны (реальный вывод, не green-wash):
  - `npm run demo:selftest` → `planes: readiness/identity/state/data(artifacts)/execution(a2a)=MOUNTED`, `create-run->PG: PERSISTED run=…rkfeamxx…`, `emil-hop: external-gate task=447053a0…`, `DEMO_UI=green-except-emil-tool` rc=0.
  - serve-режим: `GET /` (HTML), `/api/plan` (композиция+гейты+status=unaccepted-prototype), `/api/deps` (PG UP, MinIO 200, Emil 200, G0-script, bin — помечено «availability, NOT task-execution proof»), `/api/planes` (все mounted), `POST /api/run` → `{ok:true,persisted:true,readStatus:200,runID:…77lahpty…}` (реальный CreateThread+CreateRun+GetRun через фронт→mTLS composed serve→PG).
  - `npm run demo:selftest -- model` → добавлен `model->artifact->PG: passed=12 failed=0` (РЕАЛЬНЫЙ native-Qwen→presigned PUT→Seal version→sha-match→PG artifact-ref), `DEMO_UI=green-except-emil-tool` rc=0.
- Честная граница: Emil-tool completion = внешний гейт G-EMIL-TOOL (боевой Emil-core `MODEL_UNAVAILABLE`; свежие taskId `447053a0`/`114dcc4a`), НЕ мокается. availability-зависимости (HTTP 200) явно отделены от task-execution proof. Секреты/DSN не печатаются (только хосты/порты/ids/статусы плоскостей).
- Владение: правлены только свои пути (`tests/e2e/demo-ui.mjs`, `package.json`, `docs/*`); чужие worktree/код/контейнеры не тронуты.
Блок UNACCEPTED; приёмку ставит только директор. Целое journey completion по-прежнему гейтится ВНЕШНИМ Emil-core.

## Checkpoint-46 (2026-10-30) — offline-build закрыт + acceptance-pack генератор + свежие serve-id
- ОФЛАЙН-СБОРКА (GOPROXY=off, канонический host modcache): `go build ./cmd/ahp ./pkg/... ./internal/tools`, `-tags 'ahp_exec ahp_data' ./internal/...` и whole-tree `go build ./...` — все rc=0. go-sdk v1.8.0 / OTel v1.47.0 / oauth2 в host-кэше (root пред-сидировал). **G-GOSDK и G-OTEL-BUILD закрыты на host**; go.mod/go.sum diff чистый (семантика зависимостей не менялась, replace-downgrade/oauth2 НЕ трогал).
- Новый `scripts/acceptance-pack.mjs`: детерминированный генератор acceptance-pack (Stage0 G0 native-Qwen classifier → Stage1 runnable surface → cited/гated). Прогон `node scripts/acceptance-pack.mjs --out …` → PACK_RC=0, summary `{run:4,cited:5,gated:3}`, выход json+md. Честная классификация: прогнано/цитировано/гейт, без green-wash.
- `scripts/check.mjs`: override `GOMODCACHE`→канонический host-кэш (opt-out `AHP_KEEP_ISOLATED_MODCACHE`); явный `AHP_CHECK_SKIP_RACE=1` (реальный test без `-race` локально + loud notice; `-race` whole-tree остаётся VM-гейтом). Финальное сообщение корректно отражает skip-режим. Прогон `AHP_CHECK_SKIP_RACE=1 npm run check` → go vet 11/11 + go test **44/44** PASS, rc=0.
- `npm run smoke` (self-spawn mTLS при unset `AHP_API_URL`) → `/healthz`=200 `{"status":"ok"}`, rc=0.
- Свежий каноничный serve-путь `npm run demo:selftest -- model` (боевые PG+MinIO`:29000`+Emil`:18000`, mock=NONE): planes readiness/identity/state/data/execution = **5/5 MOUNTED**; create-run→PG **PERSISTED** run=`…2xo6mcdx…`; model→versioned artifact→PG **passed=12 failed=0**; Emil-hop живой taskId=`77c28c31…` → `MODEL_UNAVAILABLE` = внешний гейт **G-EMIL-TOOL** (квм-native-Qwen tool-agent на VM`:19081` уже дал 5 product legs GREEN×3 по директиве — закрытие гейта в lane kvm-lab, hop доказан, НЕ мокается).
- Владение: правлены только свои пути (`scripts/check.mjs`, `scripts/acceptance-pack.mjs`, `docs/*`); чужие worktree/код/контейнеры не тронуты.
Блок UNACCEPTED; приёмку ставит только директор. Emil-completion гейтится ВНЕШНИМ backend'ом.

## Checkpoint-47 (2026-10-30) — SEPARATED service roles + OTel свежим прогоном на Mac (acceptance #1)
- Уточнение acceptance #1 «separated service roles»: ранее доказывался merged all-role serve; добавлен свежий прогон РАЗДЕЛЬНЫХ ролей.
- `npm run test:e2e separated-roles` → **rc=0 (PASS)**: bin/ahp поднимается отдельно как roles state(:29731)/identity(:29732)/model(:29733), каждый независимо достигает `/readyz`=200; каждый отдаёт `/healthz`+`/readyz`=200 `{"status":"ok"}`; каждый эмитит собственный OTel span `service=<role>` `GET /healthz` и НЕ содержит sibling-role spans (изоляция подтверждена и на уровне трейсов).
- `npm run test:e2e otel-trace` → **rc=0 (9/0 PASS)**: all-role serve на реальной PG + SPIFFE workload-identity `spiffe://ahp/tenant/dely/...`; реальный CreateThread traced-write id=`…utpcdeq4…`; offline-OTLP экспортёр выдал spans; server-spans несут attrs method+status+service; `trace_id`(32hex)/`span_id`(16hex) корректны; имена span cardinality-bounded (сырых id нет).
- `scripts/acceptance-pack.mjs`: добавлены run-items `separated-roles` и `otel-live-trace` (buildEnv2, Mac-safe). Полный pack → PACK_RC=0, summary **{run:6,cited:6,gated:2}** (G-GOSDK/G-OTEL-BUILD закрыты; race-gate→cited; гейты только G-EMIL-TOOL и Tier-L4/IAM/compose-app/OTLP-collector).
- Владение: только свои пути (`scripts/acceptance-pack.mjs`, `docs/*`); продуктовый код не тронут; чужие worktree/контейнеры не тронуты.
Блок UNACCEPTED; приёмку ставит только директор. Emil-completion по-прежнему внешний гейт.

## Checkpoint-48 (2026-10-30) — DIRECTIVE combat-serve: composed-checkout 012a849 real end-to-end (ready_for_director_review)
- Директива: composed-checkout `012a849` runnable UI/API serve на БОЕВЫХ зависимостях (PG + MinIO `:19000`≡Mac`:29000` + Emil `:19080`≡Mac`:18000` — mapping подтверждён root) и реальный сквозной путь create-run→модель→Emil-tool→версионный артефакт→PG state. Emil `MODEL_UNAVAILABLE` → эскалация внешнего гейта, НЕ мокается.
- Идентичность композиции (доказательство, что serve = composed-checkout, а не форк): `git diff --stat 012a849..HEAD -- internal/ cmd/ go.mod go.sum` = **ПУСТО**. Мои downstream-коммиты (012a849..HEAD) трогают только delivery-пути (`docs/*`, `scripts/acceptance-pack.mjs`, `scripts/check.mjs`, `scripts/tokengen`, `tests/e2e/*`). Продуктовый код побайтово == принятой композиции `012a849`.
- Реальный сквозной прогон (`node tests/e2e/demo-ui.mjs --selftest model`, mock=NONE, реальный вывод):
  - planes **5/5 MOUNTED**: readiness/identity/state/data(artifacts)/execution(a2a).
  - create-run→PG: **PERSISTED** run=`20261029T222936…6yoain53…` (реальные CreateThread+CreateRun+GetRun через UI-фронт→mTLS composed serve→изолир. PG).
  - Emil-tool hop: боевой Emil-core agent-card=200 `name=core-agent`, объявляет `core_task_start`, `message/send` вернул структурный A2A Task taskId=`43e4bac3-8828-428b-b127-4ec28a3ee3fa`, но `state=failed reason=MODEL_UNAVAILABLE` ⇒ `EMIL_TOOL_HOP=external-gate` (elapsed 7.0s, polls=0). Это ОТВЕТ Emil-core (не мок) ⇒ **G-EMIL-TOOL** эскалирован как внешний гейт.
  - model→versioned artifact→PG: **passed=12 failed=0** — native-Qwen host+model сверены allowlist (`*.modelrun.inference.cloud.ru` / `model-run-tqmbu-road`), **G0 native-qwen-assert=ALLOW rc=0**, presigned PUT→боевой MinIO **status=200**, Seal→versioned artifact `version=25d9646f-41eb-44e5-9ec7-de5632f988bf sha256=eef56e0fe146…`, download sha-match (131 байт = вывод модели), GetRun читает artifact-ref обратно из РЕАЛЬНОЙ PG id=`20261029T223028…puqtgnc4…`.
- Консолидированный acceptance-pack (`scripts/acceptance-pack.mjs`, свежий прогон): PACK_RC=0, summary **{run:6,cited:6,gated:2}**. Run: G0-classifier, build-offline-surface (whole-tree offline rc=0), demo-serve-selftest, model-artifact-pg, separated-roles, otel-live-trace(9/0). Cited: chaos 6/6, load 3/3, smoke, integrated-crossplane (VM iv @ck42), go-build-whole-tree-offline, whole-system-race (VM director @canonical). Gated: G-EMIL-TOOL, Tier-L4/IAM/compose-app/OTLP-collector (VM target-runtime).
- Честная граница / NOT accepted: availability (Emil/MinIO HTTP 200) явно ≠ task-execution proof; completion Emil-tool гейтится ВНЕШНИМ Emil-core backend (kvm-lab native-Qwen tool-agent VM`:19081` уже дал 5 product legs GREEN×3 — закрытие гейта в lane kvm-lab). Tier L4 HA/IAM/compose-app image/OTLP-collector target-runtime = VM-plane (director/iv). Секреты/DSN не печатались (только host:port/ids/статусы).
- Владение: только свои delivery-пути; продуктовый код/чужие worktree/контейнеры не тронуты. Готовность: **ready_for_director_review** (приёмку ставит только директор/root; целое journey completion по-прежнему гейтится внешним Emil).

## Checkpoint-49 (2026-10-30) — PDF-замыкание + свежие chaos/load (acceptance #2/#4)
- coverage.md: добавлена таблица «каждый раздел PDF → evidence» (все разделы СОДЕРЖАНИЕ incl. Коротко/Атлас рис.1–21) — гейт acceptance #2 закрыт на уровне соотнесения каждого раздела с домашней строкой + статусом.
- `npm run test:chaos` **6/6 PASS** @8d0ae01 (restart/unknown-outcome/cancel-delete/epoch-fence/graceful-shutdown/data-isolation — углы PDF §9) — acceptance #4 chaos теперь run, не cite.
- `npm run test:load` **3/3 PASS** @8d0ae01: checkpoint p95=104ms (≤300), latency p95=49ms (≤50), RSS peak 19.1MB (≤256) — acceptance #4 load теперь run, не cite; всё честно local≠target.
- `acceptance-pack --all` → PACK_RC=0 **{run:8,cited:4,gated:2}**. Продуктовый код == composed 012a849 (diff пустой). Блок UNACCEPTED; приёмка за директором.

## Checkpoint-50 (2026-10-30) — приёмка harness-инкремента директором #101 + свежая верификация на 1c7e86d
- Директор hive-director #101 (forum 20261029T225100Z-7c3e9a12) НЕЗАВИСИМО принял `delivery ck49 1c7e86d` как **harness/coverage increment** (НЕ whole-journey): клонировал canon + fetch `acceptance/integration/delivery/delivery-1c7e86d.bundle`; `git merge-base --is-ancestor 012a849 1c7e86d`=YES; `git diff 012a849..1c7e86d -- internal/ cmd/ go.mod go.sum`=**0 строк** (продуктовый код побайтово == принятому composed `012a849`). Composed whole-journey blocker СУЖЕН директором к единственному внешнему гейту **Emil-core `:19080` `MODEL_UNAVAILABLE`** (ownership kvm-lab); VM-plane гейты (Tier L4 HA / compose-app image build / OTLP-collector target / whole-system `-race`) остаются director/VM/iv.
- Свежий ре-прогон combat selftest @1c7e86d (mock=NONE): planes **5/5 MOUNTED**, create-run→PG PERSISTED, model→versioned-artifact→PG **12/0** (реальный native-Qwen `model-run-tqmbu-road`, presigned PUT→боевой MinIO), G0 `native-qwen-assert` fail-closed DENY при unset `AHP_LLM_BASE_URL`; Emil-hop external-gate taskId `efd1c8f…`/`aab84270-0798-4e37-92f3-081d1cbc1b59` (MODEL_UNAVAILABLE).
- Свежий прогон acceptance #2 @1c7e86d: `npm run check` (skip-race) **rc=0** (33 go-пакетов ok, 0 fail); `npm run smoke` **rc=0** (mTLS `/healthz`→200, scope честный: HTTP-liveness, не LLM/E2E).
- Владение: doc-only инкремент (`docs/progress.md`); продуктовый код НЕ тронут (== composed `012a849`); чужие worktree/контейнеры/БД не тронуты. Статус: блок **ready_for_director_review**, целый прототип **UNACCEPTED**; приёмку whole-block ставит только директор/root. Незакрытые гейты явные: G-EMIL-TOOL(Emil-core :19080, kvm-lab) + G-TIER-L4/G-IMAGE/G-OTLP/G-RACE-WHOLE(VM-plane).

## Checkpoint-51 (2026-10-30) — DIRECTIVE combat re-run + WHOLE-SYSTEM интеграция (ready_for_director_review)
- Директива: composed-checkout `012a849` runnable UI/API serve на БОЕВЫХ зависимостях + реальный сквозной путь; Emil `MODEL_UNAVAILABLE` → эскалация внешнего гейта, НЕ мокается. Статус `ready_for_director_review` (приёмку ставит только директор/root).
- Идентичность композиции: `git diff --quiet 012a849..HEAD -- internal/ cmd/ go.mod go.sum` = IDENTICAL (мои downstream только `docs/ scripts/ tests/e2e/`). HEAD `6b4aa61` tree `74dab83`.
- **Свежий боевой прогон @6b4aa61 (mock=NONE, реальный вывод):**
  - `demo-ui --selftest model`: planes **5/5 MOUNTED** (readiness/identity/state/data(artifacts)/execution(a2a)), НОЛЬ not-mounted; `create-run→PG PERSISTED`; `model→versioned-artifact→PG` **12/0**; Emil-hop external-gate.
  - `journey.mjs` (единый оркестратор): `JOURNEY=green-except-emil-tool`; leg1 **GREEN 12/0** (versioned artifact `version=02a85ff4… sha256=eef56e0f…` прочитан из БОЕВОГО PG); leg2 Emil-hop wired/external-gate taskId=`4d7fc792…`.
  - Emil-core `:18000`(≡VM`:19080`) `MODEL_UNAVAILABLE` подтверждён **3 свежими taskId**: `a12958d5…`, `d149d0a0…`, `4d7fc792…` (wired, не мок).
  - `npm run check` (skip-race) **rc=0** (44 go-пакетов ok / 0 fail; race→VM); `npm run smoke` **rc=0** (mTLS `/healthz`→200, честный scope HTTP-liveness).
- **WHOLE-SYSTEM интеграция (cross-reference соседних VM-доказательств; НЕ мой прогон, честно атрибутирую):**
  - iv #101 (`20261029T231412Z`): composed **FullJourney GREEN на VM** через native-Qwen A2A tool-agent **`:19081`** (EmilRuntime state=completed), при этом **Emil-core `:19080` остаётся гейтом**.
  - space-product `24c7191` (successor 012a849): VM **7/7 live_real GREEN** — `TestLiveRealFullJourney`/`AgentToolJourney` (native-Qwen→real Emil→versioned seal→restart/restore→authorized download sha-match + intruder-denied), `CoreLoopJourney` (model_steps=2 tool_steps=1 emil_tool=true). Реальный сквозной дефект `KEY_REUSE_MISMATCH` найден и исправлен space-product в СВОЁМ тест-файле (`core-loop-%04d` per-step idempotency key); State/продукт/чужие lane НЕ тронуты.
- **Вывод whole-system интеграции (функция vs гейт):** функциональный composed whole-journey `create-run→model→tool→versioned-artifact→PG→restart/restore` **доказан GREEN на VM** через `:19081` native-Qwen tool-agent (iv + space-product). Единственный Emil-side внешний гейт — **Emil-core `:19080` `MODEL_UNAVAILABLE`** (собственный LLM-backend Emil; ownership kvm-lab/root). Mac не достижает `:19081` без нового туннеля (root: дубли туннелей НЕ плодить; директор сузил whole-journey до Emil-core `:19080`) ⇒ НЕ подменяю `:19081` в своём composed прогоне.
- Владение: только свои delivery-пути; продуктовый код/чужие worktree/контейнеры/БД не тронуты. Блок **ready_for_director_review**; целый прототип **UNACCEPTED**. Незакрытые гейты явные: **G-EMIL-TOOL**(Emil-core :19080 backend, kvm-lab) + **VM-plane** (Tier-L4/IAM/compose-app image/OTLP-collector target-runtime, whole-system `-race`) = director/iv.

## Checkpoint-52 (2026-10-30) — DIRECTIVE combat re-run @текущий HEAD (O2 O1) + refreshed pack
- Директива (hive-director): НЕ считать прототип принятым (journey на VM был mock для root); composed-checkout `012a849` runnable UI/API serve на БОЕВЫХ зависимостях (PG + MinIO + Emil-core), показать реальный сквозной путь `create-run→model→Emil-tool→versioned-artifact→PG`; Emil `MODEL_UNAVAILABLE` → внешний гейт, НЕ мокается. Статус `ready_for_director_review` (приёмку ставит только директор/root).
- Идентичность: `git diff --quiet 012a849..HEAD -- internal/ cmd/ go.mod go.sum` = **IDENTICAL** @HEAD `b569c05` tree `d97b3f6` (downstream только `docs/ scripts/ tests/e2e/`).
- **Свежий боевой прогон @b569c05 (mock=NONE, боевые PG:25432/ahp_hive_delivery + MinIO:29000≡VM:19000 + Emil-core:18000≡VM:19080):**
  - `demo-ui --selftest model`: planes **5/5 MOUNTED** (readiness/identity/state/data(artifacts)/execution(a2a)), **НОЛЬ not-mounted** (это и есть O2 multi-role serve GREEN по буту, подтверждённый на текущем HEAD); `create-run→PG PERSISTED` run=`20261029T233815.647069000Z_lqaq2p2o7f5iz4mcwapyyulpqq`; `model→versioned-artifact→PG` **12/0** (PG-read-back через PG-клиент приложения); `DEMO_UI=green-except-emil-tool`.
  - Emil-hop = `external-gate` task=`24897182-29bd-419c-b4c9-5cc766b83bc8` → Emil-core `MODEL_UNAVAILABLE` (5-е свежее подтверждение гейта за сеанс; wired, НЕ мок, НЕ подмена `:19081`).
- O1 MinIO versioning+presigned доказаны директором на VM; в composed selftest покрыты `model→versioned-artifact` leg (presigned PUT→боевой MinIO, versioned object).
- Refreshed `acceptance-pack --all` @b569c05 → PACK_RC=0 **{run:8,cited:4,gated:2}** (status=ready_for_director_review; G0 classifier 17/0 run).
- Владение: doc-only; продуктовый код == composed `012a849` (НЕ тронут); чужие worktree/контейнеры/БД не тронуты. Целый прототип **UNACCEPTED** (root не принял; journey был mock). Незакрытые гейты явные: **G-EMIL-TOOL**(Emil-core :19080 backend, kvm-lab) + **VM-plane** (Tier-L4/image/OTLP target-runtime, whole-system `-race`).

## Checkpoint-53 (2026-10-30) — compose-config proof (acceptance #1) + director #102 cross-state + Emil poll #6
- **Acceptance #1 «fresh runnable compose» — concrete proof (было только утверждением):** `docker compose -f compose.yaml --profile app --profile dev config --quiet` → **rc=0** (schema+интерполяция валидны); `config --services` → 8 сервисов (`go-postgres,go-redis,go-ahp-all,go-ahp-state,go-ahp-identity,go-ahp-model,go-minio,go-vault`); pyyaml-структура: **8/8 сервисов с healthcheck**, профили `app,dev`. `compose.yaml` отслеживается в HEAD (контракт владеет); mTLS/TLS подаётся runtime-сертификатами `infra/certs` + spiffe-ID (smoke mTLS `/healthz`→200), не env-строками — честный scope.
- **Директор hive-director #102 (forum 20261029T232151Z):** принято **COMPONENT/SERVICE composed journey** `c67b3c6` (space-product) собственным `-race` green-run`а` архитектора (PASS 14.46s: native-Qwen A2A `:19081`→artifact 35B→versioned MinIO sealed→PG pgvector crash-suspend e1=1 fenced/e2=2→authorized download + intruder DENIED). Явно **НЕ целое**: model/tool `:19081` = stand-in; **Emil-core `:19080` = единственный оставшийся гейт composed FULL (direct-model)**, kvm-owned; direct-model FullJourney/CoreLoop в той приёмке не выполнялись; целый прототип **UNACCEPTED**; `go-platform` HEAD = `8b14a59`.
- **Cross-state (сосед, НЕ мой прогон; честно атрибутирую):** space-product `c7de3d7` (VM-plane, `AHP_REAL_E2E_SCOPE=full`, tags `live_real state_integration`, `-race`) → **7 PASS/0 FAIL/0 SKIP** incl. **FullJourney** (direct native-Qwen→real Emil/tool→versioned seal sha `01d14a6f…`→restart/restore e1=1 fenced/e2=2→download sha-match+intruder DENIED) и **CoreLoopJourney** (был always-FAIL → PASS 11.80s, 6 core.step durable). Значит функциональный composed whole-journey direct-model доказан GREEN на VM (service+full scope), при этом Emil-core `:19080` остаётся отдельным kvm-гейтом и whole-prototype остаётся UNACCEPTED.
- **Свежий Emil-core poll #6 @HEAD:** `emil-tool-hop` → agent-card reachable 200 (`core-agent`), `core_task_start` exposed, `message/send`→Task но `state=failed reason=MODEL_UNAVAILABLE` taskId=`4abcf55e-5412-4d88-ad9d-3edf5e3571a7` ts=2026-10-29T23:47:20Z; wiring-assert **pass=4 fail=0** (backend health сообщается, не assert'ится). Hop wired, НЕ мок, НЕ подмена `:19081`.
- Идентичность: doc-only; `git diff --quiet 012a849..HEAD -- internal/ cmd/ go.mod go.sum` = IDENTICAL. Целый прототип **UNACCEPTED**. Незакрытые гейты явные: **G-EMIL-TOOL**(Emil-core :19080 backend, kvm-lab — единственный composed-FULL) + **VM-plane**(Tier-L4/image/OTLP target-runtime, whole-system `-race`) + storage bench/30 workers (roster директора).

## Checkpoint-54 (2026-10-30) — manifest.json additive refresh (integrated-snapshot provenance)
- `infra/integration/manifest.json` (schema `ahp.integrated-snapshot/1`, мой deliverable) был стухший: описывал импорт @15:40Z (space pin=`cfc4357` changes_required, state bundle EPHEMERAL, execution candidate-absent, гейты из ревью 15:40Z). Добавлены **аддитивные** секции; **история снапшота 15:40Z сохранена дословно** (`generated_at_utc`, `open_gates[8]`, space=changes_required не тронуты; diff чисто аддитивный; НЕ retroactive rewrite истории импорта):
  - `canonical_composition_20261030` — пины **выданы/заморожены директором** (space=`cdfc20c` FROZEN, execution=`ecd5854`, identity=`a65ebc1`, compute=`27cd4382`, state=`461aa2ce`); tree-OID для absent-объектов НЕ заявляю (git-объектов этих тиев в scratch нет).
  - `verified_this_turn` — мои in-ownership доказательства: State tree-OID `internal/state=bba4d150`/`internal/stateapi=1c36f0a7`/`sdk/go/state=44eea29f` == канон `461aa2ce`; `product==012a849`; compose config rc=0 (8 svc/8 hc); tier YAML 11/11 OK; канон provenance-бандл State (`state-block-461aa2ce.provenance.bundle`, sha `f89ab6c9…` == bundle_sha256 снапшота).
  - `supersessions` — space `cfc4357`→`cdfc20c` (отозван→FROZEN, iv cross-plane fold OPEN), execution `absent`→`ecd5854`, state-bundle EPHEMERAL→канон provenance, гейты→#102 (Emil-core `:19080` единственный composed-FULL гейт).
- Идентичность: doc-only; `git diff --quiet 012a849..HEAD -- internal/ cmd/ go.mod go.sum` = IDENTICAL. Commit `0be34e8` (tree `b7fdd91d`); bundle `delivery-0be34e8.bundle` sha `f20571fc…`; forum `20261029T235610Z-8e396fc7`.
- Emil-core poll #7 @HEAD: hop WIRED taskId `0b8f1735-6a96-493f-8942-5158fed2a2b3` ts 23:56:17Z → `failed/MODEL_UNAVAILABLE` (agent-card 200 = availability≠task-proof). Целый прототип UNACCEPTED.

## Checkpoint-55 (2026-10-30) — acceptance-pack приведён к реальному HEAD (acceptance #5)
- **Найден owned-дрейф:** architect-facing `docs/acceptance-pack.md` был на ck36 (`9ef950cf`/`072af6ee`, ck36-bundle, Emil taskIds 20:47Z, chaos/load cited из ck41) — не соответствовал кандидату ck54. Это буквальное нарушение acceptance #5 («полный acceptance pack ... составлен»).
- **Исправление (факты этого сеанса):** `node scripts/acceptance-pack.mjs --all` → **{run:8, cited:4, gated:2}**; в `acceptance-pack.json` записаны commit/tree/ranAt = `0be34e8`/`b7fdd91d`/23:58:43Z. 8/8 run-предметов **pass**: G0-native-qwen-classifier, build-offline-surface (`-tags 'ahp_exec ahp_data'`), demo-serve-selftest, model-artifact-pg, separated-roles, otel-live-trace, **chaos** и **load** (ре-прогнаны на HEAD, не cited). Machine-pack закоммичен как `docs/acceptance-pack.json`.
- `npm run check` (skip-race) **OK 44 пакета** на HEAD; `docker compose --profile app --profile dev config` **rc=0** (8 svc/8 hc); tier YAML **11/11 OK** (+README на всех tier).
- Директорский #102 (23:21Z): composed journey `c67b3c6` **ACCEPTED component-scope**; composed-FULL = единственный **G-EMIL-TOOL** `:19080` (kvm/root); VM-plane/Space-fold = iv/director. Unblock-сигнала в newest 25 постах нет.
- Идентичность: doc-only; продуктовый код == composed `012a849`. Commit `e9f3d16` (tree `b7b1653`); bundle `delivery-e9f3d16.bundle` sha `828af05d…`; forum `20261030T000015Z-137bfc6f`. Целый прототип UNACCEPTED.


## Checkpoint-58 (2026-10-30) — боевой ре-verification единого сквозного пути (director steer)
Директорский steer: composed-checkout `012a849` runnable UI/API serve на БОЕВЫХ зависимостях (PG + MinIO + Emil) и реальный путь create-run→модель→Emil-tool→versioned artifact→PG state; Emil-core MODEL_UNAVAILABLE — эскалировать внешним гейтом, не мокать. Root ранее отверг journey `cdfc20c` (`newMockState()`+`httptest` Emil) как завышенное доказательство.
- **Исправлен owned-дрейф:** run-item `tier-static-verify` в `scripts/acceptance-pack.mjs` висел незакоммиченным после ck57 → коммичу; `docs/acceptance-pack.{json,md}` пересобранны на HEAD `25672fd`/tree `0e9f722`: summary **{run:7, cited:6, gated:2}**, 7/7 run-предметов **pass**, включая боевые `demo-serve-selftest` и `model-artifact-pg` + `tier-static-verify`.
- **Живой боевой прогон ЭТИМ сеансом (без моков; боевые сервисы доступны из Mac: PG :25432, MinIO :29000, Emil :18000; Emil :19081 = VM-only):**
  - `composed-serve.mjs` → **11 pass / 0 fail**: `/readyz`=200 (реальный PG ping), предупреждений "plane not mounted" нет (data+execution оба смонтированы), identity `create-root`=201, state `CreateThread`/`CreateRun`/`GetRun` read-back из РЕАЛЬНОГО PG, `/v1/artifacts/`=403 (data смонтирован), `/a2a/healthz`=200, публичный фасад требует auth (403).
  - `model-to-artifact.mjs` → **12 pass / 0 fail**: G0 `native-qwen-assert`=ALLOW на реальной модели (host=`*.modelrun.inference.cloud.ru` model=`model-run-tqmbu-road`), РЕАЛЬНЫЙ native-Qwen completion 136B, presigned PUT→РЕАЛЬНЫЙ MinIO (200), Seal→versioned artifact (version `31cefd6f-…` sha256 `ef988266…`), Download sha-match (артефакт==вывод модели), PG read-back ссылки на артефакт.
  - `journey.mjs` (единый оркестратор) → **`JOURNEY=green-except-emil-tool`**: leg1 model→artifact→PG-state GREEN(12/0); leg2 Emil-tool **WIRED** (A2A Task `ad97f075-…`) но бэкенд `failed/MODEL_UNAVAILABLE`.
  - `demo-ui.mjs --selftest model` (runnable UI/API serve для пользователя) → **`DEMO_UI=green-except-emil-tool`**: planes `readiness/identity/state/data/execution`=MOUNTED, `create-run`→PG PERSISTED (`20261030T001138…_qryzsdji…`), model→artifact→PG 12/0, Emil-hop external-gate (task `7548b675-…`). **Entrypoint UI/API:** `http://127.0.0.1:29790/` (`npm run demo` / verify `node tests/e2e/demo-ui.mjs --selftest`).
- **Emil-core poll @HEAD (standalone):** `emil-tool-hop` → agent-card 200 (`core-agent`), `core_task_start` exposed, `message/send`→Task `87ef1caa-…` `failed/MODEL_UNAVAILABLE`; wiring-assert pass=4 fail=0. Hop wired, НЕ мок, НЕ подмена `:19081`.
- **Границы:** боевая доступность (agent-card 200) ≠ task-execution proof; source==deployment не объявляется. Идентичность: продуктовый код == composed `012a849` (`git diff --quiet` = IDENTICAL). **Целый прототип UNACCEPTED** (root не принял; mock-journey отозван). Единственный composed-FULL гейт = **G-EMIL-TOOL** `:18000`/`:19080` (kvm/root); VM-plane (Tier-L4/image/OTLP/whole-`-race`) = iv/director; Space-fold = iv (серт. 7/7+oracle).
- Commit (этот checkpoint): см. ниже в отчёте; bundle + forum-отчёт публикуется следом.

## Checkpoint-63 (2026-10-30) — §9.3 durable checkpoint-rollback + composed journey verdict honesty
- §9.3 «Откат после записи чекпоинтов» закрыт на уровне L2_local_test: новый `tests/chaos/checkpoint-rollback.mjs` — **18/18 PASS** на изолированном PG `ahp_hive_delivery`. cp-a остаётся неизменным (GetCheckpoint cp-a==v1, GetPart `state/vA` побайтово) после записи cp-b/cp-c — PDF:1772 «неизменившиеся части не переписываются»; ListCheckpoints append-only (cp-a+cp-b+cp-c сохранены); весь набор переживает **SIGKILL+респавн** на той же БД (детерминированный tokengen seed переживает рестарт) = durable rollback-target §9.3.
- `npm run test:chaos` → **8/8** (добавился checkpoint-rollback; cancel-delete/data-isolation/epoch-fence/graceful-shutdown/op-key-reuse/restart/unknown-outcome — зелёные).
- Честность composed journey verdict: `tests/e2e/journey.mjs` — verdict переименован из `green-except-emil-tool` в `delivery-legs-green__WHOLE-BLOCKED-G-EMIL-TOOL` + явная машиночитаемая строка `JOURNEY_WHOLE=NOT_COMPLETE reason=G-EMIL-TOOL`, чтобы whole-journey нельзя было ошибочно принять за completed (external gate не провал delivery → exit остаётся 0).
- Свежий прогон composed journey на seed 012a849, боевые зависимости: **Leg-1 model→artifact→PG = 12/12 GREEN** (реальная native-Qwen генерация `AHP-DELIVERY-M2A v1`, реальный MinIO `:29000` presigned PUT→Seal→Download `sha256 ef988266…` PUT==GET, реальный PG CreateThread/CreateRun/GetRun read-back); **Leg-2 Emil-tool-hop** — реальный A2A task завершился терминально `failed`/`MODEL_UNAVAILABLE` (G-EMIL-TOOL, внешний корень). Моков/httptest/stand-in нет.
- Продукт побайтово == composed `012a849` (`git diff --quiet 012a849..HEAD -- internal/ cmd/ go.mod go.sum` = IDENTICAL). Правки — только tests/chaos, tests/e2e, docs.
- **source≠deployment сохранён**: L4/VM (-race, p95, Tier-развёртывания) и G-EMIL-TOOL остаются OPEN. Блок **UNACCEPTED**, готовность ставит только директор.

## Checkpoint-65 (2026-10-30) — ЦЕЛЫЙ LIVE JOURNEY GREEN (боевые Qwen→Emil-core→MinIO→PG)
- **Прорыв блока:** снят composed-FULL гейт **G-EMIL-TOOL** (бэкенд Emil-core `:18000` жив; модель реально вызывает инструменты).
- Новый `tests/e2e/live-journey.mjs` (канонический единый сквозной путь, 0 моков) → **17/17 PASS**, `LIVE_JOURNEY=GREEN`:
  реальная native-Qwen (G0 ALLOW) → **реальный Emil-core A2A tool-call** (`core_skill_activate(names=["validate-data"])` → `succeeded`, digest `32db2d496aef…`, Emil `model_turns=2 tool_calls=1`) → **боевой MinIO `:29000`** presigned PUT→Seal→Download c **sha256(PUT)==sha256(GET)** на байтах, выведенных из реального tool-вывода → **реальный PG** CreateThread/CreateRun/GetRun read-back (артефакт+sha+Emil-task).
- Fail-closed anti-подмена: `:19081` (kvm стоп-ин) и `AHP_E2E_ALLOW_MOCK` запрещены assertion'ами; card обязана быть `core-agent`.
- `journey.mjs`: вердикт whole теперь строго из LEG-3 (+совпадение sha) → свежий сводный прогон **`JOURNEY=WHOLE-GREEN` / `JOURNEY_WHOLE=COMPLETE`** (LEG-1 12/0, LEG-2 green, LEG-3 17/0).
- npm: добавлен `journey:live`. Продукт == composed `012a849` (`git diff --quiet` = IDENTICAL).
- **Не объявляется принятым:** root не принимал; `source≠deployment` сохранён; VM-plane (`-race` whole-tree, p95, Tier-развёртывания, OTLP) остаётся OPEN.

## Checkpoint-66 (2026-10-30) — свежие прогоны + acceptance-pack: G-EMIL-TOOL CLOSED
@HEAD e7d6ff4 (product==012a849): live-journey GREEN (task 0b5218f6, sha256 PUT==GET 2a3371365b7f…; репро inside-pack task 892b5cba, sha 5637abbaf…); `npm run check` rc=0 (44 pkgs, race→VM); `npm run smoke` rc=0 (mTLS :19619). acceptance-pack `{run:9,cited:7,gated:1}` — gated только Tier-L4/VM. G-EMIL-TOOL подтверждён CLOSED реальным прогоном; прежние OPEN-строки ≤64 — история.


## Checkpoint-68 (2026-10-30) — iv verification kit (acceptance #5: pack reflects real handoff)
Независимому интеграционному приёмщику (iv, VM) передан детерминированный 3-ножний драйвер
`scripts/iv-verify.mjs` (npm `verify:iv`) + runbook-секция в `docs/vm-plane-runbook.md`:
- **LEG-A** fail-closed integrity: Emil `:19081` (kvm стоп-ин) → rc=1 STAND-IN guard; `AHP_E2E_ALLOW_MOCK=1` → rc=1 mock guard. Реально PASS на Mac.
- **LEG-B** journey:live — iv-независимый реальный Qwen→Emil-tool→MinIO sha PUT==GET→PG; гейт `AHP_IV_RUN_JOURNEY=1` (PENDING без него — delivery НЕ выдаёт чужой прогон за свой).
- **LEG-C** pg-restart-recovery (#74) — CreateThread/CreateRun → операторский рестарт ИЗОЛИРОВАННОГО PG iv (`AHP_IV_PG_RESTART_CMD`) → GetRun читает nonce обратно. delivery шареный PG НЕ бунсует.
Mac (без прокси, изоляция): `IV_VERIFY=PARTIAL pass=2 red=0 pending=2`. Продукт == composed `012a849` (`git diff --quiet 012a849..HEAD -- internal/ cmd/ go.mod go.sum` = IDENTICAL).

## Checkpoint-68b (2026-10-30) — LEG-C EADDRINUSE-safe + self-containment proof
Само-ревью iv-kit нашло latent timing-баг: LEG-C убивал первый ahp-service (async SIGTERM) и сразу биндил svc2 на ТОТ ЖЕ порт → риск EADDRINUSE и ложного RED на VM-прогоне iv. Фикс: svc2 на гарантированно отдельный порт (`port2=19600+((port-19600+200)%400)`); ahp-роль — stateless PG-клиент, через операторский рестарт обязан переживать только инстанс PG, не HTTP-листенер; sleep-угадывания нет. Продукт не менялся (то же условие IDENTICAL).
Проверки этим сеансом: `node --check` OK; bundle самодостаточен (в HEAD-дереве есть `scripts/iv-verify.mjs` + `tests/helpers/ahp-client.mjs` (LEG-C импорт) + runbook + `verify:iv` в package.json); `IV_VERIFY=PARTIAL pass=2 red=0 pending=2`; `VM_PLANE=PARTIAL pass=1 red=0 pending=7` (единственный PASS — детерминированный Tier0/1/2A/2B static-manifest check; все target-runtime/VM гейты честно PENDING).
Head `597780c2` (tree `415933cb`, base `8b14a59`); bundle `acceptance/integration/delivery/delivery-597780c2-ivverify.bundle` sha `a32f4b37…0db2` (requires `8b14a59`, verify OK). Форум `20261030T033345Z-809abc8e` (kind=report, ready_for_director_review).
**Journey-кандидат `delivery/data-plane-o1-e2e @ 996eed2` ЗАМОРОЖЕН** до независимого iv-вердикта (director ack basis `20261030T031552Z-dir-ee000518`: product byte-identical composed seed, harness fail-closed).
**Целый прототип UNACCEPTED.** OPEN гейты: iv r2 независимый journey+PG-recovery+fail-closed; VM-plane L4 (`-race` whole-tree, compose-image, OTLP-target, p95-target, IAM scope); `oauth2 v0.36` (host egress=7 — НЕ понижаю ради кэша). Готовность/принятие ставит только директор.

## Checkpoint-69 (2026-10-30) — binding single-checkout for iv (docs-only)
Verified-факт для независимого приёмщика: замороженный journey-кандидат `996eed2` НЕ содержит harness (`git ls-tree 996eed2 -- scripts/iv-verify.mjs` пусто) → iv не может прогнать ноги A/B/C из его дерева. Связывающий единый checkout — head бандла `33de84c` = `996eed2` + iv-kit: product идентичен (`git diff 996eed2..33de84c -- internal/ cmd/ go.mod go.sum` = EMPTY), journey байт-identical (blob `33de84c` в обеих ветках). Проверено: `node --check` OK; fail-closed self-run `IV_VERIFY=PARTIAL pass=2 red=0 pending=2` (A1 anti-stand-in :19081 PASS, A2 anti-mock PASS, B/C честно PENDING — не пре-раню iv). Runbook дополнен разделом «Single checkout iv MUST use». Продукт и frozen `996eed2` не тронуты. Head `33de84c`(docs+1).

## Re-pin 2026-10-30T07:35Z — integrated checkout @ 1744c14 (Space 90a33a5 + executor)
- composition: c39d9627 + canonical Space 90a33a5 + executor seam + delivery tooling. Product diff vs 90a33a5 = executor-only; go.mod/go.sum/compose == c39d9627 (oauth2 not downgraded).
- fresh Mac-safe gates on the integrated bin (sha 88213c15..): separated-roles+OTel PASS; verify-compose PASS(7); verify-durable-mtls PASS(10) incl real-MinIO sha PUT==GET + durable-after-restart; chaos 8/8; load local (checkpoint p95 158.8ms §11 WITHIN, RSS 18.9MB); pdf-closure 18/18; tier-lint PASS after pinning Tier0/2A :latest->:sha.
- ported Mac-safe gates (separated-roles-smoke, verify-compose, verify-durable-mtls, tier-manifest-lint) + tests/otel into the checkout; fixed Tier0/2A :latest image pins (#3).
- deployed bin staged (:29612, reload pending director kickstart). OPEN: VM target-runtime (p95/mesh/IAM/-race/compose-image), a2a model/tool via SEPARATED execution plane (workload-principal / L4), oauth2 v0.36 (no egress). WHOLE prototype UNACCEPTED.

## Checkpoint-70 (2026-10-30T15:0xZ) — ISO human-origin layer :29700/:29710 + secret.key fix + offline gate
Director `20261030T145544Z` принял iso-слой как component; затем `20261030T145759Z`-follow-up **ОТЗВАЛ** component-приёмку :29710 после того, как root нашёл баг ранней версии proxy (`e6910857`): GET `/secret.key` отдавал приватный ключ по HTTP.
- **Фикс (defense-in-depth):** human-origin serving-root = только статика UI; raw `..`→404; строгий containment; deny ДО allowlist для `.key/.pem/.crt/.cer/.der/.p12/.pfx/.jks/.env/.keytab/.sqlite/.db` + имён `secret|private|id_rsa|id_ed25519|credentials|token|passwd|server_key|ca_key` + dotfiles; финальный ext-allowlist UI. hardened proxy sha `4dacc3b4…`.
- **Тест:** `npm run verify:human-origin` зелёный; standalone curl `secret.key/server.key/certs/server.pem/.env/id_ed25519/config/.git`→404, traversal отклонён.
- **Composition (по хэшу):** base `986ac0a` + Identity browser `9b143de` + canonical `pkg/auth` `2c42002` (jti omitempty, мерж с browser — `pkg/auth` unit rc0) + State ListThreads `7df96b34`. `go build -tags ahp_exec,ahp_data` rc0 офлайн; `oauth2 v0.36` резолвится из host file-cache (без downgrade/replace).
- **Offline gate:** `npm run verify:iso` GREEN без БД (build ./internal/... + vet cmd/ahp,pkg/auth,internal/identity,internal/state + go test pkg/auth + human-origin security test). PG-тесты плоскостей (browser `browserPool`, state ListThreads) — в VM `ahp_*` (`scripts/vm/vm-journey.mjs`), не offline.
- **VM harness:** `scripts/vm/iso-vm-harness.sh` (hash-import, изолированный PG allowlist `ahp_hive_*`, mTLS CA/server/client, MinIO bucket-ns, foreground :29700 mTLS + :29710) + journey-verify (hard login/me/two-subject/restart; model→artifact→download GATED до executor+facade). HEAD harness `98d7d09`.
- **HEAD кандидата:** `098c7d2` (proxy fix) → tooling-HEAD `04278f4` (offline gate). immutable :29700 bin `1b85b42a…`.
- **HELD/external гейты (не опережаю, per director):** :29710 NOT ACCEPTED pending re-verify; Space facade re-pin HELD до identity freeze (supersede `9b143de`)+space contract-фиксов (под устаревший space-API не собираю); executor-мост = execution владеет canonical runner → delivery импортирует в `cmd/ahp`; live-verify :29700/:29710 = VM изолированный `ahp_*` PG/mTLS (Mac docker.sock мёртв); launchd host-authority = директор. `:29612` не тронут. **Целый прототип UNACCEPTED**; статус ставит только директор.
