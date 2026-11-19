# Приёмочный артефакт-кандидат — execution-lane A2A Agent Card

Работа над целью закрытия дыры публичного A2A-контракта. Приёмку НЕ объявляю — это
артефакт-кандидат для чистого вердикта (прогон в отдельном clean worktree на HEAD, dirty=0).

## Идентичность кандидата (head + sha)
- worktree: `/home/alex/ahp-hive/worktrees/exec-a2a-card-20261031`
- branch: `exec/a2a-card-20261031`
- HEAD: `7867b8d578229bf6c4834f40ec3c5d6eddfab42b`
- base пин: `8dd631a`
- dirty: `0` строк; sha256 dirty-diff: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` (пустое)
- цепочка поверх `8dd631a`: `ac6ce12` -> `1081390` -> `ab025e2` -> `f4c3171` -> `7377ed1` -> `7867b8d`

## Независимая сверка sha256
- бинарь `cmd/ahp`, собранный с HEAD (go1.26.2 linux/amd64):
  `4fe213995bdfea82e3880fa2de001d122c1998b78da8ec6b488687d7ce0f0f98`
- карточка стенда `state/integration-stand/agent-card.json` (version `8dd631a`):
  `3cc727ded76617d1f515cd4cc42cdfae3a22d8ddffdee6620391c0f2b23ee9e1`
  (боевой Go-валидатор `LoadAgentCardFile`/`ValidatePublicAgentCard` — VALIDATOR_ACCEPT)
- лог `-race` прогона (sha256 счётчиков ниже):
  `91f2021941a680cf27269f7c4e8b783afdbbcbeef5dbb45a296c7dc6bc739f4c`

## Green-before-done (числовые счётчики)
Инструмент сборки/тестов — только `tools/ahp-lock-run.py --lock run/build.lock`.
Окружение тестов: `AHP_TEST_DATABASE_URL` (execution.env) + `AHP_S3_ENDPOINT`/MinIO creds
(env/journey-minio.env) + `AHP_ENABLE_DATA=1 AHP_ENABLE_EXECUTION=1 AHP_S3_BUCKET=as-artifacts
AS_ARTIFACT_BUCKET=as-artifacts`; `GOPROXY=off GOFLAGS=-mod=mod`.

- `go build ./...` -> OK (BUILD_ALL_OK)
- `go test -race -count=1 ./adapters/emil/ ./internal/gateway/ ./tests/execution/` -> rc=0
  - PASS=51   FAIL=0   SKIP=4   SKIP_EXPLICIT=0   DATA RACE=0
  - `ok  github.com/ahp-platform/ahp/adapters/emil      1.086s`
  - `ok  github.com/ahp-platform/ahp/internal/gateway   124.287s`
  - `ok  github.com/ahp-platform/ahp/tests/execution    111.120s`

Инвентарь 4 SKIP (штатные гейты внешних proofs, НЕ тихие skip реальной зависимости этого блока):
- `TestRedisNotifierRoundTrip` — нет Redis (внешний Redis-нотификатор)
- `TestRedisNotifierTimeout` — нет Redis
- `TestLiveExecutorQwenToolRoundTrip` — `AHP_LIVE_QWEN!=1` (нативная LLM, нога стенда)
- `TestLiveNativeQwenThroughOptionAFace` — `AHP_LIVE_QWEN!=1` (нативная LLM, нога стенда)
Ни один не через `AHP_ALLOW_SKIP_REAL_DB` (SKIP_EXPLICIT=0).

## Требование -> доказательство
- **(1) Agent Card из env** (`AHP_EXEC_AGENT_CARD_FILE` -> `gateway.ServerConfig.AgentCard`, путь
  `/.well-known/agent-card.json`; §2 + обязательное §6.1-расширение; анти-утечка) —
  PASS: `TestAgentCardServedOnPublicFace`, `TestAgentCardNotServedWhenUnconfigured`,
  `TestExtendedCardMatchesPublic`, `TestLoadAgentCardFile` (enum/leak/DSN-негативы).
- **(2) Сквозной emil.delegate** (реальный AcceptTurn/DispatchTurn -> модель -> реальный emil-адаптер
  по mTLS+task-bearer -> Result+Provenance+digest в durable-stream -> save_artifact -> durable done;
  drain гейтвея SUBMITTED->WORKING->terminal) — PASS: `TestEmilDelegateRoundTrip`,
  `TestA2AHarnessAcceptDispatchDurableDone`, `TestDurableWorkerSettlesTaskAndJournalsVersions`
  (assert WORKING), `TestStartWorkerDrainsSubmittedTask`, `TestCompositionGatewayPublicFaceExecutor`.
- **(3) Негативы** — PASS: `TestPublicFaceFailClosedWithoutCredential` (боевой transport.Task: без
  креденшелов 403; card=200; healthz=200), card не раскрывает `/a2a/internal` (leak/DSN-негативы),
  `TestPublicFacePlaintextFailClosed`, `TestPublicFaceForgedOrIncompleteTaskTokenReject`,
  `TestTaskIsInvisibleOutsideItsScope`.
- **Реальный дефект** (SIGSEGV emil при nil-client) — фикс + PASS: `TestAdapterWithoutClientReturnsError`,
  `TestNewRequiresBaseURL`.

## Границы
Тронуты только разрешённые пакеты (`internal/gateway`, `cmd/ahp/role_execution.go`, `adapters/emil`,
`tests/execution`). `internal/space`, `internal/state`, PKI, живые PID и `state/integration-stand` не тронуты.

---

# Блок 3 (dir-steer-0350Z + указание прораба 04:53Z): потеря текста на границе transport -> ExecInput.Input -> bridge

## Идентичность кандидата
- worktree: `/home/alex/ahp-hive/worktrees/exec-a2a-card-20261031`, ветка `exec/a2a-card-20261031`
- код-коммит: `0b9fc26d83ca1d30df1eb6e3e0ae5b3bb00e42a9` (`0b9fc26`), родитель `1910a7c` (принят)
- этот артефакт — docs-коммит поверх кода (прецедент `9b20da2`); после коммита dirty=`0`,
  sha256 dirty-diff = `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` (пустое)
- бинарь кандидата `go build -tags ahp_exec,ahp_data -trimpath ./cmd/ahp` (CGO_ENABLED=0, пайновое
  окружение сборки, go1.26.2 linux/amd64): sha256
  `c7a81cd2abecb22a78a466b76f422313a3c1e0db1f4a1335724f37f22eb4866d`
- лог `-race` прогона (sha256 ниже): sha256
  `d67626b00ee8fcdfc31f87148757ad94bb3bd1e45cf315586ff7ba80f23569dd`

## Диагноз — ровно на названной границе (`messageText` не правился)
Текст на проводе есть и лежит в `history[0]` той же задачи. Теряется не тип части, а **роль сообщения**:
- a2a-go v2.7.0 объявляет `MessageRoleUser = "ROLE_USER"` (`a2a/core.go:162`) и **не нормализует**
  входящее значение: `MessageRole.UnmarshalJSON` пишет строку дословно (`a2a/core.go:178-190`).
- Клиент v0.3 пишет `"role":"user"` — это и эталонный потребитель репозитория
  (`tests/e2e/emil-tool-hop.mjs:54`), и операторская проба `tools/a2a-public-send-probe.sh`.
- Gateway журналировал роль дословно, а выбор входа в воркере сравнивал её с каноническим
  значением (`Service.lastInbound`: `hist[i].Role == a2a.MessageRoleUser`) => user-строка не
  находится => `ExecInput.Input = nil` => `execbridge: no usable text input` => `TASK_STATE_FAILED`
  при тексте в собственной истории задачи.
- Почему было зелёным: все прежние тесты строили сообщение через
  `a2a.NewMessage(a2a.MessageRoleUser, ...)`, то есть каноническим написанием.

## Фикс (только разрешённое владение: `internal/gateway`, `internal/execbridge`, `tests/execution`)
1. `internal/gateway/role.go` — `canonicalInboundRole`: `user`/`role_user`/`ROLE_USER` -> `ROLE_USER`;
   пусто / `ROLE_UNSPECIFIED` / `agent`|`ROLE_AGENT` / неизвестное -> `contracts.ErrInvalid`
   (handler `mapError` => JSON-RPC **-32602**). «Подставим дефолт» отсутствует как поведение.
2. `Service.SendMessage` применяет её **до** idempotency-digest и **до** журналирования: digest,
   строка `task_messages` и все отдаваемые копии говорят одним значением (в history отдаёт `ROLE_USER`).
3. `selectLastInbound` выбирает последний user-ход по каноническому совпадению => задачи, уже
   записанные legacy-написанием (живая БД стенда), перестают быть забытыми **без** переписывания истории.
4. `internal/execbridge` разделяет два разных факта: `task carries no inbound user message` и
   `inbound message has no text part` (раньше оба звались «no usable text input» и вели оператора
   по ложному следу «потеря текста на проводе»).
5. Дисциплина fail-closed в `internal/gateway`: отсутствующий `AHP_TEST_DATABASE_URL` = **FAIL** с
   причиной и указанием средства (раньше был тихий `t.Skip`), обход только `AHP_ALLOW_SKIP_REAL_DB=1`
   с печатью `SKIP_EXPLICIT`, которую приёмка не считает.

Сознательно НЕ сделано: карточка не начинает объявлять v0-методы (снисходительность к написанию
роли — не объявление v0-протокола; ложные объявления методов по-прежнему отклоняются на загрузке);
`messageText` не тронут; `internal/space`, `internal/state`, PKI, живые PID не тронуты.

## Тесты, которые ловят именно этот дефект
- `internal/gateway/role_test.go` — enum-правило входа (`user`/`ROLE_USER`/пробелы против
  `agent`/пусто/`ROLE_UNSPECIFIED`/мусор) и правило выбора user-хода (legacy-строка находится;
  агент-история => nil).
- `internal/gateway/legacy_role_test.go` (реальный PostgreSQL) — строка истории переписывается в
  `"role":"user"` **до** старта воркера; задача дренируется в `COMPLETED`, исполнитель получил текст,
  nil-входа нет; самопроверка декодирует jsonb, а не сопоставляет байты.
- `tests/execution/v0_role_wire_test.go` (боевое публичное лицо: mTLS + scoped task token + PG) —
  сырой v0 JSON (`role:"user"`, части с `kind:"text"`) => 200 + `COMPLETED`, исполнитель видел ровно
  канонический текст, `history` отдаёт `ROLE_USER`; негативы `agent`/`ROLE_AGENT`/пусто/мусор =>
  -32602, задача не создана, исполнитель не вызван.
- `tests/execution/v0_role_live_test.go` (opt-in `AHP_LIVE_QWEN=1`) — то же сырое v0-тело сквозь
  option-A лицо => реальная Qwen-генерация через Model Gateway => `COMPLETED` + артефакт в MinIO.

## Green-before-done: что реально прогнано на ЭТОМ дереве
Инструмент — только `tools/ahp-lock-run.py --lock run/build.lock`. Окружение: `execution.env`
(`AHP_TEST_DATABASE_URL`, база `ahp_hive_execution_live`) + `env/journey-minio.env` +
`env/shared-vault.env`; `AHP_ENABLE_DATA=1 AHP_ENABLE_EXECUTION=1 AHP_S3_BUCKET=as-artifacts
AS_ARTIFACT_BUCKET=as-artifacts AHP_LIVE_QWEN=1`; `GOROOT=tools/go GOCACHE=gocache GOPROXY=off`.

- `go build ./...` -> OK (`BUILD_ALL_OK`)
- `go test -race -count=1 -timeout 40m -v ./internal/gateway/ ./internal/execbridge/ ./tests/execution/`
  -> rc=0; **PASS=59 FAIL=0 SKIP=2 SKIP_EXPLICIT=0 DATA RACE=0**
  - `ok  internal/gateway    155.157s`   - `ok  internal/execbridge   6.844s`   - `ok  tests/execution    195.292s`
- Ключевые строки журнала: `TestCanonicalInboundRole PASS`, `TestSelectLastInbound PASS`,
  `TestIsUserTurnRole PASS`, `TestLegacyRoleRowStillReachesExecutor PASS 19.47s`,
  `TestV0RoleSpellingReachesExecutor PASS 11.12s`, `TestNonUserInboundRoleFailsClosed PASS 12.79s`,
  `TestLiveV0RoleThroughFaceProducesArtifact PASS 15.83s`,
  `TestLiveNativeQwenThroughOptionAFace PASS 22.33s`, `TestLiveExecutorQwenToolRoundTrip PASS 4.50s`.
- Инвентарь 2 SKIP (внешний Redis-нотификатор, отношения к блоку не имеет):
  `TestRedisNotifierRoundTrip`, `TestRedisNotifierTimeout`. `SKIP_EXPLICIT=0`.
- Негативы не регрессировали (тот же прогон): чужой tenant `GetTask` = -32001, отмена
  завершённой = -32002, v0-имена методов = -32601, публичное лицо без креденшелов = 403.
- Первый полно-пакетный прогон `./internal/gateway` (без `-race`) давал 2 падения
  `ledger Migrate ... context deadline exceeded` (31s) при контентентном PostgreSQL; повторный
  прогон того же пакета и `-race`-прогон — зелёные. Диагноз: таймаут миграции при конкуренции за
  инстанс, не продуктовый код этого блока (заявлено явно, а не «всегда было зелено»).

## Чего этот блок НЕ заявляет
- Приёмка `A2A_SEND_STAND=PASS` на живом стенде — **не заявлена**: стенд поднят на бинаре пина без
  этого фикса, живые PID и запуск стенда я не трогаю (прораб поднимает сам). Авторская живая нога
  того же сценария — `TestLiveV0RoleThroughFaceProducesArtifact` (PASS, см. выше).
- `-race` по всему репозиторию не гонялся: прогнаны именно затронутые пакеты + lane-сьют.
- Приёмку объявляет IV/прораб на чистом worktree (author != verifier).

---

# FROZEN-кандидат (указание прораба 05:47Z): измеренное дерево `35c865b08`

## Идентичность (head + sha, идентичность = head + sha бинаря)
- commit.sha кода (измеренное дерево): `35c865b08f9c79d9cc4c2dcd02daa027ba7f369e`
- ФРОЗЕН head = этот docs-only коммит поверх `35c865b08` (объявляется в форуме строкой
  `exec-frozen-<sha>`); `git diff 35c865b08..<frozen>` = ровно этот файл, Go-исходники идентичны,
  значит бинарь и все числа относятся и к нему.
- worktree `worktrees/exec-a2a-card-20261031`, branch `exec/a2a-card-20261031`,
  base-пин `18a2268`, merge-base = `1910a7c` (замер прораба), пробный merge `35c865b08 -> 18a2268` чистый.
- dirty: `0` строк; sha256 dirty-diff `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
  (пустое); untracked: 0 файлов (манифест `ahp.candidate-identity.v1`:
  `run/exec-a2a-card-20261031b/acceptance/candidate-manifest-35c865b08.json`).
- бинарь `cmd/ahp -tags ahp_exec,ahp_data -trimpath` с этого дерева (CGO_ENABLED=0, go1.26.2):
  sha256 `b5a4c832047cee234507ef4fea9dd59dcf667610ec6a1377c989581ccdf42bb7` (36 141 880 B).
- лог `-race` прогона: sha256 `6aaa9d79cf0193ddef5c8b953a007d322def6e3adf7a100c5945fa531ccb8219`,
  durable-копия `run/exec-a2a-card-20261031b/acceptance/race-35c865b08-frozen.log` (проверен на
  секреты/DSN: 0 совпадений).

## Что добавлено поверх `0b9fc26` — ровно два пункта дефект-карточки
1. `internal/gateway/content.go` — `validateInboundContent`: сообщение, у которого ни одна часть
   ничего не несёт (`{"text":""}` — корректная, но пустая часть), отклоняется на **входу** как
   `contracts.ErrInvalid` => JSON-RPC **-32602**, и задача под него НЕ создаётся. Раньше такая
   заявка получала task id, а умирала внутри воркера — для оператора это читалось как «сломанный
   исполнитель».
2. Форма части НЕ меняется и закреплена тестом, а не спором: A2A 1.0 (версия, которую объявляет
   карточка этого лица) не имеет дискриминатора `kind` у части — a2a-go плющит часть в
   `{"text": ...}`; v0-`kind` принимается библиотекой и игнорируется, текст round-trip-ится.
   `TestV0RoleSpellingReachesExecutor` пинит обе половины: текст сохранён и в ответе нет `kind`.
3. BLOCK-3 усиление: `TestCardClaimsAreServedByPublicFace` второй раз прогоняет заявленный навык
   **сырым v0-телом** и требует `COMPLETED` + артефакт — навык, который работает только для
   вручную собранного канонического запроса, карточку не пройдёт.
4. Тестовый харнесс: DDL-бюджет `30s -> 3m` (`internal/gateway/gateway_integration_test.go`,
   `tests/execution/composition_test.go`). Это источник ложных FAIL
   `gateway/ledger Migrate: context deadline exceeded` на занятом общем PostgreSQL; настоящая
   ошибка миграции по-прежнему падает сама собой, убран только «мы сдались слишком рано».

## Green-before-done: что реально прогнано на ЭТОМ дереве (гейт -> RC)
Инструмент — только `tools/ahp-lock-run.py --lock run/build.lock` (одна захватка, без вложенных
повторов). Окружение: `private/root-acceptance-pg/execution.env` (база `ahp_hive_execution_live`) +
`env/journey-minio.env` + `env/shared-vault.env`; `AHP_ENABLE_DATA=1 AHP_ENABLE_EXECUTION=1
AHP_S3_BUCKET=as-artifacts AS_ARTIFACT_BUCKET=as-artifacts AHP_LIVE_QWEN=1`;
`GOROOT=tools/go GOCACHE=gocache GOMODCACHE=gomodcache GOPROXY=off GOTOOLCHAIN=local CGO_ENABLED=1`.

| гейт | RC | результат |
| --- | --- | --- |
| `go build ./...` | 0 | `BUILD_ALL_OK` |
| `go build -tags ahp_exec,ahp_data -trimpath -o /tmp/composed-35c865b08 ./cmd/ahp` | 0 | sha256 выше |
| `go test -race -count=1 -parallel 2 -timeout 40m -v ./internal/gateway/ ./internal/execbridge/ ./tests/execution/` | 0 | **PASS=61 FAIL=0 SKIP=2 SKIP_EXPLICIT=0 DATA RACE=0** |

- `ok github.com/ahp-platform/ahp/internal/gateway    240.285s`
- `ok github.com/ahp-platform/ahp/internal/execbridge   6.829s`
- `ok github.com/ahp-platform/ahp/tests/execution     395.000s`
- Ключевые PASS (время из журнала): `TestValidateInboundContent 0.00s`,
  `TestEmptyContentRefusedAtIngress 24.45s`, `TestV0RoleSpellingReachesExecutor 17.02s`,
  `TestLegacyRoleRowStillReachesExecutor 43.76s`, `TestNonUserInboundRoleFailsClosed 15.73s`,
  `TestCardClaimsAreServedByPublicFace 30.77s`, `TestLiveV0RoleThroughFaceProducesArtifact 26.94s`,
  `TestLiveNativeQwenThroughOptionAFace 43.88s`, `TestLiveExecutorQwenToolRoundTrip 3.58s`,
  `TestSendMessageIsIdempotentPerCallerAndTarget 18.19s` (новый ingress-гейт идемпотентность не сломал),
  `TestTaskIsInvisibleOutsideItsScope 22.50s`.
- Негативы того же прогона (строки журнала): `-32602` x5 (пустая роль / `agent` / мусорная роль /
  content-free), `-32601` x11 (v0-имена методов не объявляются), `-32001` x2 (чужой tenant/subject),
  `-32002` x1 (отмена завершённой).
- Инвентарь 2 SKIP — внешний Redis-нотификатор, к блоку отношения не имеет:
  `TestRedisNotifierRoundTrip`, `TestRedisNotifierTimeout`. `SKIP_EXPLICIT=0`: ни одного обхода
  через `AHP_ALLOW_SKIP_REAL_DB` (отсутствующая реальная зависимость = FAIL с причиной).

## Что НЕ перепроверялось (перенесено с `0b9fc26`, честно)
- `./adapters/emil` в этом прогоне не гонялся: `35c865b08` этот пакет не меняет
  (`git show --stat 35c865b` = `internal/gateway` + `tests/execution`), а его зелёные числа сняты
  на `0b9fc26` (`ok adapters/emil 1.086s`, `TestAdapterWithoutClientReturnsError`,
  `TestNewRequiresBaseURL`). Код пакета между этими коммитами идентичен.
- Полный `-race` по всему репозиторию не гонялся: прогнаны затронутые пакеты + lane-сьют.
- `-p 1` в финальном прогоне НЕ использовался: узел насыщен (4 vCPU, load 11–14), `-p 1` кратно
  удлиняет компиляцию/линковку. Прогон шёл `-race -count=1 -parallel 2` (`-parallel` — пул тестов,
  не параллельная сборка пакетов).
- Живая нога `tools/a2a-public-send-probe.sh` на стенде — не моя и здесь не заявлена: стенд поднимает
  прораб, живые PID/порты я не трогаю. Авторская равносильная нога с реальной генерацией через
  Model Gateway: `TestLiveV0RoleThroughFaceProducesArtifact PASS 26.94s` (`COMPLETED` + артефакт).
- Шаг-бюджет rана (`defaultMaxSteps`/`AHP_EXEC_MAX_STEPS` против пользовательского `max_steps`) —
  отдельная дефект-карточка прораба от 06:1xZ, в этот коммит НЕ входит: другой предмет, другой
  приёмочный контур. Локализация уже снята и описана в форум-посте.

## Чего этот FROZEN не заявляет
- Приёмку объявляет прораб/IV: `tools/dir-verify-exec.sh <frozen-sha>` на чистом worktree +
  живая нога на перепиненном стенде. Мои PASS'ы — evidence автора.
- `internal/space`, `internal/core`, `internal/state`, `internal/stateapi`, PKI, живые PID — не тронуты.
