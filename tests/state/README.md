# State HTTP стенд и consumer-набор (реальные HTTPS+mTLS+PostgreSQL, без mock)

Два публичных инструмента этой ленты, чтобы SDK и кастомный A2A агент проверяли контракт против
**настоящего** сервера State, а не против mock/httptest:

| Инструмент | Что делает |
|---|---|
| `cmd/state-stand` | поднимает продуктовый `internal/stateapi` обработчик: реальный HTTPS+mTLS, реальные Ed25519 task tokens, ре PostgreSQL, схема создаётся продуктовыми миграциями |
| `cmd/state-consumer` | проходит весь контракт по проводу и пишет доказательство (JSONL), возвращает ненулевой код при любом отклонении — используется как гейт |

Реализация лежит в `internal/stateapi/stand` и `internal/stateapi/consumer` (обе — библиотеки с
`Run(...) error`, они не вызывают `os.Exit`), а `main`-файлы тонкие: только маппинг результата в код выхода.
Исторические пути `tests/state/serve` и `tests/state/httpprobe` сохранены как шимы поверх тех же библиотек:
приёмочные инструменты директора собирают `./tests/state/serve` и запускают
`./tests/state/serve/run-http-e2e.sh`, поэтому эти пути обязаны жить и вести себя как раньше.

Ничего не эмулируется: тот же handler, тот же store и те же миграции, что использует Go SDK.

## 1. Быстрый старт (одна команда на поднятие стенда)

```sh
mkdir -p /tmp/state-handshake
python3 /home/alex/ahp-hive/tools/ahp-lock-run.py --lock /home/alex/ahp-hive/run/build.lock --wait 1800 -- \
  env -u AHP_S3_ENDPOINT go run ./cmd/state-stand \
    -env /home/alex/ahp-hive/private/root-acceptance-pg/state-owner.env \
    -handshake /tmp/state-handshake/handshake.json
```

Стенд печатает `READY` и держится, пока его не убьют. Он:
* загружает env-файл ленты (`AHP_TEST_DATABASE_URL` подставляется в `AHP_STATE_DSN`);
* **отказывается** запускаться против базы вне `-db-allowlist` (по умолчанию `ahp_hive_state_owner`) —
  миграции не будут выполнены на чужой или продуктовой базе; в лог попадает только ИМЯ базы, не креды;
* поднимает свой PKI (CA + серверный и клиентский сертификаты) и монтирует workload одного tenant/deployment;
* пишет `handshake.json` (права 0600) — единственный носитель секретов для дочернего процесса.

Флаги (каждый дублируется переменной окружения; окружение — единственный источник истины, флаги
записываются в него до чтения): `-env`, `-dsn`, `-handshake`, `-addr`, `-tenant`, `-deployment`,
`-subject`, `-foreign-tenant`, `-refresh-seconds`, `-db-allowlist`.
Значения по умолчанию заполняются **по одному на переменную**: если caller (или env-файл) задал
`-tenant`, он и остаётся; недостающие `deployment`/`subject` выводятся из него (`d-<tenant>`,
`user-<tenant>`). Флаг никогда не перекрывает окружение своим значением по умолчанию.

## 2. Прогон consumer-набора против стенда

```sh
python3 /home/alex/ahp-hive/tools/ahp-lock-run.py --lock /home/alex/ahp-hive/run/build.lock --wait 1800 -- \
  env -u AHP_S3_ENDPOINT go run ./cmd/state-consumer \
    -handshake /tmp/state-handshake/handshake.json \
    -evidence /tmp/state-e2e/evidence.jsonl \
    -worker w-manual-1
```

`exit 0` = все проверки прошли и все операции покрыты успехом; `exit 1` = есть отклонение или непокрытая
операция; `exit 2` = неверный запуск (нет handshake и т.п.). Итоговая строка в stderr:
`state-consumer: calls=… failures=… ops_succeeded=37/37 missing=`.

Одной командой (стенд + набор + идентичность кандидата):

```sh
python3 /home/alex/ahp-hive/tools/ahp-lock-run.py --lock /home/alex/ahp-hive/run/build.lock --wait 1800 -- \
  env -u AHP_S3_ENDPOINT ./tests/state/serve/run-http-e2e.sh
```

Скрипт поднимает стенд на своей базе, прогоняет набор, кладёт evidence в
`/home/alex/ahp-hive/run/state-owner-20261031/evidence/` и фиксирует идентичность кандидата
(`tools/candidate-manifest.py`: head + sha dirty-diff + sha untracked + путь бинарника).
Бинарники он собирает без `-trimpath` — как и вся платформа; из-за этого хэши бинарников между машинами
не совпадают, кандидата идентифицирует манифест по git head + sha грязного дифа.

## 3. Что в handshake

```json
{"base_url":"https://127.0.0.1:PORT","ca_file":"…","cert_file":"…","key_file":"…",
 "task_token":"…","tenant":"…","deployment":"…","subject":"…",
 "foreign_tenant":"…","foreign_cert_file":"…","foreign_key_file":"…","foreign_task_token":"…"}
```

* `cert_file/key_file` — workload-сертификат этого tenant/deployment (именно им аутентифицируется `ClaimRun`);
* `task_token` — Bearer-кредент для остальных операций;
* `foreign_*` — второй, **чужой** tenant: нужен, чтобы проверить анти-прощупывание на проводе
  (чужая область отдаёт `NOT_FOUND`, а не `FORBIDDEN`);
* **важно для SDK:** task token живёт не дольше 15 минут (`pkg/auth` maxTaskTTL — продуктовый
  инвариант, не обходится). Стенд пере-минтит его каждые `AHP_STATE_REFRESH_SECONDS` (по умолчанию 240)
  и переписывает файл атомарным rename. Долгоживущий клиент обязан **перечитывать handshake**, а не
  кэшировать токен на всё время жизни контейнера.

## 4. Формы запросов (из кода пина, не из предположений)

* всё — `POST /v1/state/<Op>`; не-POST и неизвестный `<Op>` = `NOT_FOUND` (не 405);
* `ClaimRun` — только workload-сертификат (без Bearer); остальные операции — Bearer task token;
* вложенный `fence` (`{"run_id","worker","epoch"}` внутри поля `"fence"`): `PutCheckpoint`, `PutWrites`,
  `PutKV`, `DeleteKV`, `RecordIntent`, `UpdateIntent`;
* ПЛОСКИЕ `run_id`/`worker`/`epoch` верхним уровнем: только `Heartbeat`, `Release`, `Suspend`;
* KV `namespace` — **массив** строк; checkpoint `namespace` — **строка**; `SearchKV` принимает
  `namespace_prefix` (массив) + `key_prefix`, поля `namespace` у него нет;
* сервер `DisallowUnknownFields`, отклоняет дубликаты JSON-ключей, пустое тело и тело > 1 MiB;
  scope (`tenant`/`subject`/`deployment`) берётся только из проверенного токена.

### 4.1 `ImportThread`: область и адрес проверяются на входе

Байты экспорта — bearer-артефакт: по определению путешествуют между узлами (PDF rev3.2,
«переход между драйверами»), поэтому на входе действуют два правила, а не одно «создай тред».

* **Область.** Экспорт несёт происхождение в `export.thread.scope`. Если `tenant` или `subject`
 origin’а отличается от области вызывающего — **403** + `code=FORBIDDEN` +
  `reason=EXPORT_SCOPE_MISMATCH`, и в БД **не появляется ни одной строки**. Чужой субъект больше
  не может материализовать durable-клон чужого треда под своим identity.
* **Адрес.** Целевой `context_id` не должен быть занят внутри `(tenant, deployment)` — ни этим
  subject, ни другим: `context_id` — это то, как human-edge резолвит тред, поэтому два субъекта
  с одним `context_id` = перевязка идентичности на входе. Отказ: **409** + `code=CONFLICT` +
  `reason=IMPORT_CONTEXT_TAKEN`, снова без строк.
* **Retarget.** Полем `context_id` в запросе вызывающий явно называет новый адрес. Без него
  наследуется `context_id` из байтов — это законно только пока он свободен в `(tenant, deployment)`,
  поэтому «восстановиться на место» = передать новый `context_id`. `thread id` всегда новый.
  Перевязка между **deployment** одного tenant+subject разрешена (для этого операция и есть).
* Проверки считаются проведёнными кейсами `import.refusal_reason`, `restore.stream_digest_equal`,
  `restore.checkpoint_parts_digest_equal` (consumer) и тестами
  `TestImportThreadRefusesForeignIdentityOnTheWire`, `TestImportThreadRestoresUnderExplicitRetarget`,
  `TestImportThreadRefusesTakenContextWithStableReason` (`internal/stateapi`).
* SDK-снапшот (`sdk/go/state/client.go:631`) пока умеет слать только `{"export":…}`, то есть
  без retarget: владельцу SDK нужно добавить поле `context_id`, иначе восстановление в свой
  контекст из SDK недостижимо (409 по дизайну контракта). Запрос отправлен в `mac-sdk-ci`.

## 5. Покрытие и известные дефициты

Покрыто 37 форм: 36 операций, поимённо перечисленных в аддендуме `20261031T015810Z-…` (заголовок того
поста говорит «35», собственный список содержит 36 — проверено парсингом), плюс `ImportThread`, плюс
намеренные негативные проверки (`NoSuchOperation`, запрос без сертификата, лишние поля, дубликаты ключей).

Что честно не так и находится не в этой ленте:

1. Класс `KEY_REUSE_MISMATCH` живёт на **400** (решение директора от 2026-10-31): `code` в теле остаётся
   `INVALID_ARGUMENT` — Space и SDK уже маппят 400, новый класс статусов не вводится, — а доменный код
   передаётся стабильным полем **`reason=KEY_REUSE_MISMATCH`**. Форма ответа:
   `{"code":"INVALID_ARGUMENT","message":"…","reason":"KEY_REUSE_MISMATCH"}`; у «битого тела» поля
   `reason` нет вообще, поэтому «переиспользован ключ операции» и «битое тело» различимы полем ответа,
   а не текстом `message`. Закреплено тестами `pkg/transport.TestWriteErrorKeepsDomainCodeAsReason` и
   проводными проверками `error.reason_is_key_reuse` (PutKV, PutCheckpoint, ReconcileIntent) и
   `error.malformed_body_has_no_reason`. Клиенту, которому важен доменный код, читать `reason`, а не `code`.
2. Потолок task token 15 минут → долгая сессия требует перечитывания handshake (см. п.3).
3. JSONB нормализует порядок ключей: значения KV надо сравнивать по `version`/digest, а не по сырым байтам.
4. Идемпотентность KV адресована `(tenant, deployment, subject, operation_key)` — `namespace`/`key`
   входят в digest запроса, но не в идентичность квитанции. Повтор одного `operation_key` на другой
   элемент **отвергается**, и на проводе это `400` + `code=INVALID_ARGUMENT` +
   `reason=KEY_REUSE_MISMATCH` (п.1) — канон для SDK, а не для item-области.
5. Фильтр `namespace` у `ListCheckpoints`/`GetCheckpoint`/`GetWrites`/`GetPart` — **точное совпадение**,
   причём `""` — это собственное имя namespace, а не «все сразу». Space пишет снимки в
   `space/core`, поэтому чтение без `namespace` отдаёт `{"checkpoints":[]}` и выглядит как «снимков нет».
   Это поведение контракта, а не дефект: набор consumer-проверок закрепляет его проводными кейсами
   (`checkpoints.in_space_ns`, `checkpoints.empty_ns_is_not_all`). Читай `namespace` из той же
   константы, что и писатель.

## 6. Как это использует IV

```sh
# своя изолированная база: свой env-файл + своё имя в allowlist
go run ./cmd/state-stand -env <свой-env> -db-allowlist <своя_база> -handshake /tmp/iv-handshake/h.json &
go run ./cmd/state-consumer -handshake /tmp/iv-handshake/h.json -evidence /tmp/iv/evidence.jsonl -worker w-iv
```

E2E-гейт: ненулевой exit у `cmd/state-consumer` означает «кандидат не соответствует контракту».
Идентичность собранного кандидата фиксируется отдельно:

```sh
python3 /home/alex/ahp-hive/tools/candidate-manifest.py --worktree "$PWD" \
  --binary /path/to/state-stand --out /tmp/iv/candidate.json --note "…"
```

Регрессия Go-слоя (реальная PG, real mTLS-сервер, без mock). Env-файл ленты задаётся **абсолютным** путём:
относительный путь молча не подгружает базу, тесты скипаются, а тихий SKIP — это провал харнесса, а не
зелёный прогон:

```sh
python3 /home/alex/ahp-hive/tools/ahp-lock-run.py --lock /home/alex/ahp-hive/run/build.lock --wait 1800 -- \
  sh -c 'set -a; . /home/alex/ahp-hive/private/root-acceptance-pg/state-owner.env; set +a; \
         env -u AHP_S3_ENDPOINT go test -p 1 -race -count=1 -v ./internal/state/... ./internal/stateapi/... \
         | grep -E "^(    )?--- " | sort | uniq -c'
```

Ожидаемый итог — строки `--- PASS` и ровно ноль строк `--- SKIP`.

Локально зелёное ≠ приёмка: приёмку выполняет директор, автор не является верификатором.

## 7. Data-plane изоляция по tenant: две tenant в одной PostgreSQL

HTTP-состав стенда эту изоляцию проверить **не может**: dev-PKI стенда даёт человеку и workload один и
тот же tenant (`identityapi` сверяет их на exchange), разрешённый обменщик один, поэтому человек чужого
tenant получает `403` ещё на `AcceptTurn` и до предиката tenant в запросах State дело не доходит.
Кросс-**субъектная** изоляция измеряется на стенде (404 в обе стороны); кросс-**tenant** измеряет нога
`tests/state/verdict-cross-tenant.sh`.

Что она делает: один и тот же живой сервер State, одна и та же база, **две tenant**. Обе principals
создаются самим продуктовым слоем — настоящий workload-сертификат на свой SPIFFE-URI, настоящий
Ed25519 task token, все строки созданы через `CreateThread`/`CreateRun`/`ClaimRun`/`AppendStream`/
`PutCheckpoint`. SQL-подделок строк нет, PKI стенда нет. `subject` у обеих tenant одинаковый — утечка
может прийти только из предиката tenant.

Утверждения (каждое — отдельный факт с machine-readable строкой `ASSERT|name=…|verdict=…`, всего 58):

| группа | что утверждается | ожидаемый ответ |
|---|---|---|
| `DENY_A_reads_B_<Op>` / `DENY_B_reads_A_<Op>` | чужая tenant не читает тред по `GetThread`, `GetThreadByContext`, `ListCheckpoints`, `GetPart`, `ReadStream`, `ExportThread` — в обе стороны | `404` + `NOT_FOUND` |
| `NO_ORACLE_*` | отказ неотличим от «такого треда нет»: сравнимы и статус, и `code`, и `reason` | идентично ghost-запросу |
| `DENY_*_GET_stream` | форма `GET /v1/state/stream?thread_id=…` чужого треда (несуществующего роута ровно столько же, сколько ghost) | `404` + `NOT_FOUND` |
| `POSITIVE_*_owns_*` | positive control: владелец читает свой тред теми же маршрутами | `200` |
| `TRANSPORT_BIND_*` | token привязан к сертификату: чужой task token на чужом сертификате отвергается транспортом | `401`/`403` |
| `IMPORT_FOREIGN_*` | F1: экспорт чужой tenant отвергается по `thread.scope`, **ничего не записывается** (счётчик треда импортера снимается ДО попытки) | `403` + `reason=EXPORT_SCOPE_MISMATCH` |
| `IMPORT_OWNER_*` | F2: владелец восстанавливает свои байки явным `context_id` (новый `id`, совпадающие digest стрима, число частями и байты `checkpoint_parts`); восстановление в собственный живой контекст — документированный отказ | `200` / `409` + `reason=IMPORT_CONTEXT_TAKEN` |
| `REPEAT_*` | вторая свежая пара tenant в той же базе даёт тот же вердикт (нет зависимости от порядка строк и кэшей) | как выше |

```sh
python3 /home/alex/ahp-hive/tools/ahp-lock-run.py --lock /home/alex/ahp-hive/run/build.lock --wait 3600 -- \
  sh /home/alex/ahp-hive/worktrees/state-owner-20261031/tests/state/verdict-cross-tenant.sh
```

Нога отказывается считать зелёным: грязный worktree (`tracked_dirty≠0` или `untracked≠0`), база вне
allowlist, ненулевой `rc` теста, любой `verdict=FAIL`, любой `SKIP`, panic/data race, и меньше 58
утверждений. Итоговая строка — `CROSS_TENANT=PASS` или `CROSS_TENANT=FAIL`, и только PASS выходит нулём.
Доказательство: `run/state-owner-20261031/evidence/cross-tenant-<UTC>/evidence.jsonl` (по записи на
утверждение + запись вердикта) и `verdict.md` (таблица «утверждение → факт → вердикт»); в вердикт-записи
`git_head`, `tracked_dirty`, `untracked`, **имя базы** и два хеша тестового бинаря — файла и
`/proc/<pid>/exe` запущенного процесса.

## 8. Публичный стенд на Agreement-порту 18092

`tests/state/serve/run-public-stand.sh` поднимает **уже собранный** кандидат (бинари передаются через
`AHP_STATE_SERVE_BIN`/`AHP_STATE_PROBE_BIN`, сборка — только под `run/build.lock`) на
`127.0.0.1:18092` в собственной базе ленты и не убивает его после завершения ноги:

```sh
AHP_STATE_SERVE_BIN=/tmp/state-candidate-<short>/state-stand \
AHP_STATE_PROBE_BIN=/tmp/state-candidate-<short>/state-consumer \
  bash tests/state/serve/run-public-stand.sh
# остановить: kill "$(cat /home/alex/ahp-hive/run/state-owner-20261031/stand-18092/stand.pid)"
```

Нога поднимает стенд только на чистом дереве (`tracked_dirty=0` и `untracked=0`, иначе отказ) и
публикует в `run/state-owner-20261031/stand-18092/`:

- `FROZEN-<run>.json` — `git_head`, `tracked_dirty`, `untracked`, имя базы, `pid`, sha256
  `/proc/<pid>/exe`, sha256 файла бинаря, адрес, пути до handshake/evidence, список наблюдаемых операций;
- `ACCESS-<run>.md` — как подключиться: адрес, **пути** (не содержимое) к CA/client-сертификату/ключу и
  чужому tenant, где лежит `task_token` и как часто он переуказывается;
- `handshake.json` (`0600`) — единственный носитель кредов; секреты в посты и логи не попадают;
- `evidence/consumer-selftest-<run>.jsonl` + его sha256 — прогон `cmd/state-consumer` против этого же
  эндпоинта как **авторская** самопроверка (не приёмка: автор ≠ верификатор).

Task token ограничен 15 минутами, поэтому кандидат переуказывает его в handshake каждые
`AHP_STATE_REFRESH_SECONDS` (по умолчанию 420): клиент, перечитывающий handshake, всегда находит живой
токен. Слушается loopback; доступ с другой машины — туннелем, а не расширением диапазода стенда.
