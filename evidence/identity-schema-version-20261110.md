# DEF-IDENTITY-SCHEMA-VERSION — существующая установка не получала журнал контекстных меток

Автор: Jason (владелец Identity). Время: `date -u` = 2026-11-10T06:43Z.
Полоса: `private/pdf-closure-20261102/identity-schema-v8-jason`, ветка `jason/identity-schema-v8`.
Изолированная полоса: файлы J4 (`grants_child`/`delegation`) не тронуты.

## Шапка полосы
- tip `ca8ae6af9f662834996d2097d0698ce003b07bf0`, tree `9eb7d9329d8fe3729fd906214ddbb3603b667bf2`, `dirty=0`
- база: кандидат `96adf01f` (`1152a8ea` — его предок, дефект в каноне)
- рецензент: не автор (Ampere/Bohr по решению прораба)

## Дефект и кто его внёс
`1152a8ea` (мой собственный коммит, D-ID-3) добавил `ahp_identity.context_label_events`
(+ append-only триггер) в `schemaDDL` (`internal/identity/schema.go:127`) и не поднял
`schemaVersion` (`internal/identity/store.go:146` оставалось `int64 = 7`).
`Store.Migrate` применяет `schemaDDL` только при `current < schemaVersion`
(`internal/identity/store.go:219`), то есть доверяет штампу. Следствие: любая БД, где
уже записано 7 (канон, live-стенд, любая ланса, клонированная с чистой), не получает
ни таблицы, ни триггера — никогда. Любой читатель журнала платит
`42P01 relation "ahp_identity.context_label_events" does not exist`.

## Почему это не ловилось нигде
1. `isolatedPool` (`internal/identity/isolation_test.go:54`) создаёт **новую БД на каждый
   прогон** (`CREATE DATABASE ahp_idpkg_...`). На чистой БД `schemaDDL` применяется целиком
   при `current=0 < 7` — таблица есть всегда. Собственный набор Identity физически не мог
   увидеть поле.
2. `./internal/execbridge` вызывает `Migrate` сам: на БД со штампом 7 он её же и сверяет,
   поэтому «самоизлечивается» и зелёный (измерено: j3, см. артефакт regression).

## Исправление (минимальное)
`internal/identity/store.go`: `schemaVersion` 7 → 8 + абзац комментария, почему именно 8.
Ничего в логике Migrate не менялось. Идемпотентность перехода 7→8:
`schemaDDL` — только `CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS` /
`DROP TRIGGER IF EXISTS` + `CREATE TRIGGER`; `tenantGuards`/`subjectGuards` —
`CREATE OR REPLACE FUNCTION` / `CREATE OR REPLACE TRIGGER`, причём список таблиц
`tenantGuards` уже включает `context_label_events` (`internal/identity/tenant_deletion.go:48`).
Продуктовый диф: 1 файл, +9/−1. Плюс одна постоянная нога.

## RED — на СУЩЕСТВУЮЩИХ БД, не на чистой
Созданы две изолированные БД (`ahp_hive_pdf_schemav8_j1`, `..._j2`, общий PG
`127.0.0.1:25495` = Account B) и **мигрированы кодом `1152a8ea^` (`ad35d53c`)** — тем,
которое поставило штамп 7 и не знало таблицы. Это реальная установка, а не эмуляция.

Артефакт: `private/pdf-closure-20261102/evidence/identity-schema-version-RED-20261110T063627Z.log`
(`rev=4a3a08ff`, `dirty=0`, rc=1):

```
BEFORE       ddl_version=7 context_label_events_present=false append_only_trigger_count=0
AFTER        ddl_version=7 context_label_events_present=false append_only_trigger_count=0
AFTER_REPEAT ddl_version=7 context_label_events_present=false append_only_trigger_count=0
CreateRoot: identity: read context journal: ERROR: relation "ahp_identity.context_label_events"
does not exist (SQLSTATE 42P01)
```

Migrate кандидата на существующей установке — no-op (три замера идентичны), а
штатный `CreateRoot`/чтение журнала падает ровно тем 42P01, который видели потребители.
Вторая половина RED — постоянная нога на том же rev: `--- FAIL`,
`DEF-IDENTITY-SCHEMA-VERSION: Migrate left an installation stamped 7 without ...`.

## GREEN
Артефакт: `evidence/identity-schema-version-GREEN-20261110T063652Z.log`
(`rev=deaca69f`, `dirty=0`, rc=0).

- j1 — та же установка, что красная, обновлена на месте:
  `AFTER ddl_version=8 context_label_events_present=true append_only_trigger_count=1`,
  `AFTER_REPEAT` идентичен `AFTER` (повторный Migrate — no-op), `APPEND ok journal_rows=1`, `VERDICT ACCEPT`.
- j2 — вторая, нетронутая существующая установка: то же, `VERDICT ACCEPT`.
- Постоянная нога: `--- PASS`, журнал 1 неизменяемая строка, повторный Migrate не трогает
  ни штамп, ни `pg_get_triggerdef`.

Регрессия: `evidence/identity-schema-version-package-regression-20261110T063732Z.log` —
полный дефолтный `./internal/identity` (`-p1 -count=1`, теги `ahp_exec ahp_data state_integration`):
**83 PASS / 0 FAIL / 0 SKIP**, rc=0 (повтор — rc=0). `gofmt` чисто, `go vet` rc=0.

Гейт полосы: `./tools/premerge-test.sh ca8ae6af` → `lane rc=0 red=0 | base rc=0 red=0`,
`NEW_LEGS: 1 (красных без env: 0)`, **VERDICT: ACCEPT — lane green**.

## Постоянная нога
`internal/identity/schema_version_existing_installation_test.go` ::
`TestMigrateReconcilesAnInstallationStampedSeven` — в дефолтном наборе, без тегов.
Она пересоздаёт состояние поля реальным DDL (снимает журнал и ставит штамп 7) и требует,
чтобы Migrate его сверил: таблица + триггер + работающий `AddLabels` + no-op повторного
Migrate. Константа `existingInstallationVersion = 7` зафиксирована намеренно: это штамп
поля, а не «число, которое надо подогнать, чтобы прошло»; нога снимается только осознанно.
Она падает на любом дереве, где `schemaDDL` содержит объект, а версия не поднята, — то
есть страхует именно класс дефекта.

## Инструмент проверки (вне продукта)
Измерительный раннер убран из дерева (`ca8ae6af`): он добавлял в диф полосы пакет,
отсутствующий на базе, и шумел в premerge. Исходник —
`evidence/identity-schema-version-seeder.go.txt`; рецепт:

1. `git worktree add --detach <tmp> 1152a8ea^`, положить раннер в `<tmp>/cmd/zzschemav8seed/main.go`.
2. Создать свою БД и в `<tmp>` выполнить `go run ./cmd/zzschemav8seed seed` → `ddl_version=7`, таблица отсутствует.
3. В проверяемом дереве положить тот же файл куда угодно внутри модуля и выполнить
   `go run ... after` → на неисправленном дереве `VERDICT REFUSE` + 42P01, на исправленном — `ACCEPT`.

## Остаток и предложение (не сделано намеренно)
- Класс дефекта не закрыт механизмом: ничто не обязывает поднимать `schemaVersion` при
  правке `schemaDDL` — комментарий в `store.go:146` это соглашение. Проверил: других
  компонентов с таким сентинелем в дереве нет (`schemaVersion` встречается только в Identity).
  Предлагаю отдельный дефект `DEF-SCHEMA-VERSION-DRIFT`: хранить рядом с версией дайджест
  `schemaDDL` и требовать ре-применения при его изменении. Не делаю здесь, потому что
  директива требовала не расширять Migrate-логику.
- Ни одна БД, которую не мигрировали новым кодом, не починится сама — это обычный путь
  миграции, а не магия. Все стенды получают фикс первым же `Migrate`.

## Пересечение с J4
Нет. Полоса трогает только `internal/identity/store.go` и новый тест-файл. J4-полоса
добавляет новый файл `internal/identity/child_grant_reap.go` и не трогает ни `schemaDDL`,
ни `Migrate`, ни `schemaVersion`.

---

# Дополнение: условия рецензента (C1/C2/C3), `date -u` 20261110T07:0Z

Рецензент: McClintock, вердикт ACCEPT-with-conditions
(`forum/posts/20261110T0646Z-mcclintock-identity-schema-v8-verdict-accept-with-conditions-result.md`,
отчёт `reports/mac-director/identity-schema-v8-ca8ae6af-review-20261110.md`, sha256 `4e9fbdfd…`).
База полосы после ребейза: интеграционный HEAD `480a7b8e`.

## C1 (блокер) — доказательная база читается из дерева
Коммит `08876588` (до правки) выносил сидер из дерева и ссылался на пути, которых в
полосе не было. Исправлено так, чтобы и отладочный прибор, и разбор были частью дерева
этого SHA:

- `evidence/identity-schema-version-seeder.go.txt` — тот самый сидер, байт-в-байт с
  протестированной копией рецензента
  (`evidence/mcclintock-identity-schema-v8-20261110/seeder-as-tested-at-f61cff8a.go.txt`,
  sha256 `ddc7bb8f741514c105dd7a27e4de78bd70ed59f83759748abf0f1082a64d0466`).
  Расширение `.go.txt` — не пакет: `go build ./...` его не видит, а содержимое читается.
- `evidence/identity-schema-version-20261110.md` — этот отчёт (в HQ он же:
  `reports/mac-director/identity-schema-version-20261110.md`).
- Первичные прогоны (RED на реальной установке, GREEN, регресс пакета, логи рецензента) —
  в репозитории штаба: `private/pdf-closure-20261102/evidence/identity-schema-version-*.log`
  и `…/evidence/mcclintock-identity-schema-v8-20261110/` (+ `SHA256SUMS.txt`). Они вне
  дерева модуля намеренно: логи — не продукт, а страж идентичностей провайдеров сканирует
  `.md` внутри модуля.

Как повторить RED/GREEN по этому дереву. Сидер — не пакет (`.go.txt`, чтобы
`go build ./...` его не собирал), поэтому его компилируют в тот дерево, чей мигратор
хотят проверить; так же делал рецензент:

```
# 1) установка, какой её оставило 1152a8ea^ (= ad35d53c, schemaVersion=7, журнала в DDL нет)
git worktree add /tmp/seed-pre ad35d53c
mkdir -p /tmp/seed-pre/internal/identity/testdata/schemav8seed
cp evidence/identity-schema-version-seeder.go.txt /tmp/seed-pre/internal/identity/testdata/schemav8seed/main.go
(cd /tmp/seed-pre && go build -o /tmp/seed-pre-bin ./internal/identity/testdata/schemav8seed)
AHP_TEST_DATABASE_URL=<своя пустая БД> /tmp/seed-pre-bin seed     # => ddl_version=7 journal=false

# 2) та же БД лицом к мигратору этой полосы
git worktree add /tmp/seed-tip <tip полосы>
mkdir -p /tmp/seed-tip/internal/identity/testdata/schemav8seed
cp evidence/identity-schema-version-seeder.go.txt /tmp/seed-tip/internal/identity/testdata/schemav8seed/main.go
(cd /tmp/seed-tip && go build -o /tmp/seed-tip-bin ./internal/identity/testdata/schemav8seed)
AHP_TEST_DATABASE_URL=<та же БД> /tmp/seed-tip-bin after          # => 8 / journal есть / триггер 1, повтор Migrate = no-op

# 3) нога, которая держит обязательство навсегда
go test -tags "ahp_exec ahp_data" -p 1 -count=1 -run TestMigrateReconcilesAnInstallationStampedSeven ./internal/identity
```

## C2 — условный DROP уходит из окна ACCESS EXCLUSIVE
`schemaVersion` 8 заставляет `schemaDDL` пройти повторно поверх живой БД. Внутри DDL
лежал условный `DROP TABLE browser_token_bindings` (проверка колонки `token_hash` —
значит решение принималось уже внутри эксклюзивного окна). Измерено на живом сервере:
`DROP TABLE IF EXISTS` за читателем, держащим ACCESS SHARE, падает по lock timeout
(`55P03`), тогда как `CREATE TABLE IF NOT EXISTS` той же таблицы проходит рядом с этим
читателем без ожидания. То есть именно DROP, а не остальной идемпотентный DDL, превращает
миграцию в пробку перед живым трафиком.

Сделано (`internal/identity/schema_legacy_shape.go`, `internal/identity/store.go`,
`internal/identity/schema.go`):

- из `schemaDDL` убран любой `DROP TABLE`;
- `Migrate` под advisory-локом, **до** первой DDL-операции, берёт один каталожный
  `SELECT` (`information_schema.columns`, блокировки на таблицу не берёт) и шлёт
  `DROP TABLE` только установке, которая действительно несёт снятую форму;
- здоровая установка не получает `DROP TABLE` вообще.

Ноги (дефолтный набор, без тегов; две правки в одном месте не смешаны):

- `internal/identity/schema_ddl_no_drop_table_test.go` —
  `TestReapplyingTheIdentityDDLContainsNoDropTable`: в DDL нет оператора `DROP TABLE`
  (проза в комментариях не считается). Компилируется на базе, поэтому краснеет как
  поведение, а не как сборка: `--- FAIL ... DEF-C2: schemaDDL issues a DROP TABLE`,
  `TEST_RC=1` на `62231bbf` (dirty=false) —
  `private/pdf-closure-20261102/evidence/identity-schema-ddl-window-RED-20261110T065514Z.log`.
- `internal/identity/schema_ddl_exclusive_window_test.go` —
  `TestReapplyingTheIdentityDDLTakesNoExclusiveLockOnBrowserTokenBindings`: штампованная
  установка (кейс раскатки), читатель держит ACCESS SHARE на `browser_token_bindings` всю
  миграцию, у мигранта `lock_timeout=2s`. Снято: `elapsed=22ms`,
  `ungranted_access_exclusive_observations=0`, `storage_before == storage_after`
  (relfilenode не сменился) — `…-GREEN-20261110T065549Z.log`, `TEST_RC=0` на `fe79fba2`.
- там же `TestTheLegacyBrowserTokenBindingsShapeIsStillRetired`: пречек — не способ ничего
  не удалять; установка с `token_hash` переводится в текущую форму
  (`token_jti,session_id,epoch,expires_at,revoked_at`).
- Мутация (в дерево не попадает): «игнорировать каталожный пречек» красит именно
  измерительную ногу — `canceling statement due to lock timeout (SQLSTATE 55P03)`
  через 2.016 с: `…-MUTATION-20261110T065636Z.log`, `TEST_RC=1`.
- Регрессия пакета на финальном tipе: **86 PASS / 0 FAIL / 0 SKIP**, `TEST_RC=0`
  (`private/pdf-closure-20261102/evidence/identity-package-regression-20261110T065554Z.log`);
  `gofmt -l` пусто, `go vet` чисто. Страж идентичностей провайдеров
  (`TestProviderIdentitiesAreAccountedFor`, `TestEveryRoutedModelActuallyAppearsInTheTree`)
  зелёние с новыми файлами в `evidence/` — файлы в дерево модуля добавлены осознанно и
  проверены этим стражем.

## C3 — отдельная полоса-гард (не в этой)
Класс дефекта незащищён: `Migrate` доверяет штампу, поэтому «DDL изменился, версию не
подняли» остаётся системным риском. Отдельная полоса: `identity-schema-ddl-coverage-guard`
— нога «после `Migrate` поверх штампнутой БД присутствуют все объекты, которые перечисляет
`schemaDDL`» (каталог против списка объектов DDL), плюс вариант дайджеста `schemaDDL`
рядом с версией (`DEF-SCHEMA-VERSION-DRIFT`). В этой серии не реализуется, чтобы не
раздувать scope.
