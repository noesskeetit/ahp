# DEF-TEST-ENV-DEFAULT-RED: триаж «красного дефолта из-за среды»

- Отдельный список по требованию прораба, к механизму `DEF-GATE-SKIP-POLICY` отношения не имеет.
- Russell, 2026-11-10T06:14:03Z (время записи файла; прогоны сегментированы ниже по логам 06:11Z-06:13Z) (`date -u`), полоса `gate-skip-policy-russell`, rev `dac316e4`, tree `3c81c69926055b095e03f5c1ce3295e3797edfde`, `dirty=0`.
- Чужие файлов я не касаю: ниже только триаж и ожидаемое поведение. Владелец пакета решает и мигрирует сам (Volta §4).

## Метод (два измерения, а не одно)

1. Статика по исходникам: места, где нога краснеет из-за отсутствия переменной (`t.Fatal`/`Fatalf` рядом с `AHP_TEST_*`/`AHP_LIVE_*`/`AHP_S3_*`/`AHP_MINIO_*`/`must be set`), и места `t.Skip` без вида.
2. Живой поток того же дерева без среды (`go test -json -p 1 -count=1 -timeout 240s`, лог `private/pdf-closure-20261102/evidence/gate-skip-policy-realdata-20261110T061209Z.log`).

Статика одна обманывает в обе стороны: `t.Skip(reason)` в `internal/executor` выглядит «без вида», но вид в неё кладёт `realDepsSkipReason()` (`internal/executor/realdeps_test.go:56-64`), и в потоке таких ног ноль в классе untagged. Поэтому «что есть на самом деле» — второе измерение; статика нужна, чтобы видеть, сколько работы у владельца.

## Измеренная картина (поток без среды)

| пакет | PASS leaf | FAIL leaf | SKIP env | SKIP explicit | SKIP untagged | красный дефолт из-за среды |
|---|---|---|---|---|---|---|
| `internal/executor` | 11 | 0 | 11 | 0 | 0 | нет — единственный пакет, уже перешедший на токены |
| `internal/execbridge` | 37 | 45 | 0 | 0 | 0 | есть: 45 красных ног, вида нет вообще |
| `internal/model` | 52 | 20 | 0 | 0 | 86 | есть: 20 красных + 86 пропусков без вида |
| `tests/execution` | 5 | 25 | 0 | 0 | 1 | есть: 25 красных ног + 1 пропуск без вида |

Статические места (grep, включает и те, что уже токенизированы): `internal/artifactsapi` 6 красных + 2 untagged, `tests/execution` 6 + 3, `internal/execbridge` 2 + 3, `internal/gateway` 1 + 3, `internal/model` 2 + 14, `internal/executor` 2 + 4. Итого 48 мест по шести пакетам, из них в четырёх пакетах из задания — 26 (artifactsapi 8, tests/execution 9, execbridge 5, gateway 4). Твоей цифры 32 мой набор шаблонов не воспроизводит: расхождение в шаблоне, а не в дереве, поэтому триаж ниже дан по местам, а не по счётчику. Если у тебя список из 32 адресов есть, приложи — сверю каждый с классом A/B/C/D/E.

## Правило разбора (моя рекомендация, форма — за Volta)

- **A. Пропустить с `SKIP_ENV[tag=…] reason=… missing=…`**, если утверждение ноги требует живой зависимости и без неё нога физически ничего не утверждает: `internal/artifactsapi/fixtures_test.go:129`, `:133`; `internal/model/budget_test.go:20` (и все 86 его untagged-пропусков); `internal/gateway/gateway_integration_test.go:720`; `internal/execbridge/bridge_db_test.go:44`; `tests/execution/a2a_harness_test.go:227`.
- **B. Оставить красной навсегда**, если утверждение ноги и есть «конфигурация отсутствует/некорректна должна быть отвергнута» — такие ноги обязаны сами выставлять себе окружение и не читать окружение прогона: `internal/executor/artifact_binding_test.go:55` (`bad AHP_S3_ENDPOINT`), `internal/artifactsapi/fixtures_test.go:140` (тот же класс парсинга значения). Перевод таких ног в skip ослабляет приёмку, а не чинит её.
- **C. `SKIP_EXPLICIT` (жёлтый)**, если пропуск — чей-то выбор, а не отсутствие зависимости: opt-out'ы `AHP_ALLOW_SKIP_REAL_DB` / `AHP_ALLOW_SKIP_LIVE_QWEN` (`tests/execution/a2a_harness_test.go:276`, `:290`, `live_bridge_artifact_test.go:78`), live-Qwen в `internal/model/live_qwen_test.go:40`, `:47`. Сегодня они уже печатают `SKIP_EXPLICIT`, но без тега — вид определяется, тег надо добавить в словарь Volta.
- **D. Отдельный класс «нет инструмента»**, а не среды: `internal/artifactsapi/browser_upload_test.go:23` (`requires explicit Playwright module`) — это не про БД и не про опциональный выбор; нужен свой тег (например `browser_toolchain`), иначе он растворится в чужом классе.
- **E. Redis**: `internal/gateway/redis_test.go:16`, `:37` — среда или выбор? Сегодня `t.Skip` без вида; по смыслу это A (зависимость не поднята), но судит владелец пакета.

## Владелец и ожидаемое поведение

| пакет | владелец | что сделать | что при этом обязано покраснеть |
|---|---|---|---|
| `internal/executor` | Gauss | уже на токенах; останется только снять «PASS-родителя при пропущенных детях» (`TestSealReplayDoesNotCreateASecondObject`) и назначить себе designation | мутация «skip безусловный при выданной зависимости» (C1) обязана покраснить ногу, а не только приёмку |
| `internal/model` | Heisenberg | 86 пропусков без вида → `SKIP_ENV[tag=real_store_env…]`/свой тег из словаря; 20 красных разобрать по A/B | `UNTAGGED_SKIP` на этом пакете должен исчезнуть, а `not_closed` остаться ненулевым |
| `internal/execbridge` | Heisenberg | 45 красных ног по A (сейчас красный дефолт без всякого вида); opt-out'ы уже печатают `SKIP_EXPLICIT` — добавить тег | при переводе в skip без вида нога обязана остаться красной по `UNTAGGED_SKIP` |
| `tests/execution` | Goodall | `tests/execution/worker_activation_test.go:92` (уже заведено на него); 25 красных по A; opt-out'ы по C | `UNTAGGED_SKIP` исчезает, `SKIP_EXPLICIT` попадает в жёлтый список |
| `internal/artifactsapi` | владелец пакета не назначен в этом письме | 6 мест по A/B, Playwright по D | та же пара проверок |
| `internal/gateway` | владелец пакета не назначен | Redis по A или C — решает владелец | та же пара проверок |

## Порядок, который я считаю безопасным (совпадает с Volta §2)

Сначала словарь тегов и утилита, потом миграция полос своими коммитами, и только потом включается правило «полная среда + `SKIP_ENV[*]` = красный». Включённое раньше, оно покраснит половину дерева на написании, а не на продукте: на сегодняшнем дереве `--env-complete` даёт красный вердикт в `internal/executor` (11 env-пропусков) и в трёх других пакетах по другим причинам.
