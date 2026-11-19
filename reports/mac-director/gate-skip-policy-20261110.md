# DEF-GATE-SKIP-POLICY: приёмный механизм, который не считает неизмеренное пройденным

- вердикт: PASS (механизм доказан: RED по ассертам, GREEN, 10 мутаций, живые потоки). Условия и открытые ноги перечислены явно; форма политики не моя.
- Роль: Russell — исполнитель МЕХАНИЗМА. Владелец ФОРМЫ skip-политики — Volta (`reports/mac-director/skip-acceptance-policy-volta-20261110.md`, 06:04Z). Граница зафиксирована в разделе «Форма и механизм».
- Полоса: `private/pdf-closure-20261102/gate-skip-policy-russell`, ветка `russell/gate-skip-policy-20261110`.
- Tip кода и механизмов `dac316e4` (tree `3c81c69926055b095e03f5c1ce3295e3797edfde`, `dirty=0`) — на нём сняты все прогоны. Отчёт и триаж идут коммитом(ами) поверх него; итоговый SHA полосы и дерева — в квитанции прорабу, SHA кода при этом не меняется. База — фактический HEAD интеграции `47abb9c68aa9140e8f5b3b5a84ac8297bd790c52` (переставлено с `5c2aaa1b` по твоему пункту 4; `git merge-base --is-ancestor 47abb9c6 HEAD` = истина).
- Первичные логи GREEN/мутации/живые потоки сняты на этом tip; RED снят на коммите-доказательстве `81d63ff0` (ветка `russell/gsp-red-proof`, та же база 47abb9c6, `dirty=0`) — иначе RED невозможно снять на закоммиченном дереве, потому что заглушка и есть «незакоммиченное состояние до реализации». Все логи несут шапку `rev=/tree=/dirty=`, время только `date -u`. Версия 1 (06:05Z, tip `dd9e47a2`) прочитана тобой; она в git (`d037d1c2`, `4d0b513a`) и не переписывается — этот файл её заменяет, изменения перечислены ниже.
- Мета этого файла: 2026-11-10T06:14:21Z (`date -u`).

## Что изменилось с версии 1

1. Форма переведена на Volta'овскую: три вида пропуска вместо двух, его имена приёмочных кодов, объявление полноты среды до прогона, designation, жёлтый статус оп-аута, инжектируемый словарь тегов. Коммит `84bb81be`.
2. Живой прогон показал **четыре ложных срабатывания моего же правила** `STUBBED_SKIP_PASSED_AS_MEASURED` в `internal/execbridge` и `tests/execution`: красная-by-default нога печатает `SKIP_EXPLICIT`, объясняя оп-аут (`tests/execution/a2a_harness_test.go:228`, `internal/execbridge/bridge_db_test.go:47`). Правило сужено до опасной формы (токен + терминальный PASS), добавлена нога против ложного позитива. Коммит `cd32a572`.
3. Проводка добавлена в трекнутый `package.json` и закреплена ногой: без неё тест механизма не исполняется ни одним гейтом (`scripts/run-tests.mjs:22-27` обходит только `tests/<category>/*.mjs`).
4. Полоса переставлена на актуальный HEAD интеграции; всё переснято заново.

## Форма и механизм: где граница

Заимствовано у Volta и не оспаривается реализацией: виды пропуска (`SKIP_ENV` / `SKIP_EXPLICIT` / untagged), красное и жёлтое, имена кодов, требование объявить полноту до прогона, `MISSING_DESIGNATION_AT_FULL_ENV`, словарь тегов.

Остаётся за механизмом (моё): чтение потока `go test -json`, счётчики по всем уровням и честный по-листовой счёт, определение родителя без измерений, гигиена секретов, мутационная самодоказательность, коды возврата.

Расхождения, которые я передаю Volta (не решаю сам):

- **-v больше не обязателен.** Его правило `LOG_UNVERIFIABLE` написано для текстовых `-v` логов, где boundary-линии — единственный способ привязать строку к ноге. В `-json` каждая нога — событие, а родитель и дети выводятся из имён; потому `LOG_UNVERIFIABLE` здесь означает «поток не `-json` или в нём нет событий ног», и покрывает `go test` без `-v`. Прошу подтвердить замену, иначе в дереве два разных определения проверяемости.
- **`SKIP_EXPLICIT` без тега.** В дереве он существует без `[tag=…]` (`tests/execution/a2a_harness_test.go:225`, `internal/execbridge/bridge_db_test.go:42`). Его текущий KIND-регекс требует тег и относит эти 6+ ног к худшему классу (он сам это отметил в §0). Здесь тег у явного вида опционален.
- **`STUBBED_SKIP_PASSED_AS_MEASURED`** — код, которого у него нет. Прошу внести в список или снять; по умолчанию он красный.
- **Словарь тегов.** Реестр в `scripts/skip-verdict.mjs` помечен как зеркало Volta и полностью перезаписывается флагом `--tags <file.json>` (формат: `[{"tag":…,"class":…}]`). Когда он выложит `tools/` со словарём, проводка становится `--tags tools/skip-tags.json`, а зеркало удаляется — иначе в дереве четвёртая копия правила, против чего он и возражал.
- **`real_deps` против `artifact_plane`** не унифицировано: оба написания приняты в один класс `real_store_env`, переименование — только через тебя, как ты и сказал. Коммит-образец `18eebc17` предком дерева не считается (проверено: `git merge-base --is-ancestor 18eebc17 HEAD` = ложь).

## Карта условий: условие → нога → какая мутация красит

| условие | статус | чем закрыто (нога) | мутация, которая её красит |
|---|---|---|---|
| C3 (ф590505b): приёмный раннер игнорирует SKIP, зелёный пакет с десятью скипами неотличим от меренного | закрыто механизмом, сила — с момента переключения гейта на `test:go:verdict` | `countLevels: honest counters do not inflate with containers`, `rows are machine-readable…`, `text report states verdict…` | M5 (наивный счёт вместо листового), M10 (пустой поток зелёный) |
| C2 (ф590505b): родитель печатает PASS при всех скипнутых подтестах | закрыто на приёмке; продуктовый пин остаётся у Gauss'а | `findVacuousParents: PASS parent whose every child skipped`, `invariant 2: a parent may not PASS while every subtest skipped…` | M2; на живом потоке M2 переворачивает `internal/executor` в `verdict: PASS violations: 0` |
| C1 (ф590505b): безусловный skip при выданном `AHP_ARTIFACT_PARSER` даёт rc=0 и не красит ничего | **открыто** — см. отдельный раздел | половина приёмки есть (`invariant 1…`), продуктовый пин не написан | M3 красит ногу `invariant 1`; пин Gauss'а ещё не существует, поэтому полной мутации нет |
| новое: тихий skip без вида | закрыто механизмом | `an untagged skip is red in either environment…`, `classify: a bare t.Skip is untagged…` | M1, M6, M7 |
| новое: жёлтый оп-аут не должен красить чужую полосу и не должен становиться зелёным | закрыто | `invariant: explicit opt-out is yellow, never green…`, `classify: SKIP_EXPLICIT without a tag…` | M6, M7 |
| новое: объявление полноты обязано быть машинным | закрыто | `AHP_GATE_ENV=full declares completeness the same way the flag does` | M3 |
| новое: полное окружение без designation | закрыто | `a full-env run must enumerate its designation` | M8 |
| новое: вывод ноги не должен уносить креды | закрыто | `invariant: no credential value leaves the report…` | M9 |
| новое: непроверяемый поток не зелёный | закрыто | `a run whose legs cannot be attributed is unverifiable, not green` | M10 (на живом пустом потоке: мутация PASS, оригинал FAIL) |
| новое: механизм обязан исполняться гейтом | закрыто | `wiring: the mechanism is reachable from a tracked entry point…` | W1 (удалить ключ npm → pass 33 fail 1) |
| C5 (ф590505b): `tests/execution` краснеет по умолчанию | открыто, не моё | — | триаж `def-test-env-default-red-triage-20261110.md` |

## Инварианты приёмки и ноги, которые их держат

- Полное окружение + ненулевой `SKIP_ENV[*]` = падение приёмки: `violation.rule = SKIP_ENV_AT_FULL_ENV` с тегами, классами и списком отсутствующих переменных; нога `invariant 1: declared full environment plus any SKIP_ENV fails acceptance and names the tags`. На живом потоке `internal/executor` с объявлением полноты: `verdict: FAIL`, теги `real_deps`, `artifact_parser`.
- Родитель не вправе PASS при всех скипнутых подтестах: `PARENT_PASS_WITH_ALL_CHILDREN_SKIPPED`; на живом дереве найдено **два** места — `internal/executor` `TestSealReplayDoesNotCreateASecondObject` и, нового для нас, `internal/model` `TestCompactCompletionReceiptRecoveryAndDigestMismatch`.
- Машиночитаемый счётчик PASS/FAIL/SKIP по всем уровням: `counters: pass_leaf/fail_leaf/skip_leaf/pass_container/pass_all/fail_all/skip_all` плюс `SKIP_POLICY {package,kind,class,tag,reason,missing,count}` на каждый пропуск.
- Нулевые значения кредитов в выводе: вывод ног не печатается никогда; нога-канарейка проверяет, что `CANARY-DO-NOT-LEAK-4f19` не попадает ни в `renderText`, ни в `renderRows`, при том что имена ног попадают.
- Дополнительно: `acceptance_eligible` и `env: full|partial` полями, чтобы «разведка» не могла называться приёмкой.

## Мутационная самопроверка (иначе механизм бумажный)

Метод: в СВОЕЙ копии на закоммиченном tip глушится один детектор, прогоняется `npm run verify:skip-verdict`, откат `git checkout`, `dirty=0` после каждой строки. Логи: `private/pdf-closure-20261102/evidence/gate-skip-policy-MUTATIONS-20261110T061247Z.log` и дополнение `…-supplement-20261110T061312Z.log`.

| мутация | что заглушено | ноги зачли | покрасневшие ноги |
|---|---|---|---|
| M1 | детектор untagged | 32/2 | «untagged skip is red…», «exception without justification…» |
| M2 | `findVacuousParents` | 32/2 | «findVacuousParents…», «invariant 2…»; на живом потоке → `verdict: PASS` (был FAIL с 1 нарушением) |
| M3 | правило полной среды | 32/2 | «invariant 1…», «AHP_GATE_ENV=full…» |
| M4 | детектор заглушённого skip | 33/1 | «a stubbed skip that printed the token and then reached PASS is red» |
| M5 | листовой счёт → наивный | 33/1 | «countLevels: honest counters do not inflate…» |
| M6 | явный вид не узнаётся | 31/3 | обе ноги классификации явного вида + нога жёлтого статуса |
| M7 | явный вид → env (жёлтое стало красным) | 31/3 | те же три ноги |
| M8 | designation при полной среде | 33/1 | «a full-env run must enumerate its designation» |
| M9 | вывод ног попал в поля нарушения | 33/1 | «invariant: no credential value leaves the report…» |
| M10 | страж непроверяемого потока | 33/1 | «a run whose legs cannot be attributed…»; пустой поток: мутация → PASS, оригинал → FAIL |
| W1 | удалён ключ `verify:skip-verdict` | 33/1 (замер ноги, не npm) | «wiring: the mechanism is reachable…» |
| возврат | — | 34/0 | после откатов всё зелено, `dirty=0` |

Строка W1 изначально была записана слабо (падал сам npm со «missing script», нога не успевала исполниться); в лог дополнения дописана корректная строка с прямым прогоном `node --test`.

## Живые потоки: старый путь против нового (без среды, своя копия)

Первичный лог: `private/pdf-closure-20261102/evidence/gate-skip-policy-realdata-20261110T061209Z.log` (сырые потоки рядом). Старый путь не исполнялся и не правился: `run-tests.py` — чужой UNTRACKED scratch, его `--lane` не содержит этой полосы; по исходникам он фиксирует только `exit_code` (`run-tests.py:47`, `:50`), `scripts/run-tests.mjs:44-66` засчитывает `rc==0` как passed. Поэтому «что показывает старый путь» = rc того же `go test`.

| пакет | rc старого пути | PASS leaf (меренно) | PASS_all как печатает go | FAIL | SKIP env / explicit / untagged | вердикт нового |
|---|---|---|---|---|---|---|
| `internal/executor` | 0 («ok», всё зелено) | 11 | 12 | 0 | 11 / 0 / 0 | FAIL: `PARENT_PASS_WITH_ALL_CHILDREN_SKIPPED` |
| `internal/execbridge` | 1 | 37 | 38 | 45 | 0 / 0 / 0 | FAIL: `TEST_FAILURES` (45) |
| `internal/model` | 1 | 52 | 58 | 20 | 0 / 0 / 86 | FAIL: `TEST_FAILURES`, `UNTAGGED_SKIP` (86), `PARENT_PASS_WITH_ALL_CHILDREN_SKIPPED` |
| `tests/execution` | 1 | 5 | 5 | 25 | 0 / 0 / 1 | FAIL: `TEST_FAILURES`, `UNTAGGED_SKIP` |

Единственный пакет, где старый путь врёт в «зелёную» сторону, — `internal/executor`: rc=0 при одиннадцати пропусках и родителе-обманке. Остальные три красные и так; там новый вердикт приносит не цвет, а классификацию: 86 пропусков без вида в `internal/model` и designation/полнота — это то, что раньше невозможно было увидеть вовсе.

## C1 из вердикта по f590505b: ОТКРЫТО

Пишу прямо, как ты просил. Закрыта только приёмная половина: при объявленном `--env-complete` пропуск с тегом `artifact_parser` красит приёмку, и это измерено на живом потоке (теги в `SKIP_ENV_AT_FULL_ENV`). Не закрыта продуктовая половина: ноги, которая утверждала бы «при выданном `AHP_ARTIFACT_PARSER` нога обязана исполниться», нет. Без неё мутация «skip стал безусловным» не красит ничего в `internal/executor` — ровно то, что я зафиксировал в C1. Владелец ноги — Gauss; приёмный механизм этот пробел не закроет, потому что честность объявления полноты он проверить не может.

## Проводка и точные команды

- Точная команда теста механизма: `npm run verify:skip-verdict` = `node --test --test-force-exit scripts/skip-verdict.test.mjs` (прецедент `package.json:31` `verify:human-origin`). Прямой вызов: `node --test scripts/skip-verdict.test.mjs`.
- Точная команда гейта: `npm run test:go:verdict` = `node scripts/skip-verdict.mjs -- go test -json -p=1 -count=1 ./...`. Узкая форма: `node scripts/skip-verdict.mjs --designation pg:<моя_БД> --env-complete -- go test -json -p 1 -count=1 ./internal/executor/`.
- Локально исполнено: `npm run verify:skip-verdict` → `tests 34 / pass 34 / fail 0`, `npm_rc=0` (`…GREEN-20261110T061159Z.log`); `node --check` = 0 на обоих файлах.
- Широкий `test:go:verdict` по `./...` я не запускал: это широкий гейт, окно измерений у root.

## Потребляет ли механизм словарь и готов ли принять tests/dote2e

- Словарь: потребляет. Реестр живёт в `TAG_CLASS` (`scripts/skip-verdict.mjs`) и полностью перезаписывается `--tags <file.json>`; нога `the dictionary is injectable, so Volta owns one copy of it, not four` проверяет и приём файла, и отказ на неполной записи. Сам файл словаря в дереве не выложен намеренно: пустой «разрешитель» — это мишень для молчаливых правок, а настоящий словарь должен прийти от Volta.
- Готовность принять `tests/dote2e`: да, как потребитель, а не как объявитель. Сегодня его полосы объявляют своё: `tests/dote2e/interlocutor_and_recovery_test.go:86` и `tests/dote2e/verdict_semantics_test.go:105` — `t.Skip` без вида, а `tests/dote2e/dot_config_revision_test.go:70` держит приватный статус `SKIPPED_RERUN` внутри фикстуры. Ни одного упоминания `SKIP_ENV`/`VACUOUS_PARENT` в `tests/dote2e` на HEAD `47abb9c6` нет (проверено grep'ом).
- Форма границы, которую предлагаю зафиксировать (_go-полосе не нужен мой JS-модуль, ей нужен контракт_):
  1. literals стабильны: `SKIP_ENV[tag=…] reason=… missing=…`, `SKIP_EXPLICIT`, имена кодов `UNTAGGED_SKIP`, `SKIP_ENV_AT_FULL_ENV`, `PARENT_PASS_WITH_ALL_CHILDREN_SKIPPED`, `LOG_UNVERIFIABLE`, `MISSING_DESIGNATION_AT_FULL_ENV`, `STUBBED_SKIP_PASSED_AS_MEASURED`;
  2. строки `SKIP_POLICY {json}` со стандартными полями — это машиночитаемый контракт для агрегации;
  3. коды возврата CLI: 0 PASS, 1 FAIL, 2 ошибка запуска/потока;
  4. Go-стороне достаточно напечатать литерал в `t.Skip` и передать поток через `--stdin`. Если Erdos хочет утверждать это тестом внутри Go, минимальная форма — `scripts`-вызов через `os/exec` с проверкой `rule` и полей строки `SKIP_POLICY`; отдельного Go-API я не ввожу, чтобы не плодить вторую реализацию классификатора.
- Прошу не считать готовым: `SKIPPED_RERUN` — это поле отчёта Erdos, а не вид пропуска; если он хочет, чтобы приёмка его видела, он обязан печатать `SKIP_EXPLICIT[tag=…]` alongside, иначе его «не меряли» останется приватным словом в фикстуре.

## Что остаётся недоказанным этим отчётом

- Широкий гейт на этом SHA (окно измерений у root) и `-race`: не запускалось.
- Живое полное окружение: Flip «полная среда → красные env-пропуски» измерен на объявлении (`--env-complete`) и на потоке без среды; прогон с реальной PG/MinIO на этом tip я не делал, потому что для него нужна designation-БД и это уже нагрузка.
- Поток-парсер не видит ногу, которая ничего не измеряет и ничего не печатает; против неё нужен отдельный пин покрытиём, и он не принадлежит этому механизму.
- Заглушённый skip, после которого тело краснеет, виден только как красная нога, а не как обман: это осознанное сужение после четырёх ложных срабатываний, зафиксировано в `cd32a572` и в комментарии к правилу.
- Порядок мержей за тобой; красную ногу Erdos (`TestCoverageClaimsAreAdvisoryAndCannotMoveTheVerdict`) я не трогаю и зелёным широкий гейт на своём SHA не объявляю.

## Дисциплина, которую я соблюдал

`private/pdf-closure-20261102/run-tests.py` (UNTRACKED, 0600) не читался на запись, не запускался на этом дереве и не изменялся; `tools/secret-guard.py` не тронут; продуктовых байт в полосе нет (`git diff --name-status 47abb9c6..HEAD`: `scripts/skip-verdict.mjs`, `scripts/skip-verdict.test.mjs`, `package.json`, два файла отчётов); чужие полосы не перекрашивались; `docker context` не трогал, docker-сборок и нагрузки не было; секреты в вывод не печатались, значения переменных окружения не выводились (`ps eww`/`printenv` не исполнялись); время везде из `date -u`.

## Поправка (2026-11-10T06:26Z)

Строка 4 выше утверждает, что форма Volta лежит в `reports/mac-director/skip-acceptance-policy-volta-20261110.md`. На момент снятия этого отчёта файла в дереве нет (проверено `git ls-tree` по HEAD интеграции `4856b762`): Volta владеет ФОРМОЙ по назначению прораба, его заметка — ожидаемый артефакт. Ссылки в `scripts/skip-verdict.mjs` и `scripts/skip-verdict.test.mjs` переформулированы в коммите `2dca23ee` так, чтобы указатель не выдавался за доказательство. Текст выше оставлен как есть: первичные записи не переписываются.

Закрытие условий прораба (COND-1..COND-6) и обновлённые счётчики: `reports/mac-director/gate-skip-conditions-20261110.md`.
