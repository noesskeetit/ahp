# DEF-GATE-SKIP-POLICY: закрытие условий приёмки COND-1..COND-6

- Время снятия: 2026-11-10T06:26Z (`date -u`)
- Полоса: `russell/gate-skip-conditions-20261110`, worktree `private/pdf-closure-20261102/gate-skip-conditions-russell`
- База: `604178825532cab340521441daf2253aa3751ef2` (HEAD интеграции на момент постановки, tree `3c81c699…`, dirty=0)
- Коммиты полосы: `c438ee5c` (доки поверх влитого кода) → `c4a2beb7` (RED) → `cdd92649` (фикс шести условий) → `2dca23ee` (честные ссылки на форму Volta) → docs-коммит с этим отчётом
- Файлы объёма: `scripts/skip-verdict.mjs`, `scripts/skip-verdict.test.mjs`
- Вердикт по объёму: ACCEPT-with-conditions (открытые условия в конце; ни одно не блокирует мерж механизма)

## Карта «условие → нога → мутация»

| COND | что делало приёмку бумажной | ноги (47-ножная схема) | мутация и её счётчик | RED | GREEN |
| --- | --- | --- | --- | --- | --- |
| COND-1 | вход `test:go:verdict` не объявляет env → всегда ineligible → всегда rc=0; читатель rc видел «зелёный гейт» там, где ничего не мерялось | `COND-1: the exit code accounts for acceptance eligibility…`, `a partial run is reconnaissance…` (дополнена), `COND-1 CLI: an ineligible green run exits 3…`, `wiring: …` (запрет `--allow-partial` в приёмочном входе) | M5 `return 0` в `exitCodeFor` → fail=4: COND-1, COND-1 CLI, partial-run, COND-5 CLI | 16 из 47 | 47/47 |
| COND-2 | экстрактор копировал `reason=(\S+)` и `missing=(\S+)` дословно — канал утечки значений в отчёт приёмки | `COND-2: a SKIP_ENV reason that is not a label…`, `COND-2: a missing= item that is not a variable name…`, `COND-2: the tokens the tree actually prints stay well-formed…` | M2 `tokenDefect()` → `return null` → fail=2 (обе legs на значение) | ↑ | ↑ |
| COND-3 | `--allow-unexplained` снимал и `STUBBED_SKIP_PASSED_AS_MEASURED`: whitelist обезвреживал ложь | `COND-3: a declared exception never disarms the stubbed-skip detector, and the row stays in the report` | M3 вернуть `exceptionFor` в фильтр стабов → fail=1 | ↑ | ↑ |
| COND-4 | код команды не влиял на вердикт и не печатался (`-- sh -c '…; exit 1'` → PASS, rc=0) | `COND-4: the wrapped command exit code travels as a field…`, `COND-4 CLI: a wrapped command that exits nonzero cannot produce a green verdict` | M4 `if (false)` на `COMMAND_EXIT_NOT_ZERO` → fail=2 | ↑ | ↑ |
| COND-5 | механизм держал собственную копию словаря тегов, то есть вторую политику в дереве | `COND-5: with no dictionary the real tag is refused…` (страж прежнего поведения), `COND-5: the mechanism source carries no dictionary literal of its own`, `COND-5 CLI: without --tags a real token is refused…` | M6 вернуть словарь в `TAG_CLASS` → fail=2 (grep-нога + CLI) | ↑ | ↑ |
| COND-6 | пакет, где все ноги-листы — env-скипы, давал `verdict: PASS` при `pass_leaf=0` | `COND-6: a package whose every leaf skipped is NOTHING_MEASURED…`, `COND-6 CLI: a package of only env skips is red at any environment level` | M1 `filter(() => false)` на детекторе → fail=2 | ↑ | ↑ |

Первичный лог мутаций: `private/pdf-closure-20261102/evidence/gate-skip-conditions-MUTATIONS-20261110T062354Z.log` (sha256 `e92d18a2…`); после каждой мутации `reverted_dirty=0`, итоговый базовый прогон 47/47.

## COND-1 — exit-код обязан учитывать пригодность к приёмке

- Решение: `exitCodeFor()` даёт `0` только зелёному И приёмочному прогону, `1` — FAIL, `2` — ошибка использования/потока, `3` — NOT_ACCEPTANCE. Приоритет FAIL > NOT_ACCEPTANCE > PASS; `--allow-partial` никогда не отбеливает FAIL и единственный покупает `0`, печатая `mode: reconnaissance`.
- Граница формы: поле `verdict` остаётся словарём Volta (`PASS`/`FAIL`). Пригодность — отдельные поля `acceptance_eligible`, `mode`, `exit_code`. Новых слов в вердикт не вводилось.
- Вход `package.json:19` оставлен без `--env-complete` и без `--allow-partial` и закрыт ногой: приёмочный вход не вправе обезвреживать собственную проверку; режим выбирает оператор в момент вызова.
- Живое доказательство: зелёный `./pkg/contracts/` — `go test` rc=0, 14 ног, ноль нарушений, а `skip-verdict` даёт `verdict: PASS`, `mode: not_acceptance`, `exit_code: 3`; с `--allow-partial` — `mode: reconnaissance`, `exit_code: 0`.

## COND-2 — значения не копируются в отчёт

- Грамматика: `reason` — метка `^[a-z0-9][a-z0-9_.-]{0,63}$`, каждый элемент `missing` — имя переменной `^[A-Z][A-Z0-9_]{0,99}$`. Несоответствие → вид `malformed`, нарушение `MALFORMED_SKIP_TOKEN` с полем `defect` (`reason_not_a_label` / `missing_not_a_variable_name`), поля `reason`/`missing` в запись не попадают вообще (значение не копируется, а не «редактируется потом»).
- Проверено на живых токенах дерева (`internal/executor/realdeps_test.go:58`, `internal/execbridge/tool_wire_test.go:102`, `internal/model/artifact_plane_gate_test.go:37-60`): `byKind.malformed=0`, то есть честная полоса не краснеет. Отдельно проверяется токен с `endpoint=172.18.0.6:9000` — значение хоста в отчёт не уходит.
- Нога с подставным DSN (`hunter2`) требует, чтобы ни `renderText`, ни `SKIP_POLICY`-строки не содержали это значение.

## COND-3 — whitelist не обезвреживает детектор лжи

- `--allow-unexplained` работает только для непомеченных скипов («объявлено не покрытым») и больше не касается `STUBBED_SKIP_PASSED_AS_MEASURED`. Запись стаба остаётся в отчёте (`kind: stubbed-skip`) даже при наличии обоснования: «обезвреженный пропуск» и «объявленный не покрытым» — разные записи.

## COND-4 — код команды доезжает до вердикта

- Поле `command_exit: N` в текстовом отчёте и `COMMAND_EXIT_NOT_ZERO` при ненулевом. Живая реплика прораба (`-- sh -c 'cat stream; exit 1'`) даёт `verdict: FAIL`, `command_exit: 1`, rc=1; `-- sh -c 'exit 2'` без потока даёт `LOG_UNVERIFIABLE` + `COMMAND_EXIT_NOT_ZERO`.

## COND-5 — словарь живёт у владельца формы

- Встроенный словарь удалён (`export const TAG_CLASS = {}`), классы приходят только через `--tags`; незарегистрированный тег отказывается (`UNKNOWN_SKIP_TAG`), отчёт печатает `dictionary: absent` или путь и число записей. Отдельная нога запрещает появление литералов тегов в исходнике механизма.
- Следствие, которое обязан видеть прораб: на реальном дереве ни один тег не зарегистрирован, поэтому штатный вызов без `--tags` обязан быть КРАСНЫМ (живой замер: 11 env-скипов → `unknown:real_deps` 10 + `unknown:artifact_parser` 1 → rc=1). Это отказ, а не ложный зелёный; но приёмка не заработает, пока Volta не зарегистрирует словарь. Требуемые к регистрации теги: `real_deps`, `artifact_plane`, `artifact_parser` плюс новые `dot_e2e_layout`, `dot_e2e_python`, `dot_e2e_optin`, `source_revision` (полоса Erdos `4856b762`).

## COND-6 — «измерено ничего» не бывает приёмкой

- `NOTHING_MEASURED` с полем `packages: [{package, leaves, skipped, measured}]` срабатывает, если в пакете есть ноги-листы, но `measured=0`. Красный на обоих уровнях окружения: корректно названные скипы — это не покрытие.
- Живой случай: `go test -json -run 'TestExecutorRoundTripTenantIsolationAndDurability' ./internal/executor/` → сам `go` говорит `ok` (rc=0), механизм даёт `pass_leaf=0`, `NOTHING_MEASURED`, rc=1.

## Живое сравнение старого и нового пути (без env, без БД, без Docker, без нагрузки)

| путь | что видит читатель |
| --- | --- |
| `go test -p 1 -count=1 ./internal/executor/` | одна строка `ok github.com/ahp-platform/ahp/internal/executor 0.398s`, rc=0; ни PASS, ни SKIP в выводе нет вообще |
| тот же прогон как поток `-json` | `pass_leaf=11`, `skip_leaf=11`, `pass_all=12`, `not_closed=11`, `PARENT_PASS_WITH_ALL_CHILDREN_SKIPPED` на `TestSealReplayDoesNotCreateASecondObject` |
| новый путь со словарём | `verdict: FAIL`, `acceptance_eligible: false`, `mode: fail`, rc=1 |
| новый путь без словаря | то же плюс `UNKNOWN_SKIP_TAG` ×2, `dictionary: absent` |

Лог: `…/evidence/gate-skip-conditions-LIVE-20261110T062433Z.log` (sha256 `660b2c46…`).

## Карта условий C1/C2/C3 из вердикта по `f590505b`

| условие | статус | чем доказано |
| --- | --- | --- |
| C2 — родитель печатает PASS при всех скипнутых подтестах | закрыто механизмом | ноги `findVacuousParents…`, `invariant 2…`; мутация детектора (прежний лог `gate-skip-policy-MUTATIONS-20261110T061247Z.log`, M2, fail=2); живое имя `TestSealReplayDoesNotCreateASecondObject` |
| C3 — приёмный раннер игнорирует SKIP, зелёный пакет с десятью скипами неотличим | закрыто механизмом | счётчики по всем уровням + таблица старого и нового пути выше; мутация вида «скип не считается» красит ноги (тот же лог, M1/M3) |
| C1 — безусловный skip при выданном `AHP_ARTIFACT_PARSER` даёт rc=0 и не красит ничего | ЧАСТИЧНО, остаётся открытым | механизм ловит наблюдаемые формы: непомеченный скип (`UNTAGGED_SKIP`), скип при объявленном полном окружении (`SKIP_ENV_AT_FULL_ENV`), «напечатал токен и PASS» (`STUBBED_SKIP_PASSED_AS_MEASURED`). Продуктовый пин «выданный бинарь парсера действительно тот, которым исполняют» — не объём механизма, владелец Gauss |

## Граница для потребителя `tests/dote2e`

- Go-API нет и не планируется. Контракт потребителя: (а) литералы в сообщении скипа `SKIP_ENV[tag=…] reason=… missing=…` / `SKIP_EXPLICIT…`; (б) поток `go test -json`; (в) строки `SKIP_POLICY {json}` и поля `verdict:`/`env:`/`acceptance_eligible:`/`mode:`/`exit_code:`/`counters:`; (г) коды выхода 0/1/2/3.
- Если полосе нужен собственный вердикт, она вызывает CLI (`--stdin`, `--report <path>`) либо импортирует ESM-модуль: публичная поверхность уже есть — `parseGoTestJSON`, `classifySkips`, `findVacuousParents`, `countLevels`, `evaluatePolicy`, `renderRows`, `renderText`, `readAllowFile`, `readTagFile`, `exitCodeFor`, `acceptanceMode`. Новых экспортов не потребовалось.
- Полоса Erdos уже перешла на эти литералы (`4856b762`, `tests/dote2e/skip_vocabulary_test.go`), и она же policing-ит свои формы. Её теги ждут регистрации у Volta (см. COND-5).

## Точка authority (отдельный файл)

`reports/mac-director/acceptance-counter-authority-20261110.md` — один ответ на вопрос «какой из четырёх счётчиков приёмочный».

## Открытые условия

- O-1 (Volta, блокирует включение приёмной силы): зарегистрировать словарь тегов; до этого штатный вызов обязан быть красным.
- O-2 (Volta): ратифицировать имена новых правил — `NOTHING_MEASURED`, `MALFORMED_SKIP_TOKEN`, `COMMAND_EXIT_NOT_ZERO`, `STUBBED_SKIP_PASSED_AS_MEASURED`, а также коды выхода 0/1/2/3. До ратификации это рабочие имена.
- O-3 (Volta/прораб): заметка по форме `reports/mac-director/skip-acceptance-policy-volta-20261110.md` отсутствует в дереве; ссылки в коде и отчётах теперь помечены как указатель на ожидаемый артефакт, а не как доказательство.
- O-4 (Gauss): продуктовая половина C1 — пин выданного бинаря парсера.
- O-5 (прораб): переключение роя на новый раннер — решение и объявление прораба. `private/pdf-closure-20261102/run-tests.py` (untracked, читает только rc) не трогался; старый путь остаётся путём до объявления.
- O-6: триаж `DEF-TEST-ENV-DEFAULT-RED` (32 места «красный дефолт из-за среды») — `reports/mac-director/def-test-env-default-red-triage-20261110.md`; владельцы пакетов получили директивы прораба, механизм чужие пакеты не перекрашивает.

## Счётчики

- RED: tip `c4a2beb7`, tree `7ceee97ead…`, dirty=0, `node --check` 0/0, ноги 47: pass=31 fail=16, rc=1 (`…RED-20261110T062243Z.log`, sha256 `8885af83…`). Из 16 красных: 12 новых ног COND и 4 прежние ноги, чьи ожидания честно изменились (три `byKind`-проверки получили вид `malformed`, `a partial run is reconnaissance` получила exit-код). Тринадцатая новая нога, `COND-5: with no dictionary the real tag is refused`, красной не была и не могла быть: это страж уже существующего отказа от незарегистрированного тега, он закрывает регрессию, а не новое утверждение.
- GREEN: tip `cdd92649`, tree `0256b321…`, dirty=0, `npm run --silent verify:skip-verdict` → 47 pass / 0 fail / 0 skipped, rc=0 (`…GREEN-20261110T062337Z.log`, sha256 `5fda7e6f…`).
- FINAL VERIFY после коммента с честными ссылками: tip `2dca23ee`, tree `c06942ee…`, dirty=0, 47/47/0, rc=0 (`…VERIFY-20261110T062549Z.log`, sha256 `b66d0cf3…`).
- Мутации: 6, каждая красит ровно свои ноги, счётчики в таблице выше и в логе.

## Команды

- `node --test --test-force-exit scripts/skip-verdict.test.mjs` — полный набор ног механизма (34 → 47).
- `npm run --silent verify:skip-verdict` — трекнутый вход (`package.json:33`); без него `run-tests.mjs:22-27` эти ноги не видит, и приёмка молча ничего не меряет.
- `node scripts/skip-verdict.mjs --stdin [--tags <file>] [--env-complete --designation <x>] [--allow-partial] [--report <path>]`
- Узкие прогоны живых потоков: `go test -json -p 1 -count=1 -timeout 240s ./internal/executor/`, `… -run 'TestExecutorRoundTripTenantIsolationAndDurability' ./internal/executor/`, `go test -json -p 1 -count=1 -timeout 240s ./pkg/contracts/`

## Дисклеймен

Исполнял: `node --check`, `node --test` по ногам механизма, `npm run verify:skip-verdict`, шесть мутаций с откатом, узкие `go test -p 1 -count=1 -timeout 240s` по `./internal/executor/` (дважды) и `./pkg/contracts/` на своём GOCACHE, `GOPROXY=off`, без env, без БД, без Docker, без нагрузки, без `-race`, без широких гейтов. Не исполнял: широкие гейты, docker-сборки, нагрузку, `docker context use` не трогал. Секреты в вывод не выводились (`ps eww`/`printenv` не использовались); сырые потоки лежат отдельными файлами в `evidence/`, в отчёты попали только поля и счётчики. Время только из `date -u`.
