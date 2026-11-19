# Контрольный прогон журнала State API

Контрольная ревизия State API — `9711511d56ae2cd490e721474a53c90fa5f0404b`. Python SDK использует адресный `ClaimRun`. Для обеих реализаций измеряем один и тот же граф LangGraph 0.6.11 через mTLS и JWT.

## Запуск

Поднимите `cmd/state-stand` на отдельной тестовой PostgreSQL с `fsync=on` и `synchronous_commit=on`. Укажите `AHP_STATE_HANDSHAKE` с тестовыми сертификатами и токенами, `PYTHONPATH` на `sdk/python` и установите `sdk/python/requirements.txt` в отдельное окружение. Скрипт пишет в каталоги, которые ему переданы.

```sh
python profiling/state-journal/langgraph_probe.py faults /path/to/empty/faults
python profiling/state-journal/langgraph_probe.py bench /path/to/empty/sample 10 --seed-threads 10000 --prior-turns 25
```

Режим `faults` принимает пустой каталог. Режим `bench` принимает заранее созданный рабочий каталог и одно из двух значений параллельности, 1 или 10. Для чтения счётчиков PostgreSQL нужны `BENCH_PSQL`, `BENCH_PGSOCKET`, `BENCH_PGPORT` и `BENCH_PGDATABASE`. Счётчики читаются через служебную БД `postgres`.

`--seed-threads` создаёт указанное число дополнительных тредов по State API. `--prior-turns` исполняет предыдущие ходы на каждом треде нагрузки. Прогрев состоит из пяти ходов и исключён из измерений. Сценарий выполняет 100 измеряемых ходов. JSON на stdout включает все отсчёты RPC и полных ходов, фактическое число одновременных аренд, статусы и счётчики БД.

Для сравнения используйте отдельную чистую БД для каждой реализации и одинаковую последовательность режимов. Счётчик PostgreSQL приблизительный, поскольку статистика обновляется с задержкой. Для парной серии сохраняйте DDL, версии, режим записи стриминга и параметры сетевого транспорта вместе с результатами.

## Компактный стенд

`AHP_STATE_STAND_COMPACT_JOURNAL=1` включает компактный HTTP-журнал в `cmd/state-stand`. Используйте чистую PostgreSQL и тестовый mTLS handout. Стенд применяет только миграцию `ahp_journal`. Полный State API сохраняется режимом по умолчанию.

```sh
python profiling/state-journal/compact_faults.py /path/to/empty/faults
python profiling/state-journal/checkpoint_pair.py compact 10 --seed 10000 --history 25
```

Для пары прогоните тот же `checkpoint_pair.py` в режиме `legacy` с отдельной чистой БД, тем же mTLS, одинаковыми `--seed`, `--history` и параллельностью. Это измерение нативного графа и чекпоинтера внутри уже выданной аренды. Диспетчеризация, приём хода, модель и внешний инструмент остаются за границей этой пары. У компактного SDK пока есть только inline checkpoint payload.

Чтобы посчитать SQL, добавьте `pg_stat_statements` в `shared_preload_libraries` тестового PostgreSQL, оставьте `compute_query_id=on`, `pg_stat_statements.track=top` и `pg_stat_statements.track_utility=on`. Установите расширение в отдельной БД мониторинга. `psql` получает адрес, порт и учётную запись через стандартное окружение PostgreSQL. Пример для свежей БД компактного режима

```sh
psql -d postgres -c 'CREATE EXTENSION IF NOT EXISTS pg_stat_statements'
python profiling/state-journal/checkpoint_pair.py compact 10 --seed 10000 --history 25 \
  --sql-target-db "$COMPACT_BENCH_DB" --sql-monitor-db postgres
```

Для повторных серий на тех же двух БД задайте `--seed 0` и чередуйте порядок режимов. Скрипт делает два снимка счётчиков только целевой БД. Первый после прогрева и заполнения истории, второй перед освобождением аренд. `postgres.operation_counts` включает SQL-команды транзакций. `postgres.non_transaction_sql_per_graph` считает остальные команды. WAL измеряется по `pg_stat_statements.wal_bytes`. Счётчики пригодны для выделенных тестовых БД с единственным источником нагрузки.

Go-тест `TestCompactJournalLostHTTPReply` выполняет коммит через mTLS и обрывает TCP до ответа. Повтор должен вернуть тот же диапазон событий. Запуск требует `AHP_JOURNAL_TEST_DATABASE_URL` с отдельной PostgreSQL. Функциональный процессный тест `compact_faults.py` проверяет реальный LangGraph, смену эпохи после SIGKILL и внешнее действие через fsynced локальный файл.

Интеграционный тест двух HTTP-сервисов `TestCompactJournalToolGatewayRecovery` использует те же настройки PostgreSQL. База должна быть выделена под эксперимент, с именем на `ahp_carbon_compact`. Тест поднимает реальные State API и Tool Gateway по mTLS/JWT и локальный MCP peer. Проверяются обрыв ответа Gateway после коммита, обрыв ответа MCP после эффекта, отказ старому epoch, чтение квитанции новой задачей в том же контексте и дедупликация по ключу операции.

```sh
go test -race -count=1 -v ./internal/tools/toolsapi -run '^TestCompactJournalToolGatewayRecovery$'
```

Для проверки нативного LangGraph укажите `AHP_JOURNAL_GATEWAY_NATIVE_PYTHON` с интерпретатором окружения LangGraph 0.6.11 и запустите ту же команду. Тест передаёт два task JWT и mTLS-материал через временный handout. Исходный процесс LangGraph погибает после эффекта, но до локального сохранения результата. Замена получает новую аренду и продолжает граф через компактный saver и штатный Tool Gateway.

## Сравнение от приёма до завершения хода

`TestLangGraphPairedRunEnvelope` использует одинаковые State API для очереди и Run Coordinator и штатный Tool Gateway в обеих сборках стенда. Граф выполняет две параллельные ветки, одну запись через локальный MCP peer и join. Сервис сохраняет намерение и результат инструмента в обоих режимах. В компактном режиме чекпоинтер и намерения пишут в компактный State API, дополнительная аренда входит в замер. Цикл заканчивается после подтверждения `done` через GetRun.

Создайте две пустые БД на локальном PostgreSQL с префиксом `ahp_carbon_compact`, настройте `pg_stat_statements` и передайте параметры подключения `psql`. Каталог результатов должен существовать. По умолчанию выполняются три повтора для параллельности 1 и 10, по 100 ходов на серию. В каждой БД 10 000 заранее созданных тредов и по 25 предыдущих ходов на измеряемом треде.

```sh
export AHP_JOURNAL_BENCH_PYTHON=/path/to/langgraph/venv/bin/python
export AHP_JOURNAL_BENCH_PSQL=/path/to/postgres/bin/psql
export AHP_JOURNAL_BENCH_LEGACY_DSN="$LEGACY_BENCH_DSN"
export AHP_JOURNAL_BENCH_COMPACT_DSN="$COMPACT_BENCH_DSN"
export AHP_JOURNAL_BENCH_OUTPUT="$RESULT_DIRECTORY"
go test -count=1 -timeout=15m -v ./internal/tools/toolsapi -run '^TestLangGraphPairedRunEnvelope$'
```

Сырые JSON содержат длительности приёма, выдачи, графа, завершения и полного хода, максимум наблюдаемых аренд, пропускную способность, HTTP и SQL. У исходного и компактного пути разные реализации журналов. Обе сборки пользуются общей проверенной моделью приёма и завершения хода. Этот результат относится к стенду с локальным коннектором и детерминированным графом.

## Hermes

Используйте исходники Hermes на коммите [`9dbaa77c4c`](https://github.com/NousResearch/hermes-agent/commit/9dbaa77c4c5b6391510832602f841bc95d390b22) и Python 3.14 с зависимостями из его `pyproject.toml`. Запускайте каждый прогон с отдельным пустым `HERMES_HOME` и отдельным каталогом для результатов. `AHP_STATE_HANDSHAKE` должен указывать на компактный стенд.

```sh
export PYTHONPATH="$HERMES_SOURCE:$PWD:$PWD/sdk/python"
python profiling/state-journal/hermes_compact_probe.py "$MESSAGES_OUTPUT"
python profiling/state-journal/hermes_tool_probe.py "$TOOLS_OUTPUT"
python profiling/state-journal/hermes_compaction_probe.py "$COMPACTION_OUTPUT"
```

Первый скрипт вызывает штатный flush Hermes и проверяет точность нативных сообщений после перезапуска. Второй запускает встроенный цикл Hermes под арендой AHP. Модель отвечает через локальный HTTP, тестовый инструмент пишет один внешний эффект с `fsync`. Проверка охватывает нативный путь сохранения сообщений и исхода инструмента. Метаданные промпта и счётчики в этом прогоне заданы стендом.

Третий скрипт вызывает `commit_compaction` из закреплённого Hermes с готовым summary. Маркер компактизации и новые строки фиксируются одной операцией append под текущим head и эпохой. Новый процесс восстанавливает активную историю по порядку журнала. Исходные строки доступны в режиме ALL. Сценарий покрывает перенос неизменённых сообщений по ID, включение конкурентного ввода, потерю ACK и отказ при незавершённом действии. Для штатного `/compress` также требуется интеграция model config и compression lease через репозиторий сессий.

Для совместной проверки со штатным Tool Gateway используйте отдельную PostgreSQL с именем базы, начинающимся на `ahp_carbon_compact`.

```sh
export AHP_JOURNAL_TEST_DATABASE_URL="$TEST_JOURNAL_DSN"
export AHP_JOURNAL_GATEWAY_HERMES_SOURCE="$HERMES_SOURCE"
export AHP_JOURNAL_GATEWAY_HERMES_PYTHON=/path/to/hermes/venv/bin/python
go test -p 1 -race -count=2 -v -timeout=12m ./internal/tools/toolsapi -run '^TestCompactJournalToolGatewayRecovery$'
```

После получения новой аренды встраивающий код вызывает `JournalMessages.recover_gateway_effects(gateway)` до запуска нативного цикла. Функция `gateway(operation, body)` работает под JWT того же контекста и возвращает JSON либо типизированную ошибку SDK. Уже сохранённую квитанцию восстанавливает `GetResult`. Только подтверждённый `NOT_FOUND` разрешает отправку с прежним ключом. `HermesGatewayPending` означает, что вызывающий код может перепроверить состояние после продления аренды. `HermesReconciliationRequired` останавливает ход. В этом варианте квитанция должна помещаться в журнал inline.

Подтест `native_hermes_large-inline` проверяет стенд без Artifacts sink. Gateway удерживает квитанцию около 300 КиБ и отдаёт её после SIGKILL. Hermes обнаруживает превышение лимита компактного журнала перед сохранением сообщения и обращением к модели. Счётчик MCP подтверждает одно действие. Для положительного прогона с большой квитанцией требуется рабочая связка с Artifacts.

## DeepSeek Harness и Tool Gateway

С закреплёнными пакетами DeepSeek Harness `0.2.1-alpha.2` и Node.js 25.8.0 запустите составной стенд на отдельной PostgreSQL с именем базы, начинающимся на `ahp_carbon_compact`.

```sh
export AHP_JOURNAL_TEST_DATABASE_URL="$TEST_JOURNAL_DSN"
export AHP_JOURNAL_GATEWAY_DEEPSEEK_NODE="$(command -v node)"
go test -p 1 -race -count=3 -v ./internal/tools/toolsapi \
  -run '^TestCompactJournalToolGatewayRecovery$/^native_deepseek_'
```

Штатный SessionPersistence вызывает сверку перед тем, как нативный resume закроет прерванный ход. Хост передаёт авторизованный `gateway(operation, body)`. При известном исходе результат Gateway появляется в нативном `tool/result`, при подтверждённом 404 используется прежний ключ, при неизвестном исходе записывается нативный `TOOL_OUTCOME_UNKNOWN`. В новом журнале ключ включает AHP ID сессии. Для журналов, записанных до появления версии ключа, сохраняется прежний формат и консервативный нативный resume. Проверка использует inline-результаты и штатную политику checkpoint.

## Каталог OpenClaw Gateway

Запустите проверку с OpenClaw `2026.6.11 (e085fa1)` и Node.js 25.8.0. Скрипт создаёт отдельный тестовый каталог и останавливает свой процесс Gateway после прогона.

```sh
AHP_OPENCLAW_CLI=/path/to/openclaw node profiling/state-journal/openclaw_gateway_probe.mjs /path/to/results
```

Скрипт вызывает штатные WebSocket RPC для списка, истории и изменения метаданных. Кэш сессий отключён для проверки пути полного чтения с диска. В тестовом каталоге 1 407 сессий с повторяющимся текстом skills. Выходной JSON содержит размер файла до и после изменения, число сохранённых записей, число уникальных ссылок на текст и стеки файловых чтений.
