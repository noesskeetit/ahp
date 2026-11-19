# Memory: семантический поиск и эксплуатация

Основание — PDF rev3.2, §7 с.48 (BM25 + pgvector + граф, admissible set до ranking, повторная проверка прав) и §8.2 с.49 (метки чтения и маршруты моделей). Runtime Hindsight и его данные не используются.

Memory объединяет reciprocal rank fusion (k=60) для настоящего BM25 (k1=1.2, b=0.75), cosine pgvector и одношагового графа по общим evidence ArtifactRef и supersedes. BM25 считает document frequency и длины только по разрешённым источникам. Граф вычисляется по canonical rows, отдельного eventually-consistent индекса нет. Векторный поиск точный; ANN не включён, чтобы фильтрация и top-k не зависели от кандидатов чужого tenant. Это решение обеспечивает точность фильтра, но не является доказательством производительности большого корпуса.

До ranking применяются tenant, active, TTL, user owner, agent deployment, live workspace membership, visibility labels. При выдаче заново читаются строка, lifecycle и membership. Proposed доступен автору по Read, но не участвует в Search. Чтение чужого proposed и любых revoked/rejected/superseded требует отдельного `memory:review` и тех же source ACL; обычный reader получает NOT_FOUND. Deleted и истёкший TTL не читаются. Revoked/rejected/superseded/deleted не участвуют в Search. Удаление очищает текст/embedding в owning row; граф и BM25 перестают видеть источник сразу. `Reindex` не восстанавливает удалённые строки.

В identity composition чтение обязательно передаёт объединённые labels в Identity.AddLabels до возврата содержимого. Неисправный journal запрещает выдачу. Это относится также к ответам Review, AddLabels и idempotent Propose replay, которые читают существующий source: после owning commit заново проверяются ACL/lifecycle и записываются метки читающей задаче. Ошибка журнала не откатывает завершённую owning mutation; повтор с тем же operation key повторяет проверку выдачи. Если AddLabels сузил доступ так, что инициатор больше не читатель, mutation остаётся выполненной, а ответ с содержимым возвращает NOT_FOUND. Новый Propose возвращает собственный input автора; replay всегда читает current source, включая метки, добавленные позже. Scope agent проверяет весь live ceiling через labels.CheckWrite: workspace labels нельзя отбросить даже при отсутствии Subjects. Прямой library Store допускает явные in-process hooks для integration-тестов; production HTTP mount требует live Identity. `Propose` отправляет embedder объединение labels записи и потолка задачи; `Search` — live task ceiling. Политика не берётся из request body.

## Production configuration

Шаблоны: `infra/memory/embedding.env.example` и `embedding-policy.example.json`. Обязательны URL, model, dimension, operator revision, credential, provider identity и policy file. Авторитет — явно настроенный tenant route в существующем Model routing.Router; выбранные provider/model должны совпасть с подключённым Memory backend. Source labels остаются неизменны. Пример разрешает только `synthetic-memory-demo`. Поле operator-only `subject_from_scope=true` явно разрешает provider обрабатывать private данные указанного tenant; `audience.kind=private`, фиксированный `audience.subject` отсутствует. Router подставляет subject только из verified contracts.Scope в локальную копию audience и применяет неизменённый labels.CheckWrite. Чужие subject labels, secret, invalid scope и другие tenants отклоняются. Каждая route к такому provider обязана перечислять конкретные tenant IDs; wildcard и пустой список запрещены. `allow_external` не включается автоматически: обработка external-tainted данных требует отдельного явного разрешения оператора. Это доверие провайдеру и его договору обработки данных, а не утверждение физической изоляции внешнего FM. Для production оператор должен выбрать разрешённый tenant модельный endpoint и конкретные tenant IDs; данный synthetic пример не разрешает отправлять реальные пользовательские данные FM.

HTTP transport использует `pkg/netpolicy`: exact host/path/method, проверка DNS/IP при соединении, отсутствие environment proxy, bounded response/timeout. Redirect не выполняется; credentials не могут уйти на redirect target. Private RFC1918/ULA endpoint требует явного allowlist, loopback/link-local/metadata всегда запрещены. Private-host разрешение определяет сетевую достижимость и не заменяет tenant label policy.

Модель идентифицируется hash(endpoint, model, operator revision, dimensions), сохраняемым в `embedding_space` каждой записи. Другая identity исключает старые векторы из сравнения даже при равной размерности. Проверяются response model (когда provider его возвращает), dimension, finite float32 и ненулевой вектор. Имя `BAAI/bge-m3` на FM не является immutable model revision; operator revision фиксирует конфигурацию, не доказывает неизменность upstream weights. При смене alias weights оператор обязан обновить revision и переиндексировать корпус.

Без endpoint startup отказывает. Только явный `AHP_MEMORY_SEARCH_MODE=lexical` включает деградацию; `Search.mode=lexical_graph`. Наличие endpoint в lexical mode — ошибка. При hybrid ошибки embedding/policy возвращаются клиенту, lexical fallback не выполняется. `Search.mode=hybrid` означает использование настроенного semantic pipeline, а не аттестацию полноты старого индекса; перенос lexical-корпуса требует Reindex. Пустой query — `mode=browse`.

## Индексирование существующих записей

Старая пустая embedding-колонка автоматически получает размерность нового backend. Существующие несовместимые по ширине векторы не удаляются автоматически; Migrate отказывает. Изменение ширины требует maintenance drain и явной миграции после сохранения canonical records. При той же ширине новый embedding_space безопасно исключает старые векторы до reindex.

`POST /v1/memory/Reindex` принимает `{"op_key":"index-batch-001","limit":20}`. Нужны обычный memory grant и `memory:review`, verified mTLS/task token и доступ к источникам. Один batch индексирует до 20 видимых active источников без актуального embedding_space. Ключ идемпотентен и привязан к пространству модели. Повторять с новыми ключами до `indexed=0`. Строки блокируются на время policy+embedding, поэтому labels/deletion не меняются между проверкой и отправкой; tenant fence сериализован owning transaction. Batch ограничен, но держит locks на время сетевых запросов: выполняйте вне пика нагрузки.

## Воспроизводимая проверка

Используйте отдельную тестовую PG DB с pgvector и ролью CREATEDB; обычные Memory tests также используют тестовый AHP MinIO. Ни одной команды against mainstand для live proof не требуется.

```sh
# В окружении: AHP_TEST_DATABASE_URL (только isolated test DB),
# AHP_MEMORY_EMBEDDING_BASE_URL/MODEL/DIM/KEY_FILE/REVISION.
go test -tags ahp_data,ahp_semantic_live ./internal/memory \
  -run '^TestLiveSemanticRetrieval$' -count=1 -v -timeout 5m
# Реальные PG lifecycle/ACL + transport/policy contract tests:
go test -race -tags ahp_data ./internal/memory/... -count=1
```

Live test требует actual HTTP endpoint, сам создаёт отдельную throwaway DB и не содержит skip/fake path. Все отправленные тексты synthetic. Четыре пары EN→EN, RU→RU, EN→RU должны иметь target lexical matches=0 и semantic top-1; дополнительно проверяются два пользователя через один backend/policy, bidirectional private Read denial, tenant/subject/lifecycle exclusion и idempotent reindex. Fixture hash vectors в старых unit/integration tests доказывают только mechanics и не считаются semantic evidence.

`docs/memory/api_demo.py` выполняет тот же базовый сценарий через deployed Memory HTTP face. Передайте существующие AHP mTLS cert/key/CA и task token files для synthetic Alice/Bob; токены должны иметь memory, memory:review, memory:delete. Скрипт не создаёт Identity grants и не меняет policy, пишет и удаляет только свой synthetic факт. Успех library/PG proof не считается доказательством этого deployed-API сценария.

Проверенные primary sources: [Cloud.ru API](https://cloud.ru/docs/foundation-models/ug/topics/api-ref), [доступные модели](https://cloud.ru/docs/foundation-models/ug/topics/overview__available__models), [PostgreSQL text search](https://www.postgresql.org/docs/18/textsearch-features.html). Фактический FM `/v1/models` и `/v1/embeddings` дополнительно проверены live 2026-11-02; это наблюдение, не обещание будущего состава моделей.
