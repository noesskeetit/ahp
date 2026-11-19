# Общий State для native адаптеров

Рабочая opt-in интеграция; поддержка конкретного движка определяется версией адаптера и его recovery-проверками. State хранит opaque native properties и нормализованные records/relations. Переписка, workflow-resume, HITL и безопасный инструмент — разные возможности.

Сервис `ahp` в ролях `state`/`all` читает `AHP_STATE_COMMON_MODE`: `off` (также пустое значение, старый режим), `snapshot` или `interval`. Опечатка запрещает запуск. `interval` включает физические версии metadata и delta. Уже созданное состояние не переводится в прежний формат переключением флага; старые readers должны отказывать явно. Для Artifact-aware профилей в role `all` нужен включённый data plane. Выключение native writers не исключает существующие common roots из проверки Artifact GC.

Namespace ownership задаётся доверенным оператором в JSON-файле `AHP_STATE_KV_NAMESPACE_POLICIES_FILE`, например:

```json
[
 {"tenant":"tenant-example","deployment":"agent-example","namespace":["adk-user","reviewed-app"],"owner":"subject"},
 {"tenant":"tenant-example","deployment":"agent-example","namespace":["adk-app","reviewed-app"],"owner":"deployment"}
]
```

Это конфигурация согласованных `state.shared` пространств. Тело запроса не может назначить владельца: `expected_owner` лишь проверяет политику. Удаление сессии не удаляет user/app данные. Изменение владельца ранее объявленного namespace требует явной миграции.

Go: `sdk/go/state` содержит `CommonDocument`, `CommonDelta`, `CommonCommit`, `CommonReceipt`, `KVRead/KVPut`, методы `CommitRecords`, `ReadRecords`, `ReadRecordsBundle`, `PutKVBatch/GetKVBatch`. `CommonCommit.Document` и `.Delta` — альтернативные указатели. SDK не пересылает нулевой полный документ вместе с delta. Scope не принимается от клиента. Текущий task token получается из `TokenSource` на каждый вызов.

Python: framework-neutral `StateAPIClient` предоставляет `commit_records`, `read_records`, `read_records_bundle`, `put_kv_batch/get_kv_batch`. Можно передать фиксированный `token` либо `token_source` (sync или async callback). Источник вызывается заново для State и Artifact запросов; явный `with_token` создаёт независимый credential view. `ClaimRun` использует workload mTLS. Источник обновления токена должен обращаться к Identity; heartbeat обслуживает lease отдельно. Новый JWT не возвращает потерянный epoch.

Общий commit атомарно публикует snapshot вместе с переданными KV/ACK/stream/status. После неизвестного ответа повторяется тот же ключ и тело. SDK не меняет ключ и не повторяет запрос автоматически. За проверку native версии, содержимого и возобновление отвечает адаптер.

`ReadRecordsBundle` возвращает полное common состояние либо `common_unchanged` и receipt для известной revision. Проверять identity receipt обязан кэширующий адаптер; KV берутся из свежего `bundle.kv`, а не из исторического `receipt.kv`. Холодный процесс запрашивает полный снимок. Coherent bundle использует session advisory locks: для этого пути нужен direct/session-pooled PostgreSQL, transaction pooling не принят.

Legacy `PruneCheckpointHistory` common state не очищает и отказывает явно. Для common state реализованы отдельная очистка истории, большие shared values и необязательное ускорение append; их правила и ограничения приведены ниже.

Проверки интеграции: `TestCommonGoSDKActualMTLSPostgresDeltaOwnersAndReceiptReplay` (для Python leg задать `AHP_COMMON_SDK_PYTHON` на окружение с httpx), `TestCommonRootsRemainVisibleWhenWriterFeatureIsDisabled`, `TestCommonConfiguredOwnerStateSurvivesWritingSession`. `sdk/python/tests/test_common_client.py` — отдельные transport-проверки, не LLM E2E.

## Очистка и закреплённые снимки

`AHP_STATE_COMMON_RETENTION_PROFILES_FILE` — доверенный JSON-массив имён проверенных native-профилей, в которых каждый снимок содержит полное восстановимое состояние. Без файла очистка выключена. По opaque JSON нельзя автоматически определить зависимости движка.

`PutCommonRoot` закрепляет именованную revision. Операция привязана к task и действующей lease, проверяет CAS-версию pin. `revision: null` освобождает pin, сохраняя его версию и квитанцию. Метод есть в Go/Python SDK.

Человеческий admin вызывает `PruneCommonHistory`: deployment/subject, thread, namespace, operation key, ожидаемый head, желаемая нижняя граница читаемой истории и лимит (по умолчанию128, максимум512 снимков). Активная lease, неразобранный intent, legal hold и непроверенный native-профиль запрещают очистку. Именованные pins понижают границу удаления.

Удаляется только старый непрерывный префикс. Заголовки снимков и исходные квитанции сохраняются: повтор старой операции возвращает прежний результат, изменённое тело с прежним ключом запрещено. Освобождаются недостижимые версии/payload и старые явные Artifact roots. Чтение намеренно очищенного снимка возвращает NOT_FOUND; оставшийся суффикс восстанавливается точно. Pin старого снимка сохраняет весь суффикс от него, а не только один снимок. PostgreSQL DELETE не гарантирует немедленное уменьшение файлов; физическая компакция — отдельная операция обслуживания.

Перенос такой схемы использует DeploymentV6; со следующим режимом append — V7. Старые форматы отказывают на дополнительных таблицах. Нужны тот же логический scope, чистая целевая State-БД, совместимые reviewed profiles, доверенный offline-координатор и проверенный общий Artifact owner.

## Большие общие данные и ADK

Go `SharedValues` и Python `ahp_state.shared.SharedValues` сохраняют значения отдельными неизменяемыми SHA256-блоками в том же reviewed subject/deployment namespace, где лежит небольшой манифест с версиями отдельных ключей. `Prepare`/`prepare` только подготавливает блоки и возвращает KV put. Он включается в `CommitRecords`: событие и новый манифест публикуются одной транзакцией. Неуспешная публикация не меняет прежнее состояние.

При чтении проверяются хеши каждого блока и целого значения. Ограничения профиля:16MiB на значение,32MiB текущих данных владельца,256 ключей,1024 UTF-8 байта на имя ключа,512KiB на манифест. CAS у манифеста общий: параллельные изменения разных ключей могут конфликтовать. Неопубликованные блоки сохраняются консервативно; автоматического возрастного GC для них нет. Удаление сессии не удаляет shared data; данные удаляются через lifecycle их subject/deployment владельца.

Extra `ahp-langgraph[adk]` фиксирует google-adk2.14.0 и не меняет LangGraph extra. `ahp_state.adk.AHPSessionService` использует сопровождаемый SDK. Хост передаёт проверенные identity, thread, fence, session и reviewed user/app namespaces. Native `app_name` сам по себе не выдаёт прав и не выбирает tenant.

Адаптер сохраняет native events/state, поддерживает фильтрованный просмотр без потери полной истории, точный replay event-ID, отдельные shared owners и восстановление в новом процессе. Каталог ограничен одной сессией, привязанной к данному host; discovery/routing всего флота остаётся у host. Native history использует inline common-профиль с общим ограничением8MiB. Произвольное workflow-resume и произвольный размер events этим не обещаются. Для внешних эффектов необходим intent/Tool Gateway контракт платформы.

При неизвестном исходе commit адаптер останавливается и сохраняет точный pending request для сверки host-ом. Он не повторяет действие с новым ключом. Совпавший event-ID с другими данными запрещён; точный повтор уже сохранённого события не добавляет дубль.

## Ускоренный append

`AHP_STATE_COMMON_APPEND=on` работает только с `AHP_STATE_COMMON_MODE=interval`. Для каждого снимка хранится независимый корень256 SHA256-блоков: metadata записей, хеши payload, длины списков и упорядоченные связи. Обычный append читает затронутые блоки вместо всей прежней native истории. Полные снимки, новые роли и удаляющие изменения проходят полную проверку. Переполнение ограничений блока/неподходящие данные используют полный путь; валидные большие opaque native IDs не отвергаются только из-за выбранного ускорения.

Это не обещание O(1): затронутый блок metadata хешируется целиком. Полное историческое чтение восстанавливает native документ, проверяет его корень и сохранённые блоки. Повреждение нетронутого старого payload может не обнаружиться при независимом append; чтение снимка откажет на повреждённых байтах. Режим не чинит повреждённое хранилище и не превращает выключенный reader в совместимый legacy reader.

Цена ускорения — дополнительные metadata на диске. Очистка проверяет корни и блоки всех оставшихся namespace перед их освобождением. V7 переносит две дополнительные физические таблицы вместе с pins/receipts. Для V6/V7 есть human admin HTTP-маршруты с отдельной deployment-transfer capability и проверкой offline custody.
