# Перенос больших checkpoint между контекстами

Обычный `ImportThread` не переносит право читать файл: ссылка из старого
контекста остаётся недоступной новому task token. SDK предоставляет отдельную
подготовку snapshot через Artifacts owner. Это не новая модель доступа и не
перенос данных между пользователями.

## Порядок восстановления

1. Административный клиент с human credential и mTLS экспортирует thread через
   `/v1/state-admin/ExportThread`, явно указывая deployment и subject. Обычный
   task token не становится администратором. Legacy `Client.ExportThread` /
   `StateAPIClient.export_thread` сохраняет старый адрес и получает отказ.
2. Для source context нужен действующий reader task с правом `artifacts`, а для
   target context — отдельный действующий task. Tenant и subject остаются теми
   же; перенос в другой deployment возможен при наличии его workload mTLS.
3. Go `target.RehomeExport(ctx, source, export, TransferTarget{...}, stableKey)`
   или Python `rehome_export(source, target, export, deployment=..., context_id=...,
   task_id=..., key=...)` копирует только явно помеченные `ahp-artifact-v1` части,
   metadata и pending writes. Legacy bytes без encoding остаются непрозрачными.
4. Вызвать `ImportThreadTo(..., targetContext)` в Go либо
   `target.import_thread(prepared, target_context)` в Python. До создания thread
   State проверяет target binding и доступность ссылок через owner boundary.

Для прямого управления переносом Go Artifacts client предоставляет
`TransferGrant` / `CopyFromTransferGrant`, а Python — публичный
`ahp_state.TransferClient(StateAPIClient(...))` с методами `transfer_grant` /
`copy_from_transfer_grant`. Python HTTP и recovery API не зависят от LangGraph.
Оба SDK сверяют ID/version/SHA исходного descriptor с выданным ticket до Copy,
чтобы повреждённый digest в snapshot не запускал лишний эффект.

## Authority, метки и provenance

`TransferGrant` имеет отдельную таблицу и назначение; download ticket не подходит
для копирования. В выдаче сохраняются source reader grant ID/epoch, exact version,
SHA/size и target deployment/context/task. Tenant/subject берутся из проверенного
источника. TTL не более двух минут и не дольше жизни reader grant. Это grant
читателя, выдавшего ticket, а не обязательно исторического создателя файла.

При копировании повторно проверяются обе текущие authority, отзыв, expiry,
artifact lifecycle и доступ. Копия получает объединение неизменяемых меток файла,
текущих меток source reader и target task. До появления копии Identity owner
сохраняет это объединение в target task, ancestor grants и журнале контекста.
Простой `Download` + `Upload` не является разрешённой реализацией этой операции:
он потерял бы provenance и не ограничил бы дальнейший ответ target агента.

Метаданные операции хранят исходный artifact ID/version/SHA/size. Credentials в
таблицах переноса не сохраняются: ticket хранится только как digest, source
identity — как проверяемые идентификаторы и epoch.

Перенос использует собственную публикацию: после физической Copy owner читает
назад exact version и сверяет SHA/size, но artifact остаётся **unsealed**. Затем
owner принимает последнее актуальное решение по source/target authority и expiry,
сохраняет target journal taint и одной транзакцией публикует version/hash/labels
вместе с completed receipt. До этого commit обычные Get/Download/List/pins не
видят копию. Публичный Seal не может запечатать transfer-owned target — он
проверяет это также внутри своей транзакции публикации.

Это не общая транзакция Identity + Artifacts: revoke, пересекающийся с переносом
после последнего успешного решения об authority, упорядочивается после принятого
переноса. После publication commit нет нового условного отказа source policy
или удаления подтверждённых данных. Revoke до этого решения даёт отказ и
оставляет bytes неопубликованными. Потерянное подтверждение commit означает
UNKNOWN: запись могла сохраниться, повтор проверяет outcome.

Ticket удаляется вместе с исходным artifact; истёкшие tickets дополнительно
вычищаются ограниченными пакетами. Метаданные dispatch сохраняются как запись
об операции даже после удаления target artifact: иначе повтор смог бы отправить
копию заново под старым ключом. Они содержат provenance и идентификаторы, не
bytes или credentials. Их очистка требует отдельного срока хранения журнала
операций, согласованного с правилами повторов.

## UNKNOWN, ограничения и стоимость

До отправки S3 Copy owner отдельно фиксирует `dispatched`. MinIO для этой операции
сконфигурирован с одной попыткой. Потерянный ответ не вызывает вторую отправку:
повтор с тем же stable key пытается запечатать и сверить уже записанный объект.
Если его существование и целостность не доказаны, возвращается
`UNAVAILABLE / transfer_outcome_unknown`. Новый ключ означает новую операцию;
SDK не выбирает его автоматически. Даже если первый запрос фактически не дошёл
до S3, такой UNKNOWN может требовать решения оператора. Это цена отсутствия
общей транзакции PostgreSQL + S3.

В Go проверять `contracts.ReasonOf(err) == "transfer_outcome_unknown"`,
в Python — `StateAPIError.code == "transfer_outcome_unknown"`
(HTTP class остаётся `UNAVAILABLE`). Оба клиента сохраняют domain reason и
не делают автоматическую повторную отправку Copy.

Source и target защищены от GC блокировками на время ограниченной операции.
Потеря соединения освобождает блокировки: API всё равно не может вернуть успех
без exact-version Seal, проверки SHA/size и завершения транзакции. Ticket сам по
себе не удерживает файл: удалённый до Copy source приводит к отказу. Вечных pins
от этой операции не остаётся.

Пределы: `AHP_CHECKPOINT_TRANSFER_MAX_BYTES` — 1..67108864 байт на файл,
`AHP_CHECKPOINT_TRANSFER_TIMEOUT` — положительное значение не более `60s`.
Одна копия одновременно на экземпляр Artifacts Store; очередь входит в timeout.
Нужно минимум два соединения owner DB pool: одно для GC lock, второе для
устойчивой записи dispatch и Seal. Копирование занимает дополнительное место в
S3, проверка публикации читает всю копию для сверки хеша. Export response ограничен SDK
16MiB; существующий Import HTTP request — 1MiB. Это не потоковый перенос
неограниченно длинной истории.

Fingerprint операции включает immutable source descriptor, live source-reader
labels, exact target binding и purpose. Рост target labels от собственных
предыдущих переносов исключён из fingerprint: snapshot A→B→A с разными метками
должен восстановиться и повториться без новых Copy. При первом publication
target labels входят в frozen labels; при replay immutable копия сохраняется,
текущие target ограничения всё равно применяются через admission/journal.
Изменение source-reader labels с тем же ключом получает KEY_REUSE_MISMATCH:
он не ослабляет метки и не переписывает ранее запечатанную версию. Необходимость
восстановления при изменившейся политике требует новой явно выбранной операции.
Перенос между tenant/subject запрещён. Transfer не реализует общий backup KV,
удалённых исходных bytes или восстановление утратившего права reader.

Проверка блока: реальные PostgreSQL/MinIO/Identity/mTLS, Go/Python recovery,
отзыв reader, подмена получателя, expiry/purpose/labels, потеря S3 ACK, повтор
после перезапуска owner, GC при обрыве DB-соединения и pool из двух соединений.
Человеческая admin-role в тесте синтетическая; это не подтверждение Cloud IAM
или LLM E2E.
