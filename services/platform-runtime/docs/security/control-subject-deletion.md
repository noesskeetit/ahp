# Удаление данных сотрудника во всей AHP

PDF rev3.2 §6.8 требует, чтобы удаление сотрудника затрагивало все данные его
субъекта в сервисах AHP, а также объявленные `own_tables` агента. Для этого
служит `POST /v1/state-admin/ControlDeleteSubject`. Обычный `DeleteSubject`
сохраняет границу одного deployment в State; это не замена общей операции.

```json
{"target":{"deployment":"entry-deployment","subject":"target-subject"},"input":{"key":"stable-operation-key"}}
```

Tenant берётся из проверенного principal. Вход требует human administrator,
control-plane mTLS и полномочия State admin/удаления и Memory delete-subject.
Случайный task token, даже принадлежащий удаляемому человеку, этого не разрешает.
Политика HTTP-авторизации задаётся State admin facade; тело не задаёт tenant
или полномочия. Subject — явно выбранный администратором адресат удаления.

## Что происходит

1. Координатор сохраняет durable intent: tenant, subject, инициатор, стабильный
   ключ, найденные scopes и версии/адреса custom delete hooks.
2. Identity отзывает task grants и браузерные сессии. Все владельцы данных
   устанавливают постоянные write fences до окончательной инвентаризации.
   Fence ждёт уже допущенные записи; поздние callback/dispatch/upload не могут
   заново создать данные. Неизвестный исход побочного эффекта остаётся UNKNOWN.
3. Registry связывает старые task pins с субъектом через APIs Identity/Gateway,
   пока их task inventory существует. Новые pins сразу содержат проверенный
   subject; Alias переносит его вместе с неизменяемой конфигурацией.
4. Владельцы удаляют собственные данные, затем State/Artifacts и version-pinned
   custom hooks подтверждают свои шаги. Identity удаляет credentials, sessions,
   grants и context journal последней. Общие permanent tombstones остаются:
   без них старый токен или поздний writer смог бы воскресить данные.
5. Только после подтверждения всех обязательных шагов job становится completed.
   ACK и plan хранятся в State: повтор/перезапуск продолжает ту же операцию.

Production composition требует семь дополнительных `SubjectDeletionOwner`:
`compute`, `gateway`, `identity`, `model`, `registry`, `tools`, `triggers`.
State, Artifacts, Memory и Workspace уже входят в базовый cascade.
Если обязательный owner недоступен, новая операция не объявляет частичный
набор сервисов полной очисткой. SQL каждой схемы остаётся внутри её сервиса.

| Владелец | Что удаляется / что удерживает завершение |
|---|---|
| State | Threads, streams, runs, checkpoints и привязанные данные; epoch/lease fence запрещает поздние записи. |
| Artifacts | Metadata tombstones с сохранением retention/legal hold/reference правил; физические object versions удаляет GC. |
| Memory | Записи и поисковые производные субъекта во всех deployments; существующий subject fence остаётся. |
| Workspace | Персональные memberships, pins и доказанно принадлежащие субъекту drafts. Общие опубликованные ревизии сохраняются. Legacy drafts без автора не приписываются произвольно. |
| Model | Prompts, generations, бюджеты, settled receipts и provider-copy lifecycle. UNKNOWN/резервации без окончательного settlement удерживают pending. |
| Tools | Effect ledger, approvals и guardian records субъекта; собственные OAuth requests/connections и все версии секретов в Vault. Подписи удалённого человека на чужих approvals отзываются. Общая operator policy остаётся без его actor attribution. |
| Gateway | Tasks, contexts, messages, status events, push configs, outbox/inbox. Активное исполнение и неопределённая доставка удерживают pending. Terminal receipt удаляется атомарно с содержимым. |
| Triggers | Schedule/webhook источники, delivery metadata и settled ledger. Поздний fire ACK не возвращает revoked trigger в ACTIVE. |
| Compute | Computers и дочерние metadata, проверенные файлы только под их собственными каталогами runner. Живые incarnation, Redis/PG leases, PTY, pending environment или UNKNOWN command удерживают pending. Образы tenant общие и не удаляются за одного сотрудника. |
| Registry | Task-specific immutable config/version pins. Tenant installation/catalog/config/version не становятся личными только потому, что сотрудник их использовал. |
| Identity | Accounts, password credentials, sessions/JTI bindings, grants/tokens и собственный context label journal; revocation tombstone сохраняется. |

Evals хранит общие publication verdicts и task-set metrics по immutable digest;
в текущей модели они не являются личным хранилищем с subject ownership. Удалять
чужой общий verdict по совпадению строки task_id нельзя. Если в эту модель
добавляются персональные eval inputs, для них потребуется явное owner поле.

## Что остаётся после удаления и что требует решения

В PDF задано удаление данных субъекта во всех сервисах. В нём нет отдельного
формата erasure receipts, срока хранения запрета повторной записи или правила,
как угадывать владельца старой записи без `subject`. Ниже — выбранная семантика
прототипа, а не дополнительные требования, приписанные документу.

- Минимальная пара `tenant + subject` в fence/tombstone остаётся постоянно;
  State сохраняет operation key, шаги и receipt удаления. Это защита от
  возрождения данных и от повторного исполнения эффекта. Полное удаление даже
  этих идентификаторов потребует другого устойчивого механизма блокировки;
  просто очистить запрет после удаления небезопасно.
- UNKNOWN receipts и legal-hold данные остаются до установления исхода или
  снятия hold. Такой job остаётся pending: сохранённые данные не маскируются
  ответом completed. Сроки эскалации и процедура reconciliation — отдельное
  операционное решение, автоматического «считай успехом через N часов» нет.
- Опубликованные Workspace revisions, Registry catalog/config и общие Compute
  images сохраняются как общие объекты. Удаляются личные memberships/drafts/pins
  с доказанным ownership. Если общее содержимое само включает персональные
  данные, текущая модель не умеет выводить их владельца из текста: потребуется
  lineage/классификация и политика редактирования общей версии. Это развилка
  для обсуждения, а не обещание, что весь текст общего каталога обезличен.
- Для legacy pin без доказанного владельца выбран pending, вместо удаления
  всех tenant pins или назначения их текущему сотруднику. Identity сообщает
  конфликт при нескольких владельцах одного task ID; такой конфликт не
  обходится fallback-поиском. Fallback Gateway используется только при
  отсутствии Identity записи.
- Завершение Artifacts означает tombstone и передачу физической очистки GC
  согласно retention. Если для приёмки нужно немедленное уничтожение каждого
  object version, потребуется отдельный обязательный ACK от GC и политика
  обхода/истечения retention. Сейчас это не часть ответа completed.

## Сбои, holds и неизвестные результаты

HTTP 503 со `state: pending` и `deletion_id` означает незавершённую работу.
Причина остаётся наблюдаемой, receipts не стираются ради успешного ответа.
Повторять надо тот же ключ. Не следует выдавать новый ключ только для обхода
неизвестного эффекта или legal hold.

При configured `ConsumerContext` intent получает durable service authority:
consumer продолжит его после logout/revocation/удаления исходной human session.
Это не полномочие создавать новые операции или менять чужой tenant/subject.
Self-deletion без такого consumer по-прежнему отклоняется до первой мутации.

Legal hold удерживает содержимое и общий job в pending, когда участвуют
дополнительные owners. После снятия hold повтор заново проверяет состояние и
продолжает шаги. Персональные членства могут быть отозваны раньше, чтобы
удаляемый человек больше не имел доступа.

OAuth fence ждёт допущенные callbacks/refresh вместе с Vault write; поздний
callback не проходит. Для известных токенов выполняется remote revocation,
если provider зарегистрировал revoke endpoint, и затем Vault DeleteMetadata
всех версий. Недоступный provider/Vault не даёт completed. Если результат
выдачи/refresh потерян (`exchanging`/`refresh_unknown`), inventory сохраняется:
нужна проверенная reconciliation у владельца OAuth, а не повторный POST
выдачи или выдуманное подтверждение отзыва неизвестного токена.
Проверка подтверждает отзыв последних известных access/refresh tokens каждого
записанного пути; она не доказывает удаление истории у внешнего OAuth provider.
Если provider не предоставляет revoke endpoint, удаляются местные credentials
и все Vault versions, а remote revocation не объявляется выполненным.

Старый Registry pin без subject сначала разрешается по авторитетным task rows
Identity/Gateway. Если ни один владелец не может доказать связь, операция
остаётся `registry_legacy_pin_owner_unknown`: такой pin нельзя ни присвоить
удаляемому пользователю, ни молча признать чужим. Требуется восстановить
проверяемое ownership исходной записи. Shared catalog при этом не стирается.

Старые completed receipts, не содержащие текущих owner-fence/owner шагов,
возвращают `deletion_owner_upgrade_required`; для обновлённого набора owners
нужен новый ключ. Receipts без базовых write fences аналогично требуют
`deletion_fence_upgrade_required`. Обычный незавершённый job продолжает свой ключ.

## Custom agents и физические файлы

Каждая текущая и task-pinned Registry версия с `state.own_tables` участвует
через свой `state.delete_hook` на зарегистрированном HTTPS origin. Вызов идёт
по control-plane mTLS без human bearer; redirects запрещены. Payload содержит
`tenant`, `subject`, `deletion_id`, `operation_key`; `Idempotency-Key` совпадает
с operation key. Hook обязан атомарно удалить данные, сохранить receipt и
запретить поздние записи, затем ответить HTTP 200:

```json
{"status":"completed","operation_key":"the-received-operation-key"}
```

HTTP 202, timeout или потерянный ответ не означают завершение. Повтор использует
тот же записанный ключ и версию. Missing hook/mTLS/owner остаются pending.

У Artifacts completed означает durable tombstone с правилами retention/GC,
а не немедленное исчезновение всех S3 versions. Ранее выданный PUT URL может
до истечения срока оставить orphan object; он не может Seal/Restore metadata
за fenced subject. GC владеет удалением этих байтов. Compute cleanup проверяет
полный список путей и symlinks до первого удаления, не трогая чужие каталоги.

## Проверка

`TestControlSubjectAllOwnersResumeAfterSelfRevocation` проверяет real PostgreSQL,
MinIO и HTTP control cascade: все семь owners, self-revocation, UNKNOWN,
reconcile, новый consumer без user token, сохранность другого subject.
Owner package tests отдельно проверяют OAuth protocol + настоящий Vault,
late callbacks/writes, MaxConns=1, Registry migration/legacy ownership,
Compute Redis leases/файлы/PTY/cancel, и late trigger ACK.

Это acceptance удаления и восстановления, не LLM E2E и не доказательство
изоляции настоящих microVM. В Compute lifecycle тестах backend обозначен MOCK.
