# Поток Model → Core → Space → браузер

`POST /v1/model/StreamModel` использует тот же mTLS + task JWT, маршрутизацию,
метки, резерв и проверку ключа, что `SubmitModel`. Тело: `key`, `messages`,
необязательные `model`, `tools`, `max_tokens`, `after` (курсор поколения, сначала 0).
`SubmitModel` остаётся совместимым синхронным API.

Ответ `application/x-ndjson` содержит последовательность:

- `accepted` с `generation_id` — подтверждение ресурса, не готовый ответ;
- `delta` с `{generation_id, sequence, text}` — настоящий фрагмент провайдера;
- `heartbeat` — транспортная проверка соединения, без модельного текста;
- `terminal` с Generation — после всех её дельт;
- `error` — подписка прервана, успех не подтверждён.

Переподключение повторяет исходное тело с тем же ключом и последним `after`.
Новый request под занятым ключом отвергается. Один PostgreSQL advisory owner
исполняет поколение; подписчики не объявляют живую отправку потерянной.
Неизвестная отправка не повторяется: остаётся `SIDE_EFFECT_UNKNOWN` и
консервативное учётное состояние. `finish_reason`, включая `length`, и конечный
wire output сохраняются у Generation, а компактная квитанция содержит digest и
finish_reason. Восстановление проверяет digest и не теряет усечение или tool calls
между ledger commit и generation finalize. Старые inline-квитанции читаются.

Core до отправки сохраняет неизменяемый `model_intent` в State под ключом
`run:model-request:step:attempt`. Это исходный запрос и координата эффектов,
не изменение бизнес-payload Run. После перезапуска используется именно этот
запрос; новые настройки runner не переписывают уже принятое намерение.
Результаты несут `model_intent_key`. Перед следующим model/tool действием Core
ждёт подтверждения State append предыдущего результата; snapshot может отставать.
Уже записанные model/tool результаты не добавляются второй раз. Дельты получают
отдельные `run:delta:generation:sequence` ключи и не меняют номера checkpoint
границ. Насыщение канала создаёт обратное давление, а не потерю токенов.

Space отдаёт тот же durable stream через существующий
`GET /v1/space/stream/{thread}?after=...`. Browser использует Bearer + cookie
своей сессии, возобновляет курсор и показывает одну частичную карточку на
поколение. Конечный Model frame заменяет её текст. Завершение Turn подтверждает
`GetTurn`; EOF или первый HTTP byte не считаются завершением.

Отсоединение HTTP reader само по себе не отменяет inference. Явная отмена
использует `CancelModel`; при отзыве run grant detached owner дополнительно
проверяет живую Identity authority каждые 500 мс. Поэтому отказ старому JWT на
cleanup после `CancelTurn` не оставляет провайдера работать бесконечно. Model
проверяет запрос отмены каждые 250 мс даже при зависшем provider stream. При
неизвестном счёте резерв не возвращается и нулевой расход не выдумывается.

Границы: жизнь owner/подписки 10 мин, запись stalled reader 10 с, максимум 2 MiB
текстового output и 65536 дельт поколения. Журнал хранит только фрагмент, а не
полную накопленную копию на каждый токен; FK удаляет его вместе с generation.
NDJSON frame ограничен 16 MiB с учётом JSON escaping полного 2 MiB текста.
Слишком большой encoded tool envelope явно завершает generation FAILED с
учётом actual usage; текстовый лимит 2 MiB при этом не уменьшается.
Каждая подписка проверяет exp JWT самостоятельно, включая наблюдателя чужого owner.
Tenant/subject barrier блокирует новые фрагменты до удаления содержимого.
Автоматический TTL для поколений этим изменением не добавлен.

`Run.traceparent` — отдельная optional колонка, зафиксированная при INSERT из
валидного серверного W3C context. Повтор ключа сохраняет исходного родителя;
canonical payload/fingerprint не меняется, массивы и строки остаются допустимы.
Холодный runner восстанавливает ancestry и создаёт `space.run`, Core —
`core.model`. mTLS HTTP transport передаёт TraceContext; baggage не переносится.
В collector остаются только разрешённые поля, без prompts, credentials и
произвольных resource IDs.

При переходе с прежнего terminal-only runner необходимо закончить активные
legacy runs: у них ещё нет durable model intent и нового ключевого пространства.
Новые runs и их восстановление проходят новый контракт. Тесты protocol fixture
не являются LLM E2E; реальная модель и браузер проверяются отдельно на стенде.


Ограничение inline-пути Core/State отдельно: State stream payload и HTTP request
ограничены 1 MiB. Model 2 MiB не означает, что такой final frame можно целиком
сохранить в State. Существующий save_artifact возвращает ArtifactRef по явному
вызову инструмента; автоматического переноса oversized final ответа в Artifacts
в этой версии нет. Это оставшийся пункт PDF §2.5, а не повод повышать глобальный
лимит State. При отказе durable append Coordinator прекращает run с причиной.
