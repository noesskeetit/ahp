# `DownloadGrant` — контракт стороны Artifacts (2026-10-31)

Основание: `decisions/2026-10-31-artifacts-download-grant-contract.md` (HQ `6ace53d`), PDF rev3.2
§6 таблица сервисов (стр.1582: `BeginUpload, Seal, Export, DownloadGrant`), рис. 7 (стр.608)
«скачивание через прокси: текущие права, точная версия», подпись стр.614 (потолок видимости
применяется и к выдаче файла).

Владелец продукта — Artifacts (delivery). Маршрути человека (human edge / Space-прокси) —
Execution/Space; этот файл описывает **контракт вызова**, а не их реализацию.

## Операции

Обе — именованные POST-операции Artifacts, за тем же грубым гейтом, что и остальная плоскость:
mTLS workload-сертификат + task-токен с capability `artifacts`. Браузер ни одной из этих
операций не вызывает и ни одной из этих границ не пересекает.

### `POST /v1/artifacts/DownloadGrant` — выдать грант

Запрос (неизвестные поля отбрасываются строгим JSON):

| поле | обязательность | смысл |
|---|---|---|
| `id` | да | артефакт |
| `version` | да | **точная** запечатанная версия из `Get`/`Seal`; пусто или `latest` — отказ |
| `recipient` | нет | получатель; обязан совпасть с субъектом вызывающего; пусто = он же |
| `ttl_seconds` | нет | срок; нет = 120 с, больше 900 с усекается до 900 с, меньше 5 с — отказ |
| `uses` | нет | число потреблений; нет = 1; максимум 8 |

Ответ `200`: `{grant, artifact_id, version, sha256, size, recipient, one_time, uses_max,
issued_at, expires_at}`. Токен гранта возвращается один раз; в базе хранится только его
SHA-256-дайджест, поэтому чтение БД не выдаёт действующих возможностей.

Права проверяются **в момент выдачи** и ровно те же, что у чтения: tenant, capability `read`,
гейт переписки по проверенному claim `ctx`, потолок меток версии, затем lifecycle
(tombstone → `404`, legal hold → `403`). Выдать грант можно только на то, что можно прочитать
самому.

### `POST /v1/artifacts/OpenDownloadGrant` — отдать байты

Запрос `{grant}`; заголовок `Range` поддерживается как у `Download`. Ответ — поток байтов
точной версии (тот же `ETag = "<sha256>"`, те же `206`/`Content-Range`); ни редиректа, ни
presigned URL. Один вызов = одно потребление; счётчик двигается в той же транзакции, что
повторно проверяет права, поэтому отказ не «сжигает» попытку, а повтор не тратит чужой
остаток.

При потреблении проверяются: существование и срок гранта, `recipient` = субъект вызова,
tenant, tombstone, legal hold, неизменность запечатанной версии, текущий потолок меток,
`read`-capability. Гейт переписки **не** применяется: грант выдан внутри своей переписки и
назначает получателя — именно это позволяет прокси отдать файл, записанный в более раннем
чате, не получая токена этого чата.

## Коды и причины

Код определяет HTTP-класс, `reason` — машинно-различимое событие (поле `reason`, `transport.WriteError`).

| ситуация | HTTP | `code` | `reason` |
|---|---|---|---|
| нет версии / `latest` | 400 | `INVALID_ARGUMENT` | `grant_version_required` |
| версия не запечатана у этого артефакта | 403 | `FORBIDDEN` | `grant_version_mismatch` |
| чужой получатель при выдаче | 403 | `FORBIDDEN` | `grant_recipient_mismatch` |
| грант выдан другому получателю | 403 | `FORBIDDEN` | `grant_recipient_mismatch` |
| истёкший грант | 403 | `FORBIDDEN` | `grant_expired` |
| использованный/исчерпанный грант | 403 | `FORBIDDEN` | `grant_used` |
| токена нет / повреждён / чужой tenant | 403 | `FORBIDDEN` | `grant_unknown` |
| tombstone (выдача или потребление) | 404 | `NOT_FOUND` | `artifact_deleted` |
| legal hold (выдача или потребление) | 403 | `FORBIDDEN` | `artifact_legal_hold` |
| человеческий JWT без workload-сертификата | 403 | `FORBIDDEN` | — |
| диапазон за концом объекта | 416 | `range_not_satisfiable` | — (попытка не расходуется) |

## Путь браузера (рис. 7) — что нужно от владельцев маршрута

```
as-web → human edge (human JWT) → Space как прокси → Artifacts.DownloadGrant (workload-сертификат Space)
                                     ↓ тот же грант, Range
                                Artifacts.OpenDownloadGrant → поток → браузер
```

Требуется от Space/human edge (реализация не в этой ветке):

1. Маршрут входа человека вида `GET /u-ahp/v1/artifacts/{id}/versions/{version}/download`,
   доступный по human JWT, отдающий `Content-Type`, `Content-Length`, `Accept-Ranges` и тело
   проксируемого ответа.
2. Space вызывает `DownloadGrant` **своим** workload-сертификатом и task-токеном того же
   субъекта (`recipient` по умолчанию = субъект человека), затем сразу `OpenDownloadGrant`
   с тем же `Range`, который прислал браузер.
3. Прокси не кэширует содержимое, не пишет артефакты, не буферизует ответ и не отдаёт грант
   браузеру: грант живёт внутри одного запроса.
4. Заголовки `Location`/presigned-параметров на этом пути быть не может: байты идут через
   Artifacts.

## Ноги

Юниты/интеграция (реальные PostgreSQL + MinIO): `internal/artifacts/grant_test.go` (гейты
выдачи, потребление, срок, lifecycle, потолок меток) и
`internal/artifactsapi/grant_test.go` (то же поверх HTTP: коды, причины, `Range`, человеческий
JWT, чужая переписка).

Состав (изолированный стенд, реальный HTTP): `tools/download-grant-leg.sh` — положительный
контроль «байт-в-байт + sha256 точной версии», `Range 206`, чужая версия, чужой получатель,
повторное использование, истечение, неизвестный грант, tombstone, legal hold (и при выдаче,
и при потреблении), другая переписка того же субъекта, человеческий JWT прямо в Artifacts.

## Открытые обязательства, которые это НЕ закрывает

- `Export` из того же перечня стр.1582 по-прежнему отсутствует в дереве.
- Отдельное действие `grant` в allowlist плоскости данных (`cmd/ahp/role_data.go`) — одна
  строка, вне объявленных путей этой ветки; без него выдача идёт под `read`.
- `internal/executor/artifact_plane.go:122` пишет артефакты без `ContextID` (зона Execution):
  такие артефакты не достигаются каскадом и требуют `Get`/`Download` по id.

## Context scoping of the download route (ctx_scope claim)

The human download route resolves a row's reachability by subject, tenant and
deployment; the per-conversation context gate (`internal/artifacts/read_scope.go`)
is an ADDITIONAL bound that applies to task tokens.

- `ctx_scope` is an OPTIONAL, signed token claim (`omitempty`). Its only defined
  value is `authority`. Tokens minted before it existed omit it and are
  byte-for-byte unchanged.
- `ctx_scope=authority` is set ONLY by the server-side human -> Space ->
  scoped-task browser exchange mint (`internal/identityapi/browser.go`,
  `identity.Store.MintAuthorityTask`). It marks the per-session ingress
  authority whose read scope is the subject and tenant, not one conversation,
  so a human downloads their own artifacts across conversations.
- NOT INHERITED: delegated children and renewals mint through
  `identity.Store.MintTask`, which never sets `ctx_scope`; a child token is
  therefore still bound to exactly its own conversation.
- NOT REQUESTABLE: no input (exchange body, grant body, header) can set the
  claim; the exchange body forbids unknown fields, and the signer refuses any
  value other than `authority` and refuses it on human tokens.
- FORBIDDEN: the route never takes a context from query or browser parameters.
  A run task token of a DIFFERENT conversation of the same subject must return
  403 on Get/OpenRange/DownloadGrant.
