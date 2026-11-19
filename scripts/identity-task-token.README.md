# identity-task-token.mjs — штатный путь к scoped task token

Инструмент получает **scoped task token** тем же потоком, которым он устроен в продукте,
чтобы живые ноги (A→B, бюджеты дерева, Tool GW) не собирали креды вручную.

## Креды: один источник

Людей заводит оператор административным путём (продуктового эндпоинта заведения человека нет),
поэтому единственный источник кредов для этого инструмента — **0600 JSON-файл**
`AHP_ACCEPTANCE_USERS_FILE`. Пароль не передаётся ни в argv, ни в env-строке, ни в лог; файл с
групповыми/чужими битами чтения инструмент **отказывается читать** (тест это закрепляет).

Штатный путь к файлу приёмочного стенда:
`private/composed-backend/.acceptance-users.json` (username `accept`, tenant `djourney`, perms ⊇ `space:access,memory,tools`).

## Поток

1. `POST /u-ahp/v1/auth/login` `{username,password}` → HttpOnly сессионная cookie.
2. `POST /u-ahp/v1/auth/token` (только cookie + `x-ahp-session: 1`, без тела) → human access token (`aud=ahp-browser`).
3. `POST /v1/identity/control/exchange` `{human_token, task[, context, capabilities]}` — вызывающий обязан
   предъявить **собственный workload mTLS-клерт**, деплоймент которого назван сервером в
   `AHP_IDENTITY_EXCHANGER_DEPLOYMENTS` (канон: `space,djrun`) → `{token, task}`.

Ответ содержит `task` — **тот task, к которому токен привязан на самом деле** (при аттенуации
это суженный дочерний task, а не запрошенный; замороженный Tier-2 контракт). Дальше делегировать,
минтить и отменять надо по этому значению.

## Запуск

```bash
AHP_ORIGIN=https://127.0.0.1:29612 \
AHP_ACCEPTANCE_USERS_FILE=/path/.acceptance-users.json  AHP_TEST_USER_INDEX=0 \
AHP_TLS_CLIENT_CERT=/tmp/me/client.crt AHP_TLS_CLIENT_KEY=/tmp/me/client.key AHP_TLS_CA=/path/ca.crt \
node scripts/identity-task-token.mjs --task demo-1 --capabilities state,artifacts
```

Клерт выдаётся из dev CA: `AHP_CA_DIR=<stand>/infra/certs/ca AHP_CLIENT_CERT_DIR=/tmp/me bash infra/gen-client.sh <tenant> <deployment>`
(**tenant клерта обязан совпасть с tenant залогиненного человека** — иначе exchange отказывает
непрозрачным для внешнего наблюдателя `FORBIDDEN`).

## Вывод

Один JSON: `{ok, origin, subject, tenant, task, requested_task, token, headers:{Authorization}, human_expires_in, human_token_fp}`.

* `token` — bearer-креденциал: stdout нельзя логировать; для передачи в файл перенаправь в файл `0600`.
* Пароли и human token в вывод не попадают никогда (только `human_token_fp` — sha256-префикс для корреляции).
* Отказ exchange сохраняется машиночитаемо: `{ok:false, stage:"exchange", status, code, reason}` (`reason` — коды обмена: `azp_mismatch`, `token_use_invalid`, `exchange_permission_missing`, `capability_widening`, `grant_expired`, …).

## Коды возврата

`0` ок · `2` конфигурация · `3` login · `4` token · `5` exchange отказ · `6` транспорт.

## Тест

`node --test scripts/identity-task-token.test.mjs` — 4 теста на фикстуре, держащей те же
контрактные правила (cookie-сессия, отказ cookie-only bearer, отказ не-экчейнджеру).
Живой путь доказывается прогоном против стенда (`:29612`), не фикстурой.
