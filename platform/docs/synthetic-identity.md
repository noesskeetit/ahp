# Проверяемый synthetic-профиль Identity и Tool Gateway

Это локальная реализация границы из PDF §8.1: проверка подписи OIDC access assertion, текущие решения по контейнерам ресурсов и ограниченный обмен полномочий для MCP. Она **не подтверждает интеграцию с реальным Cloud.ru IAM**. Существующий `local` профиль остаётся значением по умолчанию; включение этого профиля — отдельное действие оператора.

## Что происходит при запросе

1. `synthetic-authority mint` выпускает access assertion только оператору в новый файл `0600`. Tenant и subject разрешены текущим правилом `tenant/login`. HTTP-выдачи от произвольного subject, регистрации пользователей и получения пароля нет.
2. AHP проверяет HTTPS discovery/JWKS, Ed25519/RSA/ECDSA-подпись, issuer, audience, expiry и `token_use=access`. Refresh, task и MCP credentials не подходят для публичного входа. Tenant/subject берутся из проверенного assertion; поля с ними в login body запрещены. Затем проверяется актуальное правило IAM на вход и открывается обычная отзываемая сессия AHP.
3. Identity проверяет существующий live grant и текущие правила tenant/deployment при создании, делегировании, выдаче и использовании task JWT. Workspace самостоятельно читает membership и идентификатор проекта, а IAM принимает решение по текущей роли. Явный IAM deny действует и на владельца. Artifact/Memory используют те же проверяемые membership facades.
4. Для зарегистрированного exchange-подключения Tool Gateway получает у Identity отдельное подписанное подтверждение на 60 секунд: специальный JWT `typ`, `token_use=exchange`, адрес token endpoint, конечные audience/resource/scope, connection, grant/epoch и actor Tool Gateway. Сырой human/task token внешнему token endpoint не передаётся.
5. Synthetic authority проверяет подпись подтверждения и заново читает grant и resource policy через API владельца Identity. Она выдаёт токен только зарегистрированному MCP и максимум до срока жизни подтверждения/grant. Tool Gateway повторно проверяет права после обмена. Токен живёт в памяти запроса и передаётся только MCP; в БД, логи, ответ агенту или VM он не записывается. После отказа обмена запасной статический credential не используется.

В synthetic-профиле Evaluate реализован внутри владельца Identity над его собственной PostgreSQL-схемой. Это не HTTP-протокол или API реального корпоративного IAM. Кеширования решений нет. Неисправность authority приводит к отказу.

## Поднять authority отдельно

Нужны Go, PostgreSQL этой изолированной платформы и **публичный** Ed25519 PEM-ключ текущего Identity signer. Приватный ключ платформы authority не получает. Пример использует свободный loopback-порт; если он занят, выберите другой. Все файлы с ключами и токенами должны находиться в игнорируемом каталоге вне публикуемых репозиториев.

```sh
go build -o /private/ahp-synthetic-authority ./cmd/synthetic-authority
/private/ahp-synthetic-authority init \
  --dir /private/ahp-synthetic-identity \
  --issuer https://127.0.0.1:29443 \
  --audience ahp-public \
  --ahp-kid dev-1 \
  --ahp-public-key /private/ahp-identity-public.pem
```

`init` создаёт отдельные ключи подписи и TLS, сертификат, конфигурацию. Каталог — `0700`, файлы — `0600`; существующие файлы не перезаписываются. Сертификат живёт семь дней. Для ротации создайте новый закрытый каталог и явно обновите доверие клиентов. Настройка issuer допускает только HTTPS, literal loopback IP и порт; wildcard и внешние интерфейсы отклоняются.

Добавьте в `config.json` только заранее доверенные назначения, например:

```json
"connections": {
  "trusted": {
    "audience": "mcp-trusted",
    "resource": "urn:mcp:trusted",
    "scope": "read"
  }
}
```

Передайте `AHP_DATABASE_URL` через закрытое окружение процесса; это БД **того же владельца Identity**, а не стороннего проекта. Запуск:

```sh
/private/ahp-synthetic-authority serve --dir /private/ahp-synthetic-identity
```

Authority обслуживает `/.well-known/openid-configuration`, `/jwks` и `/token` через TLS. В ответах discovery указан `ahp_profile=synthetic`. Секреты и токены в stdout не выводятся. В памяти процесса хранится ограниченный реестр использованных assertion JTI; после перезапуска он не сохраняется. Live grant/resource checks остаются обязательными после перезапуска. Это не гарантия durable exactly-once обмена.

## Провизия правил и отзыв

Оператор создаёт JSON-массив в закрытом файле. Каждое правило — точный tenant/subject/resource/action, без wildcard. Для начала необходимы `tenant/login`, а для каждого task capability — отдельные tenant/deployment правила. Для MCP дополнительно нужно `connection/use`.

```json
[
  {"tenant":"demo","subject":"alice","resource":{"kind":"tenant","id":"demo"},"action":"login","allowed":true,"expected_revision":0},
  {"tenant":"demo","subject":"alice","resource":{"kind":"tenant","id":"demo"},"action":"tools","allowed":true,"expected_revision":0},
  {"tenant":"demo","subject":"alice","resource":{"kind":"deployment","id":"agent"},"action":"tools","allowed":true,"expected_revision":0},
  {"tenant":"demo","subject":"alice","resource":{"kind":"connection","id":"trusted"},"action":"use","allowed":true,"expected_revision":0}
]
```

```sh
AHP_AUTH_PROFILE=local AHP_IAM_PROFILE=synthetic \
  AHP_SYNTHETIC_IAM_RULES_FILE=/private/iam-rules.json \
  /private/ahp --role provision-synthetic-iam
```

`expected_revision=0` создаёт отсутствующее правило. После создания revision равна 1. Чтобы отозвать доступ, внесите `allowed=false` и текущую revision; stale update отклоняется. Команда применяет CAS отдельно для каждой записи, поэтому при ошибке поздней записи предыдущие уже применены. Обычный старт сервиса правил не добавляет и отзыв не отменяет. Удаление tenant/subject удаляет правила и запрещает их повторное создание для отозванного principal.

Проект получает разрешения только из membership, прочитанного Workspace на текущем запросе. IAM сохраняет роли viewer/editor/owner и поддерживает точный deny на UUID проекта и действие, например `project/read`. Дополнительный allow без membership доступ не создаёт. Ни произвольный resource ID от клиента, ни просроченный список проектов не заменяют владельческий фильтр.

## Настроить AHP

```sh
AHP_AUTH_PROFILE=synthetic-oidc
AHP_IAM_PROFILE=synthetic
AHP_OIDC_ISSUER=https://127.0.0.1:29443
AHP_OIDC_DISCOVERY_URL=https://127.0.0.1:29443/.well-known/openid-configuration
AHP_OIDC_AUDIENCE=ahp-public
AHP_OIDC_CA_FILE=/private/ahp-synthetic-identity/tls.pem
AHP_ENABLE_BROWSER=1
```

Операторская выдача:

```sh
/private/ahp-synthetic-authority mint \
  --dir /private/ahp-synthetic-identity \
  --tenant demo --subject alice --out /private/alice-access.jwt
```

Новый файл содержит access assertion на пять минут. Login API принимает `{"access_token":"..."}` и заголовок `X-AHP-Session: 1`; существующее поле `id_token` сохранено как совместимый alias, но требует того же access-purpose. UI пока может продолжать пользоваться local login: `AHP_AUTH_PROFILE=local` совместим с явным `AHP_IAM_PROFILE=synthetic`; тогда вход тоже требует текущего `tenant/login`.

Подтверждение IdP не добавляет capabilities пользователю: действующие entitlement/grant-механизмы AHP сохраняются. OIDC JIT-профиль создаёт только локальное соответствие доказанному tenant/subject; роли и permissions назначаются оператором существующей процедурой.

В synthetic-профиле привилегированные Identity HTTP-вызовы требуют **полный SPIFFE ID**, например `spiffe://ahp/tenant/demo/deployment/space`. Установите нужные списки: `AHP_IDENTITY_EXCHANGER_SPIFFE_IDS`, `AHP_IDENTITY_DELEGATOR_SPIFFE_IDS`, `AHP_IDENTITY_CONTROL_SPIFFE_IDS`. Пустой список запрещает вызов. Старые `*_DEPLOYMENTS` не дают обхода. Для существующего local-профиля поведение не меняется.

Обмен ToolGW использует owner facade `role=all`, с существующим signer Identity; отдельный ToolGW без этого владельца не получает функцию выдачи и отказывает при конфигурации exchange. Это сознательная граница текущего PoC, а не готовый межсервисный RPC.

## Подключение MCP

В операторском `AHP_TOOLS_CONFIG` сохраните обычные `connections`, `tools` и `egress`; дополнительно задайте:

```json
"exchange": {
  "trusted": {
    "token_url": "https://127.0.0.1:29443/token",
    "audience": "mcp-trusted",
    "resource": "urn:mcp:trusted",
    "scope": "read",
    "synthetic_loopback_ca_file": "/private/ahp-synthetic-identity/tls.pem",
    "egress": {"AllowedPathPrefixes":["/token"]}
  }
}
```

Локальное исключение работает только при явно выбранном synthetic IAM profile: конкретный HTTPS literal-loopback endpoint, отдельный доверенный CA, deadline 8 секунд, response cap 64 KiB, никакого redirect/proxy/DNS. HTTP, имя `localhost`, другой CA и обычный профиль отклоняются. **Это тестовый маршрут, не доказательство production egress.** `pkg/netpolicy` и обычные connectors не изменены. Доступ к самому MCP по-прежнему ограничивает его обычный зарегистрированный egress; loopback-исключение касается только token endpoint.

MCP должен проверять подпись authority/JWKS, точные issuer/audience/resource/scope и `token_use=mcp_access`; это не AHP task JWT. Для обычного token endpoint уберите `synthetic_loopback_ca_file` и используйте штатный netpolicy. Scope ответа должен точно совпадать с зарегистрированным. Одно connection нельзя одновременно зарегистрировать как personal OAuth и token-exchange. Существующий personal OAuth/PKCE остаётся отдельным credential mode.

## Проверки и границы

На настроенном изолированном PostgreSQL/MinIO/Vault-контуре:

```sh
go test -count=1 ./cmd/synthetic-authority
go test -count=1 ./internal/identity ./internal/identityapi ./internal/workspace ./internal/tools ./internal/tools/toolsapi ./pkg/auth
go test -tags ahp_data,ahp_exec -count=1 -run 'TestSynthetic|TestExactWorkload' ./cmd/ahp
```

Standalone smoke собирает и запускает настоящий CLI отдельным процессом, проверяет TLS, private output, публичный вход и exchange/revoke/replay/cross-audience отрицательные случаи. Другие проверки используют реальные PostgreSQL, mTLS и контролируемые issuer/IAM/MCP. Они не доказывают реальный корпоративный IAM, LLM E2E, реальную microVM или готовность всей платформы.

Детальные новые grant ceilings на project/file/memory из PDF ID-03 здесь не изобретались: существующие ограничения сохранены. Развёртывание отдельными owner-сервисами, production key rotation/JWKS refresh и корпоративная схема IAM остаются отдельными интеграционными решениями. Стандарт обмена: [RFC 8693](https://www.rfc-editor.org/rfc/rfc8693); разделение назначения JWT: [RFC 8725](https://www.rfc-editor.org/rfc/rfc8725).
