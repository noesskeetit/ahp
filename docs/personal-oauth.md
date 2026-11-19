# Персональный OAuth Tool & Credential Gateway

Источник требований — архитектура AHP/Agents Space rev3.2, 64 страницы: §5.4
(согласие только субъекта), §8.1 (PKCE, Vault, single-flight/CAS), §8.2
(private label), §8.4 (egress и SSRF). Реализация находится в `internal/tools`.

При вызове зарегистрированного персонального подключения без действующего grant
`Call` возвращает HTTP 409 `AUTH_REQUIRED`, `status: auth-required`, и создаёт
запрос для владельца. Consent URL агенту не возвращается. Human API находится
за существующим Space proxy, human JWT и mTLS до tools gateway. Tenant/subject
берутся из проверенного principal, не из JSON. Пользователю требуется явное
разрешение `tools:connect` (отдельно от `tools:approve`).

| POST human route | Body | Result |
| --- | --- | --- |
| `/u-ahp/v1/tools/OAuthConnections` | `{}` | Личные состояния подключений |
| `/u-ahp/v1/tools/OAuthBegin` | `{"connection":"name"}` | `authorize_url` + HttpOnly nonce-cookie |
| `/u-ahp/v1/tools/OAuthCallback` | `{"state":"...","code":"..."}` или `error` | Одноразовое завершение, без токенов |
| `/u-ahp/v1/tools/OAuthRevoke` | `{"connection":"name"}` | Локальный отзыв, remote revoke при наличии endpoint, удаление Vault |

Внутренние имена совпадают под `/v1/tools/`. Task JWT не имеет доступа к этим
операциям. Gateway проверяет JTI через Identity BrowserTokenActive и совпадение
tenant/subject с живой сессией. Logout, refresh rotation и отзыв субъекта закрывают
все четыре OAuth операции до истечения JWT.
Cookie `ahp_oauth_<state hash>` живёт 15 минут, HttpOnly, SameSite=Lax,
Secure для HTTPS redirect. Прокси переносит только эти cookies для callback и
ответа Begin/Callback. UI удаляет `code/state` из адресной строки до API-запросов,
не сохраняет токены, PKCE или callback в browser storage. Redirect URI должен
вести на корень UI. У production URI обязателен HTTPS; loopback HTTP разрешён
для изолированного локального стенда.

`AHP_ENABLE_TOOLS=1`, существующие Vault endpoint/runtime token и
`AS_TOOLS_URL` дополняются следующими полями `AHP_TOOLS_CONFIG`:

```json
{
  "connections": {"personal": "https://mcp.example.net/mcp"},
  "tools": {"personal.read": {"Name": "read", "Connection": "personal"}},
  "oauth": {
    "personal": {
      "authorize_url": "https://accounts.example.net/authorize",
      "token_url": "https://accounts.example.net/token",
      "revoke_url": "https://accounts.example.net/revoke",
      "client_id": "registered-public-client-id",
      "client_secret_path": "secret/data/service/oauth-client",
      "redirect_uri": "https://portal.example.net/",
      "scopes": ["read"],
      "egress": {
        "AllowedHosts": ["accounts.example.net"],
        "AllowedMethods": ["GET", "POST"],
        "AllowedPathPrefixes": ["/authorize", "/token", "/revoke"]
      }
    }
  },
  "egress": {
    "personal": {
      "AllowedHosts": ["mcp.example.net"],
      "AllowedMethods": ["GET", "POST", "DELETE"],
      "AllowedPathPrefixes": ["/mcp"]
    }
  }
}
```

Домены в примере — placeholders; endpoints/scopes регистрирует оператор.
`client_secret_path` можно опустить для public PKCE client. Для confidential
client Vault хранит `client_secret`; используется `client_secret_basic`.
Провайдеры без `expires_in` или с token_type, отличным от Bearer, отказываются.
Endpoint discovery и динамические URL из браузера не поддерживаются: endpoints
пинятся в конфигурации, поэтому discovery не создаёт отдельного SSRF-пути.

`AHP_TOOLS_OAUTH_VAULT_PREFIX` — service-owned KV-v2 prefix, по умолчанию
`secret/data/ahp/tools-oauth`. Для локального финального стенда оператор задаёт
`secret/data/root-final-stand/tools`. Под ним создаются opaque tenant/request
paths. Runtime policy требует чтения и CAS-записи этих data paths и удаления
соответствующих metadata paths. Root Vault token не требуется. Секреты клиента
провайдера требуют отдельного read-only разрешения. OAuth и обычные статические
Vault connections могут сосуществовать; отказ OAuth никогда не вызывает fallback
к статическому bearer.

State и cookie nonce хранятся в БД только как SHA-256. PKCE verifier и access /
refresh tokens — только Vault. В PostgreSQL до каждой записи в Vault фиксируется
инвентарь paths. Callback атомарно помечается consumed до token POST. При
потере ответа используется новое человеческое подключение; старый code не
повторяется. Refresh сериализуется advisory lock по tenant+subject+connection
между репликами. Admission ограничен MaxConns исходного pool. Соединение session
lock отделяется через pgxpool Hijack до ожидания lock и Identity I/O: ни владелец,
ни contender не удерживает shared-pool lease. Каждый исход закрывает detached
connection, включая отмену запроса. Это добавляет максимум MaxConns одновременных
соединений на OAuthManager сверх исходного pool; ожидающие admission соединений
не занимают. Live grant и client-secret проверяются до `refresh_unknown`, marker
записывается непосредственно перед внешним POST. После ротации
Vault CAS и SQL revision CAS переводят grant в `active`. Crash, ambiguity или
CAS conflict не допускают повторного использования старого refresh token.

Credential Resolve повторно проверяет live task grant и требует успешного
Identity AddLabels(subject) перед возвратом bearer. Без live Identity/private
label sink OAuth composition не запускается. Provider/MCP HTTP проходят
netpolicy allowlist домена, метода, пути; запрещённые адреса проверяются после
DNS. Token/revoke endpoints и персональный MCP не следуют redirects.
Зарегистрированный private MCP ограничивается точным `PrivateHosts` entry;
loopback/link-local/metadata не разрешаются даже через этот список.

Revoke сначала durably запрещает локальное использование и pending callbacks.
Затем отзывает refresh/access tokens у provider (если revoke_url зарегистрирован)
и удаляет все версии из Vault. Ошибка внешнего revoke не возвращает local grant
в active; повторный вызов продолжает cleanup. Без revoke_url гарантия относится
к AHP и Vault; remote SaaS grant необходимо отзывать через provider UI.

`tools.Service.FenceTenant/DeleteTenant` объединяют operations barrier, OAuth
barrier и очищение approvals/policies/audit. Неопределённые эффекты блокируют
purge до reconciliation. Permanent tombstones остаются; OAuth deletion сначала
удаляет все Vault metadata, затем SQL inventory. Late callback/refresh не может
воссоздать удалённого tenant. Повторная cleanup после сбоя допустима.

Проверки: `go test -race ./internal/tools ./internal/tools/toolsapi` при явных
`AHP_TEST_DATABASE_URL`, `AHP_VAULT_ADDR`, `AHP_VAULT_DEV_TOKEN`. OAuth integration
использует реальные PostgreSQL/Vault, HTTP OAuth fixture, реальный mTLS между
Space proxy и gateway. Fixture DNS/dial seam направляет allowlisted hostname
в локальный test listener; это не проверка внешнего SaaS. Опциональный
`AHP_OAUTH_BROWSER_SCRIPT` запускает Chromium workflow на том же harness.

Справочники протокола (не дополнительные требования к продукту):
[PKCE RFC 7636](https://www.rfc-editor.org/rfc/rfc7636),
[OAuth Security BCP RFC 9700](https://www.rfc-editor.org/rfc/rfc9700),
[Vault KV v2 API](https://developer.hashicorp.com/vault/api-docs/secret/kv/kv-v2).
