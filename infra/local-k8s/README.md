# Изолированный локальный профиль Kubernetes

Это проверяемый эксплуатационный профиль прототипа AHP: настоящий Go backend,
Agent Space runner, UI, PostgreSQL/pgvector, Redis, MinIO, Vault и OTel Collector.
Compute явно работает в режиме **MOCK**. Профиль не является production HA:
одна K3s Node в Docker, локальные PVC, одна failure domain, ручной unseal Vault.

## Границы и включённые сервисы

| Компонент | Что включено | Что этим профилем не доказывается |
|---|---|---|
| Identity, State, Run Coordinator, Execution | Реальные HTTP/mTLS, grants, PG, durable runner | Все tiers на отдельных физических узлах |
| Model, Cost | Настоящий provider из private ввода, quota/routing/ledger | Работа другого provider без отдельного прогона |
| Artifacts, Workspace | Настоящие versioned S3 bytes и PG metadata, права пользователей | Отказ единственного диска MinIO |
| Memory | `hybrid` и BAAI/bge-m3 при переданном embedding provider | Без него используется явно обозначенный lexical режим |
| Registry + Evals | Owner/API, Evals publication gate включаются вместе | Каталог и eval suites пусты: custom image / RunEval требуют настройки и прогона |
| Tools | Owner/API и Vault wiring | `example.invalid` — незаполненное MCP connection; это не работающий внешний tool |
| Triggers | Owner/API, ticker, subject-scoped secret paths | Внешний webhook провайдер не проверен этим профилем |
| OAuth | Store/prefix предусмотрены | Внешний OAuth provider не сконфигурирован |
| Compute | Явный MOCK marker | VM isolation, escape, cold start, density |
| Telemetry | Реальный OTLP Collector, allowlist, удаление content fields | Полнота сквозной трассы зависит от принятой ревизии runtime |

Backend, runner и human edge совместно размещены в одном Pod. Это одна workload
boundary, не изоляция между этими процессами. Только edge публикует порт 8080;
backend и runner слушают loopback. Edge/runner получают только клиентский mTLS
ключ; серверный ключ доступен backend. Ключ подписи browser grants остаётся у
backend. Kubernetes service account token смонтирован только в Vault Proxy,
с audience `vault`; приложение Kubernetes API credential не получает.

Профиль ограничивает Node до 3 GiB/2 CPU, load balancer до 128 MiB/0.25 CPU.
У Pod заданы requests/limits, restricted PSS, non-root, drop ALL,
read-only root filesystem. NetworkPolicy default deny проверяется реальными
соединениями к PG/MinIO/Vault. Разрешённый egress к модели/embedding — DNS IPv4
снимок в момент deploy; при смене адресов нужно повторно render/deploy.

## Подготовка и запуск

Нужны Docker (Linux containers), kubectl, Node 22+, Go версии из `go.mod`,
OpenSSL, Git и доступ к официальным upstream image/module registries.
Проверенный host — macOS arm64; build также выбирает linux/amd64 на x64 host.
В Docker должно оставаться место и RAM помимо уже работающих чужих проектов.

Все команды выполняются из корня monorepo или экспортированного `integration`.
В экспорте рядом должны быть `platform-runtime`, `frontend`, остальные Go
модули и общий `../go.work`. Build сохраняет ревизии каждого checkout.
При ином layout задайте `AHP_K8S_SOURCE_ROOT` (каталог с `cmd/ahp`) и
`AHP_K8S_FRONTEND_ROOT` (каталог с `index.html`). Dirty source build запрещён.

```sh
node scripts/local-k8s-cluster.mjs create
```

Helper загружает k3d **v5.9.0**, сверяет официальный checksum и создаёт только
`ahp-handbook-poc` с K3s **v1.34.12-k3s1**. При существующем кластере он
отказывается его заменять. Default kubeconfig/context не изменяется.
Используется только `private/deployment-complete/kubeconfig` и context
`k3d-ahp-handbook-poc`. API/edge/OTLP публикуются на **127.0.0.1**, не в LAN.
`cluster.yaml` — читаемое описание; helper использует проверенные CLI flags.

MinIO собирается из закреплённого официального исходника:

```sh
git clone --filter=blob:none https://github.com/minio/minio.git .local/minio-src
git -C .local/minio-src checkout --detach 9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a
node scripts/build-minio.mjs
node scripts/local-k8s-cluster.mjs images
```

Публичные pinned images загружаются через Node CRI, source-built image — через
k3d import. Проверяется descriptor внутри containerd: один exit code k3d не является
доказательством наличия всех образов. Runtime image получает публичный CA
bundle из pinned Node image; HTTPS у Go продолжает проверять сертификаты.

Подготовьте **вне Git** JSON-файл с правами 0600 следующей структуры:

```json
{
  "model": {"api_key": "<private>", "base_url": "https://provider.example/v1", "model": "<model-id>"},
  "embedding": {"api_key": "<private>", "base_url": "https://embedding.example/v1"}
}
```

`embedding` необязателен; его отсутствие не является semantic Memory acceptance.
Укажите путь, а не ключ в командной строке:

```sh
AHP_K8S_MODEL_SECRET_FILE=/secure/path/provider.json node scripts/local-k8s.mjs prepare
node scripts/local-k8s.mjs build
node scripts/local-k8s.mjs deploy
```

UI: **http://127.0.0.1:30222/**. Две синтетические учётные записи находятся в
`private/deployment-complete/credentials.json`. Не публикуйте этот файл,
`provider.json`, generated Secrets, unseal keys, kubeconfig или backups.
`build.json` содержит source revisions, binary SHA256, OCI descriptor и CRI
configuration digest (это разные виды digest).

## Проверки поведения

```sh
node scripts/local-k8s-network.mjs
node scripts/local-k8s-vault.mjs
```

Network test использует один реальный Pod: deny → разрешение операторской
меткой → отзыв. Vault test временно сокращает TTL собственного role, отзывает
свой token, проверяет automatic re-auth, renewal и max TTL rotation, затем
возвращает исходные TTL. Проверяется доступ к своему секрету и отказ 403 к чужому.
Root token используется только операторским helper, не монтируется в app.
Vault Proxy поддерживает renewable Kubernetes auto-auth; постоянного
непродлеваемого 6-hour token в runtime нет.

Browser Workspace acceptance создаёт исходные versioned файлы для следующих
проверок (Playwright должен быть установлен в отдельном node environment):

```sh
AHP_PLAYWRIGHT_MODULE=/absolute/path/to/playwright/index.mjs \
AHP_UI_LIVE_ORIGIN=http://127.0.0.1:30222 \
AHP_UI_WORKSPACE_USERS_JSON=private/deployment-complete/users.json \
AHP_UI_WORKSPACE_OUTPUT=private/deployment-complete/browser \
node scripts/aifactory-workspace-browser.mjs
node scripts/local-k8s-verify.mjs
```

`users.json` — приватный массив `{username,password,subject}` тех же двух users.
`local-k8s-verify` сверяет опубликованные bytes/SHA с browser ground truth,
проверяет чужого пользователя и выполняет настоящий новый LLM turn.
HTTP health/Ready само по себе не является этой проверкой.

## Backup / restore полного состояния

Backup является **холодным согласованным снимком** данного профиля, с downtime.
Helper останавливает всех app writers; после этого снимает pg_dump, останавливает
MinIO/Vault/Redis, архивирует их PVC и сохраняет scoped Secrets/identity.
MinIO переносится физически: повторный S3 PUT изменил бы VersionID и сломал
ссылки PostgreSQL на конкретные версии. Каждому файлу присваиваются размер и
SHA256; backup содержит секреты и должен храниться отдельно с шифрованием и ACL.
Файлы передаются потоково, не загружаются целиком в RAM. Collector evidence не
входит в продуктовый backup, он имеет отдельную ограниченную retention.

```sh
cp private/deployment-complete/product-report.json private/deployment-complete/baseline-product-report.json
node scripts/local-k8s-storage.mjs backup
AHP_K8S_NAMESPACE=ahp-poc-restore node scripts/local-k8s.mjs restore-target
AHP_K8S_NAMESPACE=ahp-poc-restore node scripts/local-k8s-storage.mjs restore
AHP_K8S_NAMESPACE=ahp-poc-restore node scripts/local-k8s.mjs unseal
```

Source остаётся остановленным после backup. Restore допускается только в
`ahp-poc-restore`, с пустыми target PVC/DB; существующие данные не перезаписывает. Перед копированием проверяет SHA256 и
точное совпадение образов PostgreSQL/MinIO/Redis/Vault с backup manifest; mismatch
останавливает restore до изменения target данных.
Повтор failed restore требует отдельной осознанной очистки только этого
непринятого target namespace. PostgreSQL/MinIO/Vault версии должны совпадать со
snapshot: это не процедура обновления формата хранения.

```sh
kubectl --kubeconfig private/deployment-complete/kubeconfig --context k3d-ahp-handbook-poc -n ahp-poc-restore scale deployment/cell --replicas=1
kubectl --kubeconfig private/deployment-complete/kubeconfig --context k3d-ahp-handbook-poc -n ahp-poc-restore rollout status deployment/cell --timeout=120s
AHP_K8S_NAMESPACE=ahp-poc-restore node scripts/local-k8s-storage.mjs verify-vault
kubectl --kubeconfig private/deployment-complete/kubeconfig --context k3d-ahp-handbook-poc -n ahp-poc-restore port-forward --address=127.0.0.1 svc/cell 30223:8080
```

В другом терминале:

```sh
AHP_K8S_ORIGIN=http://127.0.0.1:30223 \
AHP_K8S_PREVIOUS_REPORT=private/deployment-complete/baseline-product-report.json \
AHP_K8S_VERIFY_OUTPUT=private/deployment-complete/restore-product-report.json \
node scripts/local-k8s-verify.mjs
```

Проверяются старые версии файлов, сохранённый done turn и stream SHA, прежние
права пользователей, новый реальный Model turn. `verify-vault` сравнивает секрет
побайтно через hash и полные version metadata, а также чужой отказ 403.
Для переноса на другую машину: доставьте backup приватно в runtime/backup,
выполните `local-k8s-storage.mjs import` в пустом runtime, затем создайте cluster,
импортируйте images, соберите ту же source revision и выполните restore steps.
Browser baseline/report нужны отдельно для воспроизведения конкретного теста.

После проверки остановите восстановленные workloads, сохранив PVC, чтобы не
дублировать нагрузку; исходный профиль возобновляется `local-k8s.mjs deploy`.
Vault после рестарта требует `local-k8s.mjs unseal`; данные секретов при unseal
повторно не записываются. Локальная CA выписана на 30 дней: перед истечением
нужен контролируемый rollout новой CA/identity, автоматическая PKI rotation
этим профилем не реализована. Root/unseal keys не попадают в приложение.

## Синхронная реплика и отказ primary

Single PG+PVC и pg_dump не закрывают гарантию §2.5 о подтверждённой State записи.
Дополнительный drill выполняется **только в восстановленном namespace**:

```sh
node scripts/local-k8s-pg-sync.mjs setup
AHP_K8S_ORIGIN=http://127.0.0.1:30223 \
AHP_K8S_VERIFY_OUTPUT=private/deployment-complete/pg-sync-product-before.json \
node scripts/local-k8s-verify.mjs
node scripts/local-k8s-pg-sync.mjs failover
AHP_K8S_ORIGIN=http://127.0.0.1:30223 \
AHP_K8S_PREVIOUS_REPORT=private/deployment-complete/pg-sync-product-before.json \
AHP_K8S_VERIFY_OUTPUT=private/deployment-complete/pg-sync-product-after.json \
node scripts/local-k8s-verify.mjs
```

Это обычная PostgreSQL streaming replication с отдельным PVC, выделенным
replication role, `synchronous_commit=remote_apply` и именованным sync standby.
После подтверждённого AHP turn primary получает immediate abort без shutdown
checkpoint. Helper оставляет StatefulSet primary в 0 и ждёт удаления Pod;
только затем вызывает `pg_promote` и переключает Service. До 60 секунд опрашивает
настоящие login и чтение ранее подтверждённого turn/stream; сохраняет каждую
ошибку и время первого успеха отдельно от времени promotion. Повторная проверка
должна прочитать подтверждённый turn и выполнить новый. После promotion
резервирование временно отсутствует: перед эксплуатацией нужно восстановить
sync standby. Автоматического failover контроллера этот helper не заменяет.
Оба PostgreSQL находятся на одном физическом host: потеря host/диска и HA
не подтверждены. Нужны независимые узлы/диски и отдельный failure-domain test.

## Rollout и rollback

Изменение source требует нового clean build/image; обновите через `deploy`.
Профиль использует Recreate: краткий downtime ожидаем. Прежде чем rollback,
убедитесь в совместимости DB schema — image rollback не откатывает миграции.
Для собственного cell можно выполнить `kubectl ... rollout undo deployment/cell`
с теми же явными kubeconfig/context/namespace, затем повторить product checks.
Микросервисы текущего профиля собраны в единый runtime cell; production rollout
сервисов независимо и распределение по failure domains — отдельная топология.

Collector доступен процессам этого профиля как `collector:4317`, локальным
проверкам как `127.0.0.1:30317` (OTLP gRPC insecure, Mac loopback).
Allowlist применяется до file exporter; убраны span events, links, status text,
trace state, resource/scope schema URL и произвольные имена. Файлы ротируются 10 MiB × 3; trace/parent IDs
сохраняются. Проверять нужно экспорт реального span: validate config не ловит
все runtime OTTL type errors.

Источники поведения: [k3d config](https://k3d.io/stable/usage/configfile/),
[K3s NetworkPolicy](https://docs.k3s.io/networking/networking-services),
[Vault Kubernetes auto-auth](https://developer.hashicorp.com/vault/docs/agent-and-proxy/autoauth/methods/kubernetes),
[Vault Proxy](https://developer.hashicorp.com/vault/docs/agent-and-proxy/proxy/apiproxy),
[OTel transformations](https://opentelemetry.io/docs/collector/transforming-telemetry/),
[PostgreSQL synchronous replication](https://www.postgresql.org/docs/17/warm-standby.html),
[pg_basebackup](https://www.postgresql.org/docs/17/app-pgbasebackup.html).
