# Compute plane — классы S/C/X, компьютеры и fencing

Реализация продуктового Compute plane AHP Agents Space. Единственный источник
продуктовых требований и приёмки — `ahp-agents-space-architecture (2) (1).pdf`,
revision 3.2, 64 стр., SHA256 `044e045b73b7c34b642bd3c9eb5701b93eed470873a1e0a44e8b1d024821a7e2`
(сверено директором; PDF в Git не копируется, читается вне репозитория).

Модуль: `github.com/ahp-platform/ahp`. Ветки контракта: `internal/compute`,
`cmd/compute-host`, `sdk/go/compute`, `infra/compute`, `tests/compute`, `docs/compute.md`.

## Назначение

Compute plane предоставляет изолированные «компьютеры» — вычислительные единицы,
на которых выполняются сессии, команды, PTY-терминалы и сборки. Каждый компьютер
принадлежит одному классу доверия (S/C/X), одному профилю жизненного цикла
(ephemeral/project/personal) и изолирован по объёмам, сети и квотам в соответствии
с классом. Plane гарантирует durability операций (command ledger), отсутствие
«половинчатых» состояний (fencing, environment rollback) и воспроизводимость
(project copy-on-write, snapshots, пакетный артефактный экспорт).

## Классы доверия S / C / X

Класс определяет жёсткий потолок ресурсов и политику исходящей сети. Запрос может
только сузить квоту относительно дефолта класса; для класса X значения дополнительно
клампятся к жёстким потолкам (`ResolveQuota`), поэтому вызывающий не может расширить
конверт недоверенного кода. Дефолты (`policy.go` `ClassDefaults`):

| Класс | Назначение | CPU (mCPU/s) | Mem (MB) | PID | Open files | Storage (MB) | TTL |
|-------|------------|-------------|----------|------|-----------|--------------|-----|
| S | Trusted multi-tenant Agents Space runtime (собственный пул нод) | 8000 | 16384 | 65536 | 1048576 | 204800 | 24h |
| C | Developer code per deployment (сборки) | 4000 | 8192 | 32768 | 262144 | 51200 | 4h |
| X | Arbitrary untrusted code в microVM | 2000 | 4096 | 8192 | 65536 | 10240 | 1h |

`MaxPTYsPerWorkspace = 64` на каждый класс.

Политика исходящей сети (`EgressPolicy`):
- **S** — allowlist `proxy.golang.org`, `pypi.org`, `registry.npmjs.org`; без приватных хостов.
- **C** — та же allowlist публичных реестров.
- **X** — deny-all внутри песочницы; «слепой» egress только через внешний Egress Proxy вне VM.

Файловая изоляция (`JailRoots`): personal → постоянный HOME-том сотрудника
(`<base>/home/<subject>`); project → том воркспейса `<base>/workspace/<ws_id>`;
ephemeral → одноразовый scratch-root. Всё остальное запрещено бэкендом
(chroot/bind/unshare в host-бэкенде; собственные монтирования в microVM-бэкенде).

## Профили жизненного цикла

- **ephemeral** — чистая VM на сессию; состояние выбрасывается по завершении.
- **project** — read-only базовая ревизия плюс copy-on-write рабочий слой.
- **personal** — компьютер сотрудника: постоянный HOME-том плюс проектный слой.

Инвариант безопасности (`PersonalRequiresTrustedClass`): **personal-компьютер
допускается только для класса S** (trusted). X/C не могут «дотянуться» до личного
компьютера сотрудника. Класс X additionally не получает доступа в realm
(`ValidatePolicy`).

## Механизмы корректности

### Incarnation + volume fencing
`AcquireIncarnation` регистрирует запуск инкарнации компьютера на бэкенде/ноде с
`vmgen_id`. `CurrentFence`/`CheckFence` хранят и проверяют номер забора тома:
старая (зombie) инкарнация не может писать в том после того, как его забрал новый
владелец. `StopIncarnation` завершает инкарнацию. Команда, чей забор устарел,
получает статус `fenced`.

### Activity leases
`AcquireLease`/`ReleaseLease`/`ActiveLeases` — ограниченные по TTL мьютексы активности
(attach, PTY, writer). Взаимно исключающие держатели; PTY-лизинг ограничен
`MaxPTYsPerWorkspace`.

### Command ledger (UNKNOWN / restart / cancel)
Каждая команда_durably_ пишется в ledger перед диспатчем. Статусы (`types.go`):
`accepted → dispatched → {succeeded|failed|cancelled|lost|side_effect_unknown|fenced}`
(терминальные — последние шесть). Ключевые гарантии:
- **UNKNOWN**: если команда дошла до бэкенда, а исход неизвестен (разрыв/таймаут),
  ledger фиксирует `side_effect_unknown` и отказывается считать её «не выполненной» —
  повторный автоповтор с побочным эффектом запрещён.
- **restart**: после рестарта хоста незавершённые записи восстанавливаются из ledger;
  `lost` — команда, потерянная при падении без подтверждения эффекта.
- **cancel**: `cancel` до выполнения → `cancelled`.
- **fence**: `Fence` переводит спорную команду в `fenced`.

### Snapshots, export ArtifactRef
`RecordSnapshot` фиксирует снапшот (kind `directory` или `memory`) с путём, дайджестом
и размером. Экспорт наружу — `ArtifactExporter.Export`: Compute **не** пишет объекты в
хранилище и **не** майнит собственные `ArtifactRef`. Он отдаёт тело внешнему Artifacts
сервису, который возвращает запечатанный (sealed) `ArtifactRef` владельца. При отсутствии
настроенного эндпоинта экспорт отключён (`nil`); при ошибке транспорта/сервиса —
`Unavailable`, фабрикация ссылки запрещена.

### Package / environment rollback
Ревизии окружения (`env_revisions`): `EnsureEnvironment` идемпотентно создаёт базовую
applied-ревизию. `BeginEnvironmentInstall` берёт writer-лизинг (конкурентный писатель →
`WORKSPACE_CONFLICT`) и создаёт `pending`-ревизию с parent = текущий head.
`FinishEnvironmentInstall(ok=false)` помечает ревизию `rolled_back` и оставляет head
равным parent — вызывающий наблюдает **rollback, а не «полуустановленное» окружение**.

### Project copy-on-write
`CommitWorkspace` для project-компьютеров пишет CoW-дельту, опираясь на ожидаемую
базовую ревизию. При устаревании базы открывается conflict-ветка и возвращается
`WORKSPACE_CONFLICT` с ней. CoW-коммиты допустимы только для project-компьютеров.
На Linux дельта ускоряется reflink (`FICLONE`, `reflink_linux.go`); на иных ОС — fallback.

## Запуск `compute-host`

```
compute-host version     # версия и commit сборки
compute-host migrate     # применяет схему ahp_compute к БД
compute-host serve       # mTLS gRPC/HTTP API-сервер
compute-host supervise   # Linux-супервизор бэкенда (UNIX-сокет)
```

Неизвестная/пустая команда → usage в stderr, exit 2. Ошибка команды → stderr
`compute-host: <err>`, exit 1 (кроме graceful `context.Canceled`).

### serve
Поднимает mTLS-API: SIGINT/SIGTERM- graceful shutdown (10s), миграция схемы при старте,
host-бэкенд для классов S и C, реестр бэкендов, артефактный экспортёр из env, верификатор
токенов и authorization-hook. Класс **X** подключается только если задан
`AHP_COMPUTE_FC_SOCKET` (тогда поднимается `MicroVMBackend("firecracker", socket)`);
иначе **X fail-closed** (не выполняется на host).

### supervise (Linux-супервизор бэкенда)
`supervise` — реальный Linux-хостовой супервизор, слушающий UNIX-сокет. Runtime
выбирается по `AHP_COMPUTE_RUNTIME` (по умолчанию `firecracker`):
- `firecracker`: требует GOOS=linux, наличия `/dev/kvm` и полного набора конфигурации
  (`AHP_FC_BINARY/KERNEL/ROOTFS/DATADIR`). При отсутствии любого условия возвращается
  типизированная ошибка `Unavailable` (см. ниже), **не** симуляция VM.
- `kata`: in-tree `VMRuntime` не поставляется — возвращается `Unavailable`
  («supply one to NewSupervisor»).
- неизвестный `AHP_COMPUTE_RUNTIME` → `invalid`.

Пример fail-closed на macOS (без KVM):
```
compute-host: compute: backend "firecracker" unavailable: the compute supervisor requires Linux; this host is darwin
```

## Конфигурация (переменные окружения)

| Переменная | Где | Назначение |
|------------|-----|------------|
| `AHP_DB_URL` / `DATABASE_URL` | serve, migrate | DSN PostgreSQL (`AHP_DB_URL` приоритетнее) |
| `AHP_COMPUTE_LISTEN` | serve | адрес mTLS-листенера (обязательна) |
| `AHP_COMPUTE_TLS_CERT` | serve | путь TLS-сертификата сервера |
| `AHP_COMPUTE_TLS_KEY` | serve | путь TLS-ключа сервера |
| `AHP_COMPUTE_TLS_CA` | serve | CA-пул клиентских сертификатов (mTLS обязателен) |
| `AHP_COMPUTE_JAIL_BASE` | serve | базовый каталог jail-root томов |
| `AHP_COMPUTE_FC_SOCKET` | serve | UNIX-сокет Firecracker-бэкенда; без него X fail-closed |
| `AHP_COMPUTE_NODE` | serve | идентификатор ноды |
| `AHP_COMPUTE_ALLOW_ALL_AUTHZ` | serve | `=1` включает allow-all authz; иначе refuse-all |
| `AHP_TOKEN_ISSUER` | serve | ожидаемый issuer токенов (обязателен) |
| `AHP_TOKEN_KIDS` | serve | CSV ключей подписи (обязателен) |
| `AHP_TOKEN_PUBKEY_<KID>` | serve | base64(std) ed25519 pubkey для `<KID>` |
| `AHP_COMPUTE_SUPERVISOR_SOCKET` | supervise | UNIX-сокет супервизора (обязателен) |
| `AHP_COMPUTE_RUNTIME` | supervise | `firecracker` (default) \| `kata` |
| `AHP_FC_BINARY` | supervise | путь к бинарнику Firecracker |
| `AHP_FC_KERNEL` | supervise | путь к guest-ядру |
| `AHP_FC_ROOTFS` | supervise | путь к rootfs |
| `AHP_FC_DATADIR` | supervise | каталог данных VM |
| `AHP_FC_VCPUS` / `AHP_FC_MEM_MB` | supervise | vCPU / память гостя |
| `AHP_FC_GUEST_AGENT` | supervise | бинарник guest-agent внутри VM |
| `AHP_FC_TAP` / `AHP_FC_GUEST_MAC` | supervise | TAP-интерфейс / MAC гостя |
| `AHP_ARTIFACTS_URL` | serve | эндпоинт Artifacts; пусто → экспорт отключён |
| `AHP_ARTIFACTS_TOKEN` | serve | токен к Artifacts |
| `AHP_REDIS_ADDR` | serve | адрес Redis лиз-слоя (`host:port`); по умолчанию `127.0.0.1:16379` |
| `AHP_REDIS_PASSWORD` | serve | пароль `requirepass` этого Redis; пусто → `AUTH` не отправляется |
| `REDIS_PASSWORD` | serve | алиас `AHP_REDIS_PASSWORD` (общая конвенция плоскостей) |
| `AHP_TEST_DATABASE_URL` | тесты | изолированная БД тестов (только allowlist `ahp_hive_*`) |

### Redis: адрес и пароль — разные переменные

Redis этой плоскости описывается **двумя** переменными и только ими:

- `AHP_REDIS_ADDR` — только `host:port`. Формат `addr?password=...` и любые
  query-параметры не изобретаются: адрес не является секретом и может попасть в
  логи, пароль — нет.
- `AHP_REDIS_PASSWORD` — сам пароль, единственный канал поставки которого есть
  environment процесса (runtime-injection по требованию PDF: секреты не живут в
  коде, конфигурационных файлах и git). Читает его `compute.RedisPasswordFromEnv`
  (`internal/compute/redis.go`), которая сначала смотрит `AHP_REDIS_PASSWORD`,
  затем `REDIS_PASSWORD`; `REDIS_PASSWORD` поддержан и в execution-плоскости
  (`internal/gateway`), чтобы один и тот же секрет не дублировали под двумя
  именами.

Пароль отправляется как `AUTH` на каждом новом соединении до любой другой
команды, поэтому пул работает с Redis, запущенным с `requirepass`; без пароля
клиент `AUTH` не отправляет вообще (неаутентифицированный Redis не меняется в
поведении). Пароль не попадает в текст ошибки.

Плоскость execution использует отдельную переменную `REDIS_URL` формата
`redis://[user:pass@]host:port[/db]` для pub/wait-нотифайера; `rediss://`
(TLS) в ней не поддерживается. Это другой клиент и другой контракт, а не второе
имя для `AHP_REDIS_ADDR`.

### Имена бакетов и DSN: не путать между плоскостями

Один и тот же MinIO/S3 и один и тот же PostgreSQL разные плоскости продуктов
описывают разными именами префиксных переменных. Совпадение значений —
свойство стенда, а не продукта: если две плоскости пишут в один бакет, значения
нужно задать одинаковыми явно.

| Переменная | Кто читает | Что именует |
|------------|------------|-------------|
| `AHP_S3_BUCKET` | `cmd/compute-host` (compute) | бакет compute-экспорта и workspace-объектов; по умолчанию `ahp-workspace` |
| `AS_ARTIFACT_BUCKET` | `cmd/as-runner` (Space) | бакет артефактов Space; обязателен вместе с группой `AS_DATA_DATABASE_URL` + `AHP_S3_ENDPOINT` + `AHP_MINIO_ROOT_USER` + `AHP_MINIO_ROOT_PASSWORD`, неполная группа → fail closed |
| `AHP_EXEC_ARTIFACT_BUCKET` | `internal/executor` (execution) | бакет артефактов исполнителя; по умолчанию `ahp-exec-artifacts` |
| `AHP_S3_ENDPOINT` | compute, Space, execution | общий endpoint вида `http(s)://host:port` (TLS выбирается схемой URL) |
| `AHP_S3_REGION` | `cmd/ahp`, `internal/executor` | регион подписи S3; пусто → значение по умолчанию клиента |
| `AHP_MINIO_ROOT_USER` / `AHP_MINIO_ROOT_PASSWORD` | compute, Space, execution | статические креды объектного хранилища |

Аналогично с DSN PostgreSQL:

| Переменная | Кто читает | Что именует |
|------------|------------|-------------|
| `AS_DATA_DATABASE_URL` | `cmd/as-runner` | DSN схемы Data/Artifacts Space; приоритет над `DATABASE_URL` и `AHP_DB_URL` |
| `AHP_DB_URL` / `DATABASE_URL` | `cmd/compute-host`, тесты | DSN плоскости compute (`AHP_DB_URL` приоритетнее) |
| `AHP_TEST_DATABASE_URL` | тесты всех пакетов | DSN тестовой БД; часть пакетов дополнительно принимает `DATABASE_URL` как fallback |

`AHP_TEST_DATABASE_URL` указывает на БД, внутри которой работают тесты. Правила
про изолированность не отменяются: `scripts/ahp-env.mjs` переписывает имя БД в
каноническом DSN на лончерский allowlist `ahp_hive_*` и отказывает на любом
другом имени. Поверх этого пакетные наборы `internal/identity` и
`internal/state` на каждый прогон тестового бинаря создают свою временную БД в
том же кластере (`ahp_idpkg_*`, `ahp_sttest_*`) и удаляют её при выходе процесса:
их DDL берёт AccessExclusive-локи, из-за чего второй прогон на общей БД уходил в
deadlock с первым. Роль поэтому должна иметь `CREATEDB`; если создать БД нельзя,
наборы откатываются на указанную БД (см. `isolation_test.go` этих пакетов).

Секреты в перечисленных переменных — только значения runtime-инъекции: ни одна
из них не записывается в код, фикстуры или репозиторий.

## Инфраструктура бэкенда (`infra/compute/compute-supervisor`)

- `kvm-check.sh` — проверка готовности Linux/KVM-стенда (на macOS exit=1 NOT READY).
- `setup.sh` — подготовка хоста (root-guard, `modprobe kvm`, udev-правила, `/run/ahp`).
- `acceptance-kvm.sh` — 3-стадийный приёмочный раннер (см. ниже).
- `firecracker.env.example`, `kata.env.example`, `compute-supervisor.service`, `README.md`.

Стенд KVM берётся у параллельного подрядчика `kvm-lab` (`labs/kvm`) через форум,
не дублируется.

## Статус верификации

### VERIFIED (проверено на доступном target runtime — macOS darwin/arm64 + Go 1.26.2 + PostgreSQL)
- `go build ./internal/compute/... ./cmd/compute-host/...` — успешно.
- Unit + integration тесты `./internal/compute` и `./tests/compute` — зелёные (56 PASS
  в `tests/compute`), включая `-race`.
- Host-исполнение классов S/C: реальный spawn процесса с изоляцией jail-root, квоты, egress.
- Store state machine + incarnation/volume fencing: проверка забора, отказ stale-инкарнации.
- Activity leases: взаимоисключение держателей, лимит PTY, TTL.
- Command ledger: `side_effect_unknown`, `lost`, `cancelled`, `fenced`, восстановление после рестарта.
- mTLS-API + верификация ed25519-токенов; authz refuse-all по умолчанию.
- Quota-слияние и кламп класса X (только сужение).
- Project CoW: `WORKSPACE_CONFLICT` при устаревшей базе.
- Environment/package rollback: `rolled_back` с сохранением parent-head.
- Artifact exporter: контракт + httptest; фабрикация `ArtifactRef` запрещена.
- Fail-closed гейты (5 PASS): отсутствие KVM/config → `Unavailable`, сокет не связывается,
  rc=1, stderr «Unavailable».
- `kvm-check.sh`/`acceptance-kvm.sh` на Mac: fail-closed (Stage C — KVM-skip).

### NOT_VERIFIED / внешний гейт (честно зафиксировано, НЕ симулируется)
- **KVM / Firecracker X-runtime**: на macOS отсутствует `/dev/kvm` → `Unavailable`, VM
  не запускается и не имитируется. Требуется Linux+KVM target (стенд `kvm-lab`).
- **Живой Artifacts export**: интерфейс и httptest зелёные; экспорт в реальный Artifacts
  сервис не выполнялся (эндпоинт внешнего контракта).
- **PTY на Linux**: на macOS PTY-путь fail-closed; фактический PTY-over-guest требует Linux.
- **`acceptance-kvm.sh` Stage C** (живой Firecracker smoke): пропущен без KVM; должен
  прогнать `kvm-lab` на своём стенде.
- **git commit в worktree**: `.git` в этом запуске read-only → доставка через git bundle
  + SHA256-манифест (parent `8b14a59`, только owned paths).
