# Приёмочный артефакт — блок 3 (Compute): изолированная команда внутри общего хода

Run `compute-workspace-sandbox-20261101T091617Z` (CONTRACT.md там же). Счётчики извлекаются из логов
прогона скриптом `make-acceptance.sh` (grep по файлам), а не переписываются вручную.

## Идентичность кандидата (head + sha)

| Поле | Значение |
|---|---|
| Ветка / HEAD | `vm2/compute-workspace-ci` / `a308cf7` (`a308cf732b075d19aaa52133808bb70cd389408a`) |
| Принятая база | `debf8b1` — да: debf8b1 — предок HEAD |
| `git status --porcelain` на момент сборки бинарей | 1 строк |
| Тяжёлый контур | один job под общим `flock /home/alex/ahp-hive/run/build.lock`, RAM-рецепт (session tmpfs 2 GiB = GOCACHE/GOTMPDIR/TMPDIR, GOMODCACHE на диске, GOMAXPROCS=2, старт при MemAvailable ≥ 6 GiB), `LOCK_ACQUIRED=2026-11-01T09:53:00Z` → `DONE=2026-11-01T09:54:25Z`, MemAvailable=12626240 KiB |
| Коммит этого артефакта | только документ: бинари собраны на `a308cf7`, между `a308cf7` и коммитом артефакта исходников нет |
| Субстрат | свой PostgreSQL 127.0.0.1:25432 (своя куча, trust, сокет в run-каталоге), Redis 127.0.0.1:16379, MinIO 127.0.0.1:16900 (свои бакеты). Прод и порты 18080/18081/18082/18110/18290-18292 не тронуты. DSN и креденшелы в артефакты не печатаются |

Бинари (`go build -tags ahp_exec,ahp_data -trimpath -o bin/<name> ./cmd/<name>`, go1.26.0):

| Бинарь | sha256 | независимая перепроверка | build metadata |
|---|---|---|---|
| `bin/compute-host` | `d33aa1e67b373c1c60d8faf092b20aa749a3b2389e0cbc97a46d57484cdfa39f` | повторный расчёт `d33aa1e67b373c1c60d8faf092b20aa749a3b2389e0cbc97a46d57484cdfa39f` — совпал | `go1.26.0`, vcs.revision `a308cf7`, vcs.modified=`false` |
| `bin/as-runner` | `2b21e95325cf9848a5cc8f46cc50923dba746fbb500356db359d91cbcfa50e1c` | повторный расчёт `2b21e95325cf9848a5cc8f46cc50923dba746fbb500356db359d91cbcfa50e1c` — совпал | `go1.26.0`, vcs.revision `a308cf7`, vcs.modified=`false` |

## Green-before-done (числовые счётчики)

| Гейт | RC | PASS | FAIL | SKIP | DATA RACE |
|---|--:|--:|--:|--:|--:|
| `go build ./...` | 0 | — | — | — | — |
| `go vet ./internal/compute ./internal/artifacts ./tests/compute` | 0 | — | — | — | — |
| `go vet -tags compute_live3 ./internal/compute/integration` | 0 | — | — | — | — |
| `go test -race -count=1 -p 2 -v ./internal/compute ./internal/artifacts ./tests/compute` | 0 | 95 | 0 | 0 | 0 |
| `go test -tags compute_live3 -race -count=1 -v -run TestB3 ./internal/compute/integration` | 0 | 5 | 0 | 0 | 0 |

Пакеты юнит-прогона: `ok` = 3, `FAIL <pkg>` = 0. Метод подсчёта: `grep -c '^--- PASS'` / `'^--- FAIL'` — тесты верхнего
уровня, вложенные подтесты (с отступом) счётчик не удваивают; `DATA RACE` — число одноимённых строк в том
же логе (0 = гонок не наблюдалось).

Пакеты и время:

- `github.com/ahp-platform/ahp/internal/compute` — 1.012s
- `github.com/ahp-platform/ahp/internal/artifacts` — 65.366s
- `github.com/ahp-platform/ahp/tests/compute` — 1.720s

Живые стендовые тесты (тег `compute_live3`):

- `TestB3ChainTurnExportDownloadForeign403` — (8.13s) 
- `TestB3TurnModelToolSandboxWithLimits` — (0.01s) 
- `TestB3TurnRefusesWhenQuotaForbidsProcesses` — (0.00s) 
- `TestB3RecoveryNoDuplicateExternalEffect` — (0.01s) 
- `TestB3SandboxEnvIsSecretFree` — (0.02s) 

Durable-логи: `race-units3.v.log`, `race-b3-3.v.log`, `green3-counters.txt` (sha256 — в конце файла).
Полная `-race` по репо не запускалась; второй тяжёлый job параллельно не поднимался.

## Требование → доказательство (CONTRACT, по порядку)

| # | Требование | Доказательство |
|--:|---|---|
| 1 | Инвентарь diff + реенейб на accepted-базу | `debf8b1` — предок HEAD; мердж 0 конфликтов (авто-мердж `internal/artifacts/store.go`); единственный изначально грязный файл — `tests/compute/smoke-transcript.jsonl` (генерируется прогонами, в нём меняются только id/таймстемпы) |
| 2 | Честная таблица (код/unit/live + применено vs декларировано) | `COMPUTE-BLOCK3-STATUS.md` (секции A/B/C) |
| 3 | Ход → модель → инструмент → команда в песочнице с лимитами → файл → 200 со sha/размером → чужой subject 403 | `TestB3ChainTurnExportDownloadForeign403` + `TestB3TurnModelToolSandboxWithLimits` (живой PG/Redis/MinIO, настоящий `core.Runtime`) |
| 4 | SIGKILL посреди команды → нет повторного внешнего эффекта, ход терминален | `TestB3RecoveryNoDuplicateExternalEffect` |
| 5 | Логи/окружение команды без секретов | `TestB3SandboxEnvIsSecretFree` |
| 6 | bounded `-race` по затронутым пакетам, exit/pid/OOM | таблица выше: RC=0, PASS=95/FAIL=0/SKIP=0/DATA RACE=0; pid и UTC-метки в `green3-counters.txt` |
| 7 | sha256 пересобранных бинарей + `go version -m` | таблица «Идентичность кандидата»: vcs.revision=`a308cf7`, vcs.modified=`false` (бинари собраны с чистого дерева), независимый пересчёт sha256 совпал |

## Границы: чего этот артефакт НЕ заявляет

- Хост-бэкенд стенда применяет `ulimit -u/-n`, таймаут, bound stdout/stderr, jail-каталог, PTY-бюджет и
  минимальное окружение. CPU/память/диск/egress на хосте НЕ применяются на уровне ядра: это зона microVM
  (class X, Firecracker) — на этом стенде они честно числятся декларированными (`COMPUTE-BLOCK3-STATUS.md`, B
  и секция D с read-only замером). Jail-песочница НЕ называется microVM.
- Выбор токена моделью задан скриптом (`scriptedModel`); весь остальной контур — боевой код
  (`core.Runtime`, `internal/compute`, `internal/artifacts`). Живой облачный Qwen в закоммиченный зелёный
  контур не включён (практика соседних lane — гейт `AHP_LIVE_QWEN`).
- `compute.run_command` НЕ зарегистрирован в боевом `cmd/as-runner/wiring.go` (файл чужой lane, владелец
  hive-director `vm2-a2a-multinode`); запрос подан на форум. Без этого правки продуктовский ход собирается
  в тесте на настоящем `core.Runtime`, а не CLI-обвязкой.
- Правки вне `internal/compute/**` и своих тестов — только шов двух принятых контрактов (B114 read-parity 404
  против DownloadGrant 403 на выдачу гранта): `internal/artifacts/store.go`, `internal/artifacts/grant.go` +
  формат-фикс `internal/artifacts/rangeapi.go`. Ни одно принятое тестовое ожидание не изменено; пост с
  указанием файла и владельца подан на форум.
- Терминальное состояние ХОДА после восстановления — зона `internal/space` (владелец `vm2-state-runner`);
  здесь доказано, что внешний эффект команды не дублируется и леджер команд приходит к терминальному статусу.

## D. KVM/microVM: точный read-only замер (не «нет KVM», а что именно не так)

Замер read-only, ничего не менялся:

| Проверка | vm2 (`vm-chupikbibi`, эта лань) | vm1 (read-only по `~/.ssh/vm2-sync`, пиннированный hostkey) |
|---|---|---|
| `/dev/kvm` | есть: `crw-rw---- 1 root kvm 10, 232` | есть: `crw-rw---- 1 root kvm 10, 232` |
| ядро/вложенность | `kvm` + `kvm_intel` загружены, `kvm_intel.nested=Y`, CPU `vmx` | Linux x86_64 |
| доступ пользователя лани | **EACCES**: `alex` состоит только в `alex,users` (не в `kvm`); `open("/dev/kvm")` → `PermissionError [Errno 13]` | **то же**: `alex (uid 1001)` только в `alex,users` |
| `firecracker` | отсутствует (`/usr/bin`, `/usr/local/bin`, `~/ahp-hive/tools`) | `firecracker=absent` |
| гостевой kernel (`vmlinux`) / `rootfs.ext4` | отсутствуют | отсутствуют |

Вывод: аппаратного KVM на доступных узлах хватает, но class X не поднимается по трём причинам —
(1) у пользователя лани нет доступа к `/dev/kvm` (группа `kvm`), (2) нет бинаря Firecracker,
(3) нет образов гостевого ядра и rootfs. Ни один из пунктов не закрыт: правка группы/ACL и установка
образа — это изменение узла вне моей зоны, поэтому подан запрос владельцу инфраструктуры с вариантами
(а) выдать доступ к `/dev/kvm` + предоставить `firecracker` + `vmlinux` + `rootfs` на vm2 (вложенная
виртуализация уже включена), (б) дать существующий авторизованный узел с KVM и образами, (в) оставить
host-бэкенд и считать CPU/память/диск/egress декларированными. Платные ресурсы не создавались.

Отдельный дефект в моём владении, который этот замер вскрыл: `FirecrackerRuntime.Probe`
(`internal/compute/firecracker.go`) проверяет только `os.Stat(/dev/kvm)` — на этом узле он рапортует
«готов», хотя boot упал бы на EACCES. Это маскировка блокера, и она исправляется следующим шагом: Probe
обязан проверять реальную доступность устройства на чтение и запись. До этого исправления class X на этом
стенде поднимать нельзя, и в логах он обязан отказывать.

## Независимая сверка sha256

- `d33aa1e67b373c1c60d8faf092b20aa749a3b2389e0cbc97a46d57484cdfa39f  run/.../bin/compute-host
- `2b21e95325cf9848a5cc8f46cc50923dba746fbb500356db359d91cbcfa50e1c  run/.../bin/as-runner

Логи прогона:

- `a2e1a9c8393de173fa70d33f44156e000cb89a01aba821d36691139319c1bd02  run/.../green3-counters.txt
- `d63e45ad98e63ddae53cb6fbd1c971596c1ad0a22277f70551f4cd396bc77517  run/.../race-units3.v.log
- `738e329bca711fb9fd2c200cee45f66131225f450ce6503d2a89fb89260318f7  run/.../race-b3-3.v.log

Как перепроверить: `bash run/.../queue-green3.sh` (сам ждёт общий build.lock; после тестов возвращает
`tests/compute/smoke-transcript.jsonl` к закоммиченной версии, чтобы дерево осталось чистым), затем
`bash run/.../make-acceptance.sh` — он перегенерирует этот файл из свежих логов.
