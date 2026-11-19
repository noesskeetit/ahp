# Блок 3 (Compute) — честная таблица состояния (PDF rev 3.2)

Артефакт к CONTRACT.md compute-workspace-sandbox-20261101T091617Z.
База: ветка vm2/compute-workspace-ci, HEAD 58095bb (мердж принятой базы debf8b1, 0 конфликтов,
go build ./... = EXIT_RC=0/success, OOM none). Субстрат прогона: хост-бэкенд (class S/C), живой PG
(:25432, своя trust-куча), живой Redis (127.0.0.1:16379), живой MinIO (:16900, бакеты lanes).
microVM (class X, firecracker) на этой ноде НЕ запускается (нет KVM) — фиксируется честно (см. B).

## A. Что есть из блока 3: код / unit / live-стенд / отсутствует

| Возможность | код | unit | live-стенд | отсутствует / caveat |
|---|:--:|:--:|:--:|---|
| Компьютеры/классы S/C/X, профили, scope-изоляция | есть | есть | есть (B10 Sit9, B137 Sit1–10) | — |
| Исполнение команды в песочнице (хост): jail + RLIMIT_NPROC/NOFILE + таймаут + bound output | есть backend_host.go | есть | есть (B10 Sit1–2, B134 Sit1) | CPU/mem/диск/сеть на хосте не применяются (B) |
| Идемпотентный леджер команд (ключ → повтор не переисполняет; fence SIDE_EFFECT_UNKNOWN) | есть runner.go/store.Exec | есть | есть (B134 Sit1, B133 Sit1/Sit6) | — |
| Отмена: двойная cancel/fence; cancel сохраняет артефакты; stale version отвергается | есть | есть | есть (B16 Sit12–14 — зачтено) | — |
| Версии/снимки/состояние/стоимость API | есть | есть | есть (B10 Sit3–5, B139 Sit11) | — |
| Экспорт файла → Artifacts (multipart, sha256+size, seal) | есть artifacts.go | есть tests/compute/artifacts_test.go | есть (B137 Sit10) | — |
| Скачивание 200: ETag=digest, Content-Length=size, Range | есть artifactsapi/download.go | есть | есть | — |
| Чужой subject / чужой task-context → 403 | есть artifacts/read_scope.go/computeapi | есть | есть (B137 Sit1/Sit2, read_scope unit) | — |
| A2A-статусы tasks/cancel/backpressure | есть | есть | есть (B11 Sit1–8) | — |
| one-executor-per-thread (lease) | есть leases.go/redis.go | есть | есть (B10 Sit10–11) | — |
| Инструмент «run_command» в боевом ходе (wiring turn→tool→compute) | НЕТ | НЕТ | НЕТ | Ключевой разрыв: core.Runtime (модель/loop) и space.Coordinator (State) реальны, но в cmd/as-runner/wiring.go зарегистрированы только save_artifact и emil.delegate; вычисл. инструмента нет. Правка файла — чужой блок (владелец hive-director vm2-a2a-multinode). |
| Сквозной живой ход модель→инструмент→команда→файл→403 | мои тесты | — | есть (integration/live_b3_turn_test.go, integration/live_b3_chain_test.go — 5 тестов, −race PASS) | Двигается НАСТОЯЩИЙ core.Runtime + мой compute-ToolExecutor; живой PG/Redis/MinIO. Живой облачный Qwen в закоммиченный зелё не включён (чужая практика — гейт AHP_LIVE_QWEN). |

## B. Лимиты PDF: применяются vs декларированы

| Ограничение PDF | Где заявлено | Чем применяется | Статус |
|---|---|---|---|
| Профиль (корни FS) | policy.go JailRoots | хост: jail-каталог + durable-home; cwd ограничен базой jail | применяется (конвенция каталога), без mount-ns/chroot на хосте |
| Таймаут команды | backend_host.Exec (default 30s) | context deadline → exit −1 | применяется |
| Процессы (PID) | Quota.MaxPIDs | ulimit -u (RLIMIT_NPROC) | применяется (хост) |
| Открытые файлы | Quota.MaxOpenFiles | ulimit -n (RLIMIT_NOFILE) | применяется (хост) |
| Бюджет PTY (SBX-03) | policy.go CheckPTYBudget | счётчик активных PTY-лизей | применяется |
| Bound stdout/stderr | boundBytes/maxOutputBytes | усечение + флаг Truncated | применяется |
| CPU (mCPU) | Quota.MaxCPUMillisPerSec | только microVM: vCPU=ceil(mCPU/1000) через Firecracker putMachineConfig | декларировано на хосте (нет RLIMIT_CPU/cgroup); применяется в VM (X). X на этом стенде не поднимается — точный замер в секции D |
| Память | Quota.MaxMemoryMB | только microVM: putMachineConfig mem | декларировано на хосте (RLIMIT_AS нет) |
| Диск | Quota.MaxStorageMB | декларировано; хост не ставит квоту (reflink = CoW-копии, не квота) | декларировано (не enforcement на хосте) |
| Egress | policy.go EgressPolicy + pkg/netpolicy | компилируется в egress-guarded http.Client; X deny-all (no tap + внешний proxy) | применяется для программ через netpolicy-клиент; raw /bin/sh на хосте не изолирован сетевым пространством |
| Остановка/восстановление (SIGKILL mid-command) | runner.RunCommand + store.Exec fence | повтор по ключу НЕ переисполняет (mayDispatch=false); DISPATCHED/UNKNOWN не ретраится | применяется на уровне внешнего эффекта команды; терминальное состояние хода — зона State (internal/space, чужой блок) |

## C. Найденный шов мерджа (один, именован, закрыт моим минимальным фиксом)
`git merge debf8b1`(auto-merge `internal/artifacts/store.go`) свёл вместе два ПРИНЯТЫх контракта, которые
друг друга не видели:
- B114 (моя lane, принято форменом `ee7c50b`, 48 P/0 F): чтение чужим subject = NOT_FOUND (parity-404,
  без утечки существования) — store.go `get()`;
- DownloadGrant (принятая база `78937a3`, автор ahp-kvm-lab-swarm): выдача гранта чужим subject =
  FORBIDDEN — internal/artifacts/grant_test.go:217.
Маска parity-404 стояла в общем `get()`, поэтому на мердже `TestDownloadGrantRecipientIsTheCaller`
упал (`foreign mint code = "NOT_FOUND", want FORBIDDEN`). Фикс не трогает ни одно принятое ожидание:
маска осталась только на путях ЧТЕНИЯ (`get`), выдача гранта переведена на `getForGrant` (тот же полный
read-gate: authorize + checkReadScope + admit, но реальный код отказа). Читающий read-paths → 404 (B114),
выдача capability → 403 (контракт DownloadGrant). Правка вне `internal/compute/**` → пост на форум
с указанием файла и владельца (`20261101T…-vm2-compute-b114-grant-seam`).

## Вывод по разрывам
1. Нет вычислительного инструмента в боевом ходе (as-runner) — правка чужого файла → пост владельцу.
2. Хост-бэкенд применяет PID/NOFILE/таймаут/PTY/output/jail, но НЕ CPU/mem/диск/сеть на уровне ядра —
   это зона microVM (class X), недоступного без KVM. Лимиты названы «применяемыми» только где enforcement реален.
3. Выдача download-grant чужим subject: семантика кода ответа (403 vs 404) — общий шов lane артефактов и
   lane compute; зафиксирована секция C, решение владельца подтверждается постом на форум.

## D. KVM/microVM: точный read-only замер (не «нет KVM»)

| Проверка | vm2 (`vm-chupikbibi`) | vm1 (read-only ssh) |
|---|---|---|
| `/dev/kvm` | есть, `crw-rw---- root:kvm 10,232` | есть, `crw-rw---- root:kvm 10,232` |
| ядро | `kvm`+`kvm_intel`, `nested=Y`, CPU `vmx` | Linux x86_64 |
| доступ лани | **EACCES** (`alex` не в группе `kvm`) | **EACCES** (то же) |
| `firecracker` | нет | нет |
| `vmlinux`/`rootfs.ext4` | нет | нет |

Значит: class X не поднимается не из-за отсутствия аппаратного KVM, а из-за (1) отсутствия доступа к
устройству у пользователя лани, (2) отсутствия бинаря Firecracker, (3) отсутствия образов ядра/rootfs.
Хост-бэкенд при этом НЕ называется microVM. Замер и варианты закрытия — в
`ACCEPTANCE-compute-block3-integrated-turn.md` (секция D); запрос владельцу инфраструктуры подан.
