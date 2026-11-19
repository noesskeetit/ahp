# RFC: at-rest encryption in owner write-paths (State / Artifacts / Memory)

- Статус: proposed. Владелец блока — at-rest-crypto. Координатор контракта с root — hive-director (я не мержу и не меняю чужие контракты).
- Основание: PDF rev3.2 §8.3 (tenant … свои State Store, бакеты и ключи KMS), §10 (Tier0 per-tenant KMS). Реализация: `internal/atrest` (принят директором, envelope AES-256-GCM + HKDF, AAD=tenant/keyID/version/purpose, fail-closed).
- Готовый примитив: `atrest.Codec.Seal/Open/OpenTransparent`, `IsEnvelope`, `objectcipher.Cipher` (объектный байт-шов с операторским включателем и прозрачным dual-read). Бенчмарки — в отчёте блока.

## 1. Что меняется по операциям (узко, по стадиям)

**Стадия 1 — Artifacts object bytes (без риска семантики; шов уже реализован).**
- Меняется только байт-граница объекта в MinIO: `objectcipher.PutObject` герметизирует тело на записи, `GetObject` вскрывает на чтении. Индексы, `sha256`, `size`, `labels`, дедупликация и гранты в PG НЕ меняются — sha/size по-прежнему считаются по plaintext, который даёт `GetObject`.
- Контракт скачивания (canonical proxy / presigned PUT / `openVersionRange`) остаётся у Artifacts-владельца (Galileo). Ему подключить `objectcipher` к фактическим точкам put/get. Пресigned-upload требует либо серверного transform, либо клиентского Seal — решение владельца (см. §5 открытые точки).
- purpose AAD: `artifact.bytes`.

**Стадия 2 — State payloads (State-владелец).**
- Колонки-носители содержимого переходят на «sealed bytea + режим»: `runs.payload`, `checkpoints.metadata`, `checkpoint_blobs.data`, `checkpoint_writes.data`, `kv.value`, `kv_ops.result`, `inbox_events.payload`, `stream_events.payload`. Для jsonb-колонок — хранить `bytea` конверта (или jsonb-обёртку base64) и читать через `OpenTransparent`.
- purpose AAD: `state.run.payload`, `state.checkpoint.blob`, `state.kv.value`, `state.event.payload`.
- НЕ шифруются `body_digest/digest` (используются в идемпотентности/поиске — см. матрицу остатка).

**Стадия 3 — Memory text + native Hindsight body (Memory-владелец, отдельный контракт).**
- `memories.text` и `ahp_longterm.memory_records.body` — содержательные, но `search tsvector` генерируется ИЗ `text`, а `embedding vector` — из текста. Прямое шифрование `text` ломает полнотекстовый поиск и ANN. Поэтому Stage 3 — НЕ часть этого rollout: сначала слепые индексы/encrypted ANN (см. матрицу). Здесь фиксируется только граница и цель.

## 2. AAD (что попадает в привязку)

Уже реализовано в `atrest`. Для каждого владельца различается только `purpose` (таблицы выше). Привязка: `tenant || keyID || keyVersion || purpose` (+ sha256 wrapped-DEK на data-слое). Это даёт: нельзя перенести шифртекст между tenant, ключом, версией или назначением; подмена ломает GCM. Ключ — операторский/tenant-авторитет (`KeyAuthority`), не bearer задачи; tenant в `Seal/Open` берётся из аутентифицированного scope владельца, не из данных.

## 3. Двойной режим чтения: флаг `encryption_mode` (читатели старых plaintext не ломаются)

- В каждую затронутую таблицу добавляется `encryption_mode smallint NOT NULL DEFAULT 0` (0 = plaintext/legacy, 1 = envelope). Мутация только ADD COLUMN с DEFAULT — неразрушающая, идемпотентная, откат = DROP COLUMN (данные не теряются, т.к. 0-строки остаются plaintext).
- Путь записи: операторский профиль `Enabled()` решает режим нового ряда; `Seal` пишет тело и ставит `encryption_mode=1`; при выключенном профиле — `mode=0`, тело как сейчас (проверено: байт-в-байт, без silent-включения).
- Путь чтения: `mode=0` → вернуть как есть; `mode=1` → `atrest.Open`. Envelope самораспознаваем (`IsEnvelope`), поэтому флаг — это authoritative-наблюдаемость и defense-in-depth (несоответствие флага и magic = сигнал порчи), а не единственная опора.
- Смена оператора OFF→ON не требует переписывания: новые рядыsealed, старые plaintext читаются. ON→OFF тоже безопасно для ЧТЕНИЯ (Open не зависит от write-флага): ранее sealed ряды остаются читаемы; но ranged-чтение sealed артефакта требует, чтобы профиль был ON (иначе native-range вернёт шифртекст) — операторский контракт, зафиксировать в runbook.

## 4. Миграция и откат

- Никакого обязательного bulk-rewrite. Опциональный фоновый re-encrypt старых строк mode 0→1 (переписать, прочитав через dual-read и записав Seal), порциями, с проверкой «после записи Open даёт исходный plaintext». Прерываем, resumable (критерий — `encryption_mode=0`), без блокировок на всю таблицу.
- Откат профиля: выключить операторский `Enabled()` → новые writes plaintext; sealed-ряды остаются читаемы (Open). Полного дешифрования всей БД откат не требует; при необходимости обратный re-encrypt 1→0 — тот же job.
- Ключ-цикл: ротация не трогает данные (старые версии остаются readable как rotated); disabled/revoked версии дают отказ чтения (fail-closed) — до revocation нужна операция повторного шифрования под живой ключ, это отдельная runbook-процедура владельца.

## 5. Инварианты тестов, которые добавляет владелец (минимальный набор на границу)

1. Раундтрип Seal/Open через РЕАЛЬНОЕ хранилище владельца (isolated PG/MinIO), байты на диске ≠ plaintext и без маркера plaintext.
2. Выключенный профиль: записанный ряд/объект байт-в-байт равен сегодняшнему plaintext; envelope-магия отсутствует (нет silent-включения).
3. Ряд mode=0 (legacy) читается при включённом профиле; новый ряд mode=1 читается; смешанное состояние в одной таблице.
4. wrong-tenant → отказ изоляции (`ErrTenantMismatch`); tamper байта → `ErrAuthFailed`.
5. Для Artifacts: эквивалентность range (зашифрованный и plaintext объекты дают одинаковый срез) и неизменность sha256 plaintext (дедуп/seal-идентичность).
6. Ротация+«рестарт» (новое соединение/новый авторитет с тем же профилем) читает ранее записанные mode=1 строки.
7. Отсутствующий ключ/авторитет → отказ записи (fail-closed), не plaintext-подмена.

## 6. Открытые точки для согласования с root/владельцами

- Artifacts presigned-upload: серверный transform vs клиентский Seal vs отказ от presigned при включённом шифровании — выбирает Galileo (меняет клиентский контракт скачивания/загрузки).
- State jsonb: `bytea` конверта vs jsonb-base64-обёртка (влияет на существующие size-check `octet_length(...)<=1MiB` — пересчитать лимит с учётом +~135 B overhead).
- Range поверх шифртекста для больших артефактов: стриминговый/сегментный decrypt (механизм-ready версия читает объект целиком → пик памяти ~2× на 100 MiB, см. таблицу оверхеда). Оптимизация — за владельцем артефактов.
- Stage 3 (Memory) не начинать до отдельного контракта blind-index/encrypted-ANN.
