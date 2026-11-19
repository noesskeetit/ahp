# Workspace Store: trusted CI, code-candidate путь, GC/lifecycle E2E

- Lane: `private/pdf-closure-20261102/workspace-ci`, ветка `root-pdf-closure-workspace-ci`
- База (frozen root reference): `7a3a7a220aed0e85979d036a9d29cdb6835338a0`
- Коммит блока: `f8a6b664f7f3b2d278342022e3c39468ebbdcee1`
- Время отчёта: 2026-11-09T15:33Z (date -u)
- Источник требований: PDF rev 3.2 (SHA256 044e045b…a7e2), §3.7 с.37, §7 с.48, §8.2 с.48–49; root-аудит `reports/root-service-audit-20261109/11-workspace-store.md`

## Что изменено в коде (владелец блока — Workspace Store)

- `internal/workspace/ci.go` — реестр доверенных CI-раннеров (`ahp_workspace.ci_runners`),
  provenance проверок (`trusted`, `runner_task`, `runner_root`, `runner_subject`),
  `SetCheckWithProvenance`, классификация code/executable, гейт `checkTrustedCI`.
- `internal/workspace/store.go` — DDL реестра и колонок provenance (fail-closed
  `trusted=false` для старых строк); owner-only действия `ci:register|list|remove`.
- `internal/workspace/publish.go` — `Publish` перед merge сверяет provenance: для
  code/executable кандидата каждый check обязан быть доверенным и digest-bound.
- `internal/workspace/revision.go` — `PurgeRevision` отказывает, если ревизию
  использует живая рабочая копия (§7 с.48). Вершина конфликтной ветки без
  рабочей копии остаётся вычищаемой (сохранено принятое поведение
  `TestWorkspacePinAndPurgeGuards`).
- `internal/workspace/candidate_review.go` — `CandidateCheck` отдаёт provenance
  (viewer/portal видят, кем и на какой digest выдан вердикт).
- `internal/workspace/workspaceapi/api.go` — `SetCheck` берёт provenance из
  проверенных claims (не из тела запроса); новые owner-операции
  `RegisterCIRunner`/`DeregisterCIRunner`/`ListCIRunners` (общая сборка уже
  монтирует их через существующий handler, правок composition не потребовалось).

Ревизии/CAS/conflict-механику не переписывал — только подключил к ней гейт.

## Требование PDF → тест → статус → evidence

| # | Требование (PDF rev3.2 / аудит) | Тест в базе | Статус | Evidence |
|---|---|---|---|---|
| 1 | «Коммит — только через кандидата с проверками»; для кода проверки выполняет доверенный CI, а не автор (аудит п.2: «browser pass-флаг не достаточен») | `TestTrustedCIGateForCodeCandidate` | принято (live PG+MinIO) | `evidence/workspace-ci-20261109T153248Z-12ec96.log` |
| 2 | То же на owner-поверхности: HTTPS/mTLS, task-токены, человеческий крединшл через user-edge не пишет вердикты | `TestTrustedCIOverOwnerHTTP` (+ ранее принятый `TestHumanCannotSubmitTrustedCheckVerdict`) | принято (live mTLS) | тот же лог |
| 3 | Проверка привязана к digest кандидата; правка файлов обнуляет зелёный verdict (stale-green replay невозможен); повторный пуск CI обязателен | кейсы `…ForCodeCandidate` (UpdateCandidateFiles→Publish 409) и HTTP-тест | принято | те же логи |
| 4 | Documents/data не попадают под CI-гейт (честная граница integrity) | `TestTrustedCIGateKeepsDocumentsUnaffected`, `TestRequiresTrustedCIClassification` | принято | тот же лог |
| 5 | Checkout→две правки→WORKSPACE_CONFLICT→сохранённая ветка→осознанный merge→новый head (аудит п.1) | `TestConflictJourneyTwoAuthorsConsciousMerge` | принято (live) | `…153248Z…`, `…152609Z-398546.log` |
| 6 | Негатив: чужой tenant не видит (NOT_FOUND) и не пишет; same-tenant посторонний — FORBIDDEN; viewer не публикует; отзыв участника между Prepare и Commit блокирует merge (аудит п.3) | тот же journey-тест | принято | те же логи |
| 7 | Ревизия не удаляется, пока её использует рабочая копия (§7 с.48; аудит п.4) | `PurgeRevision`-кейсы в journey и GC-тестах | принято | те же логи |
| 8 | GC не удаляет живые ссылки: head, pin, ветка конфликта, pending-рабочая копия защищены через reference-scan владельцев; сирота собирается | `TestWorkspaceGCE2EReferencesRestoreAndBackup` (композиция ReferenceScan как в `cmd/ahp/liveness_state.go`) | принято (live PG+MinIO, versioned bucket) | `…153248Z…` |
| 9 | Чужой restore/чтение denied: чужой tenant — NOT_FOUND на Get/Restore/чтение ревизии; его GC-прогон не трогает ни строк, ни версий этого tenant; same-tenant без членства — отказ | тот же GC-тест | принято | тот же лог |
| 10 | backup→restore сохраняет версии и CAS-хэши: копии pinned-версий всех достижимых ревизий хэшируются в CAS-дайджесты ревизий; restore внутри retention окна возвращает тот же version id и sha256; повторный sweep их не собирает; `VerifyRevisionIntegrity` сверяет с байтами MinIO | тот же GC-тест | принято (см. оговорку ниже) | тот же лог |
| 11 | Внешняя перезапись объекта по тому же ключу не меняет то, что отдаёт ревизия (читает pinned version); сирота-версию sweep убирает, pinned остаётся | тот же GC-тест | принято | тот же лог |
| 12 | Private source ceiling при проектной публикации (аудит п.3) | ранее принятые `TestWorkspacePublicationLeakGate`, `TestB118LeakGateCeiling` | без изменений, зелёные | финальный прогон |

## Полный прогон (одна команда)

```
cd private/pdf-closure-20261102 && AHP_ROOT_CHECK_IMAGE=ahp-handbook-root-check:20261109 \
 python3 run-tests.py --lane workspace-ci --linux sh -c \
 'export GOFLAGS="-p=1 -tags=ahp_data,ahp_exec"; go test -count=1 ./internal/workspace/...'
```

- На грязном дереве перед коммитом: `exit_code=0` — `evidence/workspace-ci-20261109T153127Z-602d66.log`
- На чистой ревизии `f8a6b664`: `exit_code=0`, 30 PASS, 0 FAIL — `evidence/workspace-ci-20261109T153218Z-137948.log`
- Потребители composition root (`cmd/ahp` Workspace/edge тесты) на `f8a6b664`: `exit_code=0` — `evidence/workspace-ci-20261109T153200Z-fcaf37.log`
- Базовый прогон до изменений на `7a3a7a22`: `evidence/workspace-ci-20261109T151407Z-1b39d7.log`
  (пакета `internal/workspacesvc` в репозитории нет — реальные пакеты: `internal/workspace`, `internal/workspace/workspaceapi`)
- Промежуточные падающие прогоны (не скрываются): `…152437Z-ec649a.log` (конфликт ключей загрузки в новом тесте), `…152955Z-01d56a.log` (мой guard против принятого правила purge вершины ветки — guard ослаблен до PDF-формулировки, тест не менялся)

## Live против fixture — честно

Live (реальная инфраструктура lane):
- PostgreSQL: lane-БД `ahp_hive_pdf_workspace_ci` через `AHP_TEST_DATABASE_URL` (без секретов в отчёте);
- MinIO: реальный versioned bucket (`ahp-go-ws-*`, версия включена `Migrate`), реальные CopyObject/StatObject/PutObject;
- HTTP: `httptest` TLS c `RequireAndVerifyClientCert`, клиентские сертификаты SPIFFE, реальные Ed25519 task/human токены через штатный `transport.Task`/`TaskOrUser`;
- GC: штатный `artifacts.GC` c reference-scan владельцев (композиция, как в `cmd/ahp/liveness_state.go`).

Fixture (не выдаю за live):
- coarse `authorize` в тестах всегда разрешает (решения IAM на действия в тестах не проверяются; тонкие права — членство/роли — реальные, в проде их решает ExtAuthz/IAM);
- CI-раннер как workload не выложен: тест сам подписывает task-токен раннера тестовым Signer'ом; сам конвейер (checkout→сборка→вердикт) — чужой блок (Репозиторий/CI), мой блок дал якорь доверия и доказал его ворота;
- «backup» в E2E — тест сам копирует pinned-версии объектов в отдельный bucket: прод-демон бэкапа и процедура восстановления БД из дампа не реализованы и не проверялись; доказано свойство «копии pinned-версий = CAS-хэши» и «in-place restore из tombstone = тот же version+digest»;
- живые стенды (front PID46371, runner PID46295) не задействованы и не тронуты; Redis в этом блоке не требуется.

## Незакрытое / на усмотрение директора

1. Прод-процедура disaster-recovery (дампа БД + re-point метаданных артефактов на backup-копии) — вне доказанного; при приёмке «backup→restore» считать доказанным уровень «копии верифицированы по CAS + in-place restore сохраняет version/digest».
2. `PurgeRevision` вершины конфликтной ветки: оставил принятое поведение (вычистить можно, строка `branches` повисает — `branches.rev_id` без FK). Если нужна явная `DeleteBranch`/каскад — скажите, добавлю в своём владении.
3. Rollback без расширения audience (аудит п.3) покрыт частично: restore из tombstone перепроверяет admit/метки, отказ чужому tenant доказан; сценарий «откат head на старую ревизию» как продуктовая операция не реализован (в PDF не формулируется).
4. Реальные раннеры CI нужно регистрировать операционно (`RegisterCIRunner`); составление (кто такой CI-ворклоад в проде) — контракт блока CI, не мой.
