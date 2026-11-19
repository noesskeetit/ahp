# UI: результаты приёмки 2 ноября 2026

Проверялся адаптированный интерфейс выбранного `Downloads/example`, отдельный
human origin `http://127.0.0.1:29822`, Chromium с изолированными контекстами.
Пользовательские браузерные вкладки и origin `29712` не изменялись.
Этот отчёт не подтверждает выполнение всей архитектурной матрицы PDF.
Он не содержит credentials, содержимого памяти, ответов модели или raw logs.

## Подтверждённые пути

| Путь | Измеренный результат | Граница доказательства |
|---|---|---|
| Вход / выход | Реальные login/token/logout; bearer очищен, localStorage/sessionStorage пусты | Браузерная сессия выделенного стенда |
| Memory | Propose → Review active → reload; исходная таблица непустая | Не проверка качества semantic retrieval |
| Crons | CreateSchedule → List → Revoke; непустая таблица | Не доказательство фонового webhook/cron исполнения |
| Нативный чат | Два разных turn в одном thread, оба done; второй восстановил факт первого | Реальная модель через gateway, проверен результат, а не только accepted |
| Артефакты | Точные tool ArtifactRef/version; два скачивания по 17 байт, SHA-256 совпал | UI не выбирает произвольный hex из текста модели |
| Привязка артефактов | Повтор тех же имён и байтов в новом run дал новые IDs; Artifacts.Get подтвердил новый context и соответствующий task | Проверено после исправления scope-wide ключа идемпотентности |
| Стоимость | 200; первый turn 1692 tokens/spent, второй 2519; по 2 generations; значения сохранены после reload | Наблюдаемый ledger Model API, не оценочная цена |
| Отмена | CancelTurn с terminal_frame_durable=true → canceled → reload | Проверен durable результат |
| Workspace API | Upload → Seal → candidate; Publish без checks 409; скачанные байты и canonical digest кандидата проверены; SetCheck → owner Artifacts.Publish → Workspace.Publish → ReadFile, 88 байт / SHA-256 совпали | Upload/check/publish выполнялись через task API; браузер предоставляет создание, чтение и скачивание опубликованного файла |
| Изоляция пользователей | Владелец видел существующую память и скачивал файл; второй пользователь того же tenant не видел память, GetWorkspace/ReadFile известной ревизии получили 403 | Проверялись существующие объекты, а не выдуманные IDs |
| Согласование инструмента | Реальный Call 409 → кнопка Approve с exact request_id → тот же Call/Get SUCCEEDED; независимое чтение destination MCP подтвердило ровно одну запись | Не fixture и не одна лишь HTTP квитанция |
| Compute | Create → Wake → Sleep → Stop; actual incarnation.backend=mock и видимый MOCK | Только разрешённая симуляция MicroVM; не доказательство VM-изоляции или исполнения кода |
| Выбранная установка | RunEval outcome=pass; Invoke именно installation; GetInvokedTask дошёл completed, показал непустой output с точным runtime version_digest | Reload пока не принят — см. ниже |

Нативные результаты выше повторены после согласованного READY3 backend/runner
build `6453201d`. Memory/Crons/Workspace/approval/Compute мутации подтверждены
предыдущими отдельными прогонами интегрированного стенда; последующая read-only
инвентаризация подтвердила чтение текущим backend без ошибок. Результаты отдельных
прогонов не выдаются за единый непрерывный прогон всей системы.

## Открытый дефект Registry reload

После завершения выбранной задачи reload обновляет human JWT и scoped exchange
grant. GetInvokedTask возвращает 403:
`runtime: observation requires the original parent grant`.
Браузер не сохраняет task-token и не обходит эту проверку. Владельцам Registry и
авторизации передан дефект; требуется owner-authorized durable observation без
ослабления original-parent проверки машинных вызовов. До отдельного успешного
повтора этот путь отмечен **PARTIAL**, а не E2E PASS.

## Read-only инвентаризация исходных разделов

Просмотрены Обзор, Memory, Crons, Agent Stacks, Чаты, Workspace, Sandbox,
Согласования, Stats, Repositories, Environments и AI Factory. Наблюдались
Memory=1, Crons=1, Agent Stacks=1; Workspace показывал файл, участника и ревизию;
Stats — реальную стоимость выбранной задачи. API-чтения вернули 200, ошибок
JavaScript и попыток доменных записей не было. Пустые Согласования показаны как
пустые. Repositories и Environments явно сообщают об отсутствии подключённого API.

Инвентаризация выявила и закрыла три UI-дефекта:

- mobile chat при viewport 390 расширял документ до 443 из-за длинного thread ID;
- табличный Refresh в Workspace/Sandbox/Stats запрашивал другие разделы;
- в оболочке оставались входы в демо и устаревший мастер настройки.

Каждый дефект воспроизведён регрессией до исправления. После исправления real
read-only проверка подтвердила ширину 390, соответствующие GetWorkspace,
GetComputer и Cost 200 по кнопке Refresh и отсутствие Demo/Guide controls.

Остающиеся границы интерфейса: нет Git/environment интеграций; нет браузерного
upload/check/publish Workspace; нет UI редактирования нового Registry Config;
Stats показывает выбранную задачу, а не платформенную аналитику; продвинутые
снимки, deployment logs, OAuth/webhooks и управление бюджетами здесь не приняты.
Наличие таких API или библиотек не считается подключённым экраном.

## Воспроизводимость и доказательства

- `node scripts/aifactory-browser.test.mjs`: 16 Chromium contract regressions.
  Это fixtures, не live доказательства.
- `node --test web/aifactory_canonical.test.mjs`: 9 canonical digest checks.
- `go test ./web`: встроенные frontend contract/hygiene проверки.
- `scripts/aifactory-mutation-browser.mjs`: отдельный writable human origin,
  реальные UI действия; partial/failure не превращаются в passed.
- `scripts/aifactory-workspace-acceptance.py`: реальные API и выполненная
  проверка байтов перед записью SetCheck.
- `scripts/aifactory-isolation-browser.mjs`: две настоящие human-сессии.

Content-free отчёты хранятся вне репозитория:

- `/tmp/ahp-root-ui-ready3-native.json`
- `/tmp/ahp-root-ui-ready3-artifact-binding.json`
- `/tmp/ahp-root-ui-ready3-registry.json`
- `/tmp/ahp-root-ui-workspace-api-fresh.json`
- `/tmp/ahp-root-ui-isolation.json`
- `/tmp/ahp-root-ui-approval.json`
- `/tmp/ahp-root-ui-compute-repaired.json`
- `/tmp/ahp-root-ui-readonly-inventory.json`
- `/tmp/ahp-root-ui-inventory-fixes.json`

Скриншоты находятся только в `/tmp`; в репозиторий не перенесены.
