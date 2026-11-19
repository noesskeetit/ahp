# Память общего проекта в личном Dot

Dot остаётся личным агентом сотрудника. Участники проекта получают доступ к
разрешённым фактам проекта; история диалога и Computer остаются личными.

Оператор включает `AS_SHARED_MEMORY_URL` на runner. Это HTTPS origin backend,
который публикует `/v1/memory/shared/`; нужны также `AS_MEMORY_URL` для native
personal/private-project профиля, `AS_WORKSPACE_URL`, workload mTLS и уже
разрешённые `memory,workspace` в `AS_RUN_GRANT_CAPS`. Адреса не выбирает модель.
Hindsight не превращается в общий банк и не копируется в shared Store.

Первый `AcceptTurn` задаёт:

```json
{
  "key": "new-project-turn",
  "context_id": "release-discussion",
  "project_id": "canonical-workspace-id",
  "project_memory_profile": "shared",
  "input": {"role": "user", "content": "Какой у проекта релизный код?"}
}
```

Пустой/native профиль сохраняет прежний личный/private-project путь. Shared
выбирается явно: число участников не переключает профиль автоматически.
`project_id` и профиль сохраняются в payload State run, возвращаются в истории
и наследуются при продолжении диалога. Чтобы поменять их, нужен новый диалог.

В UI после выбора Workspace у новой беседы есть поле «Память проекта».
«Общая память проекта» отправляет canonical ID. После первого хода режим
заблокирован; перезагрузка страницы восстанавливает его из ответа платформы.

## Исполнение

```text
AcceptTurn(project ID + shared)
  -> Workspace.GetWorkspace: текущая роль/членство/грант
  -> State: неизменяемая привязка проекта и профиля к диалогу
  -> Dot runner
     -> Workspace.GetWorkspace: текущий доступ
     -> shared Memory Search(workspace=ID): фильтр ДО ранжирования/журнала меток
     -> native personal recall: личные данные и их ограничения
     -> Model Gateway: текущий grant и накопленные метки
```

Shared-ошибка не переводит проект в native/private-project backend.
При восстановлении сохранённого model request новый recall не меняет его байты,
но Workspace-доступ проверяется снова. Без настроенного access validator shared
выполнение запрещено. Lease/epoch остаются отдельной обязанностью State/host.

## Инструменты

| Запрос модели | Поведение |
|---|---|
| memory_search, personal | Прежний native personal API |
| memory_search, project | Только разрешённые активные факты выбранного Workspace |
| memory_search, both | Личный и выбранный проектный поиск, с отдельными источниками |
| memory_remember, personal | Прежний native personal API |
| memory_remember, project | Propose с run-input evidence и обязательным review |
| memory_correct, project | Scoped Read исходного факта, затем Propose(supersedes), без auto-approve |

Модель не задаёт tenant, subject, bank, deployment или другой Workspace.
Runtime строит operation key из durable run и координаты вызова. Источник
обозначается `ahp-shared-memory://<id>`. Scoped Read проверяет проект до выдачи
текста и меток. Исправление сохраняет namespace и ограничения источника; Review
повторно проверяет их под блокировкой. Уже заменённый источник нельзя заменить
вторым устаревшим предложением. Сам факт создания предложения не меняет active.

Личные данные, прочитанные тем же ходом, могут сузить видимость нового
предложения до private_to_user даже внутри рубрики Workspace. Ревью не служит
автоматическим разрешением расширить эту видимость. Это требует явного решения
о разрешённой публикации, а не ослабления label checks ради общего поиска.

## Проверки и границы

Owner/PG тесты проверяют выбранный корпус, отзыв до embedding, scoped Read без
чужого taint, supersedes namespace, наследование и позднее сужение labels,
отказ устаревшего Review. Runtime-тесты проверяют routing, стабильный ключ,
запрет fallback и авторизацию cached replay. Node-тесты проверяют выбор профиля
и восстановление истории.

`TestDotSharedProjectMemoryAndRevocation` проверяет настоящий Qwen, два проекта,
создание и исправление предложений, runner restart, неизменяемый профиль и
прежний токен удалённого участника. Model output читается под перевыпущенным
токеном именно той задачи, которой принадлежит генерация; посторонний root
grant не получает историю генерации. Конкретные source/runtime revisions и
результаты лежат в receipts, наличие теста само по себе не является PASS.

Синтетический код проекта доказывает прохождение данных, не общее качество
поиска на разных языках, нагрузочную ёмкость или HA embedding provider.
