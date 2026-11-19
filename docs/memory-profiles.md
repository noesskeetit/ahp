# Два профиля Memory в одном процессе

Личная память коллеги остаётся профилем по умолчанию: `AHP_MEMORY_BACKEND=hindsight`.
Её API `/v1/memory/` обслуживает personal и приватные project. Общий проект с
несколькими участниками не становится приватным банком Hindsight автоматически.

Для отдельных записей из PDF оператор дополнительно включает
`AHP_ENABLE_SHARED_MEMORY=1`. Новый путь — `/v1/memory/shared/{Operation}`.
Он использует существующий PDF Memory Store, собственные записи PostgreSQL,
живой Identity grant, журнал меток и текущие права Workspace. Личные факты не
копируются, не переиндексируются в общий банк и не объединяются при поиске.
Это дополнительный профиль того же сервиса, а не смена владельца личной памяти.

## Запросы

Все вызовы требуют workload mTLS и токен задачи. Tenant, deployment и subject
берутся из проверенной идентичности, а не из JSON. Токен сотрудника сюда не
передаётся. Например, тело запроса Propose:

```json
{
  "kind": "workspace",
  "workspace": "<канонический ID проекта из Workspace API>",
  "text": "Согласованное правило проекта",
  "op_key": "<стабильный ключ этой записи>",
  "source_kind": "user_statement",
  "evidence": [{"source_kind": "user_statement", "detail": "<проверяемый источник>"}]
}
```

`Propose` создаёт proposed-запись. `Review` с отдельным ключом и правом
`memory:review` переводит её в active либо rejected. Поиск возвращает active.
`Read`, `AddLabels`, `Delete`, lifecycle и idempotency сохраняют контракт PDF
Memory Store. `Search.kind` может сузить поиск до user, workspace или agent.

Для workspace используются **ID**, как в Identity и Workspace API. Исторический
`ListWorkspaces` продолжает возвращать имена; новый owner-метод `ListWorkspaceIDs`
предоставляет канонические ID с той же проверкой живых прав. Удалённый участник
не может ни прочитать запись, ни создать новое предложение. Приватный источник
в workspace остаётся доступен только автору; в agent scope такой источник
отклоняется. Agent-запись дополнительно ограничена её deployment.

## Поиск и эксплуатация

Shared-профиль требует live Identity. Оператор отдельно задаёт существующий
embedding policy/profile. `AHP_MEMORY_SEARCH_MODE=lexical` означает явный
деградированный режим; он не подтверждает семантическое качество. Для hybrid
нужны настоящий embedding endpoint, версия пространства и политика разрешённых
маршрутов/меток. Режим не выбирается молча по доступности провайдера.

Оба API публикуются только после успешной подготовки обоих владельцев. При
ошибке настройки native Hindsight уже подготовленный shared API остаётся
недоступным. Миграция PDF Store выполняется перед native mount: существующий
координатор удаления обнаруживает дополнительный owner и включает его записи
в subject/tenant erasure. Собственные ACL Hindsight не ослабляются.

Потребитель выбирает нужный API явно. Go-пакет `sdk/go/sharedmemory` и классы
`ahp_clients.SharedMemoryClient` / `AsyncSharedMemoryClient` вызывают только
`/v1/memory/shared/`. Они требуют caller-owned mTLS transport и актуальный токен
задачи, запрещают redirects и выполняют одну попытку. При потерянном ответе
мутации возвращается неопределённый исход; явный повтор использует прежние
аргументы и `op_key`. Propose не делает запись active. Review требует отдельного
права `memory:review`, независимо от того, каким SDK вызван метод.

Существующие `sdk/go/memory`, `ahp_memory` и инструменты Dot сохраняют native
personal/private-project путь. Shared scope нельзя получить из них простой
заменой personal на workspace. Общего неограниченного поиска по двум владельцам
и автоматической публикации личных фактов нет. Dot подключает общий проект
через [явный durable-профиль](dot-shared-project-memory.md): выбранный Workspace
проверяется владельцами до чтения, а personal остаётся отдельным источником.

SDK предоставляет `DeleteSubject` с явным названием и отдельными правами.
Эта операция не является per-operation Fence. Канонический Store теперь принимает
`Get {op_key}` и `Fence {op_key}`: квитанция операции и закрытие одного ключа.
`Read {id}` читает запись. Правила гонок, миграции и неопределённого ответа —
в [контракте ключей](memory-operation-keys.md). Native Hindsight API не меняется.

## Проверки этого изменения

`TestSupplementalPDFMemoryPreservesPrivateHindsight`: реальные PostgreSQL,
Identity, Workspace, mTLS/JWT; общий workspace/agent, review, private ceiling,
отзыв чтения и записи, native refusal для общего проекта, atomic mount и owner
удаления. Это контрактный lexical-тест без вызова native extraction/LLM.

`TestSupplementalMemoryRequiresLiveIdentityBeforePublishing`: отказ до публикации
API при отсутствии live authority. Workspace/Memory регрессия проверяет owner ACL
и PostgreSQL; embedding fixtures в ней не являются доказательством качества
настоящей модели. Главный стенд обновляется и принимается отдельно от этих тестов.

`TestSharedMemorySDKsOnDeployedSemanticOwner`: Go Propose/replay, запрет обычному
токену согласовывать запись, отдельный куратор, PostgreSQL и настоящий neural
embedding HTTP backend; фактический lexical match=0 и первое место парафразы
среди трёх distractors. Python читает тот же record; активный private record
исключён; прежний токен удалённого участника не читает, не находит и не пишет.
Лабораторный профиль — offline CPU BAAI/bge-small-en-v1.5, 384 измерения и
английский синтетический корпус. Это не multilingual quality или capacity test.
