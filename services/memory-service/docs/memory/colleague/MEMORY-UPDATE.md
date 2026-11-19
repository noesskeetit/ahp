# Актуализация AHP и интеграции памяти

Дата: 2 ноября 2026. Все 20 веток `main` обновлены fast-forward до загруженных `origin/main`. Новый общий source checkpoint автора — `7c810ec2aa5d0d650205807f6d0e12489fd0fe62`; integration содержит также следующий commit с уточнением readiness matrix.

Локальная интеграция памяти сохранена поверх обновлений. Исходные правки и stash references сохранены локально перед актуализацией; эти резервные копии не входят в поставку. Четыре текстовых конфликта разрешены с сохранением новых upstream путей. Изменения подготовлены для согласованной ветки `feature/hindsight-memory`; исходный `source-map.json` автора сохранён.

## Адаптация контрактов

- Сохранены новые streaming/replay, durable acknowledgements и бюджетные механизмы Core. При replay используется принятый immutable model request, новый recall не выполняется. Происхождение memory-блоков сохраняется в журнале отдельно от provider payload, чтобы последующий checkpoint/resume не дублировал инъекцию.
- Исправлена cold-rehydration привязка: `ProjectID` восстанавливается из durable payload. Добавлен regression test.
- Hindsight owner реализует новый `tenantMemoryOwner`: tenant fence, inventory и subject erasure через штатный durable coordinator. В `ahp_longterm` добавлена owner directory; регистрация владельца и запись проходят одну транзакцию под shared tenant lock, fence использует exclusive lock.
- После tenant fence новые чтения/записи отвергаются, автоматическая очистка банков останавливается до явного subject-erasure intent. Это сохраняет Memory при legal hold в новом coordinator. Tenant fence — административный lifecycle, общего tenant-банка не добавлено.
- При наличии прежних `ahp_memory` tables их owner API сохраняется для inventory и erasure. Legacy corpus не мигрирует в Hindsight автоматически, но учитывается при удалении. Его embedding policy, read-label sink и reindex-код автора сохранены.
- Hindsight bridge использует новый штатный `AddGrantLabels`; дублирующий адаптер записи меток убран. State completion snapshot сохраняет новый `traceparent` run.
- Сохранены upstream обновления Workspace/Artifacts browser planes, OAuth, registry history и остальные изменения автора.

## Миграция

`longterm/pgstore.Migrate` создаёт schema version 2 и однократно восстанавливает directory существующих банков из серверных identity в turn/job records. Для перехода с версии 1 необходимо остановить старые Memory writers перед миграцией: они ещё не участвуют в новом tenant lock. Все новые replicas должны использовать эту версию. Пустой native Hindsight corpus не создаётся на пользователя до первой разрешённой записи.

## Проверка

Полная сборка и компиляция тестов всех 20 модулей пройдены после актуализации. Целевые Core/Space memory/replay tests проходят с `-race`; frontend Go suite и Node tests также проходят. Точный состав — [VALIDATION](../memory-service/VALIDATION.md). Это не объявляется полной runtime/E2E приёмкой платформы или Hindsight extraction.

PG-приёмка оставлена команде по решению пользователя. Для изолированной PG-проверки подготовлен [validate-memory-pg.sh](scripts/validate-memory-pg.sh). Он создаёт новый временный PostgreSQL cluster и сохраняет результат в ignored `integration/.memory-validation`; в sandbox запрещено открытие даже Unix socket. Native Hindsight и полный браузерный deployment здесь не поднимались.

## Зафиксированные revisions

| Репозиторий | Upstream baseline перед интеграцией |
|---|---|
| agent-runtime | `b8e8288dc6e3` |
| agents-space | `f702057b9abc` |
| artifacts-service | `a0e1885c2899` |
| compute-service | `066be49d350c` |
| deploy | `82576b2a9ac4` |
| evals-service | `cb081b7c3cdc` |
| execution-service | `4299fd2896a0` |
| frontend | `2105e831c435` |
| identity-service | `e667296e40e5` |
| integration | `69ec6591d9e6` |
| memory-service | `157998a43043` |
| model-gateway | `c8c16dd37027` |
| platform-runtime | `a82d8a443202` |
| registry-service | `9caad4c61abb` |
| sdk | `c7a82b68f799` |
| shared-common | `30ac62164b68` |
| state-service | `68459ea05ca3` |
| tool-gateway | `678ffd25e584` |
| triggers-service | `5432f9c51a8c` |
| workspace-service | `b51cf2d2a652` |
