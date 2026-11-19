# Закрытие общей сборки AHP — план

> **For agentic workers:** Execute inline with superpowers:executing-plans.

**Goal:** Исправить обнаруженные общие регрессии и выполнить полный контроль продукта с настоящими зависимостями в поддерживаемом локальном Linux-контуре.

**Architecture:** Существующие сервисы и SDK; тестовые данные должны соответствовать действующим контрактам. Без ослабления mTLS, GC roots, quota и live-model gates. Linux-процессы запускаются только в собственной Docker-среде на Mac.

**Tech Stack:** Go 1.26.2, Node, Python, PostgreSQL, Redis, Vault, MinIO.

**Spec:** Пользовательское «ну доделай» после отчёта о RED whole-platform gate; PDF rev3.2 остаётся исходным требованием.

## Global Constraints

Только Mac, одна тяжёлая suite одновременно, bounded build/test, контроль RAM/swap/диска. Только собственные БД и контейнеры ahp-handbook. Секреты не печатать/не коммитить. Базовая State-ветка 89c80e80 и её результаты неизменны. МикроВМ разрешены на моках, security sandbox/квоты этим не подменять.

## Review Focus

- Общая suite не становится зелёной из-за skips или исключения пакетов.
- Тесты удаления не обходят действующие checkpoint references.
- Linux квоты реально ограничивают процесс, на Mac исполнение остаётся fail-closed.
- Исправление SDK-тестовой гонки не ослабляет отказ без клиентского сертификата.
- Live-model результат отделён от fixture, секреты остаются в private Vault/env.

## Task 1: Общие детерминированные регрессии

- [ ] Воспроизвести profiling fixture без revision, provider accounting, UI manifest drain и mTLS logging race.
- [ ] Исправить причины, сохранить отрицательные проверки, прогнать Go/Node SDK tests.
- [ ] Исправить/усилить core checkpoint GC проверку через реальные owners, commit.

## Task 2: Воспроизводимый локальный Linux-контур

- [ ] Поднять изолированный Linux test runner с закреплёнными инструментами и отдельными cache/private outputs.
- [ ] Запустить реальные PG/Redis/Vault/MinIO с правильными alias/ownership; проверить readiness.
- [ ] Прогнать Linux-only quota/sandbox/compute участки; исправлять реальные дефекты через RED→GREEN.

## Task 3: Полная сборка и рабочий путь

- [ ] npm run check, без package exclusions; корректно зафиксировать внешние prerequisites.
- [ ] Реальный процесс, smoke и основной platform/Dots путь; fixture/live отдельно.
- [ ] Самостоятельный review diff, повтор изменённых проверок, отчёт с исходниками и exact evidence.
