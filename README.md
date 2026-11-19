# AHP Go Platform

Весь код платформы собран в одном репозитории. Исходники разложены по папкам в [`services/`](services/). В [`platform/`](platform/) лежат общие файлы сборки, Compose, интеграционные тесты и документация.

Снимок исходного кода соответствует ревизии AHP `68a169b4`.

## Посервисный каталог

| Компонент | Где открыть код |
|---|---|
| **Новый State Store** | [Хранилище](services/state-store/internal/statejournal) · [схема PostgreSQL](services/state-store/internal/statejournal/schema.sql) |
| **Новый State API** | [Точка входа](services/state-api/cmd/state-api) · [HTTP API](services/state-api/internal/statejournal/http.go) · [запуск и настройка](services/state-api/docs/compact-state-service.md) |
| **Адаптеры харнессов** | [LangGraph](services/harness-adapters/sdk/python/ahp_state/journal_checkpointer.py) · [ADK](services/harness-adapters/sdk/python/ahp_state/journal_adk.py) · [DeepSeek](services/harness-adapters/adapters/deepseek) · [Hermes](services/harness-adapters/adapters/hermes) · [OpenClaw](services/harness-adapters/adapters/openclaw) · [проверки восстановления](services/harness-adapters/profiling/state-journal) |
| State API для thread/run | [HTTP API](services/state-service/internal/stateapi) · [хранилище](services/state-service/internal/state) |
| Identity | [Сервис](services/identity-service/internal/identity) · [HTTP API](services/identity-service/internal/identityapi) |
| Model Gateway | [Маршрутизация и провайдеры](services/model-gateway/internal/model) · [HTTP API](services/model-gateway/internal/modelapi) |
| Tool Gateway | [Инструменты и журнал операций](services/tool-gateway/internal/tools) |
| Artifacts | [Хранилище](services/artifacts-service/internal/artifacts) · [HTTP API](services/artifacts-service/internal/artifactsapi) · [парсер](services/artifacts-service/cmd/artifact-parser) |
| Memory | [Сервис памяти](services/memory-service/internal/memory) · [настройка](services/memory-service/infra/memory) |
| Workspace | [Управление рабочими каталогами](services/workspace-service/internal/workspace) |
| Compute | [Управление вычислениями и песочницами](services/compute-service/internal/compute) · [compute host](services/compute-service/cmd/compute-host) |
| Registry | [Деплойменты и регистрация агентов](services/registry-service/internal/registry) |
| Execution и A2A | [Исполнение](services/execution-service/internal/executor) · [A2A gateway](services/execution-service/internal/gateway) · [связь с раннерами](services/execution-service/internal/execbridge) |
| Triggers | [Задания по расписанию и событиям](services/triggers-service/internal/triggers) |
| Evals | [Оценка агентов](services/evals-service/internal/evals) |
| Agents Space | [Серверная логика](services/agents-space/internal/space) · [раннер](services/platform-runtime/cmd/as-runner) |
| Agent Runtime | [Агент](services/agent-runtime/cmd/ahp-agent) · [образ](services/agent-runtime/infra/agent-workload) · [интеграция Emil](services/execution-service/adapters/emil) |
| SDK | [Go](services/sdk/sdk/go) · [Python](services/sdk/sdk/python) · [TypeScript](services/sdk/sdk/ts) · [примеры](services/sdk/sdk/examples) |
| Frontend | [Интерфейс](services/frontend/web/aifactory) |
| Общие библиотеки и запуск платформы | [Контракты и библиотеки](services/shared-common/pkg) · [процесс ahp](services/platform-runtime/cmd/ahp) |
| Deploy и интеграция | [Контроллер деплоя](services/deploy/internal/deploycontroller) · [манифесты](services/deploy/infra/manifests) · [Compose](platform/compose.yaml) · [тесты](platform/tests) |

## Сборка из одного клона

Для подготовки дерева нужны Git и Python 3.9+. Для сборки бинарников нужен Go 1.26+.

```bash
git clone https://github.com/noesskeetit/ahp.git
cd ahp
python3 assemble.py .build/ahp
cd .build/ahp
go build ./cmd/...
```

`assemble.py` берёт файлы из папок сервисов и `platform/` и собирает локальное дерево Go-модуля. Так сохраняются пути импортов, Docker-контексты и границы пакетов. Каталог назначения должен быть новым. Подготовка дерева выполняется локально.

Дальнейшие команды приведены в [руководстве платформы](platform/README.md) и [инструкции State API](services/state-api/docs/compact-state-service.md). Выполняй их из собранного дерева.

Секреты для локального запуска задаются через `.env` или внешнее хранилище. [Шаблон переменных окружения](platform/.env.example).
