# A2A Go SDK v2.7.0: интеграция шлюза AHP

Источник: `github.com/a2aproject/a2a-go/v2@v2.7.0`; протокол `a2a.Version = "1.0"` (`a2a/core.go`). Go ≥ 1.25.0 (`README.md`).

## Разделение ответственности

- **Шлюз AHP**: durable-эффекты, идемпотентность task/message, outbox, owned `RequestHandler`/`taskstore.Store`, `push.ConfigStore`/`push.Sender`, `eventqueue.Manager`.
- **State API**: рабочее состояние агента; не является SDK `taskstore.Store`.
- **Agents Space Coordinator**: отдельный слой; в этих интерфейсах не описан.

## Transport-agnostic RequestHandler

`a2asrv.RequestHandler` (`a2asrv/handler.go`):

```go
SendMessage(context.Context, *a2a.SendMessageRequest) (a2a.SendMessageResult, error)
SendStreamingMessage(context.Context, *a2a.SendMessageRequest) iter.Seq2[a2a.Event, error]
GetTask(context.Context, *a2a.GetTaskRequest) (*a2a.Task, error)
ListTasks(context.Context, *a2a.ListTasksRequest) (*a2a.ListTasksResponse, error)
CancelTask(context.Context, *a2a.CancelTaskRequest) (*a2a.Task, error)
SubscribeToTask(context.Context, *a2a.SubscribeToTaskRequest) iter.Seq2[a2a.Event, error]
GetTaskPushConfig(context.Context, *a2a.GetTaskPushConfigRequest) (*a2a.PushConfig, error)
ListTaskPushConfigs(context.Context, *a2a.ListTaskPushConfigRequest) (*a2a.ListTaskPushConfigResponse, error)
CreateTaskPushConfig(context.Context, *a2a.PushConfig) (*a2a.PushConfig, error)
DeleteTaskPushConfig(context.Context, *a2a.DeleteTaskPushConfigRequest) error
GetExtendedAgentCard(context.Context, *a2a.GetExtendedAgentCardRequest) (*a2a.AgentCard, error)
```

Шлюз реализует интерфейс напрямую (proxy/durable), либо использует `a2asrv.NewHandler(executor, options...)`. Второй вариант требует заменить все in-memory зависимости персистентными; иначе дефолты небезопасны для restart/cluster.

## Персистентные зависимости handler

```go
WithTaskStore(store taskstore.Store)
WithEventQueueManager(manager eventqueue.Manager)
WithPushNotifications(store push.ConfigStore, sender push.Sender)
WithUpdateMaterializer(m taskstore.UpdateMaterializer)
WithClusterMode(ClusterConfig{QueueManager, WorkQueue, TaskStore, ContextCodec})
```

Без `WithTaskStore` включается `taskstore.NewInMemory`; без `WithEventQueueManager` — `eventqueue.NewInMemoryManager`. Для durable-шлюза это недопустимо. Cluster mode panic, если `TaskStore` или `QueueManager` равны `nil`.

`taskstore.Store` (`a2asrv/taskstore/api.go`):

```go
Create(ctx context.Context, task *a2a.Task) (TaskVersion, error)
Update(ctx context.Context, update *UpdateRequest) (TaskVersion, error)
Get(ctx context.Context, taskID a2a.TaskID) (*StoredTask, error)
List(ctx context.Context, req *a2a.ListTasksRequest) (*a2a.ListTasksResponse, error)
```

Контракт: `Create` → `ErrTaskAlreadyExists`; `Get` → `a2a.ErrTaskNotFound`; `Update` при несовпадении `UpdateRequest.PrevVersion` → `ErrConcurrentModification`. `StoredTask` хранит `Task`, `Version`, `User`. `UpdateRequest` содержит `Task`, `Event`, `PrevTask`, `PrevVersion`; store не мутирует Task/Event. Это точка CAS/идемпотентной записи; outbox пишется в той же транзакции, что и Create/Update.

## JSON-RPC и REST

```go
NewJSONRPCHandler(handler RequestHandler, options ...TransportOption) http.Handler
NewRESTHandler(handler RequestHandler, opts ...TransportOption) http.Handler
NewTenantRESTHandler(tenantTemplate string, handler RequestHandler, opts ...TransportOption) http.Handler
WithTransportKeepAlive(interval time.Duration) TransportOption
WithTransportPanicHandler(handler func(r any) error) TransportOption
```

JSON-RPC: единственный `POST`; методы `message/send`, `message/stream`, `tasks/get|list|cancel|resubscribe`, push-config CRUD и extended card. REST: `POST /message:send`, `POST /message:stream`, `GET /tasks`, `GET|POST /tasks/{idAndAction}` (`:cancel`, `:subscribe`), CRUD push configs и extended card; `NewTenantRESTHandler` извлекает tenant из path. Оба binding принимают любой `RequestHandler`, поэтому AHP proxy подключается без копирования protocol codec.

## Идентичность, task/message ID и resume

- Аутентификация выполняется middleware до binding; в `CallContext` помещается `a2asrv.User{Name, Authenticated, Attributes}` (`a2asrv/auth.go`).
- `taskstore.StoredTask.User` — owner, установленный Authenticator при Create; durable store обязан проверять tenant/owner на Get/List/Update/push.
- `a2a.Message.ID` — client-generated UUID (`a2a.NewMessageID`); `TaskID`/`ContextID` находятся в `a2a.TaskInfo`. Шлюз использует `(tenant, message.ID)` как key идемпотентности SendMessage и сохраняет маппинг message→task до enqueue.
- Streaming возвращает `iter.Seq2[a2a.Event, error]`; события — `*a2a.Message`, `*a2a.Task`, `*TaskStatusUpdateEvent`, `*TaskArtifactUpdateEvent`.
- SSE resume: `SubscribeToTask`/REST `:subscribe`/JSON-RPC `tasks/resubscribe`. Дефолтный handler вызывает `execManager.Resubscribe`; при отсутствии live-подписки возвращает non-terminal Task или `ErrUnsupportedOperation` для terminal. Для кросс-процессного resume нужен durable `eventqueue.Manager`/cluster queue; SDK не предоставляет встроенного durable replay.

## Push

```go
SendPush(ctx context.Context, config *a2a.PushConfig, event a2a.Event) error
Save(ctx context.Context, taskID a2a.TaskID, config *a2a.PushConfig) (*a2a.PushConfig, error)
Get(ctx context.Context, taskID a2a.TaskID, configID string) (*a2a.PushConfig, error)
List(ctx context.Context, taskID a2a.TaskID) ([]*a2a.PushConfig, error)
Delete(ctx context.Context, taskID a2a.TaskID, configID string) error
DeleteAll(ctx context.Context, taskID a2a.TaskID) error
```

`push.Sender.SendPush` вызывается синхронно на смене состояния; ошибка останавливает execution. Production-реализация должна быть durable/idempotent (обычно enqueue outbox и фоновая доставка), а не direct HTTP. Конфигурации хранятся per Task и могут иметь несколько config ID.

## Совместимость v0.3

В проверенных первичных исходниках v2.7.0 (`README.md`, `a2a/core.go`, `a2asrv/handler.go`, `a2asrv/jsonrpc.go`, `a2asrv/rest.go`) официальный compatibility route v0.3 не объявлен; экспортируется только `a2a.Version = "1.0"`. Пути v0.3 не выдумываем; при необходимости отдельный legacy adapter вне этого SDK.
