# SDK Handoff — mac-sdk-ci (rev 3)

## Что реализовано (candidate)

### Go SDK (`pkg/sdk/`, `sdk/go/state/`)

| Файл | Назначение |
|---|---|
| `sdk/go/state/client.go` | HTTP client: mTLS, token, 16 MiB bounded, no-redirect |
| `sdk/go/state/runs.go` | Thread/Run CRUD, ClaimRun, Heartbeat, Release, Suspend, **DeleteThread** |
| `sdk/go/state/checkpoints.go` | PutCheckpoint, GetCheckpoint, ListCheckpoints, PutWrites, GetWrites |
| `sdk/go/state/kv.go` | GetKV, PutKV, DeleteKV |
| `sdk/go/state/intents.go` | RecordIntent, UpdateIntent |
| `sdk/go/state/inbox.go` | Enqueue, ReadInbox, AckInbox |
| `sdk/go/state/types.go` | Wire types, fence validation, DeleteThread DTO |
| `pkg/sdk/config.go` | SDK config with validation and defaults |
| `pkg/sdk/lease.go` | ActiveLease: auto-heartbeat, Suspend, lease-loss detection |
| `pkg/sdk/checkpoint.go` | CheckpointSaver, AHPStore — scoped abstractions |
| `pkg/sdk/app.go` | App: A2A task lifecycle, RunOnce, RunLoop, GraphFunc, InterruptError |
| `pkg/sdk/intent.go` | OperationKey (deterministic), IntentRecorder |
| `pkg/sdk/a2a_executor.go` | LangGraphExecutor: a2asrv.AgentExecutor (A2A adapter) |
| `pkg/sdk/check/check.go` | **ahp-sdk check**: import-lint + token-flow analysis (wired) |
| `pkg/sdk/check/tokenflow.go` | **Token-leak data-flow**: variable tracing, sink detection |
| `pkg/sdk/check/check_test.go` | 3 import-lint tests |
| `pkg/sdk/check/tokenflow_test.go` | **5 token-leak tests** |
| `pkg/sdk/cmd/ahp-sdk-check/main.go` | CLI binary (multi-root) |

### Python adapter (`sdks/python/`) — LangGraph 0.6.11, checkpoint 2.1.2

| Файл | Назначение |
|---|---|
| `ahp_langgraph/client.py` | Async State API client (httpx) + **delete_thread** |
| `ahp_langgraph/checkpoint_saver.py` | AHPCheckpointSaver (BaseCheckpointSaver, aput_writes task_path) |
| `ahp_langgraph/store.py` | AHPStore (BaseStore) |
| `tests/test_conformance.py` | 11 conformance tests against real LangGraph |
| `tests/test_adapter_behavior.py` | **5 real-graph tests** (resume, pending writes, list, aget) |
| `tests/test_sync_and_delete.py` | **7 tests** (sync NotImplementedError, delete_thread cascade) |
| `pyproject.toml` | langgraph==0.6.11, langgraph-checkpoint==2.1.2 (pinned) |

### Тесты и CI

| Файл | Назначение |
|---|---|
| `tests/sdk/sdk_test.go` | 6 integration tests (mock HTTP server) |
| `tests/sdk/e2e_pg_test.go` | 2 PG-backed E2E tests (build tag `e2e`) |
| `acceptance/mac-sdk-ci/check.sh` | 12-step CI gate: build, 7 tests, vet, ahp-sdk check, Python |

## CI результат (Mac arm64, Go 1.26.2, Python 3.9.6, 2026-10-30)

```
=== AHP SDK CI Acceptance Check ===
Go: go version go1.26.2 darwin/arm64
  [build-all] PASS
  [build-sdk] PASS
  [test-contracts] PASS
  [test-auth] PASS
  [test-labels] PASS
  [test-netpolicy] PASS
  [test-transport] PASS
  [test-sdk-state] PASS
  [test-sdk-integ] PASS
  [vet-all] PASS
  [ahp-sdk-check] PASS
  [python-conformance] PASS
Results: 12 passed, 0 failed
ALL CHECKS PASSED
```

## Token-leak lint (§4.2 / §6.2) — РЕАЛИЗОВАНО

`pkg/sdk/check/tokenflow.go` — data-flow AST analysis:
- **Token sources**: `TaskToken`, `Token()`, `.task_token`, `.Bearer`, `.Authorization`
- **Sinks**: `exec.Command`, `log.*`, `fmt.Sprintf`, `fmt.Println`, `http.Get/Post/Head/Dial`
- **Self-lint**: 16 SDK files, 0 violations
- **Demo (leaky agent)**: 6 violations detected (exec+token, log+token, sprintf+token, HTTP+token)

## Python: 23/23 PASS (2026-10-30)

```
test_adapter_behavior.py: 5 passed (checkpoint save/restore, pending writes, resume, list, empty)
test_conformance.py: 11 passed (subclass, methods, data flow)
test_sync_and_delete.py: 7 passed (get_tuple/put/put_writes/list raise, delete_thread×3)
```

## LangGraph adapter: доказанное поведение

| Операция | Тест | Результат |
|---|---|---|
| Checkpoint save/restore | `test_checkpoint_save_and_restore` | Graph invoke → checkpoint stored → `aget_tuple` retrieves ✅ |
| Pending writes | `test_pending_writes` | `aput_writes` → writes stored with task/channel ✅ |
| Resume | `test_resume_from_checkpoint` | Second invoke uses existing checkpoint ✅ |
| List newest-first | `test_list_checkpoints` | `alist` returns newest-first ✅ |
| Sync methods | `TestSyncMethodsRaise` (×4) | `get_tuple`/`put`/`put_writes`/`list` → `NotImplementedError` ✅ |
| DeleteThread | `TestDeleteThread` (×3) | HTTP call → thread removed, fence required ✅ |

## Compile fixes для state-ownera (НЕ изменение API)

| Файл | Строка | Проблема | Fix |
|---|---|---|---|
| `internal/state/checkpoint_parts.go` | 271 | `data := w.Data` (shadow: `no new vars`) | `data = w.Data` |
| `internal/state/kv.go` | 348 | `kvOpReplay` — 7 args, want 5 | убраны `ns, key` |
| `internal/state/kv.go` | 449 | то же | то же |

Эти три строки блокировали `go build ./...`. Изменена **только вызываемая сторона**.

## HTTP E2E: фиксация gate (req 4)

**Что сделано SDK:**
- Go SDK client полностью работает через HTTP (`sdk/go/state/client.go`)
- Все 22 Go HTTP-теста проходят (httptest + mTLS)
- Go integration tests (`tests/sdk/sdk_test.go`) используют mock HTTP server
- Python behavior tests используют реальный HTTP (HTTPServer, не mock)

**Gate для переключения на State HTTP:**
- `tests/sdk/e2e_pg_test.go` (build tag `e2e`) — переключается на HTTP client
  при наличии State API HTTP routes. Конфигурация: `AHP_STATE_URL` + cert.
- SDK conformance полностью готова к HTTP: клиент `sdk/go/state/client.go`
  уже работает по HTTP, роуты state-ownera — единственное препятствие.

**Чего SDK НЕ делает (scope state-owner):**
- Не реализует State HTTP routes (POST /v1/state/*)
- Не меняет `internal/state/` кроме 3 compile-fix (не API change)

## Ownership mac-sdk-ci (paths)

```
sdk/
sdks/
pkg/sdk/
tests/sdk/
docs/sdk/
acceptance/mac-sdk-ci/
```

## Честный Remaining (PDF rev 3.2)

| PDF требование | Статус | Доказательство / Блокер |
|---|---|---|
| §4.2 A2A adapter (AHPLangGraphApp) | ✅ Done | `pkg/sdk/a2a_executor.go` |
| §4.2 AHPCheckpointSaver (Go+Python) | ✅ Done | 23/23 Python, Go tests |
| §4.2 AHPStore (Go+Python) | ✅ Done | `pkg/sdk/checkpoint.go`, `store.py` |
| §6.2 Token-leak lint | ✅ Done | `tokenflow.go`, 5 tests, self-lint 0 |
| §4.2 ahp-sdk check (no-direct-db) | ✅ Done | `check.go`, 3 tests |
| §4.2 Contract tests SDK | ✅ Done | CI gate 12/12 |
| §4.2 delete_thread SDK client | ✅ Done | Go+Python, тесты |
| LangGraph resume | ✅ Tested | `test_resume_from_checkpoint` |
| LangGraph pending writes | ✅ Tested | `test_pending_writes` |
| LangGraph sync methods | ✅ Tested | 4 tests: `NotImplementedError` |
| HTTP E2E via State API | ⚠️ Blocked | **Требует HTTP routes от state-owner.** SDK client готов. |
| §4.2 Chaos: pod restart mid-run | ❌ Not done | Требует multi-replica + running State API |
| §6.8 delete cascade (server) | ❌ Not done | Server-side: state-owner scope. SDK client method есть. |
| CI pipeline YAML | ⚠️ Partial | `check.sh` работает; нет GitLab/GitHub CI YAML |
