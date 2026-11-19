# Python standalone custom agent — какие SDK-пакеты реально исполняются

Урок класса DEF-VERDICT-1 (2026-11-09): прежняя ревизия утверждала «the AHP Python SDK
publishes no State client» — это было ЛОЖНО: пакет `ahp_state` экспортирует
`StateAPIClient` (`sdk/python/ahp_state/__init__.py`), а проверка искала клиент только в
`ahp_clients`. Правило: перед любым утверждением «SDK X не умеет Y» — grep по всему
`sdk/python/*`, а не по одному пакету.

| Пакет SDK | Операция | Где исполняется live в этой ноге |
|---|---|---|
| `ahp_state` (`StateAPIClient`) | CreateThread, CreateRun, ClaimRunFor, Heartbeat, PutCheckpoint, GetCheckpoint, PutKV, GetKV, Release | `custom_agent.py::state_work` (fence-операции под task_token из claim, как в Go SDK) |
| `ahp_clients.model` (`ModelClient`) | SubmitModel (реальный Qwen через Model GW) | `custom_agent.py` (b/a) |
| `ahp_clients.artifacts` (`ArtifactsClient`) | Put, Download (байт-в-байт) | `custom_agent.py` (b/b) |
| `/v1/space/*` | — | не используется: standalone-путь существует именно чтобы это доказать |

Исторический артефакт `…custom-agent-standalone-python-20261109T204550Z.json` содержит
устаревшую строку про «no State client» — он не переписывается (историческое evidence,
классификация директора: known-дефект отчётности); все новые артефакты после этого
коммита исполняют State-ногу фактически.
