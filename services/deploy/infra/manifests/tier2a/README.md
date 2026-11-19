# Tier 2A · Хранение у клиента — runbook

PDF §10 (verbatim): **"Tier 2A · Хранение у клиента — данные в покое — у
клиента"**. Гарантия для ИБ (verbatim): **"residency at rest: файлы, чаты и
векторы хранятся у клиента"**.

Граница: в облаке Cloud.ru — gateway, Agents Space (координатор + раннеры),
кастомные деплои, Model Gateway + LLM, Tool Gateway, Memory + Artifacts,
телеметрия без содержимого, S3/SDS-фронт, microVM. В контуре заказчика —
**PostgreSQL (State Store, память), S3, пул microVM, KMS**. API в контур — через
VPC relay / VPC peering, **ключ BYOK** (§10 схема Tier 2A).

> Критично для ИБ (§10, verbatim): **«В Tier 2A облачные раннеры и модель видят
> открытый текст: телеметрия без содержимого, prefix-кэш с солью tenant, без
> выгрузки KV на диск»**. Tier 2A —storage + исполнение у клиента, обработка у
> вендора. Его **нельзя продавать как «платформа развёрнута внутри VPC
> клиента»** — открытый текст обрабатывается в RAM облака.

## Три обязательных условия Tier 2A (все три должны быть зафиксированы письменно)
1. **телеметрия без содержимого** — `telemetry_mode: no-content` (см.
   `tier0/configmap-rls-kms.yaml` как референс контракта; OTel-трейсы не несут payload).
2. **prefix-кэш с солью tenant + запрет выгрузки KV на диск** — `cache_salt` =
   tenant-scoped, `--no-flush-kv-to-disk` (enforce на Model Gateway / vLLM).
3. **письменная фиксация границы обработки** для ИБ заказчика (где именно
   обрабатывается открытый текст).

## Что здесь
- `deployment-state.yaml` / `deployment-identity.yaml` / `deployment-model.yaml`
  — K8s Deployment (`replicas: 2`) + Service на роль (`--role X --listen :8080`),
  readiness `/readyz`, liveness `/healthz`, `readOnlyRootFilesystem`, `runAsNonRoot`.
  Для Tier 2A State Deployment указывает DSN **клиентского** PostgreSQL (в
  EnvironmentFile/Secret `ahp-state-env`, не в git).
- `namespace.yaml` — namespace `ahp`.
- `serviceaccounts.yaml` — SAs `ahp-{state,identity,model}`.

## Apply (illustrative — НЕ выполнялось на живом кластере)
```
kubectl apply -f tier2a/namespace.yaml
kubectl apply -f tier2a/serviceaccounts.yaml
# Secret ahp-*-env должен указывать State Store / S3 / KMS ВНУТРИ контура клиента
kubectl apply -f tier2a/deployment-state.yaml
kubectl apply -f tier2a/deployment-identity.yaml
kubectl apply -f tier2a/deployment-model.yaml
```

## Verify
- `kubectl -n ahp get pods -o wide` → State Pod реально коннектится к **клиентскому**
  PostgreSQL (не облачному).
- Кросс-регион задержка State API (облако↔контур) должна укладываться в SLO §11
  (Durable ACK p95 ≤ 300 ms) — это отдельное измерение на пилоте, не здесь.
- Все 3 условия Tier 2A проверены конфигурацией (no-content telemetry, cache salt,
  boundary doc).

## Rollback
`kubectl -n ahp rollout undo deploy/ahp-{state,identity,model}`.

## Evidence discipline (source ≠ deployment)
Well-formed Deployments/NetPol + указание клиентского PostgreSQL + кодирование 3
условий = **L2_local_test**. `kubectl apply` + rollout + измеренная кросс-регион
State-задержка на пилоте = **L4 target runtime — OPEN**.
