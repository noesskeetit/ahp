# Tier 0 · Shared SaaS — runbook

PDF §10 (verbatim): **"Tier 0 · Shared SaaS — логическая изоляция, общий пул"**.
Гарантия для ИБ (verbatim): **"логическая и криптографическая изоляция · SMB и
разработчики"**.

Граница: **всё** в облаке Cloud.ru в общем пуле. В контуре заказчика остаётся
только вход/выходы и обращение к корпоративным API **через VPC relay** (§10
схема Tier 0). Контракт сервисов и бинарь `cmd/ahp` — те же, что и на Tier 1/2A/2B.

## Что здесь
- `deployment-shared-saas.yaml` — Deployment единого shared-пула (`--role all`,
  `replicas: 3`) + Service + ServiceAccount. Один парк реплик на все tenants
  (в отличие от Tier 1, где namespace/процесс на tenant).
- `configmap-rls-kms.yaml` — контракт логической изоляции (PostgreSQL RLS по
  `tenant_id`) и криптографической (KMS keyref-шаблон на tenant). Значения —
  шаблоны; реальных ключей/DSN здесь нет.

## Apply (illustrative — НЕ выполнялось на живом K8s)
```
kubectl create namespace ahp-shared || true
kubectl apply -f tier0/configmap-rls-kms.yaml
kubectl apply -f tier0/deployment-shared-saas.yaml
# применить RLS-политики к State Store (миграция)
./ahp migrate --database-url "$STATE_DB_URL"
```

## Verify
- `kubectl -n ahp-shared rollout status deploy/ahp-shared-pool`
- `curl -fsS https://ahp-shared.ahp-shared/healthz` и `/readyz`
- Изоляция tenant: запрос под `tenant_id=A` не должен видеть строки tenant `B`
  (проверяется кросс-tenant smoke — на этом уровне в репозитории это L2-сценарий
  `tests/e2e` data-isolation, НЕ живой shared-стенд).

## Rollback
`kubectl -n ahp-shared rollout undo deploy/ahp-shared-pool`. Начатые задачи
доразвиваются на своей версии до конца drain (PDF: «начатые задачи — на своей
версии до конца drain»).

## Evidence discipline (source ≠ deployment)
Well-formed YAML + выражение RLS/KMS = **L2_local_test**. Живой shared-pool под
нагрузкой с проверенной изоляцией tenant = **L4 target runtime — OPEN** (требует
реальный кластер Cloud.ru Managed Kubernetes; здесь не выполнялось).
