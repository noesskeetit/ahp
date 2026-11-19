# Tier 2B · Суверенный контур — runbook

PDF §10 (verbatim): **"Tier 2B · Суверенный контур — текст не покидает
периметр"**. Гарантия для ИБ (verbatim): **"открытый текст и промпты не покидают
периметр · КИИ, банки"**.

PDF §10 (verbatim): **"Tier 2B соответствует полной приватной ячейке драфта 3.0:
P0 с локальной моделью или P1 с разрешённым внешним inference"**.

Граница: **вся ячейка** в контуре заказчика — вход/выходы, раннеры, Model
Gateway, **локальная LLM (vLLM или SGLang)**, Tool GW, Memory, Artifacts,
PostgreSQL, S3/SDS, microVM, телеметрия, KMS. В облаке Cloud.ru остаются только
релизы и лицензии (подписанные артефакты), опционально — агрегаты без содержимого.

## Что здесь
- `hpa.yaml` — HorizontalPodAutoscaler (state/…; maxReplicas 10) для эластичности
  внутри периметра.
- `pdb.yaml` — PodDisruptionBudget `minAvailable: 1` на state/identity/model
  (устойчивость к выводу узлов).
- `networkpolicy.yaml` — **default-deny ingress** + allow только intra-`ahp` +
  egress DNS/5432/6379/9000. Это и есть механика «текст не покидает периметр»:
  нет egress наружу, кроме внутренних сервисов данных.
- `rbac.yaml` — Role/RoleBinding на чтение секретов только нужными SA.

## Apply (illustrative — НЕ выполнялось на живом кластере периметра)
```
# namespace/SA — переиспользуем tier2a (тот же контракт сервисов)
kubectl apply -f ../tier2a/namespace.yaml -f ../tier2a/serviceaccounts.yaml
kubectl apply -f tier2b/networkpolicy.yaml   # сначала netpol: default-deny
kubectl apply -f tier2b/rbac.yaml
kubectl apply -f tier2b/pdb.yaml -f tier2b/hpa.yaml
# Model GW указывает на ЛОКАЛЬНЮ LLM (vLLM/SGLang) внутри периметра
```

## Verify
- **Нет egress за периметр**: `kubectl -n ahp exec <pod> -- nslookup/nc` к внешним
  адресам должен падать (default-deny + egress allow-list).
- Локальная LLM отвечает (P0) — inference не идёт во внешние API.
- PDB выдерживает drain узла без простоя ячейки.

## Rollback
`kubectl -n ahp rollout undo ...`; NetPol/PDB — `kubectl delete -f`.

## Evidence discipline (source ≠ deployment)
Well-formed NetPol(default-deny)+HPA+PDB+RBAC = **L2_local_test**. Подтверждение
«текст не покидает периметр» на реальном суверенном кластере с локальной LLM +
наблюдаемый отсутствие внешнего egress = **L4 target runtime — OPEN**.
