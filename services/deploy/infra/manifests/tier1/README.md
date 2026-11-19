# Tier 1 · Namespace — runbook

PDF §10 (verbatim): **"Tier 1 · Namespace — выделенные пулы и БД, BYOK"**.
Гарантия для ИБ (verbatim): **"нет шумных соседей, ключи шифрования клиента"**.

Граница: вычисление и хранение — в облаке Cloud.ru, но в **выделенных**
namespace / runner-pool / microVM, со **своим** State Store и бакетом. В контуре
заказчика — VPC peering и **ключ BYOK** (§10 схема Tier 1). Контракт сервисов
тот же.

Про PDF: «namespace на tenant и проект, свои квоты CPU, RAM и моделей» и «один
деплой — один tenant».

## Что здесь
- `ahp-identity.service`, `ahp-model.service`, `ahp-state.service` — systemd-юниты
  **выделенного** процесса на роль (`--role X --listen 127.0.0.1:808N`), один
  tenant-конур на хосте/ноде. `Type=notify`, `Restart=always`,
  `ProtectSystem=strict`, `NoNewPrivileges=true`, `PrivateTmp=true`.
  EnvironmentFile=`/etc/ahp/<role>.env` — сюда кладётся **BYOK keyref** и DSN
  выделенной БД (файл не в git).

## Apply (illustrative — НЕ выполнялось на живых VM)
```
sudo cp tier1/ahp-*.service /etc/systemd/system/
sudo mkdir -p /etc/ahp /var/lib/ahp/state
# заполнить /etc/ahp/{state,identity,model}.env (BYOK keyref, выделенный DSN) — вне git
sudo systemctl daemon-reload
sudo systemctl enable --now ahp-state ahp-identity ahp-model
```

## Verify
- `systemctl status ahp-state` (Active: active, Type=notify → ready только после `sd_notify READY=1`).
- `curl -fsS http://127.0.0.1:8081/readyz` (state) и т.д. по портам 8080/8082.
- BYOK: данные покоя выделенного бакета расшифровываются только ключом клиента.
- Restart-поведение: `systemctl restart ahp-state` → восстановление readiness.

## Rollback
`sudo systemctl stop ahp-*` → вернуть предыдущий бинарь/`EnvironmentFile` →
`systemctl start`. Задачи drain-ятся на своей версии.

## Evidence discipline (source ≠ deployment)
Структура юнитов + аргменты изоляции процесса = **L2_local_test**. systemd
restart + readiness-пробы на ≥2 реальных VM + измеренная изоляция «нет шумных
соседей» = **L4 target runtime — OPEN**.
