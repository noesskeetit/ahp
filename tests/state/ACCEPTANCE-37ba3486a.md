# Приёмочный артефакт ленты State — M2 + M4

Кандидат: `37ba3486a79d560fdeeb51a7d7532ada371c10fc` (ветка `state-owner-20261031`), dirty=0, база — принятый M1 `21680e1`.
Коммиты: `37ba348 fix(transport): a flattened 400 keeps its domain code as a stable reason`; `0d8f9f9 test(state): kill the backend mid-transaction and prove the rollback is clean`; `e24fc4c feat(state): public State stand and consumer driver as real commands`; `d28b727 test(state): the KV idempotency receipt is addressed by operation_key alone`

Хэши бинарников намеренно **не сверяются**: платформа собирает Go без `-trimpath`, поэтому sha256
бинарника зависит от машины. Кандидат идентифицируется head + чистотой дерева + инвентарём sha256 ниже.
Приёмку выполняет директор: автор не является верификатором.

## Выходы гейтов

| гейт | rc |
|---|---|
| `go build ./...` | 0 |
| `go build -o /tmp/state-candidate-<short>/state-stand ./cmd/state-stand` | 0 |
| `go build -o /tmp/state-candidate-<short>/state-consumer ./cmd/state-consumer` | 0 |
| `go vet ./internal/state/... ./internal/stateapi/... ./cmd/state-stand/... ./cmd/state-consumer/... ./pkg/transport/... ./tests/state/...` | 0 |
| `go test -p 1 -race -count=1 -timeout 45m -v ./internal/state/... ./internal/stateapi/... ./pkg/transport/...` | 0 |
| `go test -p 1 -race -count=1 -timeout 20m -v ./pkg/transport/... (not lane-owned, changed by this candidate)` | в составе `go_test` |
| `go test -p 1 -race -count=1 -timeout 30m -v ./tests/state/...` | 0 |
| `./tests/state/serve/run-http-e2e.sh` | 0 |

## Числа тестов (`-p 1 -race -count=1`)

| набор | PASS | FAIL | SKIP |
|---|---|---|---|
| unit + integration | 98 | 0 | 0 |
| `pkg/transport` | посчитан в строке выше: `./pkg/transport/...` идёт той же командой `go test` |
| out-of-process `tests/state` | 26 | 0 | 0 |
| **ИТОГО** | **124** | **0** | **0** |

## Провод

```
state-consumer: calls=104 failures=0 ops_succeeded=37/37 missing=
```

- записей в evidence: 105; sha256 evidence: `7104e4ac4e78566a667fd860418a559c631d20390f32d28579b3389dd79096b7`
- манифест кандидата: `/home/alex/ahp-hive/run/state-owner-20261031/evidence/candidate-identity-20261031T065908Z.json` (sha256 `48c4f21cae937c26fb1b39edf9e78fcb6cf2c196dc1259add03b3c133162fce2`)
- операции, покрытые успешным вызовом: 37; гистограмма статусов: {'200': 68, '400': 11, '404': 11, '409': 9, '403': 3, '0': 2}
- отклонившихся вызовов: 0; непрошедших проверок: 0

## Инвентарь sha256 (diff против `21680e1`)

| файл | sha256 |
|---|---|
| `cmd/state-consumer/main.go` | `ce3795eb8a8e9b5d0261a2cf4de5e02385d009ff0c85ede3e0766803a8f79d18` |
| `cmd/state-stand/main.go` | `733679e2ea4da045ecc47aa55c566d46c87d040ad7e28abe9e045a43b7ad802d` |
| `internal/state/crashrollback_test.go` | `94dbbcd678ade3912be5e3fd11f35dc751e74cb060cb46c652deb390b7906a63` |
| `internal/state/idempotency_test.go` | `7c67b9c2634ffad8963ddf42805216e785101f4149c045e08c01135546cceffe` |
| `internal/stateapi/consumer/consumer.go` | `ad3c7a0938630232f06a354b0cf4d26b9124c67b6d790766183477ee26b1358e` |
| `internal/stateapi/consumer/consumer_test.go` | `572dae8ab50e633d68c31a87ecb32e1de2f65a2c4a0f08ffd178b041d30693da` |
| `internal/stateapi/stand/config.go` | `0c97ad90bbd18e3c7a5724fb773e51327cfc2c150696a340c074f55b7d6dbda1` |
| `internal/stateapi/stand/config_test.go` | `d7e7b9dadc04400d40946c4ef6a105d543405c66ff61ec5343f02d517b3988b9` |
| `internal/stateapi/stand/stand.go` | `efe772227488a19974fd192a655f1b7fa7682d750e2314f45d817778bb559f53` |
| `pkg/transport/json.go` | `83bc579bc2b5ebc962848eaa251aeb743498f45901a0a3d595bfc1ab3e08f1c0` |
| `pkg/transport/json_test.go` | `0250bbc9218757133194df1c6e05772dbd1a46513e8f36eea182b7c92995bbc7` |
| `tests/state/README.md` | `0db86cc820c706f6a6d36f27b53bfcbe47513d9812d28412a6ed9fab03820a16` |
| `tests/state/httpprobe/main.go` | `f823fde79d7de1e5046fb2f7e5ad659f1041fa221dbad1d7284da6c50f4d3c1e` |
| `tests/state/run_acceptance.sh` | `4218e908fd17bb6a127e4987f209fcf77bf1cfe80190ec9a020af38944ae9ed1` |
| `tests/state/serve/main.go` | `beadd2d497dac4bd851435545d4c49c0eb7f5cdcc7182d08bffcd3310f02fb33` |
| `tests/state/serve/run-http-e2e.sh` | `eb4731e6ad11bc598e65e01159adddd0c553af98b918feb7e063e7d836306ca9` |

## Повтор

```sh
python3 /home/alex/ahp-hive/tools/ahp-lock-run.py --lock /home/alex/ahp-hive/run/build.lock --wait 7200 -- sh /home/alex/ahp-hive/run/state-owner-20261031/tools/final-gates.sh
```

Окружение: `GOFLAGS='-mod=mod -buildvcs=false', env -u AHP_S3_ENDPOINT, database ahp_hive_state_owner (private/root-acceptance-pg/state-owner.env, name only)`
