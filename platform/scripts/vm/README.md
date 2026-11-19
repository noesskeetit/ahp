# VM isolated AHP test harness (delivery lane, unaccepted)
Root personal VM, ISOLATED namespace ahp_* only. Foreground (IV) — no launchd.
Order (each in its own foreground shell / process):
1. export AHP_BUNDLE=/abs/composed-iso29700-<short>.bundle  AHP_TEST_DB=ahp_hive_<...>  (allowlist ahp_hive_[a-z][a-z0-9_]{0,23})
   + PG: AHP_TEST_PGHOST/PORT; MinIO: AHP_TEST_BUCKET_NS=<ns>; AHP_TEST_USERS_JSON=[{username,password,subject},…] (>=2)
2. iso-vm-harness.sh import-build     # hash-import + offline go build -> runtime/bin
3. iso-vm-harness.sh gen-certs        # mTLS CA/server/client under runtime/certs
4. iso-vm-harness.sh run-backend      # :29700 role=all, mTLS, isolated PG (set AHP_DATABASE_URL + AHP_ENABLE_BROWSER=1 in env)
5. iso-vm-harness.sh run-origin       # :29710 human-origin -> https backend (client cert held here only)
6. iso-vm-harness.sh verify           # vm-journey: login(HttpOnly)/me/two-subject/restart HARD; run->model->artifact->download GATED until Space facade re-pin + execution canonical runner land
Never touches other containers/DBs. Secrets from env, never logged. Whole prototype UNACCEPTED.
