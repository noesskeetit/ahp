CREATE SCHEMA IF NOT EXISTS ahp_runtime;

CREATE TABLE IF NOT EXISTS ahp_runtime.executions (
    tenant_id       text        NOT NULL,
    deployment_id   text        NOT NULL,
    execution_id    text        NOT NULL,
    subject_id      text        NOT NULL,
    session_id      text        NOT NULL,
    harness_type    text        NOT NULL,
    launch_turn_seq bigint      CHECK (launch_turn_seq > 0),
    generation      bigint      NOT NULL CHECK (generation > 0),
    host_id         text        NOT NULL,
    attempt_no      bigint      NOT NULL DEFAULT 0 CHECK (attempt_no >= 0),
    status          text        NOT NULL CHECK (status IN ('STARTING', 'RUNNING', 'FAILED', 'STOPPING', 'FENCED')),
    updated_at      timestamptz NOT NULL DEFAULT clock_timestamp(),
    fenced_at       timestamptz,
    PRIMARY KEY (tenant_id, deployment_id, execution_id)
);
