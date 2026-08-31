CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS cdc;
CREATE SCHEMA IF NOT EXISTS control;

CREATE TABLE IF NOT EXISTS control.pipeline_runs (
    run_id BIGSERIAL PRIMARY KEY,
    pipeline_name VARCHAR(200) NOT NULL,
    run_type VARCHAR(30) NOT NULL
        CHECK (run_type IN ('full', 'incremental', 'cdc')),
    status VARCHAR(30) NOT NULL
        CHECK (status IN ('running', 'success', 'failed')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMPTZ,
    rows_read BIGINT NOT NULL DEFAULT 0,
    rows_written BIGINT NOT NULL DEFAULT 0,
    error_message TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::JSONB
);

CREATE TABLE IF NOT EXISTS control.cdc_offsets (
    capture_instance VARCHAR(128) PRIMARY KEY,
    last_start_lsn VARCHAR(64),
    last_seqval VARCHAR(64),
    last_commit_time TIMESTAMPTZ,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS control.data_quality_results (
    result_id BIGSERIAL PRIMARY KEY,
    run_id BIGINT REFERENCES control.pipeline_runs(run_id),
    check_name VARCHAR(200) NOT NULL,
    table_name VARCHAR(200) NOT NULL,
    status VARCHAR(20) NOT NULL
        CHECK (status IN ('passed', 'failed', 'warning')),
    expected_value TEXT,
    actual_value TEXT,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    details JSONB NOT NULL DEFAULT '{}'::JSONB
);

CREATE TABLE IF NOT EXISTS cdc.raw_events (
    event_id BIGSERIAL PRIMARY KEY,
    capture_instance VARCHAR(128) NOT NULL,
    start_lsn VARCHAR(64) NOT NULL,
    seqval VARCHAR(64) NOT NULL,
    operation_code SMALLINT NOT NULL
        CHECK (operation_code BETWEEN 1 AND 4),
    operation_type VARCHAR(30) NOT NULL,
    event_time TIMESTAMPTZ,
    business_key JSONB NOT NULL,
    payload JSONB NOT NULL,
    kafka_topic VARCHAR(200),
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMPTZ,
    processing_error TEXT,
    CONSTRAINT uq_cdc_event UNIQUE (
        capture_instance,
        start_lsn,
        seqval,
        operation_code
    )
);

CREATE INDEX IF NOT EXISTS idx_cdc_unprocessed
    ON cdc.raw_events (capture_instance, received_at)
    WHERE processed_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status
    ON control.pipeline_runs (status, started_at);
