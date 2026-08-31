BEGIN;

ALTER TABLE cdc.raw_events
    ADD COLUMN IF NOT EXISTS dw_processed_at TIMESTAMPTZ;

ALTER TABLE cdc.raw_events
    ADD COLUMN IF NOT EXISTS dw_batch_id UUID;

ALTER TABLE cdc.raw_events
    ADD COLUMN IF NOT EXISTS dw_processing_error TEXT;

ALTER TABLE cdc.raw_events
    ADD COLUMN IF NOT EXISTS dw_attempt_count INTEGER
        NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_cdc_pending_dw
    ON cdc.raw_events (event_id)
    WHERE
        processed_at IS NOT NULL
        AND processing_error IS NULL
        AND dw_processed_at IS NULL;

INSERT INTO control.cdc_service_state
(
    service_name,
    status,
    details
)
VALUES
(
    'incremental-transform',
    'stopped',
    '{}'::JSONB
)
ON CONFLICT (service_name) DO NOTHING;

COMMIT;
