BEGIN;

ALTER TABLE control.cdc_offsets
    ADD COLUMN IF NOT EXISTS source_table VARCHAR(128);

ALTER TABLE control.cdc_offsets
    ADD COLUMN IF NOT EXISTS kafka_topic VARCHAR(200);

ALTER TABLE control.cdc_offsets
    ADD COLUMN IF NOT EXISTS status VARCHAR(20)
        NOT NULL DEFAULT 'new';

ALTER TABLE control.cdc_offsets
    ADD COLUMN IF NOT EXISTS last_event_count INTEGER
        NOT NULL DEFAULT 0;

ALTER TABLE control.cdc_offsets
    ADD COLUMN IF NOT EXISTS last_polled_at TIMESTAMPTZ;

ALTER TABLE control.cdc_offsets
    ADD COLUMN IF NOT EXISTS last_error TEXT;

ALTER TABLE control.cdc_offsets
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ
        NOT NULL DEFAULT CURRENT_TIMESTAMP;

CREATE UNIQUE INDEX IF NOT EXISTS uq_cdc_kafka_position
    ON cdc.raw_events
    (
        kafka_topic,
        kafka_partition,
        kafka_offset
    )
    WHERE
        kafka_topic IS NOT NULL
        AND kafka_partition IS NOT NULL
        AND kafka_offset IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_cdc_business_key_gin
    ON cdc.raw_events
    USING GIN (business_key);

CREATE INDEX IF NOT EXISTS idx_cdc_event_time
    ON cdc.raw_events (event_time DESC);

CREATE TABLE IF NOT EXISTS control.cdc_service_state
(
    service_name    VARCHAR(100) PRIMARY KEY,
    status          VARCHAR(20) NOT NULL DEFAULT 'starting',
    last_heartbeat  TIMESTAMPTZ,
    last_error      TEXT,
    details         JSONB NOT NULL DEFAULT '{}'::JSONB,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO control.cdc_offsets
(
    capture_instance,
    source_table,
    kafka_topic,
    status
)
VALUES
(
    'dbo_Orders',
    'dbo.Orders',
    'northwind.orders.cdc',
    'new'
),
(
    'dbo_Order_Details',
    'dbo.Order Details',
    'northwind.order_details.cdc',
    'new'
)
ON CONFLICT (capture_instance)
DO UPDATE SET
    source_table = EXCLUDED.source_table,
    kafka_topic = EXCLUDED.kafka_topic,
    updated_at = CURRENT_TIMESTAMP;

INSERT INTO control.cdc_service_state
(
    service_name,
    status
)
VALUES
    ('cdc-producer', 'stopped'),
    ('cdc-consumer', 'stopped')
ON CONFLICT (service_name) DO NOTHING;

COMMIT;