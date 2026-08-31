from __future__ import annotations

import json
import logging
import os
import signal
import time
from dataclasses import dataclass
from datetime import date, datetime, time as datetime_time
from decimal import Decimal
from typing import Any

import pymssql
import psycopg
from confluent_kafka import Producer
from psycopg.types.json import Jsonb

from etl.config import Settings


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
LOGGER = logging.getLogger("northwind-cdc-producer")

STOP_REQUESTED = False

OPERATION_TYPES = {
    1: "DELETE",
    2: "INSERT",
    3: "UPDATE_BEFORE",
    4: "UPDATE_AFTER",
}


@dataclass(frozen=True)
class CdcSource:
    capture_instance: str
    function_name: str
    source_table: str
    business_key_columns: tuple[str, ...]


CDC_SOURCES = (
    CdcSource(
        capture_instance="dbo_Orders",
        function_name="cdc.fn_cdc_get_all_changes_dbo_Orders",
        source_table="dbo.Orders",
        business_key_columns=("OrderID",),
    ),
    CdcSource(
        capture_instance="dbo_Order_Details",
        function_name="cdc.fn_cdc_get_all_changes_dbo_Order_Details",
        source_table="dbo.Order Details",
        business_key_columns=("OrderID", "ProductID"),
    ),
)


def request_shutdown(signum, _frame) -> None:
    global STOP_REQUESTED
    LOGGER.info("Shutdown signal received: %s", signum)
    STOP_REQUESTED = True


def wait_seconds(seconds: float) -> None:
    deadline = time.monotonic() + seconds
    while not STOP_REQUESTED and time.monotonic() < deadline:
        time.sleep(min(0.25, deadline - time.monotonic()))


def normalize_lsn(value: Any) -> bytes | None:
    if value is None:
        return None
    if isinstance(value, memoryview):
        return value.tobytes()
    return bytes(value)


def lsn_to_hex(value: Any) -> str | None:
    normalized = normalize_lsn(value)
    if normalized is None:
        return None
    return f"0x{normalized.hex().upper()}"


def hex_to_lsn(value: str | None) -> bytes | None:
    if not value:
        return None
    normalized = value[2:] if value.lower().startswith("0x") else value
    return bytes.fromhex(normalized)


def json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date, datetime_time)):
        return value.isoformat()

    if isinstance(value, Decimal):
        return str(value)

    if isinstance(value, (bytes, bytearray, memoryview)):
        return lsn_to_hex(value)

    raise TypeError(f"Unsupported JSON type: {type(value).__name__}")


def connect_mssql(settings: Settings):
    return pymssql.connect(
        server=settings.mssql_host,
        port=settings.mssql_port,
        user=settings.mssql_user,
        password=settings.mssql_password,
        database=settings.mssql_database,
        charset="UTF-8",
        login_timeout=30,
        timeout=60,
        autocommit=True,
        appname="northwind-cdc-producer",
    )


def connect_postgres(settings: Settings):
    return psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_database,
        user=settings.postgres_user,
        password=settings.postgres_password,
        connect_timeout=30,
        autocommit=True,
    )


def load_offset(postgres_connection, capture_instance: str):
    with postgres_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                kafka_topic,
                last_start_lsn,
                last_seqval
            FROM control.cdc_offsets
            WHERE capture_instance = %s
            """,
            (capture_instance,),
        )
        row = cursor.fetchone()

    if row is None:
        raise RuntimeError(
            f"CDC offset configuration not found: {capture_instance}"
        )

    return row


def save_offset(
    postgres_connection,
    capture_instance: str,
    last_start_lsn: str,
    last_seqval: str | None,
    event_count: int,
    last_commit_time: datetime | None,
) -> None:
    with postgres_connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE control.cdc_offsets
            SET
                last_start_lsn = %s,
                last_seqval = %s,
                last_commit_time =
                    COALESCE(%s, last_commit_time),
                status = 'running',
                last_event_count = %s,
                last_polled_at = CURRENT_TIMESTAMP,
                last_error = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE capture_instance = %s
            """,
            (
                last_start_lsn,
                last_seqval,
                last_commit_time,
                event_count,
                capture_instance,
            ),
        )


def mark_offset_polled(
    postgres_connection,
    capture_instance: str,
) -> None:
    with postgres_connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE control.cdc_offsets
            SET
                status = 'running',
                last_event_count = 0,
                last_polled_at = CURRENT_TIMESTAMP,
                last_error = NULL,
                updated_at = CURRENT_TIMESTAMP
            WHERE capture_instance = %s
            """,
            (capture_instance,),
        )


def mark_offset_error(
    postgres_connection,
    capture_instance: str,
    error_message: str,
) -> None:
    with postgres_connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE control.cdc_offsets
            SET
                status = 'error',
                last_error = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE capture_instance = %s
            """,
            (error_message[:4000], capture_instance),
        )


def set_service_state(
    postgres_connection,
    status: str,
    details: dict[str, Any],
    error_message: str | None = None,
) -> None:
    with postgres_connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO control.cdc_service_state
            (
                service_name,
                status,
                last_heartbeat,
                last_error,
                details,
                updated_at
            )
            VALUES
            (
                'cdc-producer',
                %s,
                CURRENT_TIMESTAMP,
                %s,
                %s,
                CURRENT_TIMESTAMP
            )
            ON CONFLICT (service_name)
            DO UPDATE SET
                status = EXCLUDED.status,
                last_heartbeat = EXCLUDED.last_heartbeat,
                last_error = EXCLUDED.last_error,
                details = EXCLUDED.details,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                status,
                error_message[:4000] if error_message else None,
                Jsonb(details),
            ),
        )


def safely_set_service_state(
    settings: Settings,
    status: str,
    details: dict[str, Any],
    error_message: str | None = None,
) -> None:
    connection = None

    try:
        connection = connect_postgres(settings)
        set_service_state(
            connection,
            status,
            details,
            error_message,
        )
    except Exception:
        LOGGER.exception("Could not update CDC service state")
    finally:
        if connection is not None:
            connection.close()


def get_lsn_bounds(
    mssql_connection,
    capture_instance: str,
) -> tuple[bytes, bytes]:
    cursor = mssql_connection.cursor()

    try:
        cursor.execute(
            """
            SELECT
                sys.fn_cdc_get_min_lsn(%s),
                sys.fn_cdc_get_max_lsn()
            """,
            (capture_instance,),
        )
        minimum_lsn, maximum_lsn = cursor.fetchone()
    finally:
        cursor.close()

    minimum_lsn = normalize_lsn(minimum_lsn)
    maximum_lsn = normalize_lsn(maximum_lsn)

    if minimum_lsn is None or maximum_lsn is None:
        raise RuntimeError(
            f"Could not determine CDC LSN range: {capture_instance}"
        )

    return minimum_lsn, maximum_lsn


def increment_lsn(
    mssql_connection,
    current_lsn: bytes,
) -> bytes:
    cursor = mssql_connection.cursor()

    try:
        cursor.execute(
            "SELECT sys.fn_cdc_increment_lsn(%s)",
            (current_lsn,),
        )
        next_lsn = normalize_lsn(cursor.fetchone()[0])
    finally:
        cursor.close()

    if next_lsn is None:
        raise RuntimeError("SQL Server returned an empty next LSN")

    return next_lsn


def read_changes(
    mssql_connection,
    source: CdcSource,
    from_lsn: bytes,
    to_lsn: bytes,
) -> list[dict[str, Any]]:
    query = f"""
        SELECT
            sys.fn_cdc_map_lsn_to_time(
                changes.__$start_lsn
            ) AS __event_time,
            changes.*
        FROM {source.function_name}(
            %s,
            %s,
            'all update old'
        ) AS changes
        ORDER BY
            changes.__$start_lsn,
            changes.__$seqval,
            changes.__$operation
    """

    cursor = mssql_connection.cursor(as_dict=True)

    try:
        cursor.execute(query, (from_lsn, to_lsn))
        return list(cursor.fetchall())
    finally:
        cursor.close()


def build_event(
    source: CdcSource,
    row: dict[str, Any],
) -> tuple[str, dict[str, Any]]:
    operation_code = int(row["__$operation"])
    start_lsn = lsn_to_hex(row["__$start_lsn"])
    seqval = lsn_to_hex(row["__$seqval"])

    payload = {
        column_name: value
        for column_name, value in row.items()
        if not column_name.startswith("__$")
        and column_name != "__event_time"
    }

    business_key = {
        column_name: row[column_name]
        for column_name in source.business_key_columns
    }

    event = {
        "schema_version": 1,
        "event_id": (
            f"{source.capture_instance}:"
            f"{start_lsn}:{seqval}:{operation_code}"
        ),
        "capture_instance": source.capture_instance,
        "source_table": source.source_table,
        "operation_code": operation_code,
        "operation_type": OPERATION_TYPES.get(
            operation_code,
            "UNKNOWN",
        ),
        "event_time": row.get("__event_time"),
        "business_key": business_key,
        "payload": payload,
        "metadata": {
            "start_lsn": start_lsn,
            "seqval": seqval,
            "update_mask": lsn_to_hex(
                row.get("__$update_mask")
            ),
        },
    }

    order_id = business_key.get("OrderID")
    kafka_key = (
        str(order_id)
        if order_id is not None
        else event["event_id"]
    )

    return kafka_key, event


def publish_events(
    kafka_producer: Producer,
    source: CdcSource,
    topic: str,
    rows: list[dict[str, Any]],
    flush_timeout: float,
) -> int:
    delivery_errors: list[str] = []

    def delivery_report(error, _message) -> None:
        if error is not None:
            delivery_errors.append(str(error))

    for row in rows:
        kafka_key, event = build_event(source, row)

        event_bytes = json.dumps(
            event,
            ensure_ascii=False,
            default=json_default,
            separators=(",", ":"),
        ).encode("utf-8")

        while not STOP_REQUESTED:
            try:
                kafka_producer.produce(
                    topic=topic,
                    key=kafka_key.encode("utf-8"),
                    value=event_bytes,
                    headers=[
                        (
                            "capture_instance",
                            source.capture_instance.encode("utf-8"),
                        )
                    ],
                    on_delivery=delivery_report,
                )
                break
            except BufferError:
                kafka_producer.poll(1)

        kafka_producer.poll(0)

    remaining_messages = kafka_producer.flush(flush_timeout)

    if remaining_messages:
        raise RuntimeError(
            f"{remaining_messages} Kafka messages were not delivered"
        )

    if delivery_errors:
        raise RuntimeError(
            "Kafka delivery failed: "
            + "; ".join(delivery_errors[:5])
        )

    return len(rows)


def process_source(
    mssql_connection,
    postgres_connection,
    kafka_producer: Producer,
    source: CdcSource,
    start_mode: str,
    flush_timeout: float,
) -> int:
    kafka_topic, last_start_lsn, last_seqval = load_offset(
        postgres_connection,
        source.capture_instance,
    )

    minimum_lsn, maximum_lsn = get_lsn_bounds(
        mssql_connection,
        source.capture_instance,
    )

    previous_lsn = hex_to_lsn(last_start_lsn)

    if previous_lsn is None and start_mode == "latest":
        save_offset(
            postgres_connection,
            source.capture_instance,
            lsn_to_hex(maximum_lsn),
            None,
            0,
            None,
        )

        LOGGER.info(
            "Initialized %s at latest LSN %s",
            source.capture_instance,
            lsn_to_hex(maximum_lsn),
        )
        return 0

    if previous_lsn is None:
        from_lsn = minimum_lsn
    elif previous_lsn < minimum_lsn:
        LOGGER.warning(
            "%s offset expired; continuing from minimum LSN",
            source.capture_instance,
        )
        from_lsn = minimum_lsn
    elif previous_lsn > maximum_lsn:
        LOGGER.warning(
            "%s offset is ahead of SQL Server; resetting",
            source.capture_instance,
        )
        save_offset(
            postgres_connection,
            source.capture_instance,
            lsn_to_hex(maximum_lsn),
            None,
            0,
            None,
        )
        return 0
    elif previous_lsn == maximum_lsn:
        mark_offset_polled(
            postgres_connection,
            source.capture_instance,
        )
        return 0
    else:
        from_lsn = increment_lsn(
            mssql_connection,
            previous_lsn,
        )

    if from_lsn > maximum_lsn:
        mark_offset_polled(
            postgres_connection,
            source.capture_instance,
        )
        return 0

    rows = read_changes(
        mssql_connection,
        source,
        from_lsn,
        maximum_lsn,
    )

    published_count = publish_events(
        kafka_producer,
        source,
        kafka_topic,
        rows,
        flush_timeout,
    )

    current_seqval = last_seqval
    last_commit_time = None

    if rows:
        current_seqval = lsn_to_hex(rows[-1]["__$seqval"])
        last_commit_time = rows[-1].get("__event_time")

    save_offset(
        postgres_connection,
        source.capture_instance,
        lsn_to_hex(maximum_lsn),
        current_seqval,
        published_count,
        last_commit_time,
    )

    if published_count:
        LOGGER.info(
            "Published %s events from %s to %s",
            published_count,
            source.capture_instance,
            kafka_topic,
        )

    return published_count


def run() -> None:
    settings = Settings.from_env()

    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka:19092",
    )
    poll_interval = float(
        os.getenv("CDC_POLL_INTERVAL_SECONDS", "2")
    )
    retry_interval = float(
        os.getenv("CDC_RETRY_SECONDS", "5")
    )
    flush_timeout = float(
        os.getenv("CDC_FLUSH_TIMEOUT_SECONDS", "30")
    )
    start_mode = os.getenv(
        "CDC_START_MODE",
        "latest",
    ).lower()

    if start_mode not in {"latest", "earliest"}:
        raise ValueError(
            "CDC_START_MODE must be latest or earliest"
        )

    kafka_producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": os.getenv(
                "KAFKA_CLIENT_ID",
                "northwind-cdc-producer",
            ),
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "gzip",
            "message.timeout.ms": 30000,
        }
    )

    total_published = 0

    while not STOP_REQUESTED:
        mssql_connection = None
        postgres_connection = None

        try:
            mssql_connection = connect_mssql(settings)
            postgres_connection = connect_postgres(settings)

            LOGGER.info(
                "CDC Producer connected; Kafka=%s, start_mode=%s",
                bootstrap_servers,
                start_mode,
            )

            set_service_state(
                postgres_connection,
                "running",
                {
                    "bootstrap_servers": bootstrap_servers,
                    "start_mode": start_mode,
                    "total_published": total_published,
                },
            )

            while not STOP_REQUESTED:
                cycle_published = 0

                for source in CDC_SOURCES:
                    try:
                        cycle_published += process_source(
                            mssql_connection,
                            postgres_connection,
                            kafka_producer,
                            source,
                            start_mode,
                            flush_timeout,
                        )
                    except Exception as error:
                        mark_offset_error(
                            postgres_connection,
                            source.capture_instance,
                            str(error),
                        )
                        raise

                total_published += cycle_published

                set_service_state(
                    postgres_connection,
                    "running",
                    {
                        "bootstrap_servers": bootstrap_servers,
                        "start_mode": start_mode,
                        "last_cycle_events": cycle_published,
                        "total_published": total_published,
                    },
                )

                wait_seconds(poll_interval)

        except Exception as error:
            LOGGER.exception("CDC Producer cycle failed")

            safely_set_service_state(
                settings,
                "error",
                {
                    "bootstrap_servers": bootstrap_servers,
                    "total_published": total_published,
                },
                str(error),
            )

            wait_seconds(retry_interval)

        finally:
            if mssql_connection is not None:
                mssql_connection.close()

            if postgres_connection is not None:
                postgres_connection.close()

    undelivered = kafka_producer.flush(10)

    safely_set_service_state(
        settings,
        "stopped",
        {
            "total_published": total_published,
            "undelivered_messages": undelivered,
        },
    )

    LOGGER.info("CDC Producer stopped")


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)
    run()