from __future__ import annotations

import json
import logging
import os
import signal
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import psycopg
from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Producer,
)
from psycopg.types.json import Jsonb
from pymongo import ASCENDING, DESCENDING, MongoClient


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
LOGGER = logging.getLogger("northwind-cdc-consumer")

STOP_REQUESTED = False

CDC_TOPICS = (
    "northwind.orders.cdc",
    "northwind.order_details.cdc",
)

OPERATION_TYPES = {
    1: "DELETE",
    2: "INSERT",
    3: "UPDATE_BEFORE",
    4: "UPDATE_AFTER",
}


class PermanentEventError(Exception):
    """An invalid message that must be sent to the DLQ."""


def required_env(name: str) -> str:
    value = os.getenv(name)

    if not value:
        raise RuntimeError(
            f"Required environment variable is missing: {name}"
        )

    return value


def request_shutdown(signum, _frame) -> None:
    global STOP_REQUESTED
    LOGGER.info("Shutdown signal received: %s", signum)
    STOP_REQUESTED = True


def wait_seconds(seconds: float) -> None:
    deadline = time.monotonic() + seconds

    while (
        not STOP_REQUESTED
        and time.monotonic() < deadline
    ):
        time.sleep(
            min(0.25, deadline - time.monotonic())
        )


def connect_postgres():
    return psycopg.connect(
        host=os.getenv(
            "POSTGRES_HOST",
            "postgres-staging",
        ),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=required_env("POSTGRES_DB"),
        user=required_env("POSTGRES_USER"),
        password=required_env("POSTGRES_PASSWORD"),
        connect_timeout=30,
        autocommit=True,
    )


def connect_mongodb():
    client = MongoClient(
        host=os.getenv("MONGO_HOST", "mongodb"),
        port=int(os.getenv("MONGO_PORT", "27017")),
        username=required_env("MONGO_USERNAME"),
        password=required_env("MONGO_PASSWORD"),
        authSource=os.getenv(
            "MONGO_AUTH_SOURCE",
            "admin",
        ),
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
    )

    client.admin.command("ping")

    database = client[
        required_env("MONGO_DATABASE")
    ]

    collection = database[
        os.getenv(
            "MONGO_LOG_COLLECTION",
            "pipeline_logs",
        )
    ]

    collection.create_index(
        [("timestamp", DESCENDING)],
        name="idx_timestamp",
    )
    collection.create_index(
        [
            ("service", ASCENDING),
            ("event", ASCENDING),
        ],
        name="idx_service_event",
    )

    return client, collection


def write_mongo_log(
    collection,
    level: str,
    event_name: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> None:
    if collection is None:
        return

    try:
        collection.insert_one(
            {
                "timestamp": datetime.now(timezone.utc),
                "service": "cdc-consumer",
                "level": level,
                "event": event_name,
                "message": message,
                "details": details or {},
            }
        )
    except Exception:
        LOGGER.exception("Could not write log to MongoDB")


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
                'cdc-consumer',
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
                error_message[:4000]
                if error_message
                else None,
                Jsonb(details),
            ),
        )


def safely_set_service_state(
    status: str,
    details: dict[str, Any],
    error_message: str | None = None,
) -> None:
    connection = None

    try:
        connection = connect_postgres()
        set_service_state(
            connection,
            status,
            details,
            error_message,
        )
    except Exception:
        LOGGER.exception(
            "Could not update Consumer service state"
        )
    finally:
        if connection is not None:
            connection.close()


def parse_event_time(value: Any):
    if value is None or isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(
            str(value).replace("Z", "+00:00")
        )

    if parsed is not None and parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed


def parse_source_time(value: Any):
    if value is None:
        return None

    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value))

    return parsed.replace(tzinfo=None)


def to_decimal(value: Any):
    if value is None:
        return None

    return Decimal(str(value))


def to_int(value: Any):
    if value is None:
        return None

    return int(value)


def trimmed(value: Any):
    if isinstance(value, str):
        return value.rstrip()

    return value


def validate_event(event: Any) -> dict[str, Any]:
    if not isinstance(event, dict):
        raise PermanentEventError(
            "Kafka message is not a JSON object"
        )

    capture_instance = event.get("capture_instance")

    if capture_instance not in {
        "dbo_Orders",
        "dbo_Order_Details",
    }:
        raise PermanentEventError(
            f"Unsupported capture instance: {capture_instance}"
        )

    try:
        operation_code = int(event["operation_code"])
    except (KeyError, TypeError, ValueError) as error:
        raise PermanentEventError(
            "Invalid operation_code"
        ) from error

    if operation_code not in OPERATION_TYPES:
        raise PermanentEventError(
            f"Unsupported operation_code: {operation_code}"
        )

    if not isinstance(event.get("business_key"), dict):
        raise PermanentEventError(
            "business_key must be an object"
        )

    if not isinstance(event.get("payload"), dict):
        raise PermanentEventError(
            "payload must be an object"
        )

    metadata = event.get("metadata")

    if not isinstance(metadata, dict):
        raise PermanentEventError(
            "metadata must be an object"
        )

    if not metadata.get("start_lsn"):
        raise PermanentEventError(
            "metadata.start_lsn is missing"
        )

    if not metadata.get("seqval"):
        raise PermanentEventError(
            "metadata.seqval is missing"
        )

    event["operation_code"] = operation_code
    return event


def insert_raw_event(
    cursor,
    message,
    event: dict[str, Any],
):
    metadata = event["metadata"]

    cursor.execute(
        """
        INSERT INTO cdc.raw_events AS target
        (
            capture_instance,
            start_lsn,
            seqval,
            operation_code,
            operation_type,
            event_time,
            business_key,
            payload,
            kafka_topic,
            kafka_partition,
            kafka_offset
        )
        VALUES
        (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT
        (
            capture_instance,
            start_lsn,
            seqval,
            operation_code
        )
        DO UPDATE SET
            kafka_topic = COALESCE(
                target.kafka_topic,
                EXCLUDED.kafka_topic
            ),
            kafka_partition = COALESCE(
                target.kafka_partition,
                EXCLUDED.kafka_partition
            ),
            kafka_offset = COALESCE(
                target.kafka_offset,
                EXCLUDED.kafka_offset
            )
        RETURNING
            event_id,
            processed_at
        """,
        (
            event["capture_instance"],
            metadata["start_lsn"],
            metadata["seqval"],
            event["operation_code"],
            event.get(
                "operation_type",
                OPERATION_TYPES[event["operation_code"]],
            ),
            parse_event_time(event.get("event_time")),
            Jsonb(event["business_key"]),
            Jsonb(event["payload"]),
            message.topic(),
            message.partition(),
            message.offset(),
        ),
    )

    return cursor.fetchone()


def upsert_order(cursor, payload: dict[str, Any]) -> None:
    cursor.execute(
        """
        INSERT INTO staging.orders
        (
            order_id,
            customer_id,
            employee_id,
            order_date,
            required_date,
            shipped_date,
            ship_via,
            freight,
            ship_name,
            ship_address,
            ship_city,
            ship_region,
            ship_postal_code,
            ship_country,
            _batch_id,
            _loaded_at
        )
        VALUES
        (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            CURRENT_TIMESTAMP
        )
        ON CONFLICT (order_id)
        DO UPDATE SET
            customer_id = EXCLUDED.customer_id,
            employee_id = EXCLUDED.employee_id,
            order_date = EXCLUDED.order_date,
            required_date = EXCLUDED.required_date,
            shipped_date = EXCLUDED.shipped_date,
            ship_via = EXCLUDED.ship_via,
            freight = EXCLUDED.freight,
            ship_name = EXCLUDED.ship_name,
            ship_address = EXCLUDED.ship_address,
            ship_city = EXCLUDED.ship_city,
            ship_region = EXCLUDED.ship_region,
            ship_postal_code =
                EXCLUDED.ship_postal_code,
            ship_country = EXCLUDED.ship_country,
            _batch_id = EXCLUDED._batch_id,
            _loaded_at = CURRENT_TIMESTAMP
        """,
        (
            int(payload["OrderID"]),
            trimmed(payload.get("CustomerID")),
            to_int(payload.get("EmployeeID")),
            parse_source_time(payload.get("OrderDate")),
            parse_source_time(
                payload.get("RequiredDate")
            ),
            parse_source_time(
                payload.get("ShippedDate")
            ),
            to_int(payload.get("ShipVia")),
            to_decimal(payload.get("Freight")),
            payload.get("ShipName"),
            payload.get("ShipAddress"),
            payload.get("ShipCity"),
            payload.get("ShipRegion"),
            payload.get("ShipPostalCode"),
            payload.get("ShipCountry"),
            uuid.uuid4(),
        ),
    )


def upsert_order_detail(
    cursor,
    payload: dict[str, Any],
) -> None:
    cursor.execute(
        """
        INSERT INTO staging.order_details
        (
            order_id,
            product_id,
            unit_price,
            quantity,
            discount,
            _batch_id,
            _loaded_at
        )
        VALUES
        (
            %s, %s, %s, %s, %s, %s,
            CURRENT_TIMESTAMP
        )
        ON CONFLICT (order_id, product_id)
        DO UPDATE SET
            unit_price = EXCLUDED.unit_price,
            quantity = EXCLUDED.quantity,
            discount = EXCLUDED.discount,
            _batch_id = EXCLUDED._batch_id,
            _loaded_at = CURRENT_TIMESTAMP
        """,
        (
            int(payload["OrderID"]),
            int(payload["ProductID"]),
            to_decimal(payload["UnitPrice"]),
            int(payload["Quantity"]),
            float(payload["Discount"]),
            uuid.uuid4(),
        ),
    )


def apply_event(
    cursor,
    event: dict[str, Any],
) -> str:
    operation_code = event["operation_code"]
    capture_instance = event["capture_instance"]
    business_key = event["business_key"]
    payload = event["payload"]

    if operation_code == 3:
        return "history_only"

    try:
        if capture_instance == "dbo_Orders":
            if operation_code in {2, 4}:
                upsert_order(cursor, payload)
                return "upsert_order"

            if operation_code == 1:
                cursor.execute(
                    """
                    DELETE FROM staging.orders
                    WHERE order_id = %s
                    """,
                    (int(business_key["OrderID"]),),
                )
                return "delete_order"

        if capture_instance == "dbo_Order_Details":
            if operation_code in {2, 4}:
                upsert_order_detail(cursor, payload)
                return "upsert_order_detail"

            if operation_code == 1:
                cursor.execute(
                    """
                    DELETE FROM staging.order_details
                    WHERE
                        order_id = %s
                        AND product_id = %s
                    """,
                    (
                        int(business_key["OrderID"]),
                        int(business_key["ProductID"]),
                    ),
                )
                return "delete_order_detail"

    except (
        KeyError,
        TypeError,
        ValueError,
        InvalidOperation,
    ) as error:
        raise PermanentEventError(
            f"Invalid event payload: {error}"
        ) from error

    raise PermanentEventError(
        "No staging action exists for this event"
    )


def process_message(
    postgres_connection,
    message,
):
    if message.value() is None:
        raise PermanentEventError(
            "Kafka message value is empty"
        )

    try:
        decoded_value = message.value().decode("utf-8")
        event = json.loads(decoded_value)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PermanentEventError(
            f"Invalid JSON message: {error}"
        ) from error

    event = validate_event(event)

    with postgres_connection.transaction():
        with postgres_connection.cursor() as cursor:
            raw_event_id, processed_at = insert_raw_event(
                cursor,
                message,
                event,
            )

            if processed_at is None:
                action = apply_event(cursor, event)

                cursor.execute(
                    """
                    UPDATE cdc.raw_events
                    SET
                        processed_at = CURRENT_TIMESTAMP,
                        processing_error = NULL
                    WHERE event_id = %s
                    """,
                    (raw_event_id,),
                )
            else:
                action = "duplicate"

            cursor.execute(
                """
                UPDATE control.cdc_offsets
                SET
                    kafka_partition = %s,
                    kafka_offset = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE capture_instance = %s
                """,
                (
                    message.partition(),
                    message.offset(),
                    event["capture_instance"],
                ),
            )

    return event, action, raw_event_id


def send_to_dlq(
    dlq_producer: Producer,
    dlq_topic: str,
    message,
    error_message: str,
) -> None:
    delivery_errors: list[str] = []

    def delivery_report(error, _message) -> None:
        if error is not None:
            delivery_errors.append(str(error))

    dlq_event = {
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "source_topic": message.topic(),
        "source_partition": message.partition(),
        "source_offset": message.offset(),
        "key": (
            message.key().decode(
                "utf-8",
                errors="replace",
            )
            if message.key()
            else None
        ),
        "raw_value": (
            message.value().decode(
                "utf-8",
                errors="replace",
            )
            if message.value()
            else None
        ),
        "error": error_message,
    }

    dlq_producer.produce(
        topic=dlq_topic,
        key=message.key(),
        value=json.dumps(
            dlq_event,
            ensure_ascii=False,
        ).encode("utf-8"),
        on_delivery=delivery_report,
    )

    remaining = dlq_producer.flush(30)

    if remaining:
        raise RuntimeError(
            f"{remaining} DLQ messages were not delivered"
        )

    if delivery_errors:
        raise RuntimeError(
            "DLQ delivery failed: "
            + "; ".join(delivery_errors)
        )


def run() -> None:
    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka:19092",
    )
    group_id = os.getenv(
        "KAFKA_GROUP_ID",
        "northwind-cdc-staging-v1",
    )
    dlq_topic = os.getenv(
        "KAFKA_DLQ_TOPIC",
        "northwind.cdc.dlq",
    )
    retry_seconds = float(
        os.getenv("CDC_RETRY_SECONDS", "5")
    )

    totals = {
        "processed": 0,
        "duplicates": 0,
        "dlq": 0,
    }

    while not STOP_REQUESTED:
        postgres_connection = None
        mongo_client = None
        mongo_collection = None
        kafka_consumer = None

        try:
            postgres_connection = connect_postgres()
            mongo_client, mongo_collection = (
                connect_mongodb()
            )

            kafka_consumer = Consumer(
                {
                    "bootstrap.servers": bootstrap_servers,
                    "group.id": group_id,
                    "client.id": "northwind-cdc-consumer",
                    "enable.auto.commit": False,
                    "auto.offset.reset": "earliest",
                    "isolation.level": "read_committed",
                    "session.timeout.ms": 45000,
                    "max.poll.interval.ms": 300000,
                }
            )

            dlq_producer = Producer(
                {
                    "bootstrap.servers": bootstrap_servers,
                    "client.id": "northwind-cdc-dlq-producer",
                    "enable.idempotence": True,
                    "acks": "all",
                }
            )

            kafka_consumer.subscribe(list(CDC_TOPICS))

            LOGGER.info(
                "CDC Consumer connected; group=%s",
                group_id,
            )

            write_mongo_log(
                mongo_collection,
                "INFO",
                "service_started",
                "CDC Consumer started",
                {
                    "group_id": group_id,
                    "topics": list(CDC_TOPICS),
                },
            )

            set_service_state(
                postgres_connection,
                "running",
                {
                    "group_id": group_id,
                    **totals,
                },
            )

            next_heartbeat = time.monotonic() + 10

            while not STOP_REQUESTED:
                message = kafka_consumer.poll(1.0)

                if message is None:
                    if time.monotonic() >= next_heartbeat:
                        set_service_state(
                            postgres_connection,
                            "running",
                            {
                                "group_id": group_id,
                                **totals,
                            },
                        )
                        next_heartbeat = (
                            time.monotonic() + 10
                        )
                    continue

                if message.error():
                    if (
                        message.error().code()
                        == KafkaError._PARTITION_EOF
                    ):
                        continue

                    raise KafkaException(
                        message.error()
                    )

                try:
                    event, action, raw_event_id = (
                        process_message(
                            postgres_connection,
                            message,
                        )
                    )

                except PermanentEventError as error:
                    send_to_dlq(
                        dlq_producer,
                        dlq_topic,
                        message,
                        str(error),
                    )

                    kafka_consumer.commit(
                        message=message,
                        asynchronous=False,
                    )

                    totals["dlq"] += 1

                    LOGGER.error(
                        "Message sent to DLQ: topic=%s offset=%s error=%s",
                        message.topic(),
                        message.offset(),
                        error,
                    )

                    write_mongo_log(
                        mongo_collection,
                        "ERROR",
                        "message_sent_to_dlq",
                        str(error),
                        {
                            "topic": message.topic(),
                            "partition": message.partition(),
                            "offset": message.offset(),
                        },
                    )
                    continue

                kafka_consumer.commit(
                    message=message,
                    asynchronous=False,
                )

                if action == "duplicate":
                    totals["duplicates"] += 1
                else:
                    totals["processed"] += 1

                LOGGER.info(
                    "Processed topic=%s partition=%s offset=%s operation=%s action=%s",
                    message.topic(),
                    message.partition(),
                    message.offset(),
                    event["operation_type"],
                    action,
                )

                write_mongo_log(
                    mongo_collection,
                    "INFO",
                    "event_processed",
                    "CDC event processed successfully",
                    {
                        "raw_event_id": raw_event_id,
                        "topic": message.topic(),
                        "partition": message.partition(),
                        "offset": message.offset(),
                        "capture_instance": (
                            event["capture_instance"]
                        ),
                        "operation_type": (
                            event["operation_type"]
                        ),
                        "action": action,
                    },
                )

                set_service_state(
                    postgres_connection,
                    "running",
                    {
                        "group_id": group_id,
                        **totals,
                    },
                )

        except Exception as error:
            LOGGER.exception("CDC Consumer failed")

            write_mongo_log(
                mongo_collection,
                "ERROR",
                "service_error",
                str(error),
                {"group_id": group_id},
            )

            safely_set_service_state(
                "error",
                {
                    "group_id": group_id,
                    **totals,
                },
                str(error),
            )

            wait_seconds(retry_seconds)

        finally:
            if kafka_consumer is not None:
                kafka_consumer.close()

            if postgres_connection is not None:
                postgres_connection.close()

            if mongo_client is not None:
                mongo_client.close()

    safely_set_service_state(
        "stopped",
        {
            "group_id": group_id,
            **totals,
        },
    )

    LOGGER.info("CDC Consumer stopped")


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, request_shutdown)
    signal.signal(signal.SIGINT, request_shutdown)
    run()