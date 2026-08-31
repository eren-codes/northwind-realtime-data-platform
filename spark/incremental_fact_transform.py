import json
import os
import time
import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from full_transform import add_metadata, clean_text, date_key, geography_key


FACT_VALUE_COLUMNS = [
    "order_id",
    "geography_key",
    "product_key",
    "customer_key",
    "employee_key",
    "shipper_key",
    "order_date_key",
    "required_date_key",
    "shipped_date_key",
    "freight",
    "ship_name",
    "unit_price",
    "quantity",
    "discount",
    "order_date",
    "shipped_date",
    "required_date",
]


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


def sql_literal(value: str) -> str:
    cleaned = value.replace("\x00", "").replace("'", "''")
    return f"'{cleaned}'"


def execute_postgres_sql(
    spark: SparkSession,
    postgres_url: str,
    postgres_user: str,
    postgres_password: str,
    statement_text: str,
) -> int:
    spark._jvm.java.lang.Class.forName("org.postgresql.Driver")
    connection = spark._jvm.java.sql.DriverManager.getConnection(
        postgres_url,
        postgres_user,
        postgres_password,
    )
    try:
        connection.setAutoCommit(True)
        statement = connection.createStatement()
        try:
            return int(statement.executeUpdate(statement_text))
        finally:
            statement.close()
    finally:
        connection.close()


def update_service_state(
    spark: SparkSession,
    postgres_url: str,
    postgres_user: str,
    postgres_password: str,
    status: str,
    details: dict,
    error: str | None = None,
) -> None:
    details_json = json.dumps(details, ensure_ascii=False, separators=(",", ":"))
    error_sql = "NULL" if error is None else sql_literal(error[:4000])

    execute_postgres_sql(
        spark,
        postgres_url,
        postgres_user,
        postgres_password,
        f"""
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
            'incremental-transform',
            {sql_literal(status)},
            CURRENT_TIMESTAMP,
            {error_sql},
            {sql_literal(details_json)}::JSONB,
            CURRENT_TIMESTAMP
        )
        ON CONFLICT (service_name)
        DO UPDATE SET
            status = EXCLUDED.status,
            last_heartbeat = EXCLUDED.last_heartbeat,
            last_error = EXCLUDED.last_error,
            details = EXCLUDED.details,
            updated_at = EXCLUDED.updated_at
        """,
    )


def latest_rows(dataframe: DataFrame, keys: list[str]) -> DataFrame:
    order_columns = []
    if "_version" in dataframe.columns:
        order_columns.append(F.col("_version").desc())
    if "_loaded_at" in dataframe.columns:
        order_columns.append(F.col("_loaded_at").desc())

    if not order_columns:
        return dataframe.dropDuplicates(keys)

    latest_window = Window.partitionBy(*keys).orderBy(*order_columns)
    return (
        dataframe.withColumn("__latest_row", F.row_number().over(latest_window))
        .filter(F.col("__latest_row") == 1)
        .drop("__latest_row")
    )


def current_dimension_lookup(
    spark: SparkSession,
    clickhouse_database: str,
    table_name: str,
    alternate_key: str,
    surrogate_key: str,
    lookup_alternate_key: str,
    lookup_surrogate_key: str,
    alternate_type: str,
) -> DataFrame:
    dimension = spark.table(f"clickhouse.{clickhouse_database}.{table_name}")
    dimension = latest_rows(dimension, [alternate_key])

    if "is_current" in dimension.columns:
        dimension = dimension.filter(F.col("is_current") == 1)

    return dimension.select(
        F.col(alternate_key).cast(alternate_type).alias(lookup_alternate_key),
        F.col(surrogate_key).cast("long").alias(lookup_surrogate_key),
    )


def tombstone_projection(event_id_column, source_lsn_column):
    return [
        *[F.col(f"fact.{column_name}").alias(column_name) for column_name in FACT_VALUE_COLUMNS],
        F.lit(1).cast("short").alias("is_deleted"),
        source_lsn_column.alias("source_lsn"),
        event_id_column.cast("long").alias("_event_id"),
    ]


def main() -> None:
    postgres_host = os.getenv("POSTGRES_HOST", "postgres-staging")
    postgres_port = os.getenv("POSTGRES_PORT", "5432")
    postgres_database = required_env("POSTGRES_DB")
    postgres_user = required_env("POSTGRES_USER")
    postgres_password = required_env("POSTGRES_PASSWORD")

    clickhouse_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    clickhouse_port = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
    clickhouse_database = required_env("CLICKHOUSE_DB")
    clickhouse_user = required_env("CLICKHOUSE_USER")
    clickhouse_password = required_env("CLICKHOUSE_PASSWORD")

    batch_size = max(1, min(int(os.getenv("INCREMENTAL_BATCH_SIZE", "1000")), 10000))
    batch_id = str(uuid.uuid4())
    loaded_at = datetime.now(timezone.utc).replace(tzinfo=None)
    version = int(time.time() * 1000)

    spark = (
        SparkSession.builder.appName("northwind-incremental-fact-transform")
        .config("spark.sql.catalog.clickhouse", "com.clickhouse.spark.ClickHouseCatalog")
        .config("spark.sql.catalog.clickhouse.host", clickhouse_host)
        .config("spark.sql.catalog.clickhouse.protocol", "http")
        .config("spark.sql.catalog.clickhouse.http_port", clickhouse_port)
        .config("spark.sql.catalog.clickhouse.user", clickhouse_user)
        .config("spark.sql.catalog.clickhouse.password", clickhouse_password)
        .config("spark.sql.catalog.clickhouse.database", clickhouse_database)
        .config("spark.clickhouse.write.format", "json")
        .config("spark.clickhouse.write.batchSize", "1000")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    postgres_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_database}"
    event_ids: list[int] = []

    def read_postgres(dbtable: str) -> DataFrame:
        return (
            spark.read.format("jdbc")
            .option("url", postgres_url)
            .option("dbtable", dbtable)
            .option("user", postgres_user)
            .option("password", postgres_password)
            .option("driver", "org.postgresql.Driver")
            .option("fetchsize", "1000")
            .load()
        )

    def read_staging(table_name: str) -> DataFrame:
        return read_postgres(f"staging.{table_name}")

    print(f"INCREMENTAL_TRANSFORM_BATCH_ID={batch_id}")

    try:
        pending_events_query = f"""
        (
            SELECT
                event_id,
                capture_instance,
                start_lsn,
                operation_code,
                operation_type,
                business_key::TEXT AS business_key,
                payload::TEXT AS payload
            FROM cdc.raw_events
            WHERE
                processed_at IS NOT NULL
                AND processing_error IS NULL
                AND dw_processed_at IS NULL
            ORDER BY event_id
            LIMIT {batch_size}
        ) AS pending_dw_events
        """

        events = read_postgres(pending_events_query).persist(StorageLevel.MEMORY_AND_DISK)
        event_ids = [
            int(row.event_id)
            for row in events.select("event_id").orderBy("event_id").collect()
        ]

        if not event_ids:
            update_service_state(
                spark,
                postgres_url,
                postgres_user,
                postgres_password,
                "idle",
                {"batch_id": batch_id, "events_read": 0, "rows_written": 0},
            )
            print("INCREMENTAL_EVENTS_READ=0")
            print("INCREMENTAL_TRANSFORM=NOOP")
            return

        update_service_state(
            spark,
            postgres_url,
            postgres_user,
            postgres_password,
            "running",
            {
                "batch_id": batch_id,
                "events_read": len(event_ids),
                "min_event_id": min(event_ids),
                "max_event_id": max(event_ids),
            },
        )

        parsed_events = (
            events.withColumn(
                "order_id",
                F.coalesce(
                    F.get_json_object(F.col("business_key"), "$.OrderID"),
                    F.get_json_object(F.col("payload"), "$.OrderID"),
                ).cast("long"),
            )
            .withColumn(
                "product_id",
                F.coalesce(
                    F.get_json_object(F.col("business_key"), "$.ProductID"),
                    F.get_json_object(F.col("payload"), "$.ProductID"),
                ).cast("long"),
            )
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        invalid_event_count = parsed_events.filter(F.col("order_id").isNull()).count()
        if invalid_event_count:
            raise RuntimeError(
                f"Found {invalid_event_count} CDC events without a valid OrderID"
            )

        affected_orders = parsed_events.select("order_id").distinct()

        latest_event_window = Window.partitionBy("order_id").orderBy(
            F.col("event_id").desc()
        )
        latest_event_by_order = (
            parsed_events.withColumn(
                "__event_row",
                F.row_number().over(latest_event_window),
            )
            .filter(F.col("__event_row") == 1)
            .select(
                "order_id",
                F.col("event_id").cast("long").alias("source_event_id"),
                F.col("start_lsn").alias("row_source_lsn"),
            )
        )

        orders = read_staging("orders").join(
            F.broadcast(affected_orders),
            "order_id",
            "inner",
        )
        order_details = read_staging("order_details").join(
            F.broadcast(affected_orders),
            "order_id",
            "inner",
        )

        customer_lookup = current_dimension_lookup(
            spark,
            clickhouse_database,
            "dim_customer",
            "customer_alternate_key",
            "customer_key",
            "lookup_customer_id",
            "lookup_customer_key",
            "string",
        )
        employee_lookup = current_dimension_lookup(
            spark,
            clickhouse_database,
            "dim_employee",
            "employee_alternate_key",
            "employee_key",
            "lookup_employee_id",
            "lookup_employee_key",
            "long",
        )
        shipper_lookup = current_dimension_lookup(
            spark,
            clickhouse_database,
            "dim_shipper",
            "shipper_alternate_key",
            "shipper_key",
            "lookup_shipper_id",
            "lookup_shipper_key",
            "long",
        )
        product_lookup = current_dimension_lookup(
            spark,
            clickhouse_database,
            "dim_product",
            "product_alternate_key",
            "product_key",
            "lookup_product_id",
            "lookup_product_key",
            "long",
        ).persist(StorageLevel.MEMORY_AND_DISK)

        current_source_pair_count = (
            orders.select("order_id")
            .join(order_details.select("order_id", "product_id"), "order_id", "inner")
            .count()
        )

        upsert_rows = (
            orders.alias("orders")
            .join(
                order_details.alias("details"),
                F.col("orders.order_id") == F.col("details.order_id"),
                "inner",
            )
            .join(
                product_lookup.alias("product"),
                F.col("details.product_id").cast("long")
                == F.col("product.lookup_product_id"),
                "inner",
            )
            .join(
                customer_lookup.alias("customer"),
                F.col("orders.customer_id") == F.col("customer.lookup_customer_id"),
                "left",
            )
            .join(
                employee_lookup.alias("employee"),
                F.col("orders.employee_id").cast("long")
                == F.col("employee.lookup_employee_id"),
                "left",
            )
            .join(
                shipper_lookup.alias("shipper"),
                F.col("orders.ship_via").cast("long")
                == F.col("shipper.lookup_shipper_id"),
                "left",
            )
            .join(
                latest_event_by_order.alias("event"),
                F.col("orders.order_id") == F.col("event.order_id"),
                "inner",
            )
            .select(
                F.col("orders.order_id").cast("long").alias("order_id"),
                geography_key(
                    F.col("orders.ship_country"),
                    F.col("orders.ship_region"),
                    F.col("orders.ship_city"),
                    F.col("orders.ship_postal_code"),
                    F.col("orders.ship_address"),
                ).alias("geography_key"),
                F.col("product.lookup_product_key").cast("long").alias("product_key"),
                F.coalesce(F.col("customer.lookup_customer_key"), F.lit(0))
                .cast("long")
                .alias("customer_key"),
                F.coalesce(F.col("employee.lookup_employee_key"), F.lit(0))
                .cast("long")
                .alias("employee_key"),
                F.coalesce(F.col("shipper.lookup_shipper_key"), F.lit(0))
                .cast("long")
                .alias("shipper_key"),
                date_key(F.col("orders.order_date")).alias("order_date_key"),
                date_key(F.col("orders.required_date")).alias("required_date_key"),
                date_key(F.col("orders.shipped_date")).alias("shipped_date_key"),
                F.coalesce(F.col("orders.freight"), F.lit(0))
                .cast("decimal(19,4)")
                .alias("freight"),
                clean_text(F.col("orders.ship_name")).alias("ship_name"),
                F.coalesce(F.col("details.unit_price"), F.lit(0))
                .cast("decimal(19,4)")
                .alias("unit_price"),
                F.coalesce(F.col("details.quantity"), F.lit(0))
                .cast("long")
                .alias("quantity"),
                F.coalesce(F.col("details.discount"), F.lit(0))
                .cast("decimal(9,6)")
                .alias("discount"),
                F.col("orders.order_date").cast("timestamp").alias("order_date"),
                F.col("orders.shipped_date").cast("timestamp").alias("shipped_date"),
                F.col("orders.required_date").cast("timestamp").alias("required_date"),
                F.lit(0).cast("short").alias("is_deleted"),
                F.col("event.row_source_lsn").alias("source_lsn"),
                F.col("event.source_event_id").cast("long").alias("_event_id"),
            )
        )

        upsert_count = upsert_rows.count()
        if upsert_count != current_source_pair_count:
            raise RuntimeError(
                "Fact lookup failure: "
                f"expected {current_source_pair_count} current rows, got {upsert_count}"
            )

        fact_source = spark.table(
            f"clickhouse.{clickhouse_database}.fact_orders"
        ).join(F.broadcast(affected_orders), "order_id", "inner")
        current_facts = (
            latest_rows(fact_source, ["order_id", "product_key"])
            .filter(F.col("is_deleted") == 0)
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        detail_delete_window = Window.partitionBy("order_id", "product_id").orderBy(
            F.col("event_id").desc()
        )
        detail_deletes = (
            parsed_events.filter(
                (F.col("capture_instance") == "dbo_Order_Details")
                & (F.col("operation_code") == 1)
                & F.col("product_id").isNotNull()
            )
            .withColumn(
                "__delete_row",
                F.row_number().over(detail_delete_window),
            )
            .filter(F.col("__delete_row") == 1)
            .select("order_id", "product_id", "event_id", "start_lsn")
        )

        deleted_detail_keys = (
            detail_deletes.alias("event")
            .join(
                product_lookup.alias("product"),
                F.col("event.product_id") == F.col("product.lookup_product_id"),
                "inner",
            )
            .select(
                F.col("event.order_id").alias("order_id"),
                F.col("product.lookup_product_key").alias("product_key"),
                F.col("event.event_id").alias("event_id"),
                F.col("event.start_lsn").alias("delete_source_lsn"),
            )
        )

        detail_tombstones = (
            current_facts.alias("fact")
            .join(
                deleted_detail_keys.alias("event"),
                (F.col("fact.order_id") == F.col("event.order_id"))
                & (F.col("fact.product_key") == F.col("event.product_key")),
                "inner",
            )
            .select(
                *tombstone_projection(
                    F.col("event.event_id"),
                    F.col("event.delete_source_lsn"),
                )
            )
        )

        order_delete_window = Window.partitionBy("order_id").orderBy(
            F.col("event_id").desc()
        )
        order_deletes = (
            parsed_events.filter(
                (F.col("capture_instance") == "dbo_Orders")
                & (F.col("operation_code") == 1)
            )
            .withColumn(
                "__delete_row",
                F.row_number().over(order_delete_window),
            )
            .filter(F.col("__delete_row") == 1)
            .select(
                "order_id",
                "event_id",
                F.col("start_lsn").alias("delete_source_lsn"),
            )
        )

        order_tombstones = (
            current_facts.alias("fact")
            .join(
                order_deletes.alias("event"),
                F.col("fact.order_id") == F.col("event.order_id"),
                "inner",
            )
            .select(
                *tombstone_projection(
                    F.col("event.event_id"),
                    F.col("event.delete_source_lsn"),
                )
            )
        )

        detail_tombstone_count = detail_tombstones.count()
        order_tombstone_count = order_tombstones.count()

        candidate_rows = (
            upsert_rows.unionByName(detail_tombstones)
            .unionByName(order_tombstones)
        )
        winner_window = Window.partitionBy("order_id", "product_key").orderBy(
            F.col("_event_id").desc()
        )
        output_base = (
            candidate_rows.withColumn(
                "__winner_row",
                F.row_number().over(winner_window),
            )
            .filter(F.col("__winner_row") == 1)
            .drop("__winner_row", "_event_id")
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        output_count = output_base.count()
        print(f"INCREMENTAL_EVENTS_READ={len(event_ids)}")
        print(f"INCREMENTAL_UPSERT_ROWS={upsert_count}")
        print(f"INCREMENTAL_DETAIL_TOMBSTONES={detail_tombstone_count}")
        print(f"INCREMENTAL_ORDER_TOMBSTONES={order_tombstone_count}")
        print(f"INCREMENTAL_ROWS_TO_WRITE={output_count}")

        if output_count:
            output = add_metadata(
                output_base,
                batch_id,
                loaded_at,
                version,
            )
            (
                output.coalesce(1)
                .writeTo(f"clickhouse.{clickhouse_database}.fact_orders")
                .append()
            )

        event_id_list = ",".join(str(event_id) for event_id in event_ids)
        updated_events = execute_postgres_sql(
            spark,
            postgres_url,
            postgres_user,
            postgres_password,
            f"""
            UPDATE cdc.raw_events
            SET
                dw_processed_at = CURRENT_TIMESTAMP,
                dw_batch_id = {sql_literal(batch_id)}::UUID,
                dw_processing_error = NULL,
                dw_attempt_count = dw_attempt_count + 1
            WHERE event_id IN ({event_id_list})
            """,
        )

        if updated_events != len(event_ids):
            raise RuntimeError(
                f"Marked {updated_events} events, expected {len(event_ids)}"
            )

        update_service_state(
            spark,
            postgres_url,
            postgres_user,
            postgres_password,
            "success",
            {
                "batch_id": batch_id,
                "events_read": len(event_ids),
                "rows_written": output_count,
                "min_event_id": min(event_ids),
                "max_event_id": max(event_ids),
            },
        )

        print(f"INCREMENTAL_EVENTS_MARKED={updated_events}")
        print("INCREMENTAL_TRANSFORM=PASS")

    except Exception as exc:
        error_message = f"{type(exc).__name__}: {exc}"

        if event_ids:
            event_id_list = ",".join(str(event_id) for event_id in event_ids)
            try:
                execute_postgres_sql(
                    spark,
                    postgres_url,
                    postgres_user,
                    postgres_password,
                    f"""
                    UPDATE cdc.raw_events
                    SET
                        dw_processing_error = {sql_literal(error_message[:4000])},
                        dw_attempt_count = dw_attempt_count + 1
                    WHERE event_id IN ({event_id_list})
                    """,
                )
            except Exception as marker_error:
                print(f"INCREMENTAL_ERROR_MARKING_FAILED={marker_error}")

        try:
            update_service_state(
                spark,
                postgres_url,
                postgres_user,
                postgres_password,
                "failed",
                {
                    "batch_id": batch_id,
                    "events_read": len(event_ids),
                    "rows_written": 0,
                },
                error_message,
            )
        except Exception as state_error:
            print(f"INCREMENTAL_STATE_UPDATE_FAILED={state_error}")

        print(f"INCREMENTAL_TRANSFORM=FAILED: {error_message}")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
