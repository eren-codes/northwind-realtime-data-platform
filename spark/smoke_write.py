import os
import time
import uuid
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def required_env(name: str) -> str:
    value = os.getenv(name)

    if not value:
        raise RuntimeError(
            f"Required environment variable is missing: {name}"
        )

    return value


postgres_host = os.getenv(
    "POSTGRES_HOST",
    "postgres-staging",
)
postgres_port = os.getenv("POSTGRES_PORT", "5432")
postgres_database = required_env("POSTGRES_DB")
postgres_user = required_env("POSTGRES_USER")
postgres_password = required_env("POSTGRES_PASSWORD")

clickhouse_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
clickhouse_port = os.getenv(
    "CLICKHOUSE_HTTP_PORT",
    "8123",
)
clickhouse_database = required_env("CLICKHOUSE_DB")
clickhouse_user = required_env("CLICKHOUSE_USER")
clickhouse_password = required_env(
    "CLICKHOUSE_PASSWORD"
)


spark = (
    SparkSession.builder
    .appName("northwind-smoke-write")
    .config(
        "spark.sql.catalog.clickhouse",
        "com.clickhouse.spark.ClickHouseCatalog",
    )
    .config(
        "spark.sql.catalog.clickhouse.host",
        clickhouse_host,
    )
    .config(
        "spark.sql.catalog.clickhouse.protocol",
        "http",
    )
    .config(
        "spark.sql.catalog.clickhouse.http_port",
        clickhouse_port,
    )
    .config(
        "spark.sql.catalog.clickhouse.user",
        clickhouse_user,
    )
    .config(
        "spark.sql.catalog.clickhouse.password",
        clickhouse_password,
    )
    .config(
        "spark.sql.catalog.clickhouse.database",
        clickhouse_database,
    )
    .config(
        "spark.clickhouse.write.format",
        "json",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    postgres_url = (
        f"jdbc:postgresql://{postgres_host}:"
        f"{postgres_port}/{postgres_database}"
    )

    source_df = (
        spark.read
        .format("jdbc")
        .option("url", postgres_url)
        .option("dbtable", "staging.shippers")
        .option("user", postgres_user)
        .option("password", postgres_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    batch_id = str(uuid.uuid4())
    loaded_at = datetime.now(timezone.utc).replace(
        tzinfo=None
    )
    version = int(time.time() * 1000)

    max_positive_key = 9223372036854775806

    shipper_key = (
        F.pmod(
            F.xxhash64(
                F.lit("shipper"),
                F.col("shipper_id").cast("string"),
            ),
            F.lit(max_positive_key),
        )
        + F.lit(1)
    ).cast("long")

    source_hash = F.sha2(
        F.concat_ws(
            "||",
            F.coalesce(
                F.trim(F.col("company_name")),
                F.lit("<NULL>"),
            ),
            F.coalesce(
                F.trim(F.col("phone")),
                F.lit("<NULL>"),
            ),
        ),
        256,
    )

    target_df = source_df.select(
        shipper_key.alias("shipper_key"),
        F.col("shipper_id")
        .cast("long")
        .alias("shipper_alternate_key"),
        F.trim(F.col("company_name"))
        .alias("company_name"),
        F.trim(F.col("phone")).alias("phone"),
        source_hash.alias("source_hash"),
        F.lit(batch_id).alias("_batch_id"),
        F.lit(loaded_at)
        .cast("timestamp")
        .alias("_loaded_at"),
        F.lit(version)
        .cast("long")
        .alias("_version"),
    )

    source_count = target_df.count()

    print(f"SOURCE_SHIPPERS={source_count}")

    spark.sql(
        "TRUNCATE TABLE "
        f"clickhouse.{clickhouse_database}.dim_shipper"
    )

    (
        target_df.coalesce(1)
        .writeTo(
            "clickhouse."
            f"{clickhouse_database}.dim_shipper"
        )
        .append()
    )

    target_count = spark.table(
        "clickhouse."
        f"{clickhouse_database}.dim_shipper"
    ).count()

    print(f"TARGET_SHIPPERS={target_count}")

    if source_count != 3:
        raise RuntimeError(
            f"Expected 3 source shippers, got {source_count}"
        )

    if target_count != source_count:
        raise RuntimeError(
            "Source and target counts are different"
        )

    print("SPARK_WRITE_TEST=PASS")

finally:
    spark.stop()
