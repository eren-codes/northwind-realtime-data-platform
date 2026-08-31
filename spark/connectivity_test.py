import os

from pyspark.sql import SparkSession


def required_env(name: str) -> str:
    value = os.getenv(name)

    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")

    return value


postgres_host = os.getenv("POSTGRES_HOST", "postgres-staging")
postgres_port = os.getenv("POSTGRES_PORT", "5432")
postgres_database = required_env("POSTGRES_DB")
postgres_user = required_env("POSTGRES_USER")
postgres_password = required_env("POSTGRES_PASSWORD")

clickhouse_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
clickhouse_http_port = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
clickhouse_database = required_env("CLICKHOUSE_DB")
clickhouse_user = required_env("CLICKHOUSE_USER")
clickhouse_password = required_env("CLICKHOUSE_PASSWORD")


spark = (
    SparkSession.builder
    .appName("northwind-connectivity-test")
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
        clickhouse_http_port,
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

    postgres_result = (
        spark.read
        .format("jdbc")
        .option("url", postgres_url)
        .option(
            "query",
            "SELECT COUNT(*)::bigint AS row_count "
            "FROM staging.orders",
        )
        .option("user", postgres_user)
        .option("password", postgres_password)
        .option("driver", "org.postgresql.Driver")
        .load()
        .first()
    )

    postgres_orders = int(postgres_result["row_count"])

    clickhouse_url = (
        f"jdbc:ch://{clickhouse_host}:"
        f"{clickhouse_http_port}/{clickhouse_database}"
    )

    clickhouse_result = (
        spark.read
        .format("jdbc")
        .option("url", clickhouse_url)
        .option(
            "query",
            "SELECT count() AS row_count "
            "FROM fact_orders",
        )
        .option("user", clickhouse_user)
        .option("password", clickhouse_password)
        .option(
            "driver",
            "com.clickhouse.jdbc.ClickHouseDriver",
        )
        .load()
        .first()
    )

    clickhouse_fact_orders = int(
        clickhouse_result["row_count"]
    )

    clickhouse_tables = len(
        spark.sql(
            f"SHOW TABLES IN clickhouse.{clickhouse_database}"
        ).collect()
    )

    print(f"SPARK_VERSION={spark.version}")
    print(f"POSTGRES_ORDERS={postgres_orders}")
    print(
        "CLICKHOUSE_FACT_ORDERS="
        f"{clickhouse_fact_orders}"
    )
    print(f"CLICKHOUSE_TABLES={clickhouse_tables}")

    if postgres_orders != 830:
        raise RuntimeError(
            "PostgreSQL orders count must be 830, "
            f"but received {postgres_orders}"
        )

    if clickhouse_tables != 10:
        raise RuntimeError(
            "ClickHouse table count must be 10, "
            f"but received {clickhouse_tables}"
        )

    print("CONNECTIVITY_TEST=PASS")

finally:
    spark.stop()
