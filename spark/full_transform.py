import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from functools import reduce

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


MAX_POSITIVE_KEY = 9223372036854775806


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


def canonical(column):
    return F.coalesce(F.trim(column.cast("string")), F.lit("<NULL>"))


def clean_text(column):
    value = F.trim(column.cast("string"))
    return F.when(column.isNull() | (value == ""), F.lit(None).cast("string")).otherwise(value)


def stable_key(namespace: str, *columns):
    return (
        F.pmod(
            F.xxhash64(F.lit(namespace), *[canonical(column) for column in columns]),
            F.lit(MAX_POSITIVE_KEY),
        )
        + F.lit(1)
    ).cast("long")


def row_hash(*columns):
    return F.sha2(F.concat_ws("||", *[canonical(column) for column in columns]), 256)


def geography_key(country, region, city, postal_code, address):
    return stable_key("geography", country, region, city, postal_code, address)


def date_key(column):
    return (
        F.when(column.isNull(), F.lit(0))
        .otherwise(F.date_format(column, "yyyyMMdd").cast("long"))
        .cast("long")
    )


def add_metadata(
    dataframe: DataFrame,
    batch_id: str,
    loaded_at: datetime,
    version: int,
    include_version: bool = True,
) -> DataFrame:
    result = (
        dataframe.withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_loaded_at", F.lit(loaded_at).cast("timestamp"))
    )
    if include_version:
        result = result.withColumn("_version", F.lit(version).cast("long"))
    return result


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

    spark = (
        SparkSession.builder.appName("northwind-full-transform")
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

    def read_staging(table_name: str) -> DataFrame:
        return (
            spark.read.format("jdbc")
            .option("url", postgres_url)
            .option("dbtable", f"staging.{table_name}")
            .option("user", postgres_user)
            .option("password", postgres_password)
            .option("driver", "org.postgresql.Driver")
            .option("fetchsize", "1000")
            .load()
        )

    batch_id = str(uuid.uuid4())
    loaded_at = datetime.now(timezone.utc).replace(tzinfo=None)
    version = int(time.time() * 1000)

    print(f"FULL_TRANSFORM_BATCH_ID={batch_id}")

    try:
        customers = read_staging("customers")
        employees = read_staging("employees")
        suppliers = read_staging("suppliers")
        categories = read_staging("categories")
        products = read_staging("products")
        shippers = read_staging("shippers")
        regions = read_staging("regions")
        territories = read_staging("territories")
        employee_territories = read_staging("employee_territories")
        orders = read_staging("orders")
        order_details = read_staging("order_details")

        # Geography is built from customer, employee, supplier, and order shipping addresses.
        geography_sources = [
            customers.select(
                clean_text(F.col("country")).alias("country"),
                clean_text(F.col("region")).alias("region"),
                clean_text(F.col("city")).alias("city"),
                clean_text(F.col("postal_code")).alias("postal_code"),
                clean_text(F.col("address")).alias("address"),
            ),
            employees.select(
                clean_text(F.col("country")).alias("country"),
                clean_text(F.col("region")).alias("region"),
                clean_text(F.col("city")).alias("city"),
                clean_text(F.col("postal_code")).alias("postal_code"),
                clean_text(F.col("address")).alias("address"),
            ),
            suppliers.select(
                clean_text(F.col("country")).alias("country"),
                clean_text(F.col("region")).alias("region"),
                clean_text(F.col("city")).alias("city"),
                clean_text(F.col("postal_code")).alias("postal_code"),
                clean_text(F.col("address")).alias("address"),
            ),
            orders.select(
                clean_text(F.col("ship_country")).alias("country"),
                clean_text(F.col("ship_region")).alias("region"),
                clean_text(F.col("ship_city")).alias("city"),
                clean_text(F.col("ship_postal_code")).alias("postal_code"),
                clean_text(F.col("ship_address")).alias("address"),
            ),
        ]

        geography_base = reduce(DataFrame.unionByName, geography_sources).dropDuplicates(
            ["country", "region", "city", "postal_code", "address"]
        )

        geography_dim = geography_base.select(
            geography_key(
                F.col("country"),
                F.col("region"),
                F.col("city"),
                F.col("postal_code"),
                F.col("address"),
            ).alias("geography_key"),
            "country",
            "region",
            "city",
            "postal_code",
            "address",
            row_hash(
                F.col("country"),
                F.col("region"),
                F.col("city"),
                F.col("postal_code"),
                F.col("address"),
            ).alias("source_hash"),
        )
        geography_dim = add_metadata(geography_dim, batch_id, loaded_at, version)

        customer_base = customers.select(
            F.col("customer_id"),
            clean_text(F.col("company_name")).alias("company_name"),
            clean_text(F.col("contact_name")).alias("contact_name"),
            clean_text(F.col("contact_title")).alias("contact_title"),
            clean_text(F.col("phone")).alias("phone"),
            clean_text(F.col("fax")).alias("fax"),
            geography_key(
                F.col("country"),
                F.col("region"),
                F.col("city"),
                F.col("postal_code"),
                F.col("address"),
            ).alias("geography_key"),
        ).withColumn(
            "source_hash",
            row_hash(
                F.col("company_name"),
                F.col("contact_name"),
                F.col("contact_title"),
                F.col("phone"),
                F.col("fax"),
                F.col("geography_key"),
            ),
        )

        customer_dim = customer_base.select(
            stable_key(
                "customer",
                F.col("customer_id"),
                F.col("source_hash"),
            ).alias("customer_key"),
            F.col("customer_id").alias("customer_alternate_key"),
            F.col("geography_key"),
            F.col("company_name"),
            F.col("contact_name"),
            F.col("contact_title"),
            F.col("phone"),
            F.col("fax"),
            F.lit(loaded_at).cast("timestamp").alias("start_date"),
            F.lit(None).cast("timestamp").alias("end_date"),
            F.lit(1).cast("short").alias("is_current"),
            F.col("source_hash"),
        )
        customer_dim = add_metadata(customer_dim, batch_id, loaded_at, version)

        employee_base = employees.select(
            F.col("employee_id").cast("long").alias("employee_id"),
            F.col("reports_to").cast("long").alias("reports_to"),
            geography_key(
                F.col("country"),
                F.col("region"),
                F.col("city"),
                F.col("postal_code"),
                F.col("address"),
            ).alias("geography_key"),
            clean_text(F.col("first_name")).alias("first_name"),
            clean_text(F.col("last_name")).alias("last_name"),
            clean_text(F.col("title")).alias("title"),
            clean_text(F.col("title_of_courtesy")).alias("title_of_courtesy"),
            F.to_date(F.col("birth_date")).alias("birth_date"),
            F.to_date(F.col("hire_date")).alias("hire_date"),
            clean_text(F.col("home_phone")).alias("home_phone"),
            clean_text(F.col("extension")).alias("extension"),
            F.coalesce(F.base64(F.col("photo")), F.lit("")).alias("photo_base64"),
            clean_text(F.col("notes")).alias("notes"),
            clean_text(F.col("photo_path")).alias("photo_path"),
        ).withColumn(
            "source_hash",
            row_hash(
                F.col("reports_to"),
                F.col("geography_key"),
                F.col("first_name"),
                F.col("last_name"),
                F.col("title"),
                F.col("title_of_courtesy"),
                F.col("birth_date"),
                F.col("hire_date"),
                F.col("home_phone"),
                F.col("extension"),
                F.col("notes"),
                F.col("photo_path"),
            ),
        ).withColumn(
            "employee_key",
            stable_key("employee", F.col("employee_id"), F.col("source_hash")),
        )

        employee_lookup = employee_base.select(
            F.col("employee_id").alias("lookup_employee_id"),
            F.col("employee_key").alias("lookup_employee_key"),
        )

        employee_dim = (
            employee_base.alias("employee")
            .join(
                employee_lookup.alias("manager"),
                F.col("employee.reports_to") == F.col("manager.lookup_employee_id"),
                "left",
            )
            .select(
                F.col("employee.employee_key").alias("employee_key"),
                F.coalesce(F.col("manager.lookup_employee_key"), F.lit(0)).cast("long").alias(
                    "parent_employee_key"
                ),
                F.col("employee.employee_id").cast("long").alias("employee_alternate_key"),
                F.col("employee.reports_to").cast("long").alias("reports_to"),
                F.col("employee.geography_key"),
                F.col("employee.first_name"),
                F.col("employee.last_name"),
                F.col("employee.title"),
                F.col("employee.title_of_courtesy"),
                F.col("employee.birth_date"),
                F.col("employee.hire_date"),
                F.col("employee.home_phone"),
                F.col("employee.extension"),
                F.col("employee.photo_base64"),
                F.col("employee.notes"),
                F.col("employee.photo_path"),
                F.lit(loaded_at).cast("timestamp").alias("start_date"),
                F.lit(None).cast("timestamp").alias("end_date"),
                F.lit(1).cast("short").alias("is_current"),
                F.col("employee.source_hash"),
            )
        )
        employee_dim = add_metadata(employee_dim, batch_id, loaded_at, version)

        supplier_base = suppliers.select(
            F.col("supplier_id").cast("long").alias("supplier_id"),
            geography_key(
                F.col("country"),
                F.col("region"),
                F.col("city"),
                F.col("postal_code"),
                F.col("address"),
            ).alias("geography_key"),
            clean_text(F.col("company_name")).alias("company_name"),
            clean_text(F.col("contact_name")).alias("contact_name"),
            clean_text(F.col("contact_title")).alias("contact_title"),
            clean_text(F.col("phone")).alias("phone"),
            clean_text(F.col("fax")).alias("fax"),
            clean_text(F.col("home_page")).alias("home_page"),
        ).withColumn(
            "source_hash",
            row_hash(
                F.col("geography_key"),
                F.col("company_name"),
                F.col("contact_name"),
                F.col("contact_title"),
                F.col("phone"),
                F.col("fax"),
                F.col("home_page"),
            ),
        ).withColumn(
            "supplier_key",
            stable_key("supplier", F.col("supplier_id"), F.col("source_hash")),
        )

        supplier_dim = supplier_base.select(
            "supplier_key",
            F.col("supplier_id").cast("long").alias("supplier_alternate_key"),
            "geography_key",
            "company_name",
            "contact_name",
            "contact_title",
            "phone",
            "fax",
            "home_page",
            F.lit(loaded_at).cast("timestamp").alias("start_date"),
            F.lit(None).cast("timestamp").alias("end_date"),
            F.lit(1).cast("short").alias("is_current"),
            "source_hash",
        )
        supplier_dim = add_metadata(supplier_dim, batch_id, loaded_at, version)

        supplier_lookup = supplier_base.select("supplier_id", "supplier_key")

        product_base = (
            products.alias("product")
            .join(categories.alias("category"), F.col("product.category_id") == F.col("category.category_id"), "left")
            .join(supplier_lookup.alias("supplier"), F.col("product.supplier_id") == F.col("supplier.supplier_id"), "left")
            .select(
                F.col("product.product_id").cast("long").alias("product_id"),
                F.coalesce(F.col("supplier.supplier_key"), F.lit(0)).cast("long").alias("supplier_key"),
                clean_text(F.col("product.product_name")).alias("product_name"),
                clean_text(F.col("category.category_name")).alias("category_name"),
                clean_text(F.col("product.quantity_per_unit")).alias("quantity_per_unit"),
                F.coalesce(F.col("product.unit_price"), F.lit(0)).cast("decimal(19,4)").alias("unit_price"),
                F.coalesce(F.col("product.units_in_stock"), F.lit(0)).cast("int").alias("units_in_stock"),
                F.coalesce(F.col("product.units_on_order"), F.lit(0)).cast("int").alias("units_on_order"),
                F.coalesce(F.col("product.reorder_level"), F.lit(0)).cast("int").alias("reorder_level"),
                F.coalesce(F.col("product.discontinued").cast("int"), F.lit(0)).cast("short").alias("discontinued"),
            )
            .withColumn(
                "source_hash",
                row_hash(
                    F.col("supplier_key"),
                    F.col("product_name"),
                    F.col("category_name"),
                    F.col("quantity_per_unit"),
                    F.col("unit_price"),
                    F.col("units_in_stock"),
                    F.col("units_on_order"),
                    F.col("reorder_level"),
                    F.col("discontinued"),
                ),
            )
            .withColumn(
                "product_key",
                stable_key("product", F.col("product_id"), F.col("source_hash")),
            )
        )

        product_dim = product_base.select(
            "product_key",
            F.col("product_id").cast("long").alias("product_alternate_key"),
            "supplier_key",
            "product_name",
            "category_name",
            "quantity_per_unit",
            "unit_price",
            "units_in_stock",
            "units_on_order",
            "reorder_level",
            "discontinued",
            F.lit(loaded_at).cast("timestamp").alias("start_date"),
            F.lit(None).cast("timestamp").alias("end_date"),
            F.lit(1).cast("short").alias("is_current"),
            "source_hash",
        )
        product_dim = add_metadata(product_dim, batch_id, loaded_at, version)

        shipper_base = (
            shippers.select(
                F.col("shipper_id").cast("long").alias("shipper_id"),
                clean_text(F.col("company_name")).alias("company_name"),
                clean_text(F.col("phone")).alias("phone"),
            )
            .withColumn("source_hash", row_hash(F.col("company_name"), F.col("phone")))
            .withColumn("shipper_key", stable_key("shipper", F.col("shipper_id")))
        )

        shipper_dim = shipper_base.select(
            "shipper_key",
            F.col("shipper_id").cast("long").alias("shipper_alternate_key"),
            "company_name",
            "phone",
            "source_hash",
        )
        shipper_dim = add_metadata(shipper_dim, batch_id, loaded_at, version)

        territory_base = (
            territories.alias("territory")
            .join(regions.alias("region"), F.col("territory.region_id") == F.col("region.region_id"), "left")
            .select(
                F.col("territory.territory_id").alias("territory_id"),
                clean_text(F.col("region.region_description")).alias("region_description"),
                clean_text(F.col("territory.territory_description")).alias("territory_description"),
            )
            .withColumn(
                "source_hash",
                row_hash(F.col("region_description"), F.col("territory_description")),
            )
            .withColumn(
                "territory_key",
                stable_key("territory", F.col("territory_id"), F.col("source_hash")),
            )
        )

        territory_dim = territory_base.select(
            "territory_key",
            F.col("territory_id").alias("territory_alternate_key"),
            "region_description",
            "territory_description",
            F.lit(loaded_at).cast("timestamp").alias("start_date"),
            F.lit(None).cast("timestamp").alias("end_date"),
            F.lit(1).cast("short").alias("is_current"),
            "source_hash",
        )
        territory_dim = add_metadata(territory_dim, batch_id, loaded_at, version)

        date_bounds = (
            orders.select(
                F.explode(
                    F.array(
                        F.to_date(F.col("order_date")),
                        F.to_date(F.col("required_date")),
                        F.to_date(F.col("shipped_date")),
                    )
                ).alias("full_date")
            )
            .where(F.col("full_date").isNotNull())
            .agg(
                F.min("full_date").alias("min_date"),
                F.max("full_date").alias("max_date"),
            )
            .first()
        )

        if not date_bounds or not date_bounds["min_date"] or not date_bounds["max_date"]:
            raise RuntimeError("Could not calculate date dimension boundaries")

        start_date = date_bounds["min_date"] - timedelta(days=365)
        end_date = date_bounds["max_date"] + timedelta(days=365)

        date_dim = spark.range(1).select(
            F.explode(F.sequence(F.lit(start_date), F.lit(end_date))).alias("full_date_alternate_key")
        ).select(
            F.date_format(F.col("full_date_alternate_key"), "yyyyMMdd").cast("long").alias("date_key"),
            F.col("full_date_alternate_key"),
            F.year(F.col("full_date_alternate_key")).cast("int").alias("calendar_year"),
            F.quarter(F.col("full_date_alternate_key")).cast("short").alias("calendar_season"),
            F.concat(F.lit("Q"), F.quarter(F.col("full_date_alternate_key"))).alias("season_name"),
            F.month(F.col("full_date_alternate_key")).cast("short").alias("month_number_of_year"),
            F.date_format(F.col("full_date_alternate_key"), "MMMM").alias("month_name"),
            F.dayofmonth(F.col("full_date_alternate_key")).cast("short").alias("day_number_of_month"),
            F.dayofweek(F.col("full_date_alternate_key")).cast("short").alias("day_of_week"),
            F.date_format(F.col("full_date_alternate_key"), "EEEE").alias("day_of_week_name"),
            F.when(F.dayofweek(F.col("full_date_alternate_key")).isin(1, 7), F.lit(1))
            .otherwise(F.lit(0))
            .cast("short")
            .alias("is_weekend"),
        )
        date_dim = add_metadata(date_dim, batch_id, loaded_at, version, include_version=False)

        customer_lookup = customer_dim.select(
            F.col("customer_alternate_key").alias("lookup_customer_id"),
            F.col("customer_key").alias("lookup_customer_key"),
        )
        employee_fact_lookup = employee_dim.select(
            F.col("employee_alternate_key").alias("lookup_employee_id"),
            F.col("employee_key").alias("lookup_employee_key"),
        )
        shipper_lookup = shipper_dim.select(
            F.col("shipper_alternate_key").alias("lookup_shipper_id"),
            F.col("shipper_key").alias("lookup_shipper_key"),
        )
        product_lookup = product_dim.select(
            F.col("product_alternate_key").alias("lookup_product_id"),
            F.col("product_key").alias("lookup_product_key"),
        )
        territory_lookup = territory_dim.select(
            F.col("territory_alternate_key").alias("lookup_territory_id"),
            F.col("territory_key").alias("lookup_territory_key"),
        )

        fact_employee_territories = (
            employee_territories.alias("bridge")
            .join(
                employee_fact_lookup.alias("employee"),
                F.col("bridge.employee_id") == F.col("employee.lookup_employee_id"),
                "inner",
            )
            .join(
                territory_lookup.alias("territory"),
                F.col("bridge.territory_id") == F.col("territory.lookup_territory_id"),
                "inner",
            )
            .select(
                F.col("employee.lookup_employee_key").cast("long").alias("employee_key"),
                F.col("territory.lookup_territory_key").cast("long").alias("territory_key"),
                F.lit(0).cast("short").alias("is_deleted"),
                F.lit("").alias("source_lsn"),
            )
        )
        fact_employee_territories = add_metadata(
            fact_employee_territories, batch_id, loaded_at, version
        )

        fact_orders = (
            orders.alias("orders")
            .join(
                order_details.alias("details"),
                F.col("orders.order_id") == F.col("details.order_id"),
                "inner",
            )
            .join(
                product_lookup.alias("product"),
                F.col("details.product_id") == F.col("product.lookup_product_id"),
                "inner",
            )
            .join(
                customer_lookup.alias("customer"),
                F.col("orders.customer_id") == F.col("customer.lookup_customer_id"),
                "left",
            )
            .join(
                employee_fact_lookup.alias("employee"),
                F.col("orders.employee_id") == F.col("employee.lookup_employee_id"),
                "left",
            )
            .join(
                shipper_lookup.alias("shipper"),
                F.col("orders.ship_via") == F.col("shipper.lookup_shipper_id"),
                "left",
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
                F.coalesce(F.col("customer.lookup_customer_key"), F.lit(0)).cast("long").alias(
                    "customer_key"
                ),
                F.coalesce(F.col("employee.lookup_employee_key"), F.lit(0)).cast("long").alias(
                    "employee_key"
                ),
                F.coalesce(F.col("shipper.lookup_shipper_key"), F.lit(0)).cast("long").alias(
                    "shipper_key"
                ),
                date_key(F.col("orders.order_date")).alias("order_date_key"),
                date_key(F.col("orders.required_date")).alias("required_date_key"),
                date_key(F.col("orders.shipped_date")).alias("shipped_date_key"),
                F.coalesce(F.col("orders.freight"), F.lit(0)).cast("decimal(19,4)").alias("freight"),
                clean_text(F.col("orders.ship_name")).alias("ship_name"),
                F.coalesce(F.col("details.unit_price"), F.lit(0)).cast("decimal(19,4)").alias(
                    "unit_price"
                ),
                F.coalesce(F.col("details.quantity"), F.lit(0)).cast("long").alias("quantity"),
                F.coalesce(F.col("details.discount"), F.lit(0)).cast("decimal(9,6)").alias(
                    "discount"
                ),
                F.col("orders.order_date").cast("timestamp").alias("order_date"),
                F.col("orders.shipped_date").cast("timestamp").alias("shipped_date"),
                F.col("orders.required_date").cast("timestamp").alias("required_date"),
                F.lit(0).cast("short").alias("is_deleted"),
                F.lit("").alias("source_lsn"),
            )
        )
        fact_orders = add_metadata(fact_orders, batch_id, loaded_at, version)

        outputs = {
            "dim_date": date_dim,
            "dim_geography": geography_dim,
            "dim_customer": customer_dim,
            "dim_employee": employee_dim,
            "dim_supplier": supplier_dim,
            "dim_product": product_dim,
            "dim_shipper": shipper_dim,
            "dim_territory": territory_dim,
            "fact_employee_territories": fact_employee_territories,
            "fact_orders": fact_orders,
        }

        expected_counts = {
            "dim_customer": customers.count(),
            "dim_employee": employees.count(),
            "dim_supplier": suppliers.count(),
            "dim_product": products.count(),
            "dim_shipper": shippers.count(),
            "dim_territory": territories.count(),
            "fact_employee_territories": employee_territories.count(),
            "fact_orders": order_details.count(),
        }

        source_counts = {}
        for table_name, dataframe in outputs.items():
            dataframe.persist(StorageLevel.MEMORY_AND_DISK)
            row_count = dataframe.count()
            source_counts[table_name] = row_count
            print(f"TRANSFORMED_{table_name.upper()}={row_count}")

            expected = expected_counts.get(table_name)
            if expected is not None and row_count != expected:
                raise RuntimeError(
                    f"Data-quality failure for {table_name}: expected {expected}, got {row_count}"
                )

        print("PRE_LOAD_DATA_QUALITY=PASS")

        # Full load is rerunnable: clear target tables only after all transformations pass.
        for table_name in reversed(list(outputs.keys())):
            spark.sql(f"TRUNCATE TABLE clickhouse.{clickhouse_database}.{table_name}")

        for table_name, dataframe in outputs.items():
            print(f"LOADING_{table_name.upper()}={source_counts[table_name]}")
            (
                dataframe.coalesce(1)
                .writeTo(f"clickhouse.{clickhouse_database}.{table_name}")
                .append()
            )

        for table_name, expected in source_counts.items():
            actual = spark.table(f"clickhouse.{clickhouse_database}.{table_name}").count()
            print(f"CLICKHOUSE_{table_name.upper()}={actual}")
            if actual != expected:
                raise RuntimeError(
                    f"Post-load validation failed for {table_name}: expected {expected}, got {actual}"
                )

        print("POST_LOAD_DATA_QUALITY=PASS")
        print("FULL_TRANSFORM=PASS")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
