from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG


PROJECT_DIR = os.getenv(
    "NORTHWIND_PROJECT_DIR",
    "/home/elham/northwind-realtime-data-platform",
)


def project_command(command: str) -> str:
    """Run a strict Bash task from the host-identical project directory."""
    return f"""
set -euo pipefail
cd "${{NORTHWIND_PROJECT_DIR:?NORTHWIND_PROJECT_DIR is required}}"

compose() {{
  docker compose \
    -f docker-compose.yml \
    -f docker-compose.airflow.yml \
    "$@"
}}

{command}
"""


default_args = {
    "owner": "elham",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="northwind_full_load",
    description="SQL Server to PostgreSQL staging, Data Lake, Spark, and ClickHouse",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 8, 28, tz="Asia/Tehran"),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    dagrun_timeout=timedelta(hours=2),
    tags=["northwind", "full-load", "data-engineering"],
    doc_md="""
    # Northwind Full Load

    This DAG orchestrates the complete batch path:

    1. Validate Docker and Compose configuration.
    2. Load SQL Server OLTP tables into PostgreSQL staging.
    3. Extract employee images into raw and curated Data Lake zones.
    4. Transform dimensions and facts with PySpark.
    5. Verify ClickHouse row counts and employee image paths.

    The schedule is **02:00 Asia/Tehran** and catchup is disabled.
    """,
) as dag:
    preflight = BashOperator(
        task_id="preflight",
        bash_command=project_command(
            """
test -S /var/run/docker.sock
test -f docker-compose.yml
test -f docker-compose.airflow.yml
test -f etl/extract_employee_photos.py
test -f spark/full_transform.py

docker version >/dev/null
docker compose version
compose config --quiet

echo "PREFLIGHT=PASS"
"""
        ),
        env={"NORTHWIND_PROJECT_DIR": PROJECT_DIR},
        append_env=True,
        execution_timeout=timedelta(minutes=5),
    )

    full_load_staging = BashOperator(
        task_id="full_load_staging",
        bash_command=project_command(
            """
compose --profile tools run --rm etl-runner
"""
        ),
        env={"NORTHWIND_PROJECT_DIR": PROJECT_DIR},
        append_env=True,
        execution_timeout=timedelta(minutes=30),
    )

    extract_employee_photos = BashOperator(
        task_id="extract_employee_photos",
        bash_command=project_command(
            """
compose --profile tools run --rm \
  --user "$(id -u):$(id -g)" \
  etl-runner python /app/etl/extract_employee_photos.py

test "$(find data-lake/raw/employee-photos -maxdepth 1 -type f -name 'employee_*.bin' | wc -l)" -eq 9
test "$(find data-lake/curated/employee-photos -maxdepth 1 -type f -name 'employee_*.bmp' | wc -l)" -eq 9
test -f data-lake/manifests/employee_photos.json

echo "DATA_LAKE_VERIFICATION=PASS"
"""
        ),
        env={"NORTHWIND_PROJECT_DIR": PROJECT_DIR},
        append_env=True,
        execution_timeout=timedelta(minutes=15),
    )

    spark_full_transform = BashOperator(
        task_id="spark_full_transform",
        bash_command=project_command(
            """
compose --profile tools run --rm spark-runner \
  /opt/spark/bin/spark-submit \
  --master "local[1]" \
  --driver-memory 768m \
  --conf spark.sql.shuffle.partitions=1 \
  --conf spark.default.parallelism=1 \
  --conf spark.ui.enabled=false \
  /opt/spark/work-dir/spark/full_transform.py
"""
        ),
        env={"NORTHWIND_PROJECT_DIR": PROJECT_DIR},
        append_env=True,
        execution_timeout=timedelta(minutes=60),
    )

    verify_clickhouse = BashOperator(
        task_id="verify_clickhouse",
        bash_command=project_command(
            """
compose exec -T clickhouse bash -lc '
set -euo pipefail

query() {
  clickhouse-client \
    --user "$CLICKHOUSE_USER" \
    --password "$CLICKHOUSE_PASSWORD" \
    --database "$CLICKHOUSE_DB" \
    --format TSVRaw \
    --query "$1"
}

employee_count="$(query "SELECT count() FROM dim_employee FINAL WHERE is_current = 1")"
fact_count="$(query "SELECT count() FROM fact_orders FINAL WHERE is_deleted = 0")"
photo_path_count="$(query "SELECT count() FROM dim_employee FINAL WHERE is_current = 1 AND length(photo_path) > 0")"

test "$employee_count" = "9"
test "$fact_count" = "2155"
test "$photo_path_count" = "9"

echo "CLICKHOUSE_DIM_EMPLOYEE=$employee_count"
echo "CLICKHOUSE_FACT_ORDERS=$fact_count"
echo "CLICKHOUSE_EMPLOYEE_PHOTO_PATHS=$photo_path_count"
echo "AIRFLOW_WAREHOUSE_VERIFICATION=PASS"
'
"""
        ),
        env={"NORTHWIND_PROJECT_DIR": PROJECT_DIR},
        append_env=True,
        execution_timeout=timedelta(minutes=10),
    )

    (
        preflight
        >> full_load_staging
        >> extract_employee_photos
        >> spark_full_transform
        >> verify_clickhouse
    )
