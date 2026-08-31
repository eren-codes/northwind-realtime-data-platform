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
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="northwind_incremental_cdc",
    description="Near-real-time CDC micro-batches from PostgreSQL staging to ClickHouse",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 8, 29, tz="Asia/Tehran"),
    schedule="* * * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    dagrun_timeout=timedelta(minutes=15),
    tags=["northwind", "incremental", "cdc", "spark", "clickhouse"],
    doc_md="""
    # Northwind Incremental CDC

    This DAG runs once per minute and completes the near-real-time path:

    1. Check PostgreSQL for Kafka CDC events waiting for the warehouse.
    2. Skip the remaining tasks when no event is pending.
    3. Run the PySpark incremental fact transformation.
    4. Verify PostgreSQL processing state and ClickHouse output.

    `max_active_runs=1` prevents overlapping Spark micro-batches.
    """,
) as dag:
    check_pending_events = BashOperator(
        task_id="check_pending_events",
        bash_command=project_command(
            """
test -S /var/run/docker.sock
test -f docker-compose.yml
test -f docker-compose.airflow.yml
test -f spark/incremental_fact_transform.py

docker version >/dev/null
compose config --quiet

pending_count="$(
  compose exec -T postgres-staging sh -lc '
    psql \
      -U "$POSTGRES_USER" \
      -d "$POSTGRES_DB" \
      -Atqc "
        SELECT count(*)
        FROM cdc.raw_events
        WHERE
          processed_at IS NOT NULL
          AND processing_error IS NULL
          AND dw_processed_at IS NULL;
      "
  '
)"

test -n "$pending_count"
case "$pending_count" in
  *[!0-9]*)
    echo "INVALID_PENDING_COUNT=$pending_count" >&2
    exit 1
    ;;
esac

echo "PENDING_DW_EVENTS=$pending_count"

if [ "$pending_count" -eq 0 ]; then
  echo "INCREMENTAL_PIPELINE=NOOP"
  exit 99
fi

echo "INCREMENTAL_PREFLIGHT=PASS"
"""
        ),
        env={"NORTHWIND_PROJECT_DIR": PROJECT_DIR},
        append_env=True,
        skip_on_exit_code=99,
        execution_timeout=timedelta(minutes=5),
    )

    spark_incremental_transform = BashOperator(
        task_id="spark_incremental_transform",
        bash_command=project_command(
            """
compose --profile tools run --rm spark-runner \
  /opt/spark/bin/spark-submit \
  --master "local[1]" \
  --driver-memory 768m \
  --conf spark.sql.shuffle.partitions=2 \
  --conf spark.default.parallelism=1 \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.ui.enabled=false \
  /opt/spark/work-dir/spark/incremental_fact_transform.py
"""
        ),
        env={"NORTHWIND_PROJECT_DIR": PROJECT_DIR},
        append_env=True,
        execution_timeout=timedelta(minutes=10),
    )

    verify_incremental_load = BashOperator(
        task_id="verify_incremental_load",
        bash_command=project_command(
            """
compose exec -T postgres-staging sh -lc '
set -euo pipefail

query() {
  psql \
    -U "$POSTGRES_USER" \
    -d "$POSTGRES_DB" \
    -Atqc "$1"
}

service_status="$(query "
  SELECT status
  FROM control.cdc_service_state
  WHERE service_name = '\\''incremental-transform'\\'';
")"

failed_event_count="$(query "
  SELECT count(*)
  FROM cdc.raw_events
  WHERE
    dw_processed_at IS NULL
    AND dw_processing_error IS NOT NULL;
")"

test "$service_status" = "success"
test "$failed_event_count" = "0"

echo "INCREMENTAL_SERVICE_STATUS=$service_status"
echo "INCREMENTAL_FAILED_EVENTS=$failed_event_count"
'

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

current_fact_count="$(
  query "SELECT count() FROM fact_orders FINAL WHERE is_deleted = 0"
)"
incremental_fact_count="$(
  query "SELECT count() FROM fact_orders FINAL WHERE length(source_lsn) > 0"
)"

test "$current_fact_count" -gt 0
test "$incremental_fact_count" -gt 0

echo "CLICKHOUSE_CURRENT_FACTS=$current_fact_count"
echo "CLICKHOUSE_INCREMENTAL_FACTS=$incremental_fact_count"
echo "INCREMENTAL_WAREHOUSE_VERIFICATION=PASS"
'
"""
        ),
        env={"NORTHWIND_PROJECT_DIR": PROJECT_DIR},
        append_env=True,
        execution_timeout=timedelta(minutes=5),
    )

    check_pending_events >> spark_incremental_transform >> verify_incremental_load
