import json
import logging
import uuid
from typing import Any

import pymssql
import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from etl.config import Settings
from etl.table_config import TABLE_SPECS, TableSpec


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
LOGGER = logging.getLogger("northwind-full-load")


def normalize_row(row: tuple[Any, ...]) -> tuple[Any, ...]:
    return tuple(
        bytes(value)
        if isinstance(value, (bytearray, memoryview))
        else value
        for value in row
    )


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
        appname="northwind-full-load",
    )


def connect_postgres(settings: Settings, autocommit: bool = False):
    connection = psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_database,
        user=settings.postgres_user,
        password=settings.postgres_password,
        connect_timeout=30,
    )
    connection.autocommit = autocommit
    return connection


def create_pipeline_run(log_connection, batch_id: uuid.UUID) -> int:
    with log_connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO control.pipeline_runs (
                pipeline_name,
                run_type,
                status,
                metadata
            )
            VALUES (%s, %s, %s, %s)
            RETURNING run_id
            """,
            (
                "northwind_full_load",
                "full",
                "running",
                Jsonb({"batch_id": str(batch_id)}),
            ),
        )
        return cursor.fetchone()[0]


def finish_pipeline_run(
    log_connection,
    run_id: int,
    status: str,
    rows_read: int,
    rows_written: int,
    metadata: dict[str, Any],
    error_message: str | None = None,
) -> None:
    with log_connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE control.pipeline_runs
            SET
                status = %s,
                finished_at = CURRENT_TIMESTAMP,
                rows_read = %s,
                rows_written = %s,
                error_message = %s,
                metadata = %s
            WHERE run_id = %s
            """,
            (
                status,
                rows_read,
                rows_written,
                error_message,
                Jsonb(metadata),
                run_id,
            ),
        )


def record_quality_result(
    log_connection,
    run_id: int,
    table_name: str,
    source_count: int,
    target_count: int,
) -> None:
    status = "passed" if source_count == target_count else "failed"

    with log_connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO control.data_quality_results (
                run_id,
                check_name,
                table_name,
                status,
                expected_value,
                actual_value
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                "source_target_row_count",
                table_name,
                status,
                str(source_count),
                str(target_count),
            ),
        )


def truncate_staging(postgres_cursor) -> None:
    table_names = sql.SQL(", ").join(
        sql.Identifier("staging", spec.target_table)
        for spec in TABLE_SPECS
    )

    postgres_cursor.execute(
        sql.SQL("TRUNCATE TABLE {}").format(table_names)
    )


def load_table(
    source_cursor,
    postgres_cursor,
    spec: TableSpec,
    batch_id: uuid.UUID,
) -> tuple[int, int]:
    LOGGER.info("Loading staging.%s", spec.target_table)

    source_cursor.execute(spec.source_query)

    copy_columns = (*spec.target_columns, "_batch_id")

    copy_statement = sql.SQL(
        "COPY {} ({}) FROM STDIN"
    ).format(
        sql.Identifier("staging", spec.target_table),
        sql.SQL(", ").join(
            sql.Identifier(column)
            for column in copy_columns
        ),
    )

    source_count = 0

    with postgres_cursor.copy(copy_statement) as copy:
        while True:
            rows = source_cursor.fetchmany(1000)

            if not rows:
                break

            for row in rows:
                copy.write_row(
                    (*normalize_row(row), batch_id)
                )
                source_count += 1

    postgres_cursor.execute(
        sql.SQL("SELECT COUNT(*) FROM {}").format(
            sql.Identifier("staging", spec.target_table)
        )
    )
    target_count = postgres_cursor.fetchone()[0]

    if source_count != target_count:
        raise RuntimeError(
            f"Row-count mismatch for {spec.target_table}: "
            f"source={source_count}, target={target_count}"
        )

    LOGGER.info(
        "Loaded staging.%s: %s rows",
        spec.target_table,
        target_count,
    )

    return source_count, target_count


def main() -> None:
    settings = Settings.from_env()
    batch_id = uuid.uuid4()

    source_connection = None
    postgres_connection = None
    log_connection = None
    run_id = None

    counts: dict[str, dict[str, int]] = {}
    total_read = 0
    total_written = 0

    try:
        LOGGER.info("Starting Full Load, batch_id=%s", batch_id)

        source_connection = connect_mssql(settings)
        postgres_connection = connect_postgres(settings)
        log_connection = connect_postgres(
            settings,
            autocommit=True,
        )

        run_id = create_pipeline_run(log_connection, batch_id)

        source_cursor = source_connection.cursor()
        postgres_cursor = postgres_connection.cursor()

        try:
            with postgres_connection.transaction():
                truncate_staging(postgres_cursor)

                for spec in TABLE_SPECS:
                    source_count, target_count = load_table(
                        source_cursor,
                        postgres_cursor,
                        spec,
                        batch_id,
                    )

                    counts[spec.target_table] = {
                        "source": source_count,
                        "target": target_count,
                    }

                    total_read += source_count
                    total_written += target_count
        finally:
            source_cursor.close()
            postgres_cursor.close()

        for table_name, table_counts in counts.items():
            record_quality_result(
                log_connection,
                run_id,
                table_name,
                table_counts["source"],
                table_counts["target"],
            )

        finish_pipeline_run(
            log_connection=log_connection,
            run_id=run_id,
            status="success",
            rows_read=total_read,
            rows_written=total_written,
            metadata={
                "batch_id": str(batch_id),
                "tables": counts,
            },
        )

        LOGGER.info(
            "Full Load completed: read=%s, written=%s",
            total_read,
            total_written,
        )

    except Exception as error:
        LOGGER.exception("Full Load failed")

        if log_connection is not None and run_id is not None:
            finish_pipeline_run(
                log_connection=log_connection,
                run_id=run_id,
                status="failed",
                rows_read=total_read,
                rows_written=total_written,
                metadata={
                    "batch_id": str(batch_id),
                    "tables": counts,
                },
                error_message=str(error)[:4000],
            )

        raise

    finally:
        if source_connection is not None:
            source_connection.close()

        if postgres_connection is not None:
            postgres_connection.close()

        if log_connection is not None:
            log_connection.close()


if __name__ == "__main__":
    main()
