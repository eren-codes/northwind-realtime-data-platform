\pset pager off

\echo '=== Staging row-count verification ==='

WITH counts(table_name, actual_rows, expected_rows) AS (
    VALUES
        ('customers',           (SELECT COUNT(*) FROM staging.customers),           91::bigint),
        ('employees',           (SELECT COUNT(*) FROM staging.employees),            9::bigint),
        ('suppliers',           (SELECT COUNT(*) FROM staging.suppliers),            29::bigint),
        ('categories',          (SELECT COUNT(*) FROM staging.categories),            8::bigint),
        ('products',            (SELECT COUNT(*) FROM staging.products),             77::bigint),
        ('shippers',            (SELECT COUNT(*) FROM staging.shippers),              3::bigint),
        ('regions',             (SELECT COUNT(*) FROM staging.regions),               4::bigint),
        ('territories',         (SELECT COUNT(*) FROM staging.territories),           53::bigint),
        ('employee_territories',(SELECT COUNT(*) FROM staging.employee_territories), 49::bigint),
        ('orders',              (SELECT COUNT(*) FROM staging.orders),               830::bigint),
        ('order_details',       (SELECT COUNT(*) FROM staging.order_details),        2155::bigint)
),
report AS (
    SELECT table_name, actual_rows, expected_rows
    FROM counts

    UNION ALL

    SELECT
        'TOTAL',
        SUM(actual_rows),
        SUM(expected_rows)
    FROM counts
)
SELECT
    table_name,
    actual_rows,
    expected_rows,
    CASE
        WHEN actual_rows = expected_rows THEN 'PASS'
        ELSE 'FAIL'
    END AS result
FROM report
ORDER BY (table_name = 'TOTAL'), table_name;

\echo '=== Pipeline runs ==='

SELECT *
FROM control.pipeline_runs
ORDER BY started_at DESC
LIMIT 3;

\echo '=== Data-quality checks ==='

TABLE control.data_quality_results;
