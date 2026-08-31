USE northwind_dw;

SELECT *
FROM
(
    SELECT 'dim_customer' AS table_name, count() AS row_count
    FROM dim_customer FINAL

    UNION ALL
    SELECT 'dim_date', count()
    FROM dim_date

    UNION ALL
    SELECT 'dim_employee', count()
    FROM dim_employee FINAL

    UNION ALL
    SELECT 'dim_geography', count()
    FROM dim_geography FINAL

    UNION ALL
    SELECT 'dim_product', count()
    FROM dim_product FINAL

    UNION ALL
    SELECT 'dim_shipper', count()
    FROM dim_shipper FINAL

    UNION ALL
    SELECT 'dim_supplier', count()
    FROM dim_supplier FINAL

    UNION ALL
    SELECT 'dim_territory', count()
    FROM dim_territory FINAL

    UNION ALL
    SELECT 'fact_employee_territories', count()
    FROM fact_employee_territories FINAL

    UNION ALL
    SELECT 'fact_orders', count()
    FROM fact_orders FINAL
)
ORDER BY table_name;


SELECT
    count() AS fact_rows,
    countIf(product_key = 0) AS missing_product_lookup,
    countIf(customer_key = 0) AS missing_customer_lookup,
    countIf(employee_key = 0) AS missing_employee_lookup,
    countIf(shipper_key = 0) AS missing_shipper_lookup,
    countIf(geography_key = 0) AS missing_geography_lookup,
    countIf(shipped_date_key = 0) AS not_yet_shipped_lines
FROM fact_orders FINAL;


SELECT
    countIf(
        product_key NOT IN
        (
            SELECT product_key
            FROM dim_product FINAL
        )
    ) AS orphan_product_keys,

    countIf(
        customer_key != 0
        AND customer_key NOT IN
        (
            SELECT customer_key
            FROM dim_customer FINAL
        )
    ) AS orphan_customer_keys,

    countIf(
        employee_key != 0
        AND employee_key NOT IN
        (
            SELECT employee_key
            FROM dim_employee FINAL
        )
    ) AS orphan_employee_keys,

    countIf(
        shipper_key != 0
        AND shipper_key NOT IN
        (
            SELECT shipper_key
            FROM dim_shipper FINAL
        )
    ) AS orphan_shipper_keys,

    countIf(
        geography_key NOT IN
        (
            SELECT geography_key
            FROM dim_geography FINAL
        )
    ) AS orphan_geography_keys
FROM fact_orders FINAL;


SELECT
    product.product_name,
    sum(fact.quantity) AS units_sold,
    round(
        sum(
            toFloat64(fact.unit_price)
            * fact.quantity
            * (1 - toFloat64(fact.discount))
        ),
        2
    ) AS net_sales
FROM fact_orders FINAL AS fact
INNER JOIN dim_product FINAL AS product
    ON fact.product_key = product.product_key
WHERE fact.is_deleted = 0
GROUP BY product.product_name
ORDER BY net_sales DESC
LIMIT 10;
