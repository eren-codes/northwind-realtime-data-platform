SET NOCOUNT ON;
GO

USE [Northwind_DW];
GO

SELECT
    DB_NAME() AS database_name,
    COUNT(*) AS table_count
FROM sys.tables;
GO

SELECT
    s.name AS schema_name,
    t.name AS table_name,
    SUM(p.rows) AS row_count
FROM sys.tables AS t
INNER JOIN sys.schemas AS s
    ON s.schema_id = t.schema_id
INNER JOIN sys.partitions AS p
    ON p.object_id = t.object_id
   AND p.index_id IN (0, 1)
GROUP BY
    s.name,
    t.name
ORDER BY
    s.name,
    t.name;
GO
