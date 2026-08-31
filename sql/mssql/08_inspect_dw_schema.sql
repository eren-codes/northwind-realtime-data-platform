SET NOCOUNT ON;
GO

USE [Northwind_DW];
GO

PRINT 'DW TABLE COLUMNS';

SELECT
    t.name AS table_name,
    c.column_id,
    c.name AS column_name,
    ty.name AS data_type,
    CASE
        WHEN ty.name IN (N'nvarchar', N'nchar') AND c.max_length > 0
            THEN c.max_length / 2
        ELSE c.max_length
    END AS max_length,
    c.precision,
    c.scale,
    c.is_nullable,
    c.is_identity
FROM sys.tables AS t
INNER JOIN sys.columns AS c
    ON c.object_id = t.object_id
INNER JOIN sys.types AS ty
    ON ty.user_type_id = c.user_type_id
WHERE t.is_ms_shipped = 0
  AND t.name <> N'sysdiagrams'
ORDER BY t.name, c.column_id;
GO

PRINT 'DW PRIMARY KEYS';

SELECT
    t.name AS table_name,
    c.name AS primary_key_column,
    ic.key_ordinal
FROM sys.indexes AS i
INNER JOIN sys.index_columns AS ic
    ON ic.object_id = i.object_id
   AND ic.index_id = i.index_id
INNER JOIN sys.tables AS t
    ON t.object_id = i.object_id
INNER JOIN sys.columns AS c
    ON c.object_id = ic.object_id
   AND c.column_id = ic.column_id
WHERE i.is_primary_key = 1
ORDER BY t.name, ic.key_ordinal;
GO

PRINT 'DW FOREIGN KEYS';

SELECT
    OBJECT_NAME(fkc.parent_object_id) AS source_table,
    COL_NAME(
        fkc.parent_object_id,
        fkc.parent_column_id
    ) AS source_column,
    OBJECT_NAME(fkc.referenced_object_id) AS referenced_table,
    COL_NAME(
        fkc.referenced_object_id,
        fkc.referenced_column_id
    ) AS referenced_column
FROM sys.foreign_key_columns AS fkc
ORDER BY source_table, source_column;
GO

