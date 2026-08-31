SET NOCOUNT ON;
GO

USE [master];
GO

DECLARE @agent_xps INT;

SELECT @agent_xps = CONVERT(INT, value_in_use)
FROM sys.configurations
WHERE name = N'Agent XPs';

IF ISNULL(@agent_xps, 0) <> 1
BEGIN
    THROW 50001, 'SQL Server Agent is not running.', 1;
END;

PRINT 'SQL Server Agent is running.';
GO

USE [Northwind_OLTP];
GO

IF (
    SELECT is_cdc_enabled
    FROM sys.databases
    WHERE name = DB_NAME()
) = 0
BEGIN
    EXEC sys.sp_cdc_enable_db;
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM cdc.change_tables
    WHERE capture_instance = N'dbo_Orders'
)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name = N'Orders',
        @capture_instance = N'dbo_Orders',
        @role_name = NULL,
        @supports_net_changes = 1;
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM cdc.change_tables
    WHERE capture_instance = N'dbo_Order_Details'
)
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name = N'Order Details',
        @capture_instance = N'dbo_Order_Details',
        @role_name = NULL,
        @supports_net_changes = 1;
END;
GO

SELECT
    DB_NAME() AS database_name,
    is_cdc_enabled
FROM sys.databases
WHERE name = DB_NAME();
GO

SELECT
    s.name AS schema_name,
    t.name AS table_name,
    t.is_tracked_by_cdc
FROM sys.tables AS t
INNER JOIN sys.schemas AS s
    ON s.schema_id = t.schema_id
WHERE t.name IN (N'Orders', N'Order Details')
ORDER BY t.name;
GO

SELECT
    capture_instance,
    supports_net_changes,
    index_name,
    create_date
FROM cdc.change_tables
ORDER BY capture_instance;
GO

SELECT
    name AS cdc_job_name,
    enabled
FROM msdb.dbo.sysjobs
WHERE name LIKE N'cdc.Northwind_OLTP%';
GO
