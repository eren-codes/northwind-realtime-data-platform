SET NOCOUNT ON;
GO

RESTORE DATABASE [Northwind_DW]
FROM DISK = N'/var/opt/mssql/backup/Northwind_DW.bak'
WITH
    MOVE N'Northwind_BI_1404_05_DW'
        TO N'/var/opt/mssql/data/Northwind_DW.mdf',
    MOVE N'Northwind_BI_1404_05_DW_log'
        TO N'/var/opt/mssql/data/Northwind_DW_log.ldf',
    RECOVERY,
    STATS = 5;
GO

SELECT
    name,
    state_desc,
    recovery_model_desc,
    compatibility_level
FROM sys.databases
WHERE name = N'Northwind_DW';
GO
