SET NOCOUNT ON;
GO

RESTORE HEADERONLY
FROM DISK = N'/var/opt/mssql/backup/Northwind_DW.bak';
GO

RESTORE FILELISTONLY
FROM DISK = N'/var/opt/mssql/backup/Northwind_DW.bak';
GO
