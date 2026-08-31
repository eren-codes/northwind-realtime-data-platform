SET NOCOUNT ON;
GO

USE [Northwind_OLTP];
GO

DECLARE @OrderID INT = 10248;
DECLARE @ProductID INT;
DECLARE @OriginalFreight MONEY;
DECLARE @OriginalQuantity SMALLINT;

SELECT @OriginalFreight = Freight
FROM dbo.Orders
WHERE OrderID = @OrderID;

SELECT TOP (1)
    @ProductID = ProductID,
    @OriginalQuantity = Quantity
FROM dbo.[Order Details]
WHERE OrderID = @OrderID
ORDER BY ProductID;

IF @OriginalFreight IS NULL OR @ProductID IS NULL
BEGIN
    THROW 50002, 'Test order was not found.', 1;
END;

BEGIN TRY
    BEGIN TRANSACTION;

    -- تغییر جدول Master
    UPDATE dbo.Orders
    SET Freight = @OriginalFreight + CAST(0.01 AS MONEY)
    WHERE OrderID = @OrderID;

    -- تغییر جدول Detail
    UPDATE dbo.[Order Details]
    SET Quantity = @OriginalQuantity + 1
    WHERE OrderID = @OrderID
      AND ProductID = @ProductID;

    -- بازگرداندن Detail به مقدار اولیه
    UPDATE dbo.[Order Details]
    SET Quantity = @OriginalQuantity
    WHERE OrderID = @OrderID
      AND ProductID = @ProductID;

    -- بازگرداندن Master به مقدار اولیه
    UPDATE dbo.Orders
    SET Freight = @OriginalFreight
    WHERE OrderID = @OrderID;

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    THROW;
END CATCH;

-- فرصت پردازش لاگ توسط CDC Capture Job
WAITFOR DELAY '00:00:08';

DECLARE @OrdersFromLSN BINARY(10);
DECLARE @DetailsFromLSN BINARY(10);
DECLARE @ToLSN BINARY(10);

SET @OrdersFromLSN = sys.fn_cdc_get_min_lsn(N'dbo_Orders');
SET @DetailsFromLSN = sys.fn_cdc_get_min_lsn(N'dbo_Order_Details');
SET @ToLSN = sys.fn_cdc_get_max_lsn();

PRINT 'Orders CDC changes';

SELECT
    sys.fn_cdc_map_lsn_to_time(__$start_lsn) AS change_time,
    CASE __$operation
        WHEN 1 THEN 'DELETE'
        WHEN 2 THEN 'INSERT'
        WHEN 3 THEN 'UPDATE_BEFORE'
        WHEN 4 THEN 'UPDATE_AFTER'
    END AS operation_type,
    OrderID,
    Freight
FROM cdc.fn_cdc_get_all_changes_dbo_Orders(
    @OrdersFromLSN,
    @ToLSN,
    N'all update old'
)
WHERE OrderID = @OrderID
ORDER BY __$start_lsn, __$seqval, __$operation;

PRINT 'Order Details CDC changes';

SELECT
    sys.fn_cdc_map_lsn_to_time(__$start_lsn) AS change_time,
    CASE __$operation
        WHEN 1 THEN 'DELETE'
        WHEN 2 THEN 'INSERT'
        WHEN 3 THEN 'UPDATE_BEFORE'
        WHEN 4 THEN 'UPDATE_AFTER'
    END AS operation_type,
    OrderID,
    ProductID,
    Quantity
FROM cdc.fn_cdc_get_all_changes_dbo_Order_Details(
    @DetailsFromLSN,
    @ToLSN,
    N'all update old'
)
WHERE OrderID = @OrderID
  AND ProductID = @ProductID
ORDER BY __$start_lsn, __$seqval, __$operation;

PRINT 'Final source values';

SELECT
    o.OrderID,
    o.Freight,
    od.ProductID,
    od.Quantity
FROM dbo.Orders AS o
INNER JOIN dbo.[Order Details] AS od
    ON od.OrderID = o.OrderID
WHERE o.OrderID = @OrderID
  AND od.ProductID = @ProductID;
GO
