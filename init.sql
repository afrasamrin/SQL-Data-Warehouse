
-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO
exec etl_run_all

CREATE OR ALTER PROCEDURE etl_run_all AS
BEGIN
    DECLARE @start_time DATETIME = GETDATE();
    PRINT 'ETL Job Started at ' + CONVERT(VARCHAR, @start_time, 120);

    BEGIN TRY
        -- Bronze Layer Load
        EXEC load_bronze;
        PRINT 'Bronze layer completed.';

        -- Silver Layer Load
        EXEC load_silver;
        PRINT 'Silver layer completed.';

        -- Gold Layer Refresh (views are usually refreshed automatically, but can be rebuilt if needed)
        EXEC load_gold;
        PRINT 'Gold layer views refreshed.';

        PRINT 'ETL Job Completed Successfully!';
    END TRY

    BEGIN CATCH
        PRINT 'Error: ' + ERROR_MESSAGE();
        THROW;
    END CATCH;
END;
GO







