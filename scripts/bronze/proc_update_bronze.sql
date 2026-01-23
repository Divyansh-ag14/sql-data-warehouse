/*
===============================================================================
Stored Procedure: Update Bronze Layer (Staging -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure updates data in the 'bronze' schema from the staging area.
    It performs the following actions:
    - Removes leading/trailing spaces and unwanted characters from specific columns.
    - Updates the bronze tables with the cleaned data.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.update_bronze;
===============================================================================


*/

CREATE OR ALTER PROCEDURE bronze.update_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Updating Bronze Layer Data';
        PRINT '================================================';
        PRINT '>> Cleaning gen column in erp_cust_az12 table in bronze layer';
        PRINT '>> Removing leading/trailing spaces and unwanted characters from gen column';
        SET @start_time = GETDATE();
        UPDATE DataWarehouse.bronze.erp_cust_az12
        SET gen = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''), CHAR(9), '')));
        SET @end_time = GETDATE();
        PRINT '>> Update Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';   

        PRINT '>> Cleaning cntry column in erp_loc_a101 table in bronze layer';
        PRINT '>> Removing leading/trailing spaces and unwanted characters from cntry column';
        SET @start_time = GETDATE();
        UPDATE DataWarehouse.bronze.erp_loc_a101
        SET cntry = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(9), '')));
        SET @end_time = GETDATE();
        PRINT '>> Update Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        PRINT '>> Cleaning maintenance column in erp_px_cat_g1v2 table in bronze layer';
        PRINT '>> Removing leading/trailing spaces and unwanted characters from maintenance column';
        SET @start_time = GETDATE();
        UPDATE DataWarehouse.bronze.erp_px_cat_g1v2
        SET maintenance = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(maintenance, CHAR(13), ''), CHAR(10), ''), CHAR(9), '')));
        SET @end_time = GETDATE();
        PRINT '>> Update Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
        SET @batch_end_time = GETDATE();
        PRINT '=========================================='
        PRINT 'Updating Bronze Layer Data is Completed';
        PRINT '   - Total Update Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '=========================================='
    END TRY
    BEGIN CATCH
        PRINT 'Error occurred while updating Bronze Layer Data: ' + ERROR_MESSAGE();
    END CATCH
END



-- ====================================================================
-- Cleaning 'gen' column in 'erp_cust_az12' table in bronze layer
-- ====================================================================

-- Inspect first 20 non-null entries in 'gen' column
-- SELECT TOP 20
--   gen,
--   '[' + gen + ']' AS visible,
--   LEN(gen) AS len_,
--   DATALENGTH(gen) AS bytes_,
--   UNICODE(SUBSTRING(gen, LEN(gen), 1)) AS u_last
-- FROM DataWarehouse.bronze.erp_cust_az12
-- WHERE gen IS NOT NULL;


-- -- Remove leading/trailing spaces and unwanted characters from 'gen' column
-- UPDATE DataWarehouse.bronze.erp_cust_az12
-- SET gen = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''), CHAR(9), '')));

-- ====================================================================
-- Cleaning 'cntry' column in 'erp_loc_a101' table in bronze layer
-- ====================================================================

-- Inspect first 20 non-null entries in 'cntry' column
-- SELECT TOP 20
--   cntry,
--   '[' + cntry + ']' AS visible,
--   LEN(cntry) AS len_,
--   DATALENGTH(cntry) AS bytes_,
--   UNICODE(SUBSTRING(cntry, LEN(cntry), 1)) AS u_last
-- FROM DataWarehouse.bronze.erp_loc_a101
-- WHERE cntry IS NOT NULL;

-- -- Remove leading/trailing spaces and unwanted characters from 'cntry' column
-- UPDATE DataWarehouse.bronze.erp_loc_a101
-- SET cntry = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''), CHAR(9), '')));

-- -- ====================================================================
-- -- Cleaning 'maintenance' column in 'erp_px_cat_g1v2' table in bronze layer
-- -- ====================================================================

-- -- Inspect first 20 non-null entries in 'maintenance' column
-- SELECT TOP 20
--   maintenance,
--   '[' + maintenance + ']' AS visible,
--   LEN(maintenance) AS len_,
--   DATALENGTH(maintenance) AS bytes_,
--   UNICODE(SUBSTRING(maintenance, LEN(maintenance), 1)) AS u_last
-- FROM DataWarehouse.bronze.erp_px_cat_g1v2
-- WHERE maintenance IS NOT NULL;

-- -- Remove leading/trailing spaces and unwanted characters from 'maintenance' column
-- UPDATE DataWarehouse.bronze.erp_px_cat_g1v2
-- SET maintenance = LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(maintenance, CHAR(13), ''), CHAR(10), ''), CHAR(9), '')));