/*
    Stored Procedure: bronze.load_bronze

    Description:
    This stored procedure is used to load raw data into the Bronze layer 
    of a data warehouse using BULK INSERT operations.

    It performs the following tasks:
    1. Truncates each existing Bronze table to ensure fresh loading.
    2. Loads CSV files from a local directory into their respective tables.
    3. Logs the duration of each load operation using PRINT statements.
    4. Includes error handling via TRY-CATCH to display relevant error messages if any issue occurs.

    Fixes Applied:
    - Corrected a bug where the PX_CAT_G1V2 file was incorrectly being loaded into the wrong table.
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @Total_start_time DATETIME, @Total_End_Time DATETIME;

    BEGIN TRY
        SET @Total_start_time = GETDATE();

        PRINT '========================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '========================================================';

        PRINT '--------------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '--------------------------------------------------------';

        -- Load crm_cust_info
        SET @start_time = GETDATE();
        PRINT 'crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\User\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- Load crm_prd_info
        SET @start_time = GETDATE();
        PRINT '--------------------------------------------------------';
        PRINT 'crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\User\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- Load crm_sales_details
        SET @start_time = GETDATE();
        PRINT '--------------------------------------------------------';
        PRINT 'crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\User\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        PRINT '--------------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '--------------------------------------------------------';

        -- Load erp_cust_az12
        SET @start_time = GETDATE();
        PRINT 'erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\User\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- Load erp_loc_a101
        SET @start_time = GETDATE();
        PRINT '--------------------------------------------------------';
        PRINT 'erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\User\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        -- Load erp_pxx_cat_g1v2 (FIXED)
        SET @start_time = GETDATE();
        PRINT '--------------------------------------------------------';
        PRINT 'erp_pxx_cat_g1v2';
        TRUNCATE TABLE bronze.erp_pxx_cat_g1v2;

        BULK INSERT bronze.erp_pxx_cat_g1v2
        FROM 'C:\Users\User\Desktop\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';

        PRINT '--------------------------------------------------------';

    END TRY
    BEGIN CATCH
        PRINT '========================================================';
        PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
    END CATCH

    SET @Total_End_Time = GETDATE();
    PRINT 'Loading Bronze Layer is completed';
    PRINT '>> Total Loading Duration: ' + CAST(DATEDIFF(second, @Total_start_time, @Total_End_Time) AS NVARCHAR) + ' seconds';
END;

-- Execute the procedure
EXEC bronze.load_bronze;
