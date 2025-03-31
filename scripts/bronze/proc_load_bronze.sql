CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
    DECLARE @start_time DATETIME,@end_time DATETIME,@Total_start_time DATETIME,@Total_End_Time DATETIME;
	BEGIN TRY
	    SET @Total_start_time = GETDATE()
		PRINT '========================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '========================================================';

		PRINT '--------------------------------------------------------';
		PRINT 'Loading CRM Table';
		PRINT '--------------------------------------------------------';

		--Bulk Insert 
		--cust_info
		SET @start_time = GETDATE();
		PRINT 'crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info;

		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\User\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			 FIRSTROW = 2,
			 FIELDTERMINATOR = ',',
			 TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		--prd_info
		SET @start_time = GETDATE();
		PRINT '--------------------------------------------------------';
		PRINT 'crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info;

		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\User\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			 FIRSTROW = 2,
			 FIELDTERMINATOR = ',',
			 TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';

		--sales_details
		SET @start_time = GETDATE();
		PRINT '--------------------------------------------------------';
		PRINT 'crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details;

		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\User\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			 FIRSTROW = 2,
			 FIELDTERMINATOR = ',',
			 TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '--------------------------------------------------------';
		PRINT 'Loading ERP Table';
		PRINT '--------------------------------------------------------';
		SET @start_time = GETDATE();
		--CUST_AZ12
		PRINT 'erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12;

		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\User\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			 FIRSTROW = 2,
			 FIELDTERMINATOR = ',',
			 TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		--LOC_A101
		SET @start_time = GETDATE();
		PRINT '--------------------------------------------------------';
		PRINT 'erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101;

		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\User\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			 FIRSTROW = 2,
			 FIELDTERMINATOR = ',',
			 TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		--PX_CAT_G1V2
		SET @start_time = GETDATE();
		PRINT '--------------------------------------------------------';
		PRINT 'erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101;

		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\User\Desktop\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			 FIRSTROW = 2,
			 FIELDTERMINATOR = ',',
			 TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '--------------------------------------------------------';
	END TRY
	BEGIN CATCH
	PRINT '========================================================'
	PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
	PRINT 'Error Message' + ERROR_Message();
	PRINT 'Error Message' + cast(Error_number() AS NVARCHAR);
	PRINT 'Error Message' + CAST(Error_State() AS NVARCHAR);
	END CATCH
	SET @Total_end_time = GETDATE()
	PRINT 'Loading Bronze Layer is completed'
	PRINT '>> Total Loading Duration:' + CAST(DATEDIFF(second,@Total_start_time,@Total_end_time) AS NVARCHAR) + 'seconds';
	
END

EXEC bronze.load_bronze
