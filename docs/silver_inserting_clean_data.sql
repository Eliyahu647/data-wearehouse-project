CREATE OR ALTER PROCEDURE silver.load_bronze AS 
BEGIN
    DECLARE @start_time DATETIME,@end_time DATETIME,@Total_start_time DATETIME,@Total_End_Time DATETIME;
	BEGIN TRY
		SET @Total_start_time = GETDATE()
		--Correction and Inserting to silver layer
		--CRM_CUST_INFO 
		PRINT '========================================================';
		PRINT 'Loading SILVER Layer';
		PRINT '========================================================';

		PRINT '--------------------------------------------------------';
		PRINT 'Loading CRM Table';
		PRINT '--------------------------------------------------------';
		
		PRINT 'crm_cust_info'
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.crm_cust_info
		INSERT INTO silver.crm_cust_info(cst_id,cst_key,cst_firstname,cst_lastname,
										cst_marital_status,cst_gndr,cst_create_date)
		SELECT cst_id,
			   cst_key,
			   TRIM(cst_firstname) AS cst_firstname,
			   TRIM(cst_lastname) AS cst_lastname,
			   (CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
					 WHEN UPPER(TRIM(cst_marital_status)) ='M' THEN 'Married'
					 ELSE 'unknown' END) AS cst_marital_status,
			   (CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
					WHEN UPPER(TRIM(cst_gndr)) ='M' THEN 'Male'
					ELSE 'unknown' END) AS cst_gndr,
			   cst_create_date
		FROM (
		SELECT 
		*,
		ROW_NUMBER() OVER(partition by cst_id ORDER BY cst_create_date DESC) AS last_date
		FROM bronze.crm_cust_info
		) AS t
		where last_date = 1 and cst_id IS NOT NULL;
	
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';

		PRINT '--------------------------------------------------------';
		PRINT 'crm_prd_info'
		--CRM_PRD_INFO
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.crm_prd_info
		INSERT INTO silver.crm_prd_info(prd_id,cat_id,prd_key,prdnm,
					prd_cost,prd_line,prd_start_dt,prd_end_dt)
		SELECT prd_id,
			   REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id,
			   SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
			   prdnm,
			   ISNULL(prd_cost,0) AS prd_cost,
			   CASE UPPER(TRIM(prd_line)) 
					WHEN 'M' THEN 'Mountain'
					WHEN 'R' THEN 'Road'
					WHEN 'S' THEN 'Other Sales'
					WHEN 'T' THEN 'Touring'
					ELSE 'Unknown' END AS prd_line,
			   CAST(prd_start_dt AS DATE) AS prd_start_dt,
			   CAST(LEAD(prd_start_dt) OVER(PARTITION BY SUBSTRING(prd_key,7,LEN(prd_key)) ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info
		ORDER BY prd_key
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';

		PRINT '--------------------------------------------------------';
		PRINT 'crm_sales_details'
		--CRM_SALES_INFO
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.crm_sales_details
		INSERT INTO silver.crm_sales_details(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,
											 sls_due_dt,sls_sales,sls_quantity,sls_price)

		SELECT sls_ord_num,
			   sls_prd_key,
			   sls_cust_id,
			   CASE WHEN LEN(sls_order_dt) != 8 OR sls_order_dt = 0 THEN NULL
					ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
					END AS sls_order_dt,
			   CASE WHEN LEN(sls_ship_dt) != 8 OR sls_ship_dt = 0 THEN NULL
					ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
					END AS sls_ship_dt,
			   CASE WHEN LEN(sls_due_dt) != 8 OR sls_due_dt = 0 THEN NULL
					ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
					END AS sls_due_dt,
			   CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
					THEN sls_quantity * ABS(sls_price)
					ELSE sls_sales END  AS sls_sales,
				sls_quantity,
				CASE WHEN sls_price IS NULL OR sls_price = 0 THEN ABS(sls_sales) / sls_quantity
					WHEN sls_price < 0 THEN sls_price * -1
					ELSE sls_price END AS sls_price

		FROM bronze.crm_sales_details
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	
		PRINT '--------------------------------------------------------';
		PRINT 'Loading ERP Table';
		PRINT '--------------------------------------------------------';
	
		PRINT 'erp_cust_az12'
		--ERP_CUST_AZ12
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.erp_cust_az12
		INSERT INTO silver.erp_cust_az12(cid,bdate,gen)

		SELECT CASE WHEN cid LIKE 'NAS%' 
					THEN SUBSTRING(cid,4,LEN(cid)) 
					ELSE cid END AS cid,
			   CASE WHEN bdate > GETDATE() THEN NULL
					ELSE bdate END AS bdate,
			   TRIM(CASE WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
					WHEN UPPER(TRIM(gen)) IN('F','FEMALE') THEN 'Female'
					ELSE 'Unknown' END) AS gen
		FROM bronze.erp_cust_az12
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';

		--ERP_LOC_A101
		SET @start_time = GETDATE();
		PRINT '--------------------------------------------------------';
		PRINT 'erp_loc_a101'

		TRUNCATE TABLE silver.erp_loc_a101

		INSERT INTO silver.erp_loc_a101(cid,cntry)
		SELECT REPLACE(cid,'-',''),
			   CASE WHEN TRIM(UPPER(cntry)) IN ('USA','US','United state') THEN 'United States'
					WHEN TRIM(cntry) = 'DE' THEN 'Germany'
					WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'UNKNOWN'
					ELSE TRIM(cntry) END AS cntry
		FROM bronze.erp_loc_a101
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';

		--ERP_PXX_CAT_G1V2
		SET @start_time = GETDATE();

		PRINT '--------------------------------------------------------';
		PRINT 'erp_pxx_cat_g1v2';

		TRUNCATE TABLE silver.erp_pxx_cat_g1v2

		INSERT INTO silver.erp_pxx_cat_g1v2(id,cat,subcat,maintenance)
		SELECT * FROM bronze.erp_pxx_cat_g1v2
		SET @end_time = GETDATE();
		PRINT '>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
	END TRY
	BEGIN CATCH
	PRINT '========================================================'
	PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
	PRINT 'Error Message' + ERROR_Message();
	PRINT 'Error Message' + cast(Error_number() AS NVARCHAR);
	PRINT 'Error Message' + CAST(Error_State() AS NVARCHAR);
	END CATCH
	SET @Total_end_time = GETDATE()
	
	PRINT '========================================================'
	PRINT 'Loading Bronze Layer is completed'
	PRINT '>> Total Loading Duration:' + CAST(DATEDIFF(second,@Total_start_time,@Total_end_time) AS NVARCHAR) + 'seconds';
	
END

EXEC silver.load_bronze