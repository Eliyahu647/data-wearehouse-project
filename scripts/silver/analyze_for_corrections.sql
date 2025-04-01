
/*
============================================================
Data profiling and quality checks on the bronze layer prior
to transformation and insertion into the silver layer 
============================================================
*/
--CRM_CUST_INFO 
--CHECK FOR NULLS OR DUPLICATE IN PRIMARY KEY

SELECT * FROM bronze.crm_cust_info
--CHECK
SELECT cst_id,count(*)
FROM bronze.crm_cust_info
group by cst_id
HAVING count(*) > 1 OR cst_id IS NULL
order by cst_id;

--Check for unwanted spaces
--CHECK
SELECT *
FROM bronze.crm_cust_info
where cst_firstname != TRIM(cst_firstname) 

--Correction
SELECT cst_id,
       cst_key,
	   TRIM(cst_firstname) AS cst_firstname,
	   TRIM(cst_lastname) AS cst_lastname,
	   cst_marital_status,
	   cst_gndr,
	   cst_create_date
FROM bronze.crm_cust_info;

--Data standarization & Consistency
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info;

--CRM_PRD_INFO
select prd_id,
       prd_key,
	   prdnm,prd_cost,
	   prd_line,
	   prd_start_dt,
	   prd_end_dt from bronze.crm_prd_info
--CHECK FOR NULLS OR DUPLICATE IN PRIMARY KEY
SELECT prd_id,count(*)
FROM bronze.crm_prd_info
group by prd_id
having count(*) > 1

SELECT prd_key,count(*)
FROM bronze.crm_prd_info
group by prd_key
having count(*) > 1

--Check for unwanted spaces
--CHECK
SELECT *
FROM bronze.crm_prd_info
where prdnm != TRIM(prdnm) 

--CHECK FOR NULL AND NEGATIVE NUMBER
SELECT *
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

--CHECK FOR INVALID DATE ORDERS
SELECT * 
FROM bronze.crm_prd_info
where prd_end_dt < prd_start_dt

--CRM_Sales_Info
SELECT sls_ord_num,
       sls_prd_key,
	   sls_cust_id,
	   sls_order_dt,
	   sls_ship_dt,
	   sls_due_dt,
	   sls_sales,
	   sls_quantity,
	   sls_price
FROM bronze.crm_sales_details

--Check for invalid dates
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR
      LEN(sls_order_dt) != 8 OR
	  sls_order_dt > 20500101 OR
	  sls_order_dt < 19000101 OR
	  sls_order_dt IS NULL

SELECT *
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 OR
      LEN(sls_order_dt) != 8 OR
	  sls_ship_dt > 20500101 OR
	  sls_ship_dt < 19000101 OR
	  sls_ship_dt IS NULL

--CHECK if order date always < THEN ship_date
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

--CHECK for Negative  values
SELECT *
FROM bronze.crm_sales_details
WHERE sls_quantity < 0 OR sls_price < 0 OR sls_sales < 0 OR 
      sls_quantity IS NULL OR sls_price IS NULL OR sls_sales IS NULL
	   
--CHECK if Sales = Quantity*Price
SELECT *
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price

--ERP_CUST_AZ12
SELECT cid,bdate,gen
FROM bronze.erp_cust_az12

--CHECK IF cid matches cus_id
SELECT *
FROM bronze.crm_cust_info
--CHECK FOR INVALID DATES
SELECT cid,bdate,gen
FROM bronze.erp_cust_az12
WHERE DATEDIFF(YY,bdate,GETDATE()) > 100 OR
      bdate > GETDATE()
--CHECK VALUES AT GEN 
SELECT DISTINCT(gen)
FROM bronze.erp_cust_az12


--ERP_LOC_A101
SELECT cid,cntry FROM bronze.erp_loc_a101
--CHECK FOR MATCH primary key
SELECT cid,cntry FROM bronze.erp_loc_a101
WHERE cid NOT IN (SELECT cst_key FROM bronze.crm_cust_info)
--Consistency in cntry
SELECT DISTinCT(cntry)
FROM bronze.erp_loc_a101

--ERP_PXX_CAT_G1V2
SELECT * FROM bronze.erp_pxx_cat_g1v2
