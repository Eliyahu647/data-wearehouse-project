/*
    This script defines the **Gold Layer** views in a Data Warehouse architecture.

    The Gold Layer represents the final, business-ready presentation layer 
    used for analytics, dashboards, and reporting.

    It includes:
    ----------------------------------------------------------------------
    1. gold.dim_customers:
       - A cleaned and enriched customer dimension view.
       - Combines CRM and ERP data to include demographics and personal details.
       - Handles missing or unknown gender values using fallback logic.

    2. gold.dim_products:
       - A detailed product dimension view with business categories and status.
       - Filters out historical (discontinued) products.
       - Joins product master data with product classification info.

    3. gold.fact_sales:
       - A sales fact view linking customers and products to transactional data.
       - Enriched with surrogate keys (customer_key, product_key) from dimensions.
       - Serves as the core fact table for BI reporting on sales performance.

    These views are not materialized — they are always up to date 
    with the underlying Silver Layer data.
*/
CREATE VIEW gold.dim_customers AS(
SELECT 
       ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
       ci.cst_id AS customer_id,
       ci.cst_key AS customer_number,
	     ci.cst_firstname AS first_name,
	     ci.cst_lastname AS last_name,
	     ca.bdate AS birthdate,
	     la.cntry AS country,
	     ci.cst_marital_status AS marital_status,
	     CASE WHEN ci.cst_gndr != 'unknown' THEN ci.cst_gndr
	          ELSE COALESCE(ca.gen, 'unknown') END AS gender,
	     ci.cst_create_date AS create_date	   
FROM silver.crm_cust_info ci 
LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid
)


CREATE VIEW gold.dim_products AS(
SELECT 
    ROW_NUMBER () OVER(ORDER BY pn.prd_start_dt,pn.prd_key) AS product_key,
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prdnm AS product_name,
    pn.cat_id AS category_id,
    px.cat AS category_name,
    px.subcat AS subcategory_name,
    px.maintenance AS maintenance_flag,
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_pxx_cat_g1v2 px
    ON pn.cat_id = px.id
WHERE pn.prd_end_dt IS NULL -- Filtering Historical Data
)

CREATE VIEW gold.fact_sales AS(
SELECT 
    sd.sls_ord_num AS sales_order_number,
    pd.product_key AS product_key,
    cd.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt AS ship_date,
    sd.sls_due_dt AS due_date,
    sd.sls_sales AS sales_amount,
    sd.sls_quantity AS quantity_sold,
    sd.sls_price AS unit_price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pd ON sd.sls_prd_key = pd.product_number
LEFT JOIN gold.dim_customers cd ON sd.sls_cust_id = cd.customer_id
)
