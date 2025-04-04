/*
====================================================================================
Product report
====================================================================================
*/
--Base CTE
CREATE VIEW gold.Product_Report AS 
WITH Product_sales AS(
SELECT sales_order_number,
       customer_key,
       order_date,
	   quantity_sold,
	   sales_amount,
       product_name,
       category_name,
	   subcategory_name,
	   cost
FROM gold.fact_sales fs LEFT JOIN gold.dim_products pd
ON fs.product_key = pd.product_key
 
),
--Aggregate CTE
Product_Aggregates AS (
SELECT 
       product_name,
       category_name,
	   subcategory_name,
	   cost,
       COUNT(DISTINCT customer_key) AS Total_Customers,
       SUM(sales_amount) AS Total_Revenue,
	   COUNT(*) AS Total_Sales,
	   SUM(quantity_sold) AS Total_Quantity,
	   MIN(order_date) AS First_Order,
	   MAX(order_date) AS Last_order,
	   DATEDIFF(MONTH,MIN(order_date),MAX(order_date)) AS Life_Span
FROM Product_sales
GROUP BY product_name,category_name,subcategory_name,
	   cost
)
--KPIs
SELECT product_name,
       category_name,
	   subcategory_name,
	   Total_customers,
	   Total_Revenue,
	   --Performance Segmentations
	   CASE WHEN Total_Revenue < 100000 THEN 'Low Performance'
	        WHEN Total_Revenue BETWEEN 100000 AND 500000 THEN 'Mid_Range'
			ELSE 'High Performance' END AS Performance_Group,
	   --Months from the last order till now
	   DATEDIFF(MONTH,last_order,GETDATE()) AS recency,
	   --AVG Revenue Per Sale
	   Total_Revenue/Total_Sales AS AVG_Revenue_Per_Sale,
	   --AVG Revenue Per Month
	   CASE WHEN Life_Span = 0 THEN Total_Revenue
	        ELSE Total_Revenue/Life_span END AS AVG_Sales_Month
FROM Product_Aggregates


SELECT * FROM gold.Product_Report