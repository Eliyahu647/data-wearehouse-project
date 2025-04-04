/*
    Customer Report View � gold.report_customers

    This view summarizes customer activity and behavior, including:
    - Age and segmentation by age group
    - Purchase history and order metrics
    - Customer lifetime value and recency
    - Segmentation into VIP, Regular, and New based on activity

    Used for customer profiling and performance analysis.

==============================================================================================
Customer Report
==============================================================================================
*/
CREATE VIEW gold.report_customers AS 
WITH  RAW_Report AS(

SELECT cd.customer_key,
       CONCAT(cd.first_name,' ',cd.last_name) AS full_name,
	   DATEDIFF(year,cd.birthdate,getdate()) AS Age,
	   SUM(fs.sales_amount) AS Orders_Value,
	   count(DISTINCT sales_order_number) AS Total_Orders,
	   SUM(fs.quantity_sold) AS Total_Quantity,
	   COUNT(DISTINCT fs.product_key) AS Total_Products,
	   MIN(fs.order_date) AS First_Order,
	   MAX(fs.order_date) AS Last_Order,
	   DATEDIFF(MONTH,MIN(fs.order_date),MAX(fs.order_date)) AS Life_Span
FROM gold.fact_sales fs LEFT JOIN gold.dim_customers cd 
ON fs.customer_key = cd.customer_key
WHERE fs.order_date IS NOT NULL
GROUP BY cd.customer_key,
         CONCAT(cd.first_name,' ',cd.last_name),
		 cd.birthdate
)

SELECT customer_key,
       full_name,
	   Age,
	   --Age groups
	   CASE WHEN AGE < 20 THEN 'Under 20'
	        WHEN AGE BETWEEN 20 AND 29 THEN '20-29'
			WHEN AGE BETWEEN 30 AND 39 THEN '30-39'
			WHEN AGE BETWEEN 40 AND 49 THEN '40-49'
			ELSE '50 and above' END AS Age_Group,
	   Orders_value,
	   total_orders,
	   Total_quantity,
	   total_products,
	   --Customer segmentation
	   CASE WHEN DATEDIFF(MONTH,First_Order,Last_Order) >= 12 AND Orders_Value > 5000 THEN 'VIP'
	        WHEN DATEDIFF(MONTH,First_Order,Last_Order) >= 12 AND Orders_Value < 5000 THEN 'Regular'
			ELSE 'New' END AS Customer_segment,
	   Last_Order,
	   --Months from the last order till today
	   DATEDIFF(MONTH,Last_Order,GETDATE()) AS recency,
	   life_span,
	   --Average order value
	   CASE WHEN Total_Orders = 0 Then 0
	   ELSE
	   Orders_Value/Total_Orders END AS AVG_Order_Value,
	   --Average Monthly spend
	   CASE WHEN Life_Span = 0 THEN Orders_Value ELSE
	   Orders_Value/Life_Span END AS AVG_Monthly_Spend
	   
FROM RAW_Report

SELECT * FROM gold.report_customers