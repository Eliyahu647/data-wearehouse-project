/*
    Data Analysis Script – Gold Layer

    This script analyzes sales trends, product performance, 
    and customer behavior over time.

    It includes:
    - Yearly and monthly revenue trends
    - Product and category performance
    - Customer and product segmentation
*/


--Change over time--

--Sales performance over time

SELECT year(order_date) AS Year,
       SUM(sales_amount) AS Total_Revenue,
	   COUNT(DISTINCT(customer_key)) AS Total_Customers,
	   SUM(sales_amount)/COUNT(DISTINCT(customer_key)) AS AVG_Revenue
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY year(order_date)
ORDER BY year(order_date);

--Total sales per month and the running of sales over time
SELECT Year,MONTH,Total_Revenue,
       SUM(total_revenue) OVER(order by year,month)AS rolling_revenue
FROM(
SELECT year(order_date) AS Year,
       MONTH(order_date) AS MONTH,
       SUM(sales_amount) AS Total_Revenue
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY year(order_date),MONTH(order_date)
) t
ORDER BY year,month

--Moving AVG

SELECT year(order_date) AS Year,
       SUM(sales_amount) AS Total_Revenue,
	   COUNT(DISTINCT(customer_key)) AS Total_Customers,
	   SUM(sales_amount)/COUNT(DISTINCT(customer_key)) AS AVG_Revenue
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY year(order_date)
ORDER BY year(order_date);

--Total sales per month and the running of sales over time
SELECT Year,MONTH,AVG_Revenue,
       AVG(AVG_revenue) OVER(order by year,month)AS rolling_AVG
FROM(
SELECT year(order_date) AS Year,
       MONTH(order_date) AS MONTH,
       AVG(sales_amount) AS AVG_Revenue
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY year(order_date),MONTH(order_date)
) t
ORDER BY year,month


--Performances

--Yearly performance of a product 
SELECT year,
       product_name,
	   Total_Revenue,
	   LAG(Total_Revenue) OVER(PARTITION BY product_name ORDER BY YEAR) AS Previeous_Year_Sales,
	   CASE WHEN LAG(Total_Revenue) OVER(PARTITION BY product_name ORDER BY YEAR) > Total_Revenue
	        THEN 'Decrease'
			WHEN LAG(Total_Revenue) OVER(PARTITION BY product_name ORDER BY YEAR) < Total_Revenue
			THEN 'Increase'
			WHEN LAG(Total_Revenue) OVER(PARTITION BY product_name ORDER BY YEAR) = Total_Revenue
			THEN 'Even'
			ELSE NULL END AS Revenue_Change,
	   AVG(Total_Revenue) OVER(PARTITION BY product_name ORDER BY YEAR 
	                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Rolling_AVG
FROM(
SELECT YEAR(order_date) AS YEAR,
       product_name,
	   SUM(sales_amount) AS Total_Revenue
FROM gold.fact_sales fs LEFT JOIN gold.dim_products pd 
     ON fs.product_key = pd.product_key
WHERE YEAR(ORDER_DATE) IS NOT NULL
GROUP BY YEAR(order_date),product_name
) Product_Revenue_Yearly


--Which categories contribute the most to overall sales

SELECT pd.category_name,
       SUM(fs.sales_amount) AS Total_Revenue,
	   CONCAT(ROUND(CAST(SUM(fs.sales_amount) AS FLOAT)/(CAST((SELECT SUM(sales_amount)
	                              FROM gold.fact_sales) AS FLOAT))*100,3),'%') AS Revenue_Ratio
FROM gold.fact_sales fs LEFT JOIN gold.dim_products pd 
     ON fs.product_key = pd.product_key
GROUP BY pd.category_name
ORDER BY 3 DESC;


--Product Segmentation
WITH Product_Segmentation AS(

SELECT product_name,
      COST,
	   CASE WHEN COST < 100 THEN 'Below 100'
	        WHEN COST BETWEEN 101 AND 500 THEN '100-500'
			WHEN COST BETWEEN 501 AND 1000 THEN '500-1000'
			ELSE 'Above 1000' END AS Cost_Segmentaion
FROM gold.dim_products
)

SELECT Cost_Segmentaion,
       COUNT(*) AS Total_Products
FROM Product_Segmentation
GROUP BY Cost_Segmentaion
ORDER BY 2 DESC;


--Customers Segmentation
WITH  Spending_behavior AS(

SELECT cd.customer_key,
       CONCAT(cd.first_name,' ',cd.last_name) AS full_name,
	   SUM(fs.sales_amount) AS Orders_Value,
	   MIN(fs.order_date) AS First_Order,
	   MAX(fs.order_date) AS Last_Order
FROM gold.fact_sales fs LEFT JOIN gold.dim_customers cd 
ON fs.customer_key = cd.customer_key
GROUP BY cd.customer_key,
         CONCAT(cd.first_name,' ',cd.last_name)
)

SELECT Segmentation,count(*) AS Total_Customers
FROM(
SELECT customer_key,
       full_name,
	   orders_value,
	   DATEDIFF(MONTH,First_Order,GETDATE()) AS Life_Span,
	   CASE WHEN DATEDIFF(MONTH,First_Order,Last_Order) >= 12 AND Orders_Value > 5000 THEN 'VIP'
	        WHEN DATEDIFF(MONTH,First_Order,Last_Order) >= 12 AND Orders_Value < 5000 THEN 'Regular'
			ELSE 'New' END AS segmentation
FROM Spending_behavior
) t
GROUP BY Segmentation
ORDER BY 2 DESC;





